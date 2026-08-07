import asyncio
import signal
import logging
import os
from datetime import datetime, timedelta
from collections import defaultdict
from aiohttp import web
from rich.logging import RichHandler
from rich.highlighter import NullHighlighter, JSONHighlighter
from blinkbridge.stream_server import StreamServer
from blinkbridge.blink import CameraManager
from blinkbridge.config import *
import blinkbridge.patches as patches

patches.apply()


log = logging.getLogger(__name__)

# On-demand-only by design (2026-08-03 user decision) -- Blink battery
# cameras aren't built for continuous streaming, and Blink's own liveview
# sessions are a limited resource. A session that's outlived its viewer
# (e.g. a browser tab closed without calling /stop) shouldn't run forever,
# so it's force-stopped after this long regardless.
LIVEVIEW_MAX_DURATION = timedelta(minutes=5)

# 2026-08-06: cap for the failure-backoff below -- never let a
# repeatedly-failing camera wait longer than this between retries.
MAX_RESTART_BACKOFF = timedelta(minutes=15)

class Application:
    def __init__(self):
        self.stream_servers = {}
        self.cam_manager = None
        self.running = False
        # camera_name -> {"live_stream": BlinkLiveStream, "started_at": datetime}
        self.live_sessions = {}

    async def start_stream(self, camera_name: str, redownload: bool=False) -> StreamServer:
        if redownload:
            await self.cam_manager.refresh_metadata()

        log.debug(f"{camera_name}: getting latest clip")
        file_name_initial_video = await self.cam_manager.save_latest_clip(camera_name, force=redownload)

        log.info(f"{camera_name}: starting stream server")
        stream_server = StreamServer(camera_name)
        stream_server.start_server(file_name_initial_video)  
        self.stream_servers[camera_name] = stream_server

        return stream_server

    async def check_for_motion(self, camera_name: str) -> bool:
        ss = self.stream_servers[camera_name]

        if not ss.is_running():
            return False

        file_name_new_clip = await self.cam_manager.check_for_motion(camera_name)

        if not file_name_new_clip:
            return False

        log.info(f"{ss.stream_name}: motion detected, adding video")
        ss.add_video(file_name_new_clip)

        return True

    async def start_liveview(self, camera_name: str) -> bool:
        """
        Start a real, on-demand Blink liveview session for camera_name and
        relay it through to the same mediamtx output path the motion-clip
        loop normally publishes to. Idempotent -- calling this while a
        session is already active just extends its life, doesn't start a
        second one.
        """
        if camera_name not in self.stream_servers:
            log.warning(f"{camera_name}: liveview requested for unknown/disabled camera")
            return False

        if camera_name in self.live_sessions:
            log.debug(f"{camera_name}: liveview already active")
            return True

        camera = self.cam_manager.blink.cameras[camera_name]
        log.info(f"{camera_name}: starting real liveview session")
        live_stream = await camera.init_livestream()
        await live_stream.start(host="127.0.0.1", port=0)
        port = live_stream.socket.getsockname()[1]

        # Runs the auth handshake, packet relay, keepalives, and Blink
        # command-status polling for the life of this session -- see
        # blinkpy's BlinkLiveStream.feed(). Cancelled in stop_liveview().
        feed_task = asyncio.create_task(live_stream.feed())

        self.live_sessions[camera_name] = {
            "live_stream": live_stream,
            "feed_task": feed_task,
            "started_at": datetime.now(),
        }

        # Give the session a moment to complete the auth handshake with
        # Blink's server before pointing ffmpeg at the local relay port --
        # otherwise ffmpeg's first connection attempt can race the auth.
        await asyncio.sleep(1.5)

        self.stream_servers[camera_name].start_live_relay(f"tcp://127.0.0.1:{port}")
        return True

    async def stop_liveview(self, camera_name: str) -> bool:
        """Stop an active liveview session and resume the normal motion-clip loop."""
        session = self.live_sessions.pop(camera_name, None)
        if session is None:
            log.debug(f"{camera_name}: no active liveview session to stop")
            return False

        log.info(f"{camera_name}: stopping liveview session")
        session["live_stream"].stop()
        session["feed_task"].cancel()

        if camera_name in self.stream_servers:
            self.stream_servers[camera_name].stop_live_relay()

        return True

    async def _enforce_liveview_max_duration(self) -> None:
        now = datetime.now()
        expired = [
            name for name, session in self.live_sessions.items()
            if now > session["started_at"] + LIVEVIEW_MAX_DURATION
        ]
        for camera_name in expired:
            log.warning(f"{camera_name}: liveview session exceeded max duration, force-stopping")
            await self.stop_liveview(camera_name)

    async def _reap_dead_liveview_sessions(self) -> None:
        """
        Found 2026-08-03 during real-camera testing: Blink's own liveview
        server can end a session on its own (a throttled/dropped
        connection on Blink's end, not anything wrong with the ffmpeg
        relay) -- when that happens, BlinkLiveStream.feed() returns and
        the relay is left publishing nothing, with the camera stuck
        "in a live session" from this app's point of view (motion-check
        skipped, no automatic fallback) until someone happens to call
        /stop. This detects that and falls back to the normal motion-clip
        loop automatically instead of leaving the camera dark.
        """
        for camera_name, session in list(self.live_sessions.items()):
            feed_task = session["feed_task"]
            if not feed_task.done():
                continue

            exc = feed_task.exception() if not feed_task.cancelled() else None
            if exc:
                log.error(f"{camera_name}: liveview session ended unexpectedly: {exc}")
            else:
                log.warning(f"{camera_name}: liveview session ended on Blink's end, falling back to motion-clip loop")
            await self.stop_liveview(camera_name)

    async def start(self) -> None:
        self.running = True
        self.cam_manager = CameraManager()
        await self.cam_manager.start()

        # get enabled cameras
        enabled_cameras = set(CONFIG['cameras']['enabled']) if CONFIG['cameras']['enabled'] else set(self.cam_manager.get_cameras())
        enabled_cameras = enabled_cameras - set(CONFIG['cameras']['disabled'])
        log.info(f"enabled cameras: {enabled_cameras}")      

        # create stream servers for each camera
        for camera in self.cam_manager.get_cameras():
            if camera not in enabled_cameras:
                continue
            
            ss = await self.start_stream(camera)
            ss.failure_count = 0
            ss.datetime_started = datetime.now()

        log.info(f"monitoring cameras for motion")
        while self.running:
            await self._reap_dead_liveview_sessions()
            await self._enforce_liveview_max_duration()

            # check for motion on each stream server -- skip any camera
            # currently in a real liveview session; its StreamServer's
            # `process`/concat files are owned by the live relay right
            # now (see start_live_relay), not the motion-clip loop.
            for camera_name in self.stream_servers:
                if camera_name in self.live_sessions:
                    continue
                try:
                    await self.check_for_motion(camera_name)
                except Exception as e:
                    log.error(f"{camera_name}: error checking for motion: {e}")
                    self.stream_servers[camera_name].close()

            # check if any stream servers are stopped and restart them
            # (skip cameras in a live session for the same reason as above)
            for camera_name in list(self.stream_servers.keys()):
                if camera_name in self.live_sessions:
                    continue
                ss = self.stream_servers[camera_name]

                if not ss.is_running():
                    # 2026-08-06: previously popped the camera from
                    # stream_servers permanently once max_failures was
                    # hit, with nothing anywhere that ever added it back
                    # -- only a full container restart recovered (see
                    # this project's MAINTENANCE.md for the repeated
                    # real-world incident this caused). Blink's cloud
                    # API throws ordinary transient errors (brief
                    # throttling, connection failures) every 15min to a
                    # few hours -- three in a row isn't unusual and
                    # isn't evidence the camera or account is actually
                    # broken. Never give up permanently: back off longer
                    # after repeated failures instead, capped so a real
                    # outage doesn't hammer Blink's API but a transient
                    # one still recovers on its own.
                    if ss.failure_count >= CONFIG['cameras']['max_failures'] - 1:
                        backoff = min(
                            DELAY_RESTART * (2 ** (ss.failure_count - CONFIG['cameras']['max_failures'] + 2)),
                            MAX_RESTART_BACKOFF,
                        )
                        log.warning(f"{camera_name}: {ss.failure_count + 1} failures, backing off {backoff} before retrying (not disabling)")
                        if datetime.now() < ss.datetime_started + backoff:
                            continue
                    else:
                        log.warning(f"{camera_name}: server failed {ss.failure_count + 1} time(s)")

                        # do nothing if stream was last started less certain time ago
                        if datetime.now() < ss.datetime_started + DELAY_RESTART:
                            continue

                    # create new stream server
                    ss_new = await self.start_stream(camera_name, redownload=True)
                    ss_new.failure_count = ss.failure_count + 1
                    ss_new.datetime_started = datetime.now()

            await asyncio.sleep(CONFIG['blink']['poll_interval'])

    async def close(self) -> None:
        self.running = False

        for camera_name in list(self.live_sessions.keys()):
            await self.stop_liveview(camera_name)

        if self.cam_manager:
            await self.cam_manager.close()

        for ss in self.stream_servers.values():
            ss.close()


# Reachable from other containers on the shared Docker network by
# container name (e.g. http://blinkbridge:8811/...) -- no host port
# publish needed, only cabin-backend calls this. See
# cabin-orchestration-platform's CameraMediaController for the caller.
LIVEVIEW_HTTP_PORT = 8811

def make_web_app(app: Application) -> web.Application:
    async def handle_start(request: web.Request) -> web.Response:
        camera_name = request.match_info["camera"]
        ok = await app.start_liveview(camera_name)
        return web.json_response({"ok": ok}, status=200 if ok else 404)

    async def handle_stop(request: web.Request) -> web.Response:
        camera_name = request.match_info["camera"]
        ok = await app.stop_liveview(camera_name)
        return web.json_response({"ok": ok}, status=200 if ok else 404)

    web_app = web.Application()
    web_app.router.add_post("/liveview/{camera}/start", handle_start)
    web_app.router.add_post("/liveview/{camera}/stop", handle_stop)
    return web_app

async def main() -> None:
    app = Application()

    # Create a cancellation event to coordinate shutdown
    shutdown_event = asyncio.Event()

    def handle_exit():
        # Signal the shutdown event when Ctrl+C is received
        shutdown_event.set()

    # Add signal handlers using loop.add_signal_handler
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, handle_exit)

    web_runner = None
    try:
        # Start the application
        start_task = asyncio.create_task(app.start())

        # Liveview control API -- started once app.start() has had a
        # moment to log in and create stream servers, so /start requests
        # arriving immediately after boot don't race camera_manager setup.
        await asyncio.sleep(2)
        web_app = make_web_app(app)
        web_runner = web.AppRunner(web_app)
        await web_runner.setup()
        site = web.TCPSite(web_runner, "0.0.0.0", LIVEVIEW_HTTP_PORT)
        await site.start()
        log.info(f"liveview control API listening on :{LIVEVIEW_HTTP_PORT}")

        # Wait for shutdown signal
        await shutdown_event.wait()

        log.info("Shutting down...")

        # Cancel the start task and wait for it to complete
        start_task.cancel()
        try:
            await start_task
        except asyncio.CancelledError:
            pass

    except Exception as e:
        log.error(f"Unexpected error: {e}")

    finally:
        if web_runner:
            await web_runner.cleanup()
        # Ensure app is closed gracefully
        await app.close()

if __name__ == "__main__":
    logging.basicConfig(
        format="%(message)s", datefmt="[%X]", handlers=[RichHandler(highlighter=NullHighlighter())]
    )
    logging.getLogger('blinkbridge').setLevel(CONFIG['log_level'])
    logging.getLogger(__name__).setLevel(CONFIG['log_level'])

    asyncio.run(main())

