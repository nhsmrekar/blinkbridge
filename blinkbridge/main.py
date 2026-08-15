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
LIVEVIEW_RETRY_BASE = timedelta(seconds=5)
LIVEVIEW_RETRY_MAX = timedelta(minutes=1)

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
        # A liveview request outlives an individual Blink cloud session.
        # Dropped sessions are retried only while a viewer still wants live
        # footage, and never beyond LIVEVIEW_MAX_DURATION.
        self.liveview_requests = {}
        self.motion_liveview_cameras = set(
            CONFIG.get("cameras", {}).get("motion_liveview_enabled", [])
        )

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
        motion_detected, file_name_new_clip = await self.cam_manager.check_for_motion(camera_name)

        if not motion_detected:
            return False

        if camera_name in self.motion_liveview_cameras:
            log.info(f"{camera_name}: motion-triggered liveview requested")
            await self.start_liveview(camera_name)

        if file_name_new_clip:
            log.info(f"{ss.stream_name}: motion clip available, adding video")
            if ss.is_running():
                ss.add_video(file_name_new_clip)
            elif camera_name not in self.live_sessions:
                ss.start_server(file_name_new_clip)

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
            self.liveview_requests[camera_name]["requested_at"] = datetime.now()
            return True

        request = self.liveview_requests.setdefault(camera_name, {
            "requested_at": datetime.now(),
            "retry_count": 0,
            "next_retry_at": None,
            "last_error": None,
        })

        try:
            await self._open_liveview_session(camera_name)
            request["retry_count"] = 0
            request["next_retry_at"] = None
            request["last_error"] = None
            return True
        except Exception as exc:
            self._schedule_liveview_retry(camera_name, exc)
            log.warning(f"{camera_name}: liveview start failed; retry scheduled: {exc}")
            return False

    async def _open_liveview_session(self, camera_name: str) -> None:
        """Open one Blink cloud session for an existing viewer request."""

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

    def _schedule_liveview_retry(self, camera_name: str, exc: Exception | None = None) -> None:
        request = self.liveview_requests[camera_name]
        request["retry_count"] += 1
        delay = min(
            LIVEVIEW_RETRY_BASE * (2 ** (request["retry_count"] - 1)),
            LIVEVIEW_RETRY_MAX,
        )
        request["next_retry_at"] = datetime.now() + delay
        request["last_error"] = str(exc) if exc else "Blink ended the live session"

    async def stop_liveview(self, camera_name: str, *, keep_request: bool = False) -> bool:
        """Stop an active liveview session and resume the normal motion-clip loop."""
        had_request = camera_name in self.liveview_requests
        if not keep_request:
            self.liveview_requests.pop(camera_name, None)
        session = self.live_sessions.pop(camera_name, None)
        if session is None:
            log.debug(f"{camera_name}: no active liveview session to stop")
            return had_request

        log.info(f"{camera_name}: stopping liveview session")
        session["live_stream"].stop()
        session["feed_task"].cancel()

        if camera_name in self.stream_servers:
            self.stream_servers[camera_name].stop_live_relay()

        return True

    async def _enforce_liveview_max_duration(self) -> None:
        now = datetime.now()
        expired = [
            name for name, request in self.liveview_requests.items()
            if now > request["requested_at"] + LIVEVIEW_MAX_DURATION
        ]
        for camera_name in expired:
            log.warning(f"{camera_name}: liveview session exceeded max duration, force-stopping")
            await self.stop_liveview(camera_name)

    async def _retry_dropped_liveview_sessions(self) -> None:
        now = datetime.now()
        for camera_name, request in list(self.liveview_requests.items()):
            if camera_name in self.live_sessions:
                continue
            retry_at = request["next_retry_at"]
            if retry_at is None or now < retry_at:
                continue
            try:
                log.info(f"{camera_name}: retrying real liveview session")
                await self._open_liveview_session(camera_name)
                request["next_retry_at"] = None
                request["last_error"] = None
            except Exception as exc:
                self._schedule_liveview_retry(camera_name, exc)
                log.warning(f"{camera_name}: liveview retry failed; backing off: {exc}")

    def liveview_status(self, camera_name: str) -> dict:
        if camera_name not in self.stream_servers:
            return {"camera": camera_name, "mode": "unavailable", "live": False}
        request = self.liveview_requests.get(camera_name)
        if camera_name in self.live_sessions:
            return {"camera": camera_name, "mode": "live", "live": True}
        if request:
            return {
                "camera": camera_name,
                "mode": "reconnecting",
                "live": False,
                "last_error": request["last_error"],
                "next_retry_at": request["next_retry_at"].isoformat() if request["next_retry_at"] else None,
            }
        return {"camera": camera_name, "mode": "clip_loop", "live": False}

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
            await self.stop_liveview(camera_name, keep_request=True)
            self._schedule_liveview_retry(camera_name, exc)

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
            await self._retry_dropped_liveview_sessions()

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

    async def handle_status(request: web.Request) -> web.Response:
        camera_name = request.match_info["camera"]
        status = app.liveview_status(camera_name)
        return web.json_response(status, status=404 if status["mode"] == "unavailable" else 200)

    web_app = web.Application()
    web_app.router.add_post("/liveview/{camera}/start", handle_start)
    web_app.router.add_post("/liveview/{camera}/stop", handle_stop)
    web_app.router.add_get("/liveview/{camera}/status", handle_status)
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

