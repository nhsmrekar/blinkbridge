import subprocess
import logging
import sys
from typing import Union
from pathlib import Path
from datetime import datetime
from blinkbridge.utils import wait_until_file_open
from blinkbridge.config import *
from blinkbridge.ffmpeg import StillVideoCreator


log = logging.getLogger(__name__)

class StreamServer:
    def __init__(self, stream_name: str):
        self.stream_name = stream_name
        self.stream_name_sanitized = stream_name.replace(' ', '_').lower()
        self.current_still_video = None
        # On-demand real live view (see Application.start_liveview) --
        # while active, this process temporarily takes over the same
        # mediamtx output path from the normal concat-loop publisher
        # below, and the main motion-poll loop must not touch this
        # StreamServer's `process`/concat files until it stops.
        self.live_process = None
        self.is_live = False

    def _output_url(self) -> str:
        return f"{RTSP_URL}/{self.stream_name_sanitized}"

    def _run_server(self) -> str:
        output_url = self._output_url()
        input_concat_file = PATH_CONCAT / f"{self.stream_name_sanitized}.concat"

        ffmpeg_args = [
            'ffmpeg',
            *COMMON_FFMPEG_ARGS,
            '-fflags', '+igndts+genpts',
            '-re',
            '-stream_loop', '-1',
            '-f', 'concat',
            '-safe', '0',
            '-i', input_concat_file.resolve(),
            '-flush_packets', '0',
            '-c:v', 'copy',
            '-c:a', 'copy',
            '-f', 'rtsp',
            # '-avoid_negative_ts', '1',
            # '-use_wallclock_as_timestamps', '1',
            '-fps_mode', 'drop',
            output_url
        ]
        
        self.process = subprocess.Popen(ffmpeg_args, stdout=sys.stdout, stderr=sys.stderr)

        return output_url

    def _make_concat_files(self) -> str:
        log.debug(f"{self.stream_name}: making concat file")

        next_concat = PATH_CONCAT / f"{self.stream_name_sanitized}_next.concat"
        concat_file = PATH_CONCAT / f"{self.stream_name_sanitized}.concat"

        with open(concat_file, 'w') as f:
            f.write("ffconcat version 1.0\n")
            f.write(f"file '{next_concat.resolve()}'\n")
            f.write(f"option safe 0\n") # needed to propogate 'safe 0' to next concat file
            f.write(f"file '{next_concat.resolve()}'\n")
            f.write(f"option safe 0\n") # needed to propogate 'safe 0' to next concat file

        return concat_file

    def _enqueue_clip(self, video_file_name: Union[str, Path]) -> Path:
        log.debug(f"{self.stream_name}: enqueueing {video_file_name}")

        video_file_name = Path(video_file_name)
        next_concat = PATH_CONCAT / f"{self.stream_name_sanitized}_next.concat"

        with open(next_concat, 'w') as f:
            f.write("ffconcat version 1.0\n")
            f.write(f"file '{video_file_name.resolve()}'\n") 

        return next_concat

    def add_video(self, file_name_input_video: Union[str, Path], still_only: bool=False) -> None:
        if not still_only:
            # enqueue fullclip immediately
            self._enqueue_clip(file_name_input_video) 

        # make a timestamped name for the next still video
        dt = datetime.now()
        next_still_video = PATH_VIDEOS / f"{self.stream_name_sanitized}_still_{dt.strftime('%Y-%m-%d_%H-%M-%S-%f')}.mp4"

        # make still video from input video
        log.debug(f"{self.stream_name}: starting creating next still video {next_still_video}")
        svc = StillVideoCreator(file_name_input_video,
                                output_duration=CONFIG['still_video_duration'],
                                file_name_still_video=next_still_video)
        
        # wait for enqueued video to start
        if not still_only:
            log.debug(f"{self.stream_name}: waiting for new video to start")
            wait_until_file_open(file_name_input_video, self.process.pid)
            
        # enqueue next still video
        log.debug(f'{self.stream_name}: waiting for still video creation to finish')
        svc.wait()
        self._enqueue_clip(next_still_video)

        # delete old still video
        if self.current_still_video and not still_only:
            log.debug(f'{self.stream_name}: deleting old still video {self.current_still_video}')
            self.current_still_video.unlink()
        
        self.current_still_video = next_still_video
    
    def is_running(self) -> bool:
        return self.process.poll() is None

    def close(self) -> None:
        if self.is_running():
            log.info(f"{self.stream_name}: stopping server")
            self.process.kill()
        if self.live_process and self.live_process.poll() is None:
            self.live_process.kill()

    def start_server(self, file_name_initial_video: Union[str, Path]) -> None:
        log.debug(f"{self.stream_name}: starting server with {file_name_initial_video}")
        self._make_concat_files()
        self.add_video(file_name_initial_video, still_only=True)
        url = self._run_server()

        log.info(f"{self.stream_name}: stream ready at {url}")

    def start_live_relay(self, tcp_source_url: str) -> None:
        """
        Switch this camera's mediamtx output from the normal motion-clip
        loop to a real, on-demand Blink liveview session. `tcp_source_url`
        is the loopback MPEG-TS server a blinkpy BlinkLiveStream is
        already feeding (see Application.start_liveview) -- ffmpeg just
        relays it through to the same output path Frigate already reads
        from, so nothing downstream (mediamtx config, Frigate, cabin-
        backend) needs to know the difference.
        """
        if self.is_live:
            log.debug(f"{self.stream_name}: live relay already running")
            return

        log.info(f"{self.stream_name}: switching to real live view")
        # Pause the concat-loop publisher -- it owns the same output URL
        # and would otherwise fight the live relay for the RTSP publish.
        if self.is_running():
            self.process.kill()

        output_url = self._output_url()
        ffmpeg_args = [
            'ffmpeg',
            *COMMON_FFMPEG_ARGS,
            '-fflags', '+igndts+genpts',
            '-f', 'mpegts',
            '-i', tcp_source_url,
            '-c:v', 'copy',
            '-an',  # Blink's liveview payload here is video-only (see BlinkLiveStream.recv's msgtype filter) -- no audio track to carry.
            '-f', 'rtsp',
            output_url,
        ]
        self.live_process = subprocess.Popen(ffmpeg_args, stdout=sys.stdout, stderr=sys.stderr)
        self.is_live = True

    def stop_live_relay(self) -> None:
        """Stop the live relay and resume the normal motion-clip loop on the same output path."""
        if self.live_process and self.live_process.poll() is None:
            log.info(f"{self.stream_name}: stopping live relay")
            self.live_process.kill()
        self.live_process = None
        self.is_live = False
        # Resume the concat-loop publisher against the same still/clip
        # files that were already in place before live view started.
        self._run_server()

