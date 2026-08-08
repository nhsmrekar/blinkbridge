import sys
import types
import unittest
from unittest.mock import MagicMock, patch


# blinkbridge.config uses `from blinkbridge.config import *`, which
# stream_server.py depends on for RTSP_URL / PATH_CONCAT / PATH_VIDEOS /
# COMMON_FFMPEG_ARGS. Stub the whole package tree rather than requiring
# a real /config/config.json, since this test only exercises
# StreamServer's own control flow.
fake_config = types.ModuleType("blinkbridge.config")
fake_config.RTSP_URL = "rtsp://mediamtx:8554"
fake_config.PATH_CONCAT = MagicMock()
fake_config.PATH_VIDEOS = MagicMock()
fake_config.COMMON_FFMPEG_ARGS = []
fake_config.CONFIG = {"still_video_duration": 1}

fake_utils = types.ModuleType("blinkbridge.utils")
fake_utils.wait_until_file_open = MagicMock()

fake_ffmpeg = types.ModuleType("blinkbridge.ffmpeg")
fake_ffmpeg.StillVideoCreator = MagicMock()

sys.path.insert(0, "/app")

blinkbridge_pkg = types.ModuleType("blinkbridge")
blinkbridge_pkg.__path__ = ["/app/blinkbridge"]  # real package dir -- lets `blinkbridge.stream_server` still resolve from disk
sys.modules["blinkbridge"] = blinkbridge_pkg
sys.modules["blinkbridge.config"] = fake_config
sys.modules["blinkbridge.utils"] = fake_utils
sys.modules["blinkbridge.ffmpeg"] = fake_ffmpeg
from blinkbridge.stream_server import StreamServer  # noqa: E402


class StartServerNoClipTest(unittest.TestCase):
    """
    Regression test for the 2026-08-08 incident: Blink's cloud API had a
    transient outage, save_latest_clip() returned None, and that None
    reached ffmpeg's argv directly -- crashing a background thread with
    `TypeError: expected str, bytes or os.PathLike, not NoneType` instead
    of being treated as "no stream yet, retry later".
    """

    def test_none_clip_does_not_call_add_video_or_spawn_ffmpeg(self):
        server = StreamServer("Outdoor 4 - DHEE")
        with patch.object(server, "_make_concat_files"), \
             patch.object(server, "add_video") as mock_add_video, \
             patch("blinkbridge.stream_server.subprocess.Popen") as mock_popen:
            server.start_server(None)

        mock_add_video.assert_not_called()
        mock_popen.assert_not_called()

    def test_is_running_false_when_no_clip_was_available(self):
        server = StreamServer("Outdoor 4 - DHEE")
        with patch.object(server, "_make_concat_files"):
            server.start_server(None)
        self.assertFalse(server.is_running())

    def test_real_clip_still_starts_the_server_normally(self):
        server = StreamServer("Outdoor 4 - DHEE")
        with patch.object(server, "_make_concat_files"), \
             patch.object(server, "add_video") as mock_add_video, \
             patch.object(server, "_run_server", return_value="rtsp://x") as mock_run:
            server.start_server("/working/some_real_clip.mp4")

        mock_add_video.assert_called_once_with("/working/some_real_clip.mp4", still_only=True)
        mock_run.assert_called_once()


if __name__ == "__main__":
    unittest.main()
