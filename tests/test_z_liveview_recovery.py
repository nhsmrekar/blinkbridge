import asyncio
import sys
import types
import unittest
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch


fake_config = types.ModuleType("blinkbridge.config")
fake_config.CONFIG = {"cameras": {"max_failures": 3}, "blink": {"poll_interval": 1}}
fake_config.DELAY_RESTART = timedelta(seconds=1)

fake_blink = types.ModuleType("blinkbridge.blink")
fake_blink.CameraManager = MagicMock()
fake_patches = types.ModuleType("blinkbridge.patches")
fake_patches.apply = MagicMock()

fake_aiohttp = types.ModuleType("aiohttp")
fake_web = types.SimpleNamespace(Application=MagicMock, Request=MagicMock, Response=MagicMock)
fake_aiohttp.web = fake_web
fake_rich = types.ModuleType("rich")
fake_rich_logging = types.ModuleType("rich.logging")
fake_rich_logging.RichHandler = MagicMock
fake_rich_highlighter = types.ModuleType("rich.highlighter")
fake_rich_highlighter.NullHighlighter = MagicMock
fake_rich_highlighter.JSONHighlighter = MagicMock

sys.modules["blinkbridge.config"] = fake_config
sys.modules["blinkbridge.blink"] = fake_blink
sys.modules["blinkbridge.patches"] = fake_patches
sys.modules["aiohttp"] = fake_aiohttp
sys.modules["rich"] = fake_rich
sys.modules["rich.logging"] = fake_rich_logging
sys.modules["rich.highlighter"] = fake_rich_highlighter

from blinkbridge.main import Application, LIVEVIEW_MAX_DURATION  # noqa: E402


class LiveviewRecoveryTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.app = Application()
        self.app.stream_servers["driveway"] = MagicMock()
        self.app.liveview_requests["driveway"] = {
            "requested_at": datetime.now(),
            "retry_count": 0,
            "next_retry_at": None,
            "last_error": None,
        }

    async def test_dropped_session_falls_back_and_schedules_retry(self):
        task = asyncio.create_task(asyncio.sleep(0))
        await task
        self.app.live_sessions["driveway"] = {
            "live_stream": MagicMock(), "feed_task": task, "started_at": datetime.now()
        }

        await self.app._reap_dead_liveview_sessions()

        self.assertNotIn("driveway", self.app.live_sessions)
        request = self.app.liveview_requests["driveway"]
        self.assertEqual(1, request["retry_count"])
        self.assertIsNotNone(request["next_retry_at"])
        self.app.stream_servers["driveway"].stop_live_relay.assert_called_once()
        self.assertEqual("reconnecting", self.app.liveview_status("driveway")["mode"])

    async def test_due_retry_reopens_session_and_reports_live(self):
        self.app.liveview_requests["driveway"]["next_retry_at"] = datetime.now() - timedelta(seconds=1)

        async def reopened(camera_name):
            self.app.live_sessions[camera_name] = {"feed_task": MagicMock()}

        with patch.object(self.app, "_open_liveview_session", AsyncMock(side_effect=reopened)) as reopen:
            await self.app._retry_dropped_liveview_sessions()

        reopen.assert_awaited_once_with("driveway")
        self.assertEqual("live", self.app.liveview_status("driveway")["mode"])

    async def test_stop_cancels_pending_retry(self):
        self.app.liveview_requests["driveway"]["next_retry_at"] = datetime.now()
        self.assertTrue(await self.app.stop_liveview("driveway"))
        self.assertEqual("clip_loop", self.app.liveview_status("driveway")["mode"])

    async def test_max_duration_stops_retrying_request(self):
        self.app.liveview_requests["driveway"]["requested_at"] = datetime.now() - LIVEVIEW_MAX_DURATION - timedelta(seconds=1)
        await self.app._enforce_liveview_max_duration()
        self.assertNotIn("driveway", self.app.liveview_requests)

    async def test_repeated_motion_extends_active_liveview_request(self):
        self.app.live_sessions["driveway"] = {"feed_task": MagicMock()}
        original = datetime.now() - timedelta(minutes=4)
        self.app.liveview_requests["driveway"]["requested_at"] = original

        self.assertTrue(await self.app.start_liveview("driveway"))

        self.assertGreater(
            self.app.liveview_requests["driveway"]["requested_at"], original
        )


class MotionTriggeredLiveviewTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.app = Application()
        self.app.motion_liveview_cameras = {"AldrichFront"}
        self.app.stream_servers["AldrichFront"] = MagicMock()
        self.app.cam_manager = MagicMock()

    async def test_motion_without_cloud_clip_starts_liveview(self):
        self.app.cam_manager.check_for_motion = AsyncMock(return_value=(True, None))

        with patch.object(self.app, "start_liveview", AsyncMock(return_value=True)) as start:
            self.assertTrue(await self.app.check_for_motion("AldrichFront"))

        start.assert_awaited_once_with("AldrichFront")
        self.app.stream_servers["AldrichFront"].add_video.assert_not_called()

    async def test_no_new_motion_does_not_start_liveview(self):
        self.app.cam_manager.check_for_motion = AsyncMock(return_value=(False, None))

        with patch.object(self.app, "start_liveview", AsyncMock()) as start:
            self.assertFalse(await self.app.check_for_motion("AldrichFront"))

        start.assert_not_awaited()

    def test_idle_motion_only_camera_reports_awaiting_motion_not_clip_loop(self):
        self.app.stream_servers["AldrichFront"].current_still_video = None

        status = self.app.liveview_status("AldrichFront")

        self.assertEqual("idle_awaiting_motion", status["mode"])
        self.assertFalse(status["live"])

    def test_motion_only_camera_with_a_clip_still_reports_clip_loop(self):
        self.app.stream_servers["AldrichFront"].current_still_video = "some_clip.mp4"

        status = self.app.liveview_status("AldrichFront")

        self.assertEqual("clip_loop", status["mode"])


if __name__ == "__main__":
    unittest.main()
