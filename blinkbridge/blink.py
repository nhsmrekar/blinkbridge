import asyncio
from collections import defaultdict
from datetime import datetime, timedelta 
import logging
from typing import Dict, Tuple, Union
from pathlib import Path
from aiohttp import ClientSession
from blinkpy.blinkpy import Blink
from blinkpy.auth import Auth, BlinkTwoFARequiredError, TokenRefreshFailed, LoginError
from blinkpy.helpers.util import json_load
from blinkbridge.config import *


log = logging.getLogger(__name__)


def find_most_recent_clip_url(recent_clips: dict, date: str) -> str:
    # sort data in reverse order by time
    sorted_data = sorted(recent_clips, key=lambda x: x['time'], reverse=True)

    # get the first entry that does not contain "/snapshot/"
    for entry in sorted_data:
        if '/snapshot/' not in entry['clip']:
            break
    else:
        return ''
    
    # convert to datetime
    date = datetime.fromisoformat(date.replace('Z', '+00:00'))
    entry_time = datetime.fromisoformat(entry['time'].replace('Z', '+00:00'))

    # see if entry is newer than date
    if entry_time > date:
        return entry['clip']
    
    return '' 

class CameraManager:
    def __init__(self):
        self.session = ClientSession()
        self.camera_last_record = defaultdict(lambda: None)
        self.camera_motion_active = defaultdict(bool)
        self.camera_last_motion_token = defaultdict(lambda: None)
        self.metadata = None

    async def _login(self) -> None:
        """Login to Blink using OAuth v2 authentication."""
        self.blink = Blink(session=self.session)
        path_cred = PATH_CONFIG / ".cred.json"

        if not path_cred.exists():
            log.info("Logging into Blink with credentials from config")
            # Create Auth with login credentials for initial OAuth v2 flow
            self.blink.auth = Auth(CONFIG['blink']['login'], no_prompt=True, session=self.session)
        else:
            log.info("Logging into Blink with saved credentials")
            # Load saved credentials (includes OAuth tokens, hardware_id, etc.)
            saved_data = await json_load(path_cred)
            self.blink.auth = Auth(saved_data, no_prompt=True, session=self.session)

        try:
            # Start the Blink system (performs OAuth v2 authentication)
            await self.blink.start()
            log.info("Successfully authenticated with Blink")
        except BlinkTwoFARequiredError:
            # Prompt user for 2FA code
            log.info("Two-factor authentication required")
            twofa_code = input("Enter your 2FA code: ")
            
            # Complete 2FA login
            success = await self.blink.send_2fa_code(twofa_code)
            if not success:
                log.error("2FA verification failed")
                raise LoginError("2FA verification failed")
            
            log.info("Successfully authenticated with Blink (2FA completed)")
        except (TokenRefreshFailed, LoginError) as e:
            log.error(f"Authentication failed: {e}")
            # If we have saved creds that failed, remove them so we retry with fresh login
            if path_cred.exists():
                log.info("Removing invalid saved credentials")
                path_cred.unlink()
            raise

        # Save credentials after successful authentication (includes OAuth tokens)
        if not path_cred.exists():
            log.info("Saving Blink credentials for future use")
            await self.blink.save(path_cred)
        else:
            # Update saved credentials with refreshed tokens
            log.debug("Updating saved credentials with refreshed tokens")
            await self.blink.save(path_cred)

    async def refresh_metadata(self) -> None:
        log.debug('refreshing video metadata')
        dt_past = datetime.now() - timedelta(days=CONFIG['blink']['history_days'])
        self.metadata = await self.blink.get_videos_metadata(since=str(dt_past), stop=2)

    async def save_latest_clip(self, camera_name: str, force: bool=False) -> Union[Path, None]:
        '''
        Download and save latest videos for camera
        ''' 
        camera_name_sanitized = camera_name.lower().replace(' ', '_')
        file_name = PATH_VIDEOS / f"{camera_name_sanitized}_latest.mp4"
    
        # don't download if clip already exists
        if file_name.exists() and not force:
            log.debug(f"{camera_name}: skipping download, {file_name} exists")
            return file_name

        # skip deleted clips and camera snapshots
        media = next((m for m in self.metadata if m['device_name'] == camera_name 
                    if not m['deleted'] and m['source'] != 'snapshot'), None)

        if media is None:
            log.warning(f"{camera_name}: no clips found for camera")
            return None

        log.debug(f'{camera_name}: downloading video: {media}')
        response = await self.blink.do_http_get(media['media'])

        log.debug(f'{camera_name}: saving video to {file_name}')
        with open(file_name, 'wb') as f:
            f.write(await response.read())

        return file_name
    
    async def _save_clip(self, camera_name: str, url: str, file_name: Path) -> None:
        camera = self.blink.cameras[camera_name]
        response = await camera.get_video_clip(url)

        log.debug(f'{camera_name}: saving video to {file_name}')
        with open(file_name, 'wb') as f:
            f.write(await response.read())
    
    async def check_for_motion(self, camera_name: str) -> Tuple[bool, Union[Path, None]]:
        """
        Check whether Blink reports a new motion event and, when available,
        download its cloud clip.

        The motion boolean is intentionally independent of clip availability.
        Accounts without cloud recording can still expose motion_detected via
        Blink's camera status even though there is no media URL to download.
        """
        await self.blink.refresh()
        camera = self.blink.cameras[camera_name]
        attributes = camera.attributes
        motion_detected = bool(attributes.get("motion_detected"))
        motion_token = attributes.get("last_record") or attributes.get("video")
        previous_active = self.camera_motion_active[camera_name]
        previous_token = self.camera_last_motion_token[camera_name]

        is_new_motion = motion_detected and (
            not previous_active
            or (motion_token is not None and motion_token != previous_token)
        )

        self.camera_motion_active[camera_name] = motion_detected
        if motion_detected and motion_token is not None:
            self.camera_last_motion_token[camera_name] = motion_token

        if not is_new_motion:
            return False, None

        log.info(f"{camera_name}: new Blink motion event detected")

        camera_name_sanitized = camera_name.lower().replace(" ", "_")
        file_name = PATH_VIDEOS / f"{camera_name_sanitized}_latest.mp4"

        video_url = attributes.get("video") or ""
        if "/snapshot/" in video_url:
            last_record = attributes.get("last_record")
            if last_record and (url := find_most_recent_clip_url(
                attributes.get("recent_clips", []),
                last_record,
            )):
                log.debug(f"{camera_name}: found recent clip in snapshot, saving to {file_name}")
                await self._save_clip(camera_name, url, file_name)
                self.camera_last_record[camera_name] = attributes.get("last_record")
                return True, file_name

            log.info(f"{camera_name}: motion event has no downloadable cloud clip")
            self.camera_last_record[camera_name] = attributes.get("last_record")
            return True, None

        if not video_url:
            log.info(f"{camera_name}: motion event has no cloud media URL")
            self.camera_last_record[camera_name] = attributes.get("last_record")
            return True, None

        log.debug(f"{camera_name}: saving video to {file_name}")
        await camera.video_to_file(file_name)
        self.camera_last_record[camera_name] = attributes.get("last_record")
        return True, file_name

    def get_cameras(self) -> iter:
        return self.blink.cameras.keys()
    
    async def start(self) -> None:
        await self._login()
        await self.refresh_metadata()
    
    async def close(self) -> None:
        """Properly close all connections and clean up resources."""
        # Close the session only once (blink.auth.session is the same as self.session)
        if hasattr(self, 'session') and self.session is not None:
            if not self.session.closed:
                await self.session.close()
                # Give the event loop time to clean up SSL transports
                # This prevents "Unclosed connector" warnings
                await asyncio.sleep(0.25)

async def test() -> None:
    cm = CameraManager()

    await cm.start()

    for camera in cm.get_cameras():
        file_name = await cm.check_for_motion(camera)

        print(file_name)

    await cm.close()

if __name__ == "__main__":
    asyncio.run(test())
