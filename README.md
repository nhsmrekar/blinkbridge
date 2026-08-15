# blinkbridge

**Notes**: 

* There is an issue related to local storage systems; please see issue [#1](https://github.com/roger-/blinkbridge/issues/1) for a temporary fix until it's resolved. 
* Disable the "Photo Capture" feature in the Blink app for each supported camera to work around an issue where photos can prevent videos from being recognized by BlinkPy.

---

blinkbridge is a tool for creating an RTSP stream from a [Blink camera](https://blinkforhome.com/) using [FFmpeg](https://ffmpeg.org/) and [MediaMTX](https://github.com/bluenviron/mediamtx). Blink cameras are battery operated and don't have native RTSP support, so this tool uses the [BlinkPy](https://github.com/fronzbot/blinkpy) Python library to download clips every time motion is detected and then creates an RTSP stream from them. 

Due to the slow polling rate of BlinkPy, there will be a **delay of up to ~30 seconds** between when a motion is detected and when the RTSP stream updates (can be changed at risk of the Blink server banning you). The RTSP stream will persist the last recorded frame (i.e. a static video) until the next motion is detected.

Once the RTSP streams are available, you can use them in applications such as [Frigate NVR](https://github.com/blakeblackshear/frigate) (e.g. for better person detection) or [Scrypted](https://github.com/koush/scrypted) (e.g. for Homekit Secure Video support).

# How it works

1. blinkbridge downloads the latest clip for each enabled camera from the Blink server
2. FFmpeg extracts the last frame from each clip and creates a short still video (~0.5s) from it 
3. The still video is published on a loop to MediaMTX (using [FFMpeg's concat demuxer](https://trac.ffmpeg.org/wiki/Concatenate#demuxer))
4. When motion is detected, the new clip is downloaded and published
5. A still video from the last frame of the new clip is then published on a loop

# Usage

1. Download `compose.yaml` from this repo and modify accordingly
2. Download `config/config.json`, save to `./config/` and modify accordingly (be sure to enter your Blink login creditials)
3. Run `docker compose run blinkbridge` and enter your Blink verification code when prompted (this only has to be done once and will be saved in `config/.cred.json`). Exit with CTRL+c
4. Run `docker compose up` to start the service. The RTSP URLs will be printed to the console.

To start Blink Live View when motion is reported even when no cloud clip is available, include the camera in both `cameras.enabled` and `cameras.motion_liveview_enabled`. This is intended for event-triggered recording systems such as Frigate; polling delay means it cannot capture footage before the alert.

# TODO

- [ ] Better error handling
- [ ] Cleanup code
- [ ] Support FFmpeg hardware acceleration (e.g. QSV)
- [ ] Process cameras in parallel and reduce latency
- [ ] Add ONVIF server with motion events

# Related projects

* https://github.com/kaffetorsk/arlo-streamer

