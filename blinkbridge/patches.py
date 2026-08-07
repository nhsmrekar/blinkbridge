"""
Runtime patches for bugs found in third-party dependencies during
development of blinkbridge's on-demand liveview feature (2026-08-03).

blinkpy's BlinkLiveStream.recv() reads its 9-byte IMMI protocol header
and payload with `await self.target_reader.read(n)`, then checks
`len(data) < n` and gives up if short. asyncio's StreamReader.read(n)
is documented to return "up to n bytes" as soon as ANY data is
available -- it does NOT block until n bytes have arrived. On a real
network connection, a single TCP segment very often doesn't contain a
full 9-byte header or a full multi-hundred-byte payload in one read, so
this reliably killed every liveview session within a few seconds
("Insufficient data for payload: N bytes, expected M") -- confirmed
against blinkpy 0.25.7 (installed) and 0.25.9 (latest on PyPI as of
this writing) -- both have the identical bug in livestream.py.

Fix: use readexactly(n), which does block until exactly n bytes have
arrived (or the connection ends), matching what this code actually
needs. asyncio.IncompleteReadError (connection closed mid-read) is
treated the same as the original code's "not enough data" case: log
and stop, don't crash the session ungracefully.

This is a monkey-patch, not a fork -- if a future blinkpy release fixes
this upstream, this file becomes a no-op the moment blinkbridge's
Dockerfile pins a version that includes the real fix, and can be
deleted then.
"""
import asyncio
import logging
import ssl

from blinkpy.camera import BlinkLiveStream

log = logging.getLogger(__name__)


async def _patched_recv(self) -> None:
    """Copy data from one reader to multiple writers -- see module docstring for why this replaces blinkpy's own version."""
    try:
        log.debug("Starting copy from target to clients")
        while not self.target_reader.at_eof():
            # Read header from the target server
            try:
                data = await self.target_reader.readexactly(9)
            except asyncio.IncompleteReadError as e:
                log.warning("Connection ended while reading header: got %d of 9 bytes", len(e.partial))
                break

            # Handle the 9-byte IMMI protocol header
            msgtype = data[0]
            sequence = int.from_bytes(data[1:5], byteorder="big")
            payload_length = int.from_bytes(data[5:9], byteorder="big")
            log.debug("Received packet: msgtype=%d, sequence=%d, payload_length=%d", msgtype, sequence, payload_length)

            # Skip packets with invalid payload length
            if payload_length <= 0:
                log.debug("Invalid payload length: %d", payload_length)
                continue

            # Read payload from the target server
            try:
                data = await self.target_reader.readexactly(payload_length)
            except asyncio.IncompleteReadError as e:
                log.warning("Connection ended while reading payload: got %d of %d bytes", len(e.partial), payload_length)
                break

            # Skip packets other than msgtype 0x00 (regular video stream)
            if msgtype != 0x00:
                log.debug("Skipping unsupported msgtype %d", msgtype)
                continue

            # Skip video payloads missing 0x47 (transport stream packet start)
            if data[0] != 0x47:
                log.debug("Skipping video payload missing 0x47 at start")
                continue

            # Send data to all connected clients
            log.debug("Sending %d bytes to clients", len(data))
            for writer in self.clients:
                if not writer.is_closing():
                    writer.write(data)
                    await writer.drain()

            # Yield control to the event loop
            await asyncio.sleep(0)
    except ssl.SSLError as e:
        if e.reason != "APPLICATION_DATA_AFTER_CLOSE_NOTIFY":
            log.exception("SSL error while receiving data")
    except Exception:
        log.exception("Error while receiving data")
    finally:
        # Abort sending by closing the target writer
        self.target_writer.close()
        log.debug("Receiving was aborted, aborting sending")


def apply() -> None:
    BlinkLiveStream.recv = _patched_recv
    log.info("applied readexactly() patch to blinkpy.camera.BlinkLiveStream.recv")
