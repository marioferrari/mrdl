"""Single-chunk HTTP downloader with throttling and speed enforcement."""

from __future__ import annotations

import time
import asyncio
from typing import TYPE_CHECKING

import aiohttp

from mrdl.exceptions import FetchError, IncompleteChunkError, StoppedException
from mrdl.types import FileMetadata, SlowMirrorException

if TYPE_CHECKING:
    from mrdl.protocols import ConsumesTokens, ReportsProgress, WritesChunks

_FLUSH_THRESHOLD = 16 * 1024 * 1024  # 16 MB — halves write calls vs. the old 8 MB threshold


from dataclasses import dataclass

@dataclass
class FetcherConfig:
    """Configuration for chunk downloading and speed enforcement."""
    chunk_size: int
    min_speed_kbps: float
    speed_grace_period: float
    per_thread_throttle: ConsumesTokens | None = None
    global_throttle: ConsumesTokens | None = None


class ChunkFetcher:
    """Downloads individual file chunks from a mirror and writes them to disk.

    One instance is created per worker task. Encapsulates the HTTP session,
    buffer management, throttling, and speed enforcement for a single mirror.
    """

    def __init__(
        self,
        session: aiohttp.ClientSession,
        mirror_url: str,
        metadata: FileMetadata,
        writer: WritesChunks,
        progress: ReportsProgress,
        stop_event: asyncio.Event,
        config: FetcherConfig,
    ) -> None:
        """Initializes the ChunkFetcher.

        Args:
            session: Configured aiohttp ClientSession for this worker task.
            mirror_url: URL of the mirror to download from.
            metadata: File metadata (size, range support, etc.).
            writer: Disk writer component for writing downloaded data.
            progress: Progress reporter to notify on chunk completion.
            stop_event: Event signaling all workers to stop.
            config: Configuration for this fetcher.
        """
        self._session = session
        self._mirror_url = mirror_url
        self._metadata = metadata
        self._writer = writer
        self._progress = progress
        self._stop_event = stop_event
        self._config = config
        # Pre-allocate a reusable write buffer once per worker instance.
        # Avoids per-flush heap allocation; reused across every chunk this worker downloads.
        self._buffer = bytearray(config.chunk_size)
        self._buffer_view = memoryview(self._buffer)
        # Build the request timeout once instead of constructing a new object on every chunk fetch.
        self._request_timeout = aiohttp.ClientTimeout(sock_read=15, sock_connect=5)

    @property
    def metadata(self) -> FileMetadata:
        """Current file metadata, which may be updated after the first chunk of an unknown-size file."""
        return self._metadata

    async def fetch(self, chunk_idx: int) -> int:
        """Downloads a single chunk, writes it to disk, and notifies progress.

        Args:
            chunk_idx: Index of the chunk to download.

        Returns:
            Number of bytes written.

        Raises:
            StoppedException: If the stop event fires during the download.
            FetchError: If the download fails for any transport-specific reason
                (wraps IncompleteChunkError, SlowMirrorException, aiohttp errors, etc.).
        """
        start = chunk_idx * self._config.chunk_size
        if self._metadata.total_size > 0:
            end = min(start + self._config.chunk_size - 1, self._metadata.total_size - 1)
            expected_bytes: int | None = end - start + 1
        else:
            end = None
            expected_bytes = None

        bytes_written = 0
        throttle_wait_time = 0.0
        chunk_start_time = time.monotonic()
        write_pos = 0  # cursor into self._buffer; reset to 0 on each flush

        headers = {"Range": f"bytes={start}-{end}"} if self._metadata.accepts_ranges else {}

        per_throttle = self._config.per_thread_throttle
        global_throttle = self._config.global_throttle
        speed_grace_period = self._config.speed_grace_period
        min_speed_kbps = self._config.min_speed_kbps

        try:
            async with self._session.get(self._mirror_url, headers=headers, timeout=self._request_timeout) as response:
                response.raise_for_status()

                # For initially unknown file sizes, try to resolve the total from the response.
                if self._metadata.total_size <= 0:
                    content_len = int(response.headers.get("Content-Length", 0))
                    if content_len > 0:
                        self._metadata = FileMetadata(
                            total_size=content_len,
                            accepts_ranges=self._metadata.accepts_ranges,
                            etag=self._metadata.etag or response.headers.get("ETag"),
                            last_modified=self._metadata.last_modified or response.headers.get("Last-Modified"),
                        )
                        end = min(start + self._config.chunk_size - 1, self._metadata.total_size - 1)
                        expected_bytes = end - start + 1

                # iter_any() yields whatever the OS receive buffer holds — typically a full TCP
                # window (16-128 KB) rather than one MSS segment (~1.4 KB) like iter_chunks().
                # This reduces Python-loop iterations per thread by ~10-50x at high bandwidth.
                async for chunk_data in response.content.iter_any():
                    if self._stop_event.is_set():
                        raise StoppedException()
                    if not chunk_data:
                        continue

                    chunk_len = len(chunk_data)

                    if per_throttle is not None or global_throttle is not None:
                        t0 = time.monotonic()
                        if per_throttle is not None:
                            await per_throttle.consume(chunk_len)
                        if global_throttle is not None:
                            await global_throttle.consume(chunk_len)
                        throttle_wait_time += time.monotonic() - t0

                    # Clamp to the remaining expected bytes for this chunk.
                    if expected_bytes is not None:
                        remaining = expected_bytes - (bytes_written + write_pos)
                        if chunk_len > remaining:
                            chunk_data = chunk_data[:remaining]
                            chunk_len = remaining

                    # Write into the pre-allocated buffer (one copy from network into our buffer).
                    self._buffer[write_pos:write_pos + chunk_len] = chunk_data
                    write_pos += chunk_len

                    if write_pos >= _FLUSH_THRESHOLD:
                        flush_size = write_pos
                        # memoryview slice avoids a second copy into the mmap.
                        await self._writer.write(start + bytes_written, self._buffer_view[:flush_size])
                        bytes_written += flush_size
                        write_pos = 0
                        self._progress.update(flush_size)

                        elapsed = time.monotonic() - chunk_start_time
                        network_elapsed = elapsed - throttle_wait_time
                        if network_elapsed > speed_grace_period:
                            speed_kbps = (bytes_written / 1024) / network_elapsed
                            if speed_kbps < min_speed_kbps:
                                raise SlowMirrorException(f"Speed dropped to {speed_kbps:.1f} KB/s")

                    if expected_bytes is not None and bytes_written + write_pos >= expected_bytes:
                        break

                # Flush any remaining bytes that did not fill a complete threshold window.
                if write_pos > 0:
                    flush_size = write_pos
                    await self._writer.write(start + bytes_written, self._buffer_view[:flush_size])
                    bytes_written += flush_size
                    self._progress.update(flush_size)

                    elapsed = time.monotonic() - chunk_start_time
                    network_elapsed = elapsed - throttle_wait_time
                    if network_elapsed > speed_grace_period:
                        speed_kbps = (bytes_written / 1024) / network_elapsed
                        if speed_kbps < min_speed_kbps:
                            raise SlowMirrorException(f"Speed dropped to {speed_kbps:.1f} KB/s")
        except StoppedException:
            if bytes_written > 0:
                self._progress.update(-bytes_written)
            raise
        except Exception as exc:
            if bytes_written > 0:
                self._progress.update(-bytes_written)
            raise FetchError(str(exc)) from exc

        if expected_bytes is not None and bytes_written != expected_bytes:
            raise FetchError(
                f"Chunk {chunk_idx}: expected {expected_bytes} bytes, got {bytes_written}."
            ) from IncompleteChunkError(
                f"Chunk {chunk_idx}: expected {expected_bytes} bytes, got {bytes_written}."
            )

        if self._metadata.total_size <= 0:
            if hasattr(self._writer, "truncate"):
                await self._writer.truncate(bytes_written)

        await self._writer.mark_complete(chunk_idx)
        self._progress.update(0, chunk_idx)
        return bytes_written
