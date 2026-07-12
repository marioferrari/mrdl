"""Worker pool for concurrent chunk downloading across multiple sources."""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from collections.abc import Callable
from typing import TYPE_CHECKING

from mrdl.exceptions import FetchError, StoppedException

if TYPE_CHECKING:
    from mrdl.protocols import FetchesChunks, ReportsProgress, TracksHealth

_STOP_SENTINEL = -1  # Sentinel value for PriorityQueue; chunk indices are always >= 0


class WorkerPool:
    """Manages concurrent download workers across multiple sources.

    Spawns one async worker task per (source, thread_index) pair. Each worker
    pulls chunk indices from a shared priority queue, delegates to a fetcher,
    and handles retries with exponential backoff.
    """

    def __init__(
        self,
        sources: list[str],
        threads_per_source: int,
        chunk_queue: asyncio.PriorityQueue,
        fetcher_factory: Callable[[str, int], FetchesChunks],
        health: TracksHealth,
        completed_set: set[int],
        state_lock: threading.Lock,
        stop_event: asyncio.Event,
        pause_event: asyncio.Event,
        stop_event_thread: threading.Event,
        progress: ReportsProgress,
    ) -> None:
        """Initializes the WorkerPool.

        Args:
            sources: List of source identifiers (e.g. mirror URLs).
            threads_per_source: Number of concurrent workers per source.
            chunk_queue: Shared priority queue of (retry_time, chunk_idx, retries) tuples.
            fetcher_factory: Callable that creates a FetchesChunks instance
                given (source_id, worker_idx).
            health: Health tracker for banning slow/failed sources.
            completed_set: Shared mutable set of completed chunk indices.
            state_lock: Lock protecting shared mutable state.
            stop_event: Async event signaling all workers to stop.
            pause_event: Async event that blocks workers when cleared (paused).
            stop_event_thread: Thread-level stop event for cross-thread signaling.
            progress: Progress reporter for logging.
        """
        self._sources = sources
        self._threads_per_source = threads_per_source
        self._chunk_queue = chunk_queue
        self._fetcher_factory = fetcher_factory
        self._health = health
        self._completed_set = completed_set
        self._state_lock = state_lock
        self._stop_event = stop_event
        self._pause_event = pause_event
        self._stop_event_thread = stop_event_thread
        self._progress = progress
        self._last_error: str | None = None

    @property
    def last_error(self) -> str | None:
        """Returns the last fatal error message, if any."""
        return self._last_error

    async def run(self) -> bool:
        """Runs all workers until all chunks are complete or a stop is signaled.

        Returns:
            True if all chunks were successfully downloaded, False otherwise.
        """
        tasks = []
        worker_idx = 0
        for source in self._sources:
            for _ in range(self._threads_per_source):
                t = asyncio.create_task(self._worker(source, worker_idx))
                tasks.append(t)
                worker_idx += 1

        try:
            wait_task = asyncio.create_task(self._chunk_queue.join())
            stop_task = asyncio.create_task(self._stop_event.wait())
            done, pending = await asyncio.wait(
                [wait_task, stop_task],
                return_when=asyncio.FIRST_COMPLETED,
            )
            for t in pending:
                t.cancel()

            is_complete = (wait_task in done) and not self._stop_event.is_set()
        except asyncio.CancelledError:
            self._stop_event.set()
            raise
        finally:
            # Tell workers to stop
            for _ in tasks:
                self._chunk_queue.put_nowait((0.0, _STOP_SENTINEL, 0))

            try:
                # Wait for them to finish, but not forever
                done, pending = await asyncio.wait(tasks, timeout=2.0)
                for t in pending:
                    t.cancel()
            except asyncio.CancelledError:
                for t in tasks:
                    if not t.done():
                        t.cancel()
                raise

        return is_complete

    async def _worker(self, source: str, worker_idx: int) -> None:
        """Worker loop: pulls chunks from the queue and delegates to a fetcher.

        Args:
            source: Source identifier (e.g. mirror URL).
            worker_idx: Global index of this worker (used for staggered startup).
        """
        await asyncio.sleep(min(0.02 * worker_idx, 0.3))

        fetcher = self._fetcher_factory(source, worker_idx)

        while not self._stop_event.is_set():
            await self._pause_event.wait()

            try:
                queue_item = await asyncio.wait_for(self._chunk_queue.get(), timeout=1.0)
                next_retry_time, chunk_idx, retries = queue_item
            except asyncio.TimeoutError:
                continue

            try:
                if chunk_idx == _STOP_SENTINEL:
                    break

                now = time.monotonic()
                if now < next_retry_time:
                    await self._chunk_queue.put((next_retry_time, chunk_idx, retries))
                    await asyncio.sleep(min(0.5, next_retry_time - now))
                    continue

                if self._health.is_banned(source):
                    all_banned = all(self._health.is_banned(s) for s in self._sources)
                    if all_banned:
                        with self._state_lock:
                            self._last_error = "All mirrors are currently banned due to failures or slow speeds."
                        self._stop_event.set()
                        self._stop_event_thread.set()
                        break

                    await self._chunk_queue.put((next_retry_time, chunk_idx, retries))
                    await asyncio.sleep(2.0)
                    continue

                await self._process_chunk(fetcher, chunk_idx, retries, source)
            finally:
                self._chunk_queue.task_done()

    async def _process_chunk(
        self,
        fetcher: FetchesChunks,
        chunk_idx: int,
        retries: int,
        source: str,
    ) -> None:
        """Processes a single chunk download with error handling.

        Args:
            fetcher: The fetcher instance to use.
            chunk_idx: Index of the chunk to download.
            retries: Number of previous retry attempts for this chunk.
            source: Source identifier for health tracking.
        """
        try:
            await fetcher.fetch(chunk_idx)
            with self._state_lock:
                if chunk_idx not in self._completed_set:
                    self._completed_set.add(chunk_idx)
        except StoppedException:
            return
        except FetchError as e:
            logging.warning(f"SOURCE {source} FAILED WITH FetchError: {e}")
            cause = e.__cause__ if isinstance(e.__cause__, Exception) else e
            self._health.record_failure(cause, source)
            await self._handle_chunk_failure(e, chunk_idx, retries)
        except Exception as e:
            logging.exception("Bug encountered in worker:")
            self._health.record_failure(e, source)
            await self._handle_chunk_failure(e, chunk_idx, retries)

    async def _handle_chunk_failure(
        self,
        e: Exception,
        chunk_idx: int,
        retries: int,
    ) -> None:
        """Handles retrying or aborting on chunk failure.

        Args:
            e: The exception that caused the failure.
            chunk_idx: Index of the failed chunk.
            retries: Number of previous retry attempts.
        """
        if not self._stop_event.is_set():
            if retries < 5:
                backoff = min(30.0, 2.0 ** retries)
                await self._chunk_queue.put((time.monotonic() + backoff, chunk_idx, retries + 1))
            else:
                with self._state_lock:
                    self._last_error = (
                        f"Fatal error on chunk {chunk_idx} after 5 retries. "
                        f"Last error: {type(e).__name__}: {e}"
                    )
                self._progress.log(
                    f"[!] FATAL: Chunk {chunk_idx} failed after 5 retries. Aborting download."
                )
                self._progress.set_overlay(" FATAL ERROR ", color="red")
                self._stop_event.set()
                self._stop_event_thread.set()
