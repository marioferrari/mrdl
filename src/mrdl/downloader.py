from __future__ import annotations

import logging
import os
import asyncio
import random
import threading
import time
import sys
from typing import Any

from mrdl.exceptions import IncompleteChunkError, StoppedException, IncompleteHashError
from mrdl.fetcher import ChunkFetcher, FetcherConfig
from mrdl.hasher import StreamingHasher
from mrdl.mirror_health import MirrorHealthTracker
from mrdl.prober import MirrorProber
from mrdl.progress import BuiltinProgress, NoOpProgress
from mrdl.protocols import ConsumesTokens, PersistsState, ReportsProgress, VerifiesIntegrity, WritesChunks
from mrdl.state import JsonStateManager
from mrdl.throttle import TokenBucketThrottle
from mrdl.types import (
    VALID_TRANSITIONS,
    DestinationExistsError,
    DownloadConfig,
    DownloadResult,
    DownloadState,
    FileMetadata,
    HashSpec,
    InvalidStateTransition,
    SlowMirrorException
)
from mrdl.mmap_writer import MmapDiskWriter
from mrdl.writer import DiskWriter
import aiohttp

FALLBACK_UNKNOWN_SIZE_CHUNK = 1024 ** 3
_STOP_SENTINEL = -1  # Sentinel value for PriorityQueue; chunk indices are always >= 0


class Downloader:
    """A resilient, concurrent, multi-mirror file downloader.

    Coordinates segment downloads from multiple mirror servers concurrently
    while supporting state persistence, rate-limiting, and checksum verification.
    """

    def __init__(
        self,
        config: DownloadConfig,
        *,
        writer: WritesChunks | None = None,
        hasher: VerifiesIntegrity | None = None,
        state_manager: PersistsState | None = None,
        progress: ReportsProgress | None = None,
        global_throttle: ConsumesTokens | None = None,
    ):
        """Initializes the downloader configuration and dependencies."""
        self._hash_spec: HashSpec | None = HashSpec.parse(config.checksum) if config.checksum else None

        if config.max_speed_kbps is not None and config.max_speed_kbps <= 0:
            raise ValueError(f"max_speed_kbps must be a positive integer, got {config.max_speed_kbps}")
        if config.max_speed_per_thread_kbps is not None and config.max_speed_per_thread_kbps <= 0:
            raise ValueError(
                f"max_speed_per_thread_kbps must be a positive integer, got {config.max_speed_per_thread_kbps}"
            )

        self._urls = [config.urls] if isinstance(config.urls, str) else list(config.urls)
        self._filename = config.filename
        self._label = config.label or config.filename
        self._threads_per_mirror = config.threads_per_mirror
        self._chunk_size = config.chunk_size
        self._min_speed_kbps = config.min_speed_kbps
        self._speed_grace_period = config.speed_grace_period
        self._max_speed_per_thread_kbps = config.max_speed_per_thread_kbps
        self._overwrite = config.overwrite
        self._safe_state_saves = config.safe_state_saves
        self._use_mmap = config.use_mmap

        self._global_throttle: ConsumesTokens | None = global_throttle or TokenBucketThrottle(config.max_speed_kbps)

        self._state_manager = state_manager
        self._writer = writer
        self._hasher = hasher
        if config.silent:
            self._progress: ReportsProgress = NoOpProgress()
        else:
            self._progress = progress or BuiltinProgress()

        if self._use_mmap and sys.platform == 'darwin':
            self._progress.log("WARNING: --use-mmap is known to cause silent data corruption on macOS APFS. DiskWriter is highly recommended instead.")

        self._is_throttled = config.max_speed_kbps is not None
        if global_throttle is not None:
            if hasattr(global_throttle, "is_active"):
                self._is_throttled = global_throttle.is_active
            else:
                self._is_throttled = True

        self._health = MirrorHealthTracker()

        self._state = DownloadState.IDLE
        self._state_lock = threading.Lock()
        self._last_error: str | None = None
        
        # Shared with threading components (writer, hasher)
        self._stop_event_thread = threading.Event()
        self._chunk_condition = threading.Condition()
        
        # Async primitives (initialized in start)
        self._stop_event: asyncio.Event | None = None
        self._pause_event: asyncio.Event | None = None
        self._loop: asyncio.AbstractEventLoop | None = None

        self._metadata: FileMetadata | None = None
        self._download_state: dict[str, Any] = {}
        self._completed_set: set[int] = set()
        self._fd: int | None = None
        self._last_error: str | None = None
        
        self._session: aiohttp.ClientSession | None = None

    @property
    def state(self) -> DownloadState:
        """Returns the current state of the download."""
        return self._state

    def set_speed_limit(self, global_kbps: int | None = None) -> None:
        """Updates the global download speed limit dynamically."""
        if self._global_throttle is not None and hasattr(self._global_throttle, "update_rate"):
            self._global_throttle.update_rate(global_kbps) # type: ignore
            self._is_throttled = global_kbps is not None
            self._progress.set_throttled(self._is_throttled)
        else:
            self._progress.log("Warning: The current global throttle does not support live rate updates.")

    @property
    def computed_hash(self) -> str | None:
        """Returns the hex digest computed after the last download start."""
        if self._hasher is None:
            return None
        return self._hasher.computed_hash

    async def start(self) -> DownloadResult:
        """Starts or resumes the download process asynchronously.

        Returns:
            A DownloadResult containing the outcome of the download.
        """
        start_time = time.monotonic()
        self._loop = asyncio.get_running_loop()
        self._stop_event = asyncio.Event()
        self._pause_event = asyncio.Event()
        self._pause_event.set()
        self._stop_event_thread.clear()
        
        if self._state_manager is None:
            self._state_manager = JsonStateManager(f"{self._filename}.progress", safe_saves=self._safe_state_saves)
            
        if os.path.exists(self._filename):
            saved_state = await asyncio.to_thread(self._state_manager.load)
            if self._overwrite:
                try:
                    os.remove(self._filename)
                except OSError:
                    pass
                await asyncio.to_thread(self._state_manager.clear)
            elif saved_state is None:
                raise DestinationExistsError(
                    f"Destination file '{self._filename}' already exists and no resume state was found. "
                    "Use overwrite=True in DownloadConfig to replace it."
                )

        self._transition_to(DownloadState.PROBING)
        try:
            self._metadata = await MirrorProber().probe(self._urls)
        except (FileNotFoundError, ValueError) as e:
            self._progress.log(f"[!] Error: {e}")
            self._transition_to(DownloadState.FAILED)
            self._last_error = str(e)
            return DownloadResult(
                status=self._state,
                path=self._filename,
                hash_matched=False,
                time_taken=time.monotonic() - start_time,
                error=self._last_error,
                computed_hash=None,
            )

        if self._metadata.total_size == 0 or not self._metadata.accepts_ranges:
            self._progress.log("Warning: Could not fetch file size or mirrors do not support Range requests.")
            if not self._metadata.accepts_ranges:
                self._progress.log("Falling back to single-task sequential download.")
                self._threads_per_mirror = 1
                self._urls = [self._urls[0]]
                if self._metadata.total_size > 0:
                    self._chunk_size = self._metadata.total_size
                else:
                    self._chunk_size = FALLBACK_UNKNOWN_SIZE_CHUNK

        self._transition_to(DownloadState.DOWNLOADING)
        await asyncio.to_thread(self._prepare_file)
        await asyncio.to_thread(self._load_resume_state)

        remaining_chunks = self._build_chunk_queue()
        self._init_components()

        if self._writer is None:
            raise RuntimeError("Writer not initialized after _init_components. This is a bug.")
        if self._hasher is None:
            raise RuntimeError("Hasher not initialized after _init_components. This is a bug.")
        if self._metadata is None:
            raise RuntimeError("Metadata missing after probe. This is a bug.")

        self._writer.start()
        self._hasher.start()
        self._progress.start(
            self._metadata.total_size,
            self._label,
            self._chunk_size,
            self._completed_set,
        )
        self._progress.set_throttled(self._is_throttled)

        state_task = asyncio.create_task(self._run_state_saver())
        success = False
        paused = False
        
        conn = aiohttp.TCPConnector(limit=0)  # Uncapped connection pool, let downloader manage tasks
        self._session = aiohttp.ClientSession(connector=conn, read_bufsize=1024 * 1024)
        
        try:
            try:
                success = await self._download_chunks(remaining_chunks)
            except asyncio.CancelledError:
                paused = True
                if self._state == DownloadState.DOWNLOADING:
                    self._transition_to(DownloadState.PAUSED)
                self._stop_event.set()
                self._stop_event_thread.set()
                raise
            finally:
                if not success:
                    self._stop_event.set()
                    self._stop_event_thread.set()
                
                self._writer.stop()
                self._hasher.stop()
                
                if self._writer.error is not None:
                    self._last_error = f"Disk write error: {self._writer.error}"
                    self._progress.set_overlay(" FATAL ERROR ", color="red")
                    self._progress.log(f"[!] {self._last_error}")
                    if self._state in (DownloadState.DOWNLOADING, DownloadState.PAUSED):
                        self._transition_to(DownloadState.FAILED)
                elif paused or self._state == DownloadState.PAUSED:
                    self._progress.set_overlay(" SAVING STATE ", color="blue")
                
                self._save_state()

                self._stop_event.set()
                self._stop_event_thread.set()
                state_task.cancel()
                
                try:
                    if self._session is not None:
                        await self._session.close()
                except asyncio.CancelledError:
                    pass

                if self._fd is not None:
                    os.close(self._fd)
                    self._fd = None

            if success:
                try:
                    verified = await asyncio.to_thread(self._hasher.finalize)
                except IncompleteHashError as e:
                    self._transition_to(DownloadState.FAILED)
                    self._progress.set_overlay(" HASH FAILED ", success=False)
                    self._last_error = f"Hash verification failed: {e}"
                    return DownloadResult(
                        status=self._state,
                        path=self._filename,
                        hash_matched=False,
                        time_taken=time.monotonic() - start_time,
                        error=self._last_error,
                        computed_hash=self.computed_hash,
                    )

                if verified:
                    if self._state_manager is None:
                        raise RuntimeError("State manager missing at completion. This is a bug.")
                    await asyncio.to_thread(self._state_manager.clear)
                    self._transition_to(DownloadState.COMPLETED)
                    if self._hasher.has_work:
                        self._progress.set_overlay(" HASH OK ", success=True)
                    else:
                        self._progress.set_overlay(" COMPLETED ", success=True)
                else:
                    self._transition_to(DownloadState.FAILED)
                    self._progress.set_overlay(" HASH FAILED ", success=False)
                    self._last_error = "Hash verification failed."
                return DownloadResult(
                    status=self._state,
                    path=self._filename,
                    hash_matched=verified,
                    time_taken=time.monotonic() - start_time,
                    error=self._last_error,
                    computed_hash=self.computed_hash,
                )

            if self._state == DownloadState.DOWNLOADING and self._last_error:
                self._transition_to(DownloadState.FAILED)

            return DownloadResult(
                status=self._state,
                path=self._filename,
                hash_matched=False,
                time_taken=time.monotonic() - start_time,
                error=self._last_error,
                computed_hash=self.computed_hash,
            )
        finally:
            self._progress.close()

    def pause(self) -> None:
        """Pauses the active download, stopping workers and saving progress."""
        if self._state == DownloadState.DOWNLOADING:
            if self._pause_event and self._loop:
                self._loop.call_soon_threadsafe(self._pause_event.clear)
            self._transition_to(DownloadState.PAUSED)
            self._progress.set_overlay(" PAUSED ", color="yellow")
            if self._state_manager is not None:
                self._save_state()

    def resume(self) -> None:
        """Resumes a paused download, restarting workers."""
        if self._state == DownloadState.PAUSED:
            self._transition_to(DownloadState.DOWNLOADING)
            if self._pause_event and self._loop:
                self._loop.call_soon_threadsafe(self._pause_event.set)
            self._progress.set_overlay("")

    def stop(self) -> None:
        """Gracefully stops the active download and saves progress."""
        if self._state in (DownloadState.DOWNLOADING, DownloadState.PAUSED):
            if self._state != DownloadState.PAUSED:
                self._transition_to(DownloadState.PAUSED)
            if self._stop_event and self._loop:
                self._loop.call_soon_threadsafe(self._stop_event.set)
            if self._pause_event and self._loop:
                self._loop.call_soon_threadsafe(self._pause_event.set)
            self._stop_event_thread.set()

    def cancel(self) -> None:
        """Cancels the download, stopping all operations immediately."""
        self._transition_to(DownloadState.CANCELLED)
        if self._stop_event and self._loop:
            self._loop.call_soon_threadsafe(self._stop_event.set)
        if self._pause_event and self._loop:
            self._loop.call_soon_threadsafe(self._pause_event.set)
        self._stop_event_thread.set()

    # Internal lifecycle

    def _transition_to(self, new_state: DownloadState) -> None:
        with self._state_lock:
            if new_state not in VALID_TRANSITIONS.get(self._state, set()):
                raise InvalidStateTransition(self._state, new_state)
            self._state = new_state

    def _prepare_file(self) -> None:
        if self._metadata is None:
            raise RuntimeError("Metadata not set before _prepare_file.")

        self._fd = os.open(self._filename, os.O_RDWR | os.O_CREAT)
        try:
            if hasattr(os, "fallocate"):
                try:
                    os.fallocate(self._fd, 0, 0, self._metadata.total_size)
                except OSError:
                    os.ftruncate(self._fd, self._metadata.total_size)
            else:
                os.ftruncate(self._fd, self._metadata.total_size)
        except Exception:
            os.close(self._fd)
            self._fd = None
            raise

    def _load_resume_state(self) -> None:
        if self._metadata is None:
            raise RuntimeError("Metadata not set before _load_resume_state.")
        if self._state_manager is None:
            raise RuntimeError("State manager not initialized before _load_resume_state.")

        saved = self._state_manager.load()

        if saved and self._state_manager.validate_for_resume(saved, self._metadata, self._chunk_size):
            self._download_state = saved
        else:
            if saved:
                self._progress.set_overlay(" RESTARTING ", color="red")
                def _clear_restarting() -> None:
                    time.sleep(3)
                    self._progress.set_overlay("")
                threading.Thread(target=_clear_restarting, daemon=True).start()
                self._progress.log("[!] Remote file changed or parameters altered. Restarting download from scratch...")
            self._download_state = self._state_manager.build_fresh_state(self._metadata, self._chunk_size)

        completed_list = self._download_state.get("completed", [])
        if not isinstance(completed_list, list):
            completed_list = []
        self._download_state["completed"] = completed_list
        self._completed_set = set(completed_list)

    def _build_chunk_queue(self) -> asyncio.PriorityQueue:
        total_chunks = self._total_chunks
        chunk_queue: asyncio.PriorityQueue = asyncio.PriorityQueue()
        with self._state_lock:
            completed_snapshot = set(self._completed_set)
        for i in range(total_chunks):
            if i not in completed_snapshot:
                chunk_queue.put_nowait((0.0, i, 0))
        return chunk_queue

    def _init_components(self) -> None:
        if self._fd is None:
            raise RuntimeError("File descriptor not set before _init_components.")
        if self._metadata is None:
            raise RuntimeError("Metadata not set before _init_components.")

        if self._writer is None:
            if self._use_mmap and self._metadata.total_size > 0 and self._metadata.accepts_ranges:
                self._writer = MmapDiskWriter(
                    self._fd,
                    self._metadata.total_size,
                    completed_chunks=self._completed_set,
                    condition=self._chunk_condition,
                )
            else:
                self._writer = DiskWriter(
                    self._fd,
                    self._stop_event_thread,
                    completed_chunks=self._completed_set,
                    condition=self._chunk_condition,
                )

        if self._hasher is None:
            self._hasher = StreamingHasher(
                filename=self._filename,
                chunk_size=self._chunk_size,
                total_size=self._metadata.total_size,
                total_chunks=self._total_chunks,
                disk_writer=self._writer,
                stop_event=self._stop_event_thread,
                hash_spec=self._hash_spec,
                progress=self._progress,
                condition=self._chunk_condition,
            )

    @property
    def _total_chunks(self) -> int:
        if self._metadata is None:
            raise RuntimeError("Metadata not set before accessing _total_chunks.")
        if self._metadata.total_size <= 0:
            return 1
        return (self._metadata.total_size + self._chunk_size - 1) // self._chunk_size

    # Download orchestration

    async def _download_chunks(self, chunk_queue: asyncio.PriorityQueue) -> bool:
        if self._stop_event is None:
            raise RuntimeError("Stop event not initialized. This is a bug.")
            
        tasks = []
        worker_idx = 0
        for url in self._urls:
            for _ in range(self._threads_per_mirror):
                t = asyncio.create_task(self._worker(url, chunk_queue, worker_idx))
                tasks.append(t)
                worker_idx += 1

        try:
            wait_task = asyncio.create_task(chunk_queue.join())
            stop_task = asyncio.create_task(self._stop_event.wait())
            done, pending = await asyncio.wait(
                [wait_task, stop_task],
                return_when=asyncio.FIRST_COMPLETED
            )
            for t in pending:
                t.cancel()
                
            is_complete = (wait_task in done) and not self._stop_event.is_set()
        except asyncio.CancelledError:
            if self._stop_event:
                self._stop_event.set()
            raise
        finally:
            # Tell workers to stop
            for _ in tasks:
                chunk_queue.put_nowait((0.0, _STOP_SENTINEL, 0))
            
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

    async def _worker(self, mirror_url: str, chunk_queue: asyncio.PriorityQueue, worker_idx: int) -> None:
        per_thread_throttle: ConsumesTokens | None = (
            TokenBucketThrottle(self._max_speed_per_thread_kbps)
            if self._max_speed_per_thread_kbps is not None
            else None
        )

        await asyncio.sleep(min(0.02 * worker_idx, 0.3))

        if self._metadata is None:
            raise RuntimeError("Metadata not set before starting worker. This is a bug.")
        if self._writer is None:
            raise RuntimeError("Writer not set before starting worker. This is a bug.")
        if self._session is None:
            raise RuntimeError("Session not set before starting worker. This is a bug.")
        if self._stop_event is None or self._pause_event is None:
            raise RuntimeError("Events not initialized.")

        config = FetcherConfig(
            chunk_size=self._chunk_size,
            min_speed_kbps=self._min_speed_kbps,
            speed_grace_period=self._speed_grace_period,
            per_thread_throttle=per_thread_throttle,
            global_throttle=self._global_throttle,
        )
        fetcher = ChunkFetcher(
            session=self._session,
            mirror_url=mirror_url,
            metadata=self._metadata,
            writer=self._writer,
            progress=self._progress,
            stop_event=self._stop_event,
            config=config,
        )

        while not self._stop_event.is_set():
            await self._pause_event.wait()

            try:
                queue_item = await asyncio.wait_for(chunk_queue.get(), timeout=1.0)
                next_retry_time, chunk_idx, retries = queue_item
            except asyncio.TimeoutError:
                continue

            try:
                if chunk_idx == _STOP_SENTINEL:
                    break

                now = time.monotonic()
                if now < next_retry_time:
                    await chunk_queue.put((next_retry_time, chunk_idx, retries))
                    await asyncio.sleep(min(0.5, next_retry_time - now))
                    continue

                if self._health.is_banned(mirror_url):
                    all_banned = all(self._health.is_banned(m) for m in self._urls)
                    if all_banned:
                        with self._state_lock:
                            self._last_error = "All mirrors are currently banned due to failures or slow speeds."
                        self._stop_event.set()
                        self._stop_event_thread.set()
                        break
                    
                    await chunk_queue.put((next_retry_time, chunk_idx, retries))
                    await asyncio.sleep(2.0)
                    continue

                await self._process_chunk(fetcher, chunk_idx, retries, chunk_queue, mirror_url)
            finally:
                chunk_queue.task_done()

    async def _process_chunk(
        self,
        fetcher: ChunkFetcher,
        chunk_idx: int,
        retries: int,
        chunk_queue: asyncio.PriorityQueue,
        mirror_url: str,
    ) -> None:
        """Processes a single chunk download with retries."""
        try:
            await fetcher.fetch(chunk_idx)
            if fetcher.metadata is not self._metadata:
                with self._state_lock:
                    self._metadata = fetcher.metadata
            with self._state_lock:
                if chunk_idx not in self._completed_set:
                    self._completed_set.add(chunk_idx)
        except StoppedException:
            return
        except (SlowMirrorException, IncompleteChunkError, aiohttp.ClientError, asyncio.TimeoutError) as e:
            if not isinstance(self._progress, NoOpProgress):
                logging.warning(f"MIRROR {mirror_url} FAILED WITH {type(e).__name__}: {e}")
            self._health.record_failure(e, mirror_url)
            await self._handle_chunk_failure(e, chunk_idx, retries, chunk_queue)
        except Exception as e:
            logging.exception("Bug encountered in worker:")
            self._health.record_failure(e, mirror_url)
            await self._handle_chunk_failure(e, chunk_idx, retries, chunk_queue)

    async def _handle_chunk_failure(
        self,
        e: Exception,
        chunk_idx: int,
        retries: int,
        chunk_queue: asyncio.PriorityQueue,
    ) -> None:
        """Handles retrying or aborting on chunk failure."""
        if not self._stop_event.is_set(): # type: ignore
            if retries < 5:
                backoff = min(30.0, 2.0 ** retries)
                await chunk_queue.put((time.monotonic() + backoff, chunk_idx, retries + 1))
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
                self._stop_event.set() # type: ignore
                self._stop_event_thread.set()

    # State persistence

    async def _run_state_saver(self) -> None:
        last_saved_count = len(self._download_state.get("completed", []))

        while self._stop_event and not self._stop_event.is_set():
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=3.0)
            except asyncio.TimeoutError:
                pass
                
            if self._stop_event.is_set():
                break

            state_copy = None
            with self._state_lock:
                current_count = len(self._completed_set)
                if current_count > last_saved_count:
                    state_copy = dict(self._download_state)
                    state_copy["completed"] = list(self._completed_set)
                    last_saved_count = current_count
            
            if state_copy is not None:
                if self._state_manager is None:
                    raise RuntimeError("State manager not initialized in state saver. This is a bug.")
                try:
                    await asyncio.to_thread(self._state_manager.save, state_copy)
                except OSError as e:
                    self._progress.log(f"Warning: Failed to save progress state: {e}")

    def _save_state(self) -> None:
        with self._state_lock:
            if self._state_manager is None:
                raise RuntimeError("State manager not initialized in _save_state. This is a bug.")
            try:
                # Refresh the completed list from the source-of-truth set before saving.
                self._download_state["completed"] = list(self._completed_set)
                self._state_manager.save(self._download_state)
            except OSError as e:
                self._progress.log(f"Warning: Failed to save progress state: {e}")
            if self._writer is not None:
                try:
                    self._writer.flush()
                except ValueError:
                    pass
