from __future__ import annotations

import os
import asyncio
import threading
import time
from typing import Any

from mrdl.exceptions import IncompleteHashError
from mrdl.fetcher import ChunkFetcher, FetcherConfig
from mrdl.mirror_health import MirrorHealthTracker
from mrdl.prober import MirrorProber
from mrdl.progress import BuiltinProgress, NoOpProgress
from mrdl.protocols import ConsumesTokens, FetchesChunks, PersistsState, ReportsProgress, VerifiesIntegrity, WritesChunks
from mrdl.persistence import JsonStateManager
from mrdl.session import SessionManager, compute_total_chunks, FALLBACK_UNKNOWN_SIZE_CHUNK
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
)
from mrdl.worker_pool import WorkerPool
import aiohttp


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
            self._progress = progress or BuiltinProgress(compact=config.compact)


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

        # Delegate session setup to SessionManager
        session_mgr = SessionManager(
            filename=self._filename,
            chunk_size=self._chunk_size,
            metadata=self._metadata,
            state_manager=self._state_manager,
            progress=self._progress,
            hash_spec=self._hash_spec,
            stop_event_thread=self._stop_event_thread,
            chunk_condition=self._chunk_condition,
            use_mmap=self._use_mmap,
        )

        self._fd = await asyncio.to_thread(session_mgr.prepare_file)
        self._download_state, self._completed_set = await asyncio.to_thread(session_mgr.load_resume_state)

        if self._writer is None:
            self._writer = session_mgr.init_writer(self._fd, self._completed_set)
        if self._hasher is None:
            self._hasher = session_mgr.init_hasher(self._writer)

        total_chunks = compute_total_chunks(self._metadata.total_size, self._chunk_size)
        remaining_chunks = SessionManager.build_chunk_queue(total_chunks, self._completed_set)

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
        
        self._session = SessionManager.create_http_session()
        
        if self._session is None or self._metadata is None or self._writer is None or self._stop_event is None or self._pause_event is None:
            raise RuntimeError("Required components not initialized. This is a bug.")

        def _create_fetcher(source: str, worker_idx: int) -> FetchesChunks:
            per_thread_throttle: ConsumesTokens | None = (
                TokenBucketThrottle(self._max_speed_per_thread_kbps)
                if self._max_speed_per_thread_kbps is not None
                else None
            )
            config = FetcherConfig(
                chunk_size=self._chunk_size,
                min_speed_kbps=self._min_speed_kbps,
                speed_grace_period=self._speed_grace_period,
                per_thread_throttle=per_thread_throttle,
                global_throttle=self._global_throttle,
            )
            return ChunkFetcher(
                session=self._session,
                mirror_url=source,
                metadata=self._metadata,
                writer=self._writer,
                progress=self._progress,
                stop_event=self._stop_event,
                config=config,
            )

        pool = WorkerPool(
            sources=self._urls,
            threads_per_source=self._threads_per_mirror,
            chunk_queue=remaining_chunks,
            fetcher_factory=_create_fetcher,
            health=self._health,
            completed_set=self._completed_set,
            state_lock=self._state_lock,
            stop_event=self._stop_event,
            pause_event=self._pause_event,
            stop_event_thread=self._stop_event_thread,
            progress=self._progress,
        )

        try:
            try:
                success = await pool.run()
                if pool.last_error:
                    self._last_error = pool.last_error
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
                self._save_state()

                if self._writer.error is not None:
                    self._last_error = f"Disk write error: {self._writer.error}"
                    self._progress.set_overlay(" FATAL ERROR ", color="red")
                    self._progress.log(f"[!] {self._last_error}")
                    if self._state in (DownloadState.DOWNLOADING, DownloadState.PAUSED):
                        self._transition_to(DownloadState.FAILED)
                elif paused or self._state == DownloadState.PAUSED:
                    self._progress.set_overlay(" STATE SAVED ", color="blue")

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
