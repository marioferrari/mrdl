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
from mrdl.protocols import ConsumesTokens, FetchesChunks, PersistsState, ProbesMetadata, ReportsProgress, TracksHealth, VerifiesIntegrity, WritesChunks
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
        prober: ProbesMetadata | None = None,
        health: TracksHealth | None = None,
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
        self._sock_read_timeout = config.sock_read_timeout
        self._sock_connect_timeout = config.sock_connect_timeout
        self._safe_state_saves = config.safe_state_saves
        self._use_mmap = config.use_mmap

        self._global_throttle: ConsumesTokens | None = global_throttle or TokenBucketThrottle(config.max_speed_kbps)

        self._state_manager = state_manager
        self._writer = writer
        self._hasher = hasher
        if config.silent: self._progress: ReportsProgress = NoOpProgress()
        else: self._progress = progress or BuiltinProgress(
            compact=config.compact,
            speed_ema_window=config.speed_ema_window,
            speed_update_interval=config.speed_update_interval,
        )
        self._is_throttled = config.max_speed_kbps is not None
        if global_throttle is not None:
            if hasattr(global_throttle, "is_active"): self._is_throttled = global_throttle.is_active
            else: self._is_throttled = True
        self._health = health or MirrorHealthTracker()
        self._prober = prober or MirrorProber()
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
        """Starts or resumes the download process asynchronously."""
        start_time = time.monotonic()
        self._loop, self._stop_event, self._pause_event = asyncio.get_running_loop(), asyncio.Event(), asyncio.Event()
        self._pause_event.set()
        self._stop_event_thread.clear()
        
        self._state_manager = self._state_manager or JsonStateManager(f"{self._filename}.progress", safe_saves=self._safe_state_saves)
            
        if os.path.exists(self._filename):
            if self._overwrite:
                try: os.remove(self._filename)
                except OSError: pass
                await asyncio.to_thread(self._state_manager.clear)
            elif await asyncio.to_thread(self._state_manager.load) is None:
                raise DestinationExistsError(f"Destination '{self._filename}' exists and no resume state found. Use overwrite=True.")

        self._transition_to(DownloadState.PROBING)
        try:
            self._metadata = await self._prober.probe(self._urls)
        except (FileNotFoundError, ValueError) as e:
            self._progress.log(f"[!] Error: {e}")
            self._transition_to(DownloadState.FAILED)
            self._last_error = str(e)
            return DownloadResult(self._state, self._filename, False, time.monotonic() - start_time, self._last_error, None)

        if self._metadata.total_size == 0 or not self._metadata.accepts_ranges:
            self._apply_probe_fallback()

        self._transition_to(DownloadState.DOWNLOADING)

        sm = SessionManager(self._filename, self._chunk_size, self._metadata, self._state_manager, self._progress, self._hash_spec, self._stop_event_thread, self._chunk_condition, use_mmap=self._use_mmap)
        
        session = sm.create_http_session()
        state_task = None
        success = False
        paused = False

        try:
            self._fd = await asyncio.to_thread(sm.prepare_file)
            self._download_state, self._completed_set = await asyncio.to_thread(sm.load_resume_state)
            self._writer = self._writer or sm.init_writer(self._fd, self._completed_set)
            self._hasher = self._hasher or sm.init_hasher(self._writer)
            
            remaining_chunks = sm.build_chunk_queue(compute_total_chunks(self._metadata.total_size, self._chunk_size), self._completed_set)
            self._writer.start()
            self._hasher.start()
            self._progress.start(self._metadata.total_size, self._label, self._chunk_size, self._completed_set)
            self._progress.set_throttled(self._is_throttled)
            state_task = asyncio.create_task(self._run_state_saver())

            def _create_fetcher(source: str, idx: int) -> FetchesChunks:
                pt = TokenBucketThrottle(self._max_speed_per_thread_kbps) if self._max_speed_per_thread_kbps else None
                cfg = FetcherConfig(
                    self._chunk_size,
                    self._min_speed_kbps,
                    self._speed_grace_period,
                    pt,
                    self._global_throttle,
                    sock_read_timeout=self._sock_read_timeout,
                    sock_connect_timeout=self._sock_connect_timeout,
                )
                assert self._metadata is not None
                assert self._writer is not None
                assert self._stop_event is not None
                return ChunkFetcher(session, source, self._metadata, self._writer, self._progress, self._stop_event, cfg)

            pool = WorkerPool(self._urls, self._threads_per_mirror, remaining_chunks, _create_fetcher, self._health, self._completed_set, self._state_lock, self._stop_event, self._pause_event, self._stop_event_thread, self._progress)

            try:
                success = await pool.run()
                self._last_error = pool.last_error or self._last_error
            except asyncio.CancelledError:
                paused = True
                if self._state == DownloadState.DOWNLOADING: self._transition_to(DownloadState.PAUSED)
                raise
            finally:
                if not success:
                    if self._stop_event: self._stop_event.set()
                    self._stop_event_thread.set()
                
                if getattr(self, "_writer", None):
                    self._writer.stop()
                if getattr(self, "_hasher", None):
                    self._hasher.stop()
                
                self._save_state()
                if state_task:
                    state_task.cancel()

                if getattr(self._writer, "error", None):
                    self._last_error = f"Disk write error: {self._writer.error}"
                    self._progress.set_overlay(" FATAL ERROR ", color="red")
                    self._progress.log(f"[!] {self._last_error}")
                    if self._state in (DownloadState.DOWNLOADING, DownloadState.PAUSED): self._transition_to(DownloadState.FAILED)
                elif paused or self._state == DownloadState.PAUSED:
                    self._progress.set_overlay(" STATE SAVED ", color="blue")
                elif not success and self._state == DownloadState.DOWNLOADING:
                    self._transition_to(DownloadState.FAILED)

                if self._stop_event: self._stop_event.set()
                self._stop_event_thread.set()

                try: await session.close()
                except asyncio.CancelledError: pass
                if self._fd is not None:
                    os.close(self._fd)
                    self._fd = None

            if success:
                try:
                    verified = await asyncio.to_thread(self._hasher.finalize)
                    if verified:
                        await asyncio.to_thread(self._state_manager.clear)
                        self._transition_to(DownloadState.COMPLETED)
                        self._progress.set_overlay(" HASH OK " if self._hasher.has_work else " COMPLETED ", success=True)
                    else:
                        self._transition_to(DownloadState.FAILED)
                        self._progress.set_overlay(" HASH FAILED ", success=False)
                        self._last_error = "Hash verification failed."
                except IncompleteHashError as e:
                    self._transition_to(DownloadState.FAILED)
                    self._progress.set_overlay(" HASH FAILED ", success=False)
                    self._last_error = f"Hash verification failed: {e}"
                    verified = False
            else:
                verified = False

            return DownloadResult(self._state, self._filename, verified, time.monotonic() - start_time, self._last_error, self.computed_hash)
        finally:
            self._progress.close()

    def pause(self) -> None:
        if self._state == DownloadState.DOWNLOADING:
            if self._pause_event and self._loop: self._loop.call_soon_threadsafe(self._pause_event.clear)
            self._transition_to(DownloadState.PAUSED)
            self._progress.set_overlay(" PAUSED ", color="yellow")
            if self._state_manager: self._save_state()

    def resume(self) -> None:
        if self._state == DownloadState.PAUSED:
            self._transition_to(DownloadState.DOWNLOADING)
            if self._pause_event and self._loop: self._loop.call_soon_threadsafe(self._pause_event.set)
            self._progress.set_overlay("")

    def stop(self) -> None:
        if self._state in (DownloadState.DOWNLOADING, DownloadState.PAUSED):
            if self._state != DownloadState.PAUSED: self._transition_to(DownloadState.PAUSED)
            if self._stop_event and self._loop: self._loop.call_soon_threadsafe(self._stop_event.set)
            if self._pause_event and self._loop: self._loop.call_soon_threadsafe(self._pause_event.set)
            self._stop_event_thread.set()

    def cancel(self) -> None:
        self._transition_to(DownloadState.CANCELLED)
        if self._stop_event and self._loop: self._loop.call_soon_threadsafe(self._stop_event.set)
        if self._pause_event and self._loop: self._loop.call_soon_threadsafe(self._pause_event.set)
        self._stop_event_thread.set()

    def _apply_probe_fallback(self) -> None:
        if self._metadata and (self._metadata.total_size == 0 or not self._metadata.accepts_ranges):
            if self._threads_per_mirror > 1 or len(self._urls) > 1:
                self._progress.log("Warning: Could not fetch file size or mirrors do not support Range requests. Falling back to single-task.")
            self._threads_per_mirror, self._urls = 1, [self._urls[0]]
            self._chunk_size = self._metadata.total_size if self._metadata.total_size > 0 else FALLBACK_UNKNOWN_SIZE_CHUNK

    def _transition_to(self, new_state: DownloadState) -> None:
        with self._state_lock:
            if new_state not in VALID_TRANSITIONS.get(self._state, set()):
                raise InvalidStateTransition(self._state, new_state)
            self._state = new_state

    # State persistence

    async def _run_state_saver(self) -> None:
        last_saved = len(self._download_state.get("completed", []))
        while self._stop_event and not self._stop_event.is_set():
            try: await asyncio.wait_for(self._stop_event.wait(), timeout=3.0)
            except asyncio.TimeoutError: pass
            if self._stop_event.is_set(): break

            with self._state_lock:
                current = len(self._completed_set)
                if current > last_saved:
                    self._download_state["completed"] = list(self._completed_set)
                    last_saved, state_copy = current, dict(self._download_state)
                else: state_copy = None

            if state_copy and self._state_manager:
                try: await asyncio.to_thread(self._state_manager.save, state_copy)
                except OSError as e: self._progress.log(f"Warning: Failed to save progress state: {e}")

    def _save_state(self) -> None:
        with self._state_lock:
            if not self._state_manager: return
            try:
                self._download_state["completed"] = list(self._completed_set)
                self._state_manager.save(self._download_state)
            except OSError as e: self._progress.log(f"Warning: Failed to save progress state: {e}")
            if self._writer:
                try: self._writer.flush()
                except ValueError: pass
