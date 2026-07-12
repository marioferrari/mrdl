"""Download session setup: file preparation, resume state, and component initialization."""

from __future__ import annotations

import asyncio
import os
import sys
import threading
import time
from typing import TYPE_CHECKING

import aiohttp

from mrdl.hasher import StreamingHasher
from mrdl.mmap_writer import MmapDiskWriter
from mrdl.writer import DiskWriter

if TYPE_CHECKING:
    from mrdl.protocols import PersistsState, ReportsProgress, VerifiesIntegrity, WritesChunks
    from mrdl.types import FileMetadata, HashSpec

FALLBACK_UNKNOWN_SIZE_CHUNK = 1024 ** 3


def compute_total_chunks(total_size: int, chunk_size: int) -> int:
    """Computes the number of chunks needed to cover a file.

    Args:
        total_size: Total file size in bytes.
        chunk_size: Size of each chunk in bytes.

    Returns:
        Number of chunks (at least 1).
    """
    if total_size <= 0:
        return 1
    return (total_size + chunk_size - 1) // chunk_size


class SessionManager:
    """Manages pre-download setup: file allocation, resume state, and component creation.

    Encapsulates the one-time setup that must happen between probing and downloading:
    opening/allocating the destination file, loading resume state, and constructing
    the writer and hasher components.
    """

    def __init__(
        self,
        filename: str,
        chunk_size: int,
        metadata: FileMetadata,
        state_manager: PersistsState,
        progress: ReportsProgress,
        hash_spec: HashSpec | None,
        stop_event_thread: threading.Event,
        chunk_condition: threading.Condition,
        *,
        use_mmap: bool = False,
    ) -> None:
        """Initializes the SessionManager.

        Args:
            filename: Path to the destination file.
            chunk_size: Size of each download chunk in bytes.
            metadata: File metadata from the probe phase.
            state_manager: Persistence layer for download state.
            progress: Progress reporter for user-facing messages.
            hash_spec: Optional hash configuration for integrity verification.
            stop_event_thread: Thread-level stop event shared with writer/hasher.
            chunk_condition: Condition variable for chunk completion notifications.
            use_mmap: If True, use memory-mapped I/O for writing.
        """
        self._filename = filename
        self._chunk_size = chunk_size
        self._metadata = metadata
        self._state_manager = state_manager
        self._progress = progress
        self._hash_spec = hash_spec
        self._stop_event_thread = stop_event_thread
        self._chunk_condition = chunk_condition
        self._use_mmap = use_mmap

    def prepare_file(self) -> int:
        """Opens and pre-allocates the destination file.

        Returns:
            The file descriptor for the destination file.

        Raises:
            OSError: If the file cannot be created or pre-allocated.
        """
        fd = os.open(self._filename, os.O_RDWR | os.O_CREAT)
        try:
            if hasattr(os, "fallocate"):
                try:
                    os.fallocate(fd, 0, 0, self._metadata.total_size)
                except OSError:
                    os.ftruncate(fd, self._metadata.total_size)
            else:
                os.ftruncate(fd, self._metadata.total_size)
        except Exception:
            os.close(fd)
            raise
        return fd

    def load_resume_state(self) -> tuple[dict, set[int]]:
        """Loads and validates resume state, or builds fresh state.

        Returns:
            A tuple of (state_dict, completed_set).
        """
        saved = self._state_manager.load()

        if saved and self._state_manager.validate_for_resume(saved, self._metadata, self._chunk_size):
            download_state = saved
        else:
            if saved:
                self._progress.set_overlay(" RESTARTING ", color="red")

                def _clear_restarting() -> None:
                    time.sleep(3)
                    self._progress.set_overlay("")

                threading.Thread(target=_clear_restarting, daemon=True).start()
                self._progress.log("[!] Remote file changed or parameters altered. Restarting download from scratch...")
            download_state = self._state_manager.build_fresh_state(self._metadata, self._chunk_size)

        completed_list = download_state.get("completed", [])
        if not isinstance(completed_list, list):
            completed_list = []
        download_state["completed"] = completed_list
        completed_set = set(completed_list)

        return download_state, completed_set

    def init_writer(
        self,
        fd: int,
        completed_set: set[int],
    ) -> WritesChunks:
        """Creates a disk writer component.

        Args:
            fd: File descriptor for the destination file.
            completed_set: Set of already-completed chunk indices.

        Returns:
            A writer implementing the WritesChunks protocol.
        """
        if self._use_mmap and self._metadata.total_size > 0 and self._metadata.accepts_ranges:
            if sys.platform == 'darwin':
                self._progress.log(
                    "WARNING: --use-mmap is known to cause silent data corruption on macOS APFS. "
                    "DiskWriter is highly recommended instead."
                )
            return MmapDiskWriter(
                fd,
                self._metadata.total_size,
                completed_chunks=completed_set,
                condition=self._chunk_condition,
            )
        return DiskWriter(
            fd,
            self._stop_event_thread,
            completed_chunks=completed_set,
            condition=self._chunk_condition,
        )

    def init_hasher(
        self,
        writer: WritesChunks,
    ) -> VerifiesIntegrity:
        """Creates a streaming hasher component.

        Args:
            writer: The disk writer that the hasher will read completed chunks from.

        Returns:
            A hasher implementing the VerifiesIntegrity protocol.
        """
        total_chunks = compute_total_chunks(self._metadata.total_size, self._chunk_size)
        return StreamingHasher(
            filename=self._filename,
            chunk_size=self._chunk_size,
            total_size=self._metadata.total_size,
            total_chunks=total_chunks,
            disk_writer=writer,
            stop_event=self._stop_event_thread,
            hash_spec=self._hash_spec,
            progress=self._progress,
            condition=self._chunk_condition,
        )

    @staticmethod
    def build_chunk_queue(total_chunks: int, completed_set: set[int]) -> asyncio.PriorityQueue:
        """Builds the initial chunk priority queue, excluding already-completed chunks.

        Args:
            total_chunks: Total number of chunks in the file.
            completed_set: Set of already-completed chunk indices.

        Returns:
            An asyncio.PriorityQueue populated with pending chunk indices.
        """
        chunk_queue: asyncio.PriorityQueue = asyncio.PriorityQueue()
        for i in range(total_chunks):
            if i not in completed_set:
                chunk_queue.put_nowait((0.0, i, 0))
        return chunk_queue

    @staticmethod
    def create_http_session() -> aiohttp.ClientSession:
        """Creates an aiohttp ClientSession configured for concurrent downloads.

        Returns:
            A configured aiohttp.ClientSession.
        """
        conn = aiohttp.TCPConnector(limit=0)  # Uncapped connection pool, let downloader manage tasks
        return aiohttp.ClientSession(connector=conn, read_bufsize=1024 * 1024)
