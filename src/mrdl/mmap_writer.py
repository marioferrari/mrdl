from __future__ import annotations

import mmap
import threading
import asyncio


class MmapDiskWriter:
    """Manages thread-safe concurrent writes to a pre-allocated file via mmap."""

    def __init__(
        self,
        fd: int,
        total_size: int,
        completed_chunks: set[int] | None = None,
        condition: threading.Condition | None = None,
    ):
        """Initializes the MmapDiskWriter.

        Args:
            fd: File descriptor of the pre-allocated destination file.
            total_size: Total size of the pre-allocated file in bytes.
            completed_chunks: Pre-populated set of completed chunk indices (for resume).
            condition: Optional Condition variable to notify on chunk completion.
        """
        self._fd = fd
        self._total_size = total_size
        self._mmap: mmap.mmap | None = None
        self._disk_completed: set[int] = set(completed_chunks) if completed_chunks else set()
        self._lock = threading.Lock()
        self._condition = condition

    @property
    def error(self) -> Exception | None:
        """Returns the fatal exception encountered by the writer, if any."""
        return None

    def start(self) -> None:
        """Maps the pre-allocated file descriptor to virtual memory."""
        self._mmap = mmap.mmap(self._fd, self._total_size, access=mmap.ACCESS_WRITE)
        try:
            # Hint to the kernel that the hash thread will read this mapping sequentially
            # (offset 0 → EOF), enabling aggressive read-ahead and early page reclamation.
            # Random-access writers are unaffected: mmap slice assignment bypasses read-ahead.
            self._mmap.madvise(mmap.MADV_SEQUENTIAL)
        except (AttributeError, OSError):
            pass  # Not available on Windows or older kernels

    async def write(self, offset: int, data: bytes | bytearray | memoryview) -> None:
        """Writes data directly to virtual memory at the specified offset.

        Args:
            offset: The file offset to write the data to.
            data: Binary data to write.
        """
        if self._mmap is None:
            raise RuntimeError("MmapDiskWriter has not been started.")
        self._mmap[offset:offset + len(data)] = data

    async def mark_complete(self, chunk_index: int) -> None:
        """Marks a specific chunk index as completed on disk.

        Args:
            chunk_index: Completed chunk index.
        """
        with self._lock:
            self._disk_completed.add(chunk_index)
        if self._condition is not None:
            with self._condition:
                self._condition.notify_all()

    def is_on_disk(self, chunk_index: int) -> bool:
        """Checks if a chunk index is completed.

        Args:
            chunk_index: Chunk index to check.

        Returns:
            True if the chunk is completed, otherwise False.
        """
        with self._lock:
            return chunk_index in self._disk_completed

    def flush(self) -> None:
        """Flushes modified virtual memory pages back to disk."""
        if self._mmap is not None:
            try:
                self._mmap.flush()
            except ValueError:
                pass

    def read_chunk(self, offset: int, length: int) -> memoryview | None:
        """Returns a memoryview slice of the mapped file if started, otherwise None."""
        if self._mmap is not None:
            try:
                return memoryview(self._mmap)[offset:offset + length]
            except ValueError:
                return None
        return None

    def stop(self) -> None:
        """Flushes memory-mapped changes and releases the memory map mapping."""
        if self._mmap is not None:
            self._mmap.flush()
            self._mmap.close()
            self._mmap = None
