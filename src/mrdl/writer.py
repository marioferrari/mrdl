from __future__ import annotations

import os
import queue
import threading
import asyncio
from dataclasses import dataclass


@dataclass
class WriteCommand:
    """Instructs the writer to write data at a given file offset."""

    offset: int
    data: bytes | bytearray | memoryview


@dataclass
class MarkCommand:
    """Instructs the writer to mark a chunk as successfully written to disk."""

    chunk_index: int


@dataclass
class TruncateCommand:
    """Instructs the writer to truncate the file to a given size."""

    size: int


class DiskWriter:
    """Manages thread-safe asynchronous writes to the local destination file."""

    def __init__(
        self,
        fd: int,
        stop_event: threading.Event,
        maxsize: int = 128,
        completed_chunks: set[int] | None = None,
        condition: threading.Condition | None = None,
    ):
        """Initializes the DiskWriter.

        Args:
            fd: File descriptor of the destination file open for writing.
            stop_event: Event signaling threads to stop execution.
            maxsize: Maximum size of the write queue.
            completed_chunks: Pre-populated set of completed chunk indices (for resume).
            condition: Optional Condition variable to notify on chunk completion.
        """
        self._fd = fd
        self._stop_event = stop_event
        self._queue: queue.Queue[WriteCommand | MarkCommand | TruncateCommand | None] = queue.Queue(maxsize=maxsize)
        self._thread: threading.Thread | None = None
        self._disk_completed: set[int] = set(completed_chunks) if completed_chunks else set()
        self._lock = threading.Lock()
        self._condition = condition
        self._error: Exception | None = None

    @property
    def error(self) -> Exception | None:
        """Returns the fatal exception encountered by the writer, if any."""
        return self._error

    def start(self) -> None:
        """Starts the background write-worker thread."""
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    async def write(self, offset: int, data: bytes | bytearray | memoryview) -> None:
        """Queues data to be written at the specified offset.

        Args:
            offset: File write offset.
            data: Binary data to write.
        """
        cmd = WriteCommand(offset=offset, data=data)
        while not self._stop_event.is_set():
            try:
                await asyncio.to_thread(self._queue.put, cmd, True, 1)
                return
            except queue.Full:
                continue

    async def mark_complete(self, chunk_index: int) -> None:
        """Queues a marker signaling that a chunk download has completed.

        Args:
            chunk_index: Completed chunk index.
        """
        cmd = MarkCommand(chunk_index=chunk_index)
        while not self._stop_event.is_set():
            try:
                await asyncio.to_thread(self._queue.put, cmd, True, 1)
                return
            except queue.Full:
                continue

    async def truncate(self, size: int) -> None:
        """Queues a truncation of the file to the specified size.

        Args:
            size: The size to truncate the file to.
        """
        cmd = TruncateCommand(size=size)
        while not self._stop_event.is_set():
            try:
                await asyncio.to_thread(self._queue.put, cmd, True, 1)
                return
            except queue.Full:
                continue

    def is_on_disk(self, chunk_index: int) -> bool:
        """Returns True if the chunk index has been successfully written to disk.

        Args:
            chunk_index: Chunk index to check.

        Returns:
            True if the chunk is written, otherwise False.
        """
        with self._lock:
            return chunk_index in self._disk_completed

    def stop(self) -> None:
        """Stops the write worker thread, finishing any remaining writes."""
        try:
            self._queue.put(None, timeout=1)
        except queue.Full:
            pass
        if self._thread is not None:
            self._thread.join(timeout=5)

    def flush(self) -> None:
        """No-op for the standard queue-based DiskWriter."""
        pass

    def read_chunk(self, offset: int, length: int) -> memoryview | None:
        """Returns None as standard DiskWriter does not map file in memory."""
        return None

    def _run(self) -> None:
        """Worker thread loop that consumes write/mark commands and executes disk I/O."""
        try:
            while not self._stop_event.is_set():
                try:
                    item = self._queue.get(timeout=0.5)
                except queue.Empty:
                    continue

                if item is None:
                    break

                if isinstance(item, WriteCommand):
                    view = memoryview(item.data)
                    written = 0
                    fd = self._fd
                    while written < len(view):
                        n = os.pwrite(fd, view[written:], item.offset + written)
                        if n == 0:
                            raise OSError("Failed to write data (disk full?)")
                        written += n
                elif isinstance(item, MarkCommand):
                    with self._lock:
                        self._disk_completed.add(item.chunk_index)
                    if self._condition is not None:
                        with self._condition:
                            self._condition.notify_all()
                elif isinstance(item, TruncateCommand):
                    os.ftruncate(self._fd, item.size)
        except Exception as e:
            self._error = e
            self._stop_event.set()
