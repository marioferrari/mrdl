from __future__ import annotations

import hashlib
import os
import threading
import time

from mrdl.exceptions import IncompleteHashError
from mrdl.types import HashSpec

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from mrdl.protocols import ReportsProgress

_READ_CHUNK = 1 << 22  # 4 MiB — fewer syscalls and longer GIL-free C runs per chunk


class StreamingHasher:
    """Hashes a file concurrently as chunks are written to disk.

    Monitors a disk writer to process written chunks in sequential order
    and update a hash digest in a background thread.
    """

    def __init__(
        self,
        filename: str,
        chunk_size: int,
        total_size: int,
        total_chunks: int,
        disk_writer,
        stop_event: threading.Event,
        hash_spec: HashSpec | None = None,
        progress: ReportsProgress | None = None,
        condition: threading.Condition | None = None,
    ):
        """Initializes the StreamingHasher.

        Args:
            filename: Path to the target file to read and hash.
            chunk_size: Size of each download chunk in bytes.
            total_size: Total expected file size in bytes.
            total_chunks: Total number of chunks to hash.
            disk_writer: Disk writer component that tracks flushed chunks.
            stop_event: Event signaling threads to stop execution.
            hash_spec: Optional target hash configuration.
            progress: Optional progress reporter to notify of hashed chunks.
            condition: Optional Condition variable to wait on for completed chunks.
        """
        self._filename = filename
        self._chunk_size = chunk_size
        self._total_size = total_size
        self._total_chunks = total_chunks
        self._disk_writer = disk_writer
        self._stop_event = stop_event
        self._hash_spec = hash_spec
        self._progress = progress
        self._condition = condition

        self._hash_obj = hashlib.new(hash_spec.algo, usedforsecurity=False) if hash_spec else None
        self._thread: threading.Thread | None = None
        self._verified: bool | None = None
        self._computed_hash: str | None = None

    # Public interface

    @property
    def has_work(self) -> bool:
        """Returns True if hashing was requested, otherwise False."""
        return self._hash_spec is not None

    @property
    def computed_hash(self) -> str | None:
        """Returns the computed hex digest, or None if not yet available."""
        return self._computed_hash

    def start(self) -> None:
        """Starts the background hashing thread."""
        if not self.has_work:
            return
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Stops the hashing thread and waits for its termination."""
        if self._thread is not None:
            # Notify condition if we are waiting on it
            if self._condition is not None:
                with self._condition:
                    self._condition.notify_all()
            
            # Wait for the thread to finish, allowing KeyboardInterrupt
            while self._thread.is_alive():
                self._thread.join(timeout=1.0)

    def finalize(self) -> bool:
        """Validates the computed hash against the expected checksum.

        Returns:
            True if the checksum matches or no checksum verification was
            requested, otherwise False.

        Raises:
            IncompleteHashError: If the background hashing thread failed to complete.
        """
        if not self.has_work:
            return True

        if self._verified is None:
            raise IncompleteHashError("Background hashing thread failed to complete verification.")

        return self._verified

    # Internal threading

    def _run(self) -> None:
        """Opens the file and begins sequential hashing of chunks."""
        try:
            os.nice(-5)  # Raise this thread's scheduling priority to avoid starvation
        except (AttributeError, PermissionError, OSError):
            pass  # No-op on Windows or when unprivileged
        fd = os.open(self._filename, os.O_RDONLY)
        try:
            self._hash_all_chunks(fd)
        finally:
            os.close(fd)

    def _hash_all_chunks(self, fd: int) -> None:
        """Loops through all chunks, hashing them sequentially as they become available on disk.

        Args:
            fd: File descriptor of the target file opened for reading.
        """
        current_chunk = 0

        while current_chunk < self._total_chunks and not self._stop_event.is_set():
            if self._disk_writer.is_on_disk(current_chunk):
                self._hash_single_chunk(fd, current_chunk)
                if self._progress is not None:
                    self._progress.update_hashed(current_chunk)
                current_chunk += 1
            else:
                if self._condition is not None:
                    with self._condition:
                        while not self._disk_writer.is_on_disk(current_chunk) and not self._stop_event.is_set():
                            self._condition.wait(timeout=1.0)
                else:
                    time.sleep(0.5)

        if current_chunk == self._total_chunks and not self._stop_event.is_set():
            self._verify_hash()

    def _hash_single_chunk(self, fd: int, chunk_index: int) -> None:
        """Reads and hashes a single chunk from the file.

        Args:
            fd: File descriptor of the target file opened for reading.
            chunk_index: Index of the chunk to read and hash.
        """
        if self._hash_obj is None:
            raise RuntimeError("Hash object not initialized.")

        start = chunk_index * self._chunk_size
        length = min(self._chunk_size, self._total_size - start)
        
        try:
            chunk_data = self._disk_writer.read_chunk(start, length)
            if chunk_data is not None:
                self._hash_obj.update(chunk_data)
                return
        except (AttributeError, TypeError, ValueError):
            # Fall back to disk read if writer doesn't support read_chunk, returns a non-bytes object, or mmap is closed
            pass

        hash_chunk(fd, start, length, self._hash_obj, self._stop_event)

    def _verify_hash(self) -> None:
        """Computes the final hash and compares it to the expected checksum."""
        if self._hash_spec is None:
            raise RuntimeError("Hash spec not initialized.")
        if self._hash_obj is None:
            raise RuntimeError("Hash object not initialized.")

        digest = self._hash_obj.hexdigest()
        self._computed_hash = digest

        if self._hash_spec.expected is None:
            self._verified = True
        else:
            self._verified = digest.lower() == self._hash_spec.expected.lower()

def hash_chunk(fd: int, start: int, length: int, hash_obj: Any, stop_event: threading.Event | None = None) -> None:
    """Reads and hashes a specific file segment from a file descriptor."""
    os.lseek(fd, start, os.SEEK_SET)
    if hasattr(os, "posix_fadvise"):
        try:
            fadv_sequential = getattr(os, "POSIX_FADV_SEQUENTIAL", 2)
            os.posix_fadvise(fd, start, length, fadv_sequential)
        except OSError:
            pass
    bytes_read = 0
    while bytes_read < length:
        if stop_event is not None and stop_event.is_set():
            break
        read_size = min(_READ_CHUNK, length - bytes_read)
        data = os.read(fd, read_size)
        if not data:
            break
        hash_obj.update(data)
        bytes_read += len(data)


def verify_file(
    filename: str,
    hash_spec: HashSpec,
    progress: ReportsProgress | None = None,
    chunk_size: int = 1024 * 1024,
) -> tuple[bool, str]:
    """Verifies a fully downloaded file on disk against a checksum.

    Args:
        filename: Path to the file to verify.
        hash_spec: Hash configuration containing the algorithm and expected checksum.
        progress: Optional progress reporter to notify as chunks are hashed.
        chunk_size: Size of chunks to read into memory at once.

    Returns:
        A tuple containing:
        - True if the file matches the expected hash or no expected hash was provided, False otherwise.
        - The computed hex digest.

    Raises:
        FileNotFoundError: If the file does not exist.
    """
    total_size = os.path.getsize(filename)
    total_chunks = (total_size + chunk_size - 1) // chunk_size

    if progress is not None:
        progress.set_overlay(" VERIFYING ", color="blue")

    if hasattr(hashlib, "file_digest"):
        with open(filename, "rb") as f:
            hash_obj = hashlib.file_digest(f, lambda: hashlib.new(hash_spec.algo, usedforsecurity=False))
        if progress is not None:
            # Fake the progress since file_digest consumes everything in C
            for chunk_index in range(total_chunks):
                progress.update_hashed(chunk_index)
    else:
        hash_obj = hashlib.new(hash_spec.algo, usedforsecurity=False)
        fd = os.open(filename, os.O_RDONLY)
        try:
            for chunk_index in range(total_chunks):
                start = chunk_index * chunk_size
                length = min(chunk_size, total_size - start)
                hash_chunk(fd, start, length, hash_obj)
                if progress is not None:
                    progress.update_hashed(chunk_index)
        finally:
            os.close(fd)

    digest = hash_obj.hexdigest()
    if hash_spec.expected is None:
        is_valid = True
    else:
        is_valid = digest.lower() == hash_spec.expected.lower()
        
    if progress is not None:
        if is_valid:
            progress.set_overlay(" CHECKSUM OK ", success=True, color="blue")
        else:
            progress.set_overlay(" CHECKSUM INVALID ", success=False, color="red")
            
    return is_valid, digest

