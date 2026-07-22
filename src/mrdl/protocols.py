from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Literal, Protocol, runtime_checkable

if TYPE_CHECKING:
    from mrdl.types import FileMetadata


@runtime_checkable
class WritesChunks(Protocol):
    """Protocol for components handling concurrent, thread-safe disk writing."""

    @property
    def error(self) -> Exception | None:
        """Returns the fatal exception encountered by the writer, if any."""
        ...

    def start(self) -> None:
        """Starts the chunk writing worker or process."""
        ...

    async def write(self, offset: int, data: bytes | bytearray | memoryview) -> None:
        """Writes data to disk at the specified file offset."""
        ...

    async def mark_complete(self, chunk_index: int) -> None:
        """Marks a specific chunk index as successfully completed on disk."""
        ...

    def is_on_disk(self, chunk_index: int) -> bool:
        """Returns True if the specified chunk is safely written on disk."""
        ...

    def stop(self) -> None:
        """Stops the chunk writing process, completing pending writes."""
        ...

    def flush(self) -> None:
        """Flushes written memory or buffered chunks to disk."""
        ...

    def read_chunk(self, offset: int, length: int) -> bytes | bytearray | memoryview | None:
        """Reads a chunk of data from memory/disk if supported directly by the writer."""
        ...

    async def truncate(self, size: int) -> None:
        """Truncates the file to the specified size."""
        ...



@runtime_checkable
class VerifiesIntegrity(Protocol):
    """Protocol for components calculating and validating checksums."""

    @property
    def has_work(self) -> bool:
        """Returns True if hashing was requested, otherwise False."""
        ...

    @property
    def computed_hash(self) -> str | None:
        """Returns the hex digest after hashing completes, or None."""
        ...

    def start(self) -> None:
        """Starts the hashing thread or process."""
        ...

    def finalize(self) -> bool:
        """Computes the final hash and verifies it against the expected checksum."""
        ...

    def stop(self) -> None:
        """Stops the hashing thread or process."""
        ...


@runtime_checkable
class PersistsState(Protocol):
    """Protocol for components persisting and loading download state."""

    def load(self) -> dict | None:
        """Loads and returns the saved state dictionary, or None."""
        ...

    def save(self, state: dict) -> None:
        """Saves the current download state dictionary."""
        ...

    def clear(self) -> None:
        """Clears/deletes the persistent state storage."""
        ...

    def validate_for_resume(
        self,
        saved_state: dict,
        metadata: FileMetadata,
        chunk_size: int,
    ) -> bool:
        """Validates if saved state matches the remote file metadata for resuming."""
        ...

    def build_fresh_state(self, metadata: FileMetadata, chunk_size: int) -> dict:
        """Constructs a new state dictionary for starting a fresh download."""
        ...


@runtime_checkable
class ReportsProgress(Protocol):
    """Protocol for components reporting download progress to the user."""

    def start(
        self,
        total_bytes: int,
        filename: str,
        chunk_size: int,
        completed_chunks: set[int] | None = None,
        mode: Literal["download", "verify"] = "download",
    ) -> None:
        """Initializes and displays the progress tracker."""
        ...

    def update(self, bytes_downloaded: int, chunk_index: int | None = None) -> None:
        """Updates the progress tracker with newly downloaded bytes."""
        ...

    def update_hashed(self, chunk_index: int) -> None:
        """Updates the progress tracker that a chunk has been verified/hashed."""
        ...

    def close(self) -> None:
        """Closes the progress tracker."""
        ...

    def log(self, message: str) -> None:
        """Logs a message safely without breaking the progress tracker layout."""
        ...

    def set_overlay(self, text: str, success: bool = True, color: str | None = None) -> None:
        """Sets the state text to overlay on the progress tracker."""
        ...

    def set_throttled(self, is_throttled: bool) -> None:
        """Sets whether the download is currently throttled."""
        ...


@runtime_checkable
class ConsumesTokens(Protocol):
    """Protocol for rate-limiters enforcing bandwidth throttling."""

    @property
    def is_active(self) -> bool:
        """Returns True if throttling is currently active."""
        ...

    async def consume(self, n_bytes: int) -> None:
        """Consumes tokens representing a number of bytes, blocking if over limit."""
        ...


@runtime_checkable
class ProbesMetadata(Protocol):
    """Protocol for components that resolve file metadata from a download source.

    Implementations probe one or more sources (e.g. HTTP mirrors, .torrent files)
    and return unified FileMetadata describing the target file.
    """

    async def probe(self, urls: list[str]) -> FileMetadata:
        """Probes the given sources and returns file metadata.

        Args:
            urls: Source identifiers to probe (e.g. mirror URLs).

        Returns:
            FileMetadata describing the target file.

        Raises:
            FileNotFoundError: If no source responds with valid metadata.
            ValueError: If no sources are provided.
        """
        ...


@runtime_checkable
class FetchesChunks(Protocol):
    """Protocol for components that download individual file chunks.

    Implementations handle transport-specific details (HTTP range requests,
    BitTorrent piece retrieval, etc.) and write fetched data to a disk writer.
    """

    async def fetch(self, chunk_idx: int) -> int:
        """Downloads a single chunk and writes it to disk.

        Args:
            chunk_idx: Index of the chunk to download.

        Returns:
            Number of bytes written.

        Raises:
            FetchError: If the download fails for any transport-specific reason.
            StoppedException: If the download was intentionally cancelled.
        """
        ...


@runtime_checkable
class TracksHealth(Protocol):
    """Protocol for components tracking source health and ban state.

    Implementations decide when a source should be temporarily banned
    based on failure patterns (slow speed, HTTP errors, etc.).
    """

    def is_banned(self, source_id: str) -> bool:
        """Returns True if the source is currently within its ban window.

        Args:
            source_id: Identifier for the source (e.g. mirror URL).
        """
        ...

    def record_failure(self, error: Exception, source_id: str) -> None:
        """Records a failure and potentially bans the source.

        Args:
            error: The exception raised during the chunk download.
            source_id: Identifier for the source (e.g. mirror URL).
        """
        ...

    def get_active_count(self, sources: Sequence[str]) -> int:
        """Returns the number of sources that are currently unbanned.

        Args:
            sources: Sequence of source identifiers to check.
        """
        ...
