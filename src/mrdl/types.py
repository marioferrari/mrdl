from __future__ import annotations

import enum
import hashlib
from collections.abc import Sequence
from dataclasses import dataclass


class DownloadState(enum.Enum):
    """Enumeration of possible downloader states."""

    IDLE = "idle"
    PROBING = "probing"
    DOWNLOADING = "downloading"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


VALID_TRANSITIONS: dict[DownloadState, set[DownloadState]] = {
    DownloadState.IDLE: {DownloadState.PROBING, DownloadState.CANCELLED},
    DownloadState.PROBING: {DownloadState.DOWNLOADING, DownloadState.FAILED, DownloadState.CANCELLED},
    DownloadState.DOWNLOADING: {DownloadState.PAUSED, DownloadState.COMPLETED, DownloadState.FAILED, DownloadState.CANCELLED},
    DownloadState.PAUSED: {DownloadState.DOWNLOADING, DownloadState.COMPLETED, DownloadState.CANCELLED},
    DownloadState.COMPLETED: set(),
    DownloadState.FAILED: set(),
    DownloadState.CANCELLED: set(),
}


class InvalidStateTransition(Exception):
    """Exception raised when a state transition is not allowed."""

    def __init__(self, from_state: DownloadState, to_state: DownloadState):
        self.from_state = from_state
        self.to_state = to_state
        super().__init__(f"Cannot transition from {from_state.value} to {to_state.value}")


class SlowMirrorException(Exception):
    """Exception raised when a mirror's download speed drops below the threshold."""

    pass


class DestinationExistsError(Exception):
    """Exception raised when the destination file already exists and overwrite is False."""

    pass


@dataclass
class DownloadConfig:
    """Configuration options for a download session."""

    urls: Sequence[str] | str
    filename: str
    label: str | None = None
    threads_per_mirror: int = 1
    chunk_size: int = 64 * 1024 * 1024  # 64 MiB — halves per-chunk overhead for 100+ GB files
    min_speed_kbps: float = 0.0
    speed_grace_period: float = 10.0
    speed_ema_window: float = 1.0
    speed_update_interval: float = 1.0
    sock_read_timeout: float = 60.0
    sock_connect_timeout: float = 10.0
    checksum: str | None = None
    max_speed_kbps: int | None = None
    max_speed_per_thread_kbps: int | None = None
    overwrite: bool = False
    silent: bool = False
    safe_state_saves: bool = False  # If True, fsync the progress file on every save (useful on NFS/SMB).
    use_mmap: bool = False
    compact: bool = False


@dataclass(frozen=True)
class DownloadResult:
    """The result of a download session."""

    status: DownloadState
    path: str
    hash_matched: bool
    time_taken: float
    error: str | None = None
    computed_hash: str | None = None

@dataclass(frozen=True)
class FileMetadata:
    """Metadata retrieved from the remote mirrors during probing."""

    total_size: int
    accepts_ranges: bool
    etag: str | None = None
    last_modified: str | None = None


@dataclass(frozen=True)
class HashSpec:
    """Configuration for download integrity hashing."""

    algo: str
    expected: str | None

    @classmethod
    def parse(cls, hash_str: str) -> HashSpec:
        """Parses a hash specifier string (e.g. 'sha256' or 'sha256:abc123...').

        Args:
            hash_str: The hash specification string.

        Returns:
            A parsed HashSpec instance.

        Raises:
            ValueError: If the string is empty or contains an unsupported algorithm.
          """
        if not hash_str:
            raise ValueError("hash string must not be empty")

        parts = hash_str.split(":", 1)
        algo = parts[0].strip().lower()
        expected = parts[1].strip() if len(parts) == 2 else None

        if algo not in hashlib.algorithms_available:
            supported = ", ".join(sorted(hashlib.algorithms_guaranteed))
            raise ValueError(
                f"Unsupported hash algorithm '{algo}'. "
                f"Guaranteed-available algorithms: {supported}"
            )

        return cls(algo=algo, expected=expected)
