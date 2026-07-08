"""Internal exceptions for the mrdl download pipeline."""

from __future__ import annotations


class StoppedException(Exception):
    """Raised inside a worker when the stop event fires mid-download.

    This bypasses the retry path entirely — the download was intentionally
    cancelled, not failed.
    """


class IncompleteChunkError(Exception):
    """Raised when the number of bytes received does not match the expected range size."""


class IncompleteHashError(Exception):
    """Raised when the background hashing thread fails to complete verification."""
