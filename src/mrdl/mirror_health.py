"""Per-mirror health tracking and ban management."""

from __future__ import annotations

import threading
import time
import aiohttp
from mrdl.types import SlowMirrorException


class MirrorHealthTracker:
    """Tracks per-mirror ban state and computes ban durations from failure types.

    Thread-safe. One instance is shared across all worker threads for a download session.
    """

    def __init__(self) -> None:
        self._banned: dict[str, float] = {}
        self._lock = threading.Lock()

    def is_banned(self, url: str) -> bool:
        """Returns True if the mirror is currently within its ban window."""
        with self._lock:
            return time.monotonic() < self._banned.get(url, 0)

    def record_failure(self, error: Exception, url: str) -> None:
        """Bans a mirror for a duration determined by the type of error.

        Args:
            error: The exception raised during the chunk download.
            url: The mirror URL to ban.
        """
        if isinstance(error, SlowMirrorException):
            ban_duration = 120.0
        elif (
            isinstance(error, aiohttp.ClientResponseError)
            and error.status == 429
        ):
            retry_after = error.headers.get("Retry-After") if error.headers else None
            ban_duration = float(retry_after) if retry_after and retry_after.isdigit() else 120.0
        else:
            ban_duration = 60.0

        with self._lock:
            self._banned[url] = time.monotonic() + ban_duration
