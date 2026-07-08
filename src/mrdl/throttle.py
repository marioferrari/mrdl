"""Thread-safe token-bucket rate limiter."""

from __future__ import annotations

import asyncio
import time


class TokenBucketThrottle:
    """Rate limiter based on the token-bucket algorithm.

    Accumulates tokens at a configured rate up to a maximum capacity
    and blocks tasks requesting more tokens than available.
    """

    def __init__(self, rate_kbps: int | None, burst_seconds: float = 0.1) -> None:
        """Initializes the TokenBucketThrottle.

        Args:
            rate_kbps: Sustained rate limit in KB/s. If None, throttling is disabled.
            burst_seconds: Maximum burst duration in seconds. Defaults to 0.1 for smoother throttling.

        Raises:
            ValueError: If rate_kbps is negative or zero.
        """
        self._burst_seconds = burst_seconds
        self._lock = asyncio.Lock()
        
        self._rate_bps: float | None = None
        self._capacity: float = 0.0
        self._tokens: float = 0.0
        self._last_refill: float = 0.0
        
        self.update_rate(rate_kbps)

    def update_rate(self, rate_kbps: int | None) -> None:
        """Updates the rate limit dynamically.
        
        Args:
            rate_kbps: The new sustained rate limit in KB/s, or None to uncap.
        """
        if rate_kbps is not None and rate_kbps <= 0:
            raise ValueError(f"rate_kbps must be positive or None, got {rate_kbps}")
            
        # We don't acquire the async lock here because this might be called from
        # synchronous contexts like __init__ or synchronous update handlers.
        # It's a best-effort atomic update, which is usually fine for rate limits.
        if self._rate_bps is not None:
            self._refill()
            
        if rate_kbps is None:
            self._rate_bps = None
            return
            
        new_rate_bps = rate_kbps * 1024
        new_capacity = new_rate_bps * self._burst_seconds
        
        if self._rate_bps is None:
            self._tokens = new_capacity
            self._last_refill = time.monotonic()
        else:
            if self._tokens > new_capacity:
                self._tokens = new_capacity
                
        self._rate_bps = new_rate_bps
        self._capacity = new_capacity

    @property
    def is_active(self) -> bool:
        """Returns True if throttling is currently active."""
        return self._rate_bps is not None

    # Public interface

    async def consume(self, n_bytes: int) -> None:
        """Blocks until n_bytes tokens are available, then consumes them.

        Args:
            n_bytes: Number of bytes to consume.
        """
        if n_bytes <= 0 or self._rate_bps is None:
            return

        wait = 0.0
        async with self._lock:
            self._refill()
            self._tokens -= n_bytes
            if self._tokens < 0:
                wait = -self._tokens / (self._rate_bps or 1)

        if wait > 0:
            await asyncio.sleep(wait)

    # Internal helpers

    def _refill(self) -> None:
        """Adds tokens proportional to elapsed time since last refill.
        """
        if self._rate_bps is None:
            return

        now = time.monotonic()
        elapsed = now - self._last_refill
        self._last_refill = now
        self._tokens = min(self._capacity, self._tokens + elapsed * self._rate_bps)
