import asyncio
import time
import unittest
from unittest.mock import patch
import pytest

from mrdl.throttle import TokenBucketThrottle


class MockTime:
    def __init__(self):
        self.time = 0.0
        self.lock = asyncio.Lock()

    def monotonic(self):
        return self.time

    async def sleep(self, duration):
        async with self.lock:
            self.time += duration


class TestTokenBucketThrottle(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.mock_time = MockTime()
        self.patcher_monotonic = patch("mrdl.throttle.time.monotonic", side_effect=self.mock_time.monotonic)
        self.patcher_sleep = patch("mrdl.throttle.asyncio.sleep", side_effect=self.mock_time.sleep)
        self.patcher_monotonic.start()
        self.patcher_sleep.start()

    def tearDown(self):
        self.patcher_monotonic.stop()
        self.patcher_sleep.stop()

    async def test_negative_rate_raises(self):
        with pytest.raises(ValueError):
            TokenBucketThrottle(rate_kbps=-1)

    async def test_zero_rate_raises(self):
        with pytest.raises(ValueError):
            TokenBucketThrottle(rate_kbps=0)

    async def test_valid_rate_constructs(self):
        throttle = TokenBucketThrottle(rate_kbps=512)
        assert throttle is not None

    async def test_consume_zero_bytes_is_noop(self):
        throttle = TokenBucketThrottle(rate_kbps=1)  # very slow rate
        start = self.mock_time.monotonic()
        await throttle.consume(0)  # should not block
        elapsed = self.mock_time.monotonic() - start
        assert elapsed == 0.0

    async def test_consume_negative_bytes_is_noop(self):
        throttle = TokenBucketThrottle(rate_kbps=1)
        start = self.mock_time.monotonic()
        await throttle.consume(-100)
        elapsed = self.mock_time.monotonic() - start
        assert elapsed == 0.0

    async def test_does_not_block_under_rate(self):
        # 1024 KB/s = 1 MB/s; consume 512 bytes — well under budget at start.
        throttle = TokenBucketThrottle(rate_kbps=1024)
        start = self.mock_time.monotonic()
        await throttle.consume(512)
        elapsed = self.mock_time.monotonic() - start
        assert elapsed == 0.0

    async def test_blocks_when_over_rate(self):
        # 8 KB/s rate; consume 16 KB in one shot (2 seconds worth of data).
        # The bucket starts full at 8 KB so 8 KB are immediately available;
        # the remaining 8 KB should require exactly 1 second of sleeping.
        rate_kbps = 8
        throttle = TokenBucketThrottle(rate_kbps=rate_kbps, burst_seconds=1.0)

        bytes_to_consume = rate_kbps * 1024 * 2  # 16 KB = 2 s of data

        start = self.mock_time.monotonic()
        await throttle.consume(bytes_to_consume)
        elapsed = self.mock_time.monotonic() - start

        assert elapsed == 1.0

    async def test_rate_is_approximately_respected(self):
        # 64 KB/s; consume 64 KB five times and measure total elapsed time.
        # Expect exactly 4.0 s (after the first free bucket).
        rate_kbps = 64
        throttle = TokenBucketThrottle(rate_kbps=rate_kbps, burst_seconds=1.0)
        chunk = rate_kbps * 1024  # exactly 1 s worth of data

        iterations = 5
        start = self.mock_time.monotonic()
        for _ in range(iterations):
            await throttle.consume(chunk)
        elapsed = self.mock_time.monotonic() - start

        # First consume is free (full bucket). Subsequent 4 take exactly 4.0 s.
        assert elapsed == 4.0

    async def test_task_safety_concurrent_consumers(self):
        """Multiple tasks sharing a throttle must safely serialize."""
        rate_kbps = 128
        throttle = TokenBucketThrottle(rate_kbps=rate_kbps, burst_seconds=1.0)
        chunk = rate_kbps * 1024  # 1 s of data per task
        n_tasks = 4
        errors = []

        async def worker():
            try:
                await throttle.consume(chunk)
            except Exception as e:
                errors.append(e)

        tasks = [asyncio.create_task(worker()) for _ in range(n_tasks)]
        start = self.mock_time.monotonic()
        await asyncio.gather(*tasks)
        elapsed = self.mock_time.monotonic() - start

        assert errors == [], f"Worker tasks raised exceptions: {errors}"
        # n_tasks chunks at 1 s each. First is free; remaining n-1 are queued.
        # The exact total elapsed mock time should be exactly 3.0s.
        assert elapsed == 3.0

    async def test_no_race_condition_on_tokens(self):
        """Tokens should never go negative (basic race condition check)."""
        throttle = TokenBucketThrottle(rate_kbps=256)
        small_chunk = 1024  # 1 KB per call

        async def burster():
            for _ in range(50):
                await throttle.consume(small_chunk)

        tasks = [asyncio.create_task(burster()) for _ in range(8)]
        await asyncio.gather(*tasks)

    async def test_uncapped_state(self):
        throttle = TokenBucketThrottle(rate_kbps=None)
        start = self.mock_time.monotonic()
        await throttle.consume(1024 * 1024 * 100) # consume 100MB instantly
        elapsed = self.mock_time.monotonic() - start
        assert elapsed == 0.0

    async def test_update_rate_live(self):
        # Start at 10 KB/s, consume 10 KB (free from initial capacity of 1.0s)
        throttle = TokenBucketThrottle(rate_kbps=10, burst_seconds=1.0)
        await throttle.consume(10240)
        
        # Increase rate to 100 KB/s
        throttle.update_rate(100)
        start = self.mock_time.monotonic()
        
        # The bucket is currently empty (0 tokens). 
        # Consuming 100 KB at the new rate of 100 KB/s should take exactly 1.0 seconds to refill.
        await throttle.consume(102400)
        elapsed = self.mock_time.monotonic() - start
        assert elapsed == 1.0
        
        # Disable throttling
        throttle.update_rate(None)
        start = self.mock_time.monotonic()
        await throttle.consume(1024 * 1024 * 10) # 10MB
        elapsed = self.mock_time.monotonic() - start
        assert elapsed == 0.0

if __name__ == "__main__":
    unittest.main()
