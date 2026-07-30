import asyncio
import logging
import threading

import pytest

from mrdl.exceptions import FetchError
from mrdl.mirror_health import MirrorHealthTracker
from mrdl.progress import NoOpProgress, ProgressLogHandler
from mrdl.worker_pool import WorkerPool


class FailingFetcher:
    """Mock fetcher that fails with FetchError."""
    async def fetch(self, chunk_idx: int) -> int:
        raise FetchError("HTTP 404 Not Found")


@pytest.mark.asyncio
async def test_worker_pool_logs_fetch_error_at_debug(caplog):
    """Verify transient mirror FetchError is logged at DEBUG severity."""
    caplog.set_level(logging.DEBUG, logger="mrdl.worker_pool")

    sources = ["http://mirror1.test/file"]
    completed_set = set()
    state_lock = threading.Lock()
    stop_event = asyncio.Event()
    pause_event = asyncio.Event()
    pause_event.set()
    stop_event_thread = threading.Event()
    health = MirrorHealthTracker()
    progress = NoOpProgress()

    chunk_queue = asyncio.PriorityQueue()
    await chunk_queue.put((0.0, 0, 0))

    def fetcher_factory(source: str, idx: int):
        return FailingFetcher()

    # Create a worker pool with 1 chunk in priority queue
    pool = WorkerPool(
        sources=sources,
        threads_per_source=1,
        chunk_queue=chunk_queue,
        fetcher_factory=fetcher_factory,
        health=health,
        completed_set=completed_set,
        state_lock=state_lock,
        stop_event=stop_event,
        pause_event=pause_event,
        stop_event_thread=stop_event_thread,
        progress=progress,
    )

    # Let the pool run (it will retry and abort after 5 failures)
    await pool.run()

    # Inspect captured log records
    records = [r for r in caplog.records if r.name == "mrdl.worker_pool"]
    debug_records = [r for r in records if r.levelname == "DEBUG"]

    assert len(debug_records) > 0, "FetchError should be logged at DEBUG level"
    assert "Mirror source http://mirror1.test/file failed with FetchError" in debug_records[0].message


@pytest.mark.asyncio
async def test_consumer_can_suppress_debug_logs(caplog):
    """Verify consumers can suppress DEBUG failover logs by setting logger level to WARNING."""
    caplog.set_level(logging.WARNING, logger="mrdl")

    sources = ["http://mirror1.test/file"]
    completed_set = set()
    state_lock = threading.Lock()
    stop_event = asyncio.Event()
    pause_event = asyncio.Event()
    pause_event.set()
    stop_event_thread = threading.Event()
    health = MirrorHealthTracker()
    progress = NoOpProgress()

    chunk_queue = asyncio.PriorityQueue()
    await chunk_queue.put((0.0, 0, 0))

    def fetcher_factory(source: str, idx: int):
        return FailingFetcher()

    pool = WorkerPool(
        sources=sources,
        threads_per_source=1,
        chunk_queue=chunk_queue,
        fetcher_factory=fetcher_factory,
        health=health,
        completed_set=completed_set,
        state_lock=state_lock,
        stop_event=stop_event,
        pause_event=pause_event,
        stop_event_thread=stop_event_thread,
        progress=progress,
    )

    await pool.run()

    # DEBUG records should be filtered out
    debug_records = [r for r in caplog.records if r.name == "mrdl.worker_pool" and r.levelname == "DEBUG"]
    assert len(debug_records) == 0, "DEBUG records should be suppressed when level is set to WARNING"


def test_progress_log_handler_routes_module_logger():
    """Verify ProgressLogHandler correctly captures standard mrdl log records."""
    logged_messages = []

    class MockMultiProgress:
        def log(self, message: str) -> None:
            logged_messages.append(message)

    mp = MockMultiProgress()
    handler = ProgressLogHandler(mp)  # type: ignore
    handler.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))

    logger = logging.getLogger("mrdl.test_component")
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

    try:
        logger.info("Test message for progress handler")
        assert len(logged_messages) == 1
        assert logged_messages[0] == "[INFO] Test message for progress handler"
    finally:
        logger.removeHandler(handler)
