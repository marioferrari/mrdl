"""Tests for single vs multi-mirror minimum speed enforcement behavior."""

import pytest
import asyncio
import time
from unittest.mock import AsyncMock, MagicMock

from mrdl.cli import parse_args
from mrdl.fetcher import ChunkFetcher, FetcherConfig
from mrdl.mirror_health import MirrorHealthTracker
from mrdl.exceptions import FetchError
from mrdl.types import DownloadConfig, FileMetadata, SlowMirrorException


def test_download_config_default_min_speed():
    """DownloadConfig should default min_speed_kbps to 0.0 (disabled)."""
    config = DownloadConfig(urls="http://example.com/file.bin", filename="file.bin")
    assert config.min_speed_kbps == 0.0


def test_cli_parse_args_default_min_speed():
    """CLI parser should default --min-speed to 0."""
    args = parse_args(["http://example.com/file.bin", "-o", "file.bin"])
    assert args.min_speed == 0


def test_mirror_health_get_active_count():
    """MirrorHealthTracker should correctly count unbanned sources."""
    health = MirrorHealthTracker()
    sources = ["http://m1.com", "http://m2.com"]

    assert health.get_active_count(sources) == 2

    # Ban m1
    health.record_failure(SlowMirrorException("too slow"), "http://m1.com")
    assert health.get_active_count(sources) == 1

    # Ban m2
    health.record_failure(SlowMirrorException("too slow"), "http://m2.com")
    assert health.get_active_count(sources) == 0


@pytest.mark.asyncio
async def test_single_mirror_bypasses_min_speed():
    """Single mirror download should bypass min_speed checks even if min_speed_kbps > 0."""
    health = MirrorHealthTracker()
    sources = ["http://single-mirror.com"]
    metadata = FileMetadata(total_size=2000, accepts_ranges=True)

    writer = AsyncMock()
    writer.write = AsyncMock()
    writer.mark_complete = AsyncMock()
    progress = MagicMock()
    stop_event = asyncio.Event()

    fetcher_config = FetcherConfig(
        chunk_size=1000,
        min_speed_kbps=5000.0,  # Requires high speed (5 MB/s)
        speed_grace_period=0.01,  # Short grace period for test
        health=health,
        sources=sources,
    )

    data_chunk = b"X" * 1000

    async def mock_iter_any():
        yield data_chunk
        # Sleep to simulate slow transmission beyond grace period
        await asyncio.sleep(0.05)
        yield data_chunk

    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()
    mock_response.headers = {}
    mock_response.content.iter_any = mock_iter_any

    mock_session = MagicMock()
    mock_session.get.return_value.__aenter__ = AsyncMock(return_value=mock_response)

    fetcher = ChunkFetcher(
        session=mock_session,
        mirror_url="http://single-mirror.com",
        metadata=metadata,
        writer=writer,
        progress=progress,
        stop_event=stop_event,
        config=fetcher_config,
    )

    # Should complete without raising SlowMirrorException because len(sources) <= 1
    await fetcher.fetch(chunk_idx=0)


@pytest.mark.asyncio
async def test_multi_mirror_enforces_min_speed():
    """Multi-mirror download should enforce min_speed checks when multiple active mirrors exist."""
    health = MirrorHealthTracker()
    sources = ["http://m1.com", "http://m2.com"]
    metadata = FileMetadata(total_size=2000, accepts_ranges=True)

    writer = AsyncMock()
    writer.write = AsyncMock()
    writer.mark_complete = AsyncMock()
    progress = MagicMock()
    stop_event = asyncio.Event()

    fetcher_config = FetcherConfig(
        chunk_size=1000,
        min_speed_kbps=5000.0,  # Requires high speed (5 MB/s)
        speed_grace_period=0.0,
        health=health,
        sources=sources,
    )

    data_chunk = b"X" * 1000

    async def mock_iter_any():
        yield data_chunk
        await asyncio.sleep(0.05)
        yield data_chunk

    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()
    mock_response.headers = {}
    mock_response.content.iter_any = mock_iter_any

    mock_session = MagicMock()
    mock_session.get.return_value.__aenter__ = AsyncMock(return_value=mock_response)

    fetcher = ChunkFetcher(
        session=mock_session,
        mirror_url="http://m1.com",
        metadata=metadata,
        writer=writer,
        progress=progress,
        stop_event=stop_event,
        config=fetcher_config,
    )

    # Should raise FetchError wrapping SlowMirrorException because multiple active mirrors exist
    with pytest.raises((FetchError, SlowMirrorException)):
        await fetcher.fetch(chunk_idx=0)


@pytest.mark.asyncio
async def test_multi_mirror_fallback_to_single_bypasses_speed():
    """When all but 1 mirror are banned, remaining active mirror bypasses min_speed checks."""
    health = MirrorHealthTracker()
    sources = ["http://m1.com", "http://m2.com"]
    metadata = FileMetadata(total_size=2000, accepts_ranges=True)

    # Ban m1 so only m2 is active
    health.record_failure(Exception("network error"), "http://m1.com")
    assert health.get_active_count(sources) == 1

    writer = AsyncMock()
    writer.write = AsyncMock()
    writer.mark_complete = AsyncMock()
    progress = MagicMock()
    stop_event = asyncio.Event()

    fetcher_config = FetcherConfig(
        chunk_size=1000,
        min_speed_kbps=5000.0,
        speed_grace_period=0.01,
        health=health,
        sources=sources,
    )

    data_chunk = b"X" * 1000

    async def mock_iter_any():
        yield data_chunk
        await asyncio.sleep(0.05)
        yield data_chunk

    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()
    mock_response.headers = {}
    mock_response.content.iter_any = mock_iter_any

    mock_session = MagicMock()
    mock_session.get.return_value.__aenter__ = AsyncMock(return_value=mock_response)

    fetcher = ChunkFetcher(
        session=mock_session,
        mirror_url="http://m2.com",
        metadata=metadata,
        writer=writer,
        progress=progress,
        stop_event=stop_event,
        config=fetcher_config,
    )

    # Should complete cleanly without raising SlowMirrorException since active mirrors == 1
    await fetcher.fetch(chunk_idx=0)

