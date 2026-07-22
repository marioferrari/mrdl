import pytest
import asyncio
import aiohttp
from unittest.mock import AsyncMock, patch, MagicMock
from mrdl.fetcher import ChunkFetcher, FetcherConfig
from mrdl.types import FileMetadata


@pytest.mark.asyncio
async def test_chunk_fetcher_rolls_back_progress_on_error():
    # Patch _FLUSH_THRESHOLD to a small value so we flush and update progress immediately
    with patch("mrdl.fetcher._FLUSH_THRESHOLD", 10):
        session = MagicMock()
        request_context_manager = AsyncMock()
        response_mock = AsyncMock()
        request_context_manager.__aenter__.return_value = response_mock
        session.get.return_value = request_context_manager
        response_mock.raise_for_status = MagicMock()
        response_mock.headers = {"Content-Length": "100"}
        
        # Async generator that yields 20 bytes, then raises an exception
        async def mock_iter_any():
            yield b"12345678901234567890"  # 20 bytes (exceeds threshold of 10)
            raise aiohttp.ClientPayloadError("Connection dropped")
            
        response_mock.content.iter_any = mock_iter_any
        
        metadata = FileMetadata(total_size=100, accepts_ranges=True)
        writer = AsyncMock()
        progress = MagicMock()
        stop_event = asyncio.Event()
        config = FetcherConfig(chunk_size=100, min_speed_kbps=0, speed_grace_period=10)
        
        fetcher = ChunkFetcher(
            session=session,
            mirror_url="http://example.com/file",
            metadata=metadata,
            writer=writer,
            progress=progress,
            stop_event=stop_event,
            config=config,
        )
        
        from mrdl.exceptions import FetchError
        with pytest.raises(FetchError, match="Connection dropped"):
            await fetcher.fetch(chunk_idx=0)
            
        # The fetcher should have flushed 20 bytes to the writer,
        # called progress.update(20), and upon catching the exception,
        # it should have called progress.update(-20) to roll back.
        progress.update.assert_any_call(20)
        progress.update.assert_any_call(-20)


@pytest.mark.asyncio
async def test_chunk_fetcher_truncates_on_completion_for_unknown_size():
    with patch("mrdl.fetcher._FLUSH_THRESHOLD", 10):
        session = MagicMock()
        request_context_manager = AsyncMock()
        response_mock = AsyncMock()
        request_context_manager.__aenter__.return_value = response_mock
        session.get.return_value = request_context_manager
        response_mock.raise_for_status = MagicMock()
        response_mock.headers = {}  # Unknown size
        
        async def mock_iter_any():
            yield b"12345678901234567890"  # 20 bytes
            
        response_mock.content.iter_any = mock_iter_any
        
        metadata = FileMetadata(total_size=-1, accepts_ranges=False)
        writer = AsyncMock()
        progress = MagicMock()
        stop_event = asyncio.Event()
        config = FetcherConfig(chunk_size=100, min_speed_kbps=0, speed_grace_period=10)
        
        fetcher = ChunkFetcher(
            session=session,
            mirror_url="http://example.com/file",
            metadata=metadata,
            writer=writer,
            progress=progress,
            stop_event=stop_event,
            config=config,
        )
        
        await fetcher.fetch(chunk_idx=0)
        
        # Should have called truncate with the total bytes written (20)
        writer.truncate.assert_called_once_with(20)


@pytest.mark.asyncio
async def test_chunk_fetcher_small_chunk_size_does_not_overflow_buffer():
    """Verifies that chunk_size smaller than 16 MiB does not raise BufferError when streaming."""
    session = MagicMock()
    request_context_manager = AsyncMock()
    response_mock = AsyncMock()
    request_context_manager.__aenter__.return_value = response_mock
    session.get.return_value = request_context_manager
    response_mock.raise_for_status = MagicMock()
    response_mock.headers = {}  # Unknown size (no Content-Length)
    
    # Yield 4 blocks of 64 KiB (256 KiB total)
    async def mock_iter_any():
        for _ in range(4):
            yield b"X" * (64 * 1024)
            
    response_mock.content.iter_any = mock_iter_any
    
    metadata = FileMetadata(total_size=-1, accepts_ranges=False)
    writer = AsyncMock()
    progress = MagicMock()
    stop_event = asyncio.Event()
    # Configure chunk_size = 64 KiB (smaller than _FLUSH_THRESHOLD of 16 MiB)
    config = FetcherConfig(chunk_size=64 * 1024, min_speed_kbps=0, speed_grace_period=10)
    
    fetcher = ChunkFetcher(
        session=session,
        mirror_url="http://example.com/file",
        metadata=metadata,
        writer=writer,
        progress=progress,
        stop_event=stop_event,
        config=config,
    )
    
    # Should complete without BufferError
    bytes_written = await fetcher.fetch(chunk_idx=0)
    assert bytes_written == 256 * 1024


