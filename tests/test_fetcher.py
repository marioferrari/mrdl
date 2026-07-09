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
        
        with pytest.raises(aiohttp.ClientPayloadError, match="Connection dropped"):
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
