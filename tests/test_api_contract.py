import threading
import time
from pathlib import Path
import hashlib
import os

import pytest
import asyncio
from werkzeug.wrappers import Response  # pyrefly: ignore [missing-import]
from typing import Literal
from unittest.mock import patch

from mrdl.downloader import Downloader
from mrdl.types import DownloadConfig, DownloadState


class MockProgressTracker:
    def __init__(self):
        self.started = False
        self.updates = []
        self.closed = False
        
    def start(self, total_bytes: int, filename: str, chunk_size: int, completed_chunks: set[int] | None = None, mode: Literal["download", "verify"] = "download") -> None:
        self.started = True
        
    def update(self, bytes_downloaded: int, chunk_index: int | None = None) -> None:
        self.updates.append((bytes_downloaded, chunk_index))
        
    def update_hashed(self, chunk_index: int) -> None:
        pass
        
    def close(self) -> None:
        self.closed = True
        
    def log(self, message: str) -> None:
        pass
        
    def set_overlay(self, text: str, success: bool = True, color: str | None = None) -> None:
        pass

    def set_throttled(self, is_throttled: bool) -> None:
        pass


class MockStateManager:
    def __init__(self):
        self.state = None
        self.cleared = False
        
    def load(self) -> dict | None:
        return self.state
        
    def save(self, state: dict) -> None:
        self.state = state
        
    def clear(self) -> None:
        self.cleared = True
        self.state = None
        
    def validate_for_resume(self, saved_state: dict, metadata, chunk_size: int) -> bool:
        return True
        
    def build_fresh_state(self, metadata, chunk_size: int) -> dict:
        return {"file_size": metadata.total_size, "completed_chunks": []}


@pytest.mark.asyncio
async def test_dependency_injection(httpserver, tmp_path: Path):
    """Test that custom dependency injections (progress, state) are properly invoked."""
    content = os.urandom(1024 * 1024)
    expected_hash = hashlib.sha256(content).hexdigest()
    
    httpserver.expect_request("/test.bin").respond_with_data(content, headers={"Accept-Ranges": "bytes"})
    
    output_file = tmp_path / "api_out.bin"
    config = DownloadConfig(
        urls=[httpserver.url_for("/test.bin")],
        filename=str(output_file),
        threads_per_mirror=2,
        chunk_size=256 * 1024,
        checksum=f"sha256:{expected_hash}"
    )
    
    mock_progress = MockProgressTracker()
    mock_state = MockStateManager()
    
    downloader = Downloader(
        config=config,
        progress=mock_progress,
        state_manager=mock_state
    )
    
    result = await downloader.start()
    
    assert result.status == DownloadState.COMPLETED
    assert result.hash_matched is True
    assert result.computed_hash == expected_hash
    
    # Verify injected progress tracker was used
    assert mock_progress.started is True
    assert len(mock_progress.updates) > 0
    assert mock_progress.closed is True
    
    # Verify injected state manager was used (and cleared upon completion)
    assert mock_state.cleared is True


@pytest.mark.asyncio
async def test_programmatic_stop_resume(httpserver, tmp_path: Path):
    """Test the Downloader's API for stopping and resuming dynamically."""
    content = os.urandom(1024 * 1024)
    
    def slow_handler(request):
        def generate():
            chunk = 128 * 1024
            for i in range(0, len(content), chunk):
                time.sleep(0.05)
                yield content[i:i+chunk]
        return Response(generate(), direct_passthrough=True, headers={"Accept-Ranges": "bytes", "Content-Length": str(len(content))})
    
    httpserver.expect_request("/slow_api.bin").respond_with_handler(slow_handler)
    
    output_file = tmp_path / "api_pause_out.bin"
    config = DownloadConfig(
        urls=[httpserver.url_for("/slow_api.bin")],
        filename=str(output_file),
        threads_per_mirror=2,
        chunk_size=128 * 1024,
        min_speed_kbps=1  # Prevent timeout
    )
    
    downloader = Downloader(config)
    
    def stop_later():
        time.sleep(0.2)
        downloader.stop()
        
    t = threading.Thread(target=stop_later)
    t.start()
    
    result1 = await downloader.start()
    t.join()
    
    assert result1.status == DownloadState.PAUSED
    
    # Now resume the download with a new Downloader instance
    downloader2 = Downloader(config)
    result2 = await downloader2.start()
    
    assert result2.status == DownloadState.COMPLETED
    assert output_file.read_bytes() == content
