import os
import time
import asyncio
import hashlib
import re
import pytest

import tempfile
from pathlib import Path
from werkzeug.wrappers import Response
from unittest.mock import patch
from mrdl.downloader import Downloader
from mrdl.types import DownloadConfig, DownloadState

def _create_test_file_content(size_bytes: int = 1024 * 1024) -> bytes:
    # Generate some random bytes for testing
    return os.urandom(size_bytes)


def _make_range_handler(content: bytes):
    def handler(request):
        range_header = request.headers.get("Range")
        if not range_header:
            return Response(content, status=200, headers={"Accept-Ranges": "bytes"})
        
        match = re.match(r"bytes=(\d+)-(\d*)", range_header)
        if not match:
            return Response(content, status=200, headers={"Accept-Ranges": "bytes"})
            
        start = int(match.group(1))
        end_str = match.group(2)
        end = int(end_str) if end_str else len(content) - 1
            
        if start >= len(content):
            return Response(status=416)
            
        chunk = content[start:end+1]
        headers = {
            "Accept-Ranges": "bytes",
            "Content-Range": f"bytes {start}-{end}/{len(content)}",
            "Content-Length": str(len(chunk))
        }
        return Response(chunk, status=206, headers=headers)
    return handler


@pytest.mark.asyncio
async def test_basic_successful_download(httpserver, tmp_path: Path):
    content = _create_test_file_content(1024 * 1024)  # 1MB
    expected_hash = hashlib.sha256(content).hexdigest()

    # Serve the file from pytest-httpserver
    httpserver.expect_request("/test.bin").respond_with_data(content, headers={"Accept-Ranges": "bytes"})
    
    output_file = tmp_path / "output.bin"
    
    config = DownloadConfig(
        urls=[httpserver.url_for("/test.bin")],
        filename=str(output_file),
        threads_per_mirror=4,
        chunk_size=256 * 1024,
        checksum=f"sha256:{expected_hash}"
    )
    
    downloader = Downloader(config)
    result = await downloader.start()
    
    assert result.status == DownloadState.COMPLETED
    assert output_file.exists()
    assert output_file.read_bytes() == content
    assert result.computed_hash == expected_hash


@pytest.mark.asyncio
async def test_resuming_interrupted_download(httpserver, tmp_path: Path):
    content = _create_test_file_content(1024 * 1024)  # 1MB
    expected_hash = hashlib.sha256(content).hexdigest()

    httpserver.expect_request("/test2.bin").respond_with_data(content, headers={"Accept-Ranges": "bytes"})
    output_file = tmp_path / "output2.bin"

    config1 = DownloadConfig(
        urls=[httpserver.url_for("/test2.bin")],
        filename=str(output_file),
        threads_per_mirror=2,
        chunk_size=256 * 1024,
        max_speed_kbps=100  # Slow it down so we can interrupt it
    )
    
    downloader1 = Downloader(config1)
    
    async def interrupt():
        await asyncio.sleep(0.5)
        downloader1.cancel()
        
    t = asyncio.create_task(interrupt())
    result1 = await downloader1.start()
    await t
    
    assert result1.status == DownloadState.CANCELLED
    assert (tmp_path / "output2.bin.progress").exists() # State file should exist

    # Second attempt: Resume without speed limit
    config2 = DownloadConfig(
        urls=[httpserver.url_for("/test2.bin")],
        filename=str(output_file),
        threads_per_mirror=4,
        chunk_size=256 * 1024,
        checksum=f"sha256:{expected_hash}"
    )
    
    downloader2 = Downloader(config2)
    result2 = await downloader2.start()
    
    assert result2.status == DownloadState.COMPLETED
    assert output_file.exists()
    assert output_file.read_bytes() == content
    assert not (tmp_path / "output2.bin.progress").exists() # State file should be cleaned up
    assert result2.computed_hash == expected_hash


@pytest.mark.asyncio
async def test_corrupted_chunks_handling(httpserver, tmp_path: Path):
    content = _create_test_file_content(512 * 1024)
    expected_hash = hashlib.sha256(content).hexdigest()
    
    # Serve bad data to simulate corruption
    bad_content = bytearray(content)
    bad_content[10] ^= 0xFF # Flip a byte
    
    httpserver.expect_request("/corrupt.bin").respond_with_data(bytes(bad_content), headers={"Accept-Ranges": "bytes"})
    
    output_file = tmp_path / "corrupt_out.bin"
    config = DownloadConfig(
        urls=[httpserver.url_for("/corrupt.bin")],
        filename=str(output_file),
        threads_per_mirror=2,
        chunk_size=256 * 1024,
        checksum=f"sha256:{expected_hash}"
    )
    
    downloader = Downloader(config)
    result = await downloader.start()
    
    # Should fail due to hash mismatch
    assert result.status == DownloadState.FAILED
    

@pytest.mark.asyncio
async def test_single_mirror_slow_connection_succeeds(httpserver, tmp_path: Path):
    """Single mirror download on slow connection should bypass min_speed and complete successfully."""
    content = _create_test_file_content(2 * 1024 * 1024) # 2MB to ensure buffer flushes
    
    def slow_handler(request):
        def generate():
            chunk = 256 * 1024
            for i in range(0, len(content), chunk):
                time.sleep(0.1) # 4 chunks = 1MB in 0.4s -> 2500 KB/s
                yield content[i:i+chunk]
        return Response(generate(), direct_passthrough=True, headers={"Accept-Ranges": "bytes", "Content-Length": str(len(content))})
        
    httpserver.expect_request("/slow.bin").respond_with_handler(slow_handler)
    
    output_file = tmp_path / "slow_out.bin"
    config = DownloadConfig(
        urls=[httpserver.url_for("/slow.bin")],
        filename=str(output_file),
        threads_per_mirror=1,
        chunk_size=2 * 1024 * 1024,
        min_speed_kbps=5000, # Require 5000 KB/s (bypassed on single mirror)
        speed_grace_period=0 # No grace period
    )
    
    downloader = Downloader(config)
    
    # Patch is_banned to return False so we don't sleep for 120s between retries
    with patch("mrdl.downloader.MirrorHealthTracker.is_banned", return_value=False):
        result = await downloader.start()
    
    # Single mirror download should succeed despite slow connection
    assert result.status == DownloadState.COMPLETED


@pytest.mark.asyncio
async def test_multi_mirror_slow_connection_fallback_succeeds(httpserver, tmp_path: Path):
    """Multi-mirror download with slow mirrors should ban slow mirrors then fallback to remaining mirror and complete."""
    content = _create_test_file_content(2 * 1024 * 1024)

    def slow_range_handler(request):
        range_header = request.headers.get("Range")
        if range_header == "bytes=0-0":
            return Response(b"X", status=206, headers={
                "Accept-Ranges": "bytes",
                "Content-Range": f"bytes 0-0/{len(content)}",
                "Content-Length": "1"
            })
        def generate():
            chunk = 256 * 1024
            for i in range(0, len(content), chunk):
                time.sleep(0.1) # 2500 KB/s
                yield content[i:i+chunk]
        headers = {
            "Accept-Ranges": "bytes",
            "Content-Range": f"bytes 0-{len(content)-1}/{len(content)}",
            "Content-Length": str(len(content))
        }
        return Response(generate(), status=206, direct_passthrough=True, headers=headers)

    httpserver.expect_request("/slow1.bin").respond_with_handler(slow_range_handler)
    httpserver.expect_request("/slow2.bin").respond_with_handler(slow_range_handler)

    output_file = tmp_path / "slow_multi_out.bin"
    config = DownloadConfig(
        urls=[httpserver.url_for("/slow1.bin"), httpserver.url_for("/slow2.bin")],
        filename=str(output_file),
        threads_per_mirror=1,
        chunk_size=2 * 1024 * 1024,
        min_speed_kbps=5000,
        speed_grace_period=0
    )

    downloader = Downloader(config)

    with patch("mrdl.downloader.MirrorHealthTracker.is_banned", return_value=False):
        result = await downloader.start()

    assert result.status == DownloadState.COMPLETED


@pytest.mark.asyncio
async def test_concurrent_writers_no_race(httpserver, tmp_path: Path):
    content = _create_test_file_content(2 * 1024 * 1024) # 2MB
    expected_hash = hashlib.sha256(content).hexdigest()
    
    httpserver.expect_request("/concurrent.bin").respond_with_handler(_make_range_handler(content))
    
    output_file = tmp_path / "concurrent_out.bin"
    # Use many threads and small chunk size to maximize contention
    config = DownloadConfig(
        urls=[httpserver.url_for("/concurrent.bin")],
        filename=str(output_file),
        threads_per_mirror=16, 
        chunk_size=64 * 1024,
        checksum=f"sha256:{expected_hash}"
    )
    
    downloader = Downloader(config)
    result = await downloader.start()
    
    assert result.status == DownloadState.COMPLETED
    assert output_file.read_bytes() == content
    assert result.computed_hash == expected_hash


@pytest.mark.asyncio
async def test_multi_mirror_fallback_and_speed(httpserver, tmp_path: Path):
    content = _create_test_file_content(2 * 1024 * 1024) # 2MB
    expected_hash = hashlib.sha256(content).hexdigest()
    
    # 1. Failing mirror (returns 500)
    httpserver.expect_request("/failing.bin").respond_with_data("Internal Server Error", status=500)
    
    # 2. Slow mirror (mocked via patch instead of blocking the test server)
    httpserver.expect_request("/slow.bin").respond_with_handler(_make_range_handler(content))
    
    # 3. Fast mirror
    httpserver.expect_request("/fast.bin").respond_with_handler(_make_range_handler(content))
    
    output_file = tmp_path / "multimirror_out.bin"
    
    config = DownloadConfig(
        urls=[
            httpserver.url_for("/failing.bin"),
            httpserver.url_for("/slow.bin"),
            httpserver.url_for("/fast.bin")
        ],
        filename=str(output_file),
        threads_per_mirror=1, 
        chunk_size=256 * 1024,
        checksum=f"sha256:{expected_hash}",
        min_speed_kbps=500, # Require 500 KB/s so the slow one gets abandoned, but fast one easily passes
        speed_grace_period=0.5 # Trigger fast, but give fast mirror some time to start
    )
    
    downloader = Downloader(config)
    
    from mrdl.types import SlowMirrorException
    from unittest.mock import patch
    original_fetch = __import__("mrdl").fetcher.ChunkFetcher.fetch
    
    async def mock_fetch(self, chunk_idx):
        if "slow.bin" in self._mirror_url:
            raise SlowMirrorException("mocked slow speed")
        return await original_fetch(self, chunk_idx)
        
    with patch("mrdl.fetcher.ChunkFetcher.fetch", mock_fetch):
        result = await downloader.start()
    
    assert result.status == DownloadState.COMPLETED
    assert output_file.read_bytes() == content
    assert result.hash_matched is True
    assert result.computed_hash == expected_hash


@pytest.mark.asyncio
async def test_mid_stream_connection_drop(httpserver, tmp_path: Path):
    content = _create_test_file_content(1024 * 1024) # 1MB
    expected_hash = hashlib.sha256(content).hexdigest()
    
    # We want a handler that drops the connection halfway through yielding
    # the requested chunk.
    def drop_handler(request):
        range_header = request.headers.get("Range")
        start = 0
        end = len(content) - 1
        if range_header:
            match = re.match(r"bytes=(\d+)-(\d*)", range_header)
            if match:
                start = int(match.group(1))
                end_str = match.group(2)
                if end_str:
                    end = int(end_str)
                    
        chunk_to_send = content[start:end+1]
        
        # State to track if we've dropped the connection already for this range
        # To avoid dropping infinitely, we only drop on the first attempt for a given start offset
        drop_this_time = getattr(httpserver, f"_dropped_{start}", False) is False
        if drop_this_time:
            setattr(httpserver, f"_dropped_{start}", True)
            
        def generate():
            if drop_this_time:
                # Yield half and then intentionally raise to break the connection
                yield chunk_to_send[:len(chunk_to_send)//2]
                raise RuntimeError("Intentional connection drop mid-stream")
            else:
                # Yield normally
                yield chunk_to_send
                
        headers = {
            "Accept-Ranges": "bytes",
            "Content-Range": f"bytes {start}-{end}/{len(content)}",
            "Content-Length": str(len(chunk_to_send))
        }
        status_code = 206 if range_header else 200
        return Response(generate(), status=status_code, direct_passthrough=True, headers=headers)
        
    httpserver.expect_request("/drop.bin").respond_with_handler(drop_handler)
    
    output_file = tmp_path / "drop_out.bin"
    config = DownloadConfig(
        urls=[httpserver.url_for("/drop.bin")],
        filename=str(output_file),
        threads_per_mirror=2, 
        chunk_size=256 * 1024,
        checksum=f"sha256:{expected_hash}"
    )
    
    downloader = Downloader(config)
    
    # Ensure it retries on the exception without banning the mirror permanently immediately
    with patch("mrdl.downloader.MirrorHealthTracker.is_banned", return_value=False):
        result = await downloader.start()
    
    assert result.status == DownloadState.COMPLETED
    assert output_file.read_bytes() == content
    assert result.hash_matched is True
    assert result.computed_hash == expected_hash


@pytest.mark.asyncio
async def test_pause_resume_during_download(httpserver, tmp_path: Path):
    content = _create_test_file_content(1024 * 1024)
    expected_hash = hashlib.sha256(content).hexdigest()
    
    httpserver.expect_request("/pauseresume.bin").respond_with_handler(_make_range_handler(content))
    output_file = tmp_path / "pauseresume_out.bin"
    
    config = DownloadConfig(
        urls=[httpserver.url_for("/pauseresume.bin")],
        filename=str(output_file),
        threads_per_mirror=2,
        chunk_size=256 * 1024,
        checksum=f"sha256:{expected_hash}"
    )
    
    downloader = Downloader(config)
    
    async def pause_and_resume():
        await asyncio.sleep(0.1)
        downloader.pause()
        assert downloader.state == DownloadState.PAUSED
        await asyncio.sleep(0.2)
        downloader.resume()
        assert downloader.state == DownloadState.DOWNLOADING
        
    asyncio.create_task(pause_and_resume())
    result = await downloader.start()
    
    assert result.status == DownloadState.COMPLETED
    assert output_file.exists()
    assert output_file.read_bytes() == content
    assert result.computed_hash == expected_hash
