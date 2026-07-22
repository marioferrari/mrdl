import unittest
from unittest.mock import AsyncMock, patch
import pytest

from mrdl.downloader import Downloader
from mrdl.exceptions import StoppedException, IncompleteChunkError
from mrdl.types import DownloadState, InvalidStateTransition, DownloadConfig
from mrdl.cli import parse_args


class TestDownloaderState(unittest.IsolatedAsyncioTestCase):
    async def test_initial_state_is_idle(self):
        config = DownloadConfig(urls=["http://example.com"], filename="out.bin")
        downloader = Downloader(config)
        assert downloader.state == DownloadState.IDLE

    async def test_compact_config_is_accepted(self):
        config = DownloadConfig(urls=["http://example.com"], filename="out.bin", compact=True)
        assert config.compact is True
        downloader = Downloader(config)
        assert getattr(getattr(downloader._progress, "_state", None), "compact", False) is True

    async def test_cancel_from_idle(self):
        config = DownloadConfig(urls=["http://example.com"], filename="out.bin")
        downloader = Downloader(config)
        downloader.cancel()
        assert downloader.state == DownloadState.CANCELLED

    async def test_pause_resume_state(self):
        config = DownloadConfig(urls=["http://example.com"], filename="out.bin")
        downloader = Downloader(config)
        # Force state to DOWNLOADING to test pause transition
        downloader._state = DownloadState.DOWNLOADING
        downloader._pause_event = AsyncMock()
        downloader._loop = AsyncMock()
        downloader.pause()
        assert downloader.state == DownloadState.PAUSED
        downloader.resume()
        assert downloader.state == DownloadState.DOWNLOADING

    async def test_set_speed_limit(self):
        config = DownloadConfig(urls=["http://example.com"], filename="out.bin")
        downloader = Downloader(config)
        
        # We can dynamically set speed even if it was uncapped initially
        downloader.set_speed_limit(1024)
        
        # Test custom throttle lacking update_rate
        class DummyThrottle:
            @property
            def is_active(self) -> bool:
                return False

            async def consume(self, n_bytes: int) -> None:
                pass
            
        downloader_custom = Downloader(config, global_throttle=DummyThrottle())
        # This shouldn't crash, it should just log a warning
        downloader_custom.set_speed_limit(1024)

    @patch("mrdl.downloader.MirrorProber")
    async def test_start_fails_gracefully_when_no_mirrors_found(self, mock_prober_cls):
        config = DownloadConfig(urls=["http://example.com"], filename="out.bin")
        downloader = Downloader(config)
        
        mock_prober = mock_prober_cls.return_value
        mock_prober.probe = AsyncMock(side_effect=FileNotFoundError("File not found on any of the provided mirrors."))
        
        result = await downloader.start()
        
        assert result.status == DownloadState.FAILED
        assert result.error is not None
        assert "File not found" in result.error
        assert downloader.state == DownloadState.FAILED
        assert result.computed_hash is None

class TestDownloaderImports(unittest.TestCase):
    def test_top_level_imports(self):
        from mrdl import Downloader, DownloadState, SlowMirrorException
        assert Downloader is not None
        assert DownloadState is not None
        assert SlowMirrorException is not None

    def test_exception_classes(self):
        assert issubclass(StoppedException, Exception)
        assert issubclass(IncompleteChunkError, Exception)


class TestCliParsing(unittest.TestCase):
    def test_basic_args(self):
        args = parse_args(["http://example.com/file.zip", "-o", "out.zip", "-t", "4"])
        assert args.urls == ["http://example.com/file.zip"]
        assert args.output == "out.zip"
        assert args.threads_per_mirror == 4
        assert args.use_mmap is False

    def test_use_mmap_flag(self):
        args = parse_args(["http://example.com/file.zip", "-o", "out.zip", "--use-mmap"])
        assert args.use_mmap is True

    def test_hash_compute_only(self):
        args = parse_args(["http://example.com/file.zip", "-o", "out.zip", "--checksum", "sha256"])
        assert args.checksum == "sha256"

    def test_hash_with_expected(self):
        args = parse_args(["http://example.com/file.zip", "-o", "out.zip", "--checksum", "sha256:abc123"])
        assert args.checksum == "sha256:abc123"

    def test_hash_sha512(self):
        args = parse_args(["http://example.com/file.zip", "-o", "out.zip", "--checksum", "sha512"])
        assert args.checksum == "sha512"

    def test_hash_defaults_to_none(self):
        args = parse_args(["http://example.com/file.zip", "-o", "out.zip"])
        assert args.checksum is None

    def test_max_speed_flags(self):
        args = parse_args([
            "http://example.com/file.zip", "-o", "out.zip",
            "--max-speed", "512",
            "--max-speed-per-thread", "256",
        ])
        assert args.max_speed == 512
        assert args.max_speed_per_thread == 256

    def test_max_speed_defaults_to_none(self):
        args = parse_args(["http://example.com/file.zip", "-o", "out.zip"])
        assert args.max_speed is None
        assert args.max_speed_per_thread is None

    def test_cli_handles_missing_uvloop_gracefully(self):
        """Verifies that cli module handles missing uvloop and restores uvloop reference afterwards."""
        import sys
        import importlib
        from unittest.mock import patch
        
        try:
            with patch.dict(sys.modules, {"uvloop": None}):
                import mrdl.cli
                importlib.reload(mrdl.cli)
                assert getattr(mrdl.cli, "uvloop", None) is None
        finally:
            import mrdl.cli
            importlib.reload(mrdl.cli)

        if sys.platform != "win32":
            assert getattr(mrdl.cli, "uvloop", None) is not None




@pytest.mark.asyncio
async def test_prepare_file_handles_zero_or_negative_total_size(tmp_path):
    """Verifies prepare_file does not call ftruncate/fallocate with negative total_size."""
    import os
    from mrdl.session import SessionManager
    from mrdl.types import FileMetadata
    from unittest.mock import MagicMock
    
    out_file = tmp_path / "stream.bin"
    metadata = FileMetadata(total_size=-1, accepts_ranges=False)
    sm = SessionManager(
        filename=str(out_file),
        chunk_size=1024 * 1024,
        metadata=metadata,
        state_manager=MagicMock(),
        progress=MagicMock(),
        hash_spec=None,
        stop_event_thread=MagicMock(),
        chunk_condition=MagicMock(),
    )
    
    # Should not raise OSError: [Errno 22] Invalid argument
    fd = sm.prepare_file()
    assert fd is not None
    os.close(fd)


@pytest.mark.asyncio
async def test_non_range_download_caps_fetcher_buffer_memory():
    """Verifies that non-range downloads create a 1-chunk task for 0..EOF while capping RAM buffer at 16 MiB."""
    from mrdl.types import FileMetadata
    from mrdl.fetcher import ChunkFetcher, FetcherConfig
    from unittest.mock import MagicMock
    
    config = DownloadConfig(
        urls=["http://example.com/large.iso"],
        filename="out.iso",
        chunk_size=64 * 1024 * 1024,
    )
    downloader = Downloader(config)
    downloader._metadata = FileMetadata(total_size=5 * 1024 * 1024 * 1024, accepts_ranges=False)
    
    # Execute probe fallback logic via method
    downloader._apply_probe_fallback()
    assert downloader._chunk_size == 5 * 1024 * 1024 * 1024
    
    # Verify ChunkFetcher constructed with this config caps in-memory buffer to 16 MiB
    fetcher_config = FetcherConfig(chunk_size=downloader._chunk_size, min_speed_kbps=0, speed_grace_period=10)
    fetcher = ChunkFetcher(
        session=MagicMock(),
        mirror_url="http://example.com/large.iso",
        metadata=downloader._metadata,
        writer=MagicMock(),
        progress=MagicMock(),
        stop_event=MagicMock(),
        config=fetcher_config,
    )
    assert len(fetcher._buffer) == 16 * 1024 * 1024  # Capped at 16 MiB RAM, NOT 5 GiB RAM!



