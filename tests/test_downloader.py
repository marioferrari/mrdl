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
