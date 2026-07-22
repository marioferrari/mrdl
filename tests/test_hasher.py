import hashlib
import os
import tempfile
import threading
import unittest
import pytest

from mrdl.hasher import StreamingHasher, verify_file
from mrdl.types import HashSpec


class TestHashSpecParse(unittest.TestCase):
    def test_parse_algo_only(self):
        spec = HashSpec.parse("sha256")
        assert spec.algo == "sha256"
        assert spec.expected is None

    def test_parse_algo_with_expected(self):
        spec = HashSpec.parse("sha256:abc123def456")
        assert spec.algo == "sha256"
        assert spec.expected == "abc123def456"

    def test_parse_normalises_algo_to_lowercase(self):
        spec = HashSpec.parse("SHA256")
        assert spec.algo == "sha256"

    def test_parse_sha512(self):
        spec = HashSpec.parse("sha512")
        assert spec.algo == "sha512"
        assert spec.expected is None

    def test_parse_md5_with_expected(self):
        data = b"hello"
        digest = hashlib.md5(data).hexdigest()
        spec = HashSpec.parse(f"md5:{digest}")
        assert spec.algo == "md5"
        assert spec.expected == digest

    def test_parse_invalid_algo_raises(self):
        with pytest.raises(ValueError) as exc_info:
            HashSpec.parse("notrealalgo")
        assert "notrealalgo" in str(exc_info.value)

    def test_parse_empty_string_raises(self):
        with pytest.raises(ValueError):
            HashSpec.parse("")

    def test_parse_colon_only_algo_expected(self):
        """Digest part may itself contain colons (should not be split further)."""
        spec = HashSpec.parse("sha256:dead:beef")
        assert spec.algo == "sha256"
        assert spec.expected == "dead:beef"


class TestStreamingHasher(unittest.TestCase):
    def _create_file_with_data(self, data: bytes) -> str:
        tmpfile = tempfile.NamedTemporaryFile(delete=False)
        tmpfile.write(data)
        tmpfile.close()
        return tmpfile.name

    def _make_hasher(self, data: bytes, hash_str: str | None, *, chunk_size: int = 1024) -> StreamingHasher:
        filename = self._create_file_with_data(data)
        self.addCleanup(os.unlink, filename)

        from unittest.mock import MagicMock
        mock_writer = MagicMock()
        mock_writer.is_on_disk.return_value = True
        mock_writer.read_chunk.return_value = None

        total_chunks = max(1, (len(data) + chunk_size - 1) // chunk_size)
        spec = HashSpec.parse(hash_str) if hash_str else None

        return StreamingHasher(
            filename=filename,
            chunk_size=chunk_size,
            total_size=len(data),
            total_chunks=total_chunks,
            disk_writer=mock_writer,
            stop_event=threading.Event(),
            hash_spec=spec,
        )

    def test_sha256_verification_passes(self):
        data = b"A" * 1024
        digest = hashlib.sha256(data).hexdigest()
        hasher = self._make_hasher(data, f"sha256:{digest}")
        hasher.start()
        hasher.stop()
        assert hasher.finalize()
        assert hasher.computed_hash == digest

    def test_sha256_verification_fails_on_mismatch(self):
        data = b"B" * 1024
        hasher = self._make_hasher(data, "sha256:" + "0" * 64)
        hasher.start()
        hasher.stop()
        assert not hasher.finalize()

    def test_md5_verification_passes(self):
        data = b"C" * 512
        digest = hashlib.md5(data).hexdigest()
        hasher = self._make_hasher(data, f"md5:{digest}", chunk_size=512)
        hasher.start()
        hasher.stop()
        assert hasher.finalize()
        assert hasher.computed_hash == digest

    def test_sha512_verification_passes(self):
        data = b"D" * 2048
        digest = hashlib.sha512(data).hexdigest()
        hasher = self._make_hasher(data, f"sha512:{digest}", chunk_size=1024)
        hasher.start()
        hasher.stop()
        assert hasher.finalize()
        assert hasher.computed_hash == digest

    def test_sha512_verification_fails_on_mismatch(self):
        data = b"E" * 2048
        hasher = self._make_hasher(data, "sha512:" + "0" * 128, chunk_size=1024)
        hasher.start()
        hasher.stop()
        assert not hasher.finalize()

    def test_compute_only_stores_hash_and_passes(self):
        data = b"F" * 1024
        expected_digest = hashlib.sha256(data).hexdigest()
        hasher = self._make_hasher(data, "sha256")  # no expected
        hasher.start()
        hasher.stop()
        assert hasher.finalize()
        assert hasher.computed_hash == expected_digest

    def test_compute_only_md5(self):
        data = b"G" * 512
        expected_digest = hashlib.md5(data).hexdigest()
        hasher = self._make_hasher(data, "md5", chunk_size=512)
        hasher.start()
        hasher.stop()
        assert hasher.finalize()
        assert hasher.computed_hash == expected_digest

    def test_no_hash_spec_is_noop(self):
        hasher = self._make_hasher(b"whatever", None)
        assert not hasher.has_work
        assert hasher.computed_hash is None
        hasher.start()
        hasher.stop()
        assert hasher.finalize()
        assert hasher.computed_hash is None

    def test_multi_chunk_sha256(self):
        chunk_size = 512
        data = b"D" * chunk_size + b"E" * chunk_size
        digest = hashlib.sha256(data).hexdigest()
        hasher = self._make_hasher(data, f"sha256:{digest}", chunk_size=chunk_size)
        hasher.start()
        hasher.stop()
        assert hasher.finalize()
        assert hasher.computed_hash == digest

    def test_multi_chunk_sha512(self):
        chunk_size = 512
        data = b"X" * chunk_size + b"Y" * chunk_size
        digest = hashlib.sha512(data).hexdigest()
        hasher = self._make_hasher(data, f"sha512:{digest}", chunk_size=chunk_size)
        hasher.start()
        hasher.stop()
        assert hasher.finalize()
        assert hasher.computed_hash == digest

    def test_streaming_hasher_handles_unknown_total_size(self):
        """Verifies that StreamingHasher correctly hashes file data when total_size is initially -1."""
        data = b"Hello, World! Unknown size stream hashing test."
        filename = self._create_file_with_data(data)
        self.addCleanup(os.unlink, filename)

        expected_hash = hashlib.sha256(data).hexdigest()
        hash_spec = HashSpec.parse(f"sha256:{expected_hash}")
        stop_event = threading.Event()
        from unittest.mock import MagicMock
        writer = MagicMock()
        writer.is_on_disk.return_value = True
        writer.read_chunk.return_value = None

        hasher = StreamingHasher(
            filename=filename,
            chunk_size=1024,
            total_size=-1,
            total_chunks=1,
            disk_writer=writer,
            stop_event=stop_event,
            hash_spec=hash_spec,
        )

        fd = os.open(filename, os.O_RDONLY)
        try:
            hasher._hash_single_chunk(fd, 0)
            hasher._verify_hash()
        finally:
            os.close(fd)

        assert hasher.computed_hash == expected_hash
        assert hasher.finalize() is True



class TestVerifyFile(unittest.TestCase):
    def _create_file_with_data(self, data: bytes) -> str:
        tmpfile = tempfile.NamedTemporaryFile(delete=False)
        tmpfile.write(data)
        tmpfile.close()
        return tmpfile.name

    def test_verify_file_passes(self):
        data = b"hello world"
        digest = hashlib.sha256(data).hexdigest()
        filename = self._create_file_with_data(data)
        self.addCleanup(os.unlink, filename)

        spec = HashSpec.parse(f"sha256:{digest}")
        is_valid, computed = verify_file(filename, spec)
        assert is_valid
        assert computed == digest

    def test_verify_file_fails_on_mismatch(self):
        data = b"hello world"
        filename = self._create_file_with_data(data)
        self.addCleanup(os.unlink, filename)

        spec = HashSpec.parse("sha256:" + "0"*64)
        is_valid, _ = verify_file(filename, spec)
        assert not is_valid

    def test_verify_file_no_expected_hash_returns_true(self):
        data = b"hello world"
        filename = self._create_file_with_data(data)
        self.addCleanup(os.unlink, filename)

        spec = HashSpec.parse("sha256")
        is_valid, computed = verify_file(filename, spec)
        assert is_valid
        assert computed == hashlib.sha256(data).hexdigest()

    def test_verify_file_reports_progress(self):
        from unittest.mock import MagicMock
        data = b"a" * 1024 * 1024 * 3  # 3 MB
        filename = self._create_file_with_data(data)
        self.addCleanup(os.unlink, filename)

        spec = HashSpec.parse("sha256")
        mock_progress = MagicMock()
        verify_file(filename, spec, progress=mock_progress, chunk_size=1024*1024)

        assert mock_progress.update_hashed.call_count == 3
        mock_progress.update_hashed.assert_any_call(0)
        mock_progress.update_hashed.assert_any_call(1)
        mock_progress.update_hashed.assert_any_call(2)

    def test_verify_file_not_found(self):
        spec = HashSpec.parse("sha256")
        with pytest.raises(FileNotFoundError):
            verify_file("nonexistent_file.bin", spec)


if __name__ == "__main__":
    unittest.main()
