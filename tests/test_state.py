import json
import os
import tempfile
import unittest

from mrdl.state import JsonStateManager
from mrdl.types import FileMetadata


class TestJsonStateManager(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.state_file = os.path.join(self.tmpdir, "test.progress")
        self.manager = JsonStateManager(self.state_file)

    def tearDown(self):
        if os.path.exists(self.state_file):
            os.remove(self.state_file)
        if os.path.exists(self.state_file + ".tmp"):
            os.remove(self.state_file + ".tmp")
        os.rmdir(self.tmpdir)

    def test_load_returns_none_when_no_file(self):
        assert self.manager.load() is None

    def test_save_and_load_roundtrip(self):
        state = {"total_size": 1000, "completed": [0, 1, 2]}
        self.manager.save(state)

        loaded = self.manager.load()
        assert loaded == state

    def test_save_is_atomic(self):
        self.manager.save({"key": "value"})
        assert os.path.exists(self.state_file)
        assert not os.path.exists(self.state_file + ".tmp")

    def test_load_returns_none_on_corrupt_json(self):
        with open(self.state_file, "w") as f:
            f.write("{corrupt json!!!")

        assert self.manager.load() is None

    def test_clear_removes_state_file(self):
        self.manager.save({"data": True})
        assert os.path.exists(self.state_file)

        self.manager.clear()
        assert not os.path.exists(self.state_file)

    def test_clear_is_safe_when_no_file(self):
        self.manager.clear()

    def test_validate_for_resume_passes_when_matching(self):
        metadata = FileMetadata(total_size=1000, accepts_ranges=True, etag='"abc"', last_modified="date")
        saved = {"total_size": 1000, "chunk_size": 100, "etag": '"abc"', "last_modified": "date"}

        assert self.manager.validate_for_resume(saved, metadata, chunk_size=100) is True

    def test_validate_for_resume_fails_on_size_mismatch(self):
        metadata = FileMetadata(total_size=2000, accepts_ranges=True)
        saved = {"total_size": 1000, "chunk_size": 100}

        assert self.manager.validate_for_resume(saved, metadata, chunk_size=100) is False

    def test_validate_for_resume_fails_on_chunk_size_mismatch(self):
        metadata = FileMetadata(total_size=1000, accepts_ranges=True)
        saved = {"total_size": 1000, "chunk_size": 200}

        assert self.manager.validate_for_resume(saved, metadata, chunk_size=100) is False

    def test_validate_for_resume_fails_on_etag_mismatch(self):
        metadata = FileMetadata(total_size=1000, accepts_ranges=True, etag='"new"')
        saved = {"total_size": 1000, "chunk_size": 100, "etag": '"old"'}

        assert self.manager.validate_for_resume(saved, metadata, chunk_size=100) is False

    def test_validate_for_resume_ignores_etag_when_not_both_present(self):
        metadata = FileMetadata(total_size=1000, accepts_ranges=True, etag='"new"')
        saved = {"total_size": 1000, "chunk_size": 100}

        assert self.manager.validate_for_resume(saved, metadata, chunk_size=100) is True

    def test_build_fresh_state(self):
        metadata = FileMetadata(total_size=5000, accepts_ranges=True, etag='"xyz"', last_modified="date")
        state = self.manager.build_fresh_state(metadata, chunk_size=1000)

        assert state["total_size"] == 5000
        assert state["chunk_size"] == 1000
        assert state["completed"] == []
        assert state["etag"] == '"xyz"'
        assert state["last_modified"] == "date"

    def test_load_returns_none_on_zero_byte_file(self):
        with open(self.state_file, "w") as f:
            pass # Create empty file

        assert self.manager.load() is None

    def test_save_cleans_up_orphaned_tmp_file(self):
        # Create an orphaned tmp file
        tmp_file = self.state_file + ".tmp"
        with open(tmp_file, "w") as f:
            f.write("garbage from a crashed process")

        state = {"total_size": 1000, "completed": [0]}
        self.manager.save(state)

        # The save should succeed and the original tmp file content should be gone
        assert os.path.exists(self.state_file)
        assert not os.path.exists(tmp_file)
        assert self.manager.load() == state

