from __future__ import annotations

import json
import os
from typing import Any

from mrdl.types import FileMetadata


class JsonStateManager:
    """Manages persistent session state using a local JSON file."""

    def __init__(self, state_file: str, safe_saves: bool = False):
        """Initializes the JsonStateManager.

        Args:
            state_file: Path to the JSON progress file.
            safe_saves: If True, fsync the state file after every write for extra durability
                (e.g. on NFS/SMB). Defaults to False for lower latency on local disks.
        """
        self._state_file = state_file
        self._safe_saves = safe_saves

    def load(self) -> dict | None:
        """Loads and parses the persistent download state.

        Returns:
            The saved state dict, or None if the file does not exist or is invalid.
        """
        try:
            with open(self._state_file, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, FileNotFoundError, OSError):
            return None

    def save(self, state: dict) -> None:
        """Saves the current download state atomically using a temporary file.

        Args:
            state: The state dictionary to save.
        """
        tmp_path = self._state_file + ".tmp"
        with open(tmp_path, "w") as f:
            json.dump(state, f)
            if self._safe_saves:
                f.flush()
                os.fsync(f.fileno())
        os.replace(tmp_path, self._state_file)

    def clear(self) -> None:
        """Removes the persistent state file from disk."""
        try:
            os.remove(self._state_file)
        except FileNotFoundError:
            pass

    def validate_for_resume(
        self,
        saved_state: dict,
        metadata: FileMetadata,
        chunk_size: int,
    ) -> bool:
        """Validates if the saved state is compatible with the target file metadata.

        Args:
            saved_state: The state dict loaded from disk.
            metadata: The current remote FileMetadata.
            chunk_size: The configured download chunk size.

        Returns:
            True if compatible, otherwise False.
        """
        if saved_state.get("total_size") != metadata.total_size:
            return False

        if saved_state.get("chunk_size") != chunk_size:
            return False

        if saved_state.get("etag") and metadata.etag:
            if saved_state["etag"] != metadata.etag:
                return False

        if saved_state.get("last_modified") and metadata.last_modified:
            if saved_state["last_modified"] != metadata.last_modified:
                return False

        return True

    def build_fresh_state(self, metadata: FileMetadata, chunk_size: int) -> dict[str, Any]:
        """Builds a new empty state dictionary.

        Args:
            metadata: Remote FileMetadata.
            chunk_size: Segment chunk size.

        Returns:
            A new state dictionary.
        """
        return {
            "total_size": metadata.total_size,
            "chunk_size": chunk_size,
            "completed": [],
            "etag": metadata.etag,
            "last_modified": metadata.last_modified,
        }
