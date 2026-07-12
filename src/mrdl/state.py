"""Backwards-compatibility shim — module renamed to mrdl.persistence."""
from mrdl.persistence import JsonStateManager  # noqa: F401

__all__ = ["JsonStateManager"]
