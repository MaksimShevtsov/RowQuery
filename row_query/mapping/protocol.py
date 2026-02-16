"""Mapper protocol.

All mappers implement this interface. The Engine calls map_one for
fetch_one results and map_many for fetch_all results.
"""

from __future__ import annotations

from typing import Any, Protocol, TypeVar

T = TypeVar("T")


class Mapper(Protocol[T]):
    """Base mapper protocol."""

    def map_one(self, row: dict[str, Any]) -> T:
        """Map a single row dict to a target object."""
        ...

    def map_many(self, rows: list[dict[str, Any]]) -> list[T]:
        """Map multiple row dicts to a list of target objects."""
        ...
