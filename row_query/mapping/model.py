"""Simple row-to-model mapper.

Supports dataclasses, Pydantic models, and plain classes.
"""

from __future__ import annotations

import dataclasses
from typing import Any, Generic, TypeVar

from row_query.core.exceptions import ColumnMismatchError

T = TypeVar("T")


def _is_pydantic_model(cls: type) -> bool:
    """Check if a class is a Pydantic BaseModel."""
    try:
        from pydantic import BaseModel

        return issubclass(cls, BaseModel)
    except ImportError:
        return False


class ModelMapper(Generic[T]):
    """Simple row-to-model mapper.

    Detection order:
    1. Pydantic BaseModel -> model_validate(row)
    2. dataclass -> target_class(**row)
    3. Plain class -> target_class(**row)

    Args:
        target_class: The class to construct from row data.
        aliases: Optional column-name to field-name mapping.
    """

    def __init__(
        self,
        target_class: type[T],
        aliases: dict[str, str] | None = None,
    ) -> None:
        self._target_class = target_class
        self._aliases = aliases
        self._is_pydantic = _is_pydantic_model(target_class)
        self._is_dataclass = dataclasses.is_dataclass(target_class)

    def _apply_aliases(self, row: dict[str, Any]) -> dict[str, Any]:
        """Apply column aliases to the row."""
        if not self._aliases:
            return row
        result = {}
        for key, value in row.items():
            mapped_key = self._aliases.get(key, key)
            result[mapped_key] = value
        return result

    def map_one(self, row: dict[str, Any]) -> T:
        """Map a single row to target_class instance."""
        row = self._apply_aliases(row)

        if self._is_pydantic:
            try:
                return self._target_class.model_validate(row)  # type: ignore[attr-defined, no-any-return]
            except Exception as e:
                raise ColumnMismatchError(
                    self._target_class.__name__,
                    [str(e)],
                ) from e

        # For dataclasses and plain classes, try **kwargs construction
        try:
            return self._target_class(**row)
        except TypeError as e:
            # Extract missing field names from the error
            error_msg = str(e)
            raise ColumnMismatchError(
                self._target_class.__name__,
                [error_msg],
            ) from e

    def map_many(self, rows: list[dict[str, Any]]) -> list[T]:
        """Map all rows via map_one."""
        return [self.map_one(row) for row in rows]
