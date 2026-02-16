"""Repository base classes.

Thin wrappers over Engine + Mapper for DDD-oriented usage.
"""

from __future__ import annotations

from typing import Any, Generic, TypeVar

from row_query.mapping.aggregate import AggregateMapper
from row_query.mapping.plan import AggregatePlan

T = TypeVar("T")


class Repository(Generic[T]):
    """Base repository class for DDD-oriented usage.

    Subclasses define concrete data access methods that delegate to
    the engine.
    """

    def __init__(
        self,
        engine: Any,
        mapping: AggregatePlan | None = None,
    ) -> None:
        self.engine = engine
        self.mapper: AggregateMapper[T] | None = (
            AggregateMapper(mapping) if mapping is not None else None
        )


class AsyncRepository(Generic[T]):
    """Async variant of Repository."""

    def __init__(
        self,
        engine: Any,
        mapping: AggregatePlan | None = None,
    ) -> None:
        self.engine = engine
        self.mapper: AggregateMapper[T] | None = (
            AggregateMapper(mapping) if mapping is not None else None
        )
