"""Repository base classes.

Thin wrappers over Engine + Mapper for DDD-oriented usage.
"""

from __future__ import annotations

from typing import Any, Generic, TypeVar

from row_query.mapping.aggregate import AggregateMapper
from row_query.mapping.plan import AggregatePlan
from row_query.mapping.protocol import Mapper

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
        mapper: Mapper[T] | None = None,
    ) -> None:
        self.engine = engine
        # Accept either a plan (to build AggregateMapper) or a mapper directly
        if mapper is not None:
            self.mapper: Mapper[T] | None = mapper
        elif mapping is not None:
            self.mapper = AggregateMapper(mapping)
        else:
            self.mapper = None


class AsyncRepository(Generic[T]):
    """Async variant of Repository."""

    def __init__(
        self,
        engine: Any,
        mapping: AggregatePlan | None = None,
        mapper: Mapper[T] | None = None,
    ) -> None:
        self.engine = engine
        # Accept either a plan (to build AggregateMapper) or a mapper directly
        if mapper is not None:
            self.mapper: Mapper[T] | None = mapper
        elif mapping is not None:
            self.mapper = AggregateMapper(mapping)
        else:
            self.mapper = None
