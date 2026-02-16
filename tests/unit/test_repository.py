"""Unit tests for Repository and AsyncRepository base classes."""

from __future__ import annotations

from dataclasses import dataclass, field
from unittest.mock import AsyncMock, MagicMock

from row_query.mapping.aggregate import AggregateMapper
from row_query.mapping.builder import aggregate
from row_query.repository.base import AsyncRepository, Repository


@dataclass
class User:
    id: int
    name: str
    orders: list = field(default_factory=list)


@dataclass
class Order:
    id: int
    total: float


class TestRepository:
    def test_engine_attribute(self) -> None:
        engine = MagicMock()
        repo = Repository(engine=engine)
        assert repo.engine is engine

    def test_mapper_with_mapping(self) -> None:
        engine = MagicMock()
        mapping = aggregate(User, prefix="user__").key("id").auto_fields().build()
        repo = Repository(engine=engine, mapping=mapping)
        assert repo.mapper is not None
        assert isinstance(repo.mapper, AggregateMapper)

    def test_mapper_none_without_mapping(self) -> None:
        engine = MagicMock()
        repo = Repository(engine=engine)
        assert repo.mapper is None

    def test_subclass_delegates_to_engine(self) -> None:
        engine = MagicMock()
        engine.fetch_all.return_value = [{"id": 1, "name": "Alice"}]

        class UserRepo(Repository):
            def list_all(self):
                return self.engine.fetch_all("user.list", mapper=self.mapper)

        repo = UserRepo(engine=engine)
        result = repo.list_all()
        engine.fetch_all.assert_called_once_with("user.list", mapper=None)
        assert len(result) == 1


class TestAsyncRepository:
    def test_engine_attribute(self) -> None:
        engine = AsyncMock()
        repo = AsyncRepository(engine=engine)
        assert repo.engine is engine

    def test_mapper_with_mapping(self) -> None:
        engine = AsyncMock()
        mapping = aggregate(User, prefix="user__").key("id").auto_fields().build()
        repo = AsyncRepository(engine=engine, mapping=mapping)
        assert repo.mapper is not None
        assert isinstance(repo.mapper, AggregateMapper)
