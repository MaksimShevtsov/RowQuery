"""Unit tests for ModelMapper."""

from __future__ import annotations

from dataclasses import dataclass

import pytest
from pydantic import BaseModel

from row_query.core.exceptions import ColumnMismatchError
from row_query.mapping.model import ModelMapper


@dataclass
class UserDC:
    id: int
    name: str
    email: str


class UserPydantic(BaseModel):
    id: int
    name: str
    email: str


class UserPlain:
    def __init__(self, id: int, name: str, email: str) -> None:
        self.id = id
        self.name = name
        self.email = email


class TestModelMapper:
    def test_map_to_dataclass(self) -> None:
        mapper = ModelMapper(UserDC)
        row = {"id": 1, "name": "Alice", "email": "alice@ex.com"}
        result = mapper.map_one(row)
        assert isinstance(result, UserDC)
        assert result.id == 1
        assert result.name == "Alice"

    def test_map_to_pydantic(self) -> None:
        mapper = ModelMapper(UserPydantic)
        row = {"id": 1, "name": "Alice", "email": "alice@ex.com"}
        result = mapper.map_one(row)
        assert isinstance(result, UserPydantic)
        assert result.id == 1

    def test_pydantic_type_coercion(self) -> None:
        mapper = ModelMapper(UserPydantic)
        row = {"id": "42", "name": "Alice", "email": "a@ex.com"}
        result = mapper.map_one(row)
        assert result.id == 42  # Coerced from str to int

    def test_map_to_plain_class(self) -> None:
        mapper = ModelMapper(UserPlain)
        row = {"id": 1, "name": "Alice", "email": "alice@ex.com"}
        result = mapper.map_one(row)
        assert isinstance(result, UserPlain)
        assert result.name == "Alice"

    def test_map_many(self) -> None:
        mapper = ModelMapper(UserDC)
        rows = [
            {"id": 1, "name": "Alice", "email": "a@ex.com"},
            {"id": 2, "name": "Bob", "email": "b@ex.com"},
        ]
        results = mapper.map_many(rows)
        assert len(results) == 2
        assert all(isinstance(r, UserDC) for r in results)

    def test_column_mismatch_error(self) -> None:
        mapper = ModelMapper(UserDC)
        row = {"id": 1, "name": "Alice"}  # missing "email"
        with pytest.raises(ColumnMismatchError):
            mapper.map_one(row)

    def test_column_aliasing(self) -> None:
        mapper = ModelMapper(UserDC, aliases={"user_email": "email"})
        row = {"id": 1, "name": "Alice", "user_email": "alice@ex.com"}
        result = mapper.map_one(row)
        assert result.email == "alice@ex.com"

    def test_map_many_empty(self) -> None:
        mapper = ModelMapper(UserDC)
        results = mapper.map_many([])
        assert results == []
