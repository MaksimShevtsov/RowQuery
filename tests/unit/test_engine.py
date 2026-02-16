"""Unit tests for Engine."""

from __future__ import annotations

from pathlib import Path

import pytest

from row_query.core.connection import ConnectionConfig, ConnectionManager
from row_query.core.engine import Engine
from row_query.core.exceptions import MultipleRowsError, QueryNotFoundError
from row_query.core.registry import SQLRegistry


@pytest.fixture
def engine(tmp_sql_dir: Path, write_sql) -> Engine:
    """Create an engine with test SQL files and SQLite in-memory DB."""
    write_sql("user/get_by_id.sql", "SELECT id, name, email FROM users WHERE id = :user_id")
    write_sql("user/list.sql", "SELECT id, name, email FROM users ORDER BY name")
    write_sql("user/insert.sql", "INSERT INTO users (name, email) VALUES (:name, :email)")
    write_sql("user/count.sql", "SELECT COUNT(*) AS cnt FROM users")

    config = ConnectionConfig(driver="sqlite", database=":memory:", pool_size=1)
    manager = ConnectionManager(config)
    registry = SQLRegistry(tmp_sql_dir)
    eng = Engine(manager, registry)

    # Create the users table using raw connection
    with manager.get_connection() as conn:
        conn.execute(
            "CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, email TEXT)"
        )
        conn.execute("INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com')")
        conn.execute("INSERT INTO users (name, email) VALUES ('Bob', 'bob@example.com')")
        conn.commit()

    return eng


class TestEngine:
    def test_fetch_one_returns_dict(self, engine: Engine) -> None:
        result = engine.fetch_one("user.get_by_id", {"user_id": 1})
        assert result is not None
        assert result["name"] == "Alice"
        assert result["email"] == "alice@example.com"

    def test_fetch_one_returns_none_on_zero_rows(self, engine: Engine) -> None:
        result = engine.fetch_one("user.get_by_id", {"user_id": 999})
        assert result is None

    def test_fetch_one_raises_multiple_rows(self, engine: Engine) -> None:
        with pytest.raises(MultipleRowsError):
            engine.fetch_one("user.list")

    def test_fetch_all_returns_list_of_dicts(self, engine: Engine) -> None:
        results = engine.fetch_all("user.list")
        assert len(results) == 2
        assert all(isinstance(r, dict) for r in results)
        names = [r["name"] for r in results]
        assert names == ["Alice", "Bob"]

    def test_fetch_scalar(self, engine: Engine) -> None:
        result = engine.fetch_scalar("user.count")
        assert result == 2

    def test_execute_returns_row_count(self, engine: Engine) -> None:
        affected = engine.execute("user.insert", {"name": "Charlie", "email": "c@ex.com"})
        assert affected == 1

    def test_query_not_found_error(self, engine: Engine) -> None:
        with pytest.raises(QueryNotFoundError, match="nonexistent.query"):
            engine.fetch_all("nonexistent.query")

    def test_parameter_binding_works(self, engine: Engine) -> None:
        result = engine.fetch_one("user.get_by_id", {"user_id": 2})
        assert result is not None
        assert result["name"] == "Bob"
