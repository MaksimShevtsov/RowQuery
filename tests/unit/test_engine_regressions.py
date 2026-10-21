"""Regression tests for connection, transaction and error-handling behaviour.

Uses a file-backed SQLite database so that several pooled connections see
the same data.
"""

from __future__ import annotations

import asyncio
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path

import pytest

from row_query.core.connection import ConnectionConfig, ConnectionManager
from row_query.core.engine import AsyncEngine, Engine
from row_query.core.exceptions import (
    IntegrityError,
    MultipleRowsError,
    ParameterBindingError,
    PoolTimeoutError,
    QueryExecutionError,
    TransactionStateError,
)
from row_query.core.registry import SQLRegistry
from row_query.mapping.model import ModelMapper


@dataclass
class User:
    id: int
    name: str


def make_engine(tmp_path: Path, pool_size: int = 2, pool_timeout: float = 0.2) -> Engine:
    config = ConnectionConfig(
        driver="sqlite",
        database=str(tmp_path / "test.db"),
        pool_size=pool_size,
        pool_timeout=pool_timeout,
    )
    engine = Engine(ConnectionManager(config), SQLRegistry(tmp_path / "sql"))
    engine.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE)")
    engine.execute("INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob')")
    return engine


@pytest.fixture
def engine(tmp_path: Path) -> Engine:
    eng = make_engine(tmp_path)
    yield eng
    eng.close()


class TestPooling:
    def test_engine_usable_from_other_threads(self, engine: Engine) -> None:
        with ThreadPoolExecutor(max_workers=4) as executor:
            counts = list(
                executor.map(
                    lambda _: engine.fetch_scalar("SELECT COUNT(*) FROM users"), range(20)
                )
            )
        assert counts == [2] * 20

    def test_read_through_engine_inside_transaction(self, engine: Engine) -> None:
        with engine.transaction() as tx:
            tx.execute("INSERT INTO users (id, name) VALUES (3, 'Carol')")
            assert engine.fetch_scalar("SELECT COUNT(*) FROM users") == 2

    def test_exhausted_pool_raises_pool_timeout(self, tmp_path: Path) -> None:
        engine = make_engine(tmp_path, pool_size=1, pool_timeout=0.05)
        with engine.transaction(), pytest.raises(PoolTimeoutError):
            engine.fetch_scalar("SELECT 1")

    def test_transaction_acquires_connection_lazily(self, tmp_path: Path) -> None:
        engine = make_engine(tmp_path, pool_size=1, pool_timeout=0.05)
        engine.transaction()
        engine.transaction()
        assert engine.fetch_scalar("SELECT 1") == 1

    def test_uncommitted_work_is_rolled_back_on_release(self, tmp_path: Path) -> None:
        engine = make_engine(tmp_path, pool_size=1)
        manager = engine._connection_manager
        with manager.get_connection() as conn:
            conn.execute("INSERT INTO users (id, name) VALUES (9, 'Ghost')")
        assert engine.fetch_scalar("SELECT COUNT(*) FROM users") == 2

    def test_failed_execute_leaves_connection_clean(self, tmp_path: Path) -> None:
        engine = make_engine(tmp_path, pool_size=1)
        with pytest.raises(IntegrityError):
            engine.execute("INSERT INTO users (id, name) VALUES (5, 'Eve'), (6, 'Alice')")
        assert engine.fetch_scalar("SELECT COUNT(*) FROM users") == 2
        assert engine.execute("INSERT INTO users (id, name) VALUES (7, 'Zed')") == 1

    async def test_async_gather_more_queries_than_pool_size(self, tmp_path: Path) -> None:
        make_engine(tmp_path).close()
        config = ConnectionConfig(driver="sqlite", database=str(tmp_path / "test.db"), pool_size=5)
        engine = AsyncEngine.from_config(config, SQLRegistry(tmp_path / "sql"))
        try:
            results = await asyncio.gather(
                *(engine.fetch_scalar("SELECT COUNT(*) FROM users") for _ in range(12))
            )
            assert results == [2] * 12
        finally:
            await engine.close()


class TestErrorTranslation:
    def test_syntax_error_is_query_execution_error(self, engine: Engine) -> None:
        with pytest.raises(QueryExecutionError) as info:
            engine.fetch_all("SELEC * FROM users")
        assert not isinstance(info.value, ParameterBindingError)
        assert isinstance(info.value.__cause__, sqlite3.OperationalError)

    def test_unique_violation_is_integrity_error(self, engine: Engine) -> None:
        with pytest.raises(IntegrityError) as info:
            engine.execute(
                "INSERT INTO users (id, name) VALUES (:id, :name)", {"id": 3, "name": "Bob"}
            )
        assert isinstance(info.value.__cause__, sqlite3.IntegrityError)

    def test_missing_parameter_is_binding_error(self, engine: Engine) -> None:
        with pytest.raises(ParameterBindingError):
            engine.fetch_one("SELECT * FROM users WHERE id = :id", {"other": 1})


class TestTransactionApi:
    def test_fetch_one_raises_multiple_rows(self, engine: Engine) -> None:
        with engine.transaction() as tx, pytest.raises(MultipleRowsError):
            tx.fetch_one("SELECT * FROM users")

    def test_fetch_scalar_and_mapper(self, engine: Engine) -> None:
        mapper = ModelMapper(User)
        with engine.transaction() as tx:
            assert tx.fetch_scalar("SELECT COUNT(*) FROM users") == 2
            user = tx.fetch_one("SELECT id, name FROM users WHERE id = ?", 1, mapper=mapper)
            users = tx.fetch_all("SELECT id, name FROM users ORDER BY id", mapper=mapper)
        assert user == User(1, "Alice")
        assert [u.name for u in users] == ["Alice", "Bob"]

    def test_query_outside_with_block_rejected(self, engine: Engine) -> None:
        tx = engine.transaction()
        with pytest.raises(TransactionStateError):
            tx.fetch_all("SELECT * FROM users")

    def test_transaction_cannot_be_reentered(self, engine: Engine) -> None:
        tx = engine.transaction()
        with tx:
            pass
        with pytest.raises(TransactionStateError), tx:
            pass


class TestInlineSql:
    def test_single_word_statement_is_inline_sql(self, engine: Engine) -> None:
        engine.execute("VACUUM")

    def test_like_with_percent_and_named_param(self, engine: Engine) -> None:
        rows = engine.fetch_all(
            "SELECT name FROM users WHERE name LIKE 'A%' AND id = :id", {"id": 1}
        )
        assert rows == [{"name": "Alice"}]
