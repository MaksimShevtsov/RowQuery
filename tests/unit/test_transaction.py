"""Unit tests for TransactionManager."""

from __future__ import annotations

from pathlib import Path

import pytest

from row_query.core.connection import ConnectionConfig, ConnectionManager
from row_query.core.engine import Engine
from row_query.core.exceptions import TransactionStateError
from row_query.core.registry import SQLRegistry


@pytest.fixture
def tx_engine(tmp_sql_dir: Path, write_sql) -> Engine:
    """Create an engine with test SQL files for transaction testing."""
    write_sql("user/insert.sql", "INSERT INTO users (name, email) VALUES (:name, :email)")
    write_sql("user/list.sql", "SELECT id, name, email FROM users ORDER BY name")
    write_sql("user/get_by_id.sql", "SELECT id, name, email FROM users WHERE id = :user_id")
    write_sql("user/count.sql", "SELECT COUNT(*) AS cnt FROM users")

    config = ConnectionConfig(driver="sqlite", database=":memory:", pool_size=1)
    manager = ConnectionManager(config)
    registry = SQLRegistry(tmp_sql_dir)
    eng = Engine(manager, registry)

    # Create the users table
    with manager.get_connection() as conn:
        conn.execute(
            "CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, email TEXT)"
        )
        conn.commit()

    return eng


class TestTransactionManager:
    def test_commit_persists_changes(self, tx_engine: Engine) -> None:
        with tx_engine.transaction() as tx:
            tx.execute("user.insert", {"name": "Alice", "email": "alice@ex.com"})

        # Changes should be visible outside the transaction
        result = tx_engine.fetch_scalar("user.count")
        assert result == 1

    def test_auto_rollback_on_exception(self, tx_engine: Engine) -> None:
        with pytest.raises(RuntimeError, match="boom"), tx_engine.transaction() as tx:
            tx.execute("user.insert", {"name": "Alice", "email": "alice@ex.com"})
            raise RuntimeError("boom")

        # Changes should be rolled back
        result = tx_engine.fetch_scalar("user.count")
        assert result == 0

    def test_explicit_rollback(self, tx_engine: Engine) -> None:
        with tx_engine.transaction() as tx:
            tx.execute("user.insert", {"name": "Alice", "email": "alice@ex.com"})
            tx.rollback()

        result = tx_engine.fetch_scalar("user.count")
        assert result == 0

    def test_execute_within_transaction(self, tx_engine: Engine) -> None:
        with tx_engine.transaction() as tx:
            affected = tx.execute("user.insert", {"name": "Alice", "email": "alice@ex.com"})
            assert affected == 1

    def test_fetch_one_within_transaction(self, tx_engine: Engine) -> None:
        # Pre-insert a user
        tx_engine.execute("user.insert", {"name": "Alice", "email": "alice@ex.com"})

        with tx_engine.transaction() as tx:
            result = tx.fetch_one("user.get_by_id", {"user_id": 1})
            assert result is not None
            assert result["name"] == "Alice"

    def test_fetch_all_within_transaction(self, tx_engine: Engine) -> None:
        tx_engine.execute("user.insert", {"name": "Alice", "email": "alice@ex.com"})

        with tx_engine.transaction() as tx:
            results = tx.fetch_all("user.list")
            assert len(results) == 1

    def test_transaction_state_error_commit_after_rollback(self, tx_engine: Engine) -> None:
        with tx_engine.transaction() as tx:
            tx.execute("user.insert", {"name": "Alice", "email": "alice@ex.com"})
            tx.rollback()
            with pytest.raises(TransactionStateError):
                tx.commit()
