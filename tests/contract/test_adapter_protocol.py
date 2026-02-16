"""Contract tests for adapter protocol compliance."""

from __future__ import annotations

import pytest

from row_query.adapters.protocol import AsyncAdapter, SyncAdapter
from row_query.adapters.sqlite import SqliteAsyncAdapter, SqliteSyncAdapter
from row_query.core.connection import ConnectionConfig


@pytest.fixture
def sqlite_config() -> ConnectionConfig:
    return ConnectionConfig(driver="sqlite", database=":memory:", pool_size=1)


class TestSqliteSyncAdapterProtocol:
    def test_implements_sync_protocol(self) -> None:
        adapter = SqliteSyncAdapter()
        assert isinstance(adapter, SyncAdapter)

    def test_paramstyle(self) -> None:
        adapter = SqliteSyncAdapter()
        assert adapter.paramstyle == "named"

    def test_lifecycle(self, sqlite_config: ConnectionConfig) -> None:
        adapter = SqliteSyncAdapter()
        pool = adapter.create_pool(sqlite_config)
        assert len(pool) == 1

        conn = adapter.acquire_connection(pool)
        assert conn is not None

        cursor = adapter.execute(conn, "SELECT 1 AS val")
        row = cursor.fetchone()
        assert row["val"] == 1

        adapter.release_connection(conn, pool)
        assert len(pool) == 1

        adapter.close_pool(pool)
        assert len(pool) == 0


class TestSqliteAsyncAdapterProtocol:
    def test_implements_async_protocol(self) -> None:
        adapter = SqliteAsyncAdapter()
        assert isinstance(adapter, AsyncAdapter)

    def test_paramstyle(self) -> None:
        adapter = SqliteAsyncAdapter()
        assert adapter.paramstyle == "named"

    async def test_lifecycle(self, sqlite_config: ConnectionConfig) -> None:
        adapter = SqliteAsyncAdapter()
        pool = await adapter.create_pool_async(sqlite_config)
        assert len(pool) == 1

        conn = await adapter.acquire_connection_async(pool)
        assert conn is not None

        cursor = await adapter.execute_async(conn, "SELECT 1 AS val")
        row = await cursor.fetchone()
        assert row["val"] == 1

        await adapter.release_connection_async(conn, pool)
        assert len(pool) == 1

        await adapter.close_pool_async(pool)
        assert len(pool) == 0


# --- PostgreSQL protocol compliance ---


class TestPostgresqlSyncAdapterProtocol:
    def test_implements_sync_protocol(self) -> None:
        from row_query.adapters.postgresql import PostgresqlSyncAdapter

        adapter = PostgresqlSyncAdapter()
        assert isinstance(adapter, SyncAdapter)

    def test_paramstyle(self) -> None:
        from row_query.adapters.postgresql import PostgresqlSyncAdapter

        adapter = PostgresqlSyncAdapter()
        assert adapter.paramstyle == "pyformat"


class TestPostgresqlAsyncAdapterProtocol:
    def test_implements_async_protocol(self) -> None:
        from row_query.adapters.postgresql import PostgresqlAsyncAdapter

        adapter = PostgresqlAsyncAdapter()
        assert isinstance(adapter, AsyncAdapter)

    def test_paramstyle(self) -> None:
        from row_query.adapters.postgresql import PostgresqlAsyncAdapter

        adapter = PostgresqlAsyncAdapter()
        assert adapter.paramstyle == "pyformat"


# --- MySQL protocol compliance ---


class TestMysqlSyncAdapterProtocol:
    def test_implements_sync_protocol(self) -> None:
        from row_query.adapters.mysql import MysqlSyncAdapter

        adapter = MysqlSyncAdapter()
        assert isinstance(adapter, SyncAdapter)

    def test_paramstyle(self) -> None:
        from row_query.adapters.mysql import MysqlSyncAdapter

        adapter = MysqlSyncAdapter()
        assert adapter.paramstyle == "pyformat"


class TestMysqlAsyncAdapterProtocol:
    def test_implements_async_protocol(self) -> None:
        from row_query.adapters.mysql import MysqlAsyncAdapter

        adapter = MysqlAsyncAdapter()
        assert isinstance(adapter, AsyncAdapter)

    def test_paramstyle(self) -> None:
        from row_query.adapters.mysql import MysqlAsyncAdapter

        adapter = MysqlAsyncAdapter()
        assert adapter.paramstyle == "pyformat"


# --- Oracle protocol compliance ---


class TestOracleSyncAdapterProtocol:
    def test_implements_sync_protocol(self) -> None:
        from row_query.adapters.oracle import OracleSyncAdapter

        adapter = OracleSyncAdapter()
        assert isinstance(adapter, SyncAdapter)

    def test_paramstyle(self) -> None:
        from row_query.adapters.oracle import OracleSyncAdapter

        adapter = OracleSyncAdapter()
        assert adapter.paramstyle == "named"


class TestOracleAsyncAdapterProtocol:
    def test_implements_async_protocol(self) -> None:
        from row_query.adapters.oracle import OracleAsyncAdapter

        adapter = OracleAsyncAdapter()
        assert isinstance(adapter, AsyncAdapter)

    def test_paramstyle(self) -> None:
        from row_query.adapters.oracle import OracleAsyncAdapter

        adapter = OracleAsyncAdapter()
        assert adapter.paramstyle == "named"
