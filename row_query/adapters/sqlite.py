"""SQLite adapter - sync (sqlite3 stdlib) and async (aiosqlite)."""

from __future__ import annotations

import sqlite3
from typing import Any

from row_query.core.connection import ConnectionConfig


class SqliteSyncAdapter:
    """Synchronous SQLite adapter using stdlib sqlite3."""

    @property
    def paramstyle(self) -> str:
        return "named"

    def create_pool(self, config: ConnectionConfig) -> list[sqlite3.Connection]:
        """Create a 'pool' (list of connections) for SQLite."""
        pool: list[sqlite3.Connection] = []
        for _ in range(config.pool_size):
            conn = sqlite3.connect(config.database)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            pool.append(conn)
        return pool

    def acquire_connection(self, pool: list[sqlite3.Connection]) -> sqlite3.Connection:
        """Acquire a connection from the pool."""
        if not pool:
            raise RuntimeError("No connections available in pool")
        return pool.pop()

    def release_connection(
        self, connection: sqlite3.Connection, pool: list[sqlite3.Connection]
    ) -> None:
        """Release a connection back to the pool."""
        pool.append(connection)

    def close_pool(self, pool: list[sqlite3.Connection]) -> None:
        """Close all connections in the pool."""
        for conn in pool:
            conn.close()
        pool.clear()

    def execute(
        self,
        connection: sqlite3.Connection,
        sql: str,
        params: dict[str, Any] | None = None,
    ) -> sqlite3.Cursor:
        """Execute SQL and return a cursor."""
        return connection.execute(sql, params or {})


class SqliteAsyncAdapter:
    """Asynchronous SQLite adapter using aiosqlite."""

    @property
    def paramstyle(self) -> str:
        return "named"

    async def create_pool_async(self, config: ConnectionConfig) -> list[Any]:
        """Create async SQLite connection pool."""
        import aiosqlite

        pool: list[Any] = []
        for _ in range(config.pool_size):
            conn = await aiosqlite.connect(config.database)
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA journal_mode=WAL")
            pool.append(conn)
        return pool

    async def acquire_connection_async(self, pool: list[Any]) -> Any:
        """Acquire an async connection from the pool."""
        if not pool:
            raise RuntimeError("No connections available in pool")
        return pool.pop()

    async def release_connection_async(self, connection: Any, pool: list[Any]) -> None:
        """Release an async connection back to the pool."""
        pool.append(connection)

    async def close_pool_async(self, pool: list[Any]) -> None:
        """Close all async connections."""
        for conn in pool:
            await conn.close()
        pool.clear()

    async def execute_async(
        self,
        connection: Any,
        sql: str,
        params: dict[str, Any] | None = None,
    ) -> Any:
        """Execute SQL asynchronously and return a cursor."""
        return await connection.execute(sql, params or {})
