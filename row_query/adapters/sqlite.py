"""SQLite adapter - sync (sqlite3 stdlib) and async (aiosqlite)."""

from __future__ import annotations

import sqlite3
from typing import Any

from row_query.core.connection import ConnectionConfig


class SqliteSyncAdapter:
    """Synchronous SQLite adapter using stdlib sqlite3."""

    paramstyle = "named"
    positional_paramstyle = "qmark"
    backslash_escapes = False

    def connect(self, config: ConnectionConfig) -> sqlite3.Connection:
        """Open a connection usable from any thread (the pool serialises access)."""
        conn: sqlite3.Connection = sqlite3.connect(
            config.database, check_same_thread=False, **config.extra
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def close_connection(self, connection: sqlite3.Connection) -> None:
        connection.close()

    def is_connection_alive(self, connection: sqlite3.Connection) -> bool:
        return True

    def begin(self, connection: sqlite3.Connection) -> None:
        """Issue an explicit BEGIN.

        The sqlite3 module only opens transactions implicitly before DML, so
        DDL (CREATE/ALTER/DROP) would otherwise run in autocommit mode.
        """
        if not connection.in_transaction:
            connection.execute("BEGIN")

    def execute(
        self,
        connection: sqlite3.Connection,
        sql: str,
        params: dict[str, Any] | tuple[Any, ...] | None = None,
    ) -> sqlite3.Cursor:
        """Execute SQL and return a cursor."""
        return connection.execute(sql, params if params is not None else {})


class SqliteAsyncAdapter:
    """Asynchronous SQLite adapter using aiosqlite."""

    paramstyle = "named"
    positional_paramstyle = "qmark"
    backslash_escapes = False

    async def connect_async(self, config: ConnectionConfig) -> Any:
        import aiosqlite

        conn = await aiosqlite.connect(config.database, **config.extra)
        conn.row_factory = aiosqlite.Row
        await conn.execute("PRAGMA journal_mode=WAL")
        return conn

    async def close_connection_async(self, connection: Any) -> None:
        await connection.close()

    def is_connection_alive(self, connection: Any) -> bool:
        return True

    async def begin_async(self, connection: Any) -> None:
        """Issue an explicit BEGIN so that DDL is transactional too."""
        if not connection.in_transaction:
            await connection.execute("BEGIN")

    async def execute_async(
        self,
        connection: Any,
        sql: str,
        params: dict[str, Any] | tuple[Any, ...] | None = None,
    ) -> Any:
        """Execute SQL asynchronously and return a cursor."""
        return await connection.execute(sql, params if params is not None else {})
