"""MySQL adapter - sync (mysql-connector-python) and async (aiomysql)."""

from __future__ import annotations

from typing import Any

from row_query.core.connection import ConnectionConfig


class MysqlSyncAdapter:
    """Synchronous MySQL adapter using mysql-connector-python."""

    paramstyle = "pyformat"
    positional_paramstyle = "format"
    backslash_escapes = True

    def connect(self, config: ConnectionConfig) -> Any:
        import mysql.connector

        kwargs: dict[str, Any] = {
            "host": config.host,
            "port": config.port,
            "user": config.user,
            "password": config.password,
            "database": config.database,
            **config.extra,
        }
        return mysql.connector.connect(**{k: v for k, v in kwargs.items() if v is not None})

    def close_connection(self, connection: Any) -> None:
        connection.close()

    def is_connection_alive(self, connection: Any) -> bool:
        # Local check only: is_connected() pings the server on every call.
        # A connection broken server-side fails its first query and is then
        # discarded when the pool's rollback-on-release fails.
        is_closed = getattr(connection, "is_closed", None)  # C extension
        if is_closed is not None:
            return not is_closed()
        return getattr(connection, "_socket", None) is not None

    def begin(self, connection: Any) -> None:
        """Transactions start implicitly with the first statement."""

    def execute(
        self,
        connection: Any,
        sql: str,
        params: dict[str, Any] | tuple[Any, ...] | None = None,
    ) -> Any:
        """Execute SQL and return a buffered dictionary cursor.

        Buffering reads the whole result client-side so that a partially
        consumed result never blocks the next statement ("Unread result found").
        """
        cursor = connection.cursor(dictionary=True, buffered=True)
        try:
            cursor.execute(sql, params)
        except BaseException:
            cursor.close()
            raise
        return cursor


class MysqlAsyncAdapter:
    """Asynchronous MySQL adapter using aiomysql."""

    paramstyle = "pyformat"
    positional_paramstyle = "format"
    backslash_escapes = True

    async def connect_async(self, config: ConnectionConfig) -> Any:
        import aiomysql

        kwargs: dict[str, Any] = {
            "host": config.host,
            "port": config.port,
            "user": config.user,
            "password": config.password,
            "db": config.database,
            **config.extra,
        }
        return await aiomysql.connect(**{k: v for k, v in kwargs.items() if v is not None})

    async def close_connection_async(self, connection: Any) -> None:
        connection.close()

    def is_connection_alive(self, connection: Any) -> bool:
        return not connection.closed

    async def begin_async(self, connection: Any) -> None:
        """Transactions start implicitly with the first statement."""

    async def execute_async(
        self,
        connection: Any,
        sql: str,
        params: dict[str, Any] | tuple[Any, ...] | None = None,
    ) -> Any:
        """Execute SQL asynchronously and return a (buffered) DictCursor."""
        import aiomysql

        cursor = await connection.cursor(aiomysql.DictCursor)
        try:
            await cursor.execute(sql, params)
        except BaseException:
            await cursor.close()
            raise
        return cursor
