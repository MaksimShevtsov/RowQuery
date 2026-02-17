"""MySQL adapter - sync (mysql-connector-python) and async (aiomysql)."""

from __future__ import annotations

from typing import Any

from row_query.core.connection import ConnectionConfig


class MysqlSyncAdapter:
    """Synchronous MySQL adapter using mysql-connector-python."""

    @property
    def paramstyle(self) -> str:
        return "pyformat"

    def create_pool(self, config: ConnectionConfig) -> list[Any]:
        """Create a pool (list of connections) for MySQL."""
        import mysql.connector

        pool: list[Any] = []
        for _ in range(config.pool_size):
            conn = mysql.connector.connect(
                host=config.host,
                port=config.port,
                user=config.user,
                password=config.password,
                database=config.database,
            )
            pool.append(conn)
        return pool

    def acquire_connection(self, pool: list[Any]) -> Any:
        """Acquire a connection from the pool."""
        if not pool:
            raise RuntimeError("No connections available in pool")
        return pool.pop()

    def release_connection(self, connection: Any, pool: list[Any]) -> None:
        """Release a connection back to the pool."""
        pool.append(connection)

    def close_pool(self, pool: list[Any]) -> None:
        """Close all connections in the pool."""
        for conn in pool:
            conn.close()
        pool.clear()

    def execute(
        self,
        connection: Any,
        sql: str,
        params: dict[str, Any] | None = None,
    ) -> Any:
        """Execute SQL and return a cursor with dictionary results."""
        cursor = connection.cursor(dictionary=True)
        cursor.execute(sql, params or {})
        return cursor


class MysqlAsyncAdapter:
    """Asynchronous MySQL adapter using aiomysql."""

    @property
    def paramstyle(self) -> str:
        return "pyformat"

    async def create_pool_async(self, config: ConnectionConfig) -> list[Any]:
        """Create async MySQL connection pool."""
        import aiomysql

        pool: list[Any] = []
        for _ in range(config.pool_size):
            conn = await aiomysql.connect(
                host=config.host,
                port=config.port,
                user=config.user,
                password=config.password,
                db=config.database,
            )
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
            conn.close()
        pool.clear()

    async def execute_async(
        self,
        connection: Any,
        sql: str,
        params: dict[str, Any] | None = None,
    ) -> Any:
        """Execute SQL asynchronously and return a DictCursor."""
        import aiomysql

        cursor = await connection.cursor(aiomysql.DictCursor)
        await cursor.execute(sql, params or {})
        return cursor
