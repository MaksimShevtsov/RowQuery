"""PostgreSQL adapter - sync and async using psycopg (v3+)."""

from __future__ import annotations

from typing import Any

from row_query.core.connection import ConnectionConfig


def _build_conninfo(config: ConnectionConfig) -> str:
    """Build a libpq connection string from config fields."""
    parts: list[str] = []
    if config.host is not None:
        parts.append(f"host={config.host}")
    if config.port is not None:
        parts.append(f"port={config.port}")
    if config.user is not None:
        parts.append(f"user={config.user}")
    if config.password is not None:
        parts.append(f"password={config.password}")
    parts.append(f"dbname={config.database}")
    return " ".join(parts)


class PostgresqlSyncAdapter:
    """Synchronous PostgreSQL adapter using psycopg (v3+)."""

    @property
    def paramstyle(self) -> str:
        return "pyformat"

    def create_pool(self, config: ConnectionConfig) -> list[Any]:
        import psycopg
        import psycopg.rows

        conninfo = _build_conninfo(config)
        pool: list[Any] = []
        for _ in range(config.pool_size):
            conn = psycopg.connect(conninfo, row_factory=psycopg.rows.dict_row)
            pool.append(conn)
        return pool

    def acquire_connection(self, pool: list[Any]) -> Any:
        if not pool:
            raise RuntimeError("No connections available in pool")
        return pool.pop()

    def release_connection(self, connection: Any, pool: list[Any]) -> None:
        pool.append(connection)

    def close_pool(self, pool: list[Any]) -> None:
        for conn in pool:
            conn.close()
        pool.clear()

    def execute(
        self,
        connection: Any,
        sql: str,
        params: dict[str, Any] | None = None,
    ) -> Any:
        return connection.execute(sql, params)


class PostgresqlAsyncAdapter:
    """Asynchronous PostgreSQL adapter using psycopg (v3+) async support."""

    @property
    def paramstyle(self) -> str:
        return "pyformat"

    async def create_pool_async(self, config: ConnectionConfig) -> list[Any]:
        import psycopg
        import psycopg.rows

        conninfo = _build_conninfo(config)
        pool: list[Any] = []
        for _ in range(config.pool_size):
            conn = await psycopg.AsyncConnection.connect(
                conninfo, row_factory=psycopg.rows.dict_row
            )
            pool.append(conn)
        return pool

    async def acquire_connection_async(self, pool: list[Any]) -> Any:
        if not pool:
            raise RuntimeError("No connections available in pool")
        return pool.pop()

    async def release_connection_async(self, connection: Any, pool: list[Any]) -> None:
        pool.append(connection)

    async def close_pool_async(self, pool: list[Any]) -> None:
        for conn in pool:
            await conn.close()
        pool.clear()

    async def execute_async(
        self,
        connection: Any,
        sql: str,
        params: dict[str, Any] | None = None,
    ) -> Any:
        return await connection.execute(sql, params)
