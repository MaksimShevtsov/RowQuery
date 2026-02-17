"""Oracle adapter - sync and async using oracledb."""

from __future__ import annotations

from typing import Any

from row_query.core.connection import ConnectionConfig


def _build_dsn(config: ConnectionConfig) -> str:
    """Build an Oracle DSN string from config fields (host:port/database)."""
    return f"{config.host}:{config.port}/{config.database}"


def _make_row_factory(cursor: Any) -> Any:
    """Create a row factory that converts tuples to dicts using column names."""
    columns = [col[0].lower() for col in cursor.description]

    def factory(*args: Any) -> dict[str, Any]:
        return dict(zip(columns, args, strict=True))

    return factory


class OracleSyncAdapter:
    """Synchronous Oracle adapter using oracledb."""

    @property
    def paramstyle(self) -> str:
        return "named"

    def create_pool(self, config: ConnectionConfig) -> list[Any]:
        """Create a 'pool' (list of connections) for Oracle."""
        import oracledb

        dsn = _build_dsn(config)
        pool: list[Any] = []
        for _ in range(config.pool_size):
            conn = oracledb.connect(user=config.user, password=config.password, dsn=dsn)
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
        """Execute SQL and return a cursor with dict row factory."""
        cursor = connection.cursor()
        cursor.execute(sql, params or {})
        if cursor.description is not None:
            cursor.rowfactory = _make_row_factory(cursor)
        return cursor


class OracleAsyncAdapter:
    """Asynchronous Oracle adapter using oracledb async support."""

    @property
    def paramstyle(self) -> str:
        return "named"

    async def create_pool_async(self, config: ConnectionConfig) -> list[Any]:
        """Create an async Oracle connection pool."""
        import oracledb

        dsn = _build_dsn(config)
        pool: list[Any] = []
        for _ in range(config.pool_size):
            conn = await oracledb.connect_async(
                user=config.user, password=config.password, dsn=dsn
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
            await conn.close()
        pool.clear()

    async def execute_async(
        self,
        connection: Any,
        sql: str,
        params: dict[str, Any] | None = None,
    ) -> Any:
        """Execute SQL asynchronously and return a cursor with dict row factory."""
        cursor = connection.cursor()
        await cursor.execute(sql, params or {})
        if cursor.description is not None:
            cursor.rowfactory = _make_row_factory(cursor)
        return cursor
