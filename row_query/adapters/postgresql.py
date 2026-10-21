"""PostgreSQL adapter - sync and async using psycopg (v3+)."""

from __future__ import annotations

from typing import Any

from row_query.core.connection import ConnectionConfig


def _build_conninfo(config: ConnectionConfig) -> str:
    """Build a libpq connection string from config fields.

    Values are quoted by psycopg, so passwords containing spaces or quotes
    are safe. ``config.extra`` is passed through as additional libpq
    parameters (e.g. ``sslmode``, ``application_name``).
    """
    from psycopg.conninfo import make_conninfo

    kwargs: dict[str, Any] = {
        "host": config.host,
        "port": config.port,
        "user": config.user,
        "password": config.password,
        "dbname": config.database,
        **config.extra,
    }
    return str(make_conninfo("", **{k: v for k, v in kwargs.items() if v is not None}))


class PostgresqlSyncAdapter:
    """Synchronous PostgreSQL adapter using psycopg (v3+)."""

    paramstyle = "pyformat"
    positional_paramstyle = "format"
    backslash_escapes = False

    def connect(self, config: ConnectionConfig) -> Any:
        import psycopg
        import psycopg.rows

        return psycopg.connect(_build_conninfo(config), row_factory=psycopg.rows.dict_row)

    def close_connection(self, connection: Any) -> None:
        connection.close()

    def is_connection_alive(self, connection: Any) -> bool:
        return not connection.closed and not connection.broken

    def begin(self, connection: Any) -> None:
        """Transactions start implicitly with the first statement."""

    def execute(
        self,
        connection: Any,
        sql: str,
        params: dict[str, Any] | tuple[Any, ...] | None = None,
    ) -> Any:
        return connection.execute(sql, params)


class PostgresqlAsyncAdapter:
    """Asynchronous PostgreSQL adapter using psycopg (v3+) async support."""

    paramstyle = "pyformat"
    positional_paramstyle = "format"
    backslash_escapes = False

    async def connect_async(self, config: ConnectionConfig) -> Any:
        import psycopg
        import psycopg.rows

        return await psycopg.AsyncConnection.connect(
            _build_conninfo(config), row_factory=psycopg.rows.dict_row
        )

    async def close_connection_async(self, connection: Any) -> None:
        await connection.close()

    def is_connection_alive(self, connection: Any) -> bool:
        return not connection.closed and not connection.broken

    async def begin_async(self, connection: Any) -> None:
        """Transactions start implicitly with the first statement."""

    async def execute_async(
        self,
        connection: Any,
        sql: str,
        params: dict[str, Any] | tuple[Any, ...] | None = None,
    ) -> Any:
        return await connection.execute(sql, params)
