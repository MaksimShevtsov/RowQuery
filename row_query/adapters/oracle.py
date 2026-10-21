"""Oracle adapter - sync and async using oracledb."""

from __future__ import annotations

from typing import Any

from row_query.core.connection import ConnectionConfig

_DEFAULT_PORT = 1521


def _build_dsn(config: ConnectionConfig) -> str:
    """Build an Oracle DSN string (host:port/service) from config fields.

    A ``dsn`` key in ``config.extra`` takes precedence.
    """
    if "dsn" in config.extra:
        return str(config.extra["dsn"])
    return f"{config.host}:{config.port or _DEFAULT_PORT}/{config.database}"


def _connect_kwargs(config: ConnectionConfig) -> dict[str, Any]:
    extra = {k: v for k, v in config.extra.items() if k != "dsn"}
    return {"user": config.user, "password": config.password, "dsn": _build_dsn(config), **extra}


def _column_key(name: str) -> str:
    """Normalise Oracle column names.

    Oracle folds unquoted identifiers to UPPERCASE where PostgreSQL, MySQL and
    SQLite keep them as written (or lowercase), so all-uppercase names are
    lowercased to keep result keys consistent across backends. Quoted
    mixed-case aliases (``AS "userName"``) are preserved.
    """
    return name.lower() if name.isupper() else name


def _make_row_factory(cursor: Any) -> Any:
    """Create a row factory that converts tuples to dicts using column names."""
    columns = [_column_key(col[0]) for col in cursor.description]

    def factory(*args: Any) -> dict[str, Any]:
        return dict(zip(columns, args, strict=True))

    return factory


class OracleSyncAdapter:
    """Synchronous Oracle adapter using oracledb."""

    paramstyle = "named"
    positional_paramstyle = "numeric"
    backslash_escapes = False

    def connect(self, config: ConnectionConfig) -> Any:
        import oracledb

        return oracledb.connect(**_connect_kwargs(config))

    def close_connection(self, connection: Any) -> None:
        connection.close()

    def is_connection_alive(self, connection: Any) -> bool:
        return bool(connection.is_healthy())

    def begin(self, connection: Any) -> None:
        """Transactions start implicitly with the first statement."""

    def execute(
        self,
        connection: Any,
        sql: str,
        params: dict[str, Any] | tuple[Any, ...] | None = None,
    ) -> Any:
        """Execute SQL and return a cursor with dict row factory."""
        cursor = connection.cursor()
        try:
            cursor.execute(sql, params if params is not None else {})
        except BaseException:
            cursor.close()
            raise
        if cursor.description is not None:
            cursor.rowfactory = _make_row_factory(cursor)
        return cursor


class OracleAsyncAdapter:
    """Asynchronous Oracle adapter using oracledb async support."""

    paramstyle = "named"
    positional_paramstyle = "numeric"
    backslash_escapes = False

    async def connect_async(self, config: ConnectionConfig) -> Any:
        import oracledb

        return await oracledb.connect_async(**_connect_kwargs(config))

    async def close_connection_async(self, connection: Any) -> None:
        await connection.close()

    def is_connection_alive(self, connection: Any) -> bool:
        return bool(connection.is_healthy())

    async def begin_async(self, connection: Any) -> None:
        """Transactions start implicitly with the first statement."""

    async def execute_async(
        self,
        connection: Any,
        sql: str,
        params: dict[str, Any] | tuple[Any, ...] | None = None,
    ) -> Any:
        """Execute SQL asynchronously and return a cursor with dict row factory."""
        cursor = connection.cursor()
        try:
            await cursor.execute(sql, params if params is not None else {})
        except BaseException:
            cursor.close()
            raise
        if cursor.description is not None:
            cursor.rowfactory = _make_row_factory(cursor)
        return cursor
