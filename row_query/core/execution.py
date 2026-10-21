"""Query execution shared by engines and transactions.

``SyncExecutor`` and ``AsyncExecutor`` implement ``fetch_one``,
``fetch_all``, ``fetch_scalar`` and ``execute`` once; ``Engine`` and
``TransactionManager`` (and their async counterparts) only decide which
connection a query runs on and whether writes are committed immediately.
"""

from __future__ import annotations

import contextlib
import inspect
import re
from contextlib import AbstractAsyncContextManager, AbstractContextManager
from dataclasses import dataclass
from typing import Any

from row_query.core.exceptions import (
    IntegrityError,
    MultipleRowsError,
    ParameterBindingError,
    QueryExecutionError,
    RowQueryError,
)
from row_query.core.params import Params, bind_params, coerce_params, resolve_sql
from row_query.core.registry import SQLRegistry
from row_query.core.sanitizer import SQLSanitizer

# Driver messages that indicate a binding (not a SQL) problem.
_BINDING_MESSAGE = re.compile(
    r"bind|parameter|placeholder|not all arguments converted|not enough arguments",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class PreparedQuery:
    """SQL and parameters ready to be handed to a driver."""

    sql: str
    params: Params
    label: str


def translate_error(label: str, exc: BaseException) -> RowQueryError:
    """Map a driver exception to the RowQuery exception hierarchy.

    DB-API ``IntegrityError`` subclasses become :class:`IntegrityError`;
    Python-level binding failures and driver errors about parameters become
    :class:`ParameterBindingError`; everything else :class:`QueryExecutionError`.
    """
    if isinstance(exc, RowQueryError):
        return exc
    names = {cls.__name__ for cls in type(exc).__mro__}
    detail = str(exc)
    if "IntegrityError" in names:
        return IntegrityError(label, detail)
    if isinstance(exc, (KeyError, IndexError, TypeError)) or (
        names & {"ProgrammingError", "InterfaceError"} and _BINDING_MESSAGE.search(detail)
    ):
        return ParameterBindingError(label, detail)
    return QueryExecutionError(label, detail)


def rows_to_dicts(description: Any, rows: Any) -> list[dict[str, Any]]:
    """Convert DB-API rows (tuples, ``sqlite3.Row`` or dicts) to dicts."""
    if description is None:
        return []
    columns = [desc[0] for desc in description]
    return [
        dict(row) if isinstance(row, dict) else dict(zip(columns, row, strict=True))
        for row in rows
    ]


def _first_value(row: Any) -> Any:
    if isinstance(row, dict):
        return next(iter(row.values()))
    return row[0]


class _QueryPreparer:
    """Resolves, sanitizes and binds queries for an adapter."""

    _registry: SQLRegistry
    _sanitizer: SQLSanitizer | None
    _adapter: Any

    def _prepare(self, query: str, params: Any) -> PreparedQuery:
        sql, label = resolve_sql(query, self._registry, self._sanitizer)
        sql, bound = bind_params(
            sql,
            coerce_params(params),
            self._adapter.paramstyle,
            self._adapter.positional_paramstyle,
        )
        return PreparedQuery(sql, bound, label)


class SyncExecutor(_QueryPreparer):
    """Implements the query API on top of :meth:`_connection`."""

    #: Commit after each ``execute`` (engines) or leave it to the caller (transactions).
    _autocommit: bool = True

    def _connection(self) -> AbstractContextManager[Any]:
        raise NotImplementedError

    def fetch_one(
        self,
        query: str,
        params: Any = None,
        *,
        mapper: Any | None = None,
    ) -> Any:
        """Fetch a single row.

        *query* may be a registry key (e.g. ``"users.get_by_id"``) or an
        inline SQL string (e.g. ``"SELECT * FROM users WHERE id = ?"``).

        *params* may be a ``dict`` for ``:name`` binding, a ``tuple``/``list``
        for ``?`` binding, or a single scalar that is wrapped in a tuple.

        Returns None if zero rows match.
        Raises MultipleRowsError if more than one row matches.
        """
        pq = self._prepare(query, params)
        with self._connection() as conn, self._cursor(conn, pq) as cursor:
            if cursor.description is None:
                return None
            try:
                rows = cursor.fetchmany(2)
            except Exception as e:
                raise translate_error(pq.label, e) from e
            if len(rows) > 1:
                raise MultipleRowsError(pq.label, len(rows))
            dicts = rows_to_dicts(cursor.description, rows)
        if not dicts:
            return None
        return mapper.map_one(dicts[0]) if mapper is not None else dicts[0]

    def fetch_all(
        self,
        query: str,
        params: Any = None,
        *,
        mapper: Any | None = None,
    ) -> Any:
        """Fetch all matching rows.

        *query* may be a registry key or an inline SQL string.
        *params* may be a dict, tuple/list, or scalar.
        """
        pq = self._prepare(query, params)
        with self._connection() as conn, self._cursor(conn, pq) as cursor:
            if cursor.description is None:
                rows: list[dict[str, Any]] = []
            else:
                try:
                    rows = rows_to_dicts(cursor.description, cursor.fetchall())
                except Exception as e:
                    raise translate_error(pq.label, e) from e
        return mapper.map_many(rows) if mapper is not None else rows

    def fetch_scalar(self, query: str, params: Any = None) -> Any:
        """Fetch a single scalar value (first column of first row).

        *query* may be a registry key or an inline SQL string.
        *params* may be a dict, tuple/list, or scalar.
        """
        pq = self._prepare(query, params)
        with self._connection() as conn, self._cursor(conn, pq) as cursor:
            if cursor.description is None:
                return None
            try:
                row = cursor.fetchone()
            except Exception as e:
                raise translate_error(pq.label, e) from e
            return None if row is None else _first_value(row)

    def execute(self, query: str, params: Any = None) -> int:
        """Execute a write query. Returns affected row count.

        *query* may be a registry key or an inline SQL string.
        *params* may be a dict, tuple/list, or scalar.
        """
        pq = self._prepare(query, params)
        with self._connection() as conn:
            with self._cursor(conn, pq) as cursor:
                rowcount = int(cursor.rowcount)
            if self._autocommit:
                try:
                    conn.commit()
                except Exception as e:
                    raise translate_error(pq.label, e) from e
        return rowcount

    def _cursor(self, conn: Any, pq: PreparedQuery) -> AbstractContextManager[Any]:
        return sync_cursor(self._adapter, conn, pq)


class sync_cursor(AbstractContextManager[Any]):  # noqa: N801 - used like a function
    def __init__(self, adapter: Any, conn: Any, pq: PreparedQuery) -> None:
        self._adapter, self._conn, self._pq = adapter, conn, pq
        self._cursor: Any = None

    def __enter__(self) -> Any:
        try:
            self._cursor = self._adapter.execute(self._conn, self._pq.sql, self._pq.params)
        except Exception as e:
            raise translate_error(self._pq.label, e) from e
        return self._cursor

    def __exit__(self, *exc: object) -> None:
        close = getattr(self._cursor, "close", None)
        if close is not None:
            with contextlib.suppress(Exception):
                close()


class AsyncExecutor(_QueryPreparer):
    """Async variant of :class:`SyncExecutor`."""

    _autocommit: bool = True

    def _connection(self) -> AbstractAsyncContextManager[Any]:
        raise NotImplementedError

    async def fetch_one(
        self,
        query: str,
        params: Any = None,
        *,
        mapper: Any | None = None,
    ) -> Any:
        """Fetch a single row asynchronously.

        Returns None if zero rows match.
        Raises MultipleRowsError if more than one row matches.
        """
        pq = self._prepare(query, params)
        async with self._connection() as conn, self._cursor(conn, pq) as cursor:
            if cursor.description is None:
                return None
            try:
                rows = await _maybe_await(cursor.fetchmany(2))
            except Exception as e:
                raise translate_error(pq.label, e) from e
            if len(rows) > 1:
                raise MultipleRowsError(pq.label, len(rows))
            dicts = rows_to_dicts(cursor.description, rows)
        if not dicts:
            return None
        return mapper.map_one(dicts[0]) if mapper is not None else dicts[0]

    async def fetch_all(
        self,
        query: str,
        params: Any = None,
        *,
        mapper: Any | None = None,
    ) -> Any:
        """Fetch all matching rows asynchronously."""
        pq = self._prepare(query, params)
        async with self._connection() as conn, self._cursor(conn, pq) as cursor:
            if cursor.description is None:
                rows: list[dict[str, Any]] = []
            else:
                try:
                    rows = rows_to_dicts(cursor.description, await cursor.fetchall())
                except Exception as e:
                    raise translate_error(pq.label, e) from e
        return mapper.map_many(rows) if mapper is not None else rows

    async def fetch_scalar(self, query: str, params: Any = None) -> Any:
        """Fetch a single scalar value asynchronously."""
        pq = self._prepare(query, params)
        async with self._connection() as conn, self._cursor(conn, pq) as cursor:
            if cursor.description is None:
                return None
            try:
                row = await cursor.fetchone()
            except Exception as e:
                raise translate_error(pq.label, e) from e
            return None if row is None else _first_value(row)

    async def execute(self, query: str, params: Any = None) -> int:
        """Execute a write query asynchronously. Returns affected row count."""
        pq = self._prepare(query, params)
        async with self._connection() as conn:
            async with self._cursor(conn, pq) as cursor:
                rowcount = int(cursor.rowcount)
            if self._autocommit:
                try:
                    await conn.commit()
                except Exception as e:
                    raise translate_error(pq.label, e) from e
        return rowcount

    def _cursor(self, conn: Any, pq: PreparedQuery) -> AbstractAsyncContextManager[Any]:
        return async_cursor(self._adapter, conn, pq)


class async_cursor(AbstractAsyncContextManager[Any]):  # noqa: N801 - used like a function
    def __init__(self, adapter: Any, conn: Any, pq: PreparedQuery) -> None:
        self._adapter, self._conn, self._pq = adapter, conn, pq
        self._cursor: Any = None

    async def __aenter__(self) -> Any:
        try:
            self._cursor = await self._adapter.execute_async(
                self._conn, self._pq.sql, self._pq.params
            )
        except Exception as e:
            raise translate_error(self._pq.label, e) from e
        return self._cursor

    async def __aexit__(self, *exc: object) -> None:
        close = getattr(self._cursor, "close", None)
        if close is not None:
            with contextlib.suppress(Exception):
                await _maybe_await(close())


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


__all__ = [
    "AsyncExecutor",
    "PreparedQuery",
    "SyncExecutor",
    "async_cursor",
    "rows_to_dicts",
    "sync_cursor",
    "translate_error",
]
