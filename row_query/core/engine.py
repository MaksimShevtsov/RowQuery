"""Query execution engine.

The Engine resolves named queries from the SQLRegistry, binds parameters,
executes through the adapter, and optionally applies a mapper to results.
"""

from __future__ import annotations

from typing import Any, TypeVar

from row_query.core.connection import AsyncConnectionManager, ConnectionManager
from row_query.core.exceptions import (
    MultipleRowsError,
    ParameterBindingError,
)
from row_query.core.params import normalize_params
from row_query.core.registry import SQLRegistry
from row_query.core.transaction import AsyncTransactionManager, TransactionManager

T = TypeVar("T")


def _rows_to_dicts(cursor: Any) -> list[dict[str, Any]]:
    """Convert cursor results to list of dicts.

    Handles both tuple-like rows and dict-like rows from different adapters.
    """
    if cursor.description is None:
        return []
    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()
    if not rows:
        return []

    # Check if rows are already dict-like (e.g., psycopg dict_row, MySQL dict cursor)
    first_row = rows[0]
    if isinstance(first_row, dict):
        # Already dicts, return as-is (copy to ensure consistency)
        return [dict(row) for row in rows]

    # Tuple-like rows, zip with columns
    return [dict(zip(columns, row, strict=True)) for row in rows]


def _row_to_dict(cursor: Any) -> dict[str, Any] | None:
    """Convert a single cursor row to dict, or None.

    Handles both tuple-like rows and dict-like rows from different adapters.
    """
    if cursor.description is None:
        return None
    columns = [desc[0] for desc in cursor.description]
    row = cursor.fetchone()
    if row is None:
        return None

    # Check if row is already dict-like
    if isinstance(row, dict):
        return dict(row)

    # Tuple-like row, zip with columns
    return dict(zip(columns, row, strict=True))


class Engine:
    """Synchronous query execution engine."""

    def __init__(
        self,
        connection_manager: ConnectionManager,
        registry: SQLRegistry,
    ) -> None:
        self._connection_manager = connection_manager
        self._registry = registry
        self._paramstyle = connection_manager.adapter.paramstyle

    @classmethod
    def from_config(
        cls,
        config: Any,
        registry: SQLRegistry,
    ) -> Engine:
        """Create an Engine from a ConnectionConfig and SQLRegistry.

        Args:
            config: ConnectionConfig instance
            registry: SQLRegistry instance

        Returns:
            Engine instance
        """
        connection_manager = ConnectionManager(config)
        return cls(connection_manager, registry)

    def fetch_one(
        self,
        query_name: str,
        params: dict[str, Any] | None = None,
        *,
        mapper: Any | None = None,
    ) -> Any:
        """Fetch a single row.

        Returns None if zero rows match.
        Raises MultipleRowsError if more than one row matches.
        """
        sql = self._registry.get(query_name)
        sql = normalize_params(sql, self._paramstyle)

        with self._connection_manager.get_connection() as conn:
            try:
                cursor = self._connection_manager.adapter.execute(conn, sql, params)
            except Exception as e:
                raise ParameterBindingError(query_name, str(e)) from e

            rows = _rows_to_dicts(cursor)

        if len(rows) == 0:
            return None
        if len(rows) > 1:
            raise MultipleRowsError(query_name, len(rows))

        row = rows[0]
        if mapper is not None:
            return mapper.map_one(row)
        return row

    def fetch_all(
        self,
        query_name: str,
        params: dict[str, Any] | None = None,
        *,
        mapper: Any | None = None,
    ) -> Any:
        """Fetch all matching rows."""
        sql = self._registry.get(query_name)
        sql = normalize_params(sql, self._paramstyle)

        with self._connection_manager.get_connection() as conn:
            try:
                cursor = self._connection_manager.adapter.execute(conn, sql, params)
            except Exception as e:
                raise ParameterBindingError(query_name, str(e)) from e

            rows = _rows_to_dicts(cursor)

        if mapper is not None:
            return mapper.map_many(rows)
        return rows

    def fetch_scalar(
        self,
        query_name: str,
        params: dict[str, Any] | None = None,
    ) -> Any:
        """Fetch a single scalar value (first column of first row)."""
        sql = self._registry.get(query_name)
        sql = normalize_params(sql, self._paramstyle)

        with self._connection_manager.get_connection() as conn:
            try:
                cursor = self._connection_manager.adapter.execute(conn, sql, params)
            except Exception as e:
                raise ParameterBindingError(query_name, str(e)) from e

            row = cursor.fetchone()
            if row is None:
                return None

            # Handle dict rows (e.g., psycopg dict_row)
            if isinstance(row, dict):
                # Return the first value in column order
                return next(iter(row.values()))

            # Tuple-like row
            return row[0]

    def execute(
        self,
        query_name: str,
        params: dict[str, Any] | None = None,
    ) -> int:
        """Execute a write query. Returns affected row count."""
        sql = self._registry.get(query_name)
        sql = normalize_params(sql, self._paramstyle)

        with self._connection_manager.get_connection() as conn:
            try:
                cursor = self._connection_manager.adapter.execute(conn, sql, params)
            except Exception as e:
                raise ParameterBindingError(query_name, str(e)) from e

            conn.commit()
            return int(cursor.rowcount)

    def transaction(self) -> TransactionManager:
        """Create a new transaction context manager."""
        if self._connection_manager._pool is None:
            self._connection_manager.initialize_pool()
        pool = self._connection_manager._pool
        conn = self._connection_manager.adapter.acquire_connection(pool)
        return TransactionManager(
            connection=conn,
            adapter=self._connection_manager.adapter,
            registry=self._registry,
            pool=pool,
        )


class AsyncEngine:
    """Asynchronous query execution engine."""

    def __init__(
        self,
        connection_manager: AsyncConnectionManager,
        registry: SQLRegistry,
    ) -> None:
        self._connection_manager = connection_manager
        self._registry = registry
        self._paramstyle = connection_manager.adapter.paramstyle

    @classmethod
    def from_config(
        cls,
        config: Any,
        registry: SQLRegistry,
    ) -> AsyncEngine:
        """Create an AsyncEngine from a ConnectionConfig and SQLRegistry.

        Args:
            config: ConnectionConfig instance
            registry: SQLRegistry instance

        Returns:
            AsyncEngine instance
        """
        connection_manager = AsyncConnectionManager(config)
        return cls(connection_manager, registry)

    async def fetch_one(
        self,
        query_name: str,
        params: dict[str, Any] | None = None,
        *,
        mapper: Any | None = None,
    ) -> Any:
        """Fetch a single row asynchronously."""
        sql = self._registry.get(query_name)
        sql = normalize_params(sql, self._paramstyle)

        async with self._connection_manager.get_connection() as conn:
            try:
                cursor = await self._connection_manager.adapter.execute_async(conn, sql, params)
            except Exception as e:
                raise ParameterBindingError(query_name, str(e)) from e

            if cursor.description is None:
                return None
            columns = [desc[0] for desc in cursor.description]
            rows_raw = await cursor.fetchall()

            # Handle dict rows vs tuple rows
            if rows_raw and isinstance(rows_raw[0], dict):
                rows = [dict(row) for row in rows_raw]
            else:
                rows = [dict(zip(columns, row, strict=True)) for row in rows_raw]

        if len(rows) == 0:
            return None
        if len(rows) > 1:
            raise MultipleRowsError(query_name, len(rows))

        row = rows[0]
        if mapper is not None:
            return mapper.map_one(row)
        return row

    async def fetch_all(
        self,
        query_name: str,
        params: dict[str, Any] | None = None,
        *,
        mapper: Any | None = None,
    ) -> Any:
        """Fetch all matching rows asynchronously."""
        sql = self._registry.get(query_name)
        sql = normalize_params(sql, self._paramstyle)

        async with self._connection_manager.get_connection() as conn:
            try:
                cursor = await self._connection_manager.adapter.execute_async(conn, sql, params)
            except Exception as e:
                raise ParameterBindingError(query_name, str(e)) from e

            if cursor.description is None:
                return []
            columns = [desc[0] for desc in cursor.description]
            rows_raw = await cursor.fetchall()

            # Handle dict rows vs tuple rows
            if rows_raw and isinstance(rows_raw[0], dict):
                rows = [dict(row) for row in rows_raw]
            else:
                rows = [dict(zip(columns, row, strict=True)) for row in rows_raw]

        if mapper is not None:
            return mapper.map_many(rows)
        return rows

    async def fetch_scalar(
        self,
        query_name: str,
        params: dict[str, Any] | None = None,
    ) -> Any:
        """Fetch a single scalar value asynchronously."""
        sql = self._registry.get(query_name)
        sql = normalize_params(sql, self._paramstyle)

        async with self._connection_manager.get_connection() as conn:
            try:
                cursor = await self._connection_manager.adapter.execute_async(conn, sql, params)
            except Exception as e:
                raise ParameterBindingError(query_name, str(e)) from e

            row = await cursor.fetchone()
            if row is None:
                return None

            # Handle dict rows (e.g., psycopg dict_row)
            if isinstance(row, dict):
                # Return the first value in column order
                return next(iter(row.values()))

            # Tuple-like row
            return row[0]

    async def execute(
        self,
        query_name: str,
        params: dict[str, Any] | None = None,
    ) -> int:
        """Execute a write query asynchronously."""
        sql = self._registry.get(query_name)
        sql = normalize_params(sql, self._paramstyle)

        async with self._connection_manager.get_connection() as conn:
            try:
                cursor = await self._connection_manager.adapter.execute_async(conn, sql, params)
            except Exception as e:
                raise ParameterBindingError(query_name, str(e)) from e

            await conn.commit()
            return int(cursor.rowcount)

    def transaction(self) -> AsyncTransactionManager:
        """Create a new async transaction context manager.

        Returns an async context manager that acquires the connection
        in __aenter__, allowing usage as: async with engine.transaction() as tx:
        """
        return AsyncTransactionManager(
            adapter=self._connection_manager.adapter,
            registry=self._registry,
            connection_manager=self._connection_manager,
        )
