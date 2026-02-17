"""Transaction management.

Provides context managers for executing multiple SQL statements atomically.
Auto-commits on success, auto-rolls-back on exception.
"""

from __future__ import annotations

from enum import Enum
from typing import Any

from row_query.core.exceptions import TransactionStateError
from row_query.core.params import normalize_params
from row_query.core.registry import SQLRegistry


class _TxState(Enum):
    IDLE = "idle"
    ACTIVE = "active"
    COMMITTED = "committed"
    ROLLED_BACK = "rolled_back"


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

    # Check if rows are already dict-like
    first_row = rows[0]
    if isinstance(first_row, dict):
        return [dict(row) for row in rows]

    # Tuple-like rows, zip with columns
    return [dict(zip(columns, row, strict=True)) for row in rows]


class TransactionManager:
    """Synchronous transaction context manager."""

    def __init__(
        self,
        connection: Any,
        adapter: Any,
        registry: SQLRegistry,
        pool: Any = None,
    ) -> None:
        self._connection = connection
        self._adapter = adapter
        self._registry = registry
        self._pool = pool
        self._paramstyle: str = adapter.paramstyle
        self._state = _TxState.IDLE

    def __enter__(self) -> TransactionManager:
        self._state = _TxState.ACTIVE
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        try:
            if self._state == _TxState.ACTIVE:
                if exc_type is not None:
                    self._connection.rollback()
                    self._state = _TxState.ROLLED_BACK
                else:
                    self._connection.commit()
                    self._state = _TxState.COMMITTED
        finally:
            if self._pool is not None:
                self._adapter.release_connection(self._connection, self._pool)

    def execute(
        self,
        query_name: str,
        params: dict[str, Any] | None = None,
    ) -> int:
        """Execute a write query within this transaction."""
        self._check_active()
        sql = self._registry.get(query_name)
        sql = normalize_params(sql, self._paramstyle)
        cursor = self._adapter.execute(self._connection, sql, params)
        return int(cursor.rowcount)

    def fetch_one(
        self,
        query_name: str,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        """Fetch a single row within transaction context."""
        self._check_active()
        sql = self._registry.get(query_name)
        sql = normalize_params(sql, self._paramstyle)
        cursor = self._adapter.execute(self._connection, sql, params)
        rows = _rows_to_dicts(cursor)
        if not rows:
            return None
        return rows[0]

    def fetch_all(
        self,
        query_name: str,
        params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch all rows within transaction context."""
        self._check_active()
        sql = self._registry.get(query_name)
        sql = normalize_params(sql, self._paramstyle)
        cursor = self._adapter.execute(self._connection, sql, params)
        return _rows_to_dicts(cursor)

    def commit(self) -> None:
        """Explicitly commit the transaction."""
        if self._state == _TxState.ROLLED_BACK:
            raise TransactionStateError("rolled_back", "commit")
        if self._state == _TxState.COMMITTED:
            raise TransactionStateError("committed", "commit")
        self._connection.commit()
        self._state = _TxState.COMMITTED

    def rollback(self) -> None:
        """Explicitly rollback the transaction."""
        if self._state == _TxState.COMMITTED:
            raise TransactionStateError("committed", "rollback")
        self._connection.rollback()
        self._state = _TxState.ROLLED_BACK

    def _check_active(self) -> None:
        if self._state == _TxState.IDLE:
            raise TransactionStateError("idle", "execute")
        if self._state == _TxState.COMMITTED:
            raise TransactionStateError("committed", "execute")
        if self._state == _TxState.ROLLED_BACK:
            raise TransactionStateError("rolled_back", "execute")


class AsyncTransactionManager:
    """Asynchronous transaction context manager."""

    def __init__(
        self,
        adapter: Any,
        registry: SQLRegistry,
        connection_manager: Any,
        connection: Any = None,
        pool: Any = None,
    ) -> None:
        self._connection = connection
        self._adapter = adapter
        self._registry = registry
        self._pool = pool
        self._connection_manager = connection_manager
        self._paramstyle: str = adapter.paramstyle
        self._state = _TxState.IDLE

    async def __aenter__(self) -> AsyncTransactionManager:
        # Acquire connection if not already provided
        if self._connection is None:
            if self._connection_manager._pool is None:
                await self._connection_manager.initialize_pool()
            self._pool = self._connection_manager._pool
            self._connection = await self._adapter.acquire_connection_async(self._pool)
        self._state = _TxState.ACTIVE
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        try:
            if self._state == _TxState.ACTIVE:
                if exc_type is not None:
                    await self._connection.rollback()
                    self._state = _TxState.ROLLED_BACK
                else:
                    await self._connection.commit()
                    self._state = _TxState.COMMITTED
        finally:
            if self._pool is not None:
                await self._adapter.release_connection_async(self._connection, self._pool)

    async def execute(
        self,
        query_name: str,
        params: dict[str, Any] | None = None,
    ) -> int:
        """Execute a write query within this async transaction."""
        self._check_active()
        sql = self._registry.get(query_name)
        sql = normalize_params(sql, self._paramstyle)
        cursor = await self._adapter.execute_async(self._connection, sql, params)
        return int(cursor.rowcount)

    async def fetch_one(
        self,
        query_name: str,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        """Fetch a single row within async transaction context."""
        self._check_active()
        sql = self._registry.get(query_name)
        sql = normalize_params(sql, self._paramstyle)
        cursor = await self._adapter.execute_async(self._connection, sql, params)
        if cursor.description is None:
            return None
        columns = [desc[0] for desc in cursor.description]
        rows_raw = await cursor.fetchall()

        # Handle dict rows vs tuple rows
        if rows_raw and isinstance(rows_raw[0], dict):
            rows = [dict(row) for row in rows_raw]
        else:
            rows = [dict(zip(columns, row, strict=True)) for row in rows_raw]

        if not rows:
            return None
        return rows[0]

    async def fetch_all(
        self,
        query_name: str,
        params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch all rows within async transaction context."""
        self._check_active()
        sql = self._registry.get(query_name)
        sql = normalize_params(sql, self._paramstyle)
        cursor = await self._adapter.execute_async(self._connection, sql, params)
        if cursor.description is None:
            return []
        columns = [desc[0] for desc in cursor.description]
        rows_raw = await cursor.fetchall()

        # Handle dict rows vs tuple rows
        if rows_raw and isinstance(rows_raw[0], dict):
            return [dict(row) for row in rows_raw]
        else:
            return [dict(zip(columns, row, strict=True)) for row in rows_raw]

    async def commit(self) -> None:
        """Explicitly commit the async transaction."""
        if self._state == _TxState.ROLLED_BACK:
            raise TransactionStateError("rolled_back", "commit")
        if self._state == _TxState.COMMITTED:
            raise TransactionStateError("committed", "commit")
        await self._connection.commit()
        self._state = _TxState.COMMITTED

    async def rollback(self) -> None:
        """Explicitly rollback the async transaction."""
        if self._state == _TxState.COMMITTED:
            raise TransactionStateError("committed", "rollback")
        await self._connection.rollback()
        self._state = _TxState.ROLLED_BACK

    def _check_active(self) -> None:
        if self._state == _TxState.IDLE:
            raise TransactionStateError("idle", "execute")
        if self._state == _TxState.COMMITTED:
            raise TransactionStateError("committed", "execute")
        if self._state == _TxState.ROLLED_BACK:
            raise TransactionStateError("rolled_back", "execute")
