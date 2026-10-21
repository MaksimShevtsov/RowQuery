"""Transaction management.

Provides context managers for executing multiple SQL statements atomically.
Auto-commits on success, auto-rolls-back on exception. The connection is
borrowed from the pool on enter and returned on exit.

Transactions expose the same query API as engines (``fetch_one``,
``fetch_all``, ``fetch_scalar``, ``execute``, ``mapper=``), but writes are
only committed when the transaction commits.
"""

from __future__ import annotations

from contextlib import AbstractAsyncContextManager, AbstractContextManager, nullcontext
from enum import Enum
from typing import TYPE_CHECKING, Any

from row_query.core.exceptions import TransactionStateError
from row_query.core.execution import AsyncExecutor, SyncExecutor, translate_error
from row_query.core.registry import SQLRegistry
from row_query.core.sanitizer import SQLSanitizer

if TYPE_CHECKING:
    from row_query.core.connection import AsyncConnectionManager, ConnectionManager

_TX_LABEL = "<transaction>"


class _TxState(Enum):
    IDLE = "idle"
    ACTIVE = "active"
    COMMITTED = "committed"
    ROLLED_BACK = "rolled_back"


def _check_active(state: _TxState) -> None:
    if state != _TxState.ACTIVE:
        raise TransactionStateError(state.value, "execute")


def _check_can_commit(state: _TxState) -> None:
    if state in (_TxState.COMMITTED, _TxState.ROLLED_BACK, _TxState.IDLE):
        raise TransactionStateError(state.value, "commit")


def _check_can_rollback(state: _TxState) -> None:
    if state in (_TxState.COMMITTED, _TxState.IDLE):
        raise TransactionStateError(state.value, "rollback")


class TransactionManager(SyncExecutor):
    """Synchronous transaction context manager."""

    _autocommit = False

    def __init__(
        self,
        connection_manager: ConnectionManager,
        registry: SQLRegistry,
        sanitizer: SQLSanitizer | None = None,
    ) -> None:
        self._connection_manager = connection_manager
        self._adapter = connection_manager.adapter
        self._registry = registry
        self._sanitizer = sanitizer
        self._conn: Any = None
        self._state = _TxState.IDLE

    def __enter__(self) -> TransactionManager:
        if self._state != _TxState.IDLE:
            raise TransactionStateError(self._state.value, "begin")
        conn = self._connection_manager.acquire()
        try:
            self._adapter.begin(conn)
        except Exception as e:
            self._connection_manager.release(conn)
            raise translate_error(_TX_LABEL, e) from e
        self._conn = conn
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
                    self.rollback()
                else:
                    self.commit()
        finally:
            conn, self._conn = self._conn, None
            if conn is not None:
                self._connection_manager.release(conn)

    def _connection(self) -> AbstractContextManager[Any]:
        _check_active(self._state)
        return nullcontext(self._conn)

    def commit(self) -> None:
        """Explicitly commit the transaction."""
        _check_can_commit(self._state)
        try:
            self._conn.commit()
        except Exception as e:
            self._state = _TxState.ROLLED_BACK
            raise translate_error(_TX_LABEL, e) from e
        self._state = _TxState.COMMITTED

    def rollback(self) -> None:
        """Explicitly rollback the transaction."""
        _check_can_rollback(self._state)
        self._state = _TxState.ROLLED_BACK
        try:
            self._conn.rollback()
        except Exception as e:
            raise translate_error(_TX_LABEL, e) from e


class AsyncTransactionManager(AsyncExecutor):
    """Asynchronous transaction context manager."""

    _autocommit = False

    def __init__(
        self,
        connection_manager: AsyncConnectionManager,
        registry: SQLRegistry,
        sanitizer: SQLSanitizer | None = None,
    ) -> None:
        self._connection_manager = connection_manager
        self._adapter = connection_manager.adapter
        self._registry = registry
        self._sanitizer = sanitizer
        self._conn: Any = None
        self._state = _TxState.IDLE

    async def __aenter__(self) -> AsyncTransactionManager:
        if self._state != _TxState.IDLE:
            raise TransactionStateError(self._state.value, "begin")
        conn = await self._connection_manager.acquire()
        try:
            await self._adapter.begin_async(conn)
        except Exception as e:
            await self._connection_manager.release(conn)
            raise translate_error(_TX_LABEL, e) from e
        self._conn = conn
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
                    await self.rollback()
                else:
                    await self.commit()
        finally:
            conn, self._conn = self._conn, None
            if conn is not None:
                await self._connection_manager.release(conn)

    def _connection(self) -> AbstractAsyncContextManager[Any]:
        _check_active(self._state)
        return nullcontext(self._conn)

    async def commit(self) -> None:
        """Explicitly commit the async transaction."""
        _check_can_commit(self._state)
        try:
            await self._conn.commit()
        except Exception as e:
            self._state = _TxState.ROLLED_BACK
            raise translate_error(_TX_LABEL, e) from e
        self._state = _TxState.COMMITTED

    async def rollback(self) -> None:
        """Explicitly rollback the async transaction."""
        _check_can_rollback(self._state)
        self._state = _TxState.ROLLED_BACK
        try:
            await self._conn.rollback()
        except Exception as e:
            raise translate_error(_TX_LABEL, e) from e
