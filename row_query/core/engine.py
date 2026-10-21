"""Query execution engine.

The Engine resolves named queries from the SQLRegistry, binds parameters,
executes through the adapter, and optionally applies a mapper to results.
Each call borrows a pooled connection; writes made through ``execute`` are
committed immediately, and any other open transaction state is rolled back
when the connection returns to the pool.
"""

from __future__ import annotations

from contextlib import AbstractAsyncContextManager, AbstractContextManager
from typing import Any, TypeVar

from row_query.core.connection import AsyncConnectionManager, ConnectionManager
from row_query.core.execution import AsyncExecutor, SyncExecutor
from row_query.core.registry import SQLRegistry
from row_query.core.sanitizer import SQLSanitizer
from row_query.core.transaction import AsyncTransactionManager, TransactionManager

T = TypeVar("T")


class Engine(SyncExecutor):
    """Synchronous query execution engine (safe to share between threads)."""

    def __init__(
        self,
        connection_manager: ConnectionManager,
        registry: SQLRegistry,
        sanitizer: SQLSanitizer | None = None,
    ) -> None:
        self._connection_manager = connection_manager
        self._registry = registry
        self._sanitizer = sanitizer
        self._adapter = connection_manager.adapter

    @classmethod
    def from_config(
        cls,
        config: Any,
        registry: SQLRegistry,
        sanitizer: SQLSanitizer | None = None,
    ) -> Engine:
        """Create an Engine from a ConnectionConfig and SQLRegistry.

        Args:
            config: ConnectionConfig instance
            registry: SQLRegistry instance
            sanitizer: Optional SQLSanitizer applied to inline SQL strings.

        Returns:
            Engine instance
        """
        connection_manager = ConnectionManager(config)
        return cls(connection_manager, registry, sanitizer)

    def _connection(self) -> AbstractContextManager[Any]:
        return self._connection_manager.get_connection()

    def transaction(self) -> TransactionManager:
        """Create a transaction context manager.

        The connection is acquired when the ``with`` block is entered.
        """
        return TransactionManager(
            connection_manager=self._connection_manager,
            registry=self._registry,
            sanitizer=self._sanitizer,
        )

    def close(self) -> None:
        """Close all pooled connections."""
        self._connection_manager.close_pool()


class AsyncEngine(AsyncExecutor):
    """Asynchronous query execution engine."""

    def __init__(
        self,
        connection_manager: AsyncConnectionManager,
        registry: SQLRegistry,
        sanitizer: SQLSanitizer | None = None,
    ) -> None:
        self._connection_manager = connection_manager
        self._registry = registry
        self._sanitizer = sanitizer
        self._adapter = connection_manager.adapter

    @classmethod
    def from_config(
        cls,
        config: Any,
        registry: SQLRegistry,
        sanitizer: SQLSanitizer | None = None,
    ) -> AsyncEngine:
        """Create an AsyncEngine from a ConnectionConfig and SQLRegistry.

        Args:
            config: ConnectionConfig instance
            registry: SQLRegistry instance
            sanitizer: Optional SQLSanitizer applied to inline SQL strings.

        Returns:
            AsyncEngine instance
        """
        connection_manager = AsyncConnectionManager(config)
        return cls(connection_manager, registry, sanitizer)

    def _connection(self) -> AbstractAsyncContextManager[Any]:
        return self._connection_manager.get_connection()

    def transaction(self) -> AsyncTransactionManager:
        """Create an async transaction context manager.

        The connection is acquired in ``__aenter__``, allowing usage as:
        ``async with engine.transaction() as tx:``
        """
        return AsyncTransactionManager(
            connection_manager=self._connection_manager,
            registry=self._registry,
            sanitizer=self._sanitizer,
        )

    async def close(self) -> None:
        """Close all pooled connections."""
        await self._connection_manager.close_pool()
