"""Database adapter protocols.

Every adapter module MUST implement these protocols. Constitution Principle III
requires identical public interfaces across all adapters.
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from row_query.core.connection import ConnectionConfig


@runtime_checkable
class SyncAdapter(Protocol):
    """Synchronous database adapter protocol."""

    @property
    def paramstyle(self) -> str:
        """Parameter binding style: 'named' (:name) or 'pyformat' (%(name)s)."""
        ...

    def create_pool(self, config: ConnectionConfig) -> Any:
        """Create a connection pool."""
        ...

    def acquire_connection(self, pool: Any) -> Any:
        """Acquire a connection from the pool."""
        ...

    def release_connection(self, connection: Any, pool: Any) -> None:
        """Release a connection back to the pool."""
        ...

    def close_pool(self, pool: Any) -> None:
        """Close the pool and release all connections."""
        ...

    def execute(
        self,
        connection: Any,
        sql: str,
        params: dict[str, Any] | None = None,
    ) -> Any:
        """Execute SQL and return a cursor-like object."""
        ...


@runtime_checkable
class AsyncAdapter(Protocol):
    """Asynchronous database adapter protocol."""

    @property
    def paramstyle(self) -> str:
        """Parameter binding style: 'named' (:name) or 'pyformat' (%(name)s)."""
        ...

    async def create_pool_async(self, config: ConnectionConfig) -> Any:
        """Create an async connection pool."""
        ...

    async def acquire_connection_async(self, pool: Any) -> Any:
        """Acquire a connection from the async pool."""
        ...

    async def release_connection_async(self, connection: Any, pool: Any) -> None:
        """Release a connection back to the async pool."""
        ...

    async def close_pool_async(self, pool: Any) -> None:
        """Close the async pool."""
        ...

    async def execute_async(
        self,
        connection: Any,
        sql: str,
        params: dict[str, Any] | None = None,
    ) -> Any:
        """Execute SQL asynchronously and return a cursor-like object."""
        ...
