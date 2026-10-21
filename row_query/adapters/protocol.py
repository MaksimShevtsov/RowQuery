"""Database adapter protocols.

Every adapter module MUST implement these protocols. Constitution Principle III
requires identical public interfaces across all adapters.

Adapters deal with single connections only; pooling, transaction reset and
error translation live in ``row_query.core``. Connections returned by
``connect`` must expose DB-API ``commit()`` and ``rollback()`` methods
(coroutines for async adapters).
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from row_query.core.connection import ConnectionConfig


@runtime_checkable
class SyncAdapter(Protocol):
    """Synchronous database adapter protocol."""

    @property
    def paramstyle(self) -> str:
        """Named parameter style: 'named' (:name) or 'pyformat' (%(name)s)."""
        ...

    @property
    def positional_paramstyle(self) -> str:
        """Positional parameter style: 'qmark' (?), 'numeric' (:1) or 'format' (%s)."""
        ...

    @property
    def backslash_escapes(self) -> bool:
        """Whether ``\\`` escapes quotes inside plain string literals (MySQL)."""
        ...

    def connect(self, config: ConnectionConfig) -> Any:
        """Open a new connection."""
        ...

    def close_connection(self, connection: Any) -> None:
        """Close a connection."""
        ...

    def is_connection_alive(self, connection: Any) -> bool:
        """Cheap, local check that a connection has not been closed or broken."""
        ...

    def begin(self, connection: Any) -> None:
        """Start an explicit transaction that also covers DDL statements."""
        ...

    def execute(
        self,
        connection: Any,
        sql: str,
        params: dict[str, Any] | tuple[Any, ...] | None = None,
    ) -> Any:
        """Execute SQL and return a DB-API cursor-like object."""
        ...


@runtime_checkable
class AsyncAdapter(Protocol):
    """Asynchronous database adapter protocol."""

    @property
    def paramstyle(self) -> str:
        """Named parameter style: 'named' (:name) or 'pyformat' (%(name)s)."""
        ...

    @property
    def positional_paramstyle(self) -> str:
        """Positional parameter style: 'qmark' (?), 'numeric' (:1) or 'format' (%s)."""
        ...

    @property
    def backslash_escapes(self) -> bool:
        """Whether ``\\`` escapes quotes inside plain string literals (MySQL)."""
        ...

    async def connect_async(self, config: ConnectionConfig) -> Any:
        """Open a new connection."""
        ...

    async def close_connection_async(self, connection: Any) -> None:
        """Close a connection."""
        ...

    def is_connection_alive(self, connection: Any) -> bool:
        """Cheap, local check that a connection has not been closed or broken."""
        ...

    async def begin_async(self, connection: Any) -> None:
        """Start an explicit transaction that also covers DDL statements."""
        ...

    async def execute_async(
        self,
        connection: Any,
        sql: str,
        params: dict[str, Any] | tuple[Any, ...] | None = None,
    ) -> Any:
        """Execute SQL asynchronously and return a cursor-like object."""
        ...
