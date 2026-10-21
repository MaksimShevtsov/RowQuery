"""Connection configuration and management.

ConnectionConfig is a Pydantic model for type-safe connection config.
ConnectionManager and AsyncConnectionManager own a bounded connection pool
(see ``row_query.core.pool``) built on top of an adapter.
"""

from __future__ import annotations

import importlib
import threading
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager, contextmanager
from typing import Any

from pydantic import BaseModel

from row_query.core.exceptions import AdapterError
from row_query.core.pool import AsyncPool, SyncPool


class ConnectionConfig(BaseModel):
    """Configuration for database connections.

    Attributes:
        pool_size: Maximum number of open connections. Connections are opened
            lazily on demand.
        pool_timeout: Seconds to wait for a free connection before raising
            ``PoolTimeoutError``.
        pool_recycle: Close and reopen connections older than this many
            seconds (``0`` disables). Ignored for SQLite.
        extra: Additional driver-specific connection arguments.
    """

    driver: str
    host: str | None = None
    port: int | None = None
    user: str | None = None
    password: str | None = None
    database: str
    pool_size: int = 5
    pool_timeout: float = 30
    pool_recycle: float = 1800
    extra: dict[str, Any] = {}


# Adapter module mapping: driver name → (module_path, sync_class, async_class)
_ADAPTER_MAP: dict[str, tuple[str, str, str]] = {
    "sqlite": ("row_query.adapters.sqlite", "SqliteSyncAdapter", "SqliteAsyncAdapter"),
    "postgresql": (
        "row_query.adapters.postgresql",
        "PostgresqlSyncAdapter",
        "PostgresqlAsyncAdapter",
    ),
    "mysql": ("row_query.adapters.mysql", "MysqlSyncAdapter", "MysqlAsyncAdapter"),
    "oracle": ("row_query.adapters.oracle", "OracleSyncAdapter", "OracleAsyncAdapter"),
}


def _load_adapter(driver: str, kind: str) -> Any:
    """Load a sync or async adapter by driver name."""
    driver_lower = driver.lower()
    if driver_lower not in _ADAPTER_MAP:
        raise AdapterError(f"Unsupported database driver: {driver}")

    module_path, sync_cls_name, async_cls_name = _ADAPTER_MAP[driver_lower]
    cls_name = sync_cls_name if kind == "sync" else async_cls_name

    try:
        module = importlib.import_module(module_path)
        return getattr(module, cls_name)()
    except (ImportError, AttributeError) as e:
        raise AdapterError(f"Failed to load {kind} adapter for '{driver}': {e}") from e


def _recycle_seconds(config: ConnectionConfig) -> float | None:
    # SQLite has no server-side idle timeouts, and recycling a ":memory:"
    # connection would silently drop the database.
    if config.driver.lower() == "sqlite" or config.pool_recycle <= 0:
        return None
    return config.pool_recycle


class ConnectionManager:
    """Synchronous connection manager backed by a thread-safe pool."""

    def __init__(self, config: ConnectionConfig) -> None:
        self.config = config
        self._adapter = _load_adapter(config.driver, "sync")
        self._pool: SyncPool[Any] | None = None
        self._pool_lock = threading.Lock()
        # Pool each borrowed connection came from (survives close_pool()).
        self._owners: dict[int, SyncPool[Any]] = {}

    @property
    def adapter(self) -> Any:
        return self._adapter

    def initialize_pool(self) -> SyncPool[Any]:
        """Create the connection pool (connections are opened lazily)."""
        with self._pool_lock:
            if self._pool is not None:
                return self._pool
            adapter, config = self._adapter, self.config
            self._pool = SyncPool(
                connect=lambda: adapter.connect(config),
                close=adapter.close_connection,
                reset=lambda conn: conn.rollback(),
                is_alive=adapter.is_connection_alive,
                size=config.pool_size,
                timeout=config.pool_timeout,
                recycle=_recycle_seconds(config),
            )
            return self._pool

    def acquire(self) -> Any:
        """Borrow a connection; it must be returned with :meth:`release`."""
        pool = self.initialize_pool()
        connection = pool.acquire()
        self._owners[id(connection)] = pool
        return connection

    def release(self, connection: Any) -> None:
        """Return a connection to its pool, rolling back any uncommitted work.

        If that pool has been closed in the meantime, the connection is closed.
        """
        pool = self._owners.pop(id(connection), None) or self.initialize_pool()
        pool.release(connection)

    @contextmanager
    def get_connection(self) -> Iterator[Any]:
        """Borrow a connection for the duration of the ``with`` block.

        Uncommitted work is rolled back when the block exits.
        """
        connection = self.acquire()
        try:
            yield connection
        finally:
            self.release(connection)

    def close_pool(self) -> None:
        """Close the connection pool.

        Idle connections are closed now; borrowed ones when they are released.
        """
        with self._pool_lock:
            pool, self._pool = self._pool, None
        if pool is not None:
            pool.close()


class AsyncConnectionManager:
    """Asynchronous connection manager backed by an asyncio pool."""

    def __init__(self, config: ConnectionConfig) -> None:
        self.config = config
        self._adapter = _load_adapter(config.driver, "async")
        self._pool: AsyncPool[Any] | None = None
        self._owners: dict[int, AsyncPool[Any]] = {}

    @property
    def adapter(self) -> Any:
        return self._adapter

    async def initialize_pool(self) -> AsyncPool[Any]:
        """Create the async connection pool (connections are opened lazily)."""
        if self._pool is None:
            adapter, config = self._adapter, self.config

            async def reset(conn: Any) -> None:
                await conn.rollback()

            self._pool = AsyncPool(
                connect=lambda: adapter.connect_async(config),
                close=adapter.close_connection_async,
                reset=reset,
                is_alive=adapter.is_connection_alive,
                size=config.pool_size,
                timeout=config.pool_timeout,
                recycle=_recycle_seconds(config),
            )
        return self._pool

    async def acquire(self) -> Any:
        """Borrow a connection; it must be returned with :meth:`release`."""
        pool = await self.initialize_pool()
        connection = await pool.acquire()
        self._owners[id(connection)] = pool
        return connection

    async def release(self, connection: Any) -> None:
        """Return a connection to its pool, rolling back any uncommitted work.

        If that pool has been closed in the meantime, the connection is closed.
        """
        pool = self._owners.pop(id(connection), None) or await self.initialize_pool()
        await pool.release(connection)

    @asynccontextmanager
    async def get_connection(self) -> AsyncIterator[Any]:
        """Borrow a connection for the duration of the ``async with`` block.

        Uncommitted work is rolled back when the block exits.
        """
        connection = await self.acquire()
        try:
            yield connection
        finally:
            await self.release(connection)

    async def close_pool(self) -> None:
        """Close the async connection pool.

        Idle connections are closed now; borrowed ones when they are released.
        """
        pool, self._pool = self._pool, None
        if pool is not None:
            await pool.close()
