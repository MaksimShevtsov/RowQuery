"""Connection configuration and management.

ConnectionConfig is a Pydantic model for type-safe connection config.
ConnectionManager and AsyncConnectionManager use adapter protocols for
pool-based connection lifecycle.
"""

from __future__ import annotations

import importlib
from contextlib import asynccontextmanager, contextmanager
from typing import Any

from pydantic import BaseModel

from row_query.core.exceptions import AdapterError


class ConnectionConfig(BaseModel):
    """Configuration for database connections."""

    driver: str
    host: str | None = None
    port: int | None = None
    user: str | None = None
    password: str | None = None
    database: str
    pool_size: int = 5
    pool_timeout: int = 30
    pool_recycle: int = 1800
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


class ConnectionManager:
    """Synchronous connection manager using SyncAdapter protocol."""

    def __init__(self, config: ConnectionConfig) -> None:
        self.config = config
        self._adapter = _load_adapter(config.driver, "sync")
        self._pool: Any = None

    @property
    def adapter(self) -> Any:
        return self._adapter

    def initialize_pool(self) -> Any:
        """Initialize the connection pool."""
        if self._pool is None:
            self._pool = self._adapter.create_pool(self.config)
        return self._pool

    @contextmanager
    def get_connection(self):  # type: ignore[no-untyped-def]
        """Get a connection from the pool as a context manager."""
        if self._pool is None:
            self.initialize_pool()
        connection = self._adapter.acquire_connection(self._pool)
        try:
            yield connection
        finally:
            self._adapter.release_connection(connection, self._pool)

    def close_pool(self) -> None:
        """Close the connection pool."""
        if self._pool is not None:
            self._adapter.close_pool(self._pool)
            self._pool = None


class AsyncConnectionManager:
    """Asynchronous connection manager using AsyncAdapter protocol."""

    def __init__(self, config: ConnectionConfig) -> None:
        self.config = config
        self._adapter = _load_adapter(config.driver, "async")
        self._pool: Any = None

    @property
    def adapter(self) -> Any:
        return self._adapter

    async def initialize_pool(self) -> Any:
        """Initialize the async connection pool."""
        if self._pool is None:
            self._pool = await self._adapter.create_pool_async(self.config)
        return self._pool

    @asynccontextmanager
    async def get_connection(self):  # type: ignore[no-untyped-def]
        """Get an async connection from the pool as an async context manager."""
        if self._pool is None:
            await self.initialize_pool()
        connection = await self._adapter.acquire_connection_async(self._pool)
        try:
            yield connection
        finally:
            await self._adapter.release_connection_async(connection, self._pool)

    async def close_pool(self) -> None:
        """Close the async connection pool."""
        if self._pool is not None:
            await self._adapter.close_pool_async(self._pool)
            self._pool = None
