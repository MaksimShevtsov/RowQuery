"""Bounded connection pools shared by all adapters.

Connections are opened lazily up to ``size``. ``acquire`` blocks (or awaits)
until a connection is free, raising :class:`PoolTimeoutError` after
``timeout`` seconds. On ``release`` every connection is rolled back so that
no transaction state (open read snapshots, locks, aborted transactions)
leaks into the next borrower; connections that fail the reset, are reported
dead by the adapter, or are older than ``recycle`` seconds are discarded.
"""

from __future__ import annotations

import asyncio
import contextlib
import threading
import time
from collections import deque
from collections.abc import Awaitable, Callable
from typing import Generic, TypeVar

from row_query.core.exceptions import (
    ConnectionError,  # noqa: A004
    PoolError,
    PoolTimeoutError,
)

C = TypeVar("C")


class _PoolState(Generic[C]):
    """Bookkeeping shared by the sync and async pools (not thread-safe by itself)."""

    def __init__(self, size: int, timeout: float, recycle: float | None) -> None:
        if size < 1:
            raise PoolError(f"pool_size must be >= 1, got {size}")
        self.size = size
        self.timeout = timeout
        self.recycle = recycle if recycle and recycle > 0 else None
        self.idle: deque[C] = deque()
        self.born: dict[int, float] = {}
        self.opened = 0
        self.closed = False

    def expired(self, conn: C) -> bool:
        if self.recycle is None:
            return False
        return time.monotonic() - self.born.get(id(conn), 0.0) > self.recycle

    def forget(self, conn: C) -> None:
        self.born.pop(id(conn), None)
        self.opened -= 1


class SyncPool(Generic[C]):
    """Thread-safe bounded pool."""

    def __init__(
        self,
        connect: Callable[[], C],
        close: Callable[[C], None],
        reset: Callable[[C], None],
        is_alive: Callable[[C], bool],
        *,
        size: int,
        timeout: float,
        recycle: float | None = None,
    ) -> None:
        self._connect = connect
        self._close = close
        self._reset = reset
        self._is_alive = is_alive
        self._state: _PoolState[C] = _PoolState(size, timeout, recycle)
        self._cond = threading.Condition()

    @property
    def size(self) -> int:
        return self._state.size

    @property
    def idle_count(self) -> int:
        return len(self._state.idle)

    @property
    def opened_count(self) -> int:
        return self._state.opened

    def acquire(self) -> C:
        s = self._state
        deadline = time.monotonic() + s.timeout
        while True:
            idle_conn: C | None = None
            with self._cond:
                while True:
                    if s.closed:
                        raise PoolError("Connection pool is closed")
                    if s.idle:
                        idle_conn = s.idle.pop()
                        break
                    if s.opened < s.size:
                        s.opened += 1
                        break
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise PoolTimeoutError(s.size, s.timeout)
                    self._cond.wait(remaining)
            if idle_conn is None:
                break  # a slot was reserved: open a new connection below
            # Health checks run outside the lock so other threads are not blocked.
            if not s.expired(idle_conn) and self._is_alive(idle_conn):
                return idle_conn
            with self._cond:
                s.forget(idle_conn)
                self._cond.notify()
            self._safe_close(idle_conn)

        try:
            conn = self._connect()
        except Exception as e:
            with self._cond:
                s.opened -= 1
                self._cond.notify()
            raise ConnectionError(f"Failed to open database connection: {e}") from e
        with self._cond:
            s.born[id(conn)] = time.monotonic()
        return conn

    def release(self, conn: C) -> None:
        s = self._state
        try:
            self._reset(conn)
            healthy = True
        except Exception:
            healthy = False
        with self._cond:
            keep = healthy and not s.closed and not s.expired(conn)
            if keep:
                s.idle.append(conn)
            else:
                s.forget(conn)
            self._cond.notify()
        if not keep:
            self._safe_close(conn)

    def close(self) -> None:
        with self._cond:
            s = self._state
            s.closed = True
            idle = list(s.idle)
            s.idle.clear()
            for conn in idle:
                s.forget(conn)
            self._cond.notify_all()
        for conn in idle:
            self._safe_close(conn)

    def _safe_close(self, conn: C) -> None:
        with contextlib.suppress(Exception):
            self._close(conn)


class AsyncPool(Generic[C]):
    """asyncio bounded pool (use from a single event loop)."""

    def __init__(
        self,
        connect: Callable[[], Awaitable[C]],
        close: Callable[[C], Awaitable[None]],
        reset: Callable[[C], Awaitable[None]],
        is_alive: Callable[[C], bool],
        *,
        size: int,
        timeout: float,
        recycle: float | None = None,
    ) -> None:
        self._connect = connect
        self._close = close
        self._reset = reset
        self._is_alive = is_alive
        self._state: _PoolState[C] = _PoolState(size, timeout, recycle)
        self._cond: asyncio.Condition | None = None

    @property
    def size(self) -> int:
        return self._state.size

    @property
    def idle_count(self) -> int:
        return len(self._state.idle)

    @property
    def opened_count(self) -> int:
        return self._state.opened

    def _condition(self) -> asyncio.Condition:
        if self._cond is None:
            self._cond = asyncio.Condition()
        return self._cond

    async def acquire(self) -> C:
        s = self._state
        cond = self._condition()
        deadline = time.monotonic() + s.timeout
        async with cond:
            while True:
                if s.closed:
                    raise PoolError("Connection pool is closed")
                if s.idle:
                    conn = s.idle.pop()
                    if s.expired(conn) or not self._is_alive(conn):
                        s.forget(conn)
                        await self._safe_close(conn)
                        continue
                    return conn
                if s.opened < s.size:
                    s.opened += 1
                    break
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise PoolTimeoutError(s.size, s.timeout)
                try:
                    await asyncio.wait_for(cond.wait(), remaining)
                except asyncio.TimeoutError:
                    raise PoolTimeoutError(s.size, s.timeout) from None

        try:
            conn = await self._connect()
        except BaseException as e:
            async with cond:
                s.opened -= 1
                cond.notify()
            if isinstance(e, Exception):
                raise ConnectionError(f"Failed to open database connection: {e}") from e
            raise
        s.born[id(conn)] = time.monotonic()
        return conn

    async def release(self, conn: C) -> None:
        s = self._state
        try:
            await self._reset(conn)
            healthy = True
        except Exception:
            healthy = False
        keep = healthy and not s.closed and not s.expired(conn)
        if not keep:
            s.forget(conn)
            await self._safe_close(conn)
        cond = self._condition()
        async with cond:
            if keep:
                s.idle.append(conn)
            cond.notify()

    async def close(self) -> None:
        s = self._state
        s.closed = True
        idle = list(s.idle)
        s.idle.clear()
        for conn in idle:
            s.forget(conn)
            await self._safe_close(conn)
        cond = self._condition()
        async with cond:
            cond.notify_all()

    async def _safe_close(self, conn: C) -> None:
        with contextlib.suppress(Exception):
            await self._close(conn)


__all__ = ["AsyncPool", "SyncPool"]
