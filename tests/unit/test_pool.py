"""Unit tests for the bounded connection pools."""

from __future__ import annotations

import asyncio
import threading
import time
from typing import Any

import pytest

from row_query.core.exceptions import (
    ConnectionError,  # noqa: A004
    PoolError,
    PoolTimeoutError,
)
from row_query.core.pool import AsyncPool, SyncPool


class FakeConn:
    def __init__(self, n: int) -> None:
        self.n = n
        self.closed = False
        self.rollbacks = 0
        self.fail_rollback = False

    def rollback(self) -> None:
        self.rollbacks += 1
        if self.fail_rollback:
            raise RuntimeError("connection is broken")


def make_pool(size: int = 1, timeout: float = 0.2, recycle: float | None = None) -> Any:
    counter = iter(range(1000))

    def close(conn: FakeConn) -> None:
        conn.closed = True

    return SyncPool(
        connect=lambda: FakeConn(next(counter)),
        close=close,
        reset=lambda conn: conn.rollback(),
        is_alive=lambda conn: not conn.closed,
        size=size,
        timeout=timeout,
        recycle=recycle,
    )


class TestSyncPool:
    def test_connections_are_opened_lazily(self) -> None:
        pool = make_pool(size=3)
        assert pool.opened_count == 0
        conn = pool.acquire()
        assert pool.opened_count == 1
        pool.release(conn)
        assert pool.acquire() is conn

    def test_acquire_waits_for_release(self) -> None:
        pool = make_pool(size=1, timeout=2)
        first = pool.acquire()
        threading.Timer(0.05, pool.release, args=(first,)).start()
        assert pool.acquire() is first

    def test_acquire_times_out(self) -> None:
        pool = make_pool(size=1, timeout=0.05)
        pool.acquire()
        start = time.monotonic()
        with pytest.raises(PoolTimeoutError):
            pool.acquire()
        assert time.monotonic() - start < 1

    def test_release_resets_connection(self) -> None:
        pool = make_pool()
        conn = pool.acquire()
        pool.release(conn)
        assert conn.rollbacks == 1

    def test_failed_reset_discards_connection(self) -> None:
        pool = make_pool()
        conn = pool.acquire()
        conn.fail_rollback = True
        pool.release(conn)
        assert conn.closed
        assert pool.opened_count == 0
        assert pool.acquire() is not conn

    def test_dead_connection_is_replaced_on_acquire(self) -> None:
        pool = make_pool()
        conn = pool.acquire()
        pool.release(conn)
        conn.closed = True
        assert pool.acquire() is not conn

    def test_recycle_replaces_old_connections(self) -> None:
        pool = make_pool(recycle=0.01)
        conn = pool.acquire()
        pool.release(conn)
        time.sleep(0.03)
        fresh = pool.acquire()
        assert fresh is not conn
        assert conn.closed

    def test_connect_failure_frees_slot(self) -> None:
        attempts = []

        def connect() -> FakeConn:
            attempts.append(1)
            if len(attempts) == 1:
                raise OSError("server down")
            return FakeConn(1)

        pool = SyncPool(
            connect=connect,
            close=lambda c: None,
            reset=lambda c: None,
            is_alive=lambda c: True,
            size=1,
            timeout=0.1,
        )
        with pytest.raises(ConnectionError, match="server down"):
            pool.acquire()
        assert pool.acquire() is not None

    def test_closed_pool_rejects_acquire(self) -> None:
        pool = make_pool()
        pool.release(pool.acquire())
        pool.close()
        with pytest.raises(PoolError):
            pool.acquire()

    def test_invalid_size(self) -> None:
        with pytest.raises(PoolError):
            make_pool(size=0)


class TestAsyncPool:
    def _pool(self, size: int = 1, timeout: float = 0.2) -> AsyncPool[FakeConn]:
        counter = iter(range(1000))

        async def connect() -> FakeConn:
            return FakeConn(next(counter))

        async def close(conn: FakeConn) -> None:
            conn.closed = True

        async def reset(conn: FakeConn) -> None:
            conn.rollback()

        return AsyncPool(
            connect=connect,
            close=close,
            reset=reset,
            is_alive=lambda c: not c.closed,
            size=size,
            timeout=timeout,
        )

    async def test_more_tasks_than_connections(self) -> None:
        pool = self._pool(size=2, timeout=2)

        async def work() -> int:
            conn = await pool.acquire()
            await asyncio.sleep(0.01)
            await pool.release(conn)
            return conn.n

        results = await asyncio.gather(*(work() for _ in range(10)))
        assert len(results) == 10
        assert pool.opened_count <= 2

    async def test_acquire_times_out(self) -> None:
        pool = self._pool(size=1, timeout=0.05)
        await pool.acquire()
        with pytest.raises(PoolTimeoutError):
            await pool.acquire()

    async def test_failed_reset_discards_connection(self) -> None:
        pool = self._pool()
        conn = await pool.acquire()
        conn.fail_rollback = True
        await pool.release(conn)
        assert conn.closed
        assert await pool.acquire() is not conn
