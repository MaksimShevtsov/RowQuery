"""Integration tests against real PostgreSQL / MySQL servers.

Enabled by environment variables (skipped otherwise):

    ROWQUERY_TEST_POSTGRESQL=postgresql://user:password@host:port/dbname
    ROWQUERY_TEST_MYSQL=mysql://user:password@host:port/dbname
"""

from __future__ import annotations

import asyncio
import os
import sys
import uuid
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from urllib.parse import unquote, urlparse

import pytest

from row_query.core.connection import ConnectionConfig, ConnectionManager
from row_query.core.engine import AsyncEngine, Engine
from row_query.core.exceptions import IntegrityError, QueryExecutionError
from row_query.core.migration import MigrationManager
from row_query.core.registry import SQLRegistry

pytestmark = pytest.mark.integration

_BACKENDS = {"postgresql": "ROWQUERY_TEST_POSTGRESQL", "mysql": "ROWQUERY_TEST_MYSQL"}


@pytest.fixture(scope="session")
def event_loop_policy() -> asyncio.AbstractEventLoopPolicy:
    # psycopg's async mode cannot use the Windows default ProactorEventLoop.
    if sys.platform == "win32":
        return asyncio.WindowsSelectorEventLoopPolicy()  # type: ignore[attr-defined,no-any-return]
    return asyncio.DefaultEventLoopPolicy()


def _config(driver: str, **overrides: object) -> ConnectionConfig:
    url = os.environ.get(_BACKENDS[driver])
    if not url:
        pytest.skip(f"{_BACKENDS[driver]} not set")
    pytest.importorskip("psycopg" if driver == "postgresql" else "mysql.connector")
    parsed = urlparse(url)
    return ConnectionConfig(
        driver=driver,
        host=parsed.hostname,
        port=parsed.port,
        user=unquote(parsed.username or ""),
        password=unquote(parsed.password or ""),
        database=parsed.path.lstrip("/"),
        **overrides,  # type: ignore[arg-type]
    )


@pytest.fixture(params=list(_BACKENDS))
def driver(request: pytest.FixtureRequest) -> str:
    return str(request.param)


@pytest.fixture
def table() -> str:
    return f"rq_{uuid.uuid4().hex[:10]}"


@pytest.fixture
def engine(driver: str, table: str, tmp_path: Path) -> Iterator[Engine]:
    eng = Engine.from_config(_config(driver, pool_size=1, pool_timeout=5), SQLRegistry(tmp_path))
    eng.execute(f"CREATE TABLE {table} (id INTEGER PRIMARY KEY, name VARCHAR(50) UNIQUE)")
    eng.execute(f"INSERT INTO {table} (id, name) VALUES (1, 'Alice'), (2, 'Bob')")
    yield eng
    eng.execute(f"DROP TABLE {table}")
    eng.close()


class TestSyncEngine:
    def test_like_percent_with_named_param(self, engine: Engine, table: str) -> None:
        rows = engine.fetch_all(
            f"SELECT name FROM {table} WHERE name LIKE 'A%' AND id = :id", {"id": 1}
        )
        assert rows == [{"name": "Alice"}]

    def test_like_percent_without_params(self, engine: Engine, table: str) -> None:
        assert engine.fetch_scalar(f"SELECT COUNT(*) FROM {table} WHERE name LIKE '%o%'") == 1

    def test_portable_qmark_placeholders(self, engine: Engine, table: str) -> None:
        rows = engine.fetch_all(f"SELECT id FROM {table} WHERE id IN (?, ?) ORDER BY id", [1, 2])
        assert [r["id"] for r in rows] == [1, 2]

    def test_reads_see_other_sessions_commits(
        self, engine: Engine, driver: str, table: str, tmp_path: Path
    ) -> None:
        assert engine.fetch_scalar(f"SELECT COUNT(*) FROM {table}") == 2
        other = Engine.from_config(_config(driver), SQLRegistry(tmp_path))
        other.execute(f"INSERT INTO {table} (id, name) VALUES (3, 'Carol')")
        other.close()
        assert engine.fetch_scalar(f"SELECT COUNT(*) FROM {table}") == 3

    def test_failed_statement_does_not_poison_connection(self, engine: Engine, table: str) -> None:
        with pytest.raises(QueryExecutionError):
            engine.fetch_all(f"SELEC * FROM {table}")
        assert engine.fetch_scalar("SELECT 1") == 1

    def test_integrity_error(self, engine: Engine, table: str) -> None:
        with pytest.raises(IntegrityError):
            engine.execute(
                f"INSERT INTO {table} (id, name) VALUES (:id, :name)", {"id": 9, "name": "Bob"}
            )
        assert engine.fetch_scalar(f"SELECT COUNT(*) FROM {table}") == 2

    def test_partial_scalar_read_then_next_query(self, engine: Engine, table: str) -> None:
        assert engine.fetch_scalar(f"SELECT id FROM {table} ORDER BY id") == 1
        assert engine.fetch_scalar("SELECT 2") == 2

    def test_transaction_commit_and_rollback(self, engine: Engine, table: str) -> None:
        with engine.transaction() as tx:
            tx.execute(f"INSERT INTO {table} (id, name) VALUES (3, 'Carol')")
        with pytest.raises(RuntimeError), engine.transaction() as tx:
            tx.execute(f"INSERT INTO {table} (id, name) VALUES (4, 'Dan')")
            raise RuntimeError("abort")
        assert engine.fetch_scalar(f"SELECT COUNT(*) FROM {table}") == 3

    def test_no_idle_in_transaction_after_reads(
        self, engine: Engine, driver: str, table: str
    ) -> None:
        if driver != "postgresql":
            pytest.skip("pg_stat_activity is PostgreSQL-specific")
        engine.fetch_all(f"SELECT * FROM {table}")
        probe = ConnectionManager(_config(driver))
        with probe.get_connection() as conn:
            count = conn.execute(
                "SELECT COUNT(*) AS n FROM pg_stat_activity "
                "WHERE datname = current_database() AND state LIKE 'idle in transaction%%'"
            ).fetchone()["n"]
        probe.close_pool()
        assert count == 0


class TestMigrations:
    def test_multi_statement_numeric_order(self, driver: str, tmp_path: Path) -> None:
        config = _config(driver)
        suffix = uuid.uuid4().hex[:8]
        mig = tmp_path / "migrations"
        mig.mkdir()
        (mig / "9_create.sql").write_text(
            f"CREATE TABLE m_{suffix} (id INTEGER);\nCREATE TABLE n_{suffix} (id INTEGER);\n"
        )
        (mig / "10_insert.sql").write_text(f"INSERT INTO m_{suffix} (id) VALUES (1);")
        manager = ConnectionManager(config)
        mm = MigrationManager(mig, manager)
        try:
            assert [m.version for m in mm.apply()] == ["9", "10"]
            assert mm.current_version() == "10"
        finally:
            with manager.get_connection() as conn:
                for stmt in (
                    f"DROP TABLE m_{suffix}",
                    f"DROP TABLE n_{suffix}",
                    "DELETE FROM schema_migrations WHERE version IN ('9', '10')",
                ):
                    conn.cursor().execute(stmt)
                conn.commit()
            manager.close_pool()


@pytest.fixture
async def async_engine(driver: str, table: str, tmp_path: Path) -> AsyncIterator[AsyncEngine]:
    if driver == "mysql":
        pytest.importorskip("aiomysql")
    eng = AsyncEngine.from_config(_config(driver, pool_size=3), SQLRegistry(tmp_path))
    await eng.execute(f"CREATE TABLE {table} (id INTEGER PRIMARY KEY, name VARCHAR(50))")
    await eng.execute(f"INSERT INTO {table} (id, name) VALUES (1, 'Alice'), (2, 'Bob')")
    yield eng
    await eng.execute(f"DROP TABLE {table}")
    await eng.close()


class TestAsyncEngine:
    async def test_gather_more_than_pool_size(self, async_engine: AsyncEngine, table: str) -> None:
        results = await asyncio.gather(
            *(async_engine.fetch_scalar(f"SELECT COUNT(*) FROM {table}") for _ in range(10))
        )
        assert results == [2] * 10

    async def test_failed_statement_does_not_poison_connection(
        self, async_engine: AsyncEngine, table: str
    ) -> None:
        with pytest.raises(QueryExecutionError):
            await async_engine.fetch_all(f"SELEC * FROM {table}")
        assert await async_engine.fetch_scalar("SELECT 1") == 1

    async def test_transaction_and_like(self, async_engine: AsyncEngine, table: str) -> None:
        async with async_engine.transaction() as tx:
            await tx.execute(
                f"INSERT INTO {table} (id, name) VALUES (:id, :name)", {"id": 3, "name": "Ann"}
            )
            assert await tx.fetch_scalar(f"SELECT COUNT(*) FROM {table} WHERE name LIKE 'A%'") == 2
        rows = await async_engine.fetch_all(
            f"SELECT id FROM {table} WHERE name LIKE 'A%' AND id > :min ORDER BY id", {"min": 0}
        )
        assert [r["id"] for r in rows] == [1, 3]
