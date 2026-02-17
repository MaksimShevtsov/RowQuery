"""Unit tests for MigrationManager."""

from __future__ import annotations

from pathlib import Path

import pytest

from row_query.core.connection import ConnectionConfig, ConnectionManager
from row_query.core.exceptions import MigrationExecutionError, MigrationFileError
from row_query.core.migration import MigrationInfo, MigrationManager


@pytest.fixture
def migration_dir(tmp_path: Path) -> Path:
    d = tmp_path / "migrations"
    d.mkdir()
    return d


@pytest.fixture
def conn_manager() -> ConnectionManager:
    config = ConnectionConfig(driver="sqlite", database=":memory:", pool_size=1)
    return ConnectionManager(config)


def write_migration(migration_dir: Path, filename: str, sql: str) -> None:
    (migration_dir / filename).write_text(sql, encoding="utf-8")


class TestMigrationManager:
    def test_discover_finds_and_orders(
        self, migration_dir: Path, conn_manager: ConnectionManager
    ) -> None:
        write_migration(migration_dir, "002_add_orders.sql", "CREATE TABLE orders (id INT)")
        write_migration(migration_dir, "001_init.sql", "CREATE TABLE users (id INT)")

        mgr = MigrationManager(migration_dir, conn_manager)
        discovered = mgr.discover()
        assert len(discovered) == 2
        assert discovered[0].version == "001"
        assert discovered[1].version == "002"

    def test_pending_returns_unapplied(
        self, migration_dir: Path, conn_manager: ConnectionManager
    ) -> None:
        write_migration(migration_dir, "001_init.sql", "CREATE TABLE users (id INT)")
        write_migration(migration_dir, "002_add_orders.sql", "CREATE TABLE orders (id INT)")

        mgr = MigrationManager(migration_dir, conn_manager)
        pending = mgr.pending()
        assert len(pending) == 2

    def test_apply_executes_in_order(
        self, migration_dir: Path, conn_manager: ConnectionManager
    ) -> None:
        write_migration(
            migration_dir, "001_init.sql", "CREATE TABLE users (id INTEGER PRIMARY KEY)"
        )
        write_migration(
            migration_dir, "002_add_orders.sql", "CREATE TABLE orders (id INTEGER PRIMARY KEY)"
        )

        mgr = MigrationManager(migration_dir, conn_manager)
        applied = mgr.apply()
        assert len(applied) == 2
        assert applied[0].version == "001"
        assert applied[1].version == "002"

    def test_apply_records_in_schema_migrations(
        self, migration_dir: Path, conn_manager: ConnectionManager
    ) -> None:
        write_migration(migration_dir, "001_init.sql", "CREATE TABLE users (id INT)")

        mgr = MigrationManager(migration_dir, conn_manager)
        mgr.apply()

        applied = mgr.applied()
        assert len(applied) == 1
        assert applied[0].version == "001"
        assert applied[0].applied is True

    def test_apply_skips_already_applied(
        self, migration_dir: Path, conn_manager: ConnectionManager
    ) -> None:
        write_migration(migration_dir, "001_init.sql", "CREATE TABLE users (id INT)")

        mgr = MigrationManager(migration_dir, conn_manager)
        mgr.apply()

        # Add another migration
        write_migration(migration_dir, "002_add_orders.sql", "CREATE TABLE orders (id INT)")
        applied = mgr.apply()
        assert len(applied) == 1
        assert applied[0].version == "002"

    def test_apply_stops_on_failure(
        self, migration_dir: Path, conn_manager: ConnectionManager
    ) -> None:
        write_migration(migration_dir, "001_init.sql", "CREATE TABLE users (id INT)")
        write_migration(migration_dir, "002_bad.sql", "INVALID SQL SYNTAX HERE!!!")
        write_migration(migration_dir, "003_orders.sql", "CREATE TABLE orders (id INT)")

        mgr = MigrationManager(migration_dir, conn_manager)
        with pytest.raises(MigrationExecutionError, match="002"):
            mgr.apply()

        # Only 001 should be applied
        assert mgr.current_version() == "001"

    def test_current_version(self, migration_dir: Path, conn_manager: ConnectionManager) -> None:
        write_migration(migration_dir, "001_init.sql", "CREATE TABLE users (id INT)")
        write_migration(migration_dir, "002_add_orders.sql", "CREATE TABLE orders (id INT)")

        mgr = MigrationManager(migration_dir, conn_manager)
        assert mgr.current_version() is None

        mgr.apply()
        assert mgr.current_version() == "002"

    def test_migration_file_error_invalid_naming(
        self, migration_dir: Path, conn_manager: ConnectionManager
    ) -> None:
        write_migration(migration_dir, "bad_name.sql", "SELECT 1")

        mgr = MigrationManager(migration_dir, conn_manager)
        with pytest.raises(MigrationFileError, match="bad_name"):
            mgr.discover()

    def test_migration_info_fields(self, migration_dir: Path) -> None:
        info = MigrationInfo(
            version="001",
            description="init",
            file_path=migration_dir / "001_init.sql",
        )
        assert info.version == "001"
        assert info.description == "init"
        assert info.applied is False
