"""Shared test fixtures."""

from __future__ import annotations

from pathlib import Path

import pytest

from row_query.core.connection import ConnectionConfig


@pytest.fixture
def sqlite_config() -> ConnectionConfig:
    """SQLite in-memory connection config."""
    return ConnectionConfig(driver="sqlite", database=":memory:")


@pytest.fixture
def tmp_sql_dir(tmp_path: Path) -> Path:
    """Temporary directory for SQL files."""
    return tmp_path / "sql"


@pytest.fixture
def write_sql(tmp_sql_dir: Path):
    """Helper to write SQL files into the temp directory.

    Usage:
        write_sql("user/get_by_id.sql", "SELECT * FROM users WHERE id = :user_id")
    """

    def _write(relative_path: str, content: str) -> Path:
        file_path = tmp_sql_dir / relative_path
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_text(content, encoding="utf-8")
        return file_path

    return _write
