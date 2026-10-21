"""Database migration management.

Manages versioned SQL migration files with numeric ordering,
incremental execution, and tracking of applied versions.

Each migration file may contain several statements separated by ``;``.
Semicolons inside string literals, quoted identifiers, comments and
PostgreSQL dollar-quoted bodies (``$$ ... $$``) do not split. For procedural
code with internal semicolons (MySQL/Oracle ``BEGIN ... END``), add a line
``-- rowquery:no-split`` to execute the whole file as a single statement.

Each migration runs in one transaction together with its tracking row.
Note that MySQL and Oracle commit implicitly after DDL statements, so a
migration that fails half-way may leave earlier statements applied there.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from row_query.core.connection import ConnectionManager
from row_query.core.exceptions import MigrationExecutionError, MigrationFileError
from row_query.core.execution import PreparedQuery, rows_to_dicts, sync_cursor
from row_query.core.lexer import split_statements
from row_query.core.params import bind_params

_MIGRATION_PATTERN = re.compile(r"^(\d+)_(.+)\.sql$")
_NO_SPLIT_MARKER = re.compile(r"^\s*--\s*rowquery:\s*no-split\s*$", re.MULTILINE | re.IGNORECASE)

# Portable across SQLite, PostgreSQL, MySQL and Oracle.
_CREATE_TRACKING_TABLE = """
CREATE TABLE schema_migrations (
    version     VARCHAR(255) NOT NULL PRIMARY KEY,
    description VARCHAR(255) NOT NULL,
    applied_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
)
"""
_PROBE_TRACKING_TABLE = "SELECT version FROM schema_migrations WHERE 1 = 0"
_SELECT_VERSIONS = "SELECT version FROM schema_migrations"
_INSERT_VERSION = (
    "INSERT INTO schema_migrations (version, description) VALUES (:version, :description)"
)


@dataclass(frozen=True)
class MigrationInfo:
    """Metadata about a single migration file."""

    version: str
    description: str
    file_path: Path
    applied: bool = False


class MigrationManager:
    """Manages forward-only SQL schema migrations.

    Migration files must follow: NNN_description.sql and are applied in
    numeric order of NNN (``9_x.sql`` before ``10_y.sql``).
    Applied migrations are tracked in the `schema_migrations` table.
    """

    def __init__(
        self,
        migration_dir: Path | str,
        connection_manager: ConnectionManager,
    ) -> None:
        self._migration_dir = Path(migration_dir)
        self._connection_manager = connection_manager
        self._adapter = connection_manager.adapter
        self._ensure_tracking_table()

    def _run(self, conn: Any, sql: str, params: Any = None, label: str = "<migration>") -> Any:
        if params is not None:
            sql, params = bind_params(
                sql, params, self._adapter.paramstyle, self._adapter.positional_paramstyle
            )
        query = PreparedQuery(sql, params, label)
        with sync_cursor(self._adapter, conn, query) as cursor:
            if cursor.description is None:
                return []
            return rows_to_dicts(cursor.description, cursor.fetchall())

    def _ensure_tracking_table(self) -> None:
        """Create schema_migrations table if it doesn't exist."""
        with self._connection_manager.get_connection() as conn:
            try:
                self._run(conn, _PROBE_TRACKING_TABLE)
                return
            except Exception:
                conn.rollback()
            self._run(conn, _CREATE_TRACKING_TABLE)
            conn.commit()

    def _get_applied_versions(self) -> set[str]:
        """Get set of already-applied migration versions."""
        with self._connection_manager.get_connection() as conn:
            return {str(row["version"]) for row in self._run(conn, _SELECT_VERSIONS)}

    def discover(self) -> list[MigrationInfo]:
        """Discover all migration files and their applied status."""
        applied_versions = self._get_applied_versions()
        migrations: list[MigrationInfo] = []
        seen: dict[int, str] = {}

        for file_path in self._migration_dir.glob("*.sql"):
            match = _MIGRATION_PATTERN.match(file_path.name)
            if not match:
                raise MigrationFileError(
                    file_path.name,
                    "Must match pattern NNN_description.sql",
                )

            version = match.group(1)
            number = int(version)
            if number in seen:
                raise MigrationFileError(
                    file_path.name,
                    f"Duplicate migration number {number} (also used by {seen[number]})",
                )
            seen[number] = file_path.name

            migrations.append(
                MigrationInfo(
                    version=version,
                    description=match.group(2),
                    file_path=file_path,
                    applied=version in applied_versions,
                )
            )

        return sorted(migrations, key=lambda m: int(m.version))

    def pending(self) -> list[MigrationInfo]:
        """Return only unapplied migrations, sorted by version."""
        return [m for m in self.discover() if not m.applied]

    def _statements(self, sql: str) -> list[str]:
        if _NO_SPLIT_MARKER.search(sql):
            return [sql.strip()]
        return split_statements(sql, backslash_escapes=self._adapter.backslash_escapes)

    def apply(self) -> list[MigrationInfo]:
        """Apply all pending migrations in order.

        Stops on first failure. Returns list of successfully applied migrations.
        """
        applied: list[MigrationInfo] = []

        for migration in self.pending():
            sql = migration.file_path.read_text(encoding="utf-8")
            label = migration.file_path.name

            with self._connection_manager.get_connection() as conn:
                try:
                    for statement in self._statements(sql):
                        self._run(conn, statement, label=label)
                    self._run(
                        conn,
                        _INSERT_VERSION,
                        {"version": migration.version, "description": migration.description},
                        label=label,
                    )
                    conn.commit()
                except Exception as e:
                    conn.rollback()
                    raise MigrationExecutionError(migration.version, str(e)) from e

            applied.append(
                MigrationInfo(
                    version=migration.version,
                    description=migration.description,
                    file_path=migration.file_path,
                    applied=True,
                )
            )

        return applied

    def applied(self) -> list[MigrationInfo]:
        """Return list of already-applied migrations."""
        return [m for m in self.discover() if m.applied]

    def current_version(self) -> str | None:
        """Return the version string of the highest applied migration, or None."""
        versions = self._get_applied_versions()
        if not versions:
            return None
        return max(versions, key=int)
