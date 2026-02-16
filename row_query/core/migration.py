"""Database migration management.

Manages versioned SQL migration files with numeric ordering,
incremental execution, and tracking of applied versions.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

from row_query.core.connection import ConnectionManager
from row_query.core.exceptions import MigrationExecutionError, MigrationFileError

_MIGRATION_PATTERN = re.compile(r"^(\d+)_(.+)\.sql$")

_CREATE_TRACKING_TABLE = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    version     TEXT PRIMARY KEY,
    description TEXT NOT NULL,
    applied_at  TEXT NOT NULL DEFAULT (datetime('now'))
)
"""


@dataclass(frozen=True)
class MigrationInfo:
    """Metadata about a single migration file."""

    version: str
    description: str
    file_path: Path
    applied: bool = False


class MigrationManager:
    """Manages forward-only SQL schema migrations.

    Migration files must follow: NNN_description.sql
    Applied migrations are tracked in the `schema_migrations` table.
    """

    def __init__(
        self,
        migration_dir: Path | str,
        connection_manager: ConnectionManager,
    ) -> None:
        self._migration_dir = Path(migration_dir)
        self._connection_manager = connection_manager
        self._ensure_tracking_table()

    def _ensure_tracking_table(self) -> None:
        """Create schema_migrations table if it doesn't exist."""
        with self._connection_manager.get_connection() as conn:
            conn.execute(_CREATE_TRACKING_TABLE)
            conn.commit()

    def _get_applied_versions(self) -> set[str]:
        """Get set of already-applied migration versions."""
        with self._connection_manager.get_connection() as conn:
            cursor = conn.execute("SELECT version FROM schema_migrations ORDER BY version")
            return {row[0] for row in cursor.fetchall()}

    def discover(self) -> list[MigrationInfo]:
        """Discover all migration files and their applied status."""
        applied_versions = self._get_applied_versions()
        migrations: list[MigrationInfo] = []

        for file_path in sorted(self._migration_dir.glob("*.sql")):
            match = _MIGRATION_PATTERN.match(file_path.name)
            if not match:
                raise MigrationFileError(
                    file_path.name,
                    "Must match pattern NNN_description.sql",
                )

            version = match.group(1)
            description = match.group(2)

            migrations.append(
                MigrationInfo(
                    version=version,
                    description=description,
                    file_path=file_path,
                    applied=version in applied_versions,
                )
            )

        return sorted(migrations, key=lambda m: m.version)

    def pending(self) -> list[MigrationInfo]:
        """Return only unapplied migrations, sorted by version."""
        return [m for m in self.discover() if not m.applied]

    def apply(self) -> list[MigrationInfo]:
        """Apply all pending migrations in order.

        Stops on first failure. Returns list of successfully applied migrations.
        """
        pending = self.pending()
        applied: list[MigrationInfo] = []

        for migration in pending:
            sql = migration.file_path.read_text(encoding="utf-8")

            with self._connection_manager.get_connection() as conn:
                try:
                    conn.execute(sql)
                    conn.execute(
                        "INSERT INTO schema_migrations (version, description) VALUES (?, ?)",
                        (migration.version, migration.description),
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
        """Return the version string of the last applied migration, or None."""
        with self._connection_manager.get_connection() as conn:
            cursor = conn.execute(
                "SELECT version FROM schema_migrations ORDER BY version DESC LIMIT 1"
            )
            row = cursor.fetchone()
            if row is None:
                return None
            return str(row[0])
