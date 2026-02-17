"""
Example 06: Migrations

This example demonstrates database migration management using MigrationManager.
"""

from row_query import Engine, ConnectionConfig, MigrationManager
import tempfile
from pathlib import Path


def main():
    # Set up database and migrations directory
    db_file = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
    db_path = db_file.name
    db_file.close()

    migrations_dir = Path(tempfile.mkdtemp())

    # Create migration files
    (migrations_dir / "001_create_users.sql").write_text("""
-- Migration: Create users table
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT NOT NULL
);
""")

    (migrations_dir / "002_add_users_active.sql").write_text("""
-- Migration: Add active column to users
ALTER TABLE users ADD COLUMN active INTEGER DEFAULT 1;
""")

    (migrations_dir / "003_create_orders.sql").write_text("""
-- Migration: Create orders table
CREATE TABLE orders (
    id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL,
    total REAL NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users(id)
);
""")

    # Configure engine
    config = ConnectionConfig(driver="sqlite", database=db_path)
    engine = Engine.from_config(config, config_registry=None)

    # Create migration manager
    manager = MigrationManager(engine, str(migrations_dir))

    print("=== Migration Management ===\n")

    # Check pending migrations
    print("1. Check pending migrations:")
    pending = manager.pending_migrations()
    print(f"   Pending migrations: {len(pending)}")
    for migration in pending:
        print(f"   - {migration.version}: {migration.name}")
    print()

    # Apply all migrations
    print("2. Apply migrations:")
    applied = manager.apply_pending()
    print(f"   Applied {len(applied)} migrations:")
    for migration in applied:
        print(f"   - {migration.version}: {migration.name}")
    print()

    # Check current version
    print("3. Check current version:")
    current = manager.current_version()
    print(f"   Current version: {current}\n")

    # Check migration history
    print("4. Migration history:")
    history = manager.migration_history()
    for migration in history:
        print(f"   - Version {migration.version}: {migration.name}")
        print(f"     Applied: {migration.applied_at}")
    print()

    # Verify no pending migrations
    print("5. Verify all migrations applied:")
    pending = manager.pending_migrations()
    print(f"   Pending migrations: {len(pending)}\n")

    # Clean up
    Path(db_path).unlink()
    for file in migrations_dir.glob("*.sql"):
        file.unlink()
    migrations_dir.rmdir()


if __name__ == "__main__":
    main()
