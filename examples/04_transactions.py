"""
Example 04: Transactions

This example demonstrates transaction management with automatic rollback on errors.
"""

from row_query import Engine, ConnectionConfig, SQLRegistry
import tempfile
import sqlite3
from pathlib import Path


def main():
    # Set up database
    db_file = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
    db_path = db_file.name
    db_file.close()

    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT NOT NULL UNIQUE
        )
    """)
    conn.execute("""
        CREATE TABLE audit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            action TEXT NOT NULL,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()

    # Set up SQL registry
    sql_dir = Path(tempfile.mkdtemp())
    user_dir = sql_dir / "user"
    audit_dir = sql_dir / "audit"
    user_dir.mkdir()
    audit_dir.mkdir()

    (user_dir / "create.sql").write_text("INSERT INTO users (name, email) VALUES (:name, :email)")
    (user_dir / "count.sql").write_text("SELECT COUNT(*) FROM users")
    (audit_dir / "log.sql").write_text("INSERT INTO audit_log (action) VALUES (:action)")

    # Configure engine
    config = ConnectionConfig(driver="sqlite", database=db_path)
    registry = SQLRegistry(str(sql_dir))
    engine = Engine.from_config(config, registry)

    print("=== Transaction Management ===\n")

    # Example 1: Successful transaction
    print("1. Successful transaction:")
    with engine.transaction() as tx:
        tx.execute("user.create", {"name": "Alice", "email": "alice@example.com"})
        tx.execute("audit.log", {"action": "user_created"})
        # Commits automatically on exit
    count = engine.fetch_scalar("user.count")
    print(f"   Users after commit: {count}\n")

    # Example 2: Transaction with rollback on error
    print("2. Transaction with error (automatic rollback):")
    try:
        with engine.transaction() as tx:
            tx.execute("user.create", {"name": "Bob", "email": "bob@example.com"})
            # This will fail due to duplicate email
            tx.execute("user.create", {"name": "Charlie", "email": "alice@example.com"})
    except Exception as e:
        print(f"   Error occurred: {type(e).__name__}")
        print(f"   Transaction was rolled back automatically\n")

    count = engine.fetch_scalar("user.count")
    print(f"   Users after rollback: {count} (Bob was not added)\n")

    # Example 3: Nested operations in transaction
    print("3. Multiple operations in transaction:")
    with engine.transaction() as tx:
        tx.execute("user.create", {"name": "Dave", "email": "dave@example.com"})
        tx.execute("audit.log", {"action": "user_created"})
        tx.execute("user.create", {"name": "Eve", "email": "eve@example.com"})
        tx.execute("audit.log", {"action": "user_created"})
    count = engine.fetch_scalar("user.count")
    print(f"   Users after transaction: {count}\n")

    # Clean up
    Path(db_path).unlink()
    for file in user_dir.glob("*.sql"):
        file.unlink()
    for file in audit_dir.glob("*.sql"):
        file.unlink()
    user_dir.rmdir()
    audit_dir.rmdir()
    sql_dir.rmdir()


if __name__ == "__main__":
    main()
