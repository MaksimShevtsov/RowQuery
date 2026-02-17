"""
Example 01: Basic Query Execution

This example demonstrates basic query execution using RowQuery's Engine and SQLRegistry.
"""

from row_query import Engine, ConnectionConfig, SQLRegistry
import tempfile
import sqlite3
from pathlib import Path


def main():
    # Create a temporary database
    db_file = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
    db_path = db_file.name
    db_file.close()

    # Set up the database with some test data
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT NOT NULL,
            active INTEGER DEFAULT 1
        )
    """)
    conn.execute("INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com')")
    conn.execute("INSERT INTO users (name, email) VALUES ('Bob', 'bob@example.com')")
    conn.execute("INSERT INTO users (name, email, active) VALUES ('Charlie', 'charlie@example.com', 0)")
    conn.commit()
    conn.close()

    # Create temporary SQL files directory
    sql_dir = Path(tempfile.mkdtemp())
    user_dir = sql_dir / "user"
    user_dir.mkdir()

    # Create SQL query files
    (user_dir / "get_by_id.sql").write_text("SELECT * FROM users WHERE id = :id")
    (user_dir / "list_all.sql").write_text("SELECT * FROM users")
    (user_dir / "list_active.sql").write_text("SELECT * FROM users WHERE active = 1")
    (user_dir / "count.sql").write_text("SELECT COUNT(*) FROM users")

    # Configure engine
    config = ConnectionConfig(driver="sqlite", database=db_path)
    registry = SQLRegistry(str(sql_dir))
    engine = Engine.from_config(config, registry)

    print("=== Basic Query Execution ===\n")

    # fetch_one: Get a single row
    user = engine.fetch_one("user.get_by_id", {"id": 1})
    print(f"fetch_one result: {user}")
    print(f"User name: {user['name']}\n")

    # fetch_all: Get multiple rows
    users = engine.fetch_all("user.list_active")
    print(f"fetch_all result ({len(users)} rows):")
    for user in users:
        print(f"  - {user['name']} ({user['email']})")
    print()

    # fetch_scalar: Get a single value
    count = engine.fetch_scalar("user.count")
    print(f"fetch_scalar result: {count} total users\n")

    # Clean up
    Path(db_path).unlink()
    for file in user_dir.glob("*.sql"):
        file.unlink()
    user_dir.rmdir()
    sql_dir.rmdir()


if __name__ == "__main__":
    main()
