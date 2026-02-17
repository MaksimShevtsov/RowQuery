"""
Example 05: Async Support

This example demonstrates asynchronous query execution using AsyncEngine.
"""

import asyncio
from row_query import AsyncEngine, ConnectionConfig, SQLRegistry
from row_query.mapping import ModelMapper
from dataclasses import dataclass
import tempfile
import sqlite3
from pathlib import Path


@dataclass
class User:
    id: int
    name: str
    email: str


async def main():
    # Set up database
    db_file = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
    db_path = db_file.name
    db_file.close()

    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT NOT NULL
        )
    """)
    conn.execute("INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com')")
    conn.execute("INSERT INTO users (name, email) VALUES ('Bob', 'bob@example.com')")
    conn.execute("INSERT INTO users (name, email) VALUES ('Charlie', 'charlie@example.com')")
    conn.commit()
    conn.close()

    # Set up SQL registry
    sql_dir = Path(tempfile.mkdtemp())
    user_dir = sql_dir / "user"
    user_dir.mkdir()
    (user_dir / "list_all.sql").write_text("SELECT * FROM users")
    (user_dir / "get_by_id.sql").write_text("SELECT * FROM users WHERE id = :id")
    (user_dir / "count.sql").write_text("SELECT COUNT(*) FROM users")
    (user_dir / "create.sql").write_text("INSERT INTO users (name, email) VALUES (:name, :email)")

    # Configure async engine
    config = ConnectionConfig(driver="sqlite", database=db_path)
    registry = SQLRegistry(str(sql_dir))
    engine = AsyncEngine.from_config(config, registry)

    print("=== Async Query Execution ===\n")

    # Async fetch_one
    print("1. Async fetch_one:")
    user_data = await engine.fetch_one("user.get_by_id", {"id": 1})
    print(f"   {user_data}\n")

    # Async fetch_all with mapping
    print("2. Async fetch_all with mapping:")
    mapper = ModelMapper(User)
    users = await engine.fetch_all("user.list_all", mapper=mapper)
    print(f"   Found {len(users)} users:")
    for user in users:
        print(f"   - {user.name} ({user.email})")
    print()

    # Async fetch_scalar
    print("3. Async fetch_scalar:")
    count = await engine.fetch_scalar("user.count")
    print(f"   Total users: {count}\n")

    # Async transaction
    print("4. Async transaction:")
    async with engine.transaction() as tx:
        await tx.execute("user.create", {"name": "Dave", "email": "dave@example.com"})
        print(f"   Created user in transaction")

    new_count = await engine.fetch_scalar("user.count")
    print(f"   New total: {new_count}\n")

    # Concurrent queries
    print("5. Concurrent queries:")
    results = await asyncio.gather(
        engine.fetch_one("user.get_by_id", {"id": 1}),
        engine.fetch_one("user.get_by_id", {"id": 2}),
        engine.fetch_scalar("user.count")
    )
    print(f"   User 1: {results[0]['name']}")
    print(f"   User 2: {results[1]['name']}")
    print(f"   Count: {results[2]}\n")

    # Clean up
    Path(db_path).unlink()
    for file in user_dir.glob("*.sql"):
        file.unlink()
    user_dir.rmdir()
    sql_dir.rmdir()


if __name__ == "__main__":
    asyncio.run(main())
