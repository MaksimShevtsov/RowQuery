"""
Example 02: Model Mapping

This example demonstrates mapping query results to Python dataclasses and Pydantic models.
"""

from row_query import Engine, ConnectionConfig, SQLRegistry
from row_query.mapping import ModelMapper
from dataclasses import dataclass
from pydantic import BaseModel, EmailStr
import tempfile
import sqlite3
from pathlib import Path


@dataclass
class UserDataclass:
    """User model using dataclass"""
    id: int
    name: str
    email: str
    active: bool


class UserPydantic(BaseModel):
    """User model using Pydantic"""
    id: int
    name: str
    email: EmailStr
    active: bool


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
            email TEXT NOT NULL,
            active INTEGER DEFAULT 1
        )
    """)
    conn.execute("INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com')")
    conn.execute("INSERT INTO users (name, email) VALUES ('Bob', 'bob@example.com')")
    conn.commit()
    conn.close()

    # Set up SQL registry
    sql_dir = Path(tempfile.mkdtemp())
    user_dir = sql_dir / "user"
    user_dir.mkdir()
    (user_dir / "list_all.sql").write_text("SELECT * FROM users")
    (user_dir / "get_by_id.sql").write_text("SELECT * FROM users WHERE id = :id")

    # Configure engine
    config = ConnectionConfig(driver="sqlite", database=db_path)
    registry = SQLRegistry(str(sql_dir))
    engine = Engine.from_config(config, registry)

    print("=== Model Mapping ===\n")

    # Map to dataclass
    print("1. Dataclass Mapping:")
    dataclass_mapper = ModelMapper(UserDataclass)
    user = engine.fetch_one("user.get_by_id", {"id": 1}, mapper=dataclass_mapper)
    print(f"   Type: {type(user).__name__}")
    print(f"   Data: {user}")
    print(f"   Access: user.name = {user.name}\n")

    # Map to Pydantic model
    print("2. Pydantic Model Mapping:")
    pydantic_mapper = ModelMapper(UserPydantic)
    users = engine.fetch_all("user.list_all", mapper=pydantic_mapper)
    print(f"   Count: {len(users)} users")
    for u in users:
        print(f"   - {u.name}: {u.email} (active={u.active})")
    print()

    # Clean up
    Path(db_path).unlink()
    for file in user_dir.glob("*.sql"):
        file.unlink()
    user_dir.rmdir()
    sql_dir.rmdir()


if __name__ == "__main__":
    main()
