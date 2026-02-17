"""
Example 07: Repository Pattern

This example demonstrates using the Repository pattern for DDD-style code organization.
"""

from row_query import Engine, ConnectionConfig, SQLRegistry
from row_query.repository import Repository
from row_query.mapping import ModelMapper
from dataclasses import dataclass
from typing import Optional
import tempfile
import sqlite3
from pathlib import Path


@dataclass
class User:
    """User entity"""
    id: Optional[int]
    name: str
    email: str
    active: bool = True


class UserRepository(Repository[User]):
    """Repository for User entities"""

    def __init__(self, engine: Engine, registry: SQLRegistry):
        # Pass ModelMapper to the base Repository class
        super().__init__(engine, mapper=ModelMapper(User))
        self.registry = registry

    def find_by_id(self, user_id: int) -> Optional[User]:
        """Find user by ID"""
        return self.engine.fetch_one(
            "user.get_by_id",
            {"id": user_id},
            mapper=self.mapper
        )

    def find_all_active(self) -> list[User]:
        """Find all active users"""
        return self.engine.fetch_all(
            "user.list_active",
            mapper=self.mapper
        )

    def save(self, user: User) -> User:
        """Save user (insert or update)"""
        if user.id is None:
            # Insert new user
            user_id = self.engine.fetch_scalar(
                "user.create",
                {"name": user.name, "email": user.email, "active": int(user.active)}
            )
            user.id = user_id
        else:
            # Update existing user
            self.engine.execute(
                "user.update",
                {
                    "id": user.id,
                    "name": user.name,
                    "email": user.email,
                    "active": int(user.active)
                }
            )
        return user

    def delete(self, user_id: int) -> None:
        """Delete user by ID"""
        self.engine.execute("user.delete", {"id": user_id})


def main():
    # Set up database
    db_file = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
    db_path = db_file.name
    db_file.close()

    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT NOT NULL,
            active INTEGER DEFAULT 1
        )
    """)
    conn.execute("INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com')")
    conn.execute("INSERT INTO users (name, email, active) VALUES ('Bob', 'bob@example.com', 0)")
    conn.commit()
    conn.close()

    # Set up SQL registry
    sql_dir = Path(tempfile.mkdtemp())
    user_dir = sql_dir / "user"
    user_dir.mkdir()

    (user_dir / "get_by_id.sql").write_text("SELECT * FROM users WHERE id = :id")
    (user_dir / "list_active.sql").write_text("SELECT * FROM users WHERE active = 1")
    (user_dir / "create.sql").write_text(
        "INSERT INTO users (name, email, active) VALUES (:name, :email, :active) RETURNING id"
    )
    (user_dir / "update.sql").write_text(
        "UPDATE users SET name = :name, email = :email, active = :active WHERE id = :id"
    )
    (user_dir / "delete.sql").write_text("DELETE FROM users WHERE id = :id")

    # Configure engine and repository
    config = ConnectionConfig(driver="sqlite", database=db_path)
    registry = SQLRegistry(str(sql_dir))
    engine = Engine.from_config(config, registry)
    user_repo = UserRepository(engine, registry)

    print("=== Repository Pattern ===\n")

    # Find by ID
    print("1. Find user by ID:")
    user = user_repo.find_by_id(1)
    if user:
        print(f"   Found: {user.name} ({user.email})\n")

    # Find all active users
    print("2. Find all active users:")
    active_users = user_repo.find_all_active()
    print(f"   Active users: {len(active_users)}")
    for u in active_users:
        print(f"   - {u.name}")
    print()

    # Save new user
    print("3. Save new user:")
    new_user = User(id=None, name="Charlie", email="charlie@example.com")
    saved_user = user_repo.save(new_user)
    print(f"   Created user with ID: {saved_user.id}\n")

    # Update user
    print("4. Update user:")
    user = user_repo.find_by_id(1)
    if user:
        user.email = "alice.updated@example.com"
        user_repo.save(user)
        print(f"   Updated user #{user.id}\n")

    # Delete user
    print("5. Delete user:")
    user_repo.delete(2)
    print(f"   Deleted user #2\n")

    # Verify final state
    print("6. Final active users:")
    active_users = user_repo.find_all_active()
    for u in active_users:
        print(f"   - {u.name} ({u.email})")
    print()

    # Clean up
    Path(db_path).unlink()
    for file in user_dir.glob("*.sql"):
        file.unlink()
    user_dir.rmdir()
    sql_dir.rmdir()


if __name__ == "__main__":
    main()
