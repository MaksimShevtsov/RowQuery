"""
Example 03: Aggregate Mapping

This example demonstrates reconstructing complex object graphs from joined queries
using RowQuery's aggregate mapping feature.
"""

from row_query import Engine, ConnectionConfig, SQLRegistry
from row_query.mapping import aggregate, ModelMapper, AggregateMapper
from dataclasses import dataclass
import tempfile
import sqlite3
from pathlib import Path


@dataclass
class Order:
    """Order entity"""
    id: int
    total: float
    status: str


@dataclass
class UserWithOrders:
    """User aggregate root with orders collection"""
    id: int
    name: str
    email: str
    orders: list[Order]


def main():
    # Set up database with users and orders
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
    conn.execute("""
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            user_id INTEGER NOT NULL,
            total REAL NOT NULL,
            status TEXT NOT NULL,
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    """)
    conn.execute("INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com')")
    conn.execute("INSERT INTO users (name, email) VALUES ('Bob', 'bob@example.com')")
    conn.execute("INSERT INTO orders (user_id, total, status) VALUES (1, 100.50, 'completed')")
    conn.execute("INSERT INTO orders (user_id, total, status) VALUES (1, 50.25, 'pending')")
    conn.execute("INSERT INTO orders (user_id, total, status) VALUES (2, 200.00, 'completed')")
    conn.commit()
    conn.close()

    # Set up SQL registry
    sql_dir = Path(tempfile.mkdtemp())
    user_dir = sql_dir / "user"
    user_dir.mkdir()

    # Create a joined query
    (user_dir / "with_orders.sql").write_text("""
        SELECT
            u.id as user__id,
            u.name as user__name,
            u.email as user__email,
            o.id as order__id,
            o.total as order__total,
            o.status as order__status
        FROM users u
        LEFT JOIN orders o ON u.id = o.user_id
        ORDER BY u.id, o.id
    """)

    # Configure engine
    config = ConnectionConfig(driver="sqlite", database=db_path)
    registry = SQLRegistry(str(sql_dir))
    engine = Engine.from_config(config, registry)

    print("=== Aggregate Mapping ===\n")

    # Build aggregate mapping plan
    plan = (
        aggregate(UserWithOrders, prefix="user__")
        .key("id")
        .auto_fields()
        .collection("orders", Order, prefix="order__", key="id")
        .build()
    )

    # Execute joined query and map in single pass using AggregateMapper
    users = engine.fetch_all("user.with_orders", mapper=AggregateMapper(plan))

    print(f"Reconstructed {len(users)} user aggregates:\n")
    for user in users:
        print(f"User: {user.name} ({user.email})")
        print(f"  Orders ({len(user.orders)}):")
        for order in user.orders:
            print(f"    - Order #{order.id}: ${order.total:.2f} ({order.status})")
        print()

    # Clean up
    Path(db_path).unlink()
    for file in user_dir.glob("*.sql"):
        file.unlink()
    user_dir.rmdir()
    sql_dir.rmdir()


if __name__ == "__main__":
    main()
