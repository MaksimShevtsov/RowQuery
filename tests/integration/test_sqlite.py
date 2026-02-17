"""Integration test for SQLite full workflow.

Covers: registry loading, engine queries, model mapping, transactions,
migrations end-to-end against a real SQLite in-memory database.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import pytest

from row_query.core.connection import ConnectionConfig, ConnectionManager
from row_query.core.engine import Engine
from row_query.core.migration import MigrationManager
from row_query.core.registry import SQLRegistry
from row_query.mapping.builder import aggregate
from row_query.mapping.model import ModelMapper

# --- Test models ---


@dataclass
class User:
    id: int
    name: str
    email: str


@dataclass
class Order:
    id: int
    user_id: int
    amount: float


@dataclass
class UserWithOrders:
    id: int
    name: str
    email: str
    orders: list[Order] = field(default_factory=list)


# --- Fixtures ---


@pytest.fixture
def sql_dir(tmp_path: Path) -> Path:
    """Create a temp directory with SQL files for testing."""
    sql_root = tmp_path / "sql"

    # user queries
    user_dir = sql_root / "user"
    user_dir.mkdir(parents=True)
    (user_dir / "create_table.sql").write_text(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT NOT NULL)"
    )
    (user_dir / "insert.sql").write_text(
        "INSERT INTO users (id, name, email) VALUES (:id, :name, :email)"
    )
    (user_dir / "get_by_id.sql").write_text("SELECT id, name, email FROM users WHERE id = :id")
    (user_dir / "list.sql").write_text("SELECT id, name, email FROM users ORDER BY id")
    (user_dir / "count.sql").write_text("SELECT COUNT(*) FROM users")
    (user_dir / "delete.sql").write_text("DELETE FROM users WHERE id = :id")

    # order queries
    order_dir = sql_root / "order"
    order_dir.mkdir(parents=True)
    (order_dir / "create_table.sql").write_text(
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL, "
        "amount REAL NOT NULL, FOREIGN KEY (user_id) REFERENCES users(id))"
    )
    (order_dir / "insert.sql").write_text(
        "INSERT INTO orders (id, user_id, amount) VALUES (:id, :user_id, :amount)"
    )
    (order_dir / "list_by_user.sql").write_text(
        "SELECT id, user_id, amount FROM orders WHERE user_id = :user_id ORDER BY id"
    )

    # joined query for aggregate mapping
    joined_dir = sql_root / "report"
    joined_dir.mkdir(parents=True)
    (joined_dir / "user_orders.sql").write_text(
        "SELECT u.id AS user__id, u.name AS user__name, u.email AS user__email, "
        "o.id AS orders__id, o.user_id AS orders__user_id, o.amount AS orders__amount "
        "FROM users u LEFT JOIN orders o ON u.id = o.user_id ORDER BY u.id, o.id"
    )

    return sql_root


@pytest.fixture
def engine(sql_dir: Path) -> Engine:
    """Create a fully wired engine with SQLite in-memory."""
    config = ConnectionConfig(driver="sqlite", database=":memory:", pool_size=1)
    cm = ConnectionManager(config)
    registry = SQLRegistry(sql_dir)
    eng = Engine(cm, registry)

    # Create tables
    eng.execute("user.create_table")
    eng.execute("order.create_table")

    return eng


@pytest.fixture
def migration_dir(tmp_path: Path) -> Path:
    """Create temp directory with migration files."""
    mig_dir = tmp_path / "migrations"
    mig_dir.mkdir()

    (mig_dir / "001_create_users.sql").write_text(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT NOT NULL)"
    )
    (mig_dir / "002_create_orders.sql").write_text(
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL, "
        "amount REAL NOT NULL)"
    )
    (mig_dir / "003_add_status.sql").write_text(
        "ALTER TABLE orders ADD COLUMN status TEXT DEFAULT 'pending'"
    )

    return mig_dir


# --- Integration Tests ---


@pytest.mark.integration
class TestSqliteRegistryAndEngine:
    """Test SQL registry loading + engine query execution."""

    def test_registry_loads_all_queries(self, sql_dir: Path) -> None:
        registry = SQLRegistry(sql_dir)
        names = registry.query_names
        assert "user.create_table" in names
        assert "user.insert" in names
        assert "user.get_by_id" in names
        assert "user.list" in names
        assert "order.insert" in names
        assert "report.user_orders" in names
        assert len(registry) >= 9

    def test_insert_and_fetch_one(self, engine: Engine) -> None:
        engine.execute("user.insert", {"id": 1, "name": "Alice", "email": "alice@test.com"})
        row = engine.fetch_one("user.get_by_id", {"id": 1})
        assert row is not None
        assert row["id"] == 1
        assert row["name"] == "Alice"
        assert row["email"] == "alice@test.com"

    def test_fetch_one_returns_none(self, engine: Engine) -> None:
        result = engine.fetch_one("user.get_by_id", {"id": 999})
        assert result is None

    def test_fetch_all(self, engine: Engine) -> None:
        engine.execute("user.insert", {"id": 1, "name": "Alice", "email": "a@test.com"})
        engine.execute("user.insert", {"id": 2, "name": "Bob", "email": "b@test.com"})
        rows = engine.fetch_all("user.list")
        assert len(rows) == 2
        assert rows[0]["name"] == "Alice"
        assert rows[1]["name"] == "Bob"

    def test_fetch_scalar(self, engine: Engine) -> None:
        engine.execute("user.insert", {"id": 1, "name": "Alice", "email": "a@test.com"})
        engine.execute("user.insert", {"id": 2, "name": "Bob", "email": "b@test.com"})
        count = engine.fetch_scalar("user.count")
        assert count == 2

    def test_execute_returns_rowcount(self, engine: Engine) -> None:
        engine.execute("user.insert", {"id": 1, "name": "Alice", "email": "a@test.com"})
        rowcount = engine.execute("user.delete", {"id": 1})
        assert rowcount == 1


@pytest.mark.integration
class TestSqliteModelMapping:
    """Test model mapping via Engine."""

    def test_fetch_one_with_model_mapper(self, engine: Engine) -> None:
        engine.execute("user.insert", {"id": 1, "name": "Alice", "email": "a@test.com"})
        mapper = ModelMapper(User)
        user = engine.fetch_one("user.get_by_id", {"id": 1}, mapper=mapper)
        assert isinstance(user, User)
        assert user.id == 1
        assert user.name == "Alice"

    def test_fetch_all_with_model_mapper(self, engine: Engine) -> None:
        engine.execute("user.insert", {"id": 1, "name": "Alice", "email": "a@test.com"})
        engine.execute("user.insert", {"id": 2, "name": "Bob", "email": "b@test.com"})
        mapper = ModelMapper(User)
        users = engine.fetch_all("user.list", mapper=mapper)
        assert len(users) == 2
        assert all(isinstance(u, User) for u in users)

    def test_aggregate_mapping_with_joined_query(self, engine: Engine) -> None:
        # Insert users and orders
        engine.execute("user.insert", {"id": 1, "name": "Alice", "email": "a@test.com"})
        engine.execute("user.insert", {"id": 2, "name": "Bob", "email": "b@test.com"})
        engine.execute("order.insert", {"id": 10, "user_id": 1, "amount": 99.99})
        engine.execute("order.insert", {"id": 11, "user_id": 1, "amount": 49.99})
        engine.execute("order.insert", {"id": 20, "user_id": 2, "amount": 75.00})

        # Build aggregate plan
        from row_query.mapping.aggregate import AggregateMapper

        plan = (
            aggregate(UserWithOrders, prefix="user__")
            .key("id")
            .auto_fields()
            .collection("orders", Order, prefix="orders__", key="id")
            .build()
        )
        mapper = AggregateMapper(plan)

        # Fetch joined rows and map
        rows = engine.fetch_all("report.user_orders")
        users = mapper.map_many(rows)

        assert len(users) == 2
        assert users[0].name == "Alice"
        assert len(users[0].orders) == 2
        assert users[0].orders[0].amount == 99.99
        assert users[1].name == "Bob"
        assert len(users[1].orders) == 1


@pytest.mark.integration
class TestSqliteTransactions:
    """Test transaction management with SQLite."""

    def test_transaction_commit(self, engine: Engine) -> None:
        with engine.transaction() as tx:
            tx.execute("user.insert", {"id": 1, "name": "Alice", "email": "a@test.com"})
            tx.execute("user.insert", {"id": 2, "name": "Bob", "email": "b@test.com"})

        count = engine.fetch_scalar("user.count")
        assert count == 2

    def test_transaction_auto_rollback_on_error(self, engine: Engine) -> None:
        try:
            with engine.transaction() as tx:
                tx.execute("user.insert", {"id": 1, "name": "Alice", "email": "a@test.com"})
                raise ValueError("Simulated error")
        except ValueError:
            pass

        count = engine.fetch_scalar("user.count")
        assert count == 0

    def test_transaction_explicit_rollback(self, engine: Engine) -> None:
        with engine.transaction() as tx:
            tx.execute("user.insert", {"id": 1, "name": "Alice", "email": "a@test.com"})
            tx.rollback()

        count = engine.fetch_scalar("user.count")
        assert count == 0

    def test_transaction_fetch_within(self, engine: Engine) -> None:
        engine.execute("user.insert", {"id": 1, "name": "Alice", "email": "a@test.com"})

        with engine.transaction() as tx:
            row = tx.fetch_one("user.get_by_id", {"id": 1})
            assert row is not None
            assert row["name"] == "Alice"

            rows = tx.fetch_all("user.list")
            assert len(rows) == 1


@pytest.mark.integration
class TestSqliteMigrations:
    """Test migration management with SQLite."""

    def test_discover_migrations(self, migration_dir: Path, sql_dir: Path) -> None:
        config = ConnectionConfig(driver="sqlite", database=":memory:", pool_size=1)
        cm = ConnectionManager(config)
        mgr = MigrationManager(migration_dir, cm)

        discovered = mgr.discover()
        assert len(discovered) == 3
        assert discovered[0].version == "001"
        assert discovered[1].version == "002"
        assert discovered[2].version == "003"
        assert all(not m.applied for m in discovered)

    def test_apply_migrations(self, migration_dir: Path) -> None:
        config = ConnectionConfig(driver="sqlite", database=":memory:", pool_size=1)
        cm = ConnectionManager(config)
        mgr = MigrationManager(migration_dir, cm)

        applied = mgr.apply()
        assert len(applied) == 3
        assert all(m.applied for m in applied)

        # Verify tables exist by querying
        with cm.get_connection() as conn:
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            )
            tables = [row[0] for row in cursor.fetchall()]
            assert "users" in tables
            assert "orders" in tables
            assert "schema_migrations" in tables

    def test_pending_after_apply(self, migration_dir: Path) -> None:
        config = ConnectionConfig(driver="sqlite", database=":memory:", pool_size=1)
        cm = ConnectionManager(config)
        mgr = MigrationManager(migration_dir, cm)

        mgr.apply()
        pending = mgr.pending()
        assert len(pending) == 0

    def test_current_version(self, migration_dir: Path) -> None:
        config = ConnectionConfig(driver="sqlite", database=":memory:", pool_size=1)
        cm = ConnectionManager(config)
        mgr = MigrationManager(migration_dir, cm)

        assert mgr.current_version() is None
        mgr.apply()
        assert mgr.current_version() == "003"

    def test_idempotent_apply(self, migration_dir: Path) -> None:
        config = ConnectionConfig(driver="sqlite", database=":memory:", pool_size=1)
        cm = ConnectionManager(config)
        mgr = MigrationManager(migration_dir, cm)

        first = mgr.apply()
        assert len(first) == 3
        second = mgr.apply()
        assert len(second) == 0
