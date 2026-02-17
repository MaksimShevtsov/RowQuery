# RowQuery

[![PyPI version](https://badge.fury.io/py/rowquery.svg)](https://pypi.org/project/rowquery/)
[![Python Versions](https://img.shields.io/pypi/pyversions/rowquery.svg)](https://pypi.org/project/rowquery/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Tests](https://github.com/maksim-shevtsov/RowQuery/workflows/Tests/badge.svg)](https://github.com/maksim-shevtsov/RowQuery/actions)
[![codecov](https://codecov.io/gh/maksim-shevtsov/RowQuery/branch/main/graph/badge.svg)](https://codecov.io/gh/maksim-shevtsov/RowQuery)
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)

A SQL-first Python library for querying and mapping data across multiple database backends.

## Features

- **Multi-Database Support**: SQLite, PostgreSQL, MySQL, Oracle with unified interface
- **SQL-First Design**: Load queries from `.sql` files organized in namespaces
- **Flexible Mapping**: Map results to dataclasses, Pydantic models, or plain classes
- **Aggregate Mapping**: Reconstruct complex object graphs from joined queries (single-pass O(n))
- **Transaction Management**: Context manager support with automatic rollback
- **Migration Management**: Version-controlled database migrations
- **Repository Pattern**: Optional DDD-style repository base classes
- **Async Support**: Full async/await support for all operations
- **Type Safe**: Fully typed with mypy strict mode

## Installation

### Core (SQLite support included)
```bash
pip install rowquery
```

### With Database Drivers
```bash
pip install rowquery[postgres]    # PostgreSQL
pip install rowquery[mysql]       # MySQL
pip install rowquery[oracle]      # Oracle
pip install rowquery[all]         # All drivers
```

## Quick Start

### 1. Organize Your SQL Files
```
sql/
  user/
    get_by_id.sql
    list_active.sql
  order/
    create.sql
```

### 2. Execute Queries
```python
from row_query import Engine, ConnectionConfig, SQLRegistry

config = ConnectionConfig(driver="sqlite", database="app.db")
registry = SQLRegistry("sql/")
engine = Engine.from_config(config, registry)

# Execute and fetch
user = engine.fetch_one("user.get_by_id", {"id": 1})
users = engine.fetch_all("user.list_active")
count = engine.fetch_scalar("user.count")
```

### 3. Map to Models
```python
from dataclasses import dataclass
from row_query.mapping import ModelMapper

@dataclass
class User:
    id: int
    name: str
    email: str

mapper = ModelMapper(User)
user = engine.fetch_one("user.get_by_id", {"id": 1}, mapper=mapper)
# Returns: User(id=1, name="Alice", email="alice@example.com")
```

### 4. Aggregate Mapping (Reconstruct Object Graphs)
```python
from row_query.mapping import aggregate, AggregateMapper
from dataclasses import dataclass

@dataclass
class Order:
    id: int
    total: float

@dataclass
class UserWithOrders:
    id: int
    name: str
    email: str
    orders: list[Order]

# Build mapping plan for complex object graph
plan = (
    aggregate(UserWithOrders, prefix="user__")
    .key("id")
    .auto_fields()
    .collection("orders", Order, prefix="order__", key="id")
    .build()
)

# Execute joined query and map in single pass
users = engine.fetch_all("user.with_orders", mapper=AggregateMapper(plan))
```

### 5. Transactions
```python
# Use context manager for automatic rollback on error
with engine.transaction() as tx:
    tx.execute("user.create", {"name": "Alice", "email": "alice@example.com"})
    tx.execute("audit.log", {"action": "user_created"})
    # Commits on exit, rolls back on exception
```

### 6. Async Support
```python
from row_query import AsyncEngine, ConnectionConfig

config = ConnectionConfig(driver="sqlite", database="app.db")
engine = AsyncEngine.from_config(config, registry)

async def fetch_users():
    users = await engine.fetch_all("user.list_active")
    return users

# Async transactions
async with engine.transaction() as tx:
    await tx.execute("user.create", {"name": "Bob"})
```

## Documentation

- [Examples](./examples/) - Runnable code examples
- [CONTRIBUTING.md](./CONTRIBUTING.md) - Development guide
- [CHANGELOG.md](./CHANGELOG.md) - Version history

## Development

This project uses [uv](https://github.com/astral-sh/uv) for package management.

```bash
# Install dependencies
uv sync --extra all --extra dev

# Run tests
uv run pytest

# Run tests with coverage
uv run pytest --cov=row_query --cov-report=html

# Lint and format
uv run ruff check row_query/ tests/
uv run ruff format row_query/ tests/

# Type check
uv run mypy row_query/
```

## License

MIT License - see [LICENSE](LICENSE) for details.

## Contributing

Contributions welcome! Please read [CONTRIBUTING.md](CONTRIBUTING.md) first.
