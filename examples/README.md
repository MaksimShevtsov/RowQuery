# RowQuery Examples

This directory contains runnable examples demonstrating RowQuery features.

## Running Examples

All examples use SQLite with in-memory or temporary databases, so no external database setup is required.

```bash
# Run an example
python examples/01_basic_query.py
```

## Examples Overview

### 01_basic_query.py
Basic query execution using `Engine` and `SQLRegistry`. Shows:
- Creating a connection
- Loading SQL files
- `fetch_one()`, `fetch_all()`, `fetch_scalar()` methods

### 02_model_mapping.py
Mapping query results to Python objects. Shows:
- Dataclass mapping
- Pydantic model mapping
- Using `ModelMapper`

### 03_aggregate.py
Complex object graph reconstruction from joined queries. Shows:
- Aggregate mapping with collections
- Single-pass O(n) reconstruction
- The `aggregate()` DSL

### 04_transactions.py
Transaction management. Shows:
- Context manager usage
- Automatic rollback on error
- Manual commit/rollback

### 05_async.py
Async/await support. Shows:
- `AsyncEngine` usage
- Async query execution
- Async transactions

### 06_migrations.py
Database migration management. Shows:
- `MigrationManager` usage
- Version tracking
- Migration application

### 07_repository.py
Repository pattern for DDD. Shows:
- Using `Repository` base class
- Entity persistence
- Query encapsulation

## Adapting Examples

These examples use SQLite for simplicity. To use with other databases:

```python
# PostgreSQL
config = ConnectionConfig(
    driver="postgresql",
    host="localhost",
    port=5432,
    database="mydb",
    user="user",
    password="pass"
)

# MySQL
config = ConnectionConfig(
    driver="mysql",
    host="localhost",
    port=3306,
    database="mydb",
    user="user",
    password="pass"
)

# Oracle
config = ConnectionConfig(
    driver="oracle",
    host="localhost",
    port=1521,
    service_name="ORCLPDB1",
    user="user",
    password="pass"
)
```
