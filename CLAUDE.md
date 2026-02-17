# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

RowQuery is a Python library (alpha, v0.1.0) — a SQL-first query execution and projection engine for multiple database backends. Uses a driver/adapter pattern to support SQLite, PostgreSQL, MySQL, and Oracle. Requires Python >=3.10.

## Package Manager

This project uses **uv** (not pip/poetry). All commands should go through uv.

```bash
uv sync                    # Install core + dev dependencies
uv sync --extra postgres   # Install with PostgreSQL support
uv sync --extra all        # Install all database drivers
```

## Commands

```bash
uv run pytest              # Run all tests (125 tests)
uv run pytest tests/unit/  # Unit tests only
uv run pytest tests/contract/  # Protocol contract tests
uv run pytest tests/integration/  # SQLite integration tests
uv run ruff check row_query/ tests/  # Lint
uv run ruff format row_query/ tests/  # Format
uv run mypy row_query/     # Type check (strict mode)
```

## Architecture

- **`row_query/core/`** — Core engine
  - `connection.py` — `ConnectionConfig` (Pydantic), `ConnectionManager`, `AsyncConnectionManager`
  - `engine.py` — `Engine` and `AsyncEngine` (fetch_one, fetch_all, fetch_scalar, execute, transaction)
  - `registry.py` — `SQLRegistry` (loads .sql files into dot-separated namespaces)
  - `transaction.py` — `TransactionManager`, `AsyncTransactionManager`
  - `migration.py` — `MigrationManager`, `MigrationInfo`
  - `params.py` — `:name` → `%(name)s` parameter normalizer with cache
  - `enums.py` — `DatabaseBackend` enum
  - `exceptions.py` — Full exception hierarchy (18 classes)

- **`row_query/adapters/`** — Database drivers
  - `protocol.py` — `SyncAdapter`, `AsyncAdapter` runtime-checkable protocols
  - `sqlite.py` — SQLite adapter (stdlib sqlite3 + aiosqlite)
  - `postgresql.py` — PostgreSQL adapter (psycopg v3)
  - `mysql.py` — MySQL adapter (mysql-connector-python + aiomysql)
  - `oracle.py` — Oracle adapter (oracledb)

- **`row_query/mapping/`** — Result mapping
  - `protocol.py` — `Mapper[T]` protocol
  - `model.py` — `ModelMapper` (dataclass, Pydantic, plain class)
  - `plan.py` — Frozen dataclasses: `EntityPlan`, `CollectionPlan`, `ReferencePlan`, `ValueObjectPlan`, `AggregatePlan`
  - `builder.py` — `aggregate()` DSL entry point and `AggregateMappingBuilder`
  - `aggregate.py` — `AggregateMapper` (single-pass O(n) reconstruction)

- **`row_query/repository/`** — DDD pattern
  - `base.py` — `Repository`, `AsyncRepository` base classes

## Dependencies

- **Core**: pydantic >=2.0.0
- **Optional**: psycopg[binary] >=3.1.0, aiosqlite >=0.20.0, aiomysql >=0.2.0, mysql-connector-python >=8.0.0, oracledb >=2.0.0
- **Dev**: pytest, pytest-asyncio, ruff, mypy, pre-commit

## Active Technologies
- Python >=3.10 + pydantic >=2.0.0 (core); psycopg >=3.1.0, aiosqlite, aiomysql, oracledb (optional per-backend)
- SQLite, PostgreSQL, MySQL, Oracle via adapter pattern
