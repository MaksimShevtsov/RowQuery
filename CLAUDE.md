# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

RowQuery is a Python library (alpha, v0.1.0) for querying and manipulating data in rows across multiple database backends. It uses a driver/adapter pattern to support SQLite, PostgreSQL, MySQL, and Oracle. Requires Python >=3.10.

## Package Manager

This project uses **uv** (not pip/poetry). All commands should go through uv.

```bash
uv sync                    # Install core dependencies
uv sync --extra postgres   # Install with PostgreSQL support
uv sync --extra all        # Install all database drivers
```

## Architecture

- **`core/`** — Main source package
  - `connection.py` — `ConnectionConfig` (Pydantic model) and `ConnectionManager` (pool-based connection lifecycle with context manager)
  - `enums.py` — `DBAdapter` enum defining supported database backends
  - `executor.py` — Query execution (not yet implemented)
  - `transaction.py` — Transaction handling (not yet implemented)

- **Adapter pattern**: `ConnectionManager._load_adapter()` dynamically imports from `row_query.adapters.{driver}`, expecting each adapter module to expose a `DBAdapter` class with methods: `create_pool()`, `acquire_connection()`, `release_connection()`, `close_pool()`. Adapter modules are not yet implemented.

- **Configuration**: Uses Pydantic `BaseModel` for type-safe connection config with validation. Pool settings (size, timeout, recycle) are configurable.

## Dependencies

- **Core**: pydantic >=2.0.0
- **Optional (by database)**: psycopg2-binary (postgres), mysql-connector-python (mysql), oracledb (oracle), sqlite uses stdlib

## Development Status

Early-stage project. No test framework, linter, or CI/CD is configured yet. `.pre-commit-config.yaml` exists but is empty.
