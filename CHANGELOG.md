# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed
- **Transaction state no longer leaks between calls.** Connections are rolled back when they return to the pool, so reads no longer leave PostgreSQL sessions `idle in transaction`, MySQL `REPEATABLE READ` readers no longer see a stale snapshot, and a failed statement no longer leaves a PostgreSQL connection in the `current transaction is aborted` state.
- **Real connection pooling.** The pool now waits for a free connection (up to `pool_timeout`) instead of raising `RuntimeError` when exhausted, opens connections lazily, honours `pool_recycle`, and discards connections that fail the reset or are reported broken. `Engine` can be used from several threads (SQLite connections are opened with `check_same_thread=False`), and `asyncio.gather` with more tasks than `pool_size` works.
- **`%` in SQL for PostgreSQL/MySQL.** Literal `%` (e.g. `LIKE 'a%'`) is escaped when `:name` placeholders are converted to `%(name)s`.
- **Migrations**: files may contain several statements; versions are ordered numerically (`9_x` before `10_y`); the tracking table and inserts work on PostgreSQL, MySQL and Oracle; duplicate version numbers are rejected.
- **SQLSanitizer bypasses**: constructs whose extent differs between MySQL and PostgreSQL are rejected (backslashes inside any quoted literal or identifier, dollar-quoted strings, `#` outside literals, `--` not followed by whitespace, `
` inside `--` comments); the `allowed_verbs` check skips leading comments and rejects SQL whose verb cannot be determined (fail-closed); MySQL `/*! ... */` executable comments are rejected when comments are not stripped; unterminated block comments are rejected.
- `Engine.transaction()` acquires its connection on `__enter__` instead of at call time.
- Transactions now have the same query API as engines: `fetch_one` raises `MultipleRowsError`, plus `mapper=` and `fetch_scalar`.
- `fetch_one` reads at most two rows instead of the whole result.
- MySQL cursors are buffered and always closed, avoiding `Unread result found` after `fetch_scalar`/`fetch_one`.
- PostgreSQL connection strings are built with `psycopg.conninfo.make_conninfo` (passwords with spaces or quotes work); `ConnectionConfig.extra` is passed to every driver.
- Oracle: DSN defaults to port 1521 when `port` is unset; only ALL-CAPS column names are lowercased, so quoted mixed-case aliases are preserved.
- Single-word inline SQL (`VACUUM`, `COMMIT`) is no longer mistaken for a registry key.
- SQLite transactions and migrations now cover DDL: an explicit `BEGIN` is issued, so a failing transaction or migration rolls back `CREATE`/`ALTER`/`DROP` too (the `sqlite3` module only opens transactions implicitly before DML).
- A connection released after `close_pool()` is closed instead of being handed to the new pool; pool initialisation is thread-safe.
- MySQL connection liveness is checked locally instead of pinging the server on every acquire; pool health checks run outside the pool lock.
- Applied migrations are matched by version number, so renaming `9_x.sql` to `009_x.sql` does not re-apply it.
- Parameter conversion honours the backend's string-literal rules (`'C:\'` is a complete literal on PostgreSQL).
- Strict aggregate mapping also validates reference and value-object columns.
- Project URLs in `pyproject.toml` point to the correct GitHub repository.

### Added
- **Nested collections** in aggregate mappings: `.collection("lines.discounts", Discount, prefix="disc__", key="id")` nests a collection inside a previously declared one, at any depth. Children are deduplicated per parent instance; strict mode validates nested columns, prefixes and identity key columns (root, collections at every level, references) and reports collections by their full path. Dotted paths for `reference()` / `value_object()` are rejected at `build()`. `CollectionPlan.children` is now populated and used by `AggregateMapper`.
- Portable positional placeholders: `?` is converted to `%s` (PostgreSQL, MySQL) or `:1, :2, ...` (Oracle); `??` is a literal `?` (e.g. the PostgreSQL JSONB operator). SQL already using native `%s` / `%(name)s` placeholders is passed through unchanged.
- `QueryExecutionError` (database rejected the query) and `IntegrityError` (constraint violation), both carrying the driver exception as `__cause__`; `PoolTimeoutError`.
- `Engine.close()` / `AsyncEngine.close()`; `ConnectionManager.acquire()` / `release()`.
- CI runs the integration suite against PostgreSQL, MySQL and Oracle (23ai Free) service containers.

### Changed
- **Breaking:** errors that are not binding problems now raise `QueryExecutionError` instead of `ParameterBindingError` (which is now a subclass of `QueryExecutionError`). Catch `ExecutionError` or `QueryExecutionError` to handle all database errors.
- **Breaking (adapter protocol):** adapters now implement `connect`/`close_connection`/`is_connection_alive`/`execute` (async: `connect_async`/`close_connection_async`/`execute_async`) plus `begin`/`begin_async`, `positional_paramstyle` and `backslash_escapes`; pooling moved into `row_query.core.pool`. `TransactionManager`/`AsyncTransactionManager` take a connection manager instead of a raw connection.
- `ConnectionConfig.pool_timeout` and `pool_recycle` are floats and are now enforced.

## [0.1.2] - 2026-02-22

### Added
- **Inline SQL support**: All engine and transaction methods (`fetch_one`, `fetch_all`, `fetch_scalar`, `execute`) now accept raw SQL strings in addition to registry keys. A string containing whitespace is treated as inline SQL; a dot-separated identifier like `user.get_by_id` is resolved from the registry.
- **Flexible parameter binding**: `params` argument now accepts `dict` (named), `tuple`/`list` (positional), or a single scalar value (automatically wrapped in a tuple). Previously only `dict | None` was accepted.
- **`SQLSanitizer`** — configurable sanitizer applied to inline SQL before execution:
  - `strip_comments` (default `True`): removes `--` line comments and `/* */` block comments while preserving string literals.
  - `block_multiple_statements` (default `True`): rejects SQL containing a statement-terminating `;` followed by additional content (prevents query stacking attacks).
  - `allowed_verbs` (default `None`): restricts the leading SQL keyword to a caller-supplied `frozenset` (e.g. `frozenset({"SELECT"})`). Registry queries are never sanitized.
- **`SQLSanitizationError`** exception (subclass of `ExecutionError`) raised when a sanitization check fails.
- **`is_raw_sql()`** and **`coerce_params()`** helpers exported from `row_query.core.params`.
- 65 new unit tests covering all sanitizer behaviour (`tests/unit/test_sanitizer.py`).

## [0.1.0] - 2025-02-16

### Added
- Initial alpha release
- Core query execution engine (`Engine`, `AsyncEngine`)
- Connection management with pooling support (`ConnectionManager`, `AsyncConnectionManager`)
- Multi-database adapter pattern supporting SQLite, PostgreSQL, MySQL, and Oracle
- SQL registry for loading `.sql` files into dot-separated namespaces (`SQLRegistry`)
- Parameter normalization supporting `:name` syntax across all database backends
- Model mapping for dataclasses, Pydantic models, and plain classes (`ModelMapper`)
- Aggregate mapping for complex object graph reconstruction with single-pass O(n) algorithm
- Collection mapping for one-to-many relationships in aggregate patterns
- Value object mapping for embedded entities in aggregates
- Reference mapping for foreign key relationships
- Transaction management with context manager support (`TransactionManager`, `AsyncTransactionManager`)
- Migration management system with version tracking (`MigrationManager`, `MigrationInfo`)
- Repository pattern base classes for DDD-style code organization (`Repository`, `AsyncRepository`)
- Comprehensive exception hierarchy with 18 custom exception classes
- Full type safety with mypy strict mode compliance
- Support for Python 3.10, 3.11, 3.12, and 3.13
- 125 unit, contract, and integration tests

[Unreleased]: https://github.com/MaksimShevtsov/RowQuery/compare/v0.1.2...HEAD
[0.1.2]: https://github.com/MaksimShevtsov/RowQuery/releases/tag/v0.1.2
[0.1.1]: https://github.com/MaksimShevtsov/RowQuery/releases/tag/v0.1.1
[0.1.0]: https://github.com/MaksimShevtsov/RowQuery/releases/tag/v0.1.0
