# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/maksim-shevtsov/RowQuery/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/maksim-shevtsov/RowQuery/releases/tag/v0.1.0
