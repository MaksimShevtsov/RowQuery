# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
