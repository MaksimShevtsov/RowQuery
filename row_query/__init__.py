"""RowQuery - SQL-first query execution and projection engine."""

from __future__ import annotations

from row_query.core.connection import (
    AsyncConnectionManager,
    ConnectionConfig,
    ConnectionManager,
)
from row_query.core.engine import AsyncEngine, Engine
from row_query.core.enums import DatabaseBackend
from row_query.core.exceptions import (
    AdapterError,
    ColumnMismatchError,
    ConnectionError,  # noqa: A004
    DuplicateQueryError,
    ExecutionError,
    MappingError,
    MigrationError,
    MigrationExecutionError,
    MigrationFileError,
    MultipleRowsError,
    ParameterBindingError,
    PlanCompilationError,
    PoolError,
    QueryNotFoundError,
    RegistryError,
    RowQueryError,
    SQLSanitizationError,
    StrictModeViolation,
    TransactionError,
    TransactionStateError,
)
from row_query.core.migration import MigrationInfo, MigrationManager
from row_query.core.registry import SQLRegistry
from row_query.core.sanitizer import SQLSanitizer
from row_query.core.transaction import AsyncTransactionManager, TransactionManager
from row_query.mapping.model import ModelMapper

__all__ = [
    # Connection
    "ConnectionConfig",
    "ConnectionManager",
    "AsyncConnectionManager",
    # Engine
    "Engine",
    "AsyncEngine",
    # Registry
    "SQLRegistry",
    # Sanitizer
    "SQLSanitizer",
    # Transaction
    "TransactionManager",
    "AsyncTransactionManager",
    # Migration
    "MigrationManager",
    "MigrationInfo",
    # Mapping
    "ModelMapper",
    # Enums
    "DatabaseBackend",
    # Exceptions
    "RowQueryError",
    "RegistryError",
    "QueryNotFoundError",
    "DuplicateQueryError",
    "ExecutionError",
    "MultipleRowsError",
    "ParameterBindingError",
    "SQLSanitizationError",
    "MappingError",
    "ColumnMismatchError",
    "StrictModeViolation",
    "PlanCompilationError",
    "TransactionError",
    "TransactionStateError",
    "MigrationError",
    "MigrationFileError",
    "MigrationExecutionError",
    "AdapterError",
    "ConnectionError",
    "PoolError",
]
