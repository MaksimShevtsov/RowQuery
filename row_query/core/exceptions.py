"""RowQuery exception hierarchy.

All exceptions are RowQuery-specific. Raw driver exceptions are never
exposed to callers (constitution Principle III).
"""

from __future__ import annotations


class RowQueryError(Exception):
    """Base exception for all RowQuery errors."""


# --- Registry ---


class RegistryError(RowQueryError):
    """Base for SQL registry errors."""


class QueryNotFoundError(RegistryError):
    """Raised when a named query cannot be found in the registry."""

    def __init__(self, query_name: str) -> None:
        self.query_name = query_name
        super().__init__(f"Query not found: '{query_name}'")


class DuplicateQueryError(RegistryError):
    """Raised when two SQL files resolve to the same namespace key."""

    def __init__(self, query_name: str, path_a: str, path_b: str) -> None:
        self.query_name = query_name
        super().__init__(f"Duplicate query name '{query_name}': {path_a} and {path_b}")


# --- Execution ---


class ExecutionError(RowQueryError):
    """Base for query execution errors."""


class MultipleRowsError(ExecutionError):
    """Raised when fetch_one encounters more than one row."""

    def __init__(self, query_name: str, row_count: int) -> None:
        self.query_name = query_name
        self.row_count = row_count
        super().__init__(
            f"fetch_one for '{query_name}' returned {row_count} rows (expected 0 or 1)"
        )


class ParameterBindingError(ExecutionError):
    """Raised on parameter binding failures."""

    def __init__(self, query_name: str, detail: str) -> None:
        self.query_name = query_name
        super().__init__(f"Parameter binding error for '{query_name}': {detail}")


class SQLSanitizationError(ExecutionError):
    """Raised when an inline SQL string fails a sanitization check."""

    def __init__(self, detail: str) -> None:
        super().__init__(f"SQL sanitization failed: {detail}")


# --- Mapping ---


class MappingError(RowQueryError):
    """Base for mapping errors."""


class ColumnMismatchError(MappingError):
    """Raised when required fields cannot be mapped from row columns."""

    def __init__(self, target_class: str, missing_fields: list[str]) -> None:
        self.target_class = target_class
        self.missing_fields = missing_fields
        super().__init__(f"Cannot map to {target_class}: missing fields {missing_fields}")


class StrictModeViolation(MappingError):
    """Raised in strict mode for mapping integrity violations."""


class PlanCompilationError(MappingError):
    """Raised when an AggregatePlan fails validation during build()."""


# --- Transaction ---


class TransactionError(RowQueryError):
    """Base for transaction errors."""


class TransactionStateError(TransactionError):
    """Raised on invalid transaction state transitions."""

    def __init__(self, current_state: str, attempted_action: str) -> None:
        self.current_state = current_state
        self.attempted_action = attempted_action
        super().__init__(f"Cannot {attempted_action} transaction in state '{current_state}'")


# --- Migration ---


class MigrationError(RowQueryError):
    """Base for migration errors."""


class MigrationFileError(MigrationError):
    """Raised for invalid migration file naming."""

    def __init__(self, file_name: str, detail: str) -> None:
        self.file_name = file_name
        super().__init__(f"Invalid migration file '{file_name}': {detail}")


class MigrationExecutionError(MigrationError):
    """Raised when a migration fails to execute."""

    def __init__(self, version: str, detail: str) -> None:
        self.version = version
        super().__init__(f"Migration {version} failed: {detail}")


# --- Adapter ---


class AdapterError(RowQueryError):
    """Base for adapter errors."""


class ConnectionError(AdapterError):  # noqa: A001
    """Raised on connection failures."""


class PoolError(AdapterError):
    """Raised on connection pool failures."""
