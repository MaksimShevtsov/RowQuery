"""SQL parameter normalization.

Converts `:name` parameter syntax to driver-specific format.
Handles string literal exclusion and PostgreSQL `::typecast` syntax.
"""

from __future__ import annotations

import re
from functools import lru_cache
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from row_query.core.registry import SQLRegistry
    from row_query.core.sanitizer import SQLSanitizer

# Matches :name but not ::typecast and not inside words
# Negative lookbehind for : (handles ::), \w (handles mid-word colons)
_PARAM_PATTERN = re.compile(r"(?<![:\w]):([a-zA-Z_]\w*)")

# Matches single-quoted string literals (with escaped quotes handled)
_STRING_LITERAL_PATTERN = re.compile(r"'(?:[^'\\]|\\.)*'")


def normalize_params(sql: str, paramstyle: str) -> str:
    """Convert :name parameters to the target param style.

    Args:
        sql: SQL string with :name parameters.
        paramstyle: Target style - 'named' (no conversion) or 'pyformat' (%(name)s).

    Returns:
        SQL with parameters converted to the target style.
    """
    if paramstyle == "named":
        return sql
    return _convert_to_pyformat(sql)


@lru_cache(maxsize=256)
def _convert_to_pyformat(sql: str) -> str:
    """Convert :name params to %(name)s, preserving string literals."""
    # Tokenize: split into string literals and non-literal segments
    parts: list[str] = []
    last_end = 0

    for match in _STRING_LITERAL_PATTERN.finditer(sql):
        start, end = match.span()
        # Replace params in the non-literal segment before this string
        if start > last_end:
            parts.append(_PARAM_PATTERN.sub(r"%(\1)s", sql[last_end:start]))
        # Keep string literal as-is
        parts.append(match.group())
        last_end = end

    # Handle remaining text after last string literal
    if last_end < len(sql):
        parts.append(_PARAM_PATTERN.sub(r"%(\1)s", sql[last_end:]))

    return "".join(parts)


def is_raw_sql(query: str) -> bool:
    """Return True if query is an inline SQL string rather than a registry key.

    Registry keys use dot-notation (e.g. ``users.get_by_id``) and never
    contain whitespace.  Any SQL statement will contain at least one space.

    Note: Registry keys are validated during registration to ensure they do
    not contain whitespace, preventing ambiguity.
    """
    return any(c.isspace() for c in query)


def coerce_params(
    params: dict[str, Any] | tuple[Any, ...] | list[Any] | Any,
) -> dict[str, Any] | tuple[Any, ...] | None:
    """Normalize *params* to a dict, tuple, or None.

    * ``None`` / ``dict`` → returned as-is (named parameter binding).
    * ``tuple`` / ``list`` → converted to ``tuple`` (positional binding).
    * Any other scalar → wrapped in a single-element tuple.

    Note on parameter styles:
        Registry queries use `:name` style parameters (converted to driver format).
        Inline SQL can use either `:name` or `?`-style placeholders depending on
        the database driver. When using inline SQL with positional parameters,
        ensure compatibility with your target database (SQLite uses `?`, PostgreSQL
        uses `$1`, etc.).
    """
    if params is None or isinstance(params, dict):
        return params
    if isinstance(params, (tuple, list)):
        return tuple(params)
    return (params,)


def resolve_sql(
    query: str,
    registry: "SQLRegistry",
    sanitizer: "SQLSanitizer | None" = None,
) -> tuple[str, str]:
    """Return ``(sql_text, label)`` for *query*.

    If *query* is an inline SQL string (contains whitespace) it is returned
    after optional sanitization.  Otherwise it is looked up in *registry* by
    name (registry queries are trusted and never sanitized).  *label* is used
    in error messages.

    Args:
        query: Either a registry key (e.g. "users.get_by_id") or inline SQL.
        registry: SQLRegistry instance for looking up named queries.
        sanitizer: Optional SQLSanitizer applied only to inline SQL strings.

    Returns:
        Tuple of (sql_text, label) where label is "<inline>" for inline SQL
        or the registry key for named queries.
    """
    if is_raw_sql(query):
        sql = sanitizer.sanitize(query) if sanitizer is not None else query
        return sql, "<inline>"
    return registry.get(query), query
