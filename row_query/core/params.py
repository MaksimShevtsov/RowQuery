"""SQL parameter normalization.

Converts `:name` parameter syntax to driver-specific format.
Handles string literal exclusion and PostgreSQL `::typecast` syntax.
"""

from __future__ import annotations

import re
from functools import lru_cache
from typing import Any

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
    """
    return any(c.isspace() for c in query)


def coerce_params(
    params: dict[str, Any] | tuple[Any, ...] | list[Any] | Any,
) -> dict[str, Any] | tuple[Any, ...] | None:
    """Normalize *params* to a dict, tuple, or None.

    * ``None`` / ``dict`` → returned as-is (named parameter binding).
    * ``tuple`` / ``list`` → converted to ``tuple`` (positional binding).
    * Any other scalar → wrapped in a single-element tuple.
    """
    if params is None or isinstance(params, dict):
        return params
    if isinstance(params, (tuple, list)):
        return tuple(params)
    return (params,)
