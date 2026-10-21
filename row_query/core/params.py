"""SQL parameter normalization.

Named parameters are written as ``:name`` and positional parameters as ``?``
in every backend; both are converted to the driver's native placeholder
syntax. String literals, quoted identifiers, comments and PostgreSQL
``::typecast`` syntax are left untouched.
"""

from __future__ import annotations

import re
from functools import lru_cache
from typing import TYPE_CHECKING, Any

from row_query.core.lexer import tokenize

if TYPE_CHECKING:
    from row_query.core.registry import SQLRegistry
    from row_query.core.sanitizer import SQLSanitizer

# Matches :name but not ::typecast and not inside words
# Negative lookbehind for : (handles ::), \w (handles mid-word colons)
_PARAM_PATTERN = re.compile(r"(?<![:\w]):([a-zA-Z_]\w*)")

# Registry keys: dot-separated identifiers
_REGISTRY_KEY_PATTERN = re.compile(r"^[A-Za-z_][\w]*(\.[A-Za-z_][\w]*)+$")

Params = dict[str, Any] | tuple[Any, ...] | None


def normalize_params(sql: str, paramstyle: str) -> str:
    """Convert :name parameters to the target param style.

    For ``pyformat`` every literal ``%`` is doubled so that the driver does
    not treat it as a format directive. If *sql* contains no ``:name``
    parameters it is returned unchanged (and must then be executed without
    parameters, see :func:`bind_params`).

    Args:
        sql: SQL string with :name parameters.
        paramstyle: Target style - 'named' (no conversion) or 'pyformat' (%(name)s).

    Returns:
        SQL with parameters converted to the target style.
    """
    if paramstyle == "named":
        return sql
    return _convert_named_to_pyformat(sql)


def _rewrite_code(sql: str, pattern: re.Pattern[str], repl: Any, escape_percent: bool) -> str:
    """Apply *pattern* → *repl* in code tokens only; returns sql unchanged if no match."""
    tokens = tokenize(sql, backslash_escapes=True)
    if not any(kind == "code" and pattern.search(text) for kind, text in tokens):
        return sql
    parts: list[str] = []
    for kind, text in tokens:
        if escape_percent:
            text = text.replace("%", "%%")
        if kind == "code":
            text = pattern.sub(repl, text)
        parts.append(text)
    return "".join(parts)


@lru_cache(maxsize=256)
def _convert_named_to_pyformat(sql: str) -> str:
    """Convert :name params to %(name)s, escaping literal % characters."""
    return _rewrite_code(sql, _PARAM_PATTERN, r"%(\1)s", escape_percent=True)


_QMARK_PATTERN = re.compile(r"\?")


@lru_cache(maxsize=256)
def _convert_qmark(sql: str, positional_style: str) -> str:
    """Convert positional ``?`` placeholders to ``%s`` (format) or ``:1`` (numeric)."""
    if positional_style == "format":
        return _rewrite_code(sql, _QMARK_PATTERN, "%s", escape_percent=True)

    counter = 0

    def numbered(_: re.Match[str]) -> str:
        nonlocal counter
        counter += 1
        return f":{counter}"

    return _rewrite_code(sql, _QMARK_PATTERN, numbered, escape_percent=False)


def bind_params(
    sql: str,
    params: Params,
    paramstyle: str,
    positional_style: str = "qmark",
) -> tuple[str, Params]:
    """Return ``(sql, params)`` ready to hand to a driver.

    Args:
        sql: SQL using ``:name`` or ``?`` placeholders.
        params: Output of :func:`coerce_params`.
        paramstyle: Driver named style - ``"named"`` or ``"pyformat"``.
        positional_style: Driver positional style - ``"qmark"`` (``?``),
            ``"numeric"`` (``:1``) or ``"format"`` (``%s``).

    * ``dict``/``None`` params: ``:name`` placeholders are converted. For
      ``pyformat`` drivers, SQL without placeholders is passed through
      unchanged with ``None`` params so that literal ``%`` survives.
    * ``tuple`` params: ``?`` placeholders are converted to the positional
      style. SQL already written in the native positional style contains no
      ``?`` and is passed through unchanged.
    """
    if isinstance(params, tuple):
        if positional_style == "qmark":
            return sql, params
        return _convert_qmark(sql, positional_style), params

    if paramstyle == "named":
        return sql, params

    converted = normalize_params(sql, paramstyle)
    if converted == sql:  # no placeholders: conversion always changes the text
        return sql, None
    return converted, params


def is_raw_sql(query: str) -> bool:
    """Return True if query is an inline SQL string rather than a registry key.

    Registry keys use dot-notation (e.g. ``users.get_by_id``) and never
    contain whitespace.  Any string containing whitespace is inline SQL.

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
        Use ``:name`` placeholders with a dict, or ``?`` placeholders with
        positional values. Both are portable: RowQuery converts them to the
        driver's native syntax (``%(name)s``/``%s`` for psycopg and MySQL
        drivers, ``:1`` for Oracle).
    """
    if params is None or isinstance(params, dict):
        return params
    if isinstance(params, (tuple, list)):
        return tuple(params)
    return (params,)


def resolve_sql(
    query: str,
    registry: SQLRegistry,
    sanitizer: SQLSanitizer | None = None,
) -> tuple[str, str]:
    """Return ``(sql_text, label)`` for *query*.

    *query* is looked up in *registry* when it is a registered name or looks
    like a dotted registry key (``users.get_by_id``). Anything else —
    including single-word statements such as ``COMMIT`` or ``VACUUM`` — is
    treated as inline SQL and passed through the optional *sanitizer*.
    Registry queries are trusted and never sanitized. *label* is used in
    error messages.

    Args:
        query: Either a registry key (e.g. "users.get_by_id") or inline SQL.
        registry: SQLRegistry instance for looking up named queries.
        sanitizer: Optional SQLSanitizer applied only to inline SQL strings.

    Returns:
        Tuple of (sql_text, label) where label is "<inline>" for inline SQL
        or the registry key for named queries.
    """
    if not is_raw_sql(query) and (
        registry.has(query) or _REGISTRY_KEY_PATTERN.match(query) is not None
    ):
        return registry.get(query), query
    sql = sanitizer.sanitize(query) if sanitizer is not None else query
    return sql, "<inline>"
