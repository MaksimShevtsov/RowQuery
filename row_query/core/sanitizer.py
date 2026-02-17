"""Inline SQL sanitizer.

Only applied to raw SQL strings passed directly to engine/transaction methods.
Queries loaded from the SQLRegistry are trusted and never sanitized.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from row_query.core.exceptions import SQLSanitizationError

# Matches the first SQL keyword (used for verb allow-listing)
_FIRST_KEYWORD = re.compile(r"^\s*(\w+)")


# ---------------------------------------------------------------------------
# Internal tokenizer
# ---------------------------------------------------------------------------


def _tokenize(sql: str) -> list[tuple[str, str]]:
    """Split *sql* into ``('string', …)``, ``('identifier', …)``, and ``('code', …)`` tokens.

    String literals (single-quoted, with ``''`` escapes) are preserved as-is.
    Identifiers (double-quoted for PostgreSQL/MySQL ANSI_QUOTES, backtick-quoted
    for MySQL) are also preserved to avoid stripping comment-like syntax inside them.
    Everything else is a ``'code'`` token.

    Raises:
        SQLSanitizationError: If an unterminated string literal or identifier is detected.
    """
    tokens: list[tuple[str, str]] = []
    i = 0
    n = len(sql)
    last = 0

    while i < n:
        # Single-quoted string literal
        if sql[i] == "'":
            if i > last:
                tokens.append(("code", sql[last:i]))
            j = i + 1
            while j < n:
                if sql[j] == "'":
                    j += 1
                    if j >= n or sql[j] != "'":
                        break  # end of literal
                    j += 1  # '' escape — continue
                else:
                    j += 1
            # Check for unterminated string
            if j >= n and (j == i + 1 or sql[j - 1] != "'"):
                from row_query.core.exceptions import SQLSanitizationError
                raise SQLSanitizationError(
                    "Unterminated string literal detected in SQL"
                )
            tokens.append(("string", sql[i:j]))
            last = j
            i = j
        # Double-quoted identifier (PostgreSQL, MySQL ANSI_QUOTES)
        elif sql[i] == '"':
            if i > last:
                tokens.append(("code", sql[last:i]))
            j = i + 1
            while j < n:
                if sql[j] == '"':
                    j += 1
                    if j >= n or sql[j] != '"':
                        break  # end of identifier
                    j += 1  # "" escape — continue
                else:
                    j += 1
            # Check for unterminated identifier
            if j >= n and (j == i + 1 or sql[j - 1] != '"'):
                from row_query.core.exceptions import SQLSanitizationError
                raise SQLSanitizationError(
                    "Unterminated double-quoted identifier detected in SQL"
                )
            tokens.append(("identifier", sql[i:j]))
            last = j
            i = j
        # Backtick-quoted identifier (MySQL)
        elif sql[i] == "`":
            if i > last:
                tokens.append(("code", sql[last:i]))
            j = i + 1
            while j < n:
                if sql[j] == "`":
                    j += 1
                    if j >= n or sql[j] != "`":
                        break  # end of identifier
                    j += 1  # `` escape — continue
                else:
                    j += 1
            # Check for unterminated identifier
            if j >= n and (j == i + 1 or sql[j - 1] != "`"):
                from row_query.core.exceptions import SQLSanitizationError
                raise SQLSanitizationError(
                    "Unterminated backtick-quoted identifier detected in SQL"
                )
            tokens.append(("identifier", sql[i:j]))
            last = j
            i = j
        else:
            i += 1

    if last < n:
        tokens.append(("code", sql[last:]))

    return tokens


def _strip_comments_in_code(code: str) -> str:
    """Remove ``--`` line comments and ``/* */`` block comments from a code segment."""
    result: list[str] = []
    i = 0
    n = len(code)

    while i < n:
        if code[i : i + 2] == "--":
            j = code.find("\n", i)
            if j == -1:
                break
            result.append("\n")
            i = j + 1
        elif code[i : i + 2] == "/*":
            j = code.find("*/", i + 2)
            if j == -1:
                break
            result.append(" ")
            i = j + 2
        else:
            result.append(code[i])
            i += 1

    return "".join(result)


# ---------------------------------------------------------------------------
# Individual sanitization checks
# ---------------------------------------------------------------------------


def _strip_comments(sql: str) -> str:
    """Remove SQL comments while preserving string literals and identifiers."""
    parts: list[str] = []
    for kind, content in _tokenize(sql):
        if kind in ("string", "identifier"):
            parts.append(content)
        else:
            parts.append(_strip_comments_in_code(content))
    return "".join(parts)


def _check_single_statement(sql: str) -> None:
    """Raise if *sql* contains a semicolon followed by non-whitespace content."""
    for kind, content in _tokenize(sql):
        if kind in ("string", "identifier"):
            continue
        for i, ch in enumerate(content):
            if ch == ";" and content[i + 1 :].strip():
                raise SQLSanitizationError(
                    "Multiple SQL statements are not permitted in inline SQL"
                )


def _check_verb(sql: str, allowed: frozenset[str]) -> None:
    """Raise if the leading SQL keyword is not in *allowed*."""
    m = _FIRST_KEYWORD.match(sql)
    if m:
        verb = m.group(1).upper()
        if verb not in allowed:
            raise SQLSanitizationError(
                f"SQL verb '{verb}' is not permitted; allowed: {sorted(allowed)}"
            )


# ---------------------------------------------------------------------------
# Public class
# ---------------------------------------------------------------------------


@dataclass
class SQLSanitizer:
    """Configurable sanitizer for inline SQL strings.

    Applied only to raw SQL passed directly to engine/transaction methods.
    Registry-loaded queries are always trusted and never sanitized.

    **IMPORTANT SECURITY WARNING:**
        This sanitizer does NOT protect against SQL injection if user-provided
        data is concatenated directly into SQL strings. You MUST use parameterized
        queries with placeholders (e.g., `?` or `:name`) to prevent SQL injection.
        The sanitizer only provides defense-in-depth measures (comment stripping,
        statement blocking, verb restrictions) but is NOT a substitute for proper
        parameterization.

        Example of UNSAFE code:
            # NEVER DO THIS - vulnerable to SQL injection
            engine.fetch_all(f"SELECT * FROM users WHERE name = '{user_input}'")

        Example of SAFE code:
            # ALWAYS USE THIS - parameterized query
            engine.fetch_all("SELECT * FROM users WHERE name = ?", user_input)

    Attributes:
        strip_comments: Strip ``--`` and ``/* */`` comments before execution.
        block_multiple_statements: Reject SQL that contains a statement-
            terminating ``;`` followed by additional content (prevents query
            stacking such as ``SELECT 1; DROP TABLE users``).
        allowed_verbs: If not ``None``, only SQL statements whose first keyword
            appears in this set are permitted.  ``None`` means no restriction.
            Example: ``frozenset({"SELECT", "INSERT", "UPDATE", "DELETE"})``.
    """

    strip_comments: bool = True
    block_multiple_statements: bool = True
    allowed_verbs: frozenset[str] | None = None

    def sanitize(self, sql: str) -> str:
        """Apply all configured checks to *sql* and return the (cleaned) SQL.

        Raises:
            SQLSanitizationError: If any enabled check fails.
        """
        if self.strip_comments:
            sql = _strip_comments(sql)
        if self.block_multiple_statements:
            _check_single_statement(sql)
        if self.allowed_verbs is not None:
            _check_verb(sql, self.allowed_verbs)
        return sql
