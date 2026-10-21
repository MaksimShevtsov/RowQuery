"""Inline SQL sanitizer.

Only applied to raw SQL strings passed directly to engine/transaction methods.
Queries loaded from the SQLRegistry are trusted and never sanitized.

All checks are fail-closed: SQL the sanitizer cannot classify unambiguously
is rejected rather than passed through.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from row_query.core.exceptions import SQLSanitizationError
from row_query.core.lexer import Token, UnterminatedTokenError, tokenize

_LEADING_WORD = re.compile(r"\s*([A-Za-z_]\w*)")


# ---------------------------------------------------------------------------
# Internal tokenizer
# ---------------------------------------------------------------------------


def _tokenize(sql: str) -> list[Token]:
    """Tokenize *sql* strictly for sanitization.

    The sanitizer does not know the target database, so constructs whose
    extent differs between dialects are rejected (fail-closed) instead of
    guessed:

    * a backslash inside ``'...'``, ``"..."`` or ```...``` — MySQL treats
      ``\\'`` / ``\\"`` as escaped quotes (and ``"..."`` as a string), while
      PostgreSQL does not. Use bound parameters or ``E'...'`` instead.
    * ``#`` outside literals — a line comment in MySQL, an operator elsewhere.

    Raises:
        SQLSanitizationError: On unterminated or ambiguous tokens.
    """
    try:
        tokens = tokenize(sql, strict=True)
    except UnterminatedTokenError as e:
        raise SQLSanitizationError(str(e)) from None
    for kind, text in tokens:
        if kind in ("string", "identifier") and text[0] in "'\"`" and "\\" in text:
            raise SQLSanitizationError(
                "Backslashes in quoted literals or identifiers are ambiguous across "
                "databases; pass the value as a bound parameter instead"
            )
        if kind == "code" and "#" in text:
            raise SQLSanitizationError(
                "'#' outside string literals is ambiguous across databases (a comment in MySQL)"
            )
    return tokens


# ---------------------------------------------------------------------------
# Individual sanitization checks
# ---------------------------------------------------------------------------


def _strip_comments(sql: str) -> str:
    """Remove SQL comments while preserving string literals and identifiers.

    Line comments are replaced by nothing (their newline is kept), block
    comments by a single space.
    """
    parts: list[str] = []
    for kind, text in _tokenize(sql):
        if kind == "comment":
            parts.append(" " if text.startswith("/*") else "")
        else:
            parts.append(text)
    return "".join(parts)


def _check_single_statement(sql: str) -> None:
    """Raise if *sql* contains a ``;`` followed by more executable content."""
    terminated = False
    for kind, text in _tokenize(sql):
        if kind == "comment":
            continue
        if kind == "string" and text.startswith("$") and ";" in text:
            # $tag$...$tag$ is a literal only in PostgreSQL; elsewhere the ';' is code.
            raise SQLSanitizationError(
                "Semicolons inside dollar-quoted strings are not permitted in inline SQL"
            )
        for ch in text if kind == "code" else "x":
            if terminated and not ch.isspace():
                raise SQLSanitizationError(
                    "Multiple SQL statements are not permitted in inline SQL"
                )
            if ch == ";":
                terminated = True


def _leading_verb(sql: str) -> str | None:
    """Return the first keyword of *sql*, skipping comments, or None."""
    for kind, text in _tokenize(sql):
        if kind == "comment" or (kind == "code" and not text.strip()):
            continue
        if kind != "code":
            return None
        m = _LEADING_WORD.match(text)
        return m.group(1).upper() if m else None
    return None


def _check_verb(sql: str, allowed: frozenset[str]) -> None:
    """Raise unless the leading SQL keyword is in *allowed* (fail-closed)."""
    verb = _leading_verb(sql)
    if verb is None:
        raise SQLSanitizationError(f"Could not determine the SQL verb; allowed: {sorted(allowed)}")
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
        queries with placeholders (``:name`` or ``?``) to prevent SQL injection.
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
            Leading comments are skipped; SQL whose verb cannot be determined
            is rejected. Note that allowing ``WITH`` permits any statement a
            CTE can prefix (e.g. ``WITH ... DELETE`` in PostgreSQL).
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
        tokens = _tokenize(sql)  # always reject unterminated / ambiguous literals
        if not self.strip_comments and any(
            kind == "comment" and text.startswith("/*!") for kind, text in tokens
        ):
            raise SQLSanitizationError(
                "MySQL executable comments (/*! ... */) are not permitted in inline SQL"
            )
        if self.strip_comments:
            sql = _strip_comments(sql)
        if self.block_multiple_statements:
            _check_single_statement(sql)
        if self.allowed_verbs is not None:
            _check_verb(sql, self.allowed_verbs)
        return sql
