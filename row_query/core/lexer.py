"""Minimal SQL lexer shared by the parameter normalizer, sanitizer and migrations.

Splits SQL text into tokens so that callers can operate on executable code
while leaving string literals, quoted identifiers and comments untouched.

Recognised constructs:

* ``'...'`` string literals with ``''`` escapes (and optionally ``\\'``)
* ``E'...'`` PostgreSQL escape strings (backslash escapes always apply)
* ``$tag$...$tag$`` PostgreSQL dollar-quoted strings
* ``"..."`` and ```...``` quoted identifiers
* ``-- ...`` line comments and ``/* ... */`` block comments
"""

from __future__ import annotations

import re
from typing import Literal

TokenKind = Literal["code", "string", "identifier", "comment"]
Token = tuple[TokenKind, str]

_DOLLAR_TAG = re.compile(r"\$([A-Za-z_][A-Za-z0-9_]*)?\$")
_EOL = re.compile(r"[\r\n]")


class UnterminatedTokenError(ValueError):
    """Raised in strict mode when a literal, identifier or comment is not closed."""


def _scan_quoted(sql: str, start: int, quote: str, backslash_escapes: bool) -> int | None:
    """Return the index just past the closing *quote*, or None if unterminated.

    *start* is the index of the opening quote. Doubled quotes are escapes.
    """
    i = start + 1
    n = len(sql)
    while i < n:
        ch = sql[i]
        if backslash_escapes and ch == "\\":
            i += 2
            continue
        if ch == quote:
            if i + 1 < n and sql[i + 1] == quote:
                i += 2
                continue
            return i + 1
        i += 1
    return None


def tokenize(
    sql: str,
    *,
    backslash_escapes: bool = False,
    strict: bool = False,
) -> list[Token]:
    """Split *sql* into code, string, identifier and comment tokens.

    Args:
        sql: SQL text.
        backslash_escapes: Treat ``\\`` as an escape inside plain ``'...'``
            literals (MySQL default). ``E'...'`` literals always honour
            backslash escapes.
        strict: Raise :class:`UnterminatedTokenError` on unterminated
            constructs. Otherwise the remainder of the text becomes one token.
    """
    tokens: list[Token] = []
    i = 0
    n = len(sql)
    code_start = 0

    def flush_code(end: int) -> None:
        if end > code_start:
            tokens.append(("code", sql[code_start:end]))

    def unterminated(kind: TokenKind, start: int, what: str) -> int:
        if strict:
            raise UnterminatedTokenError(f"Unterminated {what} detected in SQL")
        tokens.append((kind, sql[start:]))
        return n

    while i < n:
        ch = sql[i]
        prev_is_word = i > 0 and (sql[i - 1].isalnum() or sql[i - 1] in "_$")

        if ch in "eE" and i + 1 < n and sql[i + 1] == "'" and not prev_is_word:
            flush_code(i)
            end = _scan_quoted(sql, i + 1, "'", backslash_escapes=True)
            i = (
                unterminated("string", i, "string literal")
                if end is None
                else _append(tokens, "string", sql, i, end)
            )
            code_start = i
        elif ch == "'":
            flush_code(i)
            end = _scan_quoted(sql, i, "'", backslash_escapes)
            i = (
                unterminated("string", i, "string literal")
                if end is None
                else _append(tokens, "string", sql, i, end)
            )
            code_start = i
        elif ch in '"`':
            flush_code(i)
            end = _scan_quoted(sql, i, ch, backslash_escapes=False)
            what = "double-quoted identifier" if ch == '"' else "backtick-quoted identifier"
            i = (
                unterminated("identifier", i, what)
                if end is None
                else _append(tokens, "identifier", sql, i, end)
            )
            code_start = i
        elif ch == "$" and not prev_is_word and (m := _DOLLAR_TAG.match(sql, i)):
            flush_code(i)
            tag = m.group(0)
            close = sql.find(tag, m.end())
            i = (
                unterminated("string", i, "dollar-quoted string")
                if close == -1
                else _append(tokens, "string", sql, i, close + len(tag))
            )
            code_start = i
        elif sql.startswith("--", i):
            flush_code(i)
            # PostgreSQL ends line comments at \r as well as \n; ending at the
            # earliest of the two never hides code from the sanitizer.
            m = _EOL.search(sql, i)
            end = n if m is None else m.start()
            i = _append(tokens, "comment", sql, i, end)
            code_start = i
        elif sql.startswith("/*", i):
            flush_code(i)
            close = sql.find("*/", i + 2)
            i = (
                unterminated("comment", i, "block comment")
                if close == -1
                else _append(tokens, "comment", sql, i, close + 2)
            )
            code_start = i
        else:
            i += 1

    flush_code(n)
    return tokens


def _append(tokens: list[Token], kind: TokenKind, sql: str, start: int, end: int) -> int:
    tokens.append((kind, sql[start:end]))
    return end


def split_statements(sql: str, *, backslash_escapes: bool = False) -> list[str]:
    """Split a SQL script on top-level ``;`` separators.

    Semicolons inside literals, quoted identifiers, dollar-quoted bodies and
    comments are ignored. Statements consisting only of whitespace and
    comments are dropped. Trailing semicolons are not included.
    """
    statements: list[str] = []
    current: list[str] = []
    has_code = False

    def finish() -> None:
        nonlocal has_code
        if has_code:
            statements.append("".join(current).strip())
        current.clear()
        has_code = False

    for kind, text in tokenize(sql, backslash_escapes=backslash_escapes):
        if kind != "code":
            current.append(text)
            has_code = has_code or kind != "comment"
            continue
        parts = text.split(";")
        for idx, part in enumerate(parts):
            if idx > 0:
                finish()
            current.append(part)
            has_code = has_code or bool(part.strip())
    finish()
    return statements
