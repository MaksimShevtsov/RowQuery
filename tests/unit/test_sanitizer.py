"""Unit tests for SQLSanitizer and related helpers."""

from __future__ import annotations

import pytest

from row_query.core.exceptions import SQLSanitizationError
from row_query.core.params import coerce_params, is_raw_sql
from row_query.core.sanitizer import (
    SQLSanitizer,
    _check_single_statement,
    _check_verb,
    _strip_comments,
)


# ---------------------------------------------------------------------------
# is_raw_sql
# ---------------------------------------------------------------------------


class TestIsRawSql:
    def test_sql_with_space_is_raw(self) -> None:
        assert is_raw_sql("SELECT 1") is True

    def test_sql_with_newline_is_raw(self) -> None:
        assert is_raw_sql("SELECT\n1") is True

    def test_sql_with_tab_is_raw(self) -> None:
        assert is_raw_sql("SELECT\t1") is True

    def test_registry_key_is_not_raw(self) -> None:
        assert is_raw_sql("users.get_by_id") is False

    def test_dotted_registry_key_is_not_raw(self) -> None:
        assert is_raw_sql("billing.invoice.list") is False

    def test_empty_string_is_not_raw(self) -> None:
        assert is_raw_sql("") is False


# ---------------------------------------------------------------------------
# coerce_params
# ---------------------------------------------------------------------------


class TestCoerceParams:
    def test_none_passthrough(self) -> None:
        assert coerce_params(None) is None

    def test_dict_passthrough(self) -> None:
        p = {"id": 1}
        assert coerce_params(p) is p

    def test_tuple_passthrough(self) -> None:
        p = (1, 2)
        assert coerce_params(p) == (1, 2)

    def test_list_converted_to_tuple(self) -> None:
        assert coerce_params([1, 2]) == (1, 2)

    def test_int_scalar_wrapped(self) -> None:
        assert coerce_params(42) == (42,)

    def test_str_scalar_wrapped(self) -> None:
        assert coerce_params("hello") == ("hello",)

    def test_bool_scalar_wrapped(self) -> None:
        assert coerce_params(True) == (True,)

    def test_empty_tuple_passthrough(self) -> None:
        assert coerce_params(()) == ()

    def test_empty_list_converted(self) -> None:
        assert coerce_params([]) == ()


# ---------------------------------------------------------------------------
# _strip_comments
# ---------------------------------------------------------------------------


class TestStripComments:
    def test_no_comments_passthrough(self) -> None:
        sql = "SELECT id FROM users WHERE id = 1"
        assert _strip_comments(sql) == sql

    def test_line_comment_removed(self) -> None:
        sql = "SELECT 1 -- get one"
        assert _strip_comments(sql) == "SELECT 1 "

    def test_line_comment_preserves_newline(self) -> None:
        sql = "SELECT 1 -- comment\nFROM t"
        assert _strip_comments(sql) == "SELECT 1 \nFROM t"

    def test_line_comment_at_start(self) -> None:
        sql = "-- full line comment\nSELECT 1"
        assert _strip_comments(sql) == "\nSELECT 1"

    def test_multiple_line_comments(self) -> None:
        sql = "SELECT 1 -- first\nFROM t -- second\nWHERE 1=1"
        assert _strip_comments(sql) == "SELECT 1 \nFROM t \nWHERE 1=1"

    def test_block_comment_removed(self) -> None:
        sql = "SELECT /* inline */ 1"
        assert _strip_comments(sql) == "SELECT   1"

    def test_block_comment_replaced_with_space(self) -> None:
        # Block comments become a single space to avoid token merging
        sql = "SELECT/*comment*/1"
        assert _strip_comments(sql) == "SELECT 1"

    def test_multiline_block_comment_removed(self) -> None:
        sql = "SELECT /*\n  big\n  comment\n*/ 1"
        assert _strip_comments(sql) == "SELECT   1"

    def test_string_literal_with_double_dash_preserved(self) -> None:
        sql = "SELECT '-- not a comment' FROM t"
        assert _strip_comments(sql) == sql

    def test_string_literal_with_block_comment_syntax_preserved(self) -> None:
        sql = "SELECT '/* not a comment */' FROM t"
        assert _strip_comments(sql) == sql

    def test_string_literal_adjacent_to_comment(self) -> None:
        sql = "SELECT 'value' -- comment\nFROM t"
        assert _strip_comments(sql) == "SELECT 'value' \nFROM t"

    def test_escaped_quote_in_string_literal(self) -> None:
        sql = "SELECT 'it''s fine -- not a comment' FROM t"
        assert _strip_comments(sql) == sql

    def test_unclosed_block_comment_strips_remainder(self) -> None:
        sql = "SELECT 1 /* unclosed"
        result = _strip_comments(sql)
        assert "/*" not in result
        assert result.startswith("SELECT 1 ")

    def test_line_comment_without_trailing_newline_strips_to_end(self) -> None:
        sql = "SELECT 1 -- trailing only"
        result = _strip_comments(sql)
        assert "--" not in result


# ---------------------------------------------------------------------------
# _check_single_statement
# ---------------------------------------------------------------------------


class TestCheckSingleStatement:
    def test_no_semicolon_passes(self) -> None:
        _check_single_statement("SELECT 1")  # no raise

    def test_trailing_semicolon_passes(self) -> None:
        _check_single_statement("SELECT 1;")

    def test_trailing_semicolon_with_whitespace_passes(self) -> None:
        _check_single_statement("SELECT 1;   ")

    def test_trailing_semicolon_with_newline_passes(self) -> None:
        _check_single_statement("SELECT 1;\n")

    def test_multiple_statements_raises(self) -> None:
        with pytest.raises(SQLSanitizationError, match="Multiple SQL statements"):
            _check_single_statement("SELECT 1; DROP TABLE users")

    def test_two_selects_raises(self) -> None:
        with pytest.raises(SQLSanitizationError):
            _check_single_statement("SELECT 1; SELECT 2")

    def test_no_space_between_statements_raises(self) -> None:
        with pytest.raises(SQLSanitizationError):
            _check_single_statement("SELECT 1;SELECT 2")

    def test_semicolon_in_string_literal_passes(self) -> None:
        _check_single_statement("SELECT ';' FROM t")

    def test_multiple_semicolons_in_string_passes(self) -> None:
        _check_single_statement("SELECT 'a;b;c' FROM t")

    def test_semicolon_after_string_then_statement_raises(self) -> None:
        with pytest.raises(SQLSanitizationError):
            _check_single_statement("SELECT ';' FROM t; DROP TABLE t")


# ---------------------------------------------------------------------------
# _check_verb
# ---------------------------------------------------------------------------


class TestCheckVerb:
    ALLOWED = frozenset({"SELECT", "INSERT", "UPDATE", "DELETE"})

    def test_allowed_verb_passes(self) -> None:
        _check_verb("SELECT * FROM t", self.ALLOWED)

    def test_disallowed_verb_raises(self) -> None:
        with pytest.raises(SQLSanitizationError, match="DROP"):
            _check_verb("DROP TABLE users", self.ALLOWED)

    def test_lowercase_verb_matches_case_insensitively(self) -> None:
        _check_verb("select * FROM t", self.ALLOWED)

    def test_mixed_case_verb_matches(self) -> None:
        _check_verb("Select * FROM t", self.ALLOWED)

    def test_leading_whitespace_ignored(self) -> None:
        _check_verb("  \n  SELECT * FROM t", self.ALLOWED)

    def test_truncate_blocked(self) -> None:
        with pytest.raises(SQLSanitizationError, match="TRUNCATE"):
            _check_verb("TRUNCATE TABLE users", self.ALLOWED)

    def test_alter_blocked(self) -> None:
        with pytest.raises(SQLSanitizationError, match="ALTER"):
            _check_verb("ALTER TABLE users ADD COLUMN foo TEXT", self.ALLOWED)

    def test_with_cte_can_be_allowed(self) -> None:
        allowed = self.ALLOWED | frozenset({"WITH"})
        _check_verb("WITH cte AS (SELECT 1) SELECT * FROM cte", allowed)

    def test_error_message_lists_allowed_verbs(self) -> None:
        with pytest.raises(SQLSanitizationError, match="allowed"):
            _check_verb("DROP TABLE t", frozenset({"SELECT"}))


# ---------------------------------------------------------------------------
# SQLSanitizer — configuration and defaults
# ---------------------------------------------------------------------------


class TestSQLSanitizerDefaults:
    def test_default_strips_comments(self) -> None:
        s = SQLSanitizer()
        result = s.sanitize("SELECT 1 -- comment")
        assert "--" not in result

    def test_default_blocks_multiple_statements(self) -> None:
        s = SQLSanitizer()
        with pytest.raises(SQLSanitizationError):
            s.sanitize("SELECT 1; DROP TABLE t")

    def test_default_allows_any_verb(self) -> None:
        s = SQLSanitizer()
        s.sanitize("DROP TABLE t")  # no raise — no verb restriction by default

    def test_returns_cleaned_sql_string(self) -> None:
        s = SQLSanitizer()
        result = s.sanitize("SELECT /* inline */ 1")
        assert isinstance(result, str)
        assert "/*" not in result


class TestSQLSanitizerFlags:
    def test_strip_comments_false_preserves_comments(self) -> None:
        s = SQLSanitizer(strip_comments=False)
        sql = "SELECT 1 -- comment"
        assert s.sanitize(sql) == sql

    def test_block_multiple_statements_false_allows_multi(self) -> None:
        s = SQLSanitizer(block_multiple_statements=False)
        # Should not raise
        s.sanitize("SELECT 1; SELECT 2")

    def test_allowed_verbs_none_permits_any(self) -> None:
        s = SQLSanitizer(allowed_verbs=None)
        s.sanitize("DROP TABLE t")  # no raise

    def test_allowed_verbs_set_blocks_others(self) -> None:
        s = SQLSanitizer(allowed_verbs=frozenset({"SELECT"}))
        with pytest.raises(SQLSanitizationError):
            s.sanitize("DROP TABLE t")

    def test_allowed_verbs_set_permits_listed(self) -> None:
        s = SQLSanitizer(allowed_verbs=frozenset({"SELECT", "INSERT"}))
        s.sanitize("INSERT INTO t VALUES (1)")  # no raise

    def test_all_checks_disabled(self) -> None:
        s = SQLSanitizer(
            strip_comments=False,
            block_multiple_statements=False,
            allowed_verbs=None,
        )
        # Nothing should raise, nothing should be modified
        dangerous = "DROP TABLE t; DELETE FROM users -- yolo"
        assert s.sanitize(dangerous) == dangerous


# ---------------------------------------------------------------------------
# SQLSanitizer — ordering: comments stripped before statement check
# ---------------------------------------------------------------------------


class TestSQLSanitizerOrdering:
    def test_comment_stripped_before_statement_check(self) -> None:
        # After stripping, the trailing semicolon has only whitespace after it
        s = SQLSanitizer()
        sql = "SELECT 1; -- this is a comment, not a second statement"
        # Should NOT raise: after stripping the comment, only "SELECT 1; " remains
        result = s.sanitize(sql)
        assert "SELECT 1" in result

    def test_comment_stripped_before_verb_check(self) -> None:
        # A comment-only line before the real verb should not confuse the checker
        s = SQLSanitizer(allowed_verbs=frozenset({"SELECT"}))
        sql = "-- preamble\nSELECT * FROM t"
        s.sanitize(sql)  # no raise

    def test_multiline_with_trailing_comment_passes(self) -> None:
        s = SQLSanitizer()
        sql = "SELECT *\nFROM users\nWHERE id = 1 -- primary key"
        result = s.sanitize(sql)
        assert "FROM users" in result
        assert "--" not in result


# ---------------------------------------------------------------------------
# SQLSanitizer — error message quality
# ---------------------------------------------------------------------------


class TestSQLSanitizerErrorMessages:
    def test_multiple_statements_error_message(self) -> None:
        s = SQLSanitizer()
        with pytest.raises(SQLSanitizationError) as exc_info:
            s.sanitize("SELECT 1; DROP TABLE t")
        assert "Multiple SQL statements" in str(exc_info.value)

    def test_verb_error_includes_offending_verb(self) -> None:
        s = SQLSanitizer(allowed_verbs=frozenset({"SELECT"}))
        with pytest.raises(SQLSanitizationError) as exc_info:
            s.sanitize("TRUNCATE TABLE users")
        assert "TRUNCATE" in str(exc_info.value)

    def test_verb_error_includes_allowed_list(self) -> None:
        s = SQLSanitizer(allowed_verbs=frozenset({"SELECT"}))
        with pytest.raises(SQLSanitizationError) as exc_info:
            s.sanitize("DROP TABLE t")
        assert "SELECT" in str(exc_info.value)

    def test_exception_is_subclass_of_execution_error(self) -> None:
        from row_query.core.exceptions import ExecutionError

        s = SQLSanitizer()
        with pytest.raises(ExecutionError):
            s.sanitize("SELECT 1; DROP TABLE t")
