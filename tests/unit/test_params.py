"""Unit tests for parameter normalizer."""

from __future__ import annotations

from row_query.core.params import normalize_params


class TestNormalizeParams:
    def test_named_passthrough(self) -> None:
        sql = "SELECT * FROM users WHERE id = :user_id"
        assert normalize_params(sql, "named") == sql

    def test_pyformat_conversion(self) -> None:
        sql = "SELECT * FROM users WHERE id = :user_id"
        expected = "SELECT * FROM users WHERE id = %(user_id)s"
        assert normalize_params(sql, "pyformat") == expected

    def test_multiple_params(self) -> None:
        sql = "SELECT * FROM users WHERE id = :id AND name = :name"
        expected = "SELECT * FROM users WHERE id = %(id)s AND name = %(name)s"
        assert normalize_params(sql, "pyformat") == expected

    def test_typecast_exclusion(self) -> None:
        sql = "SELECT value::integer FROM t WHERE id = :id"
        expected = "SELECT value::integer FROM t WHERE id = %(id)s"
        assert normalize_params(sql, "pyformat") == expected

    def test_string_literal_exclusion(self) -> None:
        sql = "SELECT * FROM t WHERE col = ':not_a_param' AND id = :id"
        expected = "SELECT * FROM t WHERE col = ':not_a_param' AND id = %(id)s"
        assert normalize_params(sql, "pyformat") == expected

    def test_duplicate_param_names(self) -> None:
        sql = "SELECT * FROM t WHERE a = :val OR b = :val"
        expected = "SELECT * FROM t WHERE a = %(val)s OR b = %(val)s"
        assert normalize_params(sql, "pyformat") == expected

    def test_no_params(self) -> None:
        sql = "SELECT 1"
        assert normalize_params(sql, "pyformat") == sql

    def test_cache_returns_same_result(self) -> None:
        sql = "SELECT * FROM t WHERE id = :id"
        result1 = normalize_params(sql, "pyformat")
        result2 = normalize_params(sql, "pyformat")
        assert result1 == result2

    def test_underscore_in_param_name(self) -> None:
        sql = "SELECT * FROM t WHERE user_id = :user_id"
        expected = "SELECT * FROM t WHERE user_id = %(user_id)s"
        assert normalize_params(sql, "pyformat") == expected
