"""Regression tests for parameter binding, sanitizer, lexer, migrations and adapters."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pytest

from row_query.core.connection import ConnectionConfig, ConnectionManager
from row_query.core.exceptions import (
    MigrationFileError,
    SQLSanitizationError,
    StrictModeViolation,
)
from row_query.core.lexer import split_statements
from row_query.core.migration import MigrationManager
from row_query.core.params import bind_params, normalize_params
from row_query.core.sanitizer import SQLSanitizer
from row_query.mapping.aggregate import AggregateMapper
from row_query.mapping.builder import aggregate


class TestPyformatConversion:
    def test_percent_in_literal_is_escaped_when_binding(self) -> None:
        sql = "SELECT * FROM t WHERE n LIKE 'a%' AND id = :id"
        assert normalize_params(sql, "pyformat") == (
            "SELECT * FROM t WHERE n LIKE 'a%%' AND id = %(id)s"
        )

    def test_sql_without_placeholders_is_untouched_and_unbound(self) -> None:
        sql = "SELECT * FROM t WHERE n LIKE 'a%'"
        assert bind_params(sql, None, "pyformat", "format") == (sql, None)
        assert bind_params(sql, {"unused": 1}, "pyformat", "format") == (sql, None)

    def test_named_param_in_comment_and_dollar_quote_ignored(self) -> None:
        sql = "SELECT $$ :a $$, :b::int -- :c"
        assert normalize_params(sql, "pyformat") == "SELECT $$ :a $$, %(b)s::int -- :c"

    def test_qmark_converted_for_format_drivers(self) -> None:
        sql, params = bind_params(
            "SELECT * FROM t WHERE a = ? AND b LIKE 'x%' AND c = '?'", (1,), "pyformat", "format"
        )
        assert sql == "SELECT * FROM t WHERE a = %s AND b LIKE 'x%%' AND c = '?'"
        assert params == (1,)

    def test_qmark_converted_for_oracle(self) -> None:
        sql, _ = bind_params("SELECT * FROM t WHERE a = ? AND b = ?", (1, 2), "named", "numeric")
        assert sql == "SELECT * FROM t WHERE a = :1 AND b = :2"

    def test_native_format_placeholders_pass_through(self) -> None:
        sql = "SELECT * FROM t WHERE a = %s"
        assert bind_params(sql, (1,), "pyformat", "format") == (sql, (1,))

    def test_qmark_untouched_for_sqlite(self) -> None:
        sql = "SELECT * FROM t WHERE a = ?"
        assert bind_params(sql, (1,), "named", "qmark") == (sql, (1,))


class TestSanitizerBypasses:
    @pytest.mark.parametrize(
        "sql",
        [
            "SELECT $a$'$a$; DROP TABLE users; --'",
            "SELECT E'x\\'' ; DROP TABLE users; --'",
            "SELECT 'x\\'' ; DROP TABLE users; --'",
        ],
    )
    def test_stacked_queries_hidden_in_literals_are_rejected(self, sql: str) -> None:
        with pytest.raises(SQLSanitizationError):
            SQLSanitizer().sanitize(sql)

    def test_dollar_quoted_strings_are_rejected(self) -> None:
        with pytest.raises(SQLSanitizationError, match="Dollar-quoted"):
            SQLSanitizer().sanitize("SELECT $$ harmless $$")

    def test_comment_requires_space_after_dashes(self) -> None:
        sanitizer = SQLSanitizer(strip_comments=False)
        # MySQL parses this as `1 - -x; DROP ...`, not as a comment
        with pytest.raises(SQLSanitizationError, match="followed by a space"):
            sanitizer.sanitize("SELECT 1 --x; DROP TABLE users")
        assert sanitizer.sanitize("SELECT 1 -- ok") == "SELECT 1 -- ok"

    @pytest.mark.parametrize(
        "sql",
        [
            # MySQL: "..." is a string with backslash escapes, not an identifier
            'SELECT "a\\"" ; DROP TABLE users; -- "',
            # MySQL: '#' starts a line comment
            "SELECT 1 # '\n; DROP TABLE users; -- '",
            # MySQL: $a$ is an identifier, so the ';' is code
            "SELECT $a$ ; DROP TABLE users; $a$",
            # MySQL: a quote inside the "dollar body" opens a string that swallows the rest
            "SELECT $a$ ' $a$, '; DROP TABLE users; -- '",
            # MySQL NO_BACKSLASH_ESCAPES: \ does not escape in E'...'
            "SELECT E'x\\'', 1 ; DROP TABLE users; -- '",
        ],
    )
    def test_mysql_parser_differentials_are_rejected(self, sql: str) -> None:
        with pytest.raises(SQLSanitizationError):
            SQLSanitizer().sanitize(sql)

    def test_verb_check_skips_leading_comment(self) -> None:
        sanitizer = SQLSanitizer(strip_comments=False, allowed_verbs=frozenset({"SELECT"}))
        with pytest.raises(SQLSanitizationError, match="DELETE"):
            sanitizer.sanitize("/* x */ DELETE FROM users")

    def test_verb_check_is_fail_closed(self) -> None:
        sanitizer = SQLSanitizer(allowed_verbs=frozenset({"SELECT"}))
        with pytest.raises(SQLSanitizationError, match="Could not determine"):
            sanitizer.sanitize("(SELECT 1)")

    def test_mysql_executable_comment_rejected_when_not_stripped(self) -> None:
        with pytest.raises(SQLSanitizationError, match="executable comments"):
            SQLSanitizer(strip_comments=False).sanitize("SELECT 1 /*!; DROP TABLE users */")


class TestSplitStatements:
    def test_splits_on_top_level_semicolons_only(self) -> None:
        script = """
            CREATE TABLE a (x TEXT DEFAULT ';');  -- trailing; comment
            CREATE FUNCTION f() RETURNS int AS $$ BEGIN RETURN 1; END; $$ LANGUAGE plpgsql;
            /* only a comment; */
        """
        assert split_statements(script) == [
            "CREATE TABLE a (x TEXT DEFAULT ';')",
            "-- trailing; comment\n            CREATE FUNCTION f() RETURNS int AS "
            "$$ BEGIN RETURN 1; END; $$ LANGUAGE plpgsql",
        ]

    def test_backslash_escapes(self) -> None:
        assert split_statements(r"SELECT 'a\';b'; SELECT 2", backslash_escapes=True) == [
            r"SELECT 'a\';b'",
            "SELECT 2",
        ]


@pytest.fixture
def manager(tmp_path: Path) -> ConnectionManager:
    config = ConnectionConfig(driver="sqlite", database=str(tmp_path / "m.db"), pool_size=1)
    return ConnectionManager(config)


class TestMigrations:
    def test_multiple_statements_per_file(
        self, tmp_path: Path, manager: ConnectionManager
    ) -> None:
        mig = tmp_path / "migrations"
        mig.mkdir()
        (mig / "001_init.sql").write_text(
            "CREATE TABLE a (id INTEGER);\nCREATE TABLE b (id INTEGER);\n", encoding="utf-8"
        )
        MigrationManager(mig, manager).apply()
        with manager.get_connection() as conn:
            names = {r[0] for r in conn.execute("SELECT name FROM sqlite_master")}
        assert {"a", "b"} <= names

    def test_numeric_ordering(self, tmp_path: Path, manager: ConnectionManager) -> None:
        mig = tmp_path / "migrations"
        mig.mkdir()
        (mig / "9_a.sql").write_text("CREATE TABLE x (id INTEGER)", encoding="utf-8")
        (mig / "10_b.sql").write_text("CREATE INDEX ix ON x (id)", encoding="utf-8")
        mm = MigrationManager(mig, manager)
        assert [m.version for m in mm.apply()] == ["9", "10"]
        assert mm.current_version() == "10"

    def test_duplicate_numbers_rejected(self, tmp_path: Path, manager: ConnectionManager) -> None:
        mig = tmp_path / "migrations"
        mig.mkdir()
        (mig / "1_a.sql").write_text("SELECT 1", encoding="utf-8")
        (mig / "001_b.sql").write_text("SELECT 1", encoding="utf-8")
        with pytest.raises(MigrationFileError, match="Duplicate"):
            MigrationManager(mig, manager).discover()

    def test_no_split_marker(self, tmp_path: Path, manager: ConnectionManager) -> None:
        mig = tmp_path / "migrations"
        mig.mkdir()
        (mig / "001_trigger.sql").write_text(
            "-- rowquery:no-split\nCREATE TABLE t (id INTEGER);\n",
            encoding="utf-8",
        )
        # A single statement with a trailing ';' still executes as one statement.
        MigrationManager(mig, manager).apply()


class TestStrictModeCoversAllParts:
    @dataclass
    class Address:
        city: str

    @dataclass
    class Customer:
        id: int
        name: str

    @dataclass
    class Order:
        id: int
        customer: object = None
        address: object = None

    def test_missing_reference_column(self) -> None:
        plan = (
            aggregate(self.Order, prefix="order__")
            .key("id")
            .field("id")
            .reference("customer", self.Customer, prefix="customer__")
            .strict()
            .build()
        )
        with pytest.raises(StrictModeViolation, match="customer__name"):
            AggregateMapper(plan).map_many([{"order__id": 1, "customer__id": 1}])

    def test_missing_value_object_column(self) -> None:
        plan = (
            aggregate(self.Order, prefix="order__")
            .key("id")
            .field("id")
            .value_object("address", self.Address, prefix="address__")
            .strict()
            .build()
        )
        with pytest.raises(StrictModeViolation, match="address__city"):
            AggregateMapper(plan).map_many([{"order__id": 1}])


class TestAdapterHelpers:
    def test_oracle_dsn_default_port(self) -> None:
        from row_query.adapters.oracle import _build_dsn

        config = ConnectionConfig(driver="oracle", host="db", database="XEPDB1")
        assert _build_dsn(config) == "db:1521/XEPDB1"

    def test_oracle_column_keys(self) -> None:
        from row_query.adapters.oracle import _column_key

        assert _column_key("USER_ID") == "user_id"
        assert _column_key("userName") == "userName"

    def test_postgres_conninfo_quotes_password(self) -> None:
        pytest.importorskip("psycopg")
        from psycopg.conninfo import conninfo_to_dict

        from row_query.adapters.postgresql import _build_conninfo

        config = ConnectionConfig(
            driver="postgresql",
            host="localhost",
            user="app",
            password="p a's\\s",
            database="db",
            extra={"sslmode": "disable"},
        )
        parsed = conninfo_to_dict(_build_conninfo(config))
        assert parsed["password"] == "p a's\\s"
        assert parsed["sslmode"] == "disable"
