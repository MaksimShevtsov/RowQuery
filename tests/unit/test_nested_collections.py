"""Tests for nested collections (collections inside collection members)."""

from __future__ import annotations

from dataclasses import dataclass, field

import pytest
from pydantic import BaseModel

from row_query.core.exceptions import PlanCompilationError, StrictModeViolation
from row_query.mapping.aggregate import AggregateMapper
from row_query.mapping.builder import aggregate


@dataclass
class Discount:
    id: int
    percent: int


@dataclass
class Line:
    id: int
    sku: str
    discounts: list[Discount] = field(default_factory=list)


@dataclass
class Order:
    id: int
    total: float
    lines: list[Line] = field(default_factory=list)


def order_plan(strict: bool = False):  # type: ignore[no-untyped-def]
    return (
        aggregate(Order, prefix="order__")
        .key("id")
        .auto_fields()
        .collection("lines", Line, prefix="line__", key="id")
        .collection("lines.discounts", Discount, prefix="disc__", key="id")
        .strict(strict)
        .build()
    )


def row(order: int, line: int | None, sku: str | None, disc: int | None, pct: int | None) -> dict:  # type: ignore[type-arg]
    return {
        "order__id": order,
        "order__total": 100.0,
        "line__id": line,
        "line__sku": sku,
        "disc__id": disc,
        "disc__percent": pct,
    }


class TestBuilder:
    def test_plan_tree(self) -> None:
        plan = order_plan()
        assert [c.attribute_name for c in plan.collection_plans] == ["lines"]
        lines = plan.collection_plans[0]
        assert [c.attribute_name for c in lines.children] == ["discounts"]
        # The nested collection attribute is not mapped from a column
        assert "discounts" not in lines.entity_plan.field_map
        assert lines.entity_plan.field_map == {"id": "id", "sku": "sku"}

    def test_three_levels(self) -> None:
        @dataclass
        class Note:
            id: int

        @dataclass
        class Disc:
            id: int
            notes: list[Note] = field(default_factory=list)

        @dataclass
        class Ln:
            id: int
            discounts: list[Disc] = field(default_factory=list)

        @dataclass
        class Ord:
            id: int
            lines: list[Ln] = field(default_factory=list)

        plan = (
            aggregate(Ord, prefix="o__")
            .key("id")
            .auto_fields()
            .collection("lines", Ln, prefix="l__", key="id")
            .collection("lines.discounts", Disc, prefix="d__", key="id")
            .collection("lines.discounts.notes", Note, prefix="n__", key="id")
            .build()
        )
        rows = [
            {"o__id": 1, "l__id": 1, "d__id": 1, "n__id": 1},
            {"o__id": 1, "l__id": 1, "d__id": 1, "n__id": 2},
            {"o__id": 1, "l__id": 1, "d__id": 2, "n__id": None},
        ]
        (order,) = AggregateMapper(plan).map_many(rows)
        (line,) = order.lines
        assert [d.id for d in line.discounts] == [1, 2]
        assert [n.id for n in line.discounts[0].notes] == [1, 2]
        assert line.discounts[1].notes == []

    def test_parent_must_be_declared_first(self) -> None:
        builder = (
            aggregate(Order, prefix="order__")
            .key("id")
            .collection("lines.discounts", Discount, prefix="disc__", key="id")
            .collection("lines", Line, prefix="line__", key="id")
        )
        with pytest.raises(PlanCompilationError, match="parent collection 'lines'"):
            builder.build()

    def test_duplicate_path_rejected(self) -> None:
        builder = (
            aggregate(Order, prefix="order__")
            .key("id")
            .collection("lines", Line, prefix="line__", key="id")
            .collection("lines", Line, prefix="line2__", key="id")
        )
        with pytest.raises(PlanCompilationError, match="more than once"):
            builder.build()

    def test_duplicate_prefix_across_levels_rejected(self) -> None:
        builder = (
            aggregate(Order, prefix="order__")
            .key("id")
            .collection("lines", Line, prefix="line__", key="id")
            .collection("lines.discounts", Discount, prefix="line__", key="id")
        )
        with pytest.raises(PlanCompilationError, match="Duplicate prefix"):
            builder.build()


class TestMapper:
    def test_reconstructs_and_deduplicates(self) -> None:
        rows = [
            row(1, 10, "A", 100, 5),
            row(1, 10, "A", 101, 10),
            row(1, 11, "B", None, None),
            row(2, 20, "C", 200, 15),
        ]
        orders = AggregateMapper(order_plan()).map_many(rows)

        assert [o.id for o in orders] == [1, 2]
        first, second = orders
        assert [ln.sku for ln in first.lines] == ["A", "B"]
        assert [d.percent for d in first.lines[0].discounts] == [5, 10]
        assert first.lines[1].discounts == []
        assert [d.id for d in second.lines[0].discounts] == [200]

    def test_children_are_scoped_to_their_parent(self) -> None:
        # The same discount id under two different lines yields one discount per line.
        rows = [row(1, 10, "A", 100, 5), row(1, 11, "B", 100, 5)]
        (order,) = AggregateMapper(order_plan()).map_many(rows)
        assert [len(ln.discounts) for ln in order.lines] == [1, 1]
        assert order.lines[0].discounts[0] is not order.lines[1].discounts[0]

    def test_rows_in_any_order(self) -> None:
        rows = [row(1, 10, "A", 100, 5), row(1, 11, "B", 110, 7), row(1, 10, "A", 101, 10)]
        (order,) = AggregateMapper(order_plan()).map_many(rows)
        assert [d.id for d in order.lines[0].discounts] == [100, 101]
        assert [d.id for d in order.lines[1].discounts] == [110]

    def test_pydantic_models(self) -> None:
        class PDiscount(BaseModel):
            id: int
            percent: int

        class PLine(BaseModel):
            id: int
            sku: str
            discounts: list[PDiscount] = []

        class POrder(BaseModel):
            id: int
            total: float
            lines: list[PLine] = []

        plan = (
            aggregate(POrder, prefix="order__")
            .key("id")
            .auto_fields()
            .collection("lines", PLine, prefix="line__", key="id")
            .collection("lines.discounts", PDiscount, prefix="disc__", key="id")
            .build()
        )
        (order,) = AggregateMapper(plan).map_many(
            [row(1, 10, "A", 100, 5), row(1, 10, "A", 101, 10)]
        )
        assert [d.id for d in order.lines[0].discounts] == [100, 101]

    def test_strict_checks_nested_columns(self) -> None:
        bad = row(1, 10, "A", 100, 5)
        del bad["disc__percent"]
        with pytest.raises(StrictModeViolation, match="disc__percent"):
            AggregateMapper(order_plan(strict=True)).map_many([bad])

    def test_strict_accepts_nested_prefix(self) -> None:
        orders = AggregateMapper(order_plan(strict=True)).map_many([row(1, 10, "A", 100, 5)])
        assert orders[0].lines[0].discounts[0].percent == 5


class TestEndToEnd:
    def test_three_table_join_through_engine(self, tmp_path) -> None:  # type: ignore[no-untyped-def]
        from row_query.core.connection import ConnectionConfig
        from row_query.core.engine import Engine
        from row_query.core.registry import SQLRegistry

        config = ConnectionConfig(driver="sqlite", database=":memory:", pool_size=1)
        engine = Engine.from_config(config, SQLRegistry(tmp_path))
        for stmt in (
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, total REAL)",
            "CREATE TABLE lines (id INTEGER PRIMARY KEY, order_id INTEGER, sku TEXT)",
            "CREATE TABLE discounts (id INTEGER PRIMARY KEY, line_id INTEGER, percent INTEGER)",
            "INSERT INTO orders VALUES (1, 100.0), (2, 50.0)",
            "INSERT INTO lines VALUES (10, 1, 'A'), (11, 1, 'B'), (20, 2, 'C')",
            "INSERT INTO discounts VALUES (100, 10, 5), (101, 10, 10), (200, 20, 15)",
        ):
            engine.execute(stmt)

        orders = engine.fetch_all(
            """
            SELECT o.id AS order__id, o.total AS order__total,
                   l.id AS line__id, l.sku AS line__sku,
                   d.id AS disc__id, d.percent AS disc__percent
            FROM orders o
            LEFT JOIN lines l ON l.order_id = o.id
            LEFT JOIN discounts d ON d.line_id = l.id
            ORDER BY o.id, l.id, d.id
            """,
            mapper=AggregateMapper(order_plan(strict=True)),
        )
        engine.close()

        assert [
            (o.id, [(ln.sku, [d.percent for d in ln.discounts]) for ln in o.lines]) for o in orders
        ] == [
            (1, [("A", [5, 10]), ("B", [])]),
            (2, [("C", [15])]),
        ]
