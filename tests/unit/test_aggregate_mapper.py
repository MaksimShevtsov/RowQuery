"""Unit tests for AggregateMapper."""

from __future__ import annotations

from dataclasses import dataclass, field

import pytest

from row_query.core.exceptions import StrictModeViolation
from row_query.mapping.aggregate import AggregateMapper
from row_query.mapping.builder import aggregate


@dataclass
class User:
    id: int
    name: str
    email: str
    orders: list = field(default_factory=list)
    address: object | None = None


@dataclass
class Order:
    id: int
    total: float


@dataclass
class Address:
    street: str
    city: str


class TestAggregateMapper:
    def test_single_pass_reconstruction(self) -> None:
        plan = (
            aggregate(User, prefix="user__")
            .key("id")
            .auto_fields()
            .collection("orders", Order, prefix="orders__", key="id")
            .value_object("address", Address, prefix="address__")
            .build()
        )
        mapper = AggregateMapper(plan)

        rows = [
            {
                "user__id": 1,
                "user__name": "Alice",
                "user__email": "alice@ex.com",
                "orders__id": 10,
                "orders__total": 99.99,
                "address__street": "123 Main St",
                "address__city": "Springfield",
            },
            {
                "user__id": 1,
                "user__name": "Alice",
                "user__email": "alice@ex.com",
                "orders__id": 11,
                "orders__total": 49.99,
                "address__street": "123 Main St",
                "address__city": "Springfield",
            },
        ]

        results = mapper.map_many(rows)
        assert len(results) == 1

        user = results[0]
        assert user.id == 1
        assert user.name == "Alice"
        assert len(user.orders) == 2
        assert user.orders[0].id == 10
        assert user.orders[1].id == 11
        assert user.address is not None
        assert user.address.street == "123 Main St"

    def test_null_root_key_skipped(self) -> None:
        plan = aggregate(User, prefix="user__").key("id").auto_fields().build()
        mapper = AggregateMapper(plan)

        rows = [
            {"user__id": None, "user__name": "Ghost", "user__email": "g@ex.com"},
            {"user__id": 1, "user__name": "Alice", "user__email": "a@ex.com"},
        ]

        results = mapper.map_many(rows)
        assert len(results) == 1
        assert results[0].id == 1

    def test_null_child_key_skipped(self) -> None:
        plan = (
            aggregate(User, prefix="user__")
            .key("id")
            .auto_fields()
            .collection("orders", Order, prefix="orders__", key="id")
            .build()
        )
        mapper = AggregateMapper(plan)

        rows = [
            {
                "user__id": 1,
                "user__name": "Alice",
                "user__email": "a@ex.com",
                "orders__id": None,
                "orders__total": None,
            },
        ]

        results = mapper.map_many(rows)
        assert len(results) == 1
        assert len(results[0].orders) == 0

    def test_identity_deduplication(self) -> None:
        plan = (
            aggregate(User, prefix="user__")
            .key("id")
            .auto_fields()
            .collection("orders", Order, prefix="orders__", key="id")
            .build()
        )
        mapper = AggregateMapper(plan)

        # Duplicate rows from JOIN
        rows = [
            {
                "user__id": 1,
                "user__name": "Alice",
                "user__email": "a@ex.com",
                "orders__id": 10,
                "orders__total": 99.99,
            },
            {
                "user__id": 1,
                "user__name": "Alice",
                "user__email": "a@ex.com",
                "orders__id": 10,
                "orders__total": 99.99,
            },
        ]

        results = mapper.map_many(rows)
        assert len(results) == 1
        assert len(results[0].orders) == 1  # Deduplicated

    def test_reference_construction(self) -> None:
        @dataclass
        class Post:
            id: int
            title: str
            author: object | None = None

        @dataclass
        class Author:
            id: int
            name: str

        plan = (
            aggregate(Post, prefix="post__")
            .key("id")
            .auto_fields()
            .reference("author", Author, prefix="author__", key="id")
            .build()
        )
        mapper = AggregateMapper(plan)

        rows = [
            {
                "post__id": 1,
                "post__title": "Hello",
                "author__id": 42,
                "author__name": "Alice",
            },
        ]

        results = mapper.map_many(rows)
        assert len(results) == 1
        assert results[0].author is not None
        assert results[0].author.name == "Alice"

    def test_map_one_raises_not_implemented(self) -> None:
        plan = aggregate(User, prefix="user__").key("id").auto_fields().build()
        mapper = AggregateMapper(plan)

        with pytest.raises(NotImplementedError):
            mapper.map_one({"user__id": 1, "user__name": "Alice", "user__email": "a@ex.com"})

    def test_empty_rows(self) -> None:
        plan = aggregate(User, prefix="user__").key("id").auto_fields().build()
        mapper = AggregateMapper(plan)
        results = mapper.map_many([])
        assert results == []

    def test_multiple_roots(self) -> None:
        plan = aggregate(User, prefix="user__").key("id").auto_fields().build()
        mapper = AggregateMapper(plan)

        rows = [
            {"user__id": 1, "user__name": "Alice", "user__email": "a@ex.com"},
            {"user__id": 2, "user__name": "Bob", "user__email": "b@ex.com"},
        ]

        results = mapper.map_many(rows)
        assert len(results) == 2
        assert results[0].name == "Alice"
        assert results[1].name == "Bob"


class TestStrictMode:
    def test_strict_missing_mapped_column(self) -> None:
        @dataclass
        class SimpleUser:
            id: int
            name: str
            email: str

        plan = aggregate(SimpleUser, prefix="user__").key("id").auto_fields().strict().build()
        mapper = AggregateMapper(plan)

        # Missing user__email column
        rows = [{"user__id": 1, "user__name": "Alice"}]

        with pytest.raises(StrictModeViolation, match="Missing mapped column"):
            mapper.map_many(rows)

    def test_strict_unknown_prefix_group(self) -> None:
        @dataclass
        class SimpleUser:
            id: int
            name: str
            email: str

        plan = aggregate(SimpleUser, prefix="user__").key("id").auto_fields().strict().build()
        mapper = AggregateMapper(plan)

        # foo__ prefix not in any plan
        rows = [
            {
                "user__id": 1,
                "user__name": "Alice",
                "user__email": "a@ex.com",
                "foo__bar": "unknown",
            }
        ]

        with pytest.raises(StrictModeViolation, match="Unknown prefix group"):
            mapper.map_many(rows)

    def test_non_strict_ignores_missing_columns(self) -> None:
        plan = aggregate(User, prefix="user__").key("id").auto_fields().build()
        mapper = AggregateMapper(plan)

        # Missing user__email - non-strict should not raise
        rows = [{"user__id": 1, "user__name": "Alice"}]
        results = mapper.map_many(rows)
        assert len(results) == 1
        assert results[0].email is None  # Gracefully set to None

    def test_non_strict_ignores_unknown_prefix(self) -> None:
        plan = aggregate(User, prefix="user__").key("id").auto_fields().build()
        mapper = AggregateMapper(plan)

        rows = [
            {
                "user__id": 1,
                "user__name": "Alice",
                "user__email": "a@ex.com",
                "foo__bar": "unknown",
            }
        ]
        # Non-strict should not raise
        results = mapper.map_many(rows)
        assert len(results) == 1


class TestBenchmark10kRows:
    """Benchmark: 10k-row aggregate reconstruction per SC-004."""

    def test_10k_row_single_pass(self) -> None:
        """Generate 10,000 joined rows and verify single-pass reconstruction."""

        @dataclass
        class Customer:
            id: int
            name: str
            email: str
            items: list = field(default_factory=list)

        @dataclass
        class Item:
            id: int
            product: str
            price: float

        plan = (
            aggregate(Customer, prefix="customer__")
            .key("id")
            .auto_fields()
            .collection("items", Item, prefix="item__", key="id")
            .build()
        )
        mapper = AggregateMapper(plan)

        # Generate 10,000 rows: 1,000 customers x 10 items each
        rows: list[dict] = []
        for cust_id in range(1, 1001):
            for item_offset in range(10):
                item_id = cust_id * 100 + item_offset
                rows.append(
                    {
                        "customer__id": cust_id,
                        "customer__name": f"Customer {cust_id}",
                        "customer__email": f"c{cust_id}@test.com",
                        "item__id": item_id,
                        "item__product": f"Product {item_id}",
                        "item__price": float(item_id) * 1.5,
                    }
                )

        assert len(rows) == 10_000

        # Single-pass reconstruction
        results = mapper.map_many(rows)

        # Verify correctness
        assert len(results) == 1_000
        assert all(len(c.items) == 10 for c in results)
        assert results[0].name == "Customer 1"
        assert results[999].name == "Customer 1000"
        assert results[0].items[0].product == "Product 100"
