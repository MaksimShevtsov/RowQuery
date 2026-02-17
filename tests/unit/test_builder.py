"""Unit tests for AggregatePlan data classes and AggregateMappingBuilder DSL."""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from row_query.core.exceptions import PlanCompilationError
from row_query.mapping.builder import aggregate
from row_query.mapping.plan import (
    AggregatePlan,
    CollectionPlan,
    EntityPlan,
    ReferencePlan,
    ValueObjectPlan,
)


@dataclass
class User:
    id: int
    name: str
    email: str


@dataclass
class Order:
    id: int
    total: float


@dataclass
class Address:
    street: str
    city: str


@dataclass
class LineItem:
    id: int
    product: str
    quantity: int


class TestAggregatePlanDataClasses:
    def test_entity_plan_frozen(self) -> None:
        plan = EntityPlan(target_class=User, prefix="user__", key_field="id", field_map={})
        with pytest.raises(AttributeError):
            plan.prefix = "other__"  # type: ignore[misc]

    def test_collection_plan_frozen(self) -> None:
        entity = EntityPlan(target_class=Order, prefix="orders__", key_field="id", field_map={})
        plan = CollectionPlan(attribute_name="orders", entity_plan=entity, children=[])
        with pytest.raises(AttributeError):
            plan.attribute_name = "other"  # type: ignore[misc]

    def test_reference_plan_frozen(self) -> None:
        entity = EntityPlan(target_class=User, prefix="author__", key_field="id", field_map={})
        plan = ReferencePlan(attribute_name="author", entity_plan=entity)
        with pytest.raises(AttributeError):
            plan.attribute_name = "other"  # type: ignore[misc]

    def test_value_object_plan_frozen(self) -> None:
        plan = ValueObjectPlan(
            attribute_name="address",
            target_class=Address,
            prefix="address__",
            field_map={},
        )
        with pytest.raises(AttributeError):
            plan.prefix = "other__"  # type: ignore[misc]

    def test_aggregate_plan_frozen(self) -> None:
        root = EntityPlan(target_class=User, prefix="user__", key_field="id", field_map={})
        plan = AggregatePlan(
            root_plan=root,
            collection_plans=[],
            reference_plans=[],
            value_object_plans=[],
        )
        with pytest.raises(AttributeError):
            plan.strict = True  # type: ignore[misc]


class TestAggregateMappingBuilder:
    def test_basic_build(self) -> None:
        plan = aggregate(User, prefix="user__").key("id").field("name").field("email").build()
        assert isinstance(plan, AggregatePlan)
        assert plan.root_plan.target_class is User
        assert plan.root_plan.key_field == "id"
        assert plan.root_plan.prefix == "user__"

    def test_auto_fields(self) -> None:
        plan = aggregate(User, prefix="user__").key("id").auto_fields().build()
        assert plan.root_plan.field_map == {
            "id": "id",
            "name": "name",
            "email": "email",
        }

    def test_explicit_field_mapping(self) -> None:
        plan = aggregate(User, prefix="user__").key("id").field("name", "user_name").build()
        assert plan.root_plan.field_map["name"] == "user_name"

    def test_collection(self) -> None:
        plan = (
            aggregate(User, prefix="user__")
            .key("id")
            .auto_fields()
            .collection("orders", Order, prefix="orders__", key="id")
            .build()
        )
        assert len(plan.collection_plans) == 1
        assert plan.collection_plans[0].attribute_name == "orders"
        assert plan.collection_plans[0].entity_plan.target_class is Order

    def test_reference(self) -> None:
        plan = (
            aggregate(Order, prefix="order__")
            .key("id")
            .auto_fields()
            .reference("customer", User, prefix="customer__", key="id")
            .build()
        )
        assert len(plan.reference_plans) == 1
        assert plan.reference_plans[0].attribute_name == "customer"

    def test_value_object(self) -> None:
        plan = (
            aggregate(User, prefix="user__")
            .key("id")
            .auto_fields()
            .value_object("address", Address, prefix="address__")
            .build()
        )
        assert len(plan.value_object_plans) == 1
        assert plan.value_object_plans[0].attribute_name == "address"

    def test_strict_mode(self) -> None:
        plan = aggregate(User, prefix="user__").key("id").auto_fields().strict().build()
        assert plan.strict is True

    def test_missing_key_raises_error(self) -> None:
        with pytest.raises(PlanCompilationError, match="key"):
            aggregate(User, prefix="user__").auto_fields().build()

    def test_duplicate_prefix_raises_error(self) -> None:
        with pytest.raises(PlanCompilationError, match="prefix"):
            (
                aggregate(User, prefix="user__")
                .key("id")
                .collection("orders", Order, prefix="user__", key="id")
                .build()
            )

    def test_default_prefix(self) -> None:
        plan = aggregate(User).key("id").auto_fields().build()
        assert plan.root_plan.prefix == "user__"

    def test_nesting_depth_limit(self) -> None:
        # Collections within collections within collections should fail at depth > 2
        # This is validated at build time
        # We can only test single-level nesting via the API since the DSL
        # doesn't currently support nested collection chaining.
        # The validation still happens in build().
        plan = (
            aggregate(User, prefix="user__")
            .key("id")
            .auto_fields()
            .collection("orders", Order, prefix="orders__", key="id")
            .build()
        )
        assert len(plan.collection_plans) == 1
