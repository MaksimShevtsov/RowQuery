"""Aggregate mapping plan data classes.

Frozen dataclasses representing compiled, validated mapping plans.
Used by AggregateMapper at execution time.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class EntityPlan:
    """Mapping plan for a single entity (root, collection member, or reference)."""

    target_class: type
    prefix: str
    key_field: str
    field_map: dict[str, str]  # attribute_name -> column_name (without prefix)


@dataclass(frozen=True)
class CollectionPlan:
    """Mapping plan for a collection (one-to-many relationship)."""

    attribute_name: str
    entity_plan: EntityPlan
    children: list[CollectionPlan] = field(default_factory=list)


@dataclass(frozen=True)
class ReferencePlan:
    """Mapping plan for a single reference (many-to-one or one-to-one)."""

    attribute_name: str
    entity_plan: EntityPlan


@dataclass(frozen=True)
class ValueObjectPlan:
    """Mapping plan for a value object (no identity, part of aggregate)."""

    attribute_name: str
    target_class: type
    prefix: str
    field_map: dict[str, str]


@dataclass(frozen=True)
class AggregatePlan:
    """Compiled, validated aggregate mapping plan."""

    root_plan: EntityPlan
    collection_plans: list[CollectionPlan] = field(default_factory=list)
    reference_plans: list[ReferencePlan] = field(default_factory=list)
    value_object_plans: list[ValueObjectPlan] = field(default_factory=list)
    strict: bool = False
