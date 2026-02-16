"""Mapping layer - transform row dicts into typed objects."""

from __future__ import annotations

from row_query.mapping.aggregate import AggregateMapper
from row_query.mapping.builder import AggregateMappingBuilder, aggregate
from row_query.mapping.model import ModelMapper
from row_query.mapping.plan import (
    AggregatePlan,
    CollectionPlan,
    EntityPlan,
    ReferencePlan,
    ValueObjectPlan,
)

__all__ = [
    "ModelMapper",
    "AggregateMapper",
    "AggregateMappingBuilder",
    "aggregate",
    "AggregatePlan",
    "EntityPlan",
    "CollectionPlan",
    "ReferencePlan",
    "ValueObjectPlan",
]
