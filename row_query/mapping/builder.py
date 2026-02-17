"""Aggregate mapping DSL builder.

Provides a fluent builder for defining aggregate mapping plans.
"""

from __future__ import annotations

import dataclasses
import inspect

from row_query.core.exceptions import PlanCompilationError
from row_query.mapping.plan import (
    AggregatePlan,
    CollectionPlan,
    EntityPlan,
    ReferencePlan,
    ValueObjectPlan,
)


def _get_field_names(cls: type) -> list[str]:
    """Extract field names from a class (dataclass, Pydantic, or plain)."""
    # Pydantic model
    if hasattr(cls, "model_fields"):
        return list(cls.model_fields.keys())

    # Dataclass
    if dataclasses.is_dataclass(cls):
        return [f.name for f in dataclasses.fields(cls)]

    # Plain class - use __init__ parameters
    try:
        sig = inspect.signature(cls.__init__)  # type: ignore[misc]
        return [
            name
            for name, param in sig.parameters.items()
            if name != "self" and param.default is inspect.Parameter.empty
        ]
    except (ValueError, TypeError):
        return []


def aggregate(root_class: type, prefix: str | None = None) -> AggregateMappingBuilder:
    """Entry point for the aggregate mapping DSL.

    Args:
        root_class: The aggregate root class.
        prefix: Column prefix for root fields. Defaults to
                lowercase class name + "__".

    Returns:
        A builder for chaining mapping declarations.
    """
    if prefix is None:
        prefix = root_class.__name__.lower() + "__"
    return AggregateMappingBuilder(root_class, prefix)


class AggregateMappingBuilder:
    """Fluent builder for aggregate mapping definitions."""

    def __init__(self, root_class: type, prefix: str) -> None:
        self._root_class = root_class
        self._prefix = prefix
        self._key_field: str | None = None
        self._field_map: dict[str, str] = {}
        self._auto_fields_enabled = False
        self._collections: list[tuple[str, type, str, str]] = []  # name, cls, prefix, key
        self._references: list[tuple[str, type, str, str]] = []  # name, cls, prefix, key
        self._value_objects: list[tuple[str, type, str]] = []  # name, cls, prefix
        self._strict_mode = False

    def key(self, field_name: str) -> AggregateMappingBuilder:
        """Set the identity key field for the root entity."""
        self._key_field = field_name
        return self

    def auto_fields(self) -> AggregateMappingBuilder:
        """Auto-map all fields of the root class by attribute name."""
        self._auto_fields_enabled = True
        return self

    def field(self, attr_name: str, column_name: str | None = None) -> AggregateMappingBuilder:
        """Explicitly map a single field."""
        self._field_map[attr_name] = column_name or attr_name
        return self

    def collection(
        self,
        name: str,
        entity_class: type,
        prefix: str,
        key: str,
    ) -> AggregateMappingBuilder:
        """Declare a child collection (one-to-many)."""
        self._collections.append((name, entity_class, prefix, key))
        return self

    def reference(
        self,
        name: str,
        entity_class: type,
        prefix: str,
        key: str | None = None,
    ) -> AggregateMappingBuilder:
        """Declare a single-entity reference."""
        self._references.append((name, entity_class, prefix, key or "id"))
        return self

    def value_object(
        self,
        name: str,
        vo_class: type,
        prefix: str,
    ) -> AggregateMappingBuilder:
        """Declare a value object (no identity)."""
        self._value_objects.append((name, vo_class, prefix))
        return self

    def strict(self, enabled: bool = True) -> AggregateMappingBuilder:
        """Enable or disable strict mode for this mapping."""
        self._strict_mode = enabled
        return self

    def build(self) -> AggregatePlan:
        """Compile and validate the mapping into an AggregatePlan."""
        # Validate key field
        if self._key_field is None:
            raise PlanCompilationError("Root entity must have a key field set via .key()")

        # Build root field map
        # Collect names that are populated by collections/references/value_objects
        composite_names = set()
        for name, *_ in self._collections:
            composite_names.add(name)
        for name, *_ in self._references:
            composite_names.add(name)
        for name, *_ in self._value_objects:
            composite_names.add(name)

        field_map = dict(self._field_map)
        if self._auto_fields_enabled:
            for name in _get_field_names(self._root_class):
                if name not in field_map and name not in composite_names:
                    field_map[name] = name

        root_plan = EntityPlan(
            target_class=self._root_class,
            prefix=self._prefix,
            key_field=self._key_field,
            field_map=field_map,
        )

        # Validate prefixes are unique
        all_prefixes = [self._prefix]

        # Build collection plans
        collection_plans = []
        for name, cls, prefix, key in self._collections:
            if prefix in all_prefixes:
                raise PlanCompilationError(
                    f"Duplicate prefix '{prefix}': each entity must have a unique prefix"
                )
            all_prefixes.append(prefix)

            child_field_map = {n: n for n in _get_field_names(cls)}
            entity_plan = EntityPlan(
                target_class=cls,
                prefix=prefix,
                key_field=key,
                field_map=child_field_map,
            )
            collection_plans.append(
                CollectionPlan(
                    attribute_name=name,
                    entity_plan=entity_plan,
                    children=[],
                )
            )

        # Build reference plans
        reference_plans = []
        for name, cls, prefix, key in self._references:
            if prefix in all_prefixes:
                raise PlanCompilationError(
                    f"Duplicate prefix '{prefix}': each entity must have a unique prefix"
                )
            all_prefixes.append(prefix)

            ref_field_map = {n: n for n in _get_field_names(cls)}
            entity_plan = EntityPlan(
                target_class=cls,
                prefix=prefix,
                key_field=key,
                field_map=ref_field_map,
            )
            reference_plans.append(ReferencePlan(attribute_name=name, entity_plan=entity_plan))

        # Build value object plans
        value_object_plans = []
        for name, cls, prefix in self._value_objects:
            if prefix in all_prefixes:
                raise PlanCompilationError(
                    f"Duplicate prefix '{prefix}': each entity must have a unique prefix"
                )
            all_prefixes.append(prefix)

            vo_field_map = {n: n for n in _get_field_names(cls)}
            value_object_plans.append(
                ValueObjectPlan(
                    attribute_name=name,
                    target_class=cls,
                    prefix=prefix,
                    field_map=vo_field_map,
                )
            )

        return AggregatePlan(
            root_plan=root_plan,
            collection_plans=collection_plans,
            reference_plans=reference_plans,
            value_object_plans=value_object_plans,
            strict=self._strict_mode,
        )
