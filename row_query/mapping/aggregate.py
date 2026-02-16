"""Aggregate reconstruction mapper.

Single-pass O(n) reconstruction using identity maps.
"""

from __future__ import annotations

from typing import Any, Generic, TypeVar

from row_query.core.exceptions import StrictModeViolation
from row_query.mapping.plan import AggregatePlan

T = TypeVar("T")


def _extract_fields(row: dict[str, Any], prefix: str, field_map: dict[str, str]) -> dict[str, Any]:
    """Extract fields from a row using prefix and field map."""
    result: dict[str, Any] = {}
    for attr_name, col_name in field_map.items():
        full_col = prefix + col_name
        result[attr_name] = row.get(full_col)
    return result


class AggregateMapper(Generic[T]):
    """Aggregate reconstruction mapper.

    Reconstructs aggregate object graphs from joined SQL result sets.
    Uses identity-based deduplication in a single pass.
    """

    def __init__(self, plan: AggregatePlan) -> None:
        self._plan = plan

    def map_one(self, row: dict[str, Any]) -> T:
        """Not supported for aggregates."""
        raise NotImplementedError(
            "AggregateMapper.map_one is not supported. Use map_many for "
            "aggregate reconstruction from joined result sets."
        )

    def map_many(self, rows: list[dict[str, Any]]) -> list[T]:
        """Reconstruct aggregate object graphs from rows.

        Single-pass O(n) algorithm using identity maps.
        """
        if not rows:
            return []

        plan = self._plan

        # Strict mode validation
        if plan.strict and rows:
            self._validate_strict(rows[0])

        # Identity maps
        root_identity: dict[Any, Any] = {}
        root_order: list[Any] = []  # Preserve insertion order

        # Per-root collection identity maps
        # root_key -> collection_name -> set of child keys
        collection_ids: dict[Any, dict[str, set[Any]]] = {}

        for row in rows:
            # Extract root key
            root_key_col = plan.root_plan.prefix + plan.root_plan.key_field
            root_key = row.get(root_key_col)

            # Skip NULL root key
            if root_key is None:
                continue

            # Create or retrieve root instance
            if root_key not in root_identity:
                root_fields = _extract_fields(row, plan.root_plan.prefix, plan.root_plan.field_map)
                # Initialize collection attributes as empty lists
                for coll in plan.collection_plans:
                    root_fields[coll.attribute_name] = []
                # Initialize reference and value object attributes as None
                for ref in plan.reference_plans:
                    root_fields[ref.attribute_name] = None
                for vo in plan.value_object_plans:
                    root_fields[vo.attribute_name] = None

                root_instance = plan.root_plan.target_class(**root_fields)
                root_identity[root_key] = root_instance
                root_order.append(root_key)
                collection_ids[root_key] = {
                    coll.attribute_name: set() for coll in plan.collection_plans
                }

            root_instance = root_identity[root_key]

            # Process collections
            for coll in plan.collection_plans:
                child_key_col = coll.entity_plan.prefix + coll.entity_plan.key_field
                child_key = row.get(child_key_col)

                # Skip NULL child key
                if child_key is None:
                    continue

                # Deduplicate by identity
                if child_key not in collection_ids[root_key][coll.attribute_name]:
                    child_fields = _extract_fields(
                        row, coll.entity_plan.prefix, coll.entity_plan.field_map
                    )
                    child_instance = coll.entity_plan.target_class(**child_fields)
                    getattr(root_instance, coll.attribute_name).append(child_instance)
                    collection_ids[root_key][coll.attribute_name].add(child_key)

            # Process references (construct once per root)
            for ref in plan.reference_plans:
                if getattr(root_instance, ref.attribute_name) is None:
                    ref_key_col = ref.entity_plan.prefix + ref.entity_plan.key_field
                    ref_key = row.get(ref_key_col)
                    if ref_key is not None:
                        ref_fields = _extract_fields(
                            row, ref.entity_plan.prefix, ref.entity_plan.field_map
                        )
                        ref_instance = ref.entity_plan.target_class(**ref_fields)
                        object.__setattr__(root_instance, ref.attribute_name, ref_instance)

            # Process value objects (construct once per root)
            for vo in plan.value_object_plans:
                if getattr(root_instance, vo.attribute_name) is None:
                    vo_fields = _extract_fields(row, vo.prefix, vo.field_map)
                    # Only create if at least one field is non-None
                    if any(v is not None for v in vo_fields.values()):
                        vo_instance = vo.target_class(**vo_fields)
                        object.__setattr__(root_instance, vo.attribute_name, vo_instance)

        return [root_identity[key] for key in root_order]

    def _validate_strict(self, sample_row: dict[str, Any]) -> None:
        """Validate mapping against actual row columns in strict mode."""
        row_columns = set(sample_row.keys())
        plan = self._plan

        # Check root fields
        for attr_name, col_name in plan.root_plan.field_map.items():
            full_col = plan.root_plan.prefix + col_name
            if full_col not in row_columns:
                raise StrictModeViolation(
                    f"Missing mapped column '{full_col}' for root "
                    f"field '{attr_name}' in {plan.root_plan.target_class.__name__}"
                )

        # Check collection fields
        for coll in plan.collection_plans:
            for attr_name, col_name in coll.entity_plan.field_map.items():
                full_col = coll.entity_plan.prefix + col_name
                if full_col not in row_columns:
                    raise StrictModeViolation(
                        f"Missing mapped column '{full_col}' for collection "
                        f"'{coll.attribute_name}' field '{attr_name}'"
                    )

        # Check for unknown prefix groups
        known_prefixes = {plan.root_plan.prefix}
        for coll in plan.collection_plans:
            known_prefixes.add(coll.entity_plan.prefix)
        for ref in plan.reference_plans:
            known_prefixes.add(ref.entity_plan.prefix)
        for vo in plan.value_object_plans:
            known_prefixes.add(vo.prefix)

        for col in row_columns:
            if "__" in col:
                prefix = col[: col.index("__") + 2]
                if prefix not in known_prefixes:
                    raise StrictModeViolation(
                        f"Unknown prefix group '{prefix}' in column '{col}'. "
                        f"Known prefixes: {sorted(known_prefixes)}"
                    )
