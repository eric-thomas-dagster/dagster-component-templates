"""DataFrame → Airtable Records upsert.

Mirrors an upstream DataFrame into an Airtable table using Airtable's
native server-side upsert endpoint —
`PATCH /v0/{baseId}/{tableName}?performUpsert[fieldsToMergeOn][]=<field>`.
Airtable handles create-or-update by matching `key_fields`, so the sink
is idempotent by design.

Pairs with:
  - ``airtable_resource`` — connection (required)
"""
from typing import Dict, List, Optional

import dagster as dg
from pydantic import Field


class AirtableRecordUpsertComponent(dg.Component, dg.Model, dg.Resolvable):
    """Batch-upsert rows from an upstream DataFrame into an Airtable table.

    Example:
        ```yaml
        type: dagster_community_components.AirtableRecordUpsertComponent
        attributes:
          asset_name: airtable_tasks_mirror
          upstream_asset_key: tasks_seed
          resource_key: airtable
          base_id: appXXXXXXXXX
          table: "Table 1"
          key_fields: [Name]
          fields_map:
            name: Name
            description: Notes
            state: Status
          typecast: true
        ```
    """

    asset_name: str = Field(description="Output Dagster asset name.")
    upstream_asset_key: str = Field(description="Upstream asset providing the DataFrame.")
    resource_key: str = Field(
        default="airtable",
        description="Resource key registered by AirtableResourceComponent.",
    )

    base_id: str = Field(description="Airtable base ID (starts with `app`).")
    table: str = Field(description="Target table name or ID (starts with `tbl`).")

    key_fields: List[str] = Field(
        description=(
            "Airtable field name(s) to match on. Airtable's server-side upsert "
            "matches rows where ALL listed fields equal the incoming row's values. "
            "Usually one field like `[\"Name\"]`."
        ),
    )
    fields_map: Dict[str, str] = Field(
        description="Upstream column → Airtable field name.",
    )
    typecast: bool = Field(
        default=True,
        description=(
            "If true, Airtable coerces string values into typed fields "
            "(e.g. accept '2026-08-15' for a date, 'Todo' for a singleSelect). "
            "Default true — matches the typical DataFrame-of-strings case."
        ),
    )
    batch_size: int = Field(
        default=1000,
        description="Max upstream rows per run (safety cap). Airtable batches at 10 per API call — the resource chunks automatically.",
    )

    group_name: Optional[str] = Field(default="airtable", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds (auto-includes 'airtable').")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("airtable")

        @dg.asset(
            key=dg.AssetKey.from_user_string(_self.asset_name),
            ins={"upstream": dg.AssetIn(key=dg.AssetKey.from_user_string(_self.upstream_asset_key))},
            group_name=_self.group_name,
            kinds=kinds,
            owners=_self.owners,
            tags=_self.tags,
            required_resource_keys={_self.resource_key},
            description=_self.description or (
                f"Upsert DataFrame rows into Airtable {_self.base_id}/{_self.table}."
            ),
        )
        def _asset(context: dg.AssetExecutionContext, upstream):
            at = getattr(context.resources, _self.resource_key)

            import pandas as pd
            if not isinstance(upstream, pd.DataFrame):
                df = pd.DataFrame([upstream]) if isinstance(upstream, dict) else pd.DataFrame(upstream)
            else:
                df = upstream

            if len(df) == 0:
                context.log.warning("Upstream DataFrame is empty — nothing to upsert.")
                return dg.MaterializeResult(metadata={"rows_upserted": dg.MetadataValue.int(0)})

            if len(df) > _self.batch_size:
                context.log.warning(
                    f"Upstream has {len(df)} rows; capped at batch_size={_self.batch_size}."
                )
                df = df.head(_self.batch_size)

            required_cols = set(_self.fields_map.keys())
            missing_cols = [c for c in required_cols if c not in df.columns]
            if missing_cols:
                raise dg.Failure(
                    f"Columns not in upstream: {missing_cols}. Available: {list(df.columns)}"
                )

            def _row_value(v):
                if v is None or (isinstance(v, float) and pd.isna(v)):
                    return None
                return v

            rows: List[dict] = []
            for _, row in df.iterrows():
                fields: dict = {}
                for col, airtable_field in _self.fields_map.items():
                    v = _row_value(row[col])
                    if v is not None:
                        fields[airtable_field] = v
                if not fields:
                    continue
                rows.append({"fields": fields})

            # Verify key_fields are actually being sent — otherwise the upsert
            # can't match and every row is treated as new.
            mapped_airtable_fields = set(_self.fields_map.values())
            missing_keys = [k for k in _self.key_fields if k not in mapped_airtable_fields]
            if missing_keys:
                raise dg.Failure(
                    f"key_fields {missing_keys} not present in fields_map values. "
                    f"key_fields must be columns you're upserting. "
                    f"fields_map values: {sorted(mapped_airtable_fields)}"
                )

            result = at.upsert_records(
                _self.base_id, _self.table,
                rows=rows,
                key_fields=_self.key_fields,
                typecast=_self.typecast,
            )
            created = len(result.get("createdRecords", []))
            updated = len(result.get("updatedRecords", []))
            context.log.info(
                f"Airtable upsert complete: {created} created, {updated} updated "
                f"(matched on {_self.key_fields})."
            )
            return dg.MaterializeResult(
                metadata={
                    "airtable_base_id": dg.MetadataValue.text(_self.base_id),
                    "airtable_table": dg.MetadataValue.text(_self.table),
                    "rows_created": dg.MetadataValue.int(created),
                    "rows_updated": dg.MetadataValue.int(updated),
                    "rows_upserted": dg.MetadataValue.int(created + updated),
                }
            )

        return dg.Definitions(assets=[_asset])
