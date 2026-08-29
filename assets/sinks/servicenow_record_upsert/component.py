"""DataFrame → ServiceNow record upsert.

Mirrors an upstream DataFrame into a ServiceNow table via the Table API. Since
ServiceNow has no native upsert endpoint, this sink does search-then-write per
row: `GET /api/now/table/{table}?sysparm_query={key_field}={value}` → PATCH the
match if found, else POST a new record.

Pairs with:
  - ``servicenow_resource`` — connection + auth (required)

Live-validation notes:
  - Rate limits: ServiceNow's default rate limit is 60 req/min for basic auth
    on developer instances. The resource retries on 429 with exponential
    backoff up to `max_retries` (default 3).
  - Uniqueness: if `key_field` is not indexed as unique on the target table,
    concurrent runs racing on the same key can create duplicates. Add a
    unique index in ServiceNow to enforce.
  - Empty strings vs None: string values that are literally "" are sent
    verbatim to ServiceNow (which stores them as blank). None / NaN values
    are omitted from the request body so ServiceNow keeps its current value.
"""
from typing import Dict, List, Optional

import dagster as dg
from pydantic import Field


class ServiceNowRecordUpsertComponent(dg.Component, dg.Model, dg.Resolvable):
    """Batch-upsert rows from an upstream DataFrame into a ServiceNow table.

    Example:
        ```yaml
        type: dagster_component_templates.ServiceNowRecordUpsertComponent
        attributes:
          asset_name: servicenow_incidents_from_alerts
          upstream_asset_key: alerts_to_incidents
          resource_key: servicenow_resource
          table: incident
          key_field: correlation_id
          fields_map:
            alert_id: correlation_id
            title: short_description
            details: description
            severity: impact
          batch_size: 500
          group_name: reverse_etl
        ```

    For every row in the upstream DataFrame:
      1. GET the ServiceNow table by `key_field == <row[key_field]>`
      2. If found, PATCH the matched sys_id with the mapped fields
      3. If not, POST a new record with the mapped fields
    """

    asset_name: str = Field(description="Output Dagster asset name.")
    upstream_asset_key: str = Field(description="Upstream asset providing the DataFrame.")
    resource_key: str = Field(
        default="servicenow_resource",
        description="Resource key registered by ServiceNowResourceComponent.",
    )

    table: str = Field(
        description="Target ServiceNow table name (e.g. 'incident', 'change_request', 'cmdb_ci_server').",
    )
    key_field: str = Field(
        description=(
            "ServiceNow field name used to match existing records. Every upstream row "
            "must supply a value for this field via `fields_map`."
        ),
    )
    fields_map: Dict[str, str] = Field(
        description="Upstream column → ServiceNow field name.",
    )
    batch_size: int = Field(
        default=500,
        description="Max upstream rows per run (safety cap). Each row is one search-then-write pair (2 REST calls when the record exists, 1 when new).",
    )

    group_name: Optional[str] = Field(
        default="servicenow", description="Dagster asset group name."
    )
    description: Optional[str] = Field(
        default=None, description="Asset description."
    )
    owners: Optional[List[str]] = Field(
        default=None, description="Asset owners."
    )
    tags: Optional[Dict[str, str]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds (auto-includes 'servicenow').",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("servicenow")

        # Validate key_field is in fields_map values — otherwise the upsert
        # can't match and every row will be created as new.
        mapped_fields = set(self.fields_map.values())
        if self.key_field not in mapped_fields:
            raise ValueError(
                f"ServiceNowRecordUpsertComponent: key_field={self.key_field!r} not "
                f"present in fields_map values. key_field must be a ServiceNow field "
                f"you're upserting. fields_map values: {sorted(mapped_fields)}"
            )

        @dg.asset(
            key=dg.AssetKey.from_user_string(_self.asset_name),
            ins={"upstream": dg.AssetIn(key=dg.AssetKey.from_user_string(_self.upstream_asset_key))},
            group_name=_self.group_name,
            kinds=kinds,
            owners=_self.owners,
            tags=_self.tags,
            required_resource_keys={_self.resource_key},
            description=_self.description or (
                f"Upsert DataFrame rows into ServiceNow table {_self.table} "
                f"(match on {_self.key_field})."
            ),
        )
        def _asset(context: dg.AssetExecutionContext, upstream):
            sn = getattr(context.resources, _self.resource_key)

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

            # The upstream column that maps to key_field on the ServiceNow side.
            key_col = next(
                (col for col, sn_field in _self.fields_map.items() if sn_field == _self.key_field),
                None,
            )
            if key_col is None:
                raise dg.Failure(
                    f"fields_map has no column mapping to key_field={_self.key_field!r}. "
                    f"fields_map: {_self.fields_map}"
                )

            def _row_value(v):
                if v is None or (isinstance(v, float) and pd.isna(v)):
                    return None
                return v

            # In-run cache: sys_id of records we created during THIS run so a
            # duplicate row (or eventual-consistency lag on ServiceNow's index)
            # doesn't double-post.
            just_created_sys_ids: Dict[str, str] = {}

            created = 0
            updated = 0
            errors: List[str] = []

            for i, row in df.iterrows():
                key_value = _row_value(row[key_col])
                if key_value is None:
                    errors.append(f"row {i}: {key_col!r} is None — skipped")
                    continue

                body: dict = {}
                for col, sn_field in _self.fields_map.items():
                    v = _row_value(row[col])
                    if v is not None:
                        body[sn_field] = v
                if not body:
                    continue

                # Try the in-run cache first — avoids a re-lookup and dodges
                # eventual consistency on ServiceNow's own index.
                cached_sys_id = just_created_sys_ids.get(str(key_value))
                try:
                    if cached_sys_id is not None:
                        sn.update_record(_self.table, cached_sys_id, body)
                        updated += 1
                        continue
                    result = sn.upsert_record(_self.table, _self.key_field, key_value, body)
                    if result.get("action") == "created":
                        created += 1
                        rec = result.get("record") or {}
                        if rec.get("sys_id"):
                            just_created_sys_ids[str(key_value)] = rec["sys_id"]
                    else:
                        updated += 1
                except Exception as e:  # noqa: BLE001
                    errors.append(f"row {i} (key={key_value}): {type(e).__name__}: {e}")

            context.log.info(
                f"ServiceNow upsert into {_self.table}: {created} created, "
                f"{updated} updated, {len(errors)} errors (matched on {_self.key_field})."
            )
            if errors:
                context.log.error(
                    "First few errors:\n" + "\n".join(errors[:5])
                )

            metadata = {
                "servicenow_table": dg.MetadataValue.text(_self.table),
                "key_field": dg.MetadataValue.text(_self.key_field),
                "rows_created": dg.MetadataValue.int(created),
                "rows_updated": dg.MetadataValue.int(updated),
                "rows_upserted": dg.MetadataValue.int(created + updated),
                "rows_errored": dg.MetadataValue.int(len(errors)),
            }
            if errors:
                metadata["first_errors"] = dg.MetadataValue.json(errors[:5])

            return dg.MaterializeResult(metadata=metadata)

        return dg.Definitions(assets=[_asset])
