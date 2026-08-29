"""DataFrame → Salesforce SObject upsert (native External-ID).

Mirrors an upstream DataFrame into a Salesforce SObject using Salesforce's
native External-ID upsert endpoint —
`PATCH /services/data/vXX.0/sobjects/{Object}/{ExtIdField}/{ExtIdValue}`.

Salesforce handles create-or-update atomically in a single request: if a
record with the matching external Id exists it's updated, otherwise a new
one is created. No search-then-write, no race conditions, no eventual
consistency window.

Requires the target External-ID field on the SObject to be marked as
`External ID` in the Salesforce schema (Setup → Object Manager → Fields).

Composite mode (`use_composite: true`, default) batches records at up to
200 per request via `/composite/sobjects/{Object}/{ExtIdField}` — the
fastest write path Salesforce offers short of the Bulk 2.0 API.

Pairs with:
  - ``salesforce_resource`` — connection + auth + workhorse HTTP (required)
"""
from typing import Dict, List, Optional

import dagster as dg
from pydantic import Field


class SalesforceRecordUpsertComponent(dg.Component, dg.Model, dg.Resolvable):
    """Batch-upsert rows from an upstream DataFrame into a Salesforce SObject.

    Example:
        ```yaml
        type: dagster_component_templates.SalesforceRecordUpsertComponent
        attributes:
          asset_name: salesforce_accounts_mirror
          upstream_asset_key: dbt_marts_accounts
          resource_key: salesforce
          sobject: Account
          external_id_field: External_Account_Id__c
          fields_map:
            account_id: External_Account_Id__c
            name: Name
            industry: Industry
            annual_revenue: AnnualRevenue
          batch_size: 5000
          use_composite: true          # 200-record batches (default true)
          composite_all_or_none: false # partial-success semantics
        ```

    For every upstream row:
      - `use_composite: true` (default) — batches 200 records per call to
        `PATCH /composite/sobjects/{Object}/{ExternalIdField}` with atomic
        create-or-update semantics per record.
      - `use_composite: false` — one HTTP call per row via
        `PATCH /sobjects/{Object}/{ExternalIdField}/{value}` (slower, but
        needed when payloads are large or you want per-row error isolation).
    """

    asset_name: str = Field(description="Output Dagster asset name.")
    upstream_asset_key: str = Field(description="Upstream asset providing the DataFrame.")
    resource_key: str = Field(
        default="salesforce",
        description="Resource key registered by SalesforceResourceComponent.",
    )

    sobject: str = Field(
        description=(
            "Target Salesforce SObject API name (e.g. 'Account', 'Contact', "
            "'Opportunity', 'CustomThing__c')."
        ),
    )
    external_id_field: str = Field(
        description=(
            "SObject field marked as 'External ID' — used as the upsert match "
            "key. MUST be present in fields_map values. Custom External Ids "
            "end in `__c`."
        ),
    )
    fields_map: Dict[str, str] = Field(
        description="Upstream column → Salesforce field API name.",
    )
    batch_size: int = Field(
        default=5000,
        description=(
            "Max upstream rows per run (safety cap). Composite mode chunks at "
            "200 per HTTP call automatically."
        ),
    )
    use_composite: bool = Field(
        default=True,
        description=(
            "Use `/composite/sobjects/{Object}/{ExtIdField}` for 200-record "
            "batching. Set false for per-row PATCHes (slower, but per-row "
            "error isolation)."
        ),
    )
    composite_all_or_none: bool = Field(
        default=False,
        description=(
            "Only used when `use_composite: true`. If true, Salesforce rolls "
            "back the entire 200-record batch on any error. Default false "
            "(partial success — each row succeeds or fails independently)."
        ),
    )

    group_name: Optional[str] = Field(
        default="salesforce", description="Dagster asset group name."
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
        description="Asset kinds (auto-includes 'salesforce').",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("salesforce")

        # Validate external_id_field is in fields_map values.
        mapped_fields = set(self.fields_map.values())
        if self.external_id_field not in mapped_fields:
            raise ValueError(
                f"SalesforceRecordUpsertComponent: external_id_field="
                f"{self.external_id_field!r} not in fields_map values. "
                f"external_id_field must be a Salesforce field you're "
                f"upserting. fields_map values: {sorted(mapped_fields)}"
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
                f"Upsert DataFrame rows into Salesforce {_self.sobject} "
                f"(match on external Id {_self.external_id_field})."
            ),
        )
        def _asset(context: dg.AssetExecutionContext, upstream):
            sf = getattr(context.resources, _self.resource_key)

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

            # Upstream column mapped to external_id_field.
            ext_id_col = next(
                (col for col, sf_field in _self.fields_map.items()
                 if sf_field == _self.external_id_field),
                None,
            )
            if ext_id_col is None:
                raise dg.Failure(
                    f"fields_map has no column mapping to external_id_field="
                    f"{_self.external_id_field!r}. fields_map: {_self.fields_map}"
                )

            def _row_value(v):
                if v is None or (isinstance(v, float) and pd.isna(v)):
                    return None
                return v

            # Build the records list.
            records: List[dict] = []
            skipped_no_key = 0
            for _, row in df.iterrows():
                ext_id_value = _row_value(row[ext_id_col])
                if ext_id_value is None:
                    skipped_no_key += 1
                    continue
                rec: dict = {}
                for col, sf_field in _self.fields_map.items():
                    v = _row_value(row[col])
                    if v is not None:
                        rec[sf_field] = v
                if not rec:
                    continue
                records.append(rec)

            if not records:
                context.log.warning("No records with valid external Id values — nothing to upsert.")
                return dg.MaterializeResult(metadata={"rows_upserted": dg.MetadataValue.int(0)})

            created = 0
            updated = 0
            errors: List[str] = []

            if _self.use_composite:
                # Chunk at 200 per Salesforce composite limits.
                for chunk_start in range(0, len(records), 200):
                    chunk = records[chunk_start:chunk_start + 200]
                    try:
                        result = sf.composite_upsert(
                            _self.sobject,
                            _self.external_id_field,
                            chunk,
                            all_or_none=_self.composite_all_or_none,
                        )
                    except Exception as e:  # noqa: BLE001
                        errors.append(
                            f"composite chunk {chunk_start}-{chunk_start + len(chunk) - 1}: "
                            f"{type(e).__name__}: {e}"
                        )
                        continue
                    # Composite response: [{'id', 'success', 'created', 'errors'}].
                    for i, r in enumerate(result):
                        if not r.get("success"):
                            errors.append(
                                f"row {chunk_start + i}: "
                                f"{r.get('errors') or 'unknown error'}"
                            )
                            continue
                        if r.get("created"):
                            created += 1
                        else:
                            updated += 1
            else:
                for i, rec in enumerate(records):
                    ext_id_value = rec.get(_self.external_id_field)
                    body = {k: v for k, v in rec.items() if k != _self.external_id_field}
                    try:
                        result = sf.upsert_record(
                            _self.sobject,
                            _self.external_id_field,
                            ext_id_value,
                            body,
                        )
                        if result.get("action") == "created":
                            created += 1
                        else:
                            updated += 1
                    except Exception as e:  # noqa: BLE001
                        errors.append(
                            f"row {i} ({_self.external_id_field}={ext_id_value}): "
                            f"{type(e).__name__}: {e}"
                        )

            context.log.info(
                f"Salesforce upsert into {_self.sobject}: {created} created, "
                f"{updated} updated, {len(errors)} errors, {skipped_no_key} skipped "
                f"(missing external Id) — matched on {_self.external_id_field}."
            )
            if errors:
                context.log.error(
                    "First few errors:\n" + "\n".join(errors[:5])
                )

            metadata = {
                "salesforce_sobject": dg.MetadataValue.text(_self.sobject),
                "external_id_field": dg.MetadataValue.text(_self.external_id_field),
                "rows_created": dg.MetadataValue.int(created),
                "rows_updated": dg.MetadataValue.int(updated),
                "rows_upserted": dg.MetadataValue.int(created + updated),
                "rows_errored": dg.MetadataValue.int(len(errors)),
                "rows_skipped_no_key": dg.MetadataValue.int(skipped_no_key),
                "composite_batching": dg.MetadataValue.bool(_self.use_composite),
            }
            if errors:
                metadata["first_errors"] = dg.MetadataValue.json(errors[:5])

            return dg.MaterializeResult(metadata=metadata)

        return dg.Definitions(assets=[_asset])
