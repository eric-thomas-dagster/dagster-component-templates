"""DataFrame -> Microsoft Dynamics 365 (Dataverse) record upsert (native
alternate-key upsert).

Mirrors an upstream DataFrame into a Dataverse entity set using Dataverse's
native alternate-key upsert endpoint --
`PATCH {org_url}/api/data/{api_version}/{entity_set}({key}='{value}')`.

Dataverse handles create-or-update atomically in a single request: if no
row matches the alternate key, one is created (with that key value set);
if a row matches, it's updated. No search-then-write, no race conditions.

Entity SET names are the plural, lowercase form of the table's logical
name (`accounts`, `contacts`, `leads`, `opportunities`, or a custom
table's plural name e.g. `new_customthings`) -- NOT the singular
display-name-cased form shown in the maker portal UI.

Requires the target field (`alternate_key_field`) to be a pre-defined
**Alternate Key** on the Dataverse table (maker portal -> table -> Keys).
Composite (multi-column) alternate keys are NOT exposed by this component
(one alternate-key field per sink instance) -- see
`dynamics_crm_resource.upsert_by_key()` directly if you need composite-key
upsert from custom asset code.

Dataverse has no single-call bulk/composite upsert the way Salesforce
does. An OData `$batch` endpoint exists (multipart/mixed changesets) but
requires hand-building/parsing raw MIME bodies -- real complexity for a
win that's about round-trip count, not server-side parallelism. This
sink issues one PATCH per row; `$batch` is a documented, not-yet-
implemented future optimization (see README), not a fake "composite
mode."

Pairs with:
  - ``dynamics_crm_resource`` -- connection + Azure AD auth + workhorse HTTP (required)
"""
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


class DynamicsCrmRecordUpsertComponent(dg.Component, dg.Model, dg.Resolvable):
    """Batch-upsert rows from an upstream DataFrame into a Dynamics 365
    (Dataverse) entity set via its native alternate-key upsert.

    Example:
        ```yaml
        type: dagster_component_templates.DynamicsCrmRecordUpsertComponent
        attributes:
          asset_name: dynamics_crm_accounts_mirror
          upstream_asset_key: dbt_marts_accounts
          resource_key: dynamics_crm
          entity_set_name: accounts
          alternate_key_field: cr_external_account_id
          fields_map:
            account_id: cr_external_account_id
            name: name
            industry: industrycode
            annual_revenue: revenue
          batch_size: 5000
        ```

    For every upstream row:
      - `PATCH {entity_set}({alternate_key_field}='{value}')` -- atomic
        create-or-update keyed on a pre-defined Dataverse Alternate Key.
      - Uses `Prefer: return=representation` so the response status code
        (201 vs 200) tells us created vs. updated, and the response body
        carries the row's real GUID primary key back in the same round
        trip (without this header Dataverse always returns 204 for both
        outcomes -- genuinely indistinguishable).
    """

    asset_name: str = Field(description="Output Dagster asset name.")

    # Two source shapes -- supply exactly one.
    upstream_asset_key: Optional[str] = Field(
        default=None,
        description=(
            "Upstream Dagster asset providing the DataFrame. Mutually exclusive "
            "with `source:`."
        ),
    )
    source: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Inline source config. Mutually exclusive with `upstream_asset_key`. "
            "Shapes: {kind: sql, resource_key/database_url_env_var, query}, "
            "{kind: csv, path, read_csv_kwargs}, {kind: inline, rows}."
        ),
    )

    resource_key: str = Field(
        default="dynamics_crm",
        description="Resource key registered by DynamicsCrmResourceComponent.",
    )

    entity_set_name: str = Field(
        description=(
            "Target Dataverse entity SET name -- plural, lowercase logical "
            "name (e.g. 'accounts', 'contacts', 'leads', 'opportunities', or "
            "a custom table's plural logical name like 'new_customthings'). "
            "NOT the singular display name shown in the maker portal UI."
        ),
    )
    alternate_key_field: str = Field(
        description=(
            "Dataverse field logical name used as the upsert match key -- "
            "MUST be a pre-defined Alternate Key on the table (maker portal "
            "-> table -> Keys). MUST be present in fields_map values."
        ),
    )
    fields_map: Dict[str, str] = Field(
        description="Upstream column -> Dataverse field logical name.",
    )
    batch_size: int = Field(
        default=5000,
        description="Max upstream rows per run (safety cap). One PATCH per row.",
    )
    prefer_representation: bool = Field(
        default=True,
        description=(
            "Send `Prefer: return=representation` on every PATCH so the "
            "response status code (201/200) distinguishes created vs. "
            "updated and the row's GUID comes back in the same round trip. "
            "Set false to skip the extra server-side retrieve (faster, but "
            "every row's action is reported as 'unknown' -- Dataverse "
            "always returns 204 for both outcomes without this header)."
        ),
    )

    group_name: Optional[str] = Field(
        default="dynamics_crm", description="Dagster asset group name."
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
        description="Asset kinds (auto-includes 'dynamics_crm').",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("dynamics_crm")

        # Validate: exactly one of upstream_asset_key OR source: must be set.
        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError(
                "DynamicsCrmRecordUpsertComponent: supply exactly one of "
                "`upstream_asset_key` OR `source:` (got both or neither)."
            )

        # Validate alternate_key_field is in fields_map values.
        mapped_fields = set(self.fields_map.values())
        if self.alternate_key_field not in mapped_fields:
            raise ValueError(
                f"DynamicsCrmRecordUpsertComponent: alternate_key_field="
                f"{self.alternate_key_field!r} not in fields_map values. "
                f"alternate_key_field must be a Dataverse field you're "
                f"upserting. fields_map values: {sorted(mapped_fields)}"
            )

        use_source = self.source is not None
        extra_rks: set = set()
        if use_source and (self.source.get("kind") or "").lower() == "sql":
            _rk = self.source.get("resource_key")
            if _rk:
                extra_rks.add(_rk)

        # ── Source resolver (self-contained per no-shared-code rule) ──────
        def _resolve_source_df(exec_ctx):
            import pandas as pd
            src = _self.source or {}
            kind = (src.get("kind") or "").lower()
            if kind == "sql":
                query = src.get("query")
                if not query:
                    raise ValueError("source kind=sql requires 'query'")
                rk = src.get("resource_key")
                if rk:
                    resource = getattr(exec_ctx.resources, rk)
                    if hasattr(resource, "get_engine"):
                        return pd.read_sql(query, resource.get_engine())
                    if hasattr(resource, "get_connection"):
                        with resource.get_connection() as conn:
                            if hasattr(conn, "execute") and hasattr(conn, "df"):
                                return conn.execute(query).df()
                            return pd.read_sql(query, conn)
                    raise ValueError(f"source kind=sql: resource {rk!r} must expose .get_engine() or .get_connection()")
                env = src.get("database_url_env_var")
                if env:
                    import os
                    from sqlalchemy import create_engine
                    url = os.environ.get(env, "")
                    if not url:
                        raise ValueError(f"database_url_env_var {env!r} is unset")
                    return pd.read_sql(query, create_engine(url))
                raise ValueError("source kind=sql requires 'resource_key' OR 'database_url_env_var'")
            if kind == "csv":
                path = src.get("path")
                if not path:
                    raise ValueError("source kind=csv requires 'path'")
                return pd.read_csv(path, **(src.get("read_csv_kwargs") or {}))
            if kind == "inline":
                return pd.DataFrame(src.get("rows") or [])
            raise ValueError(f"DynamicsCrmRecordUpsertComponent source kind={kind!r} not supported (sql / csv / inline)")

        def _run_upsert(context, upstream):
            dyn = getattr(context.resources, _self.resource_key)

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

            # Upstream column mapped to alternate_key_field.
            key_col = next(
                (col for col, dv_field in _self.fields_map.items()
                 if dv_field == _self.alternate_key_field),
                None,
            )
            if key_col is None:
                raise dg.Failure(
                    f"fields_map has no column mapping to alternate_key_field="
                    f"{_self.alternate_key_field!r}. fields_map: {_self.fields_map}"
                )

            def _row_value(v):
                if v is None or (isinstance(v, float) and pd.isna(v)):
                    return None
                return v

            created = 0
            updated = 0
            unknown = 0
            skipped_no_key = 0
            errors: List[str] = []

            for idx, row in df.iterrows():
                key_value = _row_value(row[key_col])
                if key_value is None:
                    skipped_no_key += 1
                    continue

                body: dict = {}
                for col, dv_field in _self.fields_map.items():
                    if dv_field == _self.alternate_key_field:
                        # Don't send the alternate key value in the body --
                        # Dataverse ignores it on update and sets it from the
                        # URL on create, so including it is redundant at
                        # best and rejected at worst.
                        continue
                    v = _row_value(row[col])
                    if v is not None:
                        body[dv_field] = v

                try:
                    result = dyn.upsert_by_key(
                        _self.entity_set_name,
                        _self.alternate_key_field,
                        key_value,
                        body,
                        prefer_representation=_self.prefer_representation,
                    )
                    action = result.get("action")
                    if action == "created":
                        created += 1
                    elif action == "updated":
                        updated += 1
                    else:
                        unknown += 1
                except Exception as e:  # noqa: BLE001
                    errors.append(
                        f"row {idx} ({_self.alternate_key_field}={key_value}): "
                        f"{type(e).__name__}: {e}"
                    )

            context.log.info(
                f"Dynamics CRM upsert into {_self.entity_set_name}: {created} created, "
                f"{updated} updated, {unknown} unknown (no Prefer header), "
                f"{len(errors)} errors, {skipped_no_key} skipped "
                f"(missing alternate key) — matched on {_self.alternate_key_field}."
            )
            if errors:
                context.log.error(
                    "First few errors:\n" + "\n".join(errors[:5])
                )

            metadata = {
                "dynamics_entity_set": dg.MetadataValue.text(_self.entity_set_name),
                "alternate_key_field": dg.MetadataValue.text(_self.alternate_key_field),
                "rows_created": dg.MetadataValue.int(created),
                "rows_updated": dg.MetadataValue.int(updated),
                "rows_unknown_action": dg.MetadataValue.int(unknown),
                "rows_upserted": dg.MetadataValue.int(created + updated + unknown),
                "rows_errored": dg.MetadataValue.int(len(errors)),
                "rows_skipped_no_key": dg.MetadataValue.int(skipped_no_key),
            }
            if errors:
                metadata["first_errors"] = dg.MetadataValue.json(errors[:5])

            return dg.MaterializeResult(metadata=metadata)

        common_kwargs = dict(
            key=dg.AssetKey.from_user_string(_self.asset_name),
            group_name=_self.group_name,
            kinds=kinds,
            owners=_self.owners,
            tags=_self.tags,
            description=_self.description or (
                f"Upsert DataFrame rows into Dynamics 365 {_self.entity_set_name} "
                f"(match on alternate key {_self.alternate_key_field})."
            ),
        )

        if use_source:
            required_rks = {_self.resource_key} | extra_rks

            @dg.asset(required_resource_keys=required_rks, **common_kwargs)
            def _asset(context: dg.AssetExecutionContext):
                df = _resolve_source_df(context)
                return _run_upsert(context, df)
        else:
            @dg.asset(
                ins={"upstream": dg.AssetIn(key=dg.AssetKey.from_user_string(_self.upstream_asset_key))},
                required_resource_keys={_self.resource_key},
                **common_kwargs,
            )
            def _asset(context: dg.AssetExecutionContext, upstream):
                return _run_upsert(context, upstream)

        return dg.Definitions(assets=[_asset])
