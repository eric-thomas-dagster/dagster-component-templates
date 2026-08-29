"""DataFrame → HubSpot CRM object batch upsert (native).

Mirrors an upstream DataFrame into a HubSpot CRM object type using HubSpot's
native batch upsert endpoint —
`POST /crm/v3/objects/{objectType}/batch/upsert`.

HubSpot atomically creates or updates records based on matching a unique
property (`key_property`). Up to 100 records per HTTP call — this sink
chunks larger DataFrames automatically.

Object types include the standard set (`contacts`, `companies`, `deals`,
`tickets`, `line_items`, `products`, `quotes`) plus any custom objects
defined in the HubSpot portal (fully-qualified names like
`p123456_custom_thing`).

Pairs with:
  - ``hubspot_resource`` — Private App bearer auth + workhorse HTTP (required)
"""
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


class HubSpotObjectUpsertComponent(dg.Component, dg.Model, dg.Resolvable):
    """Batch-upsert rows from an upstream DataFrame into a HubSpot CRM object type.

    Example:
        ```yaml
        type: dagster_component_templates.HubSpotObjectUpsertComponent
        attributes:
          asset_name: hubspot_contacts_mirror
          upstream_asset_key: dbt_marts_customers
          resource_key: hubspot
          object_type: contacts
          key_property: email
          fields_map:
            email: email
            first_name: firstname
            last_name: lastname
            lifecycle_stage: lifecyclestage
            health_score: customer_health_score__c
          batch_size: 100
          group_name: reverse_etl
        ```

    For every upstream row:
      - HubSpot's native /batch/upsert atomically creates or updates based
        on matching `key_property`. No search-then-write, no race conditions.
      - Requests chunk at 100 records per HTTP call (HubSpot's limit).
    """

    asset_name: str = Field(description="Output Dagster asset name.")

    # Two source shapes — supply exactly one.
    upstream_asset_key: Optional[str] = Field(
        default=None,
        description="Upstream Dagster asset providing the DataFrame. Mutually exclusive with `source:`.",
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
        default="hubspot",
        description="Resource key registered by HubSpotResourceComponent.",
    )

    object_type: str = Field(
        description=(
            "HubSpot CRM object type (e.g. 'contacts', 'companies', 'deals', "
            "'tickets', 'line_items', 'products', 'quotes', or a custom object "
            "like 'p123456_custom_thing')."
        ),
    )
    key_property: str = Field(
        description=(
            "HubSpot property used as the upsert match key. Common: 'email' "
            "for contacts, 'domain' for companies, or any custom property "
            "marked 'unique'. MUST be present in fields_map values."
        ),
    )
    fields_map: Dict[str, str] = Field(
        description="Upstream column → HubSpot property name.",
    )
    batch_size: int = Field(
        default=100,
        description=(
            "Records per HubSpot API call. HubSpot caps at 100 per batch — "
            "the sink also enforces this cap."
        ),
    )
    max_rows: int = Field(
        default=10000,
        description="Overall safety cap on rows per run (independent of batch_size).",
    )

    group_name: Optional[str] = Field(
        default="hubspot", description="Dagster asset group name."
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
        description="Asset kinds (auto-includes 'hubspot').",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("hubspot")

        # Validate: exactly one of upstream_asset_key OR source: must be set.
        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError(
                "HubSpotObjectUpsertComponent: supply exactly one of "
                "`upstream_asset_key` OR `source:` (got both or neither)."
            )

        # Validate key_property is in fields_map values.
        mapped_fields = set(self.fields_map.values())
        if self.key_property not in mapped_fields:
            raise ValueError(
                f"HubSpotObjectUpsertComponent: key_property="
                f"{self.key_property!r} not in fields_map values. "
                f"key_property must be a HubSpot property you're upserting. "
                f"fields_map values: {sorted(mapped_fields)}"
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
                        conn = resource.get_connection()
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
            raise ValueError(f"HubSpotObjectUpsertComponent source kind={kind!r} not supported (sql / csv / inline)")

        def _run_upsert(context, upstream):
            hs = getattr(context.resources, _self.resource_key)

            import pandas as pd
            if not isinstance(upstream, pd.DataFrame):
                df = pd.DataFrame([upstream]) if isinstance(upstream, dict) else pd.DataFrame(upstream)
            else:
                df = upstream

            if len(df) == 0:
                context.log.warning("Upstream DataFrame is empty — nothing to upsert.")
                return dg.MaterializeResult(metadata={"rows_upserted": dg.MetadataValue.int(0)})

            if len(df) > _self.max_rows:
                context.log.warning(
                    f"Upstream has {len(df)} rows; capped at max_rows={_self.max_rows}."
                )
                df = df.head(_self.max_rows)

            required_cols = set(_self.fields_map.keys())
            missing_cols = [c for c in required_cols if c not in df.columns]
            if missing_cols:
                raise dg.Failure(
                    f"Columns not in upstream: {missing_cols}. Available: {list(df.columns)}"
                )

            # Upstream column mapped to key_property.
            key_col = next(
                (col for col, hs_prop in _self.fields_map.items()
                 if hs_prop == _self.key_property),
                None,
            )
            if key_col is None:
                raise dg.Failure(
                    f"fields_map has no column mapping to key_property="
                    f"{_self.key_property!r}. fields_map: {_self.fields_map}"
                )

            def _row_value(v):
                if v is None or (isinstance(v, float) and pd.isna(v)):
                    return None
                # HubSpot properties are always strings on the wire.
                return str(v) if not isinstance(v, str) else v

            records: List[dict] = []
            skipped_no_key = 0
            for _, row in df.iterrows():
                key_value = _row_value(row[key_col])
                if key_value is None or key_value == "":
                    skipped_no_key += 1
                    continue
                props: dict = {}
                for col, hs_prop in _self.fields_map.items():
                    v = _row_value(row[col])
                    if v is not None:
                        props[hs_prop] = v
                if not props or _self.key_property not in props:
                    skipped_no_key += 1
                    continue
                records.append(props)

            if not records:
                context.log.warning("No records with valid key_property values — nothing to upsert.")
                return dg.MaterializeResult(metadata={"rows_upserted": dg.MetadataValue.int(0)})

            errors: List[str] = []
            success_count = 0
            batch_count = 0

            batch_size = max(1, min(_self.batch_size, 100))
            try:
                responses = hs.upsert_objects(
                    _self.object_type,
                    _self.key_property,
                    records,
                    batch_size=batch_size,
                )
            except Exception as e:  # noqa: BLE001
                raise dg.Failure(
                    f"HubSpot batch upsert failed: {type(e).__name__}: {e}"
                )

            for resp in responses:
                batch_count += 1
                results = resp.get("results") or []
                success_count += len(results)
                num_errors = resp.get("numErrors") or 0
                if num_errors:
                    for err in (resp.get("errors") or [])[:5]:
                        errors.append(str(err))

            context.log.info(
                f"HubSpot upsert into {_self.object_type}: "
                f"{success_count} records upserted across {batch_count} batches, "
                f"{len(errors)} errors, {skipped_no_key} skipped "
                f"(missing key_property) — matched on {_self.key_property}."
            )
            if errors:
                context.log.error(
                    "First few errors:\n" + "\n".join(errors[:5])
                )

            metadata = {
                "hubspot_object_type": dg.MetadataValue.text(_self.object_type),
                "key_property": dg.MetadataValue.text(_self.key_property),
                "rows_upserted": dg.MetadataValue.int(success_count),
                "rows_errored": dg.MetadataValue.int(len(errors)),
                "rows_skipped_no_key": dg.MetadataValue.int(skipped_no_key),
                "batch_count": dg.MetadataValue.int(batch_count),
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
                f"Upsert DataFrame rows into HubSpot {_self.object_type} "
                f"(match on {_self.key_property})."
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
