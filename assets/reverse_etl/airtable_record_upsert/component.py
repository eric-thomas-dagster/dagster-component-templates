"""DataFrame → Airtable Records upsert.

Mirrors an upstream DataFrame into an Airtable table using Airtable's
native server-side upsert endpoint —
`PATCH /v0/{baseId}/{tableName}?performUpsert[fieldsToMergeOn][]=<field>`.
Airtable handles create-or-update by matching `key_fields`, so the sink
is idempotent by design.

Pairs with:
  - ``airtable_resource`` — connection (required)
"""
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field



# BEGIN upstream-asset-key normalization (inlined per component; keep copies identical — see tests/test_upstream_keys.py)
from typing import List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def normalize_upstream_keys(single, plural=None) -> List[str]:
    """Merge ``upstream_asset_key`` / ``upstream_asset_keys`` into one clean list.

    Each argument may be a str, a list/tuple of str, or None. Entries are
    stripped, empties dropped, and duplicates removed (order preserved).
    """
    out: List[str] = []
    for value in (single, plural):
        if value is None:
            continue
        items = [value] if isinstance(value, str) else value
        if not isinstance(items, (list, tuple)):
            raise ValueError(
                f"upstream asset key(s) must be a string or a list of strings, got {value!r}"
            )
        for item in items:
            if not isinstance(item, str):
                raise ValueError(f"upstream asset keys must be strings, got {item!r}")
            item = item.strip()
            if item and item not in out:
                out.append(item)
    return out


def coerce_upstream_input(data, *, component: str, mode: str, optional: bool = True):
    """Rewrite raw component input so both spellings/shapes land on the declared field(s).

    mode "single": declared field is ``upstream_asset_key`` (str); >1 key is an error.
    mode "multi":  declared field is ``upstream_asset_keys`` (list).
    mode "both":   both fields are declared; everything is merged into the plural one.
    """
    if not isinstance(data, dict):
        return data
    if "upstream_asset_key" not in data and "upstream_asset_keys" not in data:
        return data
    keys = normalize_upstream_keys(data.get("upstream_asset_key"), data.get("upstream_asset_keys"))
    out = {k: v for k, v in data.items() if k not in ("upstream_asset_key", "upstream_asset_keys")}
    if mode == "single":
        if len(keys) > 1:
            raise ValueError(f"{component} takes exactly one upstream asset, got {keys}")
        out["upstream_asset_key"] = keys[0] if keys else (None if optional else "")
    else:
        out["upstream_asset_keys"] = keys or (None if optional else [])
    return out
# END upstream-asset-key normalization



# BEGIN field-shape normalization (inlined per component; keep copies identical — see tests/test_field_shapes.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def coerce_field_shapes(data, *, lists=(), csv=(), tags=()):
    """Accept the shapes people (and LLMs) naturally write for common boilerplate fields."""
    if not isinstance(data, dict):
        return data
    out = dict(data)
    for k in lists:  # List[str]: "a" / "a, b" -> ["a", "b"]; strip + drop empties
        v = out.get(k)
        if isinstance(v, str):
            v = v.split(",")
        if isinstance(v, (list, tuple)):
            out[k] = [x.strip() if isinstance(x, str) else x for x in v if not (isinstance(x, str) and not x.strip())]
    for k in csv:  # comma-separated str given as a list -> "a,b"
        v = out.get(k)
        if isinstance(v, (list, tuple, set)):
            out[k] = ",".join(str(x).strip() for x in v)
    for k in tags:  # Dict[str, str]: scalars -> str, ["k=v"] / ["k:v"] -> {k: v}
        v = out.get(k)
        if isinstance(v, (list, tuple)) and v and all(isinstance(x, str) and ("=" in x or ":" in x) for x in v):
            v = dict((p.strip() for p in (x.split("=", 1) if "=" in x else x.split(":", 1))) for x in v)
        if isinstance(v, dict):
            out[k] = {str(a).strip(): (str(b).lower() if isinstance(b, bool) else str(b)) for a, b in v.items() if b is not None}
    return out
# END field-shape normalization


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
    # Two source shapes — supply exactly one.
    upstream_asset_key: Optional[Union[str, List[str]]] = Field(
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
    tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds (auto-includes 'airtable').")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('owners', 'kinds'), csv=(), tags=('tags',))

    @model_validator(mode="before")
    @classmethod
    def _coerce_upstream_asset_keys(cls, data):
        return coerce_upstream_input(data, component=cls.__name__, mode="single", optional=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("airtable")

        # Validate: exactly one of upstream_asset_key OR source: must be set.
        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError(
                "AirtableRecordUpsertComponent: supply exactly one of "
                "`upstream_asset_key` OR `source:` (got both or neither)."
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
            raise ValueError(f"AirtableRecordUpsertComponent source kind={kind!r} not supported (sql / csv / inline)")

        def _run_upsert(context, upstream):
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

        common_kwargs = dict(
            key=dg.AssetKey.from_user_string(_self.asset_name),
            group_name=_self.group_name,
            kinds=kinds,
            owners=_self.owners,
            tags=_self.tags,
            description=_self.description or (
                f"Upsert DataFrame rows into Airtable {_self.base_id}/{_self.table}."
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
