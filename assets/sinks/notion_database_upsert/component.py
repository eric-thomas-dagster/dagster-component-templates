"""DataFrame → Notion database upsert.

Mirrors an upstream DataFrame into a Notion database. Each row is matched
against existing pages by a `key_property`; matches are updated, misses
are inserted.

Property values are serialized based on the database's schema, retrieved
from the Notion API at materialize time. Values not in `properties_map`
are left alone.

Pairs with:
  - ``notion_resource`` — connection (required)
  - ``notion_page_sync`` — single-page analogue
"""
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


def _serialize_property(value, schema_prop: dict) -> dict:
    """Serialize a Python value into a Notion property object.

    Uses the property's type from the database schema (or from an existing
    page's property) to pick the right shape.
    """
    ptype = schema_prop.get("type")
    if value is None:
        if ptype in ("title", "rich_text"):
            return {ptype: []}
        if ptype == "multi_select":
            return {"multi_select": []}
        return {ptype: None}

    if ptype == "title" or ptype == "rich_text":
        return {ptype: [{"type": "text", "text": {"content": str(value)}}]}
    if ptype == "number":
        try:
            return {"number": float(value)}
        except (TypeError, ValueError):
            return {"number": None}
    if ptype == "select":
        return {"select": {"name": str(value)}}
    if ptype == "multi_select":
        if isinstance(value, (list, tuple)):
            items = [str(v) for v in value]
        else:
            items = [s.strip() for s in str(value).split(",") if s.strip()]
        return {"multi_select": [{"name": v} for v in items]}
    if ptype == "checkbox":
        if isinstance(value, bool):
            return {"checkbox": value}
        return {"checkbox": str(value).lower() in ("true", "1", "yes", "y", "t")}
    if ptype == "date":
        try:
            iso = value.isoformat() if hasattr(value, "isoformat") else str(value)
        except Exception:  # noqa: BLE001
            iso = str(value)
        return {"date": {"start": iso}}
    if ptype in ("url", "email", "phone_number"):
        return {ptype: str(value) if value else None}
    if ptype == "status":
        return {"status": {"name": str(value)}}
    return {"rich_text": [{"type": "text", "text": {"content": str(value)}}]}


def _extract_property_value(prop: dict):
    """Extract a plain Python value from a Notion property object (for key matching)."""
    ptype = prop.get("type")
    if ptype == "title":
        return "".join(t.get("plain_text", "") for t in (prop.get("title") or []))
    if ptype == "rich_text":
        return "".join(t.get("plain_text", "") for t in (prop.get("rich_text") or []))
    if ptype == "number":
        return prop.get("number")
    if ptype == "select":
        return (prop.get("select") or {}).get("name")
    if ptype == "checkbox":
        return prop.get("checkbox")
    if ptype in ("url", "email", "phone_number"):
        return prop.get(ptype)
    if ptype == "status":
        return (prop.get("status") or {}).get("name")
    if ptype == "date":
        return (prop.get("date") or {}).get("start")
    return prop.get(ptype)


class NotionDatabaseUpsertComponent(dg.Component, dg.Model, dg.Resolvable):
    """Upsert rows from an upstream DataFrame into a Notion database.

    Example:
        ```yaml
        type: dagster_community_components.NotionDatabaseUpsertComponent
        attributes:
          asset_name: notion_incidents_mirror
          upstream_asset_key: incidents_current
          database_id: "abc123def456"
          resource_key: notion_resource
          key_property: Incident ID       # unique key in the Notion DB
          key_column: incident_id         # matching column in the DataFrame
          properties_map:
            incident_id: Incident ID
            title: Name
            severity: Severity
            status: Status
            opened_at: Opened
        ```
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

    database_id: str = Field(description="Notion database ID (UUID).")
    resource_key: str = Field(
        default="notion_resource",
        description="Resource key registered by NotionResourceComponent.",
    )

    key_property: str = Field(
        description=(
            "Notion property name that uniquely identifies a row (usually the "
            "title property). Rows in the upstream DataFrame are matched to "
            "existing Notion pages by this property's value."
        ),
    )
    key_column: str = Field(
        description="Upstream DataFrame column holding the value that matches `key_property`.",
    )
    properties_map: Dict[str, str] = Field(
        description="Upstream column → Notion property name.",
    )

    delete_missing: bool = Field(
        default=False,
        description=(
            "If true, archive Notion pages whose key value is NOT in the upstream "
            "DataFrame. Off by default — archiving is destructive."
        ),
    )
    batch_size: int = Field(
        default=100,
        description="Max upstream rows to process per run (safety cap).",
    )

    group_name: Optional[str] = Field(default="notion", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds (auto-includes 'notion').")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("notion")

        # Validate: exactly one of upstream_asset_key OR source: must be set.
        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError(
                "NotionDatabaseUpsertComponent: supply exactly one of "
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
            raise ValueError(f"NotionDatabaseUpsertComponent source kind={kind!r} not supported (sql / csv / inline)")

        def _run_upsert(context, upstream):
            notion = getattr(context.resources, _self.resource_key)

            import pandas as pd
            if not isinstance(upstream, pd.DataFrame):
                df = pd.DataFrame(upstream) if not isinstance(upstream, dict) else pd.DataFrame([upstream])
            else:
                df = upstream

            if len(df) == 0:
                context.log.warning("Upstream DataFrame is empty — nothing to upsert.")
                return dg.MaterializeResult(metadata={"rows_upserted": dg.MetadataValue.int(0)})

            if len(df) > _self.batch_size:
                context.log.warning(
                    f"Upstream has {len(df)} rows; capped at batch_size={_self.batch_size}. "
                    "Increase batch_size or partition the asset to sync more."
                )
                df = df.head(_self.batch_size)

            # Column existence checks
            missing_cols = [c for c in _self.properties_map.keys() if c not in df.columns]
            if _self.key_column not in df.columns:
                missing_cols.append(_self.key_column)
            if missing_cols:
                raise dg.Failure(
                    f"Columns not in upstream: {missing_cols}. Available: {list(df.columns)}"
                )

            # Retrieve DB schema (properties + types). Notion's 2025 API moved
            # properties from database → data_source, so we go through the DB
            # to find its primary data source, then fetch that.
            db = notion.get_database(_self.database_id)
            data_sources = db.get("data_sources") or []
            if data_sources:
                ds = notion.get_client().data_sources.retrieve(data_source_id=data_sources[0]["id"])
                schema = ds.get("properties") or {}
            else:
                # Fallback for older Notion API responses where properties live on the DB
                schema = db.get("properties") or {}
            if _self.key_property not in schema:
                raise dg.Failure(
                    f"key_property '{_self.key_property}' not in Notion DB schema. "
                    f"Available: {list(schema.keys())}"
                )
            missing_props = [
                p for p in _self.properties_map.values() if p not in schema
            ]
            if missing_props:
                raise dg.Failure(
                    f"Properties not in Notion DB schema: {missing_props}. "
                    f"Available: {list(schema.keys())}"
                )

            # Index existing pages by key value (auto-paginated)
            existing_by_key: dict = {}
            for page in notion.iter_query_database(database_id=_self.database_id):
                props = page.get("properties") or {}
                key_prop = props.get(_self.key_property) or {}
                key_val = _extract_property_value(key_prop)
                if key_val is not None:
                    existing_by_key[str(key_val)] = page

            created = 0
            updated = 0
            for _, row in df.iterrows():
                key_val = row[_self.key_column]
                if isinstance(key_val, float) and pd.isna(key_val):
                    context.log.warning(
                        f"Row has null key ({_self.key_column}) — skipping."
                    )
                    continue
                key_str = str(key_val)

                # Serialize all mapped props
                new_props: dict = {}
                for col, notion_prop in _self.properties_map.items():
                    value = row[col]
                    if isinstance(value, float) and pd.isna(value):
                        value = None
                    new_props[notion_prop] = _serialize_property(value, schema[notion_prop])

                existing = existing_by_key.get(key_str)
                if existing:
                    notion.update_page(page_id=existing["id"], properties=new_props)
                    updated += 1
                else:
                    notion.create_page(
                        parent={"database_id": _self.database_id},
                        properties=new_props,
                    )
                    created += 1

            archived = 0
            if _self.delete_missing:
                upstream_keys = {str(v) for v in df[_self.key_column].dropna().tolist()}
                for key_str, page in existing_by_key.items():
                    if key_str not in upstream_keys:
                        notion.update_page(page_id=page["id"], archived=True)
                        archived += 1

            context.log.info(
                f"Notion upsert complete: {created} created, {updated} updated, {archived} archived."
            )
            return dg.MaterializeResult(
                metadata={
                    "notion_database_id": dg.MetadataValue.text(_self.database_id),
                    "rows_created": dg.MetadataValue.int(created),
                    "rows_updated": dg.MetadataValue.int(updated),
                    "rows_archived": dg.MetadataValue.int(archived),
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
                f"Upsert DataFrame rows into Notion database {_self.database_id}."
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
