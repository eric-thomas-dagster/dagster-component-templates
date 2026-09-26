"""DataFrame -> Intercom contact upsert (search-then-write).

Intercom has no single-call native upsert for Contacts: this sink searches
`/contacts/search` by external_id (or email) via the resource's
`upsert_contact`, then PUTs the match or POSTs a new contact.

Pairs with:
  - ``intercom`` resource -- Bearer token auth (required)"""
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field
class IntercomContactUpsertComponent(dg.Component, dg.Model, dg.Resolvable):
    """Upsert rows from an upstream DataFrame into Intercom contacts.

    Example:
        ```yaml
        type: dagster_component_templates.IntercomContactUpsertComponent
        attributes:
          asset_name: intercom_contacts_mirror
          upstream_asset_key: dbt_marts_customers
          resource_key: intercom
          email_column: email
          attributes_map:
            name: full_name
          custom_attributes_map:
            health_score: health_score
          group_name: reverse_etl
        ```
    """

    asset_name: str = Field(description="Output Dagster asset name.")

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
        default="intercom",
        description="Resource key registered by IntercomResourceComponent.",
    )

    email_column: Optional[str] = Field(default=None, description="Upstream column holding the contact email (used as the Intercom match key if set).")

    external_id_column: Optional[str] = Field(default=None, description="Upstream column holding an external_id (used as the Intercom match key if set; takes precedence over email).")

    attributes_map: Dict[str, str] = Field(default_factory=dict, description="Upstream column -> Intercom CORE contact field (e.g. name, phone).")

    custom_attributes_map: Dict[str, str] = Field(default_factory=dict, description="Upstream column -> Intercom custom_attributes key (must already exist in the Intercom workspace custom data attributes).")

    max_rows: int = Field(default=10000, description="Overall safety cap on rows per run.")

    group_name: Optional[str] = Field(default="intercom", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds (auto-includes 'intercom').")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add('intercom')

        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError(
                "IntercomContactUpsertComponent: supply exactly one of "
                "`upstream_asset_key` OR `source:` (got both or neither)."
            )

        if not self.email_column and not self.external_id_column:
            raise ValueError(
                "IntercomContactUpsertComponent: at least one of email_column or "
                "external_id_column is required."
            )

        use_source = self.source is not None
        extra_rks: set = set()
        if use_source and (self.source.get("kind") or "").lower() == "sql":
            _rk = self.source.get("resource_key")
            if _rk:
                extra_rks.add(_rk)

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
            raise ValueError(f"IntercomContactUpsertComponent source kind={kind!r} not supported (sql / csv / inline)")

        def _run_write(context, upstream):
            svc = getattr(context.resources, _self.resource_key)

            import pandas as pd
            if not isinstance(upstream, pd.DataFrame):
                df = pd.DataFrame([upstream]) if isinstance(upstream, dict) else pd.DataFrame(upstream)
            else:
                df = upstream

            if len(df) == 0:
                context.log.warning("Upstream DataFrame is empty -- nothing to write.")
                return dg.MaterializeResult(metadata={"rows_written": dg.MetadataValue.int(0)})

            if len(df) > _self.max_rows:
                context.log.warning(f"Upstream has {len(df)} rows; capped at max_rows={_self.max_rows}.")
                df = df.head(_self.max_rows)

            required_cols = set(_self.attributes_map.keys()) | set(_self.custom_attributes_map.keys())
            if _self.email_column:
                required_cols.add(_self.email_column)
            if _self.external_id_column:
                required_cols.add(_self.external_id_column)
            missing_cols = [c for c in required_cols if c not in df.columns]
            if missing_cols:
                raise dg.Failure(f"Columns not in upstream: {missing_cols}. Available: {list(df.columns)}")

            def _row_value(v):
                if v is None or (isinstance(v, float) and pd.isna(v)):
                    return None
                return v

            success_count = 0
            skipped_no_key = 0
            errors: List[str] = []
            for _, row in df.iterrows():
                email = _row_value(row[_self.email_column]) if _self.email_column else None
                external_id = _row_value(row[_self.external_id_column]) if _self.external_id_column else None
                if not email and not external_id:
                    skipped_no_key += 1
                    continue
                attrs = {}
                for col, ic_field in _self.attributes_map.items():
                    v = _row_value(row[col])
                    if v is not None:
                        attrs[ic_field] = v
                custom_attrs = {}
                for col, ic_field in _self.custom_attributes_map.items():
                    v = _row_value(row[col])
                    if v is not None:
                        custom_attrs[ic_field] = v
                if custom_attrs:
                    attrs["custom_attributes"] = custom_attrs
                try:
                    svc.upsert_contact(
                        external_id=str(external_id) if external_id else None,
                        email=str(email) if email else None,
                        attributes=attrs,
                    )
                    success_count += 1
                except Exception as e:  # noqa: BLE001
                    errors.append(f"{email or external_id}: {type(e).__name__}: {e}")

            context.log.info(
                f"Intercom contact upsert: {success_count} succeeded, {len(errors)} errors, "
                f"{skipped_no_key} skipped (missing email/external_id)."
            )
            if errors:
                context.log.error("First few errors:\n" + "\n".join(errors[:5]))
            metadata = {
                "rows_upserted": dg.MetadataValue.int(success_count),
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
            description=_self.description or ("Upsert DataFrame rows into Intercom contacts (match on email/external_id)."),
        )

        if use_source:
            required_rks = {_self.resource_key} | extra_rks

            @dg.asset(required_resource_keys=required_rks, **common_kwargs)
            def _asset(context: dg.AssetExecutionContext):
                df = _resolve_source_df(context)
                return _run_write(context, df)
        else:
            @dg.asset(
                ins={"upstream": dg.AssetIn(key=dg.AssetKey.from_user_string(_self.upstream_asset_key))},
                required_resource_keys={_self.resource_key},
                **common_kwargs,
            )
            def _asset(context: dg.AssetExecutionContext, upstream):
                return _run_write(context, upstream)

        return dg.Definitions(assets=[_asset])
