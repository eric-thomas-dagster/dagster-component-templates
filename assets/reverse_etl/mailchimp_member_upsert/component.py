"""DataFrame -> Mailchimp list member upsert (native).

Mirrors an upstream DataFrame into a Mailchimp audience (list) via the
native upsert endpoint -- `PUT /lists/{list_id}/members/{subscriber_hash}`
-- which atomically creates or updates based on the member's email
(hashed to form subscriber_hash). No search-then-write needed.

Pairs with:
  - ``mailchimp`` resource -- API key auth (required)"""
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field
class MailchimpMemberUpsertComponent(dg.Component, dg.Model, dg.Resolvable):
    """Upsert rows from an upstream DataFrame into a Mailchimp audience.

    Example:
        ```yaml
        type: dagster_component_templates.MailchimpMemberUpsertComponent
        attributes:
          asset_name: mailchimp_members_mirror
          upstream_asset_key: dbt_marts_customers
          resource_key: mailchimp
          list_id: a1b2c3d4e5
          email_column: email
          merge_fields_map:
            full_name: FNAME
            plan_tier: PLAN
          tags_column: segment_tags
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
        default="mailchimp",
        description="Resource key registered by MailchimpResourceComponent.",
    )

    list_id: str = Field(description="Mailchimp audience (list) ID to upsert members into.")

    email_column: str = Field(description="Upstream column holding the member email (Mailchimp match key).")

    merge_fields_map: Dict[str, str] = Field(default_factory=dict, description="Upstream column -> Mailchimp merge field tag (e.g. FNAME, LNAME). Must already exist in the audience merge field schema.")

    tags_column: Optional[str] = Field(default=None, description="Upstream column holding tags to apply (list, or comma-separated string).")

    status_if_new: str = Field(default="subscribed", description="Status to set when creating a new member: subscribed / unsubscribed / pending / transactional.")

    max_rows: int = Field(default=10000, description="Overall safety cap on rows per run.")

    group_name: Optional[str] = Field(default="mailchimp", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds (auto-includes 'mailchimp').")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add('mailchimp')

        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError(
                "MailchimpMemberUpsertComponent: supply exactly one of "
                "`upstream_asset_key` OR `source:` (got both or neither)."
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
            raise ValueError(f"MailchimpMemberUpsertComponent source kind={kind!r} not supported (sql / csv / inline)")

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

            required_cols = {_self.email_column} | set(_self.merge_fields_map.keys())
            if _self.tags_column:
                required_cols.add(_self.tags_column)
            missing_cols = [c for c in required_cols if c not in df.columns]
            if missing_cols:
                raise dg.Failure(f"Columns not in upstream: {missing_cols}. Available: {list(df.columns)}")

            def _row_value(v):
                if v is None or (isinstance(v, float) and pd.isna(v)):
                    return None
                return v

            def _coerce_tags(value):
                if value is None:
                    return []
                if isinstance(value, (list, tuple)):
                    return [str(v) for v in value]
                return [s.strip() for s in str(value).split(",") if s.strip()]

            success_count = 0
            skipped_no_key = 0
            errors: List[str] = []
            for _, row in df.iterrows():
                email = _row_value(row[_self.email_column])
                if not email:
                    skipped_no_key += 1
                    continue
                merge_fields = {}
                for col, mc_field in _self.merge_fields_map.items():
                    v = _row_value(row[col])
                    if v is not None:
                        merge_fields[mc_field] = v
                tags = _coerce_tags(row[_self.tags_column]) if _self.tags_column else []
                try:
                    svc.upsert_member(
                        _self.list_id,
                        str(email),
                        merge_fields=merge_fields or None,
                        tags=tags or None,
                        status_if_new=_self.status_if_new,
                    )
                    success_count += 1
                except Exception as e:  # noqa: BLE001
                    errors.append(f"{email}: {type(e).__name__}: {e}")

            context.log.info(
                f"Mailchimp member upsert into list={_self.list_id}: {success_count} succeeded, "
                f"{len(errors)} errors, {skipped_no_key} skipped (missing email) -- matched on email."
            )
            if errors:
                context.log.error("First few errors:\n" + "\n".join(errors[:5]))
            metadata = {
                "mailchimp_list_id": dg.MetadataValue.text(_self.list_id),
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
            description=_self.description or (f"Upsert DataFrame rows into Mailchimp list {_self.list_id} (match on email)."),
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
