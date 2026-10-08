"""DataFrame -> Default workflow trigger fire (one API trigger invocation
per upstream row).

Default (default.com) is a GTM/revenue-operations platform: workflow
automation, lead routing, and scheduling. Its API trigger "[runs] the
workflow connected to the API trigger with the submitted form responses"
(https://docs.os.default.com/api-reference/triggers/fire-a-trigger.md) --
this component fires that trigger once per upstream row, e.g. to kick off
lead-routing/scheduling automations for every newly-scored lead.

Default's real request shape is NOT a flat column->value dict: `email` is
a REQUIRED, separate top-level identity field ("The lead being routed.
Identity always comes from this field."), and the mapped form fields ride
under a nested `responses` object. This component therefore has a
required `email_column` in addition to `field_mapping` (upstream column ->
Default form-field name, which populates `responses`).

Default has no upsert concept for trigger firings -- every run fires the
workflow again per row. Re-running this on the same data re-fires it
(same caveat as Asana's "no upsert for tasks").

Pairs with:
  - ``default_resource`` -- Default API key (Bearer token) auth (required)
"""
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


def _call_fire_trigger(resource, trigger_id: str, email: str, responses: Optional[dict]) -> dict:
    """Isolates the one external, paid-API boundary so it can be
    monkeypatched wholesale in tests without real HTTP calls."""
    return resource.fire_trigger(trigger_id, email=email, responses=responses or None)


class DefaultWorkflowTriggerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Fire one Default API trigger per row of an upstream DataFrame.

    Example:
        ```yaml
        type: dagster_component_templates.DefaultWorkflowTriggerComponent
        attributes:
          asset_name: default_lead_routing_triggers
          upstream_asset_key: scored_leads
          resource_key: default_resource
          trigger_id: "3fa85f64-5717-4562-b3fc-2c963f66afa6"
          email_column: lead_email
          field_mapping:
            company_name: Company
            deal_size_usd: Deal Size
          group_name: default
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
        default="default_resource",
        description="Resource key registered by DefaultResourceComponent.",
    )

    trigger_id: str = Field(
        description=(
            "Default API trigger id (UUID) to fire -- from Default's "
            "list-triggers endpoint or the API trigger node in the Default "
            "workflow builder."
        ),
    )

    email_column: str = Field(
        description=(
            "Upstream column holding the lead's email. Default's fire-a-trigger "
            "endpoint requires `email` as a separate, required top-level field -- "
            "it is always the identity Default routes on, distinct from the "
            "`responses` form payload built from `field_mapping`."
        )
    )

    field_mapping: Dict[str, str] = Field(
        default_factory=dict,
        description=(
            "Upstream column -> Default form-field name. Populates the "
            "trigger's `responses` payload (form field values the connected "
            "workflow reads)."
        ),
    )

    max_rows: int = Field(default=10000, description="Overall safety cap on rows per run.")

    group_name: Optional[str] = Field(default="default", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds (auto-includes 'default').")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("default")

        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError(
                "DefaultWorkflowTriggerComponent: supply exactly one of "
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
            raise ValueError(f"DefaultWorkflowTriggerComponent source kind={kind!r} not supported (sql / csv / inline)")

        def _run_write(context, upstream):
            svc = getattr(context.resources, _self.resource_key)

            import pandas as pd
            if not isinstance(upstream, pd.DataFrame):
                df = pd.DataFrame([upstream]) if isinstance(upstream, dict) else pd.DataFrame(upstream)
            else:
                df = upstream

            if len(df) == 0:
                context.log.warning("Upstream DataFrame is empty -- nothing to fire.")
                return dg.MaterializeResult(metadata={"rows_fired": dg.MetadataValue.int(0)})

            if len(df) > _self.max_rows:
                context.log.warning(f"Upstream has {len(df)} rows; capped at max_rows={_self.max_rows}.")
                df = df.head(_self.max_rows)

            required_cols = {_self.email_column} | set(_self.field_mapping.keys())
            missing_cols = [c for c in required_cols if c not in df.columns]
            if missing_cols:
                raise dg.Failure(f"Columns not in upstream: {missing_cols}. Available: {list(df.columns)}")

            def _row_value(v):
                if v is None or (isinstance(v, float) and pd.isna(v)):
                    return None
                return v

            success_count = 0
            skipped_no_email = 0
            errors: List[str] = []
            for _, row in df.iterrows():
                email = _row_value(row[_self.email_column])
                if not email:
                    skipped_no_email += 1
                    continue
                responses: Dict[str, Any] = {}
                for col, field_name in _self.field_mapping.items():
                    v = _row_value(row[col])
                    if v is not None:
                        responses[field_name] = v
                try:
                    _call_fire_trigger(svc, _self.trigger_id, str(email), responses)
                    success_count += 1
                except Exception as e:  # noqa: BLE001
                    errors.append(f"{email}: {type(e).__name__}: {e}")

            context.log.info(
                f"Default workflow trigger fire: trigger={_self.trigger_id} "
                f"{success_count} fired, {len(errors)} errors, "
                f"{skipped_no_email} skipped (missing {_self.email_column})."
            )
            if errors:
                context.log.error("First few errors:\n" + "\n".join(errors[:5]))
            metadata = {
                "trigger_id": dg.MetadataValue.text(_self.trigger_id),
                "rows_fired": dg.MetadataValue.int(success_count),
                "rows_errored": dg.MetadataValue.int(len(errors)),
                "rows_skipped_no_email": dg.MetadataValue.int(skipped_no_email),
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
            description=_self.description or ("Fire one Default API trigger per upstream row."),
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
