"""DataFrame → PagerDuty Incidents upsert.

Mirrors an upstream DataFrame into PagerDuty incidents against a target
service. Each row's `key_column` value becomes the incident's
`incident_key` — PagerDuty's server-side dedup mechanism. Same key
submitted twice returns the existing open incident on the second call,
so upsert is idempotent by design.

Optional `status_column` drives per-row transitions to
`acknowledged` / `resolved`.

Pairs with:
  - ``pagerduty_resource`` — connection (required)
"""
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


class PagerDutyIncidentUpsertComponent(dg.Component, dg.Model, dg.Resolvable):
    """Upsert rows from an upstream DataFrame into PagerDuty incidents.

    Example:
        ```yaml
        type: dagster_community_components.PagerDutyIncidentUpsertComponent
        attributes:
          asset_name: pagerduty_incidents_mirror
          upstream_asset_key: incidents_seed
          service_id: PFF0H74           # PagerDuty service ID (from the UI URL)
          resource_key: pd
          key_column: incident_id       # → PagerDuty incident_key (dedup marker)
          title_column: name
          details_column: description
          urgency_column: urgency       # optional: 'high' or 'low' per row
          status_column: status         # optional: 'triggered' / 'acknowledged' / 'resolved'
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
    service_id: str = Field(description="Target PagerDuty service ID (starts with 'P').")
    resource_key: str = Field(
        default="pd",
        description="Resource key registered by PagerDutyResourceComponent.",
    )

    key_column: str = Field(
        description=(
            "Upstream column holding a stable unique key. Written to each incident's "
            "`incident_key` field — PagerDuty's server-side dedup marker."
        ),
    )
    title_column: str = Field(description="Column holding the incident title.")
    details_column: Optional[str] = Field(
        default=None,
        description="Column holding the incident body/details (plain text).",
    )
    urgency_column: Optional[str] = Field(
        default=None,
        description="Column holding urgency ('high' or 'low'). Defaults to 'high' if unset.",
    )
    status_column: Optional[str] = Field(
        default=None,
        description=(
            "Column holding target status per row: 'triggered' / 'acknowledged' / "
            "'resolved'. If set, each row's incident is transitioned to that state "
            "after upsert."
        ),
    )
    default_urgency: str = Field(default="high", description="Urgency fallback when `urgency_column` unset.")

    key_prefix: str = Field(
        default="dagster-",
        description=(
            "Prepended to each key_column value to form the PagerDuty incident_key. "
            "Keeps demo/scratch incidents easy to filter and prevents collisions "
            "with existing keys."
        ),
    )
    batch_size: int = Field(default=100, description="Max upstream rows to process per run (safety cap).")

    group_name: Optional[str] = Field(default="pagerduty", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds (auto-includes 'pagerduty').")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("pagerduty")

        # Validate: exactly one of upstream_asset_key OR source: must be set.
        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError(
                "PagerDutyIncidentUpsertComponent: supply exactly one of "
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
            raise ValueError(f"PagerDutyIncidentUpsertComponent source kind={kind!r} not supported (sql / csv / inline)")

        def _run_upsert(context, upstream):
            pd_res = getattr(context.resources, _self.resource_key)

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

            required_cols = {_self.key_column, _self.title_column}
            for c in (_self.details_column, _self.urgency_column, _self.status_column):
                if c:
                    required_cols.add(c)
            missing_cols = [c for c in required_cols if c not in df.columns]
            if missing_cols:
                raise dg.Failure(
                    f"Columns not in upstream: {missing_cols}. Available: {list(df.columns)}"
                )

            # Index existing incidents on this service by incident_key so we know
            # who's already there and what state they're in. Include resolved
            # incidents — creating with a key that ONLY matches a resolved one
            # WILL create a duplicate (PagerDuty only dedups against open ones).
            existing_by_key: Dict[str, dict] = {}
            for inc in pd_res.iter_incidents(
                service_ids=[_self.service_id],
                statuses=["triggered", "acknowledged", "resolved"],
            ):
                k = inc.get("incident_key")
                if k:
                    existing_by_key[k] = inc

            created = 0
            updated = 0
            transitioned = 0
            for _, row in df.iterrows():
                raw_key = row[_self.key_column]
                if isinstance(raw_key, float) and pd.isna(raw_key):
                    context.log.warning(f"Row has null key ({_self.key_column}) — skipping.")
                    continue
                incident_key = f"{_self.key_prefix}{raw_key}"
                title = str(row[_self.title_column])
                details = str(row[_self.details_column]) if _self.details_column and not (
                    isinstance(row[_self.details_column], float) and pd.isna(row[_self.details_column])
                ) else ""
                urgency = (
                    str(row[_self.urgency_column]).lower()
                    if _self.urgency_column
                    else _self.default_urgency
                )
                if urgency not in ("high", "low"):
                    urgency = _self.default_urgency

                existing = existing_by_key.get(incident_key)
                if existing:
                    # Only patch what the API allows on an existing incident (title / urgency)
                    pd_res.update_incident(existing["id"], title=title, urgency=urgency)
                    incident_id = existing["id"]
                    updated += 1
                else:
                    # PagerDuty auto-dedups by incident_key server-side; POST with an
                    # existing open key just returns the existing one, but we
                    # already checked, so this is a genuine create.
                    inc = pd_res.create_incident(
                        service_id=_self.service_id,
                        title=title,
                        details=details,
                        incident_key=incident_key,
                        urgency=urgency,
                    )
                    incident_id = inc.get("id")
                    if incident_id:
                        existing_by_key[incident_key] = inc  # cache for in-run retries
                    created += 1

                # Optional status transition. PagerDuty only moves forward:
                # triggered → acknowledged → resolved. Skip no-op transitions
                # and warn on backward ones (e.g. resolved incident, row says
                # triggered) rather than trying an invalid API call.
                if _self.status_column and incident_id:
                    target = str(row[_self.status_column]).lower()
                    current = (existing.get("status") if existing else "triggered") or "triggered"
                    forward = {"triggered": 0, "acknowledged": 1, "resolved": 2}
                    if target == current or target == "" or target == "triggered":
                        pass  # no-op — already at target or default state
                    elif target not in forward:
                        context.log.warning(
                            f"Unknown status {target!r} on row (key={incident_key}); skipping transition."
                        )
                    elif forward[target] < forward.get(current, 0):
                        context.log.warning(
                            f"Cannot move {incident_id} from {current!r} back to {target!r} — "
                            "PagerDuty transitions only move forward. Skipping."
                        )
                    else:
                        try:
                            if target == "acknowledged":
                                pd_res.acknowledge_incident(incident_id)
                            else:
                                pd_res.resolve_incident(incident_id)
                            transitioned += 1
                        except Exception as e:  # noqa: BLE001
                            context.log.warning(
                                f"Status transition to {target!r} on {incident_id} failed: {e}"
                            )

            context.log.info(
                f"PagerDuty upsert complete: {created} created, {updated} updated, {transitioned} transitioned."
            )
            return dg.MaterializeResult(
                metadata={
                    "pagerduty_service_id": dg.MetadataValue.text(_self.service_id),
                    "rows_created": dg.MetadataValue.int(created),
                    "rows_updated": dg.MetadataValue.int(updated),
                    "rows_transitioned": dg.MetadataValue.int(transitioned),
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
                f"Upsert DataFrame rows into PagerDuty incidents on service {_self.service_id}."
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
