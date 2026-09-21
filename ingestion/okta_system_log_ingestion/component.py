"""OktaSystemLogIngestionComponent.

Pull Okta System Log events (every authentication, MFA challenge, admin action) via /api/v1/logs.

Authentication: this component reads credentials from environment variables —
configure your tenant before running. See README.md for the full list.
"""

import json
import os
from typing import Optional

import dagster as dg
import pandas as pd
from pydantic import Field


class OktaSystemLogIngestionComponent(dg.Component, dg.Model, dg.Resolvable):
    """Pull Okta System Log events (every authentication, MFA challenge, admin action) via /api/v1/logs."""

    asset_name: str = Field(description="Dagster asset name")

    okta_domain: str = Field(description="Okta domain (e.g. 'acme.okta.com')")
    api_token_env: str = Field(default="OKTA_API_TOKEN", description="Env var holding the SSWS API token")
    lookback_hours: int = Field(default=24, description="How far back to fetch")
    event_filter: Optional[str] = Field(default=None, description="Okta filter expression (e.g. 'eventType eq \"user.session.start\"')")
    limit: int = Field(default=1000, description="Per-page limit (max 1000)")

    description: Optional[str] = Field(default=None, description="Asset description")
    group_name: str = Field(default="security_audit", description="Dagster asset group")
    deps: Optional[list[str]] = Field(default=None, description="Upstream asset deps")
    owners: Optional[list[str]] = Field(default=None, description="Asset owners")
    asset_tags: Optional[dict] = Field(default=None, description="Catalog tags")
    kinds: Optional[list[str]] = Field(default=None, description="Asset kinds")
    freshness_max_lag_minutes: Optional[int] = Field(default=None)
    freshness_cron: Optional[str] = Field(default=None)
    retry_policy_max_retries: Optional[int] = Field(default=None)
    retry_policy_delay_seconds: Optional[int] = Field(default=None)
    retry_policy_backoff: str = Field(default="exponential")
    include_preview_metadata: bool = Field(default=True, description="Emit preview metadata")
    preview_rows: int = Field(default=20, description="Preview row count")

    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily' / 'weekly' / 'monthly' / 'hourly' / 'static' / 'dynamic' / None for unpartitioned.",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.",
    )
    partition_values: Optional[str] = Field(
        default=None,
        description="Comma-separated values for static partitioning, e.g. 'us,eu,asia'.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition when partition_type='dynamic'.",
    )

    sinks: Optional[list] = Field(
        default=None,
        description=(
            "Optional list of sinks that write the fetched DataFrame to a warehouse "
            "table via an existing Dagster resource, in addition to returning the "
            "DataFrame -- skips needing a separate downstream writer asset. Each: "
            "{kind: table, resource_key: <name>, table: <name>, schema: <optional>, "
            "if_exists: append|replace, mode: upsert_on_match, match: [col, col]}. "
            "mode:upsert_on_match gives partition-rewrite idempotency (DELETE-then-"
            "INSERT keyed by match, in a transaction). Auto-detects DuckDB "
            ".register() fast path; falls back to SQLAlchemy for postgres/"
            "snowflake/bigquery/mysql/mssql. The resource named by resource_key "
            "must already be configured elsewhere in the project (e.g. a "
            "snowflake_resource or postgres_resource component instance)."
        ),
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        partitions_def = None
        if self.partition_type:
            from dagster import (
                DailyPartitionsDefinition, WeeklyPartitionsDefinition,
                MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
                StaticPartitionsDefinition, DynamicPartitionsDefinition,
            )
            _pt = self.partition_type
            _values = [v.strip() for v in (self.partition_values or "").split(",") if v.strip()]
            if _pt in ("daily", "weekly", "monthly", "hourly") and not self.partition_start:
                raise ValueError(f"partition_type={_pt!r} requires partition_start (ISO date).")
            if _pt == "daily":
                partitions_def = DailyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "weekly":
                partitions_def = WeeklyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "monthly":
                partitions_def = MonthlyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "hourly":
                partitions_def = HourlyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "static":
                if not _values:
                    raise ValueError("partition_type='static' requires partition_values.")
                partitions_def = StaticPartitionsDefinition(_values)
            elif _pt == "dynamic":
                if not self.dynamic_partition_name:
                    raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
                partitions_def = DynamicPartitionsDefinition(name=self.dynamic_partition_name)

        _self = self
        sinks = self.sinks or []
        # Compute required_resource_keys from sinks -- Dagster wires only what
        # we declare, so this must reflect every `resource_key` we call into.
        required_resource_keys: set = set()
        for _sink in sinks:
            _rk = _sink.get("resource_key")
            if _rk:
                required_resource_keys.add(_rk)
        retry = None
        if self.retry_policy_max_retries:
            retry = dg.RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=dg.Backoff.EXPONENTIAL if self.retry_policy_backoff == "exponential" else dg.Backoff.LINEAR,
            )
        freshness = None
        if self.freshness_max_lag_minutes:
            freshness = dg.FreshnessPolicy(
                maximum_lag_minutes=self.freshness_max_lag_minutes,
                cron_schedule=self.freshness_cron,
            )

        @dg.asset(
            key=dg.AssetKey.from_user_string(self.asset_name),
            description=self.description or "Pull Okta System Log events (every authentication, MFA challenge, admin action) via /api/v1/logs.",
            group_name=self.group_name,
            kinds=set(self.kinds or ['okta', 'audit', 'iam']),
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
            owners=self.owners or None,
            tags=self.asset_tags or None,
            freshness_policy=freshness,
            retry_policy=retry,
            partitions_def=partitions_def,
            required_resource_keys=required_resource_keys or None,
        )
        def _asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            df: pd.DataFrame
            import requests, datetime as dt
            token = os.environ[_self.api_token_env]
            end = dt.datetime.utcnow()
            start = end - dt.timedelta(hours=_self.lookback_hours)
            url = f"https://{_self.okta_domain}/api/v1/logs"
            params = {
                "since": start.isoformat() + "Z",
                "until": end.isoformat() + "Z",
                "limit": _self.limit,
            }
            if _self.event_filter:
                params["filter"] = _self.event_filter
            headers = {"Authorization": f"SSWS {token}", "Accept": "application/json"}
            all_events = []
            next_url = url
            while next_url:
                r = requests.get(next_url, params=params if next_url == url else None, headers=headers, timeout=60)
                r.raise_for_status()
                all_events.extend(r.json())
                link = r.headers.get("Link", "")
                next_url = None
                for part in link.split(","):
                    if 'rel="next"' in part:
                        next_url = part.split(";")[0].strip().strip("<>")
            df = pd.DataFrame(all_events)
            metadata = {
                "dagster/row_count": dg.MetadataValue.int(len(df)),
            }
            if _self.include_preview_metadata and len(df) > 0:
                try:
                    sample = df.sample(min(_self.preview_rows, len(df))) if len(df) > _self.preview_rows * 10 else df.head(_self.preview_rows)
                    metadata["preview"] = dg.MetadataValue.md(sample.to_markdown(index=False))
                except Exception as exc:
                    context.log.warning(f"preview emission failed: {exc}")

            # ── Sinks: write the DataFrame to configured warehouse tables ────
            # Auto-detects DuckDB `.register()` fast path; falls back to
            # SQLAlchemy `to_sql`. Supports `mode: upsert_on_match` for
            # partition-rewrite idempotency (DELETE + INSERT in a tx). The
            # asset still returns `df` either way, so this is additive -- a
            # downstream asset can keep chaining off this one as a DataFrame
            # source even when a sink is also configured.
            if sinks:
                from contextlib import nullcontext
                sink_df = df.copy()  # avoid mutating the asset's return value
                for sink in sinks:
                    kind = (sink.get("kind") or "table").lower()
                    if kind != "table":
                        raise ValueError(
                            f"okta_system_log_ingestion sinks: only kind=table is "
                            f"supported (got {kind!r})"
                        )
                    sink_resource_key = sink.get("resource_key")
                    if not sink_resource_key:
                        raise ValueError("sink kind=table requires 'resource_key'")
                    sink_table = sink.get("table")
                    if not sink_table:
                        raise ValueError("sink kind=table requires 'table'")
                    sink_schema = sink.get("schema")
                    sink_if_exists = sink.get("if_exists", "append")
                    sink_mode = (sink.get("mode") or "").lower() or None
                    sink_match = list(sink.get("match") or [])
                    if sink_mode == "upsert_on_match" and not sink_match:
                        raise ValueError(
                            "sink mode=upsert_on_match requires 'match: [col, ...]'"
                        )

                    sink_resource = getattr(context.resources, sink_resource_key)

                    def _acquire():
                        if hasattr(sink_resource, "get_connection"):
                            gc = sink_resource.get_connection()
                            return gc if hasattr(gc, "__enter__") else nullcontext(gc)
                        if hasattr(sink_resource, "get_engine"):
                            eng = sink_resource.get_engine()
                            return eng if hasattr(eng, "__enter__") else nullcontext(eng)
                        raise ValueError(
                            f"sink resource {sink_resource_key!r} must expose "
                            f".get_connection() or .get_engine()"
                        )

                    qualified = f"{sink_schema}.{sink_table}" if sink_schema else sink_table
                    with _acquire() as conn:
                        # Fast path: DuckDB .register() / .execute() / .unregister().
                        if (
                            hasattr(conn, "register")
                            and hasattr(conn, "execute")
                            and hasattr(conn, "unregister")
                        ):
                            conn.register("_okta_sink_batch", sink_df)
                            try:
                                if sink_mode == "upsert_on_match":
                                    conn.execute(
                                        f"CREATE TABLE IF NOT EXISTS {qualified} AS "
                                        f"SELECT * FROM _okta_sink_batch WHERE 1=0"
                                    )
                                    match_tuple = ", ".join(sink_match)
                                    conn.execute("BEGIN TRANSACTION")
                                    try:
                                        conn.execute(
                                            f"DELETE FROM {qualified} WHERE ({match_tuple}) IN "
                                            f"(SELECT DISTINCT {match_tuple} FROM _okta_sink_batch)"
                                        )
                                        conn.execute(
                                            f"INSERT INTO {qualified} "
                                            f"SELECT * FROM _okta_sink_batch"
                                        )
                                        conn.execute("COMMIT")
                                    except Exception:
                                        conn.execute("ROLLBACK")
                                        raise
                                elif sink_if_exists == "replace":
                                    conn.execute(
                                        f"CREATE OR REPLACE TABLE {qualified} AS "
                                        f"SELECT * FROM _okta_sink_batch"
                                    )
                                else:
                                    conn.execute(
                                        f"CREATE TABLE IF NOT EXISTS {qualified} AS "
                                        f"SELECT * FROM _okta_sink_batch WHERE 1=0"
                                    )
                                    conn.execute("BEGIN TRANSACTION")
                                    try:
                                        conn.execute(
                                            f"INSERT INTO {qualified} "
                                            f"SELECT * FROM _okta_sink_batch"
                                        )
                                        conn.execute("COMMIT")
                                    except Exception:
                                        conn.execute("ROLLBACK")
                                        raise
                                metadata[f"sink/{qualified}/fast_path"] = "duckdb-register"
                            finally:
                                try:
                                    conn.unregister("_okta_sink_batch")
                                except Exception:  # noqa: BLE001
                                    pass
                        else:
                            # SQLAlchemy fallback path.
                            if sink_mode == "upsert_on_match":
                                from sqlalchemy import text as _sa_text
                                distinct = sink_df[sink_match].drop_duplicates()
                                match_tuple = ", ".join(sink_match)
                                if len(distinct) > 0:
                                    placeholders = ", ".join(
                                        "(" + ", ".join(f":v{i}_{j}" for j in range(len(sink_match))) + ")"
                                        for i in range(len(distinct))
                                    )
                                    params_sql = {}
                                    for i, row in enumerate(distinct.itertuples(index=False)):
                                        for j, v in enumerate(row):
                                            params_sql[f"v{i}_{j}"] = v
                                    tx = conn.begin() if hasattr(conn, "begin") else None
                                    if tx is not None:
                                        with tx as _c:
                                            _c.execute(
                                                _sa_text(
                                                    f"DELETE FROM {qualified} "
                                                    f"WHERE ({match_tuple}) IN ({placeholders})"
                                                ),
                                                params_sql,
                                            )
                                            sink_df.to_sql(
                                                sink_table, _c, schema=sink_schema,
                                                if_exists="append", index=False,
                                            )
                                    else:
                                        sink_df.to_sql(
                                            sink_table, conn, schema=sink_schema,
                                            if_exists="append", index=False,
                                        )
                                else:
                                    sink_df.to_sql(
                                        sink_table, conn, schema=sink_schema,
                                        if_exists="append", index=False,
                                    )
                            else:
                                sink_df.to_sql(
                                    sink_table, conn, schema=sink_schema,
                                    if_exists=sink_if_exists, index=False,
                                )
                            metadata[f"sink/{qualified}/fast_path"] = "sqlalchemy-to_sql"

                    metadata[f"sink/{qualified}/rows"] = len(sink_df)
                    metadata[f"sink/{qualified}/mode"] = (
                        f"upsert_on_match({','.join(sink_match)})"
                        if sink_mode == "upsert_on_match"
                        else sink_if_exists
                    )
                    context.log.info(
                        f"sink → {qualified} (via {sink_resource_key}, "
                        f"{metadata[f'sink/{qualified}/mode']}, "
                        f"{len(sink_df)} rows)"
                    )

            return dg.MaterializeResult(value=df, metadata=metadata)

        return dg.Definitions(assets=[_asset])
