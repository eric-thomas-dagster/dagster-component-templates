"""RouterComponent.

Multi-output conditional split — emit each output asset with rows matching its predicate. Equivalent to ADF Conditional Split / Informatica Router.
"""

from typing import Any, Dict, Optional

import dagster as dg
import pandas as pd
from pydantic import Field


def _ingest_warehouse_query(source_config: dict, context) -> "pd.DataFrame":
    """Execute SQL via a Dagster resource that exposes .get_engine()
    (SQLAlchemy), .get_connection() (DBAPI), or .get_client() (vendor
    client -- dispatched by the client's own shape since "get_client" means
    something different per vendor: BigQuery's .query(sql).to_dataframe(),
    Redshift's .execute_query(sql, fetch_results=True, cursor_factory=
    RealDictCursor)), or a bare SQLAlchemy engine built from
    `database_url_env_var` when no Dagster resource is registered. Same
    helper, same contract, as every other dual-ingestion component in this
    repo (e.g. automl_asset, logistic_regression_model, churn_prediction).

    Note (router-specific): each route is a separate sibling asset that
    independently consumes the same upstream -- when source mode is used,
    this means the SQL query runs once PER ROUTE, not once overall. Same
    cost profile as N separate assets each depending on the same upstream
    asset via ins= (Dagster doesn't share execution state across sibling
    consumers either way), just paid at query time instead of IO-manager
    read time."""
    sql = source_config["sql"]
    resource_key = source_config.get("resource_key")
    if resource_key:
        resource = getattr(context.resources, resource_key)
        if hasattr(resource, "get_engine"):
            return pd.read_sql(sql, resource.get_engine())
        if hasattr(resource, "get_connection"):
            with resource.get_connection() as conn:
                return pd.read_sql(sql, conn)
        if hasattr(resource, "get_client"):
            client = resource.get_client()
            if hasattr(client, "query"):
                job = client.query(sql)
                if hasattr(job, "to_dataframe"):
                    return job.to_dataframe()
            if hasattr(client, "execute_query"):
                try:
                    from psycopg2.extras import RealDictCursor
                    rows = client.execute_query(sql, fetch_results=True, cursor_factory=RealDictCursor)
                except ImportError:
                    rows = client.execute_query(sql, fetch_results=True)
                return pd.DataFrame([dict(r) for r in (rows or [])])
            raise ValueError(
                f"resource {resource_key!r}'s get_client() returned {type(client).__name__}, "
                "which this helper doesn't know how to query (no .query()/.to_dataframe() "
                "or .execute_query() method found). Add a dispatch branch for it."
            )
        raise ValueError(
            f"resource {resource_key!r} must expose .get_engine() (SQLAlchemy), "
            f".get_connection() (DBAPI), or .get_client() (vendor client); got {type(resource).__name__}"
        )
    env_var = source_config.get("database_url_env_var")
    if env_var:
        import os
        from sqlalchemy import create_engine
        url = os.environ.get(env_var, "")
        if not url:
            raise ValueError(f"database_url_env_var {env_var!r} is unset")
        return pd.read_sql(sql, create_engine(url))
    raise ValueError("source requires 'resource_key' OR 'database_url_env_var'")


class RouterComponent(dg.Component, dg.Model, dg.Resolvable):
    """Multi-output conditional split — emit each output asset with rows matching its predicate. Equivalent to ADF Conditional Split / Informatica Router."""


    upstream_asset_key: Optional[str] = Field(
        default=None,
        description="Upstream DataFrame. Mutually exclusive with `source` -- set exactly one.",
    )
    source: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Pull rows directly via SQL instead of from an upstream asset: "
            "{kind: warehouse_query, resource_key: <registered resource> OR database_url_env_var: <env var>, sql: <query>}. "
            "Mutually exclusive with `upstream_asset_key` -- set exactly one. Note: the query runs once "
            "per route (see _ingest_warehouse_query's docstring), not once overall."
        ),
    )

    routes: list = Field(description="List of {asset_name, condition} dicts. Conditions evaluated in order; non-matching rows go to default route if set.")
    default_asset_name: Optional[str] = Field(default=None, description="Name for the catch-all asset (rows that didn't match any condition)")
    exclusive: bool = Field(default=True, description="If True, each row goes to exactly one route (first match). If False, a row may appear in multiple routes (overlapping conditions).")

    group_name: str = Field(default="transforms")
    kinds: Optional[list[str]] = Field(default=None)

    retry_policy_max_retries: Optional[int] = Field(
        default=None,
        description="Max retries on asset failure. Useful for transient errors like network glitches or rate limits.",
    )
    retry_policy_delay_seconds: Optional[int] = Field(
        default=None,
        description="Seconds between retries (default 1).",
    )
    retry_policy_backoff: str = Field(
        default="exponential",
        description="Backoff strategy: 'linear' or 'exponential'.",
    )

    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale.",
    )
    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5'.",
    )

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

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError("RouterComponent: set exactly one of `upstream_asset_key` or `source`.")
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

        freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy

            freshness_policy = FreshnessPolicy(
                maximum_lag_minutes=self.freshness_max_lag_minutes,
                cron_schedule=self.freshness_cron,
            )

        retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy

            retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        _self = self
        upstream_key = self.upstream_asset_key

        def _make_route(name, condition):
            @dg.asset(
                name=name,
                description=f"router branch: {condition}",
                group_name=_self.group_name,
                kinds=set(_self.kinds or ["router"]),
                ins=({"df": dg.AssetIn(key=dg.AssetKey.from_user_string(upstream_key))} if upstream_key else None),
                required_resource_keys=({_self.source["resource_key"]} if (_self.source and _self.source.get("resource_key")) else None),
                retry_policy=retry_policy,
                freshness_policy=freshness_policy,
                partitions_def=partitions_def,
            )
            def _route(context: dg.AssetExecutionContext, **kwargs) -> pd.DataFrame:
                df = kwargs.get("df")
                if df is None:
                    df = _ingest_warehouse_query(_self.source, context)
                out = df.query(condition) if condition else df
                context.add_output_metadata({
                    "dagster/row_count": dg.MetadataValue.int(len(out)),
                    "condition": dg.MetadataValue.text(condition),
                })
                return out
            return _route

        assets = []
        if _self.exclusive:
            # Each row goes to exactly one — use a chained-mask helper.
            def _make_exclusive(name, condition, prior_conditions):
                @dg.asset(
                    name=name,
                    description=f"router branch (exclusive): {condition}",
                    group_name=_self.group_name,
                    kinds=set(_self.kinds or ["router"]),
                    ins=({"df": dg.AssetIn(key=dg.AssetKey.from_user_string(upstream_key))} if upstream_key else None),
                    required_resource_keys=({_self.source["resource_key"]} if (_self.source and _self.source.get("resource_key")) else None),
                )
                def _route(context: dg.AssetExecutionContext, **kwargs) -> pd.DataFrame:
                    df = kwargs.get("df")
                    if df is None:
                        df = _ingest_warehouse_query(_self.source, context)
                    mask = pd.Series([True] * len(df), index=df.index)
                    for prior in prior_conditions:
                        mask = mask & ~df.eval(prior).fillna(False)
                    if condition:
                        mask = mask & df.eval(condition).fillna(False)
                    out = df[mask]
                    context.add_output_metadata({
                        "dagster/row_count": dg.MetadataValue.int(len(out)),
                        "condition": dg.MetadataValue.text(condition or "(default)"),
                    })
                    return out
                return _route
            prior = []
            for r in _self.routes:
                assets.append(_make_exclusive(r["asset_name"], r["condition"], list(prior)))
                prior.append(r["condition"])
            if _self.default_asset_name:
                assets.append(_make_exclusive(_self.default_asset_name, None, list(prior)))
        else:
            for r in _self.routes:
                assets.append(_make_route(r["asset_name"], r["condition"]))

        return dg.Definitions(assets=assets)
