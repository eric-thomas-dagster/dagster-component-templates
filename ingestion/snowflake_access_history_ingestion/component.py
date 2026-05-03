"""SnowflakeAccessHistoryIngestionComponent.

Pull Snowflake access history (every query, every column accessed) from the ACCOUNT_USAGE views.

Authentication: this component reads credentials from environment variables —
configure your tenant before running. See README.md for the full list.
"""

import json
import os
from typing import Optional

import dagster as dg
import pandas as pd
from pydantic import Field


class SnowflakeAccessHistoryIngestionComponent(dg.Component, dg.Model, dg.Resolvable):
    """Pull Snowflake access history (every query, every column accessed) from the ACCOUNT_USAGE views."""

    asset_name: str = Field(description="Dagster asset name")

    account: str = Field(description="Snowflake account identifier (e.g. 'acme-prod')")
    user: str = Field(description="Snowflake user")
    password_env: str = Field(default="SNOWFLAKE_PASSWORD")
    warehouse: Optional[str] = Field(default=None)
    role: Optional[str] = Field(default="ACCOUNTADMIN", description="Role with access to ACCOUNT_USAGE")
    lookback_hours: int = Field(default=24, description="How far back")
    table: str = Field(default="ACCESS_HISTORY", description="ACCESS_HISTORY | LOGIN_HISTORY | QUERY_HISTORY")

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

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
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
            name=self.asset_name,
            description=self.description or "Pull Snowflake access history (every query, every column accessed) from the ACCOUNT_USAGE views.",
            group_name=self.group_name,
            kinds=set(self.kinds or ['snowflake', 'audit', 'warehouse']),
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
            owners=self.owners or None,
            tags=self.asset_tags or None,
            freshness_policy=freshness,
            retry_policy=retry,
        )
        def _asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            df: pd.DataFrame
            import snowflake.connector
            import datetime as dt
            conn = snowflake.connector.connect(
                account=_self.account, user=_self.user,
                password=os.environ[_self.password_env],
                warehouse=_self.warehouse, role=_self.role,
            )
            try:
                end = dt.datetime.utcnow()
                start = end - dt.timedelta(hours=_self.lookback_hours)
                time_col = {
                    "ACCESS_HISTORY": "QUERY_START_TIME",
                    "LOGIN_HISTORY": "EVENT_TIMESTAMP",
                    "QUERY_HISTORY": "START_TIME",
                }[_self.table]
                sql = (
                    f"SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.{_self.table} "
                    f"WHERE {time_col} >= TO_TIMESTAMP_LTZ('{start.isoformat()}Z')"
                )
                df = pd.read_sql(sql, conn)
            finally:
                conn.close()
            metadata = {
                "dagster/row_count": dg.MetadataValue.int(len(df)),
            }
            if _self.include_preview_metadata and len(df) > 0:
                try:
                    sample = df.sample(min(_self.preview_rows, len(df))) if len(df) > _self.preview_rows * 10 else df.head(_self.preview_rows)
                    metadata["preview"] = dg.MetadataValue.md(sample.to_markdown(index=False))
                except Exception as exc:
                    context.log.warning(f"preview emission failed: {exc}")
            return dg.MaterializeResult(value=df, metadata=metadata)

        return dg.Definitions(assets=[_asset])
