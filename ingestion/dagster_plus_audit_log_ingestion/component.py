"""DagsterPlusAuditLogIngestionComponent.

Pull Dagster+ audit-log entries (logins, deployment changes, code-location
edits, permission changes, run launches, etc.) via the Dagster+ GraphQL API.

Authentication
--------------
Reads `Dagster-Cloud-Api-Token` header from an env var (default:
`DAGSTER_PLUS_USER_TOKEN`). Generate one in your Dagster+ deployment under
Settings → Tokens → User Tokens.

GraphQL endpoint
----------------
- US:  ``https://<org>.dagster.cloud/<deployment>/graphql``
- EU:  ``https://<org>.eu.dagster.cloud/<deployment>/graphql``

Query shape (audit log)
-----------------------
Validated against the live Dagster+ schema as of 2026-05. The query field is
`auditLog.auditLogEntries`, paginated via the `cursor` arg (no separate
cursor/hasMore on the response — the list returns ``[]`` once exhausted).

Required filter input: at least one of `afterDatetime` / `beforeDatetime`
(server returns Internal Server Error without a time bound). The component
sets `afterDatetime` from `lookback_minutes` automatically.

Available filter fields:
- `eventTypes: [AuditLogEventType]`
- `userEmails: [String]`
- `deploymentNames: [String]`
- `afterDatetime: Float` (epoch seconds)
- `beforeDatetime: Float` (epoch seconds)
- `isBranchDeployment: Boolean`

Available entry fields (from AuditLogEntry):
- `id`, `eventType`, `authorUserEmail`, `authorAgentTokenId`,
  `eventMetadata`, `timestamp`, `deploymentName`, `branchDeploymentName`
"""

import json
import os
from typing import Optional

import dagster as dg
import pandas as pd
from pydantic import Field


DEFAULT_QUERY = """query DagsterPlusAuditLogs($limit: Int, $cursor: String, $filters: AuditLogFilters) {
  auditLog {
    auditLogEntries(limit: $limit, cursor: $cursor, filters: $filters) {
      id
      eventType
      authorUserEmail
      authorAgentTokenId
      eventMetadata
      timestamp
      deploymentName
      branchDeploymentName
    }
  }
}"""


class DagsterPlusAuditLogIngestionComponent(dg.Component, dg.Model, dg.Resolvable):
    """Pull Dagster+ audit-log entries via the Dagster+ GraphQL API."""

    asset_name: str = Field(description="Dagster asset name")
    endpoint_url: str = Field(
        description="Dagster+ GraphQL endpoint — US: 'https://<org>.dagster.cloud/<deployment>/graphql', EU: 'https://<org>.eu.dagster.cloud/<deployment>/graphql'"
    )
    user_token_env: str = Field(
        default="DAGSTER_PLUS_USER_TOKEN",
        description="Env var holding the Dagster+ user token (sent as Dagster-Cloud-Api-Token header)",
    )

    # Filters — match the live AuditLogFilters input type
    event_types: Optional[list[str]] = Field(
        default=None,
        description="AuditLog event-type codes (e.g. ['LOG_IN', 'CREATE_CODE_LOCATION', 'CHANGE_USER_PERMISSIONS']). None = all.",
    )
    user_emails: Optional[list[str]] = Field(
        default=None,
        description="Restrict to events authored by these user emails. None = all users.",
    )
    deployment_names: Optional[list[str]] = Field(
        default=None,
        description="Restrict to events from these deployment names (e.g. ['prod']). None = all.",
    )
    is_branch_deployment: Optional[bool] = Field(
        default=None,
        description="True = only branch deployments; False = only non-branch; None = both.",
    )

    page_size: int = Field(default=100, description="Per-page limit (max 100 per Dagster+ API)")
    lookback_minutes: int = Field(
        default=60,
        description="Time window. Sets filters.afterDatetime server-side. The Dagster+ audit-log API requires at least one time bound, so this is effectively required.",
    )

    # Escape hatches
    query: Optional[str] = Field(
        default=None,
        description="Override the default GraphQL query — useful when the audit-log schema changes.",
    )
    extra_variables: Optional[dict] = Field(
        default=None,
        description="Extra GraphQL variables merged into the request (won't override limit/cursor/filters).",
    )
    result_path: str = Field(
        default="auditLog.auditLogEntries",
        description="Dot-path to the list of records inside the GraphQL response.",
    )

    description: Optional[str] = Field(default=None)
    group_name: str = Field(default="dagster_plus")
    deps: Optional[list[str]] = Field(default=None)
    owners: Optional[list[str]] = Field(default=None)
    asset_tags: Optional[dict] = Field(default=None)
    kinds: Optional[list[str]] = Field(default=None)
    freshness_max_lag_minutes: Optional[int] = Field(default=None)
    freshness_cron: Optional[str] = Field(default=None)
    retry_policy_max_retries: Optional[int] = Field(default=None)
    retry_policy_delay_seconds: Optional[int] = Field(default=None)
    retry_policy_backoff: str = Field(default="exponential")
    include_preview_metadata: bool = Field(default=True)
    preview_rows: int = Field(default=20)
    max_pages: int = Field(default=200, description="Safety cap on cursor pagination.")

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
            description=self.description or "Dagster+ audit-log events",
            group_name=self.group_name,
            kinds=set(self.kinds or ["dagster-plus", "audit", "security"]),
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
            owners=self.owners or None,
            tags=self.asset_tags or None,
            freshness_policy=freshness,
            retry_policy=retry,
            partitions_def=partitions_def,
        )
        def _asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            import datetime as dt
            import requests

            token = os.environ[_self.user_token_env]
            headers = {
                "Dagster-Cloud-Api-Token": token,
                "Content-Type": "application/json",
            }
            query = _self.query or DEFAULT_QUERY

            now = dt.datetime.now(dt.timezone.utc)
            after = now - dt.timedelta(minutes=_self.lookback_minutes)

            filters: dict = {
                "afterDatetime": after.timestamp(),
                "beforeDatetime": now.timestamp(),
            }
            if _self.event_types:
                filters["eventTypes"] = _self.event_types
            if _self.user_emails:
                filters["userEmails"] = _self.user_emails
            if _self.deployment_names:
                filters["deploymentNames"] = _self.deployment_names
            if _self.is_branch_deployment is not None:
                filters["isBranchDeployment"] = _self.is_branch_deployment

            base_vars: dict = dict(_self.extra_variables or {})
            base_vars["limit"] = _self.page_size
            base_vars["filters"] = filters

            def _get(obj, path):
                for key in path.split("."):
                    if isinstance(obj, dict):
                        obj = obj.get(key)
                    else:
                        return None
                    if obj is None:
                        return None
                return obj

            all_records: list[dict] = []
            cursor = None
            page_count = 0
            while True:
                page_count += 1
                vars_payload = dict(base_vars)
                vars_payload["cursor"] = cursor
                r = requests.post(
                    _self.endpoint_url,
                    json={"query": query, "variables": vars_payload},
                    headers=headers,
                    timeout=120,
                )
                r.raise_for_status()
                body = r.json()
                if "errors" in body:
                    raise Exception(f"Dagster+ GraphQL errors: {body['errors']}")
                data = body.get("data") or {}
                page_records = _get(data, _self.result_path) or []
                all_records.extend(page_records)
                # Stop when we got fewer than page_size — that's the last page.
                if len(page_records) < _self.page_size:
                    break
                if page_count >= _self.max_pages:
                    context.log.warning(
                        f"hit max_pages={_self.max_pages}; stopping (raise max_pages to fetch more)"
                    )
                    break
                # Cursor = id of last entry. Server uses it for the next page.
                last = page_records[-1]
                cursor = last.get("id") if isinstance(last, dict) else None
                if not cursor:
                    break

            df = pd.DataFrame(all_records)
            metadata = {
                "dagster/row_count": dg.MetadataValue.int(len(df)),
                "endpoint": dg.MetadataValue.text(_self.endpoint_url),
                "pages_fetched": dg.MetadataValue.int(page_count),
                "filters": dg.MetadataValue.json({k: v for k, v in filters.items() if k != "afterDatetime" and k != "beforeDatetime"}),
                "after_datetime": dg.MetadataValue.text(after.isoformat()),
                "before_datetime": dg.MetadataValue.text(now.isoformat()),
            }
            if _self.include_preview_metadata and len(df) > 0:
                try:
                    sample = (
                        df.sample(min(_self.preview_rows, len(df)))
                        if len(df) > _self.preview_rows * 10
                        else df.head(_self.preview_rows)
                    )
                    metadata["preview"] = dg.MetadataValue.md(sample.to_markdown(index=False))
                except Exception as exc:
                    context.log.warning(f"preview emission failed: {exc}")
            return dg.MaterializeResult(value=df, metadata=metadata)

        return dg.Definitions(assets=[_asset])
