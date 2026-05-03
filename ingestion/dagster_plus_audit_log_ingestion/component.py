"""DagsterPlusAuditLogIngestionComponent.

Pull Dagster+ audit-log entries (logins, deployment changes, permission edits)
via the Dagster+ GraphQL API.

Authentication
--------------
Reads `Dagster-Cloud-User-Token` from an env var (default: DAGSTER_PLUS_USER_TOKEN).
Generate one at <https://your-org.dagster.cloud/<deployment>/settings/tokens>.

GraphQL endpoint
----------------
- US:  ``https://<org>.dagster.cloud/<deployment>/graphql``
- EU:  ``https://<org>.eu.dagster.cloud/<deployment>/graphql``

Query shape (audit log)
-----------------------
The API uses **cursor-based pagination** with a `filters` object:

    {
      "limit": 100,
      "cursor": null,
      "filters": {
        "eventTypes": ["LOG_IN"],
        "userEmails": ["user@example.com"],
        "deploymentNames": ["prod"]
      }
    }

There is no `startTime`/`endTime` server-side filter. To bound the fetch, set
`lookback_minutes` — pagination stops once timestamps fall outside the window.

The default `entries { ... }` selection is a best-guess set of fields and
should be validated against your live GraphQL schema. Override `query`
entirely if you need different fields, or pass extra `variables`.
"""

import json
import os
from typing import Optional

import dagster as dg
import pandas as pd
from pydantic import Field


DEFAULT_QUERY = """query DagsterPlusAuditLogs($limit: Int, $cursor: String, $filters: AuditLogFilters) {
  auditLogs(limit: $limit, cursor: $cursor, filters: $filters) {
    entries {
      timestamp
      userEmail
      eventType
      targetType
      targetIdentifier
      metadata
    }
    cursor
    hasMore
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
        description="Env var holding the Dagster+ user token",
    )

    # Filters — match the live AuditLogFilters input type
    event_types: Optional[list[str]] = Field(
        default=None,
        description="AuditLog event-type codes to include (e.g. ['LOG_IN', 'DEPLOYMENT_CREATED']). None = all.",
    )
    user_emails: Optional[list[str]] = Field(
        default=None,
        description="Restrict to events by these user emails. None = all users.",
    )
    deployment_names: Optional[list[str]] = Field(
        default=None,
        description="Restrict to events from these deployment names (e.g. ['prod']). None = all.",
    )

    page_size: int = Field(default=100, description="Per-page limit (max 100 per Dagster+ API)")
    lookback_minutes: Optional[int] = Field(
        default=60,
        description="Stop pagination once entry timestamps fall outside this window. None = drain all available history.",
    )

    # Escape hatches
    query: Optional[str] = Field(
        default=None,
        description="Override the default GraphQL query — useful when the audit-log schema diverges from the shipped default.",
    )
    extra_variables: Optional[dict] = Field(
        default=None,
        description="Extra GraphQL variables merged into the request (won't override limit/cursor/filters).",
    )
    result_path: str = Field(
        default="auditLogs.entries",
        description="Dot-path to the list of records inside the GraphQL response.",
    )
    cursor_path: str = Field(
        default="auditLogs.cursor",
        description="Dot-path to the next-page cursor.",
    )
    has_more_path: str = Field(
        default="auditLogs.hasMore",
        description="Dot-path to the boolean has-more flag.",
    )
    timestamp_field: str = Field(
        default="timestamp",
        description="Field on each entry used for the lookback_minutes cutoff. Numeric (epoch s/ms) or ISO-8601 string.",
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
            description=self.description or "Dagster+ audit-log events",
            group_name=self.group_name,
            kinds=set(self.kinds or ["dagster-plus", "audit", "security"]),
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
            owners=self.owners or None,
            tags=self.asset_tags or None,
            freshness_policy=freshness,
            retry_policy=retry,
        )
        def _asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            import datetime as dt

            import requests

            token = os.environ[_self.user_token_env]
            headers = {
                "Dagster-Cloud-User-Token": token,
                "Content-Type": "application/json",
            }
            query = _self.query or DEFAULT_QUERY

            # Build filters: only include keys the user set, so the server doesn't
            # interpret an empty list as "match nothing".
            filters: dict = {}
            if _self.event_types:
                filters["eventTypes"] = _self.event_types
            if _self.user_emails:
                filters["userEmails"] = _self.user_emails
            if _self.deployment_names:
                filters["deploymentNames"] = _self.deployment_names

            base_vars: dict = dict(_self.extra_variables or {})
            base_vars["limit"] = _self.page_size
            base_vars["filters"] = filters

            cutoff_epoch = None
            if _self.lookback_minutes:
                cutoff_epoch = (
                    dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=_self.lookback_minutes)
                ).timestamp()

            def _get(obj, path):
                if not path:
                    return None
                for key in path.split("."):
                    if isinstance(obj, dict):
                        obj = obj.get(key)
                    else:
                        return None
                    if obj is None:
                        return None
                return obj

            def _entry_epoch(entry: dict) -> Optional[float]:
                ts = entry.get(_self.timestamp_field)
                if ts is None:
                    return None
                if isinstance(ts, (int, float)):
                    # Heuristic: ms timestamps are > ~10^12; seconds are smaller.
                    return ts / 1000.0 if ts > 1e12 else float(ts)
                try:
                    parsed = dt.datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
                    if parsed.tzinfo is None:
                        parsed = parsed.replace(tzinfo=dt.timezone.utc)
                    return parsed.timestamp()
                except Exception:
                    return None

            all_records: list[dict] = []
            cursor = None
            page_count = 0
            stopped_early = False
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

                # Apply the time-window cutoff client-side; bail out as soon as we
                # see entries older than the cutoff (entries arrive newest-first).
                kept_in_page = 0
                for entry in page_records:
                    if cutoff_epoch is not None:
                        e = _entry_epoch(entry)
                        if e is not None and e < cutoff_epoch:
                            stopped_early = True
                            break
                    all_records.append(entry)
                    kept_in_page += 1

                next_cursor = _get(data, _self.cursor_path)
                has_more = _get(data, _self.has_more_path)
                if stopped_early or not next_cursor or not has_more:
                    break
                if page_count >= _self.max_pages:
                    context.log.warning(
                        f"hit max_pages={_self.max_pages}; stopping (set max_pages higher to fetch more)"
                    )
                    break
                cursor = next_cursor

            df = pd.DataFrame(all_records)
            metadata = {
                "dagster/row_count": dg.MetadataValue.int(len(df)),
                "endpoint": dg.MetadataValue.text(_self.endpoint_url),
                "pages_fetched": dg.MetadataValue.int(page_count),
                "filters": dg.MetadataValue.json(filters),
                "stopped_at_lookback_cutoff": dg.MetadataValue.bool(stopped_early),
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
