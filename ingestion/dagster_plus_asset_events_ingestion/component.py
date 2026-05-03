"""DagsterPlusAssetEventsIngestionComponent.

Pull Dagster+ asset materialization / observation events via the GraphQL API.

Authentication
--------------
Reads `Dagster-Cloud-Api-Token` from an env var (default: DAGSTER_PLUS_USER_TOKEN).
Generate one at <https://your-org.dagster.cloud/<deployment>/settings/tokens>.

GraphQL endpoint
----------------
The default endpoint pattern is:

    https://{org}.dagster.cloud/{deployment}/graphql

Override via `endpoint_url` if you self-host the agent or use a custom deployment URL.

Query override
--------------
A best-guess GraphQL query is shipped in `default_query`. **Validate against your
actual Dagster+ schema** — the GraphQL field names may have evolved. Use the
`query` field to pass an exact query, and `result_path` to point at the list of
records to flatten.
"""

import json
import os
from typing import Optional

import dagster as dg
import pandas as pd
from pydantic import Field


DEFAULT_QUERY = """query AssetEvents($cursor: String, $limit: Int) {
  assetsOrError(cursor: $cursor, limit: $limit) {
    ... on AssetConnection {
      nodes {
        key { path }
        latestEventSortKey
        latestRunForPartition(partition: null) { runId status }
      }
    }
  }
}"""


class DagsterPlusAssetEventsIngestionComponent(dg.Component, dg.Model, dg.Resolvable):
    """Pull Dagster+ asset materialization / observation events via the GraphQL API."""

    asset_name: str = Field(description="Dagster asset name")
    endpoint_url: str = Field(description="Dagster+ GraphQL endpoint (e.g. 'https://acme.dagster.cloud/prod/graphql')")
    user_token_env: str = Field(default="DAGSTER_PLUS_USER_TOKEN", description="Env var holding Dagster+ user token")
    query: Optional[str] = Field(default=None, description="Override GraphQL query (defaults to a best-guess query — validate against your schema)")
    variables: Optional[dict] = Field(default=None, description="Extra GraphQL variables to pass through (merged with auto-paging vars)")
    result_path: str = Field(default='assetsOrError.nodes', description="Dot-path to the list of records inside the GraphQL response")
    cursor_path: Optional[str] = Field(default=None, description="Dot-path to the next-page cursor (None if not paginated)")
    has_more_path: Optional[str] = Field(default=None, description="Dot-path to the boolean has-more flag")
    page_size: int = Field(default=500, description="Per-page limit")
    lookback_minutes: Optional[int] = Field(default=60, description="Time window to fetch (None = unbounded)")
    description: Optional[str] = Field(default=None)
    group_name: str = Field(default="dagster_plus")
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
            description=self.description or "Pull Dagster+ asset materialization / observation events via the GraphQL API.",
            group_name=self.group_name,
            kinds=set(self.kinds or ['dagster-plus', 'assets', 'events']),
            owners=self.owners or None,
            tags=self.asset_tags or None,
            freshness_policy=freshness,
            retry_policy=retry,
        )
        def _asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            import requests, datetime as dt

            token = os.environ[_self.user_token_env]
            headers = {"Dagster-Cloud-Api-Token": token, "Content-Type": "application/json"}
            query = _self.query or DEFAULT_QUERY

            base_vars = dict(_self.variables or {})
            base_vars["limit"] = _self.page_size
            if _self.lookback_minutes:
                end = dt.datetime.utcnow()
                start = end - dt.timedelta(minutes=_self.lookback_minutes)
                # Dagster+ uses unix-seconds floats for time filters in many places
                base_vars.setdefault("startTime", start.timestamp())
                base_vars.setdefault("endTime", end.timestamp())

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

            all_records = []
            cursor = None
            page_count = 0
            while True:
                page_count += 1
                vars_payload = dict(base_vars)
                if cursor and _self.cursor_path:
                    vars_payload["cursor"] = cursor
                r = requests.post(_self.endpoint_url, json={"query": query, "variables": vars_payload}, headers=headers, timeout=120)
                r.raise_for_status()
                body = r.json()
                if "errors" in body:
                    raise Exception(f"Dagster+ GraphQL errors: {body['errors']}")
                data = body.get("data") or {}
                page_records = _get(data, _self.result_path) or []
                all_records.extend(page_records)
                next_cursor = _get(data, _self.cursor_path) if _self.cursor_path else None
                has_more = _get(data, _self.has_more_path) if _self.has_more_path else False
                if not _self.cursor_path or not next_cursor or not has_more:
                    break
                cursor = next_cursor
                if page_count > 100:
                    context.log.warning("hit 100-page safety limit; stopping")
                    break

            df = pd.DataFrame(all_records)
            metadata = {
                "dagster/row_count": dg.MetadataValue.int(len(df)),
                "endpoint": dg.MetadataValue.text(_self.endpoint_url),
                "pages_fetched": dg.MetadataValue.int(page_count),
            }
            if _self.include_preview_metadata and len(df) > 0:
                try:
                    sample = df.sample(min(_self.preview_rows, len(df))) if len(df) > _self.preview_rows * 10 else df.head(_self.preview_rows)
                    metadata["preview"] = dg.MetadataValue.md(sample.to_markdown(index=False))
                except Exception as exc:
                    context.log.warning(f"preview emission failed: {exc}")
            return dg.MaterializeResult(value=df, metadata=metadata)

        return dg.Definitions(assets=[_asset])
