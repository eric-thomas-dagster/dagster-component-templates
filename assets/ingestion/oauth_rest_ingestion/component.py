"""OAuth REST Ingestion Component.

Generic OAuth2-backed REST API → pandas DataFrame. Covers the non-OData side of
the SAP family (Concur, Ariba) and any other JSON REST API that takes a Bearer
token: GitHub Enterprise APIs, Atlassian Cloud, Datadog, PagerDuty, etc.

Difference from `rest_api_fetcher`:
- Built-in OAuth token refresh (paired with `oauth_token_resource`)
- Native pagination support (next_url / cursor / page / offset / none)
- Returns a flattened pandas DataFrame, not raw JSON

Headless-friendly: no interactive auth, no browser dance. Pair with
`oauth_token_resource` for token lifecycle, or pass a pre-minted bearer token
via `auth_token_env_var`.

Pagination patterns:

- **next_url**: response contains a URL to the next page.
  Example: Concur's `{Items: [...], NextPage: "https://..."}`.
  Config: `pagination: next_url`, `next_url_path: NextPage`.

- **cursor**: response contains an opaque cursor that you send in the next
  request as a query param. Example: Ariba `pageToken` / Slack `next_cursor`.
  Config: `pagination: cursor`, `cursor_response_path: nextPageToken`,
  `cursor_param: pageToken`.

- **page**: increment a `page` query param until the response is empty or
  contains `<N` results.
  Config: `pagination: page`, `page_param: page`, `page_start: 1`.

- **offset**: `offset` + `limit` query params, increment by page size.
  Config: `pagination: offset`, `offset_param: offset`, `limit_param: limit`,
  `page_size: 100`.

- **none**: single request.
  Config: `pagination: none` (default).
"""

import os
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
from dagster import (
    AssetExecutionContext,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    MetadataValue,
    Model,
    Output,
    Resolvable,
    asset,
)
from pydantic import Field


def _build_partitions_def(
    partition_type, partition_start, partition_values, dynamic_partition_name
):
    from dagster import (
        DailyPartitionsDefinition,
        WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition,
        HourlyPartitionsDefinition,
        StaticPartitionsDefinition,
        DynamicPartitionsDefinition,
    )

    if not partition_type:
        return None
    _values = (
        [str(v).strip() for v in partition_values if str(v).strip()]
        if isinstance(partition_values, (list, tuple))
        else [v.strip() for v in (str(partition_values) if partition_values else "").split(",") if v.strip()]
    )
    if partition_type in ("daily", "weekly", "monthly", "hourly") and not partition_start:
        raise ValueError(f"partition_type={partition_type!r} requires partition_start.")
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "static":
        if not _values:
            raise ValueError("partition_type='static' requires partition_values.")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    raise ValueError(f"unknown partition_type: {partition_type!r}")


def _extract_path(obj: Any, path: str) -> Any:
    """Navigate a JSON object via dotted path. Empty path returns obj."""
    if not path:
        return obj
    for part in path.split("."):
        if obj is None:
            return None
        if isinstance(obj, dict):
            obj = obj.get(part)
        elif isinstance(obj, list) and part.isdigit():
            i = int(part)
            obj = obj[i] if i < len(obj) else None
        else:
            return None
    return obj


class OAuthRestIngestionComponent(Component, Model, Resolvable):
    """OAuth2-backed REST GET → DataFrame with built-in pagination.

    Example — Ariba Operational Reporting:

        ```yaml
        type: dagster_component_templates.OAuthRestIngestionComponent
        attributes:
          asset_name: ariba_purchase_requisitions
          api_url: https://api.ariba.com/reporting/view/realm/MyRealm/RequisitionList
          oauth_token_resource_key: ariba_token
          pagination: cursor
          cursor_response_path: NextToken
          cursor_param: nextToken
          records_path: Records
          query_params:
            limit: "100"
        ```
    """

    asset_name: str = Field(description="Dagster asset name")

    api_url: str = Field(description="GET endpoint URL")
    query_params: Optional[Dict[str, str]] = Field(
        default=None,
        description="Initial query params. `{partition_key}` substitution supported.",
    )

    # --- Auth ---------------------------------------------------------------

    oauth_token_resource_key: Optional[str] = Field(
        default=None,
        description=(
            "Name of an `oauth_token_resource` registered in the project. The component "
            "calls `get_access_token()` on it for each request (cached + auto-refreshed). "
            "Prefer this over `auth_token_env_var` for headless rotation support."
        ),
    )
    auth_token_env_var: Optional[str] = Field(
        default=None,
        description="Env var with a pre-minted bearer token. Use when you don't need rotation.",
    )

    extra_headers: Optional[Dict[str, str]] = Field(default=None)

    # --- Pagination ----------------------------------------------------------

    pagination: str = Field(
        default="none",
        description="'none' | 'next_url' | 'cursor' | 'page' | 'offset'",
    )

    records_path: str = Field(
        default="",
        description=(
            "Dotted path to the records array in each response. "
            "Empty = whole body must be a list. Examples: `Items`, `data.results`."
        ),
    )

    # next_url pagination:
    next_url_path: str = Field(
        default="NextPage",
        description="Dotted path to the URL of the next page. Used when pagination='next_url'.",
    )

    # cursor pagination:
    cursor_response_path: str = Field(
        default="nextPageToken",
        description="Dotted path to the cursor in the response. Used when pagination='cursor'.",
    )
    cursor_param: str = Field(
        default="pageToken",
        description="Query-param name for sending the cursor. Used when pagination='cursor'.",
    )

    # page pagination:
    page_param: str = Field(default="page")
    page_start: int = Field(default=1)

    # offset pagination:
    offset_param: str = Field(default="offset")
    limit_param: str = Field(default="limit")
    page_size: int = Field(default=100)

    max_pages: int = Field(default=1000, description="Safety cap on pagination follow-throughs.")
    timeout_seconds: int = Field(default=120)
    verify_ssl: bool = Field(default=True)

    # --- Standard fields ----------------------------------------------------

    description: Optional[str] = Field(default=None)
    group_name: Optional[str] = Field(default="oauth_rest")
    owners: Optional[List[str]] = Field(default=None)
    asset_tags: Optional[Dict[str, str]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)
    freshness_max_lag_minutes: Optional[int] = Field(default=None)
    freshness_cron: Optional[str] = Field(default=None)
    include_preview_metadata: bool = Field(default=True)
    preview_rows: int = Field(default=25, ge=1, le=500)
    deps: Optional[List[str]] = Field(default=None)

    partition_type: Optional[str] = Field(default=None)
    partition_start: Optional[str] = Field(default=None)
    partition_values: Optional[str] = Field(default=None)
    dynamic_partition_name: Optional[str] = Field(default=None)

    retry_policy_max_retries: Optional[int] = Field(default=None)
    retry_policy_delay_seconds: Optional[int] = Field(default=None)
    retry_policy_backoff: str = Field(default="exponential")

    def _build_resource_keys(self) -> set:
        return (
            {self.oauth_token_resource_key} if self.oauth_token_resource_key else set()
        )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        component = self
        asset_name = self.asset_name
        description = self.description or f"OAuth REST ingestion ({asset_name})"

        kinds = list(self.kinds or []) or ["rest", "oauth"]
        all_tags = dict(self.asset_tags or {})
        for k in kinds:
            all_tags[f"dagster/kind/{k}"] = ""

        freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy

            freshness_policy = FreshnessPolicy(
                maximum_lag_minutes=self.freshness_max_lag_minutes,
                cron_schedule=self.freshness_cron,
            )

        partitions_def = _build_partitions_def(
            self.partition_type,
            self.partition_start,
            self.partition_values,
            self.dynamic_partition_name,
        )

        retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy

            retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        required_resource_keys = component._build_resource_keys()

        @asset(
            name=asset_name,
            description=description,
            owners=self.owners or [],
            tags=all_tags,
            freshness_policy=freshness_policy,
            group_name=self.group_name,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
            partitions_def=partitions_def,
            retry_policy=retry_policy,
            required_resource_keys=required_resource_keys,
        )
        def oauth_rest_ingestion_asset(context: AssetExecutionContext):
            partition_key = context.partition_key if context.has_partition_key else None

            # Build auth header
            def _auth_header() -> str:
                if component.oauth_token_resource_key:
                    res = getattr(context.resources, component.oauth_token_resource_key)
                    return res.get_authorization_header()
                if component.auth_token_env_var:
                    tok = os.environ.get(component.auth_token_env_var)
                    if not tok:
                        raise ValueError(
                            f"{component.auth_token_env_var!r} is set but empty."
                        )
                    return f"Bearer {tok}"
                raise ValueError(
                    "Either oauth_token_resource_key or auth_token_env_var must be set."
                )

            base_headers = {"Accept": "application/json"}
            if component.extra_headers:
                base_headers.update(component.extra_headers)

            base_params: Dict[str, str] = {}
            if component.query_params:
                for k, v in component.query_params.items():
                    if partition_key is not None and "{partition_key}" in v:
                        v = v.replace("{partition_key}", partition_key)
                    base_params[k] = v

            all_records: list = []
            pages = 0
            mode = component.pagination
            next_url: Optional[str] = component.api_url
            next_params = dict(base_params)
            cursor: Optional[str] = None
            page_num = component.page_start
            offset = 0

            while next_url and pages < component.max_pages:
                params = dict(next_params)
                if mode == "cursor" and cursor is not None:
                    params[component.cursor_param] = cursor
                elif mode == "page":
                    params[component.page_param] = str(page_num)
                elif mode == "offset":
                    params[component.offset_param] = str(offset)
                    params[component.limit_param] = str(component.page_size)

                headers = dict(base_headers)
                headers["Authorization"] = _auth_header()

                context.log.info(
                    f"OAuth REST GET page {pages + 1}: {next_url} params={list(params.keys())}"
                )
                resp = requests.get(
                    next_url,
                    params=params,
                    headers=headers,
                    timeout=component.timeout_seconds,
                    verify=component.verify_ssl,
                )
                if resp.status_code >= 400:
                    raise RuntimeError(
                        f"HTTP {resp.status_code} from {next_url}: {resp.text[:500]}"
                    )
                body = resp.json()

                page_records = _extract_path(body, component.records_path)
                if page_records is None:
                    page_records = []
                if not isinstance(page_records, list):
                    page_records = [page_records]
                all_records.extend(page_records)
                pages += 1

                if mode == "next_url":
                    nu = _extract_path(body, component.next_url_path)
                    next_url = nu if isinstance(nu, str) and nu else None
                    next_params = {}  # NextPage URL already encodes its own params
                elif mode == "cursor":
                    nc = _extract_path(body, component.cursor_response_path)
                    cursor = nc if isinstance(nc, str) and nc else None
                    next_url = component.api_url if cursor else None
                elif mode == "page":
                    if not page_records:
                        next_url = None
                    else:
                        page_num += 1
                elif mode == "offset":
                    if not page_records or len(page_records) < component.page_size:
                        next_url = None
                    else:
                        offset += len(page_records)
                else:
                    # 'none': single page
                    next_url = None

            if pages >= component.max_pages:
                context.log.warning(
                    f"Hit max_pages={component.max_pages}. Increase max_pages or add a tighter filter."
                )

            df = pd.json_normalize(all_records, sep="_") if all_records else pd.DataFrame()
            context.log.info(
                f"OAuth REST ingestion: {len(df)} rows × {len(df.columns)} cols in {pages} pages"
            )

            metadata: Dict[str, Any] = {
                "row_count": MetadataValue.int(len(df)),
                "column_count": MetadataValue.int(len(df.columns)),
                "pages_fetched": MetadataValue.int(pages),
                "api_url": MetadataValue.text(component.api_url),
                "pagination": MetadataValue.text(mode),
            }
            if partition_key:
                metadata["partition_key"] = MetadataValue.text(partition_key)
            if component.include_preview_metadata and len(df) > 0:
                try:
                    sample = (
                        df.sample(min(component.preview_rows, len(df)))
                        if len(df) > component.preview_rows * 10
                        else df.head(component.preview_rows)
                    )
                    metadata["preview"] = MetadataValue.md(sample.to_markdown(index=False))
                except Exception as e:
                    context.log.warning(f"preview emission failed: {e}")
            return Output(value=df, metadata=metadata)

        return Definitions(assets=[oauth_rest_ingestion_asset])
