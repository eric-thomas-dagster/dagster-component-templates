"""DataFrame to Braze.

Reverse-ETL sink — batch-POST DataFrame rows to Braze's REST API for
customer activation flows (segment exports, custom-attribute sync,
re-engagement lists, etc.). Use for warehouse → Braze pushes where each
row is a user attribute update.

Two endpoints supported (pick via ``endpoint``):

- ``users_track`` (default) — POST to ``/users/track`` with per-row
  attribute updates. Each row is one user; column values become
  ``attributes`` or ``custom_attributes`` (nested) on that user's Braze
  profile. Braze batches up to 75 users per call.
- ``catalogs`` — POST rows to a Braze Catalog via ``/catalogs/{name}/items``.
  Batches up to 50 items per call per Braze's docs.

Braze's REST endpoint URL is region-specific (US-01, US-02, EU-01, etc.);
supply the correct one via ``rest_endpoint`` (usually a per-tenant
environment variable). API auth is a REST API key with the required
scope (``users.track`` or ``catalogs.<name>.update_items``).

Docs:
    - /users/track:  https://www.braze.com/docs/api/endpoints/user_data/post_user_track
    - /catalogs:     https://www.braze.com/docs/api/endpoints/catalogs
"""

import os
from typing import Any, Dict, List, Literal, Optional, Union

import pandas as pd
import requests
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    MaterializeResult,
    MetadataValue,
    Model,
    Resolvable,
    RetryPolicy,
    asset,
)
from pydantic import Field


class DataframeToBrazeComponent(Component, Model, Resolvable):
    """Push a DataFrame to Braze via the REST API (users/track or catalogs)."""

    asset_name: str = Field(description="Dagster asset name")
    upstream_asset_key: str = Field(
        description="Asset key of the upstream DataFrame to consume."
    )

    api_key_env_var: str = Field(
        default="BRAZE_API_KEY",
        description="Env var holding a Braze REST API key with the required scope.",
    )
    rest_endpoint: str = Field(
        description=(
            "Braze REST endpoint URL, region-specific — e.g. "
            "https://rest.iad-01.braze.com (US-01), "
            "https://rest.iad-02.braze.com (US-02), "
            "https://rest.fra-01.braze.com (EU-01). Look up in the Braze "
            "dashboard under Settings → REST API Keys."
        ),
    )

    endpoint: Literal["users_track", "catalogs"] = Field(
        default="users_track",
        description=(
            "Which Braze endpoint to POST to: 'users_track' (user attribute "
            "sync) or 'catalogs' (custom catalog item upsert)."
        ),
    )
    catalog_name: Optional[str] = Field(
        default=None,
        description=(
            "Required when endpoint='catalogs'. The Braze catalog name to "
            "upsert items into."
        ),
    )

    user_id_column: str = Field(
        default="external_id",
        description=(
            "For endpoint='users_track': column holding the user's "
            "external_id (Braze's stable user identifier). Falls back to "
            "'braze_id' if 'external_id' isn't in the DataFrame."
        ),
    )
    attribute_columns: Optional[List[str]] = Field(
        default=None,
        description=(
            "For endpoint='users_track': columns to send as top-level user "
            "attributes (email, first_name, etc.). Default: all columns "
            "except user_id_column and custom_attribute_columns."
        ),
    )
    custom_attribute_columns: Optional[List[str]] = Field(
        default=None,
        description=(
            "For endpoint='users_track': columns to send as custom "
            "attributes (nested under 'custom_attributes' on the user's "
            "Braze profile). Use for arbitrary custom fields Braze doesn't "
            "reserve as first-class."
        ),
    )
    item_id_column: str = Field(
        default="id",
        description=(
            "For endpoint='catalogs': column holding each catalog item's "
            "unique id. All other columns become the item's fields."
        ),
    )

    batch_size: Optional[int] = Field(
        default=None,
        description=(
            "Rows per HTTP request. Default = 75 for users_track (Braze's "
            "max) or 50 for catalogs (Braze's max). Lower values reduce "
            "rate-limit pressure at the cost of more requests."
        ),
    )
    request_timeout_seconds: int = Field(
        default=30,
        description="Per-request HTTP timeout in seconds.",
    )
    dry_run: bool = Field(
        default=False,
        description=(
            "When True, build the payload and log it but skip the HTTP POST. "
            "Useful for validating field mappings without hitting Braze."
        ),
    )

    group_name: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    asset_tags: Optional[Dict[str, str]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)

    retry_policy_max_retries: Optional[int] = Field(
        default=None,
        description="Max retries on asset failure (rate limits, transient network errors).",
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

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
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
                    raise ValueError("partition_type='static' requires non-empty partition_values.")
                partitions_def = StaticPartitionsDefinition(_values)
            elif _pt == "dynamic":
                if not self.dynamic_partition_name:
                    raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
                partitions_def = DynamicPartitionsDefinition(name=self.dynamic_partition_name)
            else:
                raise ValueError(f"Unknown partition_type: {_pt!r}")

        if self.endpoint == "catalogs" and not self.catalog_name:
            raise ValueError("endpoint='catalogs' requires catalog_name.")

        retry_policy = None
        if self.retry_policy_max_retries is not None:
            retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=self.retry_policy_backoff,  # type: ignore[arg-type]
            )

        cfg = self
        upstream_key = AssetKey(cfg.upstream_asset_key.split("/"))

        @asset(
            name=cfg.asset_name,
            ins={"upstream": AssetIn(key=upstream_key)},
            group_name=cfg.group_name,
            description=cfg.description,
            deps=[AssetKey(d.split("/")) for d in (cfg.deps or [])],
            owners=cfg.owners,
            tags=cfg.asset_tags,
            kinds=cfg.kinds,
            retry_policy=retry_policy,
            partitions_def=partitions_def,
        )
        def braze_export(context: AssetExecutionContext, upstream: pd.DataFrame) -> MaterializeResult:
            if not isinstance(upstream, pd.DataFrame):
                raise TypeError(f"upstream must be a DataFrame, got {type(upstream).__name__}")

            token = os.environ.get(cfg.api_key_env_var)
            if not token and not cfg.dry_run:
                raise RuntimeError(
                    f"env var {cfg.api_key_env_var!r} is empty or unset — set your Braze REST API key."
                )

            base_url = cfg.rest_endpoint.rstrip("/")
            if cfg.endpoint == "users_track":
                url = f"{base_url}/users/track"
                default_batch = 75
            else:  # catalogs
                url = f"{base_url}/catalogs/{cfg.catalog_name}/items"
                default_batch = 50
            batch_size = cfg.batch_size or default_batch

            headers = {
                "Authorization": f"Bearer {token or 'DRY_RUN'}",
                "Content-Type": "application/json",
            }

            records = upstream.to_dict(orient="records")
            total = len(records)
            sent = 0
            failed = 0
            batches = 0

            for start in range(0, total, batch_size):
                chunk = records[start : start + batch_size]
                if cfg.endpoint == "users_track":
                    payload = _build_users_track_payload(chunk, cfg)
                else:
                    payload = _build_catalog_items_payload(chunk, cfg)

                if cfg.dry_run:
                    context.log.info(
                        f"[dry_run] Would POST {len(chunk)} rows to {url}. "
                        f"First payload key preview: {list(payload)[0] if payload else 'empty'}"
                    )
                    sent += len(chunk)
                    batches += 1
                    continue

                resp = requests.post(
                    url, json=payload, headers=headers, timeout=cfg.request_timeout_seconds,
                )
                batches += 1
                if 200 <= resp.status_code < 300:
                    sent += len(chunk)
                else:
                    failed += len(chunk)
                    body = (resp.text or "")[:400]
                    context.log.warning(
                        f"Braze POST failed: HTTP {resp.status_code} body={body}"
                    )

            context.log.info(
                f"Braze export ({cfg.endpoint}): {sent}/{total} rows in {batches} batches"
                f"{' — dry_run' if cfg.dry_run else ''}"
            )
            return MaterializeResult(
                metadata={
                    "rows_total": MetadataValue.int(total),
                    "rows_sent": MetadataValue.int(sent),
                    "rows_failed": MetadataValue.int(failed),
                    "batches": MetadataValue.int(batches),
                    "endpoint": MetadataValue.text(cfg.endpoint),
                    "rest_endpoint": MetadataValue.text(cfg.rest_endpoint),
                    "batch_size": MetadataValue.int(batch_size),
                    "dry_run": MetadataValue.bool(cfg.dry_run),
                }
            )

        return Definitions(assets=[braze_export])


def _pick_user_id(row: Dict[str, Any], cfg: DataframeToBrazeComponent) -> Optional[str]:
    """Braze accepts external_id (customer's own id) OR braze_id (Braze's).
    Prefer the configured user_id_column; fall back to the other well-known
    key so imports don't silently drop rows because of a naming mismatch."""
    val = row.get(cfg.user_id_column)
    if val is None or (isinstance(val, float) and pd.isna(val)):
        # try the OTHER Braze-known id column
        other = "braze_id" if cfg.user_id_column != "braze_id" else "external_id"
        val = row.get(other)
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    return str(val)


def _build_users_track_payload(
    chunk: List[Dict[str, Any]], cfg: DataframeToBrazeComponent
) -> Dict[str, Any]:
    """Shape the /users/track request body: {"attributes": [...]}."""
    all_cols: set = set()
    for r in chunk:
        all_cols.update(r.keys())
    id_col = cfg.user_id_column
    custom_set = set(cfg.custom_attribute_columns or [])
    if cfg.attribute_columns is not None:
        attr_cols = list(cfg.attribute_columns)
    else:
        attr_cols = [c for c in all_cols if c != id_col and c not in custom_set]

    users = []
    for row in chunk:
        uid = _pick_user_id(row, cfg)
        if not uid:
            continue
        entry: Dict[str, Any] = {"external_id" if cfg.user_id_column != "braze_id" else "braze_id": uid}
        for c in attr_cols:
            if c in row and row[c] is not None and not (isinstance(row[c], float) and pd.isna(row[c])):
                entry[c] = row[c]
        if custom_set:
            custom_attrs: Dict[str, Any] = {}
            for c in custom_set:
                if c in row and row[c] is not None and not (isinstance(row[c], float) and pd.isna(row[c])):
                    custom_attrs[c] = row[c]
            if custom_attrs:
                entry["custom_attributes"] = custom_attrs
        users.append(entry)

    return {"attributes": users}


def _build_catalog_items_payload(
    chunk: List[Dict[str, Any]], cfg: DataframeToBrazeComponent
) -> Dict[str, Any]:
    """Shape the /catalogs/{name}/items request body: {"items": [...]}.
    Each item must have an 'id' field; other columns become item fields."""
    items = []
    for row in chunk:
        raw_id = row.get(cfg.item_id_column)
        if raw_id is None or (isinstance(raw_id, float) and pd.isna(raw_id)):
            continue
        item: Dict[str, Any] = {"id": str(raw_id)}
        for c, v in row.items():
            if c == cfg.item_id_column:
                continue
            if v is None or (isinstance(v, float) and pd.isna(v)):
                continue
            item[c] = v
        items.append(item)
    return {"items": items}
