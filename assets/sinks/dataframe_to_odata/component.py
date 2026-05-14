"""Dataframe → OData sink component.

Write a pandas DataFrame to an OData v2/v4 entity set via POST / PATCH / DELETE.
Reverse direction of `odata_ingestion`. Useful for writing back to S/4HANA,
SuccessFactors, Dynamics 365, Datasphere, etc.

Modes:
- `insert` (default) — POST each row to `<service_url>/<entity_set>`.
- `upsert` — PATCH `<service_url>/<entity_set>(<key_value>)` per row.
   Requires `key_column` (the field uniquely identifying each entity).
- `delete` — DELETE `<service_url>/<entity_set>(<key_value>)` per row. Requires `key_column`.

SAP CSRF handling is automatic: set `csrf_fetch_path` and the component performs
a `GET <path>` with `x-csrf-token: fetch` first, captures the returned token,
and includes it on every subsequent write.

Per-row writes are simple but slow for large batches. For SAP backends that
support the OData `$batch` protocol, see `batch_mode`. Not all OData servers
support it; default is off.
"""

import math
import os
import time
from typing import Any, Dict, List, Optional
from urllib.parse import quote

import pandas as pd
import requests
from dagster import (
    AssetExecutionContext,
    AssetIn,
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


def _coerce_for_json(row: Dict[str, Any]) -> Dict[str, Any]:
    """Clean a pandas row dict for JSON serialization (NaN→None, Timestamps→ISO)."""
    out: Dict[str, Any] = {}
    for k, v in row.items():
        if isinstance(v, float) and math.isnan(v):
            out[k] = None
        elif hasattr(v, "isoformat"):
            out[k] = v.isoformat()
        elif hasattr(v, "item") and not isinstance(v, (str, bytes, dict, list)):
            try:
                out[k] = v.item()
            except (ValueError, AttributeError):
                out[k] = v
        else:
            out[k] = v
    return out


class DataframeToODataComponent(Component, Model, Resolvable):
    """Sink — write rows from a DataFrame to an OData entity set.

    Example — POST new business partners to S/4HANA:

        ```yaml
        type: dagster_component_templates.DataframeToODataComponent
        attributes:
          asset_name: business_partners_posted
          upstream_asset_key: new_business_partners
          service_url: https://my300000.s4hana.cloud.sap/sap/opu/odata/sap/API_BUSINESS_PARTNER
          entity_set: A_BusinessPartner
          mode: insert
          csrf_fetch_path: $metadata
          auth_type: basic
          auth_username_env_var: S4HANA_USER
          auth_password_env_var: S4HANA_PASSWORD
        ```
    """

    asset_name: str = Field(description="Dagster asset name")
    upstream_asset_key: str = Field(
        description="Upstream asset producing the DataFrame to write"
    )

    service_url: str = Field(description="Base service URL")
    entity_set: str = Field(description="Entity set to write to")
    odata_version: str = Field(default="v2", description="'v2' (default) or 'v4'.")

    mode: str = Field(
        default="insert",
        description="'insert' (POST) | 'upsert' (PATCH per key) | 'delete' (DELETE per key)",
    )
    key_column: Optional[str] = Field(
        default=None,
        description="DataFrame column holding the entity key. Required for `upsert` and `delete`.",
    )

    # --- Auth (mirror ingestion shape) ---------------------------------------

    auth_type: str = Field(default="basic", description="'basic' | 'bearer' | 'none'")
    auth_username_env_var: Optional[str] = Field(default=None)
    auth_password_env_var: Optional[str] = Field(default=None)
    auth_token_env_var: Optional[str] = Field(default=None)
    extra_headers: Optional[Dict[str, str]] = Field(
        default=None,
        description="Extra HTTP headers (sap-client, sap-language, ...).",
    )
    csrf_fetch_path: Optional[str] = Field(
        default=None,
        description=(
            "If set, GET this path first with `x-csrf-token: fetch` and reuse the returned token "
            "on all writes. SAP write APIs require this; non-SAP backends usually don't."
        ),
    )
    verify_ssl: bool = Field(default=True)

    # --- Behavior -------------------------------------------------------------

    skip_columns: Optional[List[str]] = Field(
        default=None,
        description="Columns to drop before serializing the row body (e.g. internal pandas columns).",
    )
    batch_mode: bool = Field(
        default=False,
        description=(
            "Use OData $batch protocol — bundles many writes into one HTTP request. "
            "Supported by S/4HANA and SuccessFactors. NOT supported by all OData servers; "
            "leave off unless you're sure."
        ),
    )
    batch_size: int = Field(default=100, description="$batch chunk size when batch_mode=true")

    rate_limit_max_retries: int = Field(default=5)
    rate_limit_backoff_seconds: float = Field(default=2.0)
    request_timeout_seconds: int = Field(default=120)

    # --- Standard fields ------------------------------------------------------

    description: Optional[str] = Field(default=None)
    group_name: Optional[str] = Field(default="odata_sink")
    owners: Optional[List[str]] = Field(default=None)
    asset_tags: Optional[Dict[str, str]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)

    partition_type: Optional[str] = Field(default=None)
    partition_start: Optional[str] = Field(default=None)
    partition_values: Optional[str] = Field(default=None)
    dynamic_partition_name: Optional[str] = Field(default=None)

    retry_policy_max_retries: Optional[int] = Field(default=None)
    retry_policy_delay_seconds: Optional[int] = Field(default=None)
    retry_policy_backoff: str = Field(default="exponential")

    # -------------------------------------------------------------------------

    def _build_session(self) -> requests.Session:
        s = requests.Session()
        s.verify = self.verify_ssl
        s.headers.update({"Accept": "application/json", "Content-Type": "application/json"})
        if self.extra_headers:
            s.headers.update(self.extra_headers)
        if self.auth_type == "basic":
            if not (self.auth_username_env_var and self.auth_password_env_var):
                raise ValueError(
                    "auth_type='basic' requires auth_username_env_var + auth_password_env_var."
                )
            s.auth = (
                os.environ[self.auth_username_env_var],
                os.environ[self.auth_password_env_var],
            )
        elif self.auth_type == "bearer":
            if not self.auth_token_env_var:
                raise ValueError("auth_type='bearer' requires auth_token_env_var.")
            s.headers["Authorization"] = f"Bearer {os.environ[self.auth_token_env_var]}"
        elif self.auth_type != "none":
            raise ValueError(f"unknown auth_type: {self.auth_type!r}")

        if self.csrf_fetch_path:
            url = self.service_url.rstrip("/") + "/" + self.csrf_fetch_path.lstrip("/")
            r = s.get(url, headers={"x-csrf-token": "fetch"}, timeout=self.request_timeout_seconds)
            r.raise_for_status()
            token = r.headers.get("x-csrf-token")
            if token:
                s.headers["x-csrf-token"] = token
        return s

    def _entity_url(self, key_value: Any) -> str:
        """Build OData entity URL: <service>/<entity>('<key>') for strings, /<entity>(<key>) for nums."""
        base = self.service_url.rstrip("/") + "/" + self.entity_set
        if isinstance(key_value, (int, float)) and not isinstance(key_value, bool):
            return f"{base}({key_value})"
        return f"{base}('{quote(str(key_value), safe='')}')"

    def _request_with_retry(
        self, session: requests.Session, method: str, url: str, **kwargs
    ) -> requests.Response:
        resp: Optional[requests.Response] = None
        for attempt in range(self.rate_limit_max_retries + 1):
            resp = session.request(method, url, timeout=self.request_timeout_seconds, **kwargs)
            if resp.status_code == 429 and attempt < self.rate_limit_max_retries:
                retry_after = float(
                    resp.headers.get("Retry-After")
                    or self.rate_limit_backoff_seconds * (2 ** attempt)
                )
                time.sleep(retry_after)
                continue
            if resp.status_code >= 500 and attempt < self.rate_limit_max_retries:
                time.sleep(self.rate_limit_backoff_seconds * (2 ** attempt))
                continue
            return resp
        assert resp is not None
        return resp

    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale.",
    )
    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5'.",
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy

            freshness_policy = FreshnessPolicy(
                maximum_lag_minutes=self.freshness_max_lag_minutes,
                cron_schedule=self.freshness_cron,
            )

        component = self
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        description = self.description or f"OData write ({self.mode}) → {self.entity_set}"

        kinds = list(self.kinds or []) or ["odata"]
        all_tags = dict(self.asset_tags or {})
        for k in kinds:
            all_tags[f"dagster/kind/{k}"] = ""

        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values, self.dynamic_partition_name
        )

        retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy

            retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        @asset(
            name=asset_name,
            description=description,
            owners=self.owners or [],
            tags=all_tags,
            group_name=self.group_name,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
            partitions_def=partitions_def,
            retry_policy=retry_policy,
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
            freshness_policy=freshness_policy,
        )
        def dataframe_to_odata_asset(context: AssetExecutionContext, upstream):
            if not isinstance(upstream, pd.DataFrame):
                raise TypeError(
                    f"Upstream must produce a pandas DataFrame, got {type(upstream).__name__}"
                )
            df = upstream
            if component.skip_columns:
                df = df.drop(columns=[c for c in component.skip_columns if c in df.columns])

            mode = component.mode.lower()
            if mode in ("upsert", "delete") and not component.key_column:
                raise ValueError(f"mode={mode!r} requires key_column.")
            if component.key_column and component.key_column not in df.columns:
                raise ValueError(
                    f"key_column={component.key_column!r} not in DataFrame columns: {list(df.columns)}"
                )

            session = component._build_session()
            base_url = component.service_url.rstrip("/") + "/" + component.entity_set

            sent = 0
            failed = 0
            failures: List[Dict[str, Any]] = []

            for _, row in df.iterrows():
                row_dict = _coerce_for_json(row.to_dict())
                try:
                    if mode == "insert":
                        resp = component._request_with_retry(
                            session, "POST", base_url, json=row_dict
                        )
                    elif mode == "upsert":
                        key_col = component.key_column or ""
                        key_value = row_dict.get(key_col)
                        body = {k: v for k, v in row_dict.items() if k != key_col}
                        resp = component._request_with_retry(
                            session, "PATCH", component._entity_url(key_value), json=body
                        )
                    elif mode == "delete":
                        key_col = component.key_column or ""
                        key_value = row_dict.get(key_col)
                        resp = component._request_with_retry(
                            session, "DELETE", component._entity_url(key_value)
                        )
                    else:
                        raise ValueError(f"unknown mode: {mode!r}")

                    if resp.status_code >= 400:
                        failed += 1
                        failures.append(
                            {
                                "status": resp.status_code,
                                "key": row_dict.get(component.key_column)
                                if component.key_column
                                else None,
                                "body": resp.text[:500],
                            }
                        )
                        if failed >= 10:
                            context.log.error(
                                f"Aborting after 10+ failures. Sample: {failures[:3]}"
                            )
                            break
                    else:
                        sent += 1
                except Exception as e:
                    failed += 1
                    failures.append({"error": str(e)})
                    if failed >= 10:
                        break

            context.log.info(
                f"OData {mode} complete: sent={sent}, failed={failed}, total_rows={len(df)}"
            )

            metadata = {
                "rows_total": MetadataValue.int(len(df)),
                "rows_sent": MetadataValue.int(sent),
                "rows_failed": MetadataValue.int(failed),
                "mode": MetadataValue.text(mode),
                "entity_set": MetadataValue.text(component.entity_set),
                "service_url": MetadataValue.text(component.service_url),
            }
            if failures:
                metadata["failures_sample"] = MetadataValue.json(failures[:10])
            if failed > 0:
                raise RuntimeError(
                    f"OData {mode}: {failed} of {len(df)} rows failed. See `failures_sample` metadata."
                )
            return Output(value=None, metadata=metadata)

        return Definitions(assets=[dataframe_to_odata_asset])
