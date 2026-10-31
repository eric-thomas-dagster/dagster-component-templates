"""BigQueryQueryAssetComponent — run a SQL query against BigQuery, return a DataFrame.

Lightweight, asset-shaped BigQuery query runner. Drop-in peer of
`duckdb_query_reader` (DuckDB) and `database_query` (generic SQL). For
the heavier "import every BigQuery entity as an asset" pattern, see
`google_bigquery`.

Auth via service-account JSON (standard `GOOGLE_APPLICATION_CREDENTIALS`
fallback or explicit `credentials_path`). Returns a pandas DataFrame
materialized with TableSchema metadata so downstream Dagster types and
column-lineage hooks pick it up automatically.
"""

import json
import os
from typing import Any, Dict, List, Optional

import pandas as pd

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
    partition_type, partition_start, partition_values,
    dynamic_partition_name, partition_dimensions,
    cron_schedule=None,
    cron_fmt="%Y-%m-%d",
    end_offset=0,
    timezone=None,
):
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )
    if partition_dimensions and partition_type:
        raise ValueError("Set either partition_type or partition_dimensions, not both.")

    def _build_axis(spec):
        t = spec.get("type")
        _end_offset = spec.get("end_offset", 0)
        _timezone = spec.get("timezone")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dim type={t!r} requires 'start'")
        if t == "daily":   return DailyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "weekly":  return WeeklyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "monthly": return MonthlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "hourly":  return HourlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "cron":
            from dagster import TimeWindowPartitionsDefinition
            if not spec.get("start"):
                raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
            if not spec.get("cron_schedule"):
                raise ValueError("partition dimension type='cron' requires 'cron_schedule'")
            return TimeWindowPartitionsDefinition(start=spec["start"], fmt=spec.get("fmt") or "%Y-%m-%d", cron_schedule=spec["cron_schedule"], end_offset=_end_offset, timezone=_timezone)
        if t == "static":
            vals = spec.get("values") or []
            if isinstance(vals, str):
                vals = [v.strip() for v in vals.split(",") if v.strip()]
            if not vals:
                raise ValueError("partition dim type='static' requires 'values'")
            return StaticPartitionsDefinition(list(vals))
        if t == "dynamic":
            name = spec.get("dynamic_partition_name") or spec.get("name")
            if not name:
                raise ValueError("partition dim type='dynamic' requires a name")
            return DynamicPartitionsDefinition(name=name)
        raise ValueError(f"unknown partition type: {t!r}")

    if partition_dimensions:
        if len(partition_dimensions) == 1:
            return _build_axis(partition_dimensions[0])
        axes = {d["name"]: _build_axis(d) for d in partition_dimensions}
        return MultiPartitionsDefinition(axes)

    if not partition_type:
        return None
    if isinstance(partition_values, (list, tuple)):
        _values = [str(v).strip() for v in partition_values if str(v).strip()]
    else:
        _values = [v.strip() for v in (str(partition_values) if partition_values else "").split(",") if v.strip()]
    if partition_type in ("daily", "weekly", "monthly", "hourly", "cron") and not partition_start:
        raise ValueError(f"partition_type={partition_type!r} requires partition_start")
    if partition_type == "daily":   return DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "weekly":  return WeeklyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "monthly": return MonthlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "hourly":  return HourlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "cron":
        from dagster import TimeWindowPartitionsDefinition
        if not cron_schedule:
            raise ValueError("partition_type='cron' requires cron_schedule, e.g. '0 0 * * 1-5' for weekdays only.")
        return TimeWindowPartitionsDefinition(start=partition_start, fmt=cron_fmt, cron_schedule=cron_schedule, end_offset=end_offset, timezone=timezone)
    if partition_type == "static":
        if not _values:
            raise ValueError("partition_type='static' requires partition_values")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError("partition_type='dynamic' requires dynamic_partition_name")
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    if partition_type == "multi":
        if not _values or not partition_start:
            raise ValueError("partition_type='multi' requires partition_start + partition_values")
        return MultiPartitionsDefinition({
            "date": DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone),
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")



# BEGIN field-shape normalization (inlined per component; keep copies identical — see tests/test_field_shapes.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def coerce_field_shapes(data, *, lists=(), csv=(), tags=()):
    """Accept the shapes people (and LLMs) naturally write for common boilerplate fields."""
    if not isinstance(data, dict):
        return data
    out = dict(data)
    for k in lists:  # List[str]: "a" / "a, b" -> ["a", "b"]; strip + drop empties
        v = out.get(k)
        if isinstance(v, str):
            v = v.split(",")
        if isinstance(v, (list, tuple)):
            out[k] = [x.strip() if isinstance(x, str) else x for x in v if not (isinstance(x, str) and not x.strip())]
    for k in csv:  # comma-separated str given as a list -> "a,b"
        v = out.get(k)
        if isinstance(v, (list, tuple, set)):
            out[k] = ",".join(str(x).strip() for x in v)
    for k in tags:  # Dict[str, str]: scalars -> str, ["k=v"] / ["k:v"] -> {k: v}
        v = out.get(k)
        if isinstance(v, (list, tuple)) and v and all(isinstance(x, str) and ("=" in x or ":" in x) for x in v):
            v = dict((p.strip() for p in (x.split("=", 1) if "=" in x else x.split(":", 1))) for x in v)
        if isinstance(v, dict):
            out[k] = {str(a).strip(): (str(b).lower() if isinstance(b, bool) else str(b)) for a, b in v.items() if b is not None}
    return out
# END field-shape normalization


class BigQueryQueryAssetComponent(Component, Model, Resolvable):
    """Run a SQL query against Google BigQuery and return a pandas DataFrame.

    Drop-in peer of `duckdb_query_reader` and `database_query`. For multi-
    asset "import every entity from BigQuery" use `google_bigquery`.
    """

    asset_name: str = Field(description="Output asset name.")

    # Auth
    credentials: Optional[Dict[str, Any]] = Field(default=None)
    credentials_path: Optional[str] = Field(
        default=None,
        description="Path to service-account JSON. Falls back to GOOGLE_APPLICATION_CREDENTIALS.",
    )

    # Project / location
    project_id: Optional[str] = Field(
        default=None,
        description="GCP project for the BQ job (the billing project). Defaults to the SA's project.",
    )
    location: Optional[str] = Field(
        default=None,
        description="BigQuery location, e.g. 'US', 'EU', 'us-central1'. Defaults to BQ auto-detect.",
    )

    # Query
    query: str = Field(
        description=(
            "SQL query to run. Supports Jinja-like {placeholders} pulled from "
            "query_params at submit time. For partitioned assets, you can "
            "reference {{ partition_key }} via your own query_params."
        ),
    )
    query_params: Dict[str, Any] = Field(
        default_factory=dict,
        description="Mapping substituted into {placeholder} segments of `query` before submission.",
    )
    use_legacy_sql: bool = Field(default=False, description="Use legacy SQL (default: standard SQL).")
    dry_run: bool = Field(
        default=False,
        description="If True, validate the query and return its byte estimate without scanning data.",
    )

    # Output controls
    max_rows: Optional[int] = Field(
        default=None,
        description="Optional hard cap on result rows fetched into the DataFrame.",
    )

    # Standard Dagster attrs
    description: Optional[str] = Field(default=None)
    group_name: Optional[str] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)
    tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)

    partition_type: Optional[str] = Field(default=None)

    partition_cron_schedule: Optional[str] = Field(
        default=None,
        description=(
            "Required when partition_type='cron'. A cron expression restricting the date axis to a "
            "custom cadence, e.g. '0 0 * * 1-5' for weekdays only (business-day cadence). Built on "
            "Dagster's TimeWindowPartitionsDefinition(cron_schedule=...)."
        ),
    )
    partition_cron_fmt: Optional[str] = Field(
        default="%Y-%m-%d",
        description="strftime format for partition keys produced by partition_type='cron'. Only change this for a sub-daily cron_schedule.",
    )
    partition_end_offset: int = Field(
        default=0,
        description=(
            "Passed through to the underlying Daily/Weekly/Monthly/Hourly/cron "
            "partitions definition. 0 (the Dagster default) means a time-based "
            "partition set only ever holds COMPLETED windows -- e.g. a daily set "
            "never includes today until tomorrow. Set to 1 to include the "
            "current, still-in-progress window."
        ),
    )
    partition_timezone: Optional[str] = Field(
        default=None,
        description="IANA timezone for the underlying Daily/Weekly/Monthly/Hourly/cron partitions definition (defaults to UTC when unset, matching Dagster's own default).",
    )
    partition_start: Optional[str] = Field(default=None)
    partition_values: Optional[Union[str, List[str]]] = Field(default=None)
    dynamic_partition_name: Optional[str] = Field(default=None)
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(default=None)

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

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('deps', 'owners'), csv=('partition_values',), tags=('tags',))

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy

            freshness_policy = _freshness_policy_compat(self.freshness_max_lag_minutes, self.freshness_cron)

        retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy

            retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        creds_dict = self.credentials
        if creds_dict is None:
            cred_path = self.credentials_path or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
            if cred_path:
                with open(cred_path, "r") as fh:
                    creds_dict = json.load(fh)
        if creds_dict is None:
            raise ValueError(
                "BigQueryQueryAssetComponent: provide one of `credentials`, "
                "`credentials_path`, or set GOOGLE_APPLICATION_CREDENTIALS."
            )

        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values,
            self.dynamic_partition_name, self.partition_dimensions,
        
    cron_schedule=self.partition_cron_schedule,
    cron_fmt=self.partition_cron_fmt or "%Y-%m-%d", end_offset=self.partition_end_offset, timezone=self.partition_timezone,
)

        asset_name = self.asset_name
        project_id = self.project_id or creds_dict.get("project_id")
        location = self.location
        query_template = self.query
        query_params = self.query_params or {}
        use_legacy_sql = self.use_legacy_sql
        dry_run = self.dry_run
        max_rows = self.max_rows

        @asset(
            key=AssetKey.from_user_string(asset_name),
            description=self.description or f"BigQuery query result from project {project_id}.",
            group_name=self.group_name,
            kinds={"bigquery", "sql"},
            tags=self.tags or None,
            owners=self.owners or None,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])] or None,
            partitions_def=partitions_def,
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
        )
        def _asset(context: AssetExecutionContext):
            try:
                from google.cloud import bigquery
                from google.oauth2 import service_account
            except ImportError:
                raise ImportError(
                    "Install google-cloud-bigquery + google-auth: "
                    "pip install google-cloud-bigquery google-auth"
                )

            sa_creds = service_account.Credentials.from_service_account_info(creds_dict)
            client = bigquery.Client(
                credentials=sa_creds, project=project_id, location=location,
            )

            # Render placeholder substitutions. Plus a context-aware
            # default for `partition_key` if the asset is partitioned.
            params = dict(query_params)
            if context.has_partition_key:
                params.setdefault("partition_key", context.partition_key)
            try:
                rendered_query = query_template.format(**params) if params else query_template
            except KeyError as e:
                raise ValueError(
                    f"query references missing placeholder {e}; "
                    f"available query_params: {list(params.keys())}"
                )

            context.log.info(f"BigQuery: project={project_id}, location={location or '<auto>'}, dry_run={dry_run}")
            context.log.info(f"Query (first 200 chars): {rendered_query[:200]!r}")

            job_config = bigquery.QueryJobConfig(
                use_legacy_sql=use_legacy_sql,
                dry_run=dry_run,
                use_query_cache=True,
            )

            try:
                job = client.query(rendered_query, job_config=job_config)
            except Exception as e:
                err_str = str(e)
                if "SERVICE_DISABLED" in err_str or ("403" in err_str and "bigquery" in err_str.lower()):
                    context.log.error(
                        "BigQuery API not enabled on the project. The error message "
                        "above includes the activation URL — click + Enable, ~30s."
                    )
                elif "403" in err_str:
                    sa_email = creds_dict.get("client_email", "<sa-email>")
                    context.log.error(
                        f"BigQuery 403 PERMISSION_DENIED. The service account "
                        f"({sa_email!r}) needs at least roles/bigquery.dataViewer "
                        f"+ roles/bigquery.jobUser on the project, or roles/owner "
                        f"on the project. Grant at "
                        f"https://console.cloud.google.com/iam-admin/iam"
                    )
                raise

            if dry_run:
                bytes_processed = job.total_bytes_processed or 0
                return Output(
                    value=pd.DataFrame(),
                    metadata={
                        "dry_run":             MetadataValue.bool(True),
                        "bytes_processed":     MetadataValue.int(int(bytes_processed)),
                        "estimated_cost_usd":  MetadataValue.float(round(bytes_processed / (1024 ** 4) * 5.0, 4)),
                        "rendered_query":      MetadataValue.text(rendered_query),
                    },
                )

            df = job.to_dataframe(create_bqstorage_client=False)
            if max_rows is not None and len(df) > max_rows:
                df = df.head(max_rows)

            preview_md = df.head(10).to_markdown(index=False) if not df.empty else "(empty result)"
            md = {
                "row_count":       MetadataValue.int(len(df)),
                "column_count":    MetadataValue.int(len(df.columns)),
                "bytes_processed": MetadataValue.int(int(job.total_bytes_processed or 0)),
                "bytes_billed":    MetadataValue.int(int(job.total_bytes_billed or 0)),
                "cache_hit":       MetadataValue.bool(bool(job.cache_hit)),
                "slot_millis":     MetadataValue.int(int(job.slot_millis or 0)),
                "preview":         MetadataValue.md(preview_md or ""),
                "rendered_query":  MetadataValue.text(rendered_query),
            }
            return Output(value=df, metadata=md)

        return Definitions(assets=[_asset])


# BEGIN freshness-policy compat (inlined per component; keep copies identical, see tests/test_freshness_policy_compat.py)
def _freshness_policy_compat(max_lag_minutes, cron_schedule=None):
    """Build a FreshnessPolicy on the current Dagster API (time_window / cron) and on the older constructor."""
    from datetime import timedelta

    from dagster import FreshnessPolicy

    lag = timedelta(minutes=max_lag_minutes)
    if hasattr(FreshnessPolicy, "time_window"):
        if cron_schedule:
            return FreshnessPolicy.cron(deadline_cron=cron_schedule, lower_bound_delta=lag)
        return FreshnessPolicy.time_window(fail_window=lag)
    return FreshnessPolicy(maximum_lag_minutes=max_lag_minutes, cron_schedule=cron_schedule)
# END freshness-policy compat
