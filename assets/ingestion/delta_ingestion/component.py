"""Delta Lake Ingestion Component.

Read from an EXISTING external Delta Lake table — one written by Spark,
Databricks, Trino, Flink, Snowflake-via-Iceberg-uniform, or any other engine —
into a Dagster asset as a pandas DataFrame.

Uses `delta-rs` (the Rust implementation of the Delta protocol, exposed via
the `deltalake` Python package). Much lighter dependency than pyspark — no JVM,
no Spark runtime, just a single wheel.

Differs from the official `dagster_deltalake.DeltaLakePyarrowIOManager` (which
makes Dagster the OWNER of the table). Use this component when the table is
shared across engines and Dagster is just one reader.

Storage backends supported:
- **S3** (incl. Minio, R2 — via S3-compatible mode)
- **ADLS Gen2** (Azure)
- **GCS**
- **Local filesystem**
- **Unity Catalog** (Databricks — via catalog URI scheme)

Auth follows fsspec / object_store conventions: AWS env vars, AZURE_*, GCP ADC.
"""

import os
from typing import Any, Dict, List, Literal, Optional, Union

import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetKey,
    BackfillPolicy,
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
    partition_type, partition_start, partition_values, dynamic_partition_name,
    cron_schedule=None,
    cron_fmt="%Y-%m-%d",
    end_offset=0,
    timezone=None,
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
    if partition_type in ("daily", "weekly", "monthly", "hourly", "cron") and not partition_start:
        raise ValueError(f"partition_type={partition_type!r} requires partition_start.")
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "cron":
        from dagster import TimeWindowPartitionsDefinition
        if not cron_schedule:
            raise ValueError(
                "partition_type='cron' requires cron_schedule, e.g. '0 0 * * 1-5' for weekdays only."
            )
        return TimeWindowPartitionsDefinition(start=partition_start, fmt=cron_fmt, cron_schedule=cron_schedule, end_offset=end_offset, timezone=timezone)
    if partition_type == "static":
        if not _values:
            raise ValueError("partition_type='static' requires partition_values.")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    raise ValueError(f"unknown partition_type: {partition_type!r}")


def _resolve_env_vars(d: Optional[Dict[str, str]]) -> Dict[str, str]:
    if not d:
        return {}
    out = {}
    for k, v in d.items():
        if isinstance(v, str) and v.startswith("${") and v.endswith("}"):
            out[k] = os.environ.get(v[2:-1], "")
        else:
            out[k] = v
    return out



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


class DeltaIngestionComponent(Component, Model, Resolvable):
    """Read an existing Delta Lake table into a Dagster asset (pandas DataFrame).

    Example — S3:

        ```yaml
        type: dagster_community_components.DeltaIngestionComponent
        attributes:
          asset_name: events_delta
          table_uri: s3://my-bucket/lakehouse/events
          storage_options:
            AWS_REGION: us-east-1
            # AWS_ACCESS_KEY_ID + AWS_SECRET_ACCESS_KEY come from env
          select_columns: [event_id, user_id, event_type, event_timestamp]
          partition_filters:
            - [event_date, '=', '{partition_key}']
        ```

    Example — ADLS Gen2:

        ```yaml
        attributes:
          table_uri: az://my-container@my-account.dfs.core.windows.net/lakehouse/events
          storage_options:
            AZURE_STORAGE_ACCOUNT_NAME: my-account
            AZURE_STORAGE_KEY: "${AZURE_STORAGE_KEY}"
        ```

    Example — Unity Catalog (Databricks):

        ```yaml
        attributes:
          table_uri: uc://catalog.schema.table_name
          storage_options:
            UC_TOKEN: "${DATABRICKS_TOKEN}"
            UC_HOST: "https://my-workspace.cloud.databricks.com"
        ```
    """

    asset_name: str = Field(description="Dagster asset name")

    table_uri: str = Field(
        description=(
            "Delta table location. Examples: "
            "'s3://bucket/path', 'az://container@account.dfs.core.windows.net/path', "
            "'gs://bucket/path', '/local/path', or 'uc://catalog.schema.table' for Unity Catalog."
        ),
    )
    storage_options: Optional[Dict[str, str]] = Field(
        default=None,
        description=(
            "Cloud-storage credentials and config. Values shaped '${ENV_VAR}' are expanded. "
            "Keys are object_store's standard names: AWS_REGION, AWS_ACCESS_KEY_ID, "
            "AZURE_STORAGE_ACCOUNT_NAME, AZURE_STORAGE_KEY, GOOGLE_SERVICE_ACCOUNT_KEY, etc."
        ),
    )

    # --- Read options ------------------------------------------------------

    select_columns: Optional[List[Union[str, int]]] = Field(
        default=None, description="Projection. If unset, reads all columns."
    )
    partition_filters: Optional[List[List[str]]] = Field(
        default=None,
        description=(
            "List of [column, op, value] partition predicates pushed down to delta-rs. "
            "Example: [['event_date', '=', '{partition_key}']]. Supports `{partition_key}`."
        ),
    )

    # Time travel (one of)
    version: Optional[int] = Field(
        default=None, description="Time travel: read a specific Delta version."
    )
    timestamp: Optional[str] = Field(
        default=None,
        description="Time travel: read the version active at this ISO timestamp (e.g. '2024-01-15T00:00:00Z').",
    )

    # --- Standard fields ---------------------------------------------------

    description: Optional[str] = Field(default=None)
    group_name: Optional[str] = Field(default="delta")
    owners: Optional[List[str]] = Field(default=None)
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)
    freshness_max_lag_minutes: Optional[int] = Field(default=None)
    freshness_cron: Optional[str] = Field(default=None)
    include_preview_metadata: bool = Field(default=True)
    preview_rows: int = Field(default=25, ge=1, le=500)
    deps: Optional[List[str]] = Field(default=None)

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

    backfill_policy: Optional[Literal["single_run", "multi_run"]] = Field(
        default=None,
        description=(
            "Backfill execution strategy for a partitioned asset. 'single_run' "
            "materializes an entire backfill range in one run (one "
            "to_pandas() read per partition in the range, each with its own "
            "partition_filters substitution, concatenated together). "
            "'multi_run' launches one run per partition (optionally capped "
            "via backfill_policy_max_partitions_per_run). Leave unset for "
            "Dagster's default (one run per partition, uncapped). Ignored "
            "when partition_type is not set. 'single_run' requires a "
            "partition-range-aware IO manager (e.g. a DB-table IO manager "
            "with partition_expr set) -- a generic filesystem/default IO "
            "manager cannot persist a single_run output spanning multiple "
            "partitions and will raise a clear error at materialization time."
        ),
    )
    backfill_policy_max_partitions_per_run: Optional[int] = Field(
        default=None,
        description="For backfill_policy='multi_run' only: maximum partitions materialized per run. Leave unset for Dagster's default (1 partition per run).",
    )

    retry_policy_max_retries: Optional[int] = Field(default=None)
    retry_policy_delay_seconds: Optional[int] = Field(default=None)
    retry_policy_backoff: str = Field(default="exponential")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('owners', 'kinds', 'deps'), csv=('partition_values',), tags=('asset_tags',))

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        component = self
        asset_name = self.asset_name
        description = self.description or f"Delta ingestion ({self.table_uri})"

        kinds = list(self.kinds or []) or ["delta", "lakehouse"]
        all_tags = dict(self.asset_tags or {})
        for k in kinds:
            all_tags[f"dagster/kind/{k}"] = ""

        freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy

            freshness_policy = _freshness_policy_compat(self.freshness_max_lag_minutes, self.freshness_cron)

        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values, self.dynamic_partition_name
        ,
    cron_schedule=self.partition_cron_schedule,
    cron_fmt=self.partition_cron_fmt or "%Y-%m-%d", end_offset=self.partition_end_offset, timezone=self.partition_timezone,
)

        retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy

            retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        backfill_policy_obj = None
        if partitions_def is not None and component.backfill_policy == "single_run":
            backfill_policy_obj = BackfillPolicy.single_run()
        elif partitions_def is not None and component.backfill_policy == "multi_run":
            backfill_policy_obj = BackfillPolicy.multi_run(
                max_partitions_per_run=component.backfill_policy_max_partitions_per_run or 1
            )

        @asset(
            key=AssetKey.from_user_string(asset_name),
            description=description,
            owners=self.owners or [],
            tags=all_tags,
            freshness_policy=freshness_policy,
            group_name=self.group_name,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
            partitions_def=partitions_def,
            backfill_policy=backfill_policy_obj,
            retry_policy=retry_policy,
        )
        def delta_ingestion_asset(context: AssetExecutionContext):
            from deltalake import DeltaTable

            # Partition key(s) this call must process: a normal tick gives
            # exactly one; a single_run backfill gives the whole range.
            partition_keys: List[Optional[str]] = [None]
            if context.has_partition_key or context.has_partition_key_range:
                partition_keys = list(context.partition_keys)

            storage_opts = _resolve_env_vars(component.storage_options)

            dt_kwargs: Dict[str, Any] = {}
            if storage_opts:
                dt_kwargs["storage_options"] = storage_opts
            if component.version is not None:
                dt_kwargs["version"] = component.version

            dt = DeltaTable(component.table_uri, **dt_kwargs)

            if component.timestamp and component.version is None:
                dt.load_as_version(component.timestamp)  # accepts ISO timestamp

            def _read_one(partition_key: Optional[str]):
                # Substitute partition_key into filters
                pfilters = None
                if component.partition_filters:
                    pfilters = []
                    for f in component.partition_filters:
                        col, op, val = f[0], f[1], f[2]
                        if partition_key is not None and isinstance(val, str) and "{partition_key}" in val:
                            val = val.replace("{partition_key}", partition_key)
                        pfilters.append((col, op, val))

                read_kwargs: Dict[str, Any] = {}
                if component.select_columns:
                    read_kwargs["columns"] = list(component.select_columns)
                if pfilters:
                    read_kwargs["partitions"] = pfilters

                return dt.to_pandas(**read_kwargs)

            dfs = [_read_one(pk) for pk in partition_keys]
            df = pd.concat(dfs, ignore_index=True) if len(dfs) > 1 else dfs[0]

            context.log.info(
                f"Delta ingestion: {len(df)} rows × {len(df.columns)} cols from {component.table_uri}"
            )

            metadata: Dict[str, Any] = {
                "row_count": MetadataValue.int(len(df)),
                "column_count": MetadataValue.int(len(df.columns)),
                "columns": MetadataValue.json(list(df.columns)),
                "table_uri": MetadataValue.text(component.table_uri),
                "delta_version": MetadataValue.int(dt.version()),
            }
            try:
                history = dt.history(limit=1)
                if history:
                    metadata["latest_commit_time_ms"] = MetadataValue.int(
                        history[0].get("timestamp", 0)
                    )
                    metadata["latest_commit_operation"] = MetadataValue.text(
                        history[0].get("operation", "")
                    )
            except Exception:
                pass
            if partition_keys != [None]:
                metadata["partition_keys"] = MetadataValue.json(partition_keys)
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

        return Definitions(assets=[delta_ingestion_asset])


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
