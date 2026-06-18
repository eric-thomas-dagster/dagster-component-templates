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
from typing import Any, Dict, List, Optional, Union

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


class DeltaIngestionComponent(Component, Model, Resolvable):
    """Read an existing Delta Lake table into a Dagster asset (pandas DataFrame).

    Example — S3:

        ```yaml
        type: dagster_component_templates.DeltaIngestionComponent
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

            freshness_policy = FreshnessPolicy(
                maximum_lag_minutes=self.freshness_max_lag_minutes,
                cron_schedule=self.freshness_cron,
            )

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
            key=AssetKey.from_user_string(asset_name),
            description=description,
            owners=self.owners or [],
            tags=all_tags,
            freshness_policy=freshness_policy,
            group_name=self.group_name,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
            partitions_def=partitions_def,
            retry_policy=retry_policy,
        )
        def delta_ingestion_asset(context: AssetExecutionContext):
            from deltalake import DeltaTable

            partition_key = context.partition_key if context.has_partition_key else None
            storage_opts = _resolve_env_vars(component.storage_options)

            dt_kwargs: Dict[str, Any] = {}
            if storage_opts:
                dt_kwargs["storage_options"] = storage_opts
            if component.version is not None:
                dt_kwargs["version"] = component.version

            dt = DeltaTable(component.table_uri, **dt_kwargs)

            if component.timestamp and component.version is None:
                dt.load_as_version(component.timestamp)  # accepts ISO timestamp

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

            df = dt.to_pandas(**read_kwargs)
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

        return Definitions(assets=[delta_ingestion_asset])
