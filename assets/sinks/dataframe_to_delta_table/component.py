"""Dataframe → Delta Table sink component.

Write a pandas DataFrame to an EXISTING Delta Lake table. Uses `delta-rs` (no
Spark / JVM). For Delta tables shared with other engines — Databricks, Trino,
Spark, Flink — where Dagster is one of the writers.

Differs from `dataframe_to_databricks` (Databricks-specific, Unity Catalog
focused) and from the official `dagster_deltalake.DeltaLakePyarrowIOManager`
(which OWNS the table as an IO manager).

Modes: `append`, `overwrite`, `merge` (basic MERGE — match-then-update by key).
For complex MERGE logic (when-matched-update / when-not-matched-insert /
when-matched-delete), use Spark/Databricks SQL.
"""

import os
from typing import Any, Dict, List, Optional

import pandas as pd
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


class DataframeToDeltaTableComponent(Component, Model, Resolvable):
    """Sink — write a DataFrame to an existing Delta Lake table.

    Example — append to S3-backed Delta table:

        ```yaml
        type: dagster_component_templates.DataframeToDeltaTableComponent
        attributes:
          asset_name: events_delta_loaded
          upstream_asset_key: curated_events
          table_uri: s3://my-bucket/lakehouse/events
          storage_options:
            AWS_REGION: us-east-1
          mode: append
          partition_by: [event_date]
        ```
    """

    asset_name: str = Field(description="Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset producing the DataFrame")

    table_uri: str = Field(description="Delta table location (s3://, az://, gs://, /local, uc://)")
    storage_options: Optional[Dict[str, str]] = Field(
        default=None,
        description="Cloud creds + config. ${ENV_VAR} expansion supported.",
    )

    mode: str = Field(
        default="append",
        description=(
            "'append' | 'overwrite' | 'merge' (basic) | 'error' | 'ignore'. "
            "For complex MERGE logic, use Spark/Databricks SQL."
        ),
    )

    # --- Partition / overwrite behavior -------------------------------------

    partition_by: Optional[List[str]] = Field(
        default=None,
        description="Partition columns. Set ONCE at table creation — must match the table's existing partitioning.",
    )
    overwrite_partition_filter: Optional[List[List[str]]] = Field(
        default=None,
        description=(
            "When mode='overwrite': replace only the rows matching these partition predicates "
            "(format same as delta_ingestion's partition_filters). Supports {partition_key}."
        ),
    )

    # --- Merge options -----------------------------------------------------

    merge_predicate: Optional[str] = Field(
        default=None,
        description=(
            "When mode='merge': SQL predicate matching target ↔ source rows by key, "
            "e.g. 'target.id = source.id'. Required for merge mode."
        ),
    )

    # --- Schema --------------------------------------------------------------

    schema_mode: Optional[str] = Field(
        default=None,
        description=(
            "Schema evolution. 'merge' allows new columns to be added; None enforces the "
            "existing schema. Use 'merge' cautiously — it can mask upstream bugs."
        ),
    )

    skip_columns: Optional[List[str]] = Field(default=None)

    # --- Standard fields ---------------------------------------------------

    description: Optional[str] = Field(default=None)
    group_name: Optional[str] = Field(default="delta_sink")
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
        description = self.description or f"Delta sink ({self.mode}) → {self.table_uri}"

        kinds = list(self.kinds or []) or ["delta"]
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
            key=AssetKey.from_user_string(asset_name),
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
        def dataframe_to_delta_table_asset(context: AssetExecutionContext, upstream):
            from deltalake import write_deltalake, DeltaTable

            if not isinstance(upstream, pd.DataFrame):
                raise TypeError(
                    f"Upstream must produce a pandas DataFrame, got {type(upstream).__name__}"
                )
            df = upstream
            if component.skip_columns:
                df = df.drop(columns=[c for c in component.skip_columns if c in df.columns])

            partition_key = context.partition_key if context.has_partition_key else None
            storage_opts = _resolve_env_vars(component.storage_options)

            mode = component.mode.lower()

            kwargs: Dict[str, Any] = {}
            if storage_opts:
                kwargs["storage_options"] = storage_opts
            if component.partition_by:
                kwargs["partition_by"] = list(component.partition_by)
            if component.schema_mode:
                kwargs["schema_mode"] = component.schema_mode

            if mode == "merge":
                if not component.merge_predicate:
                    raise ValueError("mode='merge' requires merge_predicate.")
                dt = DeltaTable(component.table_uri, storage_options=storage_opts or None)
                # delta-rs MERGE API: TableMerger.execute()
                merge = (
                    dt.merge(
                        source=df,
                        predicate=component.merge_predicate,
                        source_alias="source",
                        target_alias="target",
                    )
                    .when_matched_update_all()
                    .when_not_matched_insert_all()
                )
                merge.execute()
            elif mode == "overwrite" and component.overwrite_partition_filter:
                # Partition-level overwrite via predicate
                pfilters = []
                for f in component.overwrite_partition_filter:
                    col, op, val = f[0], f[1], f[2]
                    if partition_key is not None and isinstance(val, str) and "{partition_key}" in val:
                        val = val.replace("{partition_key}", partition_key)
                    pfilters.append((col, op, val))
                kwargs["mode"] = "overwrite"
                kwargs["partition_filters"] = pfilters
                write_deltalake(component.table_uri, df, **kwargs)
            elif mode in ("append", "overwrite", "error", "ignore"):
                kwargs["mode"] = mode
                write_deltalake(component.table_uri, df, **kwargs)
            else:
                raise ValueError(f"unknown mode: {mode!r}")

            # Read back the new version for metadata
            dt = DeltaTable(component.table_uri, storage_options=storage_opts or None)

            metadata: Dict[str, Any] = {
                "rows_written": MetadataValue.int(len(df)),
                "mode": MetadataValue.text(mode),
                "table_uri": MetadataValue.text(component.table_uri),
                "delta_version": MetadataValue.int(dt.version()),
            }
            if partition_key:
                metadata["partition_key"] = MetadataValue.text(partition_key)
            try:
                history = dt.history(limit=1)
                if history:
                    metadata["latest_commit_operation"] = MetadataValue.text(
                        history[0].get("operation", "")
                    )
            except Exception:
                pass
            context.log.info(
                f"Delta sink: wrote {len(df)} rows ({mode}) → {component.table_uri} (v{dt.version()})"
            )
            return Output(value=None, metadata=metadata)

        return Definitions(assets=[dataframe_to_delta_table_asset])
