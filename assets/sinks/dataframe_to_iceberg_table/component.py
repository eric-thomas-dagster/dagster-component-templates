"""Dataframe → Iceberg Table sink component.

Write a pandas DataFrame to an EXISTING Iceberg table (one that Dagster does
not own as an IO manager). Use this when the target table is shared across
engines — Snowflake / Trino / Spark / Flink / Databricks / etc. — and Dagster
is one of the writers.

Differs from `iceberg_io_manager` (which makes Dagster the OWNER of the table
via the official dagster-iceberg PyIceberg IO manager).

Modes:
- `append` — append rows; preserves existing data
- `overwrite` — replace all rows (or rows matching `overwrite_filter`)

PyIceberg's MERGE / upsert support is limited — use Spark / Trino / Snowflake
SQL for upserts. This component handles append + overwrite, which covers
the typical Dagster batch-pipeline output pattern.
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


class DataframeToIcebergTableComponent(Component, Model, Resolvable):
    """Sink — write a DataFrame to an existing Iceberg table.

    Example:

        ```yaml
        type: dagster_component_templates.DataframeToIcebergTableComponent
        attributes:
          asset_name: orders_iceberg_loaded
          upstream_asset_key: curated_orders
          catalog_type: rest
          catalog_properties:
            uri: https://catalog.example.com
            credential: "${CATALOG_CREDENTIAL}"
            warehouse: my_warehouse
          namespace: sales
          table_name: orders
          mode: append
        ```
    """

    asset_name: str = Field(description="Dagster asset name")
    upstream_asset_key: str = Field(
        description="Upstream asset producing the DataFrame to write"
    )

    # --- Catalog (mirrors iceberg_ingestion) --------------------------------

    catalog_type: str = Field(
        default="rest",
        description="'rest' (default) | 'glue' | 'hive' | 'hadoop' | 'sql'",
    )
    catalog_name: str = Field(default="default")
    catalog_properties: Optional[Dict[str, str]] = Field(
        default=None,
        description="Catalog-specific config. ${ENV_VAR} expansion supported.",
    )

    # --- Table identity ----------------------------------------------------

    namespace: str = Field(description="Iceberg namespace")
    table_name: str = Field(description="Iceberg table name")

    # --- Write options -----------------------------------------------------

    mode: str = Field(
        default="append",
        description="'append' | 'overwrite'. PyIceberg's MERGE/upsert is limited — use Spark/Trino for that.",
    )
    overwrite_filter: Optional[str] = Field(
        default=None,
        description=(
            "When mode='overwrite': Iceberg row filter that decides which existing rows are "
            "replaced. Without this, ALL rows are overwritten. Supports {partition_key}."
        ),
    )

    skip_columns: Optional[List[str]] = Field(
        default=None,
        description="Columns to drop from the DataFrame before writing (e.g. internal pandas cols).",
    )
    schema_evolution: bool = Field(
        default=False,
        description=(
            "Allow Iceberg schema evolution if the DataFrame has new columns the table lacks. "
            "Use cautiously — Iceberg writers may fail if the table schema is locked."
        ),
    )

    # --- Standard fields ---------------------------------------------------

    description: Optional[str] = Field(default=None)
    group_name: Optional[str] = Field(default="iceberg_sink")
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

    def _load_catalog(self):
        from pyiceberg.catalog import load_catalog

        props = _resolve_env_vars(self.catalog_properties)
        props["type"] = self.catalog_type
        return load_catalog(self.catalog_name, **props)

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
        description = self.description or f"Iceberg sink ({self.mode}) → {self.namespace}.{self.table_name}"

        kinds = list(self.kinds or []) or ["iceberg"]
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
        def dataframe_to_iceberg_table_asset(context: AssetExecutionContext, upstream):
            import pyarrow as pa

            if not isinstance(upstream, pd.DataFrame):
                raise TypeError(
                    f"Upstream must produce a pandas DataFrame, got {type(upstream).__name__}"
                )
            df = upstream
            if component.skip_columns:
                df = df.drop(columns=[c for c in component.skip_columns if c in df.columns])

            arrow_table = pa.Table.from_pandas(df)

            catalog = component._load_catalog()
            from pyiceberg.exceptions import NoSuchTableError, NoSuchNamespaceError
            try:
                table = catalog.load_table((component.namespace, component.table_name))
            except NoSuchTableError:
                try:
                    catalog.create_namespace(component.namespace)
                except (NoSuchNamespaceError, Exception):
                    pass
                table = catalog.create_table(
                    identifier=(component.namespace, component.table_name),
                    schema=arrow_table.schema,
                )

            partition_key = context.partition_key if context.has_partition_key else None

            mode = component.mode.lower()
            if mode == "append":
                table.append(arrow_table)
            elif mode == "overwrite":
                if component.overwrite_filter:
                    f = component.overwrite_filter
                    if partition_key is not None and "{partition_key}" in f:
                        f = f.replace("{partition_key}", partition_key)
                    table.overwrite(arrow_table, overwrite_filter=f)
                else:
                    table.overwrite(arrow_table)
            else:
                raise ValueError(f"unknown mode: {mode!r} (expected 'append' or 'overwrite')")

            current_snap = table.current_snapshot()
            metadata: Dict[str, Any] = {
                "rows_written": MetadataValue.int(len(df)),
                "mode": MetadataValue.text(mode),
                "table": MetadataValue.text(f"{component.namespace}.{component.table_name}"),
                "catalog_type": MetadataValue.text(component.catalog_type),
            }
            if current_snap is not None:
                metadata["snapshot_id"] = MetadataValue.int(current_snap.snapshot_id)
                metadata["snapshot_timestamp_ms"] = MetadataValue.int(current_snap.timestamp_ms)
            if partition_key:
                metadata["partition_key"] = MetadataValue.text(partition_key)
            context.log.info(
                f"Iceberg sink: wrote {len(df)} rows ({mode}) to "
                f"{component.namespace}.{component.table_name}"
            )
            return Output(value=None, metadata=metadata)

        return Definitions(assets=[dataframe_to_iceberg_table_asset])
