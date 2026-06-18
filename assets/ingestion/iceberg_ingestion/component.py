"""Iceberg Ingestion Component.

Read from an EXISTING external Apache Iceberg table — one that Dagster does not
own as an IO manager. The table may be written by Snowflake / Trino / Spark /
Flink / Databricks / etc., and you want a Dagster asset that materializes the
current snapshot (or a specific snapshot via time travel) as a DataFrame.

Differs from `iceberg_io_manager` (which makes Dagster the owner of the table
via the official `dagster_iceberg` PyIceberg IO manager). Use this component
when the table is shared across engines and Dagster is just one of the readers.

Catalogs supported (via `pyiceberg.catalog.load_catalog`):
- **REST** — Nessie, Polaris (Apache), Lakekeeper, Tabular, Unity Catalog,
  Snowflake managed catalog, S3 Tables
- **AWS Glue Data Catalog**
- **Hive Metastore**
- **Hadoop catalog** (filesystem path)
- **SQL catalog** (Postgres / MySQL / SQLite-backed metadata)

Auth follows the catalog's native mechanism — for REST: bearer token or
OAuth2 client_credentials; for Glue: AWS env / instance profile; for S3
underneath: standard fsspec storage credentials.
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
    """Allow values shaped ${ENV_VAR} to be expanded at runtime."""
    if not d:
        return {}
    out = {}
    for k, v in d.items():
        if isinstance(v, str) and v.startswith("${") and v.endswith("}"):
            out[k] = os.environ.get(v[2:-1], "")
        else:
            out[k] = v
    return out


class IcebergIngestionComponent(Component, Model, Resolvable):
    """Read an existing Iceberg table into a Dagster asset (pandas DataFrame).

    Example — read from a REST catalog (Nessie / Polaris / Lakekeeper):

        ```yaml
        type: dagster_component_templates.IcebergIngestionComponent
        attributes:
          asset_name: orders_iceberg
          catalog_type: rest
          catalog_properties:
            uri: https://catalog.example.com
            credential: "${CATALOG_CREDENTIAL}"   # 'client_id:client_secret' for OAuth
            warehouse: my_warehouse
          namespace: sales
          table_name: orders
          select_columns: [order_id, customer_id, amount, order_date]
          row_filter: order_date >= '2024-01-01'
        ```

    Example — Snowflake-managed Iceberg catalog:

        ```yaml
        attributes:
          catalog_type: rest
          catalog_properties:
            uri: https://abc-xy12345.snowflakecomputing.com/api/v2/catalogs/MY_CAT/iceberg
            credential: "${SNOWFLAKE_PAT}"
            warehouse: MY_WAREHOUSE
            scope: PRINCIPAL_ROLE:MY_ROLE
        ```

    Example — AWS Glue catalog:

        ```yaml
        attributes:
          catalog_type: glue
          catalog_properties:
            warehouse: s3://my-bucket/iceberg/
          namespace: my_glue_db
          table_name: orders
        ```
    """

    asset_name: str = Field(description="Dagster asset name")

    # --- Catalog ------------------------------------------------------------

    catalog_type: str = Field(
        default="rest",
        description=(
            "PyIceberg catalog type: 'rest' (REST API), 'glue' (AWS Glue), "
            "'hive' (Hive Metastore), 'hadoop' (filesystem), 'sql' (Postgres/MySQL/SQLite)."
        ),
    )
    catalog_name: str = Field(
        default="default",
        description="Local name for the catalog (used as PyIceberg's catalog identifier).",
    )
    catalog_properties: Optional[Dict[str, str]] = Field(
        default=None,
        description=(
            "Catalog-specific properties. Values shaped '${ENV_VAR}' are expanded at runtime. "
            "Common keys: uri, credential, warehouse, scope, token, region. See PyIceberg docs."
        ),
    )

    # --- Table identity -----------------------------------------------------

    namespace: str = Field(description="Iceberg namespace (database name)")
    table_name: str = Field(description="Iceberg table name")

    # --- Read options -------------------------------------------------------

    select_columns: Optional[List[Union[str, int]]] = Field(
        default=None, description="Projection. If unset, reads all columns."
    )
    row_filter: Optional[str] = Field(
        default=None,
        description="Iceberg expression for predicate pushdown, e.g. \"order_date >= '2024-01-01'\". Supports `{partition_key}`.",
    )
    snapshot_id: Optional[int] = Field(
        default=None,
        description="Time-travel: read a specific snapshot. Mutually exclusive with `as_of_timestamp_ms` and `branch`.",
    )
    as_of_timestamp_ms: Optional[int] = Field(
        default=None,
        description="Time-travel: read the snapshot active at this Unix-millis timestamp.",
    )
    branch: Optional[str] = Field(
        default=None, description="Read from a named branch (Iceberg branching, v2 tables)."
    )
    limit: Optional[int] = Field(
        default=None, description="Row limit (for development/testing)."
    )

    # --- Standard fields ----------------------------------------------------

    description: Optional[str] = Field(default=None)
    group_name: Optional[str] = Field(default="iceberg")
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

    def _load_catalog(self):
        from pyiceberg.catalog import load_catalog

        props = _resolve_env_vars(self.catalog_properties)
        props["type"] = self.catalog_type
        return load_catalog(self.catalog_name, **props)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        component = self
        asset_name = self.asset_name
        description = self.description or f"Iceberg ingestion ({self.namespace}.{self.table_name})"

        kinds = list(self.kinds or []) or ["iceberg"]
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
        def iceberg_ingestion_asset(context: AssetExecutionContext):
            partition_key = context.partition_key if context.has_partition_key else None
            catalog = component._load_catalog()

            table = catalog.load_table((component.namespace, component.table_name))

            # Optional: select a specific snapshot / timestamp / branch
            scan_kwargs: Dict[str, Any] = {}
            if component.snapshot_id is not None:
                scan_kwargs["snapshot_id"] = component.snapshot_id
            if component.row_filter:
                rf = component.row_filter
                if partition_key is not None and "{partition_key}" in rf:
                    rf = rf.replace("{partition_key}", partition_key)
                scan_kwargs["row_filter"] = rf
            if component.select_columns:
                scan_kwargs["selected_fields"] = tuple(component.select_columns)
            if component.limit is not None:
                scan_kwargs["limit"] = component.limit

            scan = table.scan(**scan_kwargs)

            # as_of_timestamp / branch are configured on the scan, not table
            if component.as_of_timestamp_ms is not None and not component.snapshot_id:
                scan = scan.use_ref(component.as_of_timestamp_ms)  # type: ignore[attr-defined]
            if component.branch:
                # Newer PyIceberg API
                if hasattr(scan, "use_ref"):
                    scan = scan.use_ref(component.branch)

            arrow_table = scan.to_arrow()
            df = arrow_table.to_pandas()
            context.log.info(
                f"Iceberg ingestion: {len(df)} rows × {len(df.columns)} cols from "
                f"{component.namespace}.{component.table_name}"
            )

            current_snap = table.current_snapshot()
            metadata: Dict[str, Any] = {
                "row_count": MetadataValue.int(len(df)),
                "column_count": MetadataValue.int(len(df.columns)),
                "columns": MetadataValue.json(list(df.columns)),
                "table": MetadataValue.text(f"{component.namespace}.{component.table_name}"),
                "catalog_type": MetadataValue.text(component.catalog_type),
            }
            if current_snap is not None:
                metadata["snapshot_id"] = MetadataValue.int(current_snap.snapshot_id)
                metadata["snapshot_timestamp_ms"] = MetadataValue.int(current_snap.timestamp_ms)
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

        return Definitions(assets=[iceberg_ingestion_asset])
