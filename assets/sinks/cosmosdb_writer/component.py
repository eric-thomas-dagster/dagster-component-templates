"""CosmosDB Writer Component.

Write a DataFrame to an Azure Cosmos DB container.
Supports upsert and insert modes.
"""

import os
from dataclasses import dataclass
from typing import Optional
import pandas as pd
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
    asset,
)
from pydantic import Field


@dataclass
class CosmosdbWriterComponent(Component, Model, Resolvable):
    """Component for writing a DataFrame to an Azure Cosmos DB container.

    Accepts an upstream DataFrame asset and writes each row as a Cosmos DB item,
    supporting upsert (create or replace) and insert modes.

    Example:
        ```yaml
        type: dagster_component_templates.CosmosdbWriterComponent
        attributes:
          asset_name: write_orders_to_cosmos
          upstream_asset_key: processed_orders
          database: ecommerce
          container: orders
          if_exists: upsert
          group_name: sinks
        ```
    """

    asset_name: str = Field(description="Name of the output asset to create")
    upstream_asset_key: str = Field(
        description="Asset key of the upstream DataFrame asset"
    )
    endpoint_env_var: str = Field(
        default="COSMOS_ENDPOINT",
        description="Environment variable containing the Cosmos DB endpoint URL",
    )
    key_env_var: str = Field(
        default="COSMOS_KEY",
        description="Environment variable containing the Cosmos DB primary key",
    )
    database: str = Field(description="Cosmos DB database name")
    container: str = Field(description="Cosmos DB container name")
    if_exists: str = Field(
        default="upsert",
        description="Write mode: 'upsert' (create or replace) or 'insert' (create only)",
    )
    group_name: Optional[str] = Field(
        default=None,
        description="Asset group for organization",
    )
    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', or None for unpartitioned",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.",
    )
    partition_date_column: Optional[str] = Field(
        default=None,
        description="Column used to filter upstream DataFrame to the current date partition key.",
    )
    partition_values: Optional[str] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning, e.g. 'customer_a,customer_b,customer_c'.",
    )
    partition_static_dim: Optional[str] = Field(
        default=None,
        description="Dimension name for the static axis in multi-partitioning, e.g. 'customer' or 'region'.",
    )
    partition_static_column: Optional[str] = Field(
        default=None,
        description="Column used to filter upstream DataFrame to the current static partition dimension (e.g. 'customer_id').",
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        endpoint_env_var = self.endpoint_env_var
        key_env_var = self.key_env_var
        database = self.database
        container = self.container
        if_exists = self.if_exists
        group_name = self.group_name

        # Build partition definition
        partitions_def = None
        if self.partition_type:
            from dagster import (
                DailyPartitionsDefinition, WeeklyPartitionsDefinition,
                MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
                StaticPartitionsDefinition, MultiPartitionsDefinition,
            )
            _start = self.partition_start or "2020-01-01"
            _values = [v.strip() for v in (self.partition_values or "").split(",") if v.strip()]
            if self.partition_type == "daily":
                partitions_def = DailyPartitionsDefinition(start_date=_start)
            elif self.partition_type == "weekly":
                partitions_def = WeeklyPartitionsDefinition(start_date=_start)
            elif self.partition_type == "monthly":
                partitions_def = MonthlyPartitionsDefinition(start_date=_start)
            elif self.partition_type == "hourly":
                partitions_def = HourlyPartitionsDefinition(start_date=_start)
            elif self.partition_type == "static":
                partitions_def = StaticPartitionsDefinition(_values)
            elif self.partition_type == "multi":
                _dim = self.partition_static_dim or "segment"
                partitions_def = MultiPartitionsDefinition({
                    "date": DailyPartitionsDefinition(start_date=_start),
                    _dim: StaticPartitionsDefinition(_values),
                })
        partition_type = self.partition_type
        partition_date_column = self.partition_date_column
        partition_static_column = self.partition_static_column
        partition_static_dim = self.partition_static_dim

        @asset(
            name=asset_name,
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
            partitions_def=partitions_def,
            group_name=group_name,
            description=f"Write DataFrame to Cosmos DB {database}.{container}",
        )
        def cosmosdb_writer_asset(
            context: AssetExecutionContext, upstream: pd.DataFrame
            # Filter to current partition if partitioned
            if context.has_partition_key:
                _pk = context.partition_key
                _is_multi = hasattr(_pk, "keys_by_dimension")
                _date_key = _pk.keys_by_dimension.get("date", "") if _is_multi else str(_pk)
                _static_key = _pk.keys_by_dimension.get(partition_static_dim or "segment", "") if _is_multi else None
                if partition_date_column and partition_date_column in upstream.columns and _date_key:
                    upstream = upstream[upstream[partition_date_column].astype(str) == _date_key]
                if partition_static_column and partition_static_column in upstream.columns and _static_key:
                    upstream = upstream[upstream[partition_static_column].astype(str) == _static_key]
                elif partition_static_column and partition_static_column in upstream.columns and not _is_multi:
                    upstream = upstream[upstream[partition_static_column].astype(str) == str(_pk)]
        ) -> MaterializeResult:
            """Write DataFrame rows to Cosmos DB container."""
            try:
                from azure.cosmos import CosmosClient
            except ImportError:
                raise ImportError("azure-cosmos required: pip install azure-cosmos")

            client = CosmosClient(os.environ[endpoint_env_var], os.environ[key_env_var])
            db = client.get_database_client(database)
            container_client = db.get_container_client(container)

            records = upstream.to_dict(orient="records")
            context.log.info(
                f"Writing {len(records)} items to Cosmos DB {database}.{container} (mode: {if_exists})"
            )

            for item in records:
                if if_exists == "upsert":
                    container_client.upsert_item(item)
                elif if_exists == "insert":
                    container_client.create_item(item)
                else:
                    raise ValueError(
                        f"Invalid if_exists='{if_exists}'. Use 'upsert' or 'insert'."
                    )

            context.log.info(f"Successfully wrote {len(records)} items to Cosmos DB {database}.{container}")
            return MaterializeResult(
                metadata={
                    "row_count": MetadataValue.int(len(upstream)),
                    "database": MetadataValue.text(database),
                    "container": MetadataValue.text(container),
                    "write_mode": MetadataValue.text(if_exists),
                }
            )

        return Definitions(assets=[cosmosdb_writer_asset])
