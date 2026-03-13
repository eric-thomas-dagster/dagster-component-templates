"""CosmosDB Reader Component.

Query items from an Azure Cosmos DB container using SQL-like syntax,
returning results as a DataFrame.
"""

import os
from dataclasses import dataclass
from typing import Optional, List
import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    MetadataValue,
    Model,
    Resolvable,
    asset,
)
from pydantic import Field


@dataclass
class CosmosdbReaderComponent(Component, Model, Resolvable):
    """Component for querying items from an Azure Cosmos DB container.

    Executes a SQL-like Cosmos DB query against a container,
    returning results as a DataFrame.

    Example:
        ```yaml
        type: dagster_component_templates.CosmosdbReaderComponent
        attributes:
          asset_name: cosmos_orders
          database: mydb
          container: orders
          query: "SELECT * FROM c WHERE c.status = 'active'"
          group_name: sources
        ```
    """

    asset_name: str = Field(description="Name of the asset to create")
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
    query: str = Field(
        default="SELECT * FROM c",
        description="SQL-like Cosmos DB query string",
    )
    max_items: Optional[int] = Field(
        default=None,
        description="Maximum number of items per page (None = use service default)",
    )
    deps: Optional[List[str]] = Field(
        default=None,
        description="Upstream asset keys for lineage",
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
        endpoint_env_var = self.endpoint_env_var
        key_env_var = self.key_env_var
        database = self.database
        container = self.container
        query = self.query
        max_items = self.max_items
        deps = self.deps
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
            description=f"Cosmos DB query on {database}.{container}",
            partitions_def=partitions_def,
            group_name=group_name,
            deps=[AssetKey.from_user_string(d) for d in (deps or [])],
        )
        def cosmosdb_reader_asset(context: AssetExecutionContext) -> pd.DataFrame:
            # Log partition key for source components
            if context.has_partition_key:
                context.log.info(f"Running for partition: {context.partition_key}")
            """Query Cosmos DB container and return items as DataFrame."""
            try:
                from azure.cosmos import CosmosClient
            except ImportError:
                raise ImportError("azure-cosmos required: pip install azure-cosmos")

            client = CosmosClient(os.environ[endpoint_env_var], os.environ[key_env_var])
            db = client.get_database_client(database)
            container_client = db.get_container_client(container)

            context.log.info(f"Querying Cosmos DB {database}.{container}: {query}")

            kwargs = {}
            if max_items:
                kwargs["max_item_count"] = max_items

            items = list(
                container_client.query_items(
                    query=query,
                    enable_cross_partition_query=True,
                    **kwargs,
                )
            )
            df = pd.DataFrame(items)

            context.log.info(f"Retrieved {len(df)} items from Cosmos DB {database}.{container}")
            context.add_output_metadata({
                "row_count": MetadataValue.int(len(df)),
                "database": MetadataValue.text(database),
                "container": MetadataValue.text(container),
            })
            return df

        return Definitions(assets=[cosmosdb_reader_asset])
