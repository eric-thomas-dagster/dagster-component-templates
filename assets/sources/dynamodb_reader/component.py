"""DynamoDB Reader Component.

Scan an AWS DynamoDB table and return all items as a DataFrame.
Supports pagination, filtering, and GSI queries.
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
class DynamodbReaderComponent(Component, Model, Resolvable):
    """Component for reading items from an AWS DynamoDB table.

    Performs a full table scan with optional filtering and pagination,
    returning results as a DataFrame. Supports Global Secondary Indexes.

    Example:
        ```yaml
        type: dagster_component_templates.DynamodbReaderComponent
        attributes:
          asset_name: dynamo_orders
          table_name: orders
          aws_region: us-east-1
          limit: 1000
          group_name: sources
        ```
    """

    asset_name: str = Field(description="Name of the asset to create")
    table_name: str = Field(description="DynamoDB table name")
    aws_region: str = Field(
        default="us-east-1",
        description="AWS region where the DynamoDB table is located",
    )
    aws_access_key_env_var: Optional[str] = Field(
        default=None,
        description="Environment variable containing AWS access key ID (uses instance role if None)",
    )
    aws_secret_key_env_var: Optional[str] = Field(
        default=None,
        description="Environment variable containing AWS secret access key",
    )
    filter_expression: Optional[str] = Field(
        default=None,
        description="boto3 FilterExpression as string (e.g., 'Attr(\"status\").eq(\"active\")')",
    )
    limit: Optional[int] = Field(
        default=None,
        description="Maximum number of items to return (None = all items)",
    )
    index_name: Optional[str] = Field(
        default=None,
        description="Global Secondary Index (GSI) name for query operations",
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
        table_name = self.table_name
        aws_region = self.aws_region
        aws_access_key_env_var = self.aws_access_key_env_var
        aws_secret_key_env_var = self.aws_secret_key_env_var
        limit = self.limit
        index_name = self.index_name
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
            description=f"DynamoDB scan of table {table_name}",
            partitions_def=partitions_def,
            group_name=group_name,
            deps=[AssetKey.from_user_string(d) for d in (deps or [])],
        )
        def dynamodb_reader_asset(context: AssetExecutionContext) -> pd.DataFrame:
            # Log partition key for source components
            if context.has_partition_key:
                context.log.info(f"Running for partition: {context.partition_key}")
            """Scan DynamoDB table and return items as DataFrame."""
            try:
                import boto3
            except ImportError:
                raise ImportError("boto3 required: pip install boto3")

            kwargs = {"region_name": aws_region}
            if aws_access_key_env_var:
                kwargs["aws_access_key_id"] = os.environ[aws_access_key_env_var]
                kwargs["aws_secret_access_key"] = os.environ[aws_secret_key_env_var]

            dynamodb = boto3.resource("dynamodb", **kwargs)
            table = dynamodb.Table(table_name)

            scan_kwargs = {}
            if limit:
                scan_kwargs["Limit"] = limit
            if index_name:
                scan_kwargs["IndexName"] = index_name

            context.log.info(f"Scanning DynamoDB table {table_name}")

            items = []
            response = table.scan(**scan_kwargs)
            items.extend(response.get("Items", []))
            while "LastEvaluatedKey" in response and (not limit or len(items) < limit):
                scan_kwargs["ExclusiveStartKey"] = response["LastEvaluatedKey"]
                response = table.scan(**scan_kwargs)
                items.extend(response.get("Items", []))

            df = pd.DataFrame(items)
            context.log.info(f"Retrieved {len(df)} items from DynamoDB table {table_name}")
            context.add_output_metadata({
                "row_count": MetadataValue.int(len(df)),
                "table_name": MetadataValue.text(table_name),
            })
            return df

        return Definitions(assets=[dynamodb_reader_asset])
