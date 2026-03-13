"""DynamoDB Writer Component.

Write a DataFrame to an AWS DynamoDB table using batch writes for efficiency.
Converts float values to Decimal as required by DynamoDB.
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
class DynamodbWriterComponent(Component, Model, Resolvable):
    """Component for writing a DataFrame to an AWS DynamoDB table.

    Uses DynamoDB batch_writer for efficient bulk writes, automatically
    converting float values to Decimal as required by DynamoDB.

    Example:
        ```yaml
        type: dagster_component_templates.DynamodbWriterComponent
        attributes:
          asset_name: write_orders_to_dynamo
          upstream_asset_key: processed_orders
          table_name: orders
          aws_region: us-east-1
          group_name: sinks
        ```
    """

    asset_name: str = Field(description="Name of the output asset to create")
    upstream_asset_key: str = Field(
        description="Asset key of the upstream DataFrame asset"
    )
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
        table_name = self.table_name
        aws_region = self.aws_region
        aws_access_key_env_var = self.aws_access_key_env_var
        aws_secret_key_env_var = self.aws_secret_key_env_var
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
            description=f"Write DataFrame to DynamoDB table {table_name}",
        )
        def dynamodb_writer_asset(
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
            """Write DataFrame records to DynamoDB table using batch writes."""
            try:
                import boto3
                from decimal import Decimal
            except ImportError:
                raise ImportError("boto3 required: pip install boto3")

            kwargs = {"region_name": aws_region}
            if aws_access_key_env_var:
                kwargs["aws_access_key_id"] = os.environ[aws_access_key_env_var]
                kwargs["aws_secret_access_key"] = os.environ[aws_secret_key_env_var]

            dynamodb = boto3.resource("dynamodb", **kwargs)
            table = dynamodb.Table(table_name)
            records = upstream.to_dict(orient="records")

            def convert(v):
                if isinstance(v, float):
                    return Decimal(str(v))
                if isinstance(v, dict):
                    return {k: convert(val) for k, val in v.items()}
                return v

            context.log.info(
                f"Writing {len(records)} records to DynamoDB table {table_name}"
            )

            with table.batch_writer() as batch:
                for rec in records:
                    batch.put_item(
                        Item={k: convert(v) for k, v in rec.items() if v is not None}
                    )

            context.log.info(f"Successfully wrote {len(records)} records to DynamoDB table {table_name}")
            return MaterializeResult(
                metadata={
                    "row_count": MetadataValue.int(len(upstream)),
                    "table_name": MetadataValue.text(table_name),
                    "aws_region": MetadataValue.text(aws_region),
                }
            )

        return Definitions(assets=[dynamodb_writer_asset])
