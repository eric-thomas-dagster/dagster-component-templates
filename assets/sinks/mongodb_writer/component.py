"""MongoDB Writer Component.

Write a DataFrame to a MongoDB collection.
Supports replace (drop+insert), append (insert_many), and upsert modes.
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
class MongodbWriterComponent(Component, Model, Resolvable):
    """Component for writing a DataFrame to a MongoDB collection.

    Accepts an upstream DataFrame asset and writes its records to MongoDB,
    supporting replace, append, and upsert modes.

    Example:
        ```yaml
        type: dagster_component_templates.MongodbWriterComponent
        attributes:
          asset_name: write_users_to_mongo
          upstream_asset_key: transformed_users
          database: mydb
          collection: users
          if_exists: replace
          group_name: sinks
        ```
    """

    asset_name: str = Field(description="Name of the output asset to create")
    upstream_asset_key: str = Field(
        description="Asset key of the upstream DataFrame asset"
    )
    connection_string_env_var: str = Field(
        default="MONGODB_URI",
        description="Environment variable containing the MongoDB connection string",
    )
    database: str = Field(description="MongoDB database name")
    collection: str = Field(description="MongoDB collection name")
    if_exists: str = Field(
        default="replace",
        description="Write mode: 'replace' (drop+insert), 'append' (insert_many), or 'upsert' (update with upsert=True)",
    )
    upsert_key: Optional[str] = Field(
        default=None,
        description="Field to use as the upsert key when if_exists='upsert'",
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
        connection_string_env_var = self.connection_string_env_var
        database = self.database
        collection = self.collection
        if_exists = self.if_exists
        upsert_key = self.upsert_key
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
            description=f"Write DataFrame to MongoDB {database}.{collection}",
        )
        def mongodb_writer_asset(
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
            """Write DataFrame records to MongoDB collection."""
            try:
                from pymongo import MongoClient, UpdateOne
            except ImportError:
                raise ImportError("pymongo required: pip install pymongo")

            client = MongoClient(os.environ[connection_string_env_var])
            coll = client[database][collection]
            records = upstream.to_dict(orient="records")

            context.log.info(
                f"Writing {len(records)} records to MongoDB {database}.{collection} (mode: {if_exists})"
            )

            if if_exists == "replace":
                coll.drop()
                coll.insert_many(records)
            elif if_exists == "append":
                coll.insert_many(records)
            elif if_exists == "upsert" and upsert_key:
                ops = [
                    UpdateOne(
                        {upsert_key: r[upsert_key]},
                        {"$set": r},
                        upsert=True,
                    )
                    for r in records
                ]
                coll.bulk_write(ops)
            else:
                raise ValueError(
                    f"Invalid if_exists='{if_exists}'. Use 'replace', 'append', or 'upsert' (with upsert_key)."
                )

            context.log.info(f"Successfully wrote {len(records)} records to {database}.{collection}")
            return MaterializeResult(
                metadata={
                    "row_count": MetadataValue.int(len(upstream)),
                    "collection": MetadataValue.text(collection),
                    "database": MetadataValue.text(database),
                    "write_mode": MetadataValue.text(if_exists),
                }
            )

        return Definitions(assets=[mongodb_writer_asset])
