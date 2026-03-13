"""MongoDB Reader Component.

Read documents from a MongoDB collection, returning them as a DataFrame.
Supports filtering, projection, sorting, and limiting results.
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
class MongodbReaderComponent(Component, Model, Resolvable):
    """Component for reading documents from a MongoDB collection.

    Connects to MongoDB, executes a find query with optional filtering,
    projection, sorting, and limiting, and returns results as a DataFrame.

    Example:
        ```yaml
        type: dagster_component_templates.MongodbReaderComponent
        attributes:
          asset_name: mongo_users
          database: mydb
          collection: users
          query:
            active: true
          limit: 500
          group_name: sources
        ```
    """

    asset_name: str = Field(description="Name of the asset to create")
    connection_string_env_var: str = Field(
        default="MONGODB_URI",
        description="Environment variable containing the MongoDB connection string",
    )
    database: str = Field(description="MongoDB database name")
    collection: str = Field(description="MongoDB collection name")
    query: Optional[dict] = Field(
        default=None,
        description="MongoDB filter query (None = all documents)",
    )
    projection: Optional[dict] = Field(
        default=None,
        description="Fields to include/exclude in results",
    )
    limit: int = Field(
        default=0,
        description="Maximum number of documents to return (0 = no limit)",
    )
    sort_field: Optional[str] = Field(
        default=None,
        description="Field name to sort results by",
    )
    sort_direction: int = Field(
        default=1,
        description="Sort direction: 1 for ascending, -1 for descending",
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
        connection_string_env_var = self.connection_string_env_var
        database = self.database
        collection = self.collection
        query = self.query
        projection = self.projection
        limit = self.limit
        sort_field = self.sort_field
        sort_direction = self.sort_direction
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
            description=f"MongoDB reader for {database}.{collection}",
            partitions_def=partitions_def,
            group_name=group_name,
            deps=[AssetKey.from_user_string(d) for d in (deps or [])],
        )
        def mongodb_reader_asset(context: AssetExecutionContext) -> pd.DataFrame:
            # Log partition key for source components
            if context.has_partition_key:
                context.log.info(f"Running for partition: {context.partition_key}")
            """Read documents from MongoDB collection and return as DataFrame."""
            try:
                from pymongo import MongoClient
            except ImportError:
                raise ImportError("pymongo required: pip install pymongo")

            client = MongoClient(os.environ[connection_string_env_var])
            db = client[database]
            coll = db[collection]

            context.log.info(
                f"Querying MongoDB {database}.{collection} with filter: {query}"
            )

            cursor = coll.find(query or {}, projection or None)
            if sort_field:
                cursor = cursor.sort(sort_field, sort_direction)
            if limit:
                cursor = cursor.limit(limit)

            docs = list(cursor)
            for doc in docs:
                doc["_id"] = str(doc.get("_id", ""))

            df = pd.DataFrame(docs)
            context.log.info(f"Retrieved {len(df)} documents from {database}.{collection}")
            context.add_output_metadata({
                "row_count": MetadataValue.int(len(df)),
                "database": MetadataValue.text(database),
                "collection": MetadataValue.text(collection),
            })
            return df

        return Definitions(assets=[mongodb_reader_asset])
