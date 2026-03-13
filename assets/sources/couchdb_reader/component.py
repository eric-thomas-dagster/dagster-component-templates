"""CouchDB Reader Component.

Query documents from an Apache CouchDB database using the Mango query API,
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
class CouchdbReaderComponent(Component, Model, Resolvable):
    """Component for querying documents from an Apache CouchDB database.

    Uses the CouchDB Mango query API (_find endpoint) to retrieve documents,
    returning results as a DataFrame.

    Example:
        ```yaml
        type: dagster_component_templates.CouchdbReaderComponent
        attributes:
          asset_name: couchdb_orders
          database: orders
          selector:
            status: active
          limit: 500
          group_name: sources
        ```
    """

    asset_name: str = Field(description="Name of the asset to create")
    url_env_var: str = Field(
        default="COUCHDB_URL",
        description="Environment variable containing the CouchDB URL (e.g. http://admin:password@localhost:5984)",
    )
    database: str = Field(description="CouchDB database name")
    selector: Optional[dict] = Field(
        default=None,
        description="Mango selector query (None = all documents)",
    )
    fields: Optional[List[str]] = Field(
        default=None,
        description="List of fields to return (None = all fields)",
    )
    limit: int = Field(
        default=1000,
        description="Maximum number of documents to return",
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
        url_env_var = self.url_env_var
        database = self.database
        selector = self.selector
        fields = self.fields
        limit = self.limit
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
            description=f"CouchDB reader for database {database}",
            partitions_def=partitions_def,
            group_name=group_name,
            deps=[AssetKey.from_user_string(d) for d in (deps or [])],
        )
        def couchdb_reader_asset(context: AssetExecutionContext) -> pd.DataFrame:
            # Log partition key for source components
            if context.has_partition_key:
                context.log.info(f"Running for partition: {context.partition_key}")
            """Query CouchDB database via Mango API and return documents as DataFrame."""
            try:
                import requests
            except ImportError:
                raise ImportError("requests required: pip install requests")

            url = os.environ[url_env_var]
            find_url = f"{url.rstrip('/')}/{database}/_find"
            body = {"selector": selector or {}, "limit": limit}
            if fields:
                body["fields"] = fields

            context.log.info(f"Querying CouchDB database {database} with selector: {selector}")
            resp = requests.post(
                find_url,
                json=body,
                headers={"Content-Type": "application/json"},
            )
            resp.raise_for_status()

            docs = resp.json().get("docs", [])
            df = pd.DataFrame(docs)

            context.log.info(f"Retrieved {len(df)} documents from CouchDB database {database}")
            context.add_output_metadata({
                "row_count": MetadataValue.int(len(df)),
                "database": MetadataValue.text(database),
            })
            return df

        return Definitions(assets=[couchdb_reader_asset])
