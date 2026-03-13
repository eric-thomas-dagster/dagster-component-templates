"""CouchDB Writer Component.

Write a DataFrame to an Apache CouchDB database.
Supports upsert (PUT with revision) and insert (POST) modes.
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
class CouchdbWriterComponent(Component, Model, Resolvable):
    """Component for writing a DataFrame to an Apache CouchDB database.

    Accepts an upstream DataFrame asset and writes each row as a CouchDB
    document. Supports upsert (PUT with existing revision for updates) and
    insert (POST new documents) modes.

    Example:
        ```yaml
        type: dagster_component_templates.CouchdbWriterComponent
        attributes:
          asset_name: write_orders_to_couchdb
          upstream_asset_key: processed_orders
          database: orders
          if_exists: upsert
          id_column: order_id
          group_name: sinks
        ```
    """

    asset_name: str = Field(description="Name of the output asset to create")
    upstream_asset_key: str = Field(
        description="Asset key of the upstream DataFrame asset"
    )
    url_env_var: str = Field(
        default="COUCHDB_URL",
        description="Environment variable containing the CouchDB URL (e.g. http://admin:password@localhost:5984)",
    )
    database: str = Field(description="CouchDB database name")
    if_exists: str = Field(
        default="upsert",
        description="Write mode: 'upsert' (PUT with revision if exists) or 'insert' (POST new document)",
    )
    id_column: Optional[str] = Field(
        default=None,
        description="DataFrame column to use as CouchDB document _id (None = auto-generate via POST)",
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
        url_env_var = self.url_env_var
        database = self.database
        if_exists = self.if_exists
        id_column = self.id_column
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
            description=f"Write DataFrame to CouchDB database {database}",
        )
        def couchdb_writer_asset(
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
            """Write DataFrame rows to CouchDB as documents."""
            try:
                import requests
            except ImportError:
                raise ImportError("requests required: pip install requests")

            base_url = os.environ[url_env_var].rstrip("/")
            db_url = f"{base_url}/{database}"
            headers = {"Content-Type": "application/json"}

            records = upstream.to_dict(orient="records")
            context.log.info(
                f"Writing {len(records)} documents to CouchDB database {database} (mode: {if_exists})"
            )

            written = 0
            for row in records:
                doc = dict(row)

                if id_column and id_column in doc:
                    doc_id = str(doc[id_column])
                    doc["_id"] = doc_id

                    if if_exists == "upsert":
                        # Fetch existing revision to enable update
                        existing = requests.get(
                            f"{db_url}/{doc_id}",
                            headers=headers,
                        )
                        if existing.status_code == 200:
                            doc["_rev"] = existing.json().get("_rev")
                        # PUT creates or updates
                        resp = requests.put(
                            f"{db_url}/{doc_id}",
                            json=doc,
                            headers=headers,
                        )
                    else:
                        resp = requests.put(
                            f"{db_url}/{doc_id}",
                            json=doc,
                            headers=headers,
                        )
                else:
                    # POST to auto-generate ID
                    resp = requests.post(db_url, json=doc, headers=headers)

                resp.raise_for_status()
                written += 1

            context.log.info(f"Successfully wrote {written} documents to CouchDB database {database}")
            return MaterializeResult(
                metadata={
                    "row_count": MetadataValue.int(len(upstream)),
                    "database": MetadataValue.text(database),
                    "write_mode": MetadataValue.text(if_exists),
                }
            )

        return Definitions(assets=[couchdb_writer_asset])
