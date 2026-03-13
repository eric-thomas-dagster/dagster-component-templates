"""Firestore Reader Component.

Query documents from a Google Cloud Firestore collection,
returning them as a DataFrame. Supports filtering, ordering, and limiting.
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
class FirestoreReaderComponent(Component, Model, Resolvable):
    """Component for reading documents from a Google Cloud Firestore collection.

    Queries Firestore with optional filters, ordering, and limiting,
    returning results as a DataFrame with document ID included.

    Example:
        ```yaml
        type: dagster_component_templates.FirestoreReaderComponent
        attributes:
          asset_name: firestore_users
          collection: users
          filters:
            - field: age
              op: ">="
              value: 18
          limit: 500
          group_name: sources
        ```
    """

    asset_name: str = Field(description="Name of the asset to create")
    collection: str = Field(description="Firestore collection name")
    project_id_env_var: Optional[str] = Field(
        default=None,
        description="Environment variable containing the GCP project ID",
    )
    credentials_env_var: Optional[str] = Field(
        default=None,
        description="Environment variable containing path to service account JSON file",
    )
    filters: Optional[List[dict]] = Field(
        default=None,
        description="List of filter dicts with keys: field, op, value (e.g. [{field: age, op: '>=', value: 18}])",
    )
    order_by: Optional[str] = Field(
        default=None,
        description="Field name to order results by",
    )
    limit: Optional[int] = Field(
        default=None,
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
        collection = self.collection
        project_id_env_var = self.project_id_env_var
        credentials_env_var = self.credentials_env_var
        filters = self.filters
        order_by = self.order_by
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
            description=f"Firestore reader for collection {collection}",
            partitions_def=partitions_def,
            group_name=group_name,
            deps=[AssetKey.from_user_string(d) for d in (deps or [])],
        )
        def firestore_reader_asset(context: AssetExecutionContext) -> pd.DataFrame:
            # Log partition key for source components
            if context.has_partition_key:
                context.log.info(f"Running for partition: {context.partition_key}")
            """Query Firestore collection and return documents as DataFrame."""
            try:
                from google.cloud import firestore
                from google.oauth2 import service_account
            except ImportError:
                raise ImportError(
                    "google-cloud-firestore required: pip install google-cloud-firestore"
                )

            creds = None
            if credentials_env_var:
                creds = service_account.Credentials.from_service_account_file(
                    os.environ[credentials_env_var]
                )
            project = os.environ.get(project_id_env_var) if project_id_env_var else None

            db = firestore.Client(project=project, credentials=creds)
            query = db.collection(collection)

            for f in (filters or []):
                query = query.where(f["field"], f["op"], f["value"])
            if order_by:
                query = query.order_by(order_by)
            if limit:
                query = query.limit(limit)

            context.log.info(f"Querying Firestore collection {collection}")
            docs = [{"_id": doc.id, **doc.to_dict()} for doc in query.stream()]
            df = pd.DataFrame(docs)

            context.log.info(f"Retrieved {len(df)} documents from Firestore collection {collection}")
            context.add_output_metadata({
                "row_count": MetadataValue.int(len(df)),
                "collection": MetadataValue.text(collection),
            })
            return df

        return Definitions(assets=[firestore_reader_asset])
