"""Firestore Writer Component.

Write a DataFrame to a Google Cloud Firestore collection using batch writes.
Supports merge vs. overwrite and optional document ID column.
"""

import os
from dataclasses import dataclass
from typing import Dict, List, Optional
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
class FirestoreWriterComponent(Component, Model, Resolvable):
    """Component for writing a DataFrame to a Google Cloud Firestore collection.

    Uses Firestore batch writes for efficiency. Each DataFrame row becomes
    a Firestore document. Supports optional document ID column and merge mode.

    Example:
        ```yaml
        type: dagster_component_templates.FirestoreWriterComponent
        attributes:
          asset_name: write_users_to_firestore
          upstream_asset_key: processed_users
          collection: users
          id_column: user_id
          merge: false
          group_name: sinks
        ```
    """

    asset_name: str = Field(description="Name of the output asset to create")
    upstream_asset_key: str = Field(
        description="Asset key of the upstream DataFrame asset"
    )
    collection: str = Field(description="Firestore collection name")
    project_id_env_var: Optional[str] = Field(
        default=None,
        description="Environment variable containing the GCP project ID",
    )
    credentials_env_var: Optional[str] = Field(
        default=None,
        description="Environment variable containing path to service account JSON file",
    )
    id_column: Optional[str] = Field(
        default=None,
        description="DataFrame column to use as the Firestore document ID (None = auto-generate)",
    )
    merge: bool = Field(
        default=False,
        description="If True, merge fields into existing documents; if False, overwrite",
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
    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners — list of team names or email addresses, e.g. ['team:analytics', 'user@company.com']",
    )
    asset_tags: Optional[Dict[str, str]] = Field(
        default=None,
        description="Additional key-value tags to apply to the asset, e.g. {'domain': 'finance', 'tier': 'gold'}",
    )
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds for the Dagster catalog, e.g. ['snowflake', 'python']. Auto-inferred from component name if not set.",
    )
    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale. Defines a FreshnessPolicy.",
    )
    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5' (weekdays at 9am).",
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        collection = self.collection
        project_id_env_var = self.project_id_env_var
        credentials_env_var = self.credentials_env_var
        id_column = self.id_column
        merge = self.merge
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

        # Infer kinds from component name if not explicitly set
        _comp_name = "firestore_writer"  # component directory name
        _kind_map = {
            "snowflake": "snowflake", "bigquery": "bigquery", "redshift": "redshift",
            "postgres": "postgres", "postgresql": "postgres", "mysql": "mysql",
            "s3": "s3", "adls": "azure", "azure": "azure", "gcs": "gcp",
            "google": "gcp", "databricks": "databricks", "dbt": "dbt",
            "kafka": "kafka", "mongodb": "mongodb", "redis": "redis",
            "neo4j": "neo4j", "elasticsearch": "elasticsearch", "pinecone": "pinecone",
            "chromadb": "chromadb", "pgvector": "postgres",
        }
        _inferred_kinds = self.kinds or []
        if not _inferred_kinds:
            _comp_lower = asset_name.lower()
            for keyword, kind in _kind_map.items():
                if keyword in _comp_lower:
                    _inferred_kinds.append(kind)
            if not _inferred_kinds:
                _inferred_kinds = ["python"]

        # Build combined tags: user tags + kind tags
        _all_tags = dict(self.asset_tags or {})
        for _kind in _inferred_kinds:
            _all_tags[f"dagster/kind/{_kind}"] = ""

        # Build freshness policy
        _freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy
            _freshness_policy = FreshnessPolicy(
                maximum_lag_minutes=self.freshness_max_lag_minutes,
                cron_schedule=self.freshness_cron,
            )

        owners = self.owners or []
        column_lineage = self.column_lineage if hasattr(self, 'column_lineage') else None


        @asset(
            name=asset_name,
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
            partitions_def=partitions_def,
                        owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
group_name=group_name,
            description=f"Write DataFrame to Firestore collection {collection}",
        )
        def firestore_writer_asset(
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
            """Write DataFrame rows to Firestore collection using batch writes."""
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
            coll_ref = db.collection(collection)

            records = upstream.to_dict(orient="records")
            context.log.info(
                f"Writing {len(records)} documents to Firestore collection {collection}"
            )

            # Firestore batch supports max 500 operations per batch
            batch_size = 500
            total_written = 0
            for i in range(0, len(records), batch_size):
                batch = db.batch()
                chunk = records[i : i + batch_size]
                for row in chunk:
                    if id_column and id_column in row:
                        doc_id = str(row[id_column])
                        doc_ref = coll_ref.document(doc_id)
                    else:
                        doc_ref = coll_ref.document()
                    batch.set(doc_ref, row, merge=merge)
                batch.commit()
                total_written += len(chunk)

            context.log.info(f"Successfully wrote {total_written} documents to Firestore {collection}")
            return MaterializeResult(
                metadata={
                    "row_count": MetadataValue.int(len(upstream)),
                    "collection": MetadataValue.text(collection),
                    "merge": MetadataValue.bool(merge),
                
                    "dagster/row_count": MetadataValue.int(len(upstream)),}
            )

        return Definitions(assets=[firestore_writer_asset])
