"""FirestoreReaderAssetComponent — read documents from a Firestore collection.

Returns a pandas DataFrame, one row per document, with the document id
as a column. Supports basic filters (where_clauses), ordering, limit,
and collection-group queries.
"""

import json
import os
from typing import Any, Dict, List, Literal, Optional

import pandas as pd

from dagster import (
    AssetExecutionContext,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    MetadataValue,
    Model,
    Output,
    Resolvable,
    asset,
)
from pydantic import Field


class FirestoreReaderAssetComponent(Component, Model, Resolvable):
    """Read documents from a Firestore collection (or collection group) into a DataFrame."""

    asset_name: str = Field(description="Output asset name.")
    credentials: Optional[Dict[str, Any]] = Field(default=None)
    credentials_path: Optional[str] = Field(default=None, description="Falls back to GOOGLE_APPLICATION_CREDENTIALS.")

    project_id: Optional[str] = Field(default=None)
    database: str = Field(default="(default)", description="Firestore database id ('(default)' or named).")

    collection: str = Field(description="Collection path (e.g. 'orders' or 'tenants/acme/orders').")
    collection_group: bool = Field(
        default=False,
        description="If True, treat `collection` as a collection-group query (matches every collection of that name across the database).",
    )

    where: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description=(
            "Optional list of {field, op, value} filter dicts. op is one of: "
            "==, !=, <, <=, >, >=, in, not-in, array-contains, array-contains-any."
        ),
    )
    order_by: Optional[List[str]] = Field(
        default=None,
        description="List of `field` (asc) or `-field` (desc) entries.",
    )
    limit: Optional[int] = Field(default=None, description="Optional max documents to return.")

    description: Optional[str] = Field(default=None)
    group_name: Optional[str] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)
    tags: Optional[Dict[str, str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)

    retry_policy_max_retries: Optional[int] = Field(
        default=None,
        description="Max retries on asset failure. Useful for transient errors like network glitches or rate limits.",
    )
    retry_policy_delay_seconds: Optional[int] = Field(
        default=None,
        description="Seconds between retries (default 1).",
    )
    retry_policy_backoff: str = Field(
        default="exponential",
        description="Backoff strategy: 'linear' or 'exponential'.",
    )

    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale.",
    )
    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5'.",
    )

    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily' / 'weekly' / 'monthly' / 'hourly' / 'static' / 'dynamic' / None for unpartitioned.",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.",
    )
    partition_values: Optional[str] = Field(
        default=None,
        description="Comma-separated values for static partitioning, e.g. 'us,eu,asia'.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition when partition_type='dynamic'.",
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        partitions_def = None
        if self.partition_type:
            from dagster import (
                DailyPartitionsDefinition, WeeklyPartitionsDefinition,
                MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
                StaticPartitionsDefinition, DynamicPartitionsDefinition,
            )
            _pt = self.partition_type
            _values = [v.strip() for v in (self.partition_values or "").split(",") if v.strip()]
            if _pt in ("daily", "weekly", "monthly", "hourly") and not self.partition_start:
                raise ValueError(f"partition_type={_pt!r} requires partition_start (ISO date).")
            if _pt == "daily":
                partitions_def = DailyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "weekly":
                partitions_def = WeeklyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "monthly":
                partitions_def = MonthlyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "hourly":
                partitions_def = HourlyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "static":
                if not _values:
                    raise ValueError("partition_type='static' requires partition_values.")
                partitions_def = StaticPartitionsDefinition(_values)
            elif _pt == "dynamic":
                if not self.dynamic_partition_name:
                    raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
                partitions_def = DynamicPartitionsDefinition(name=self.dynamic_partition_name)

        freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy

            freshness_policy = FreshnessPolicy(
                maximum_lag_minutes=self.freshness_max_lag_minutes,
                cron_schedule=self.freshness_cron,
            )

        retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy

            retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        creds_dict = self.credentials
        if creds_dict is None:
            cred_path = self.credentials_path or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
            if cred_path:
                with open(cred_path, "r") as fh:
                    creds_dict = json.load(fh)
        if creds_dict is None:
            raise ValueError("Provide credentials, credentials_path, or set GOOGLE_APPLICATION_CREDENTIALS.")

        asset_name = self.asset_name
        project_id = self.project_id or creds_dict.get("project_id")
        database = self.database
        collection = self.collection
        collection_group = self.collection_group
        where_clauses = self.where or []
        order_by = self.order_by or []
        limit = self.limit

        @asset(
            key=AssetKey.from_user_string(asset_name),
            description=self.description or f"Firestore read: {collection} in {project_id}/{database}.",
            group_name=self.group_name,
            kinds={"google", "firestore"},
            tags=self.tags or None,
            owners=self.owners or None,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])] or None,
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
            partitions_def=partitions_def,
        )
        def _asset(context: AssetExecutionContext):
            try:
                from google.cloud import firestore
                from google.oauth2 import service_account
            except ImportError:
                raise ImportError("pip install google-cloud-firestore google-auth")

            sa_creds = service_account.Credentials.from_service_account_info(creds_dict)
            client = firestore.Client(project=project_id, credentials=sa_creds, database=database)

            ref = client.collection_group(collection.split("/")[-1]) if collection_group else client.collection(collection)
            query = ref
            for clause in where_clauses:
                field = clause["field"]; op = clause["op"]; val = clause["value"]
                query = query.where(filter=firestore.FieldFilter(field, op, val))
            for ob in order_by:
                if ob.startswith("-"):
                    query = query.order_by(ob[1:], direction=firestore.Query.DESCENDING)
                else:
                    query = query.order_by(ob)
            if limit is not None:
                query = query.limit(limit)

            docs = list(query.stream())
            context.log.info(f"Firestore returned {len(docs)} document(s) for {collection}")

            rows = []
            for d in docs:
                row = {"_id": d.id}
                row.update(d.to_dict() or {})
                rows.append(row)

            df = pd.DataFrame(rows)
            preview = df.head(10).to_markdown(index=False) if not df.empty else "(no docs)"
            return Output(
                value=df,
                metadata={
                    "collection":    MetadataValue.text(collection),
                    "is_group":      MetadataValue.bool(collection_group),
                    "doc_count":     MetadataValue.int(len(df)),
                    "preview":       MetadataValue.md(preview or ""),
                },
            )

        return Definitions(assets=[_asset])
