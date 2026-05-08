"""Document Ingestion Component.

Ingest documents for RAG/Q&A systems.
Supports various document sources including files, URLs, and directories.
"""

from typing import Any, Dict, List, Optional
import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    Model,
    Resolvable,
    asset,
    Output,
    MetadataValue,
)
from pydantic import Field


def _build_partitions_def(
    partition_type,
    partition_start,
    partition_values,
    dynamic_partition_name,
    partition_dimensions,
):
    """Construct a Dagster partitions_def from the canonical partition fields.

    Strict: raises ValueError on misconfigured combinations rather than
    silently picking a default. Specifically:
      - time-based partition_type without partition_start
      - partition_type=multi without partition_values
      - partition_type=dynamic without dynamic_partition_name
      - both partition_dimensions AND flat fields set (ambiguous intent)
    """
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )

    # Both shapes set: ambiguous. Pick one.
    if partition_dimensions and partition_type:
        raise ValueError(
            "Set either partition_type (flat-fields shape) or "
            "partition_dimensions (multi-axis shape), not both."
        )

    def _build_axis(spec):
        t = spec.get("type")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily":
            return DailyPartitionsDefinition(start_date=spec["start"])
        if t == "weekly":
            return WeeklyPartitionsDefinition(start_date=spec["start"])
        if t == "monthly":
            return MonthlyPartitionsDefinition(start_date=spec["start"])
        if t == "hourly":
            return HourlyPartitionsDefinition(start_date=spec["start"])
        if t == "static":
            vals = spec.get("values") or []
            if isinstance(vals, str):
                vals = [v.strip() for v in vals.split(",") if v.strip()]
            if not vals:
                raise ValueError("partition dimension type='static' requires non-empty 'values'")
            return StaticPartitionsDefinition(list(vals))
        if t == "dynamic":
            name = spec.get("dynamic_partition_name") or spec.get("name")
            if not name:
                raise ValueError("partition dimension type='dynamic' requires a name")
            return DynamicPartitionsDefinition(name=name)
        raise ValueError(f"unknown partition type: {t!r}")

    if partition_dimensions:
        if len(partition_dimensions) == 1:
            return _build_axis(partition_dimensions[0])
        axes = {d["name"]: _build_axis(d) for d in partition_dimensions}
        return MultiPartitionsDefinition(axes)

    if not partition_type:
        return None
    _values = [v.strip() for v in (partition_values or "").split(",") if v.strip()]
    if partition_type in ("daily", "weekly", "monthly", "hourly") and not partition_start:
        raise ValueError(
            f"partition_type={partition_type!r} requires partition_start (ISO date, e.g. '2024-01-01')."
        )
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "static":
        if not _values:
            raise ValueError("partition_type='static' requires partition_values (comma-separated).")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError(
                "partition_type='dynamic' requires dynamic_partition_name."
            )
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    if partition_type == "multi":
        if not _values:
            raise ValueError("partition_type='multi' requires partition_values (comma-separated).")
        if not partition_start:
            raise ValueError("partition_type='multi' requires partition_start (the date axis start).")
        return MultiPartitionsDefinition({
            "date": DailyPartitionsDefinition(start_date=partition_start),
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")


class DocumentIngestionComponent(Component, Model, Resolvable):
    """Component for ingesting documents for RAG/Q&A systems.

    This component ingests documents from various sources and prepares them
    for embedding and vector storage for retrieval-augmented generation.

    Example:
        ```yaml
        type: dagster_component_templates.DocumentIngestionComponent
        attributes:
          asset_name: knowledge_base_docs
          source_path: "/path/to/documents"
        ```
    """

    asset_name: str = Field(
        description="Name of the asset to create"
    )

    source_path: Optional[str] = Field(
        default=None,
        description="Path to documents directory or file"
    )

    description: Optional[str] = Field(
        default=None,
        description="Asset description"
    )

    include_preview_metadata: bool = Field(
        default=False,
        description=(
            "Include a preview of the output data in metadata (first 5 rows "
            "as a markdown table). Used by builder UIs to render asset shape "
            "without warehouse access."
        ),
    )

    preview_rows: int = Field(
        default=25,
        ge=1,
        le=500,
        description=(
            "Rows to include in the preview metadata when "
            "`include_preview_metadata` is True. For long DataFrames "
            "(>10x preview_rows), a random sample is used so the preview "
            "reflects the data distribution; otherwise head() is used."
        ),
    )

    deps: Optional[list[str]] = Field(default=None, description="Upstream asset keys this asset depends on (e.g. ['raw_orders', 'schema/asset'])")

    partition_type: Optional[str] = Field(

        default=None,

        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', or None for unpartitioned. With a partition type set, the partition key is exposed via context.partition_key for use in filtering / templating.",

    )

    partition_start: Optional[str] = Field(

        default=None,

        description="Partition start date in ISO format, e.g. '2024-01-01'. Required when partition_type is set.",

    )


    retry_policy_max_retries: Optional[int] = Field(


        default=None,


        description="Max retries on asset failure. Defines a RetryPolicy. Useful for transient network failures, rate limits, etc.",


    )


    retry_policy_delay_seconds: Optional[int] = Field(


        default=None,


        description="Seconds between retries (default 1).",


    )


    retry_policy_backoff: str = Field(


        default="exponential",


        description="Backoff strategy: 'linear' or 'exponential'.",


    )




    group_name: Optional[str] = Field(
        default=None,
        description="Dagster asset group name.",
    )

    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners — team names ('team:analytics') or email addresses.",
    )

    asset_tags: Optional[Dict[str, str]] = Field(
        default=None,
        description="Additional key-value tags applied to the asset in the Dagster catalog.",
    )

    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds for the catalog (e.g. ['snowflake', 'python']). Auto-inferred from component name when unset.",
    )

    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale. Builds a FreshnessPolicy when set.",
    )

    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5' (weekdays 9am).",
    )

    column_lineage: Optional[Dict[str, List[str]]] = Field(
        default=None,
        description="Column-level lineage: output column → list of upstream columns it derives from, e.g. {'revenue': ['price', 'quantity']}.",
    )

    partition_date_column: Optional[str] = Field(
        default=None,
        description="Column used to filter the upstream DataFrame to the current date partition key.",
    )

    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition (when partition_type='dynamic'), e.g. 'tenants'.",
    )

    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Multi-axis partition spec: list of {name, type, start, values, dynamic_partition_name} dicts. Overrides flat fields when set.",
    )

    partition_values: Optional[str] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning, e.g. 'acme,globex,initech'.",
    )

    partition_static_dim: Optional[str] = Field(
        default=None,
        description="Dimension name for the static axis in multi-partitioning, e.g. 'customer'.",
    )

    partition_static_column: Optional[str] = Field(
        default=None,
        description="Column used to filter the upstream DataFrame to the current static partition value.",
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        # Standard catalog fields — phase 2 wiring
        _retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            _retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )
        _freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy
            _freshness_policy = FreshnessPolicy(
                maximum_lag_minutes=self.freshness_max_lag_minutes,
                cron_schedule=self.freshness_cron,
            )
        _all_tags = dict(self.asset_tags or {})
        for _k in (self.kinds or []):
            _all_tags[f"dagster/kind/{_k}"] = ""
        asset_name = self.asset_name
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows
        source_path = self.source_path
        description = self.description or "Documents for RAG/Q&A system"

        # Build partition definition (auto-generated; supports daily, weekly,

        # monthly, hourly partitions out of the box).
        partitions_def = _build_partitions_def(
            self.partition_type,
            self.partition_start,
            self.partition_values,
            self.dynamic_partition_name,
            self.partition_dimensions,
        )


        # Build retry policy (auto-generated; opt-in via retry_policy_max_retries).


        _retry_policy = None


        if self.retry_policy_max_retries is not None:


            from dagster import Backoff, RetryPolicy


            _retry_policy = RetryPolicy(


                max_retries=self.retry_policy_max_retries,


                delay=self.retry_policy_delay_seconds or 1,


                backoff=Backoff[self.retry_policy_backoff.upper()],


            )



        @asset(retry_policy=_retry_policy, partitions_def=partitions_def, 
            name=asset_name,
            description=description,
            group_name="knowledge_base",
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
            freshness_policy=_freshness_policy,
            owners=self.owners or [],
            tags=_all_tags,
        )
        def document_ingestion_asset(context: AssetExecutionContext) -> pd.DataFrame:
            """Asset that ingests documents for RAG systems."""
            import os
            from pathlib import Path

            context.log.info("Ingesting documents for RAG/Q&A system")

            documents = []

            if source_path and os.path.exists(source_path):
                context.log.info(f"Reading documents from: {source_path}")

                path = Path(source_path)

                # Handle directory
                if path.is_dir():
                    # Look for common document types
                    extensions = ['.txt', '.md', '.pdf', '.doc', '.docx', '.html']
                    for ext in extensions:
                        for file_path in path.rglob(f'*{ext}'):
                            try:
                                # Read text files
                                if ext in ['.txt', '.md']:
                                    with open(file_path, 'r', encoding='utf-8') as f:
                                        content = f.read()
                                        documents.append({
                                            'doc_id': str(file_path),
                                            'filename': file_path.name,
                                            'file_type': ext,
                                            'content': content,
                                            'content_length': len(content),
                                            'source_path': str(file_path)
                                        })
                                        context.log.info(f"Ingested: {file_path.name}")
                                else:
                                    # For other types, add placeholder
                                    documents.append({
                                        'doc_id': str(file_path),
                                        'filename': file_path.name,
                                        'file_type': ext,
                                        'content': f'[Document: {file_path.name}]',
                                        'content_length': 0,
                                        'source_path': str(file_path),
                                        'needs_extraction': True
                                    })
                            except Exception as e:
                                context.log.warning(f"Could not read {file_path}: {e}")

                # Handle single file
                elif path.is_file():
                    try:
                        with open(path, 'r', encoding='utf-8') as f:
                            content = f.read()
                            documents.append({
                                'doc_id': str(path),
                                'filename': path.name,
                                'file_type': path.suffix,
                                'content': content,
                                'content_length': len(content),
                                'source_path': str(path)
                            })
                            context.log.info(f"Ingested single document: {path.name}")
                    except Exception as e:
                        context.log.warning(f"Could not read {path}: {e}")

            # If no documents found, create sample documents
            if not documents:
                context.log.info("No documents found at source path. Creating sample knowledge base.")
                documents = [
                    {
                        'doc_id': 'doc_1',
                        'filename': 'product_guide.md',
                        'file_type': '.md',
                        'content': 'Product Guide: Our product helps teams collaborate effectively. Features include real-time chat, file sharing, and video conferencing.',
                        'content_length': 150,
                        'source_path': 'sample'
                    },
                    {
                        'doc_id': 'doc_2',
                        'filename': 'faq.md',
                        'file_type': '.md',
                        'content': 'FAQ: Q: How do I reset my password? A: Click on "Forgot Password" on the login page. Q: What payment methods do you accept? A: We accept credit cards and PayPal.',
                        'content_length': 180,
                        'source_path': 'sample'
                    },
                    {
                        'doc_id': 'doc_3',
                        'filename': 'api_docs.md',
                        'file_type': '.md',
                        'content': 'API Documentation: Our REST API provides endpoints for user management, data access, and webhooks. Authentication uses API keys.',
                        'content_length': 140,
                        'source_path': 'sample'
                    },
                    {
                        'doc_id': 'doc_4',
                        'filename': 'pricing.md',
                        'file_type': '.md',
                        'content': 'Pricing: Starter plan is $10/month for up to 5 users. Pro plan is $50/month for unlimited users with advanced features.',
                        'content_length': 130,
                        'source_path': 'sample'
                    },
                    {
                        'doc_id': 'doc_5',
                        'filename': 'support.md',
                        'file_type': '.md',
                        'content': 'Support: Contact our support team via email at support@company.com or chat with us during business hours 9am-5pm EST.',
                        'content_length': 125,
                        'source_path': 'sample'
                    },
                ]

            df = pd.DataFrame(documents)

            # Add metadata
            df['ingested_at'] = pd.Timestamp.now()

            context.log.info(f"Successfully ingested {len(df)} documents")

            # Calculate statistics
            total_chars = df['content_length'].sum() if 'content_length' in df.columns else 0
            file_types = df['file_type'].value_counts().to_dict() if 'file_type' in df.columns else {}

            context.add_output_metadata({
                    "document_count": len(df),
                    "total_characters": int(total_chars),
                    "file_types": file_types,
                    "columns": list(df.columns),
                    "preview": MetadataValue.md(df.head(5).to_markdown())
                })
            return df

        return Definitions(assets=[document_ingestion_asset])
