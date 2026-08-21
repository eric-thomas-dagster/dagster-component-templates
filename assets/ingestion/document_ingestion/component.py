"""Document Ingestion Component.

Ingest documents for RAG/Q&A systems.
Supports various document sources including files, URLs, and directories.
"""

from typing import Any, Dict, List, Optional, Union
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
    if isinstance(partition_values, (list, tuple)):
        _values = [str(v).strip() for v in partition_values if str(v).strip()]
    else:
        _values = [v.strip() for v in (str(partition_values) if partition_values else "").split(",") if v.strip()]
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
        description=(
            "Documents directory or file. Local paths (`/data/docs/`) and "
            "cloud URIs (`s3://bucket/docs/`, `gs://bucket/docs/`, "
            "`az://container/docs/`, `abfs://`) both work — cloud sources "
            "route through fsspec, so install the matching backend "
            "(`s3fs` / `gcsfs` / `adlfs`). Walks recursively for "
            "`.txt`, `.md`, `.pdf`, `.doc`, `.docx`, `.html`."
        ),
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

    partition_date_column: Optional[Union[str, int]] = Field(
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

    partition_static_column: Optional[Union[str, int]] = Field(
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
            key=AssetKey.from_user_string(asset_name),
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

            # Universal source resolution — local paths OR cloud URIs
            # (s3://, gs://, az://, abfs://, http(s)://). Uses fsspec so a
            # single code path handles both. Requires an fsspec backend for
            # cloud (s3fs / gcsfs / adlfs — install once, works everywhere).
            _is_uri_source = source_path and "://" in source_path

            def _list_files(root: str, extensions: List[str]) -> List[str]:
                """Return absolute paths for files under `root` matching any extension."""
                if _is_uri_source:
                    try:
                        import fsspec
                    except ImportError:
                        raise ImportError(
                            "document_ingestion: cloud sources require fsspec + a backend "
                            "(pip install 's3fs' / 'gcsfs' / 'adlfs')."
                        )
                    fs, _root = fsspec.core.url_to_fs(root)
                    hits: List[str] = []
                    # fs.walk yields (dirpath, dirnames, filenames)
                    for dirpath, _dirs, files in fs.walk(_root):
                        for fname in files:
                            for ext in extensions:
                                if fname.lower().endswith(ext):
                                    # Reconstruct the URI so downstream reads route back via fsspec.
                                    _proto = fs.protocol if isinstance(fs.protocol, str) else fs.protocol[0]
                                    hits.append(f"{_proto}://{dirpath}/{fname}")
                                    break
                    return hits
                # Local path
                root_path = Path(root)
                return [str(p) for ext in extensions for p in root_path.rglob(f"*{ext}")]

            def _read_text(uri: str) -> str:
                if "://" in uri:
                    import fsspec
                    with fsspec.open(uri, "r", encoding="utf-8") as f:
                        return f.read()
                with open(uri, "r", encoding="utf-8") as f:
                    return f.read()

            def _is_source_available(root: str) -> bool:
                if not root:
                    return False
                if "://" in root:
                    try:
                        import fsspec
                        fs, _root = fsspec.core.url_to_fs(root)
                        return fs.exists(_root)
                    except Exception:  # noqa: BLE001
                        return False
                return os.path.exists(root)

            if source_path and _is_source_available(source_path):
                context.log.info(f"Reading documents from: {source_path}")

                # Look for common document types
                extensions = ['.txt', '.md', '.pdf', '.doc', '.docx', '.html']
                text_exts = {'.txt', '.md'}

                # Directory-walk for both local and cloud. For a single file,
                # _list_files still works if `source_path` is the file itself
                # AND the extension matches — otherwise fall through to the
                # single-file branch below.
                candidate_paths = _list_files(source_path, extensions) if _is_uri_source or Path(source_path).is_dir() else []

                if candidate_paths:
                    for file_path in candidate_paths:
                        ext = ("." + file_path.rsplit(".", 1)[-1]).lower() if "." in file_path else ""
                        try:
                            filename = file_path.rsplit("/", 1)[-1] if "://" in file_path else Path(file_path).name
                            if ext in text_exts:
                                content = _read_text(file_path)
                                documents.append({
                                    'doc_id': str(file_path),
                                    'filename': filename,
                                    'file_type': ext,
                                    'content': content,
                                    'content_length': len(content),
                                    'source_path': str(file_path),
                                })
                                context.log.info(f"Ingested: {filename}")
                            else:
                                documents.append({
                                    'doc_id': str(file_path),
                                    'filename': filename,
                                    'file_type': ext,
                                    'content': f'[Document: {filename}]',
                                    'content_length': 0,
                                    'source_path': str(file_path),
                                    'needs_extraction': True,
                                })
                        except Exception as e:  # noqa: BLE001
                            context.log.warning(f"Could not read {file_path}: {e}")

                # Single-file branch (local only — cloud single-files are
                # handled by the walk above if the ext matches).
                elif not _is_uri_source and Path(source_path).is_file():
                    path = Path(source_path)
                    try:
                        content = _read_text(str(path))
                        documents.append({
                            'doc_id': str(path),
                            'filename': path.name,
                            'file_type': path.suffix,
                            'content': content,
                            'content_length': len(content),
                            'source_path': str(path),
                        })
                        context.log.info(f"Ingested single document: {path.name}")
                    except Exception as e:  # noqa: BLE001
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
