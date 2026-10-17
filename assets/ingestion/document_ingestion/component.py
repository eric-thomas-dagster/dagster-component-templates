"""Document Ingestion Component.

Ingest documents for RAG/Q&A systems.
Supports various document sources including files, URLs, and directories.
"""

from typing import Any, Dict, List, Literal, Optional, Union
import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetKey,
    BackfillPolicy,
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


def _filter_df_to_partition(df: pd.DataFrame, context, partition_date_column, partition_static_column) -> pd.DataFrame:
    """Filter a fully-fetched DataFrame down to the current partition's rows.

    This is the ONE piece of the partitioning puzzle these components were
    missing: partition_type/partition_date_column/partition_static_column
    were declared and documented ("Column used to filter the upstream
    DataFrame to the current date/static partition value") but nothing
    ever actually filtered anything -- every partition silently returned
    the identical full dataset. Time-based partitions use
    context.partition_time_window, which dagster itself already keeps
    correctly range-aware for a single tick AND a single_run/multi_run
    backfill range alike -- no per-key looping needed. Static/dynamic/multi
    partitions have no natural time window, so rows are matched against the
    set of context.partition_keys instead (1 key normally, the whole range
    for a backfill).
    """
    if not (context.has_partition_key or context.has_partition_key_range):
        return df
    if partition_date_column and partition_date_column in df.columns:
        try:
            window = context.partition_time_window
        except Exception:
            return df  # static/dynamic/multi partition -- no time window
        dates = pd.to_datetime(df[partition_date_column], errors="coerce", utc=True)
        start = pd.Timestamp(window.start)
        end = pd.Timestamp(window.end)
        return df[(dates >= start) & (dates < end)]
    if partition_static_column and partition_static_column in df.columns:
        keys = {str(k) for k in context.partition_keys}
        return df[df[partition_static_column].astype(str).isin(keys)]
    return df


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



# BEGIN field-shape normalization (inlined per component; keep copies identical — see tests/test_field_shapes.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def coerce_field_shapes(data, *, lists=(), csv=(), tags=()):
    """Accept the shapes people (and LLMs) naturally write for common boilerplate fields."""
    if not isinstance(data, dict):
        return data
    out = dict(data)
    for k in lists:  # List[str]: "a" / "a, b" -> ["a", "b"]; strip + drop empties
        v = out.get(k)
        if isinstance(v, str):
            v = v.split(",")
        if isinstance(v, (list, tuple)):
            out[k] = [x.strip() if isinstance(x, str) else x for x in v if not (isinstance(x, str) and not x.strip())]
    for k in csv:  # comma-separated str given as a list -> "a,b"
        v = out.get(k)
        if isinstance(v, (list, tuple, set)):
            out[k] = ",".join(str(x).strip() for x in v)
    for k in tags:  # Dict[str, str]: scalars -> str, ["k=v"] / ["k:v"] -> {k: v}
        v = out.get(k)
        if isinstance(v, (list, tuple)) and v and all(isinstance(x, str) and ("=" in x or ":" in x) for x in v):
            v = dict((p.strip() for p in (x.split("=", 1) if "=" in x else x.split(":", 1))) for x in v)
        if isinstance(v, dict):
            out[k] = {str(a).strip(): (str(b).lower() if isinstance(b, bool) else str(b)) for a, b in v.items() if b is not None}
    return out
# END field-shape normalization


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

    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(
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
        description="Column-level lineage, emitted as `dagster/column_lineage` metadata on every materialization: {output column: [upstream columns]}, e.g. {'revenue': ['price', 'quantity']}. The upstream asset is `upstream_asset_key`, or the only entry of `deps`; with several upstream assets, name each column '<asset_key>:<column>'. Ignored with a warning when no upstream asset can be resolved.",
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

    partition_values: Optional[Union[str, List[str]]] = Field(
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

    backfill_policy: Optional[Literal["single_run", "multi_run"]] = Field(
        default=None,
        description=(
            "Backfill execution strategy for a partitioned asset. 'single_run' "
            "materializes an entire backfill range in one run: the full "
            "dataset is fetched once (as always) and then filtered down to "
            "the whole range via partition_date_column/partition_static_column "
            "(same mechanism as a normal tick, just spanning more rows). "
            "'multi_run' launches one run per partition (optionally capped "
            "via backfill_policy_max_partitions_per_run). Leave unset for "
            "Dagster's default (one run per partition, uncapped). Ignored "
            "when partition_type is not set. Requires a partition-range-aware "
            "IO manager (e.g. a DB-table IO manager with partition_expr set) "
            "-- a generic filesystem/default IO manager cannot persist a "
            "single_run output spanning multiple partitions and will raise a "
            "clear error at materialization time."
        ),
    )
    backfill_policy_max_partitions_per_run: Optional[int] = Field(
        default=None,
        description="For backfill_policy='multi_run' only: maximum partitions materialized per run. Leave unset for Dagster's default (1 partition per run).",
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('deps', 'owners', 'kinds'), csv=('partition_values',), tags=('asset_tags',))

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        # Standard catalog fields — phase 2 wiring
        _column_lineage_md = _column_lineage_metadata(self)
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
            _freshness_policy = _freshness_policy_compat(self.freshness_max_lag_minutes, self.freshness_cron)
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



        _backfill_policy_obj = None
        if partitions_def is not None and self.backfill_policy == "single_run":
            _backfill_policy_obj = BackfillPolicy.single_run()
        elif partitions_def is not None and self.backfill_policy == "multi_run":
            _backfill_policy_obj = BackfillPolicy.multi_run(
                max_partitions_per_run=self.backfill_policy_max_partitions_per_run or 1
            )

        partition_date_column = self.partition_date_column
        partition_static_column = self.partition_static_column

        @asset(retry_policy=_retry_policy, partitions_def=partitions_def, backfill_policy=_backfill_policy_obj,
            key=AssetKey.from_user_string(asset_name),
            description=description,
            group_name=self.group_name or "knowledge_base",
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
            freshness_policy=_freshness_policy,
            owners=self.owners or [],
            tags=_all_tags,
        )
        def document_ingestion_asset(context: AssetExecutionContext) -> pd.DataFrame:
            """Asset that ingests documents for RAG systems."""
            if _column_lineage_md:
                context.add_output_metadata(_column_lineage_md)
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

            df = _filter_df_to_partition(df, context, partition_date_column, partition_static_column)

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


# BEGIN freshness-policy compat (inlined per component; keep copies identical, see tests/test_freshness_policy_compat.py)
def _freshness_policy_compat(max_lag_minutes, cron_schedule=None):
    """Build a FreshnessPolicy on the current Dagster API (time_window / cron) and on the older constructor."""
    from datetime import timedelta

    from dagster import FreshnessPolicy

    lag = timedelta(minutes=max_lag_minutes)
    if hasattr(FreshnessPolicy, "time_window"):
        if cron_schedule:
            return FreshnessPolicy.cron(deadline_cron=cron_schedule, lower_bound_delta=lag)
        return FreshnessPolicy.time_window(fail_window=lag)
    return FreshnessPolicy(maximum_lag_minutes=max_lag_minutes, cron_schedule=cron_schedule)
# END freshness-policy compat

# BEGIN column-lineage helper (inlined per component; keep copies identical, see tests/test_column_lineage.py)
def _column_lineage_metadata(component):
    """`dagster/column_lineage` metadata from the component's `column_lineage` mapping ({output column: [upstream columns]}).

    The upstream asset is `upstream_asset_key`, or the only entry of `deps`. With several upstream assets, name each
    column as "<asset_key>:<column>". Returns {} when the mapping is unset, or (with a warning) when no upstream asset
    can be resolved, since lineage needs an upstream asset to point at."""
    import warnings

    import dagster as _dg

    mapping = getattr(component, "column_lineage", None)
    if not mapping:
        return {}
    ups = getattr(component, "upstream_asset_key", None) or getattr(component, "deps", None) or []
    ups = [ups] if isinstance(ups, (str, _dg.AssetKey)) else list(ups)
    ups = [u.to_user_string() if isinstance(u, _dg.AssetKey) else str(u) for u in ups]
    deps = {}
    for out_col, in_cols in mapping.items():
        refs = []
        for ic in [in_cols] if isinstance(in_cols, str) else in_cols:
            key, _, col = str(ic).rpartition(":")
            if key in ups and col:
                refs.append(_dg.TableColumnDep(asset_key=_dg.AssetKey.from_user_string(key), column_name=col))
            elif len(ups) == 1:
                refs.append(_dg.TableColumnDep(asset_key=_dg.AssetKey.from_user_string(ups[0]), column_name=str(ic)))
            else:
                warnings.warn(
                    f"column_lineage ignored: cannot tell which upstream asset {ic!r} belongs to "
                    f"(upstream assets: {ups or 'none'}; use '<asset_key>:<column>' with several upstream assets)"
                )
                return {}
        deps[str(out_col)] = refs
    return {"dagster/column_lineage": _dg.MetadataValue.column_lineage(_dg.TableColumnLineage(deps))}
# END column-lineage helper
