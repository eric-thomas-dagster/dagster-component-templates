"""Image Similarity Scorer Component.

Score visual similarity between images using CLIP embeddings.
"""

from typing import Any, Dict, List, Optional
import pandas as pd

from dagster import (
    AssetExecutionContext,
    AssetIn,
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


def _build_partitions_def(
    partition_type,
    partition_start,
    partition_values,
    dynamic_partition_name,
    partition_dimensions,
):
    """Construct a Dagster partitions_def from the canonical partition fields."""
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )

    def _build_axis(spec):
        t = spec.get("type")
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
            return StaticPartitionsDefinition(list(vals))
        if t == "dynamic":
            name = spec.get("dynamic_partition_name") or spec.get("name")
            if not name:
                raise ValueError("dynamic partition dimension requires a name")
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
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start or "2024-01-01")
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start or "2024-01-01")
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start or "2024-01-01")
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start or "2024-01-01")
    if partition_type == "static":
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError(
                "partition_type='dynamic' requires dynamic_partition_name."
            )
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    if partition_type == "multi":
        return MultiPartitionsDefinition({
            "date": DailyPartitionsDefinition(start_date=partition_start or "2024-01-01"),
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")


class ImageSimilarityScorerComponent(Component, Model, Resolvable):
    """Component for scoring visual similarity between images using CLIP embeddings.

    Computes cosine similarity between CLIP image embeddings for each row.
    Compares two image columns or one image column against a fixed reference image.

    Features:
    - CLIP-based embedding comparison
    - Pairwise column comparison or reference image comparison
    - Cosine similarity score 0-1
    - CPU, CUDA, and MPS device support

    Use Cases:
    - Duplicate image detection
    - Product image quality comparison
    - Before/after change detection
    - Visual search relevance scoring
    """

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(
        description="Upstream asset key providing a DataFrame"
    )
    image_column_a: str = Field(description="First image column (file paths or URLs)")
    image_column_b: Optional[str] = Field(
        default=None,
        description="Second image column; if omitted, compare against reference_image",
    )
    reference_image: Optional[str] = Field(
        default=None,
        description="Fixed reference image path/URL to compare all rows against",
    )
    output_column: str = Field(
        default="image_similarity",
        description="Column for cosine similarity score (0-1)",
    )
    model_name: str = Field(
        default="openai/clip-vit-base-patch32",
        description="HuggingFace CLIP model ID",
    )
    device: str = Field(
        default="cpu",
        description="Compute device: cpu, cuda, or mps",
    )
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name")
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
    column_lineage: Optional[Dict[str, List[str]]] = Field(
        default=None,
        description="Column-level lineage mapping: output column name → list of upstream column names it was derived from, e.g. {'revenue': ['price', 'quantity']}",
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



    description: Optional[str] = Field(
        default=None,
        description="Asset description shown in the Dagster catalog.",
    )

    deps: Optional[List[str]] = Field(
        default=None,
        description="Lineage-only upstream asset keys (no data passed at runtime).",
    )

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows
        upstream_asset_key = self.upstream_asset_key
        image_column_a = self.image_column_a
        image_column_b = self.image_column_b
        reference_image = self.reference_image
        output_column = self.output_column
        model_name = self.model_name
        device = self.device

        partitions_def = _build_partitions_def(
            self.partition_type,
            self.partition_start,
            self.partition_values,
            self.dynamic_partition_name,
            self.partition_dimensions,
        )
        partition_type = self.partition_type
        partition_date_column = self.partition_date_column
        partition_static_column = self.partition_static_column
        partition_static_dim = self.partition_static_dim

        # Infer kinds from component name if not explicitly set
        _comp_name = "image_similarity_scorer"  # component directory name
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


        # Build retry policy (auto-generated; opt-in via retry_policy_max_retries).


        _retry_policy = None


        if self.retry_policy_max_retries is not None:


            from dagster import Backoff, RetryPolicy


            _retry_policy = RetryPolicy(


                max_retries=self.retry_policy_max_retries,


                delay=self.retry_policy_delay_seconds or 1,


                backoff=Backoff[self.retry_policy_backoff.upper()],


            )



        @asset(retry_policy=_retry_policy, 
            name=asset_name,
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
            partitions_def=partitions_def,
                        owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
group_name=self.group_name,
        )
        def _asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
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
            try:
                from transformers import CLIPProcessor, CLIPModel
            except ImportError:
                raise ImportError("transformers required: pip install transformers")
            try:
                from PIL import Image
            except ImportError:
                raise ImportError("Pillow required: pip install Pillow")
            try:
                import torch
                import torch.nn.functional as F
            except ImportError:
                raise ImportError("torch required: pip install torch")

            if image_column_b is None and reference_image is None:
                raise ValueError(
                    "Either image_column_b or reference_image must be provided"
                )

            df = upstream.copy()
            context.log.info(f"Loading CLIP model {model_name}")
            model = CLIPModel.from_pretrained(model_name).to(device)
            processor = CLIPProcessor.from_pretrained(model_name)

            def encode_image(path_or_url: str):
                img = Image.open(str(path_or_url)).convert("RGB")
                inputs = processor(images=img, return_tensors="pt")
                inputs = {k: v.to(device) for k, v in inputs.items()}
                with torch.no_grad():
                    features = model.get_image_features(**inputs)
                return F.normalize(features, dim=-1)

            # Pre-encode reference if needed
            ref_embedding = None
            if reference_image:
                try:
                    ref_embedding = encode_image(reference_image)
                    context.log.info(f"Encoded reference image: {reference_image}")
                except Exception as e:
                    raise RuntimeError(f"Could not encode reference image: {e}")

            similarities = []
            for idx, row in df.iterrows():
                try:
                    emb_a = encode_image(row[image_column_a])
                    if ref_embedding is not None:
                        emb_b = ref_embedding
                    else:
                        emb_b = encode_image(row[image_column_b])
                    cos_sim = F.cosine_similarity(emb_a, emb_b).item()
                    similarities.append(round(float(cos_sim), 4))
                except Exception as e:
                    context.log.warning(f"Similarity scoring failed for row {idx}: {e}")
                    similarities.append(None)

            df[output_column] = similarities

            context.add_output_metadata(
                {
                    "row_count": MetadataValue.int(len(df)),
                    "model": model_name,
                    "mean_similarity": round(
                        sum(s for s in similarities if s is not None)
                        / max(1, sum(1 for s in similarities if s is not None)),
                        4,
                    ),
                    "preview": MetadataValue.md(df[[image_column_a, output_column]].head(5).to_markdown()),
                }
            )
            # Build column schema metadata
            from dagster import TableSchema, TableColumn, TableColumnLineage, TableColumnDep
            _col_schema = TableSchema(columns=[
                TableColumn(name=str(col), type=str(df.dtypes[col]))
                for col in df.columns
            ])
            _metadata = {
                "dagster/row_count": MetadataValue.int(len(df)),
                "dagster/column_schema": MetadataValue.table_schema(_col_schema),
            }
            # Use explicit lineage, or auto-infer passthrough columns at runtime
            _effective_lineage = column_lineage
            if not _effective_lineage:
                try:
                    _upstream_cols = set(upstream.columns)
                    _effective_lineage = {
                        col: [col] for col in _col_schema.columns_by_name
                        if col in _upstream_cols
                    }
                except Exception:
                    pass
            if _effective_lineage:
                _upstream_key = AssetKey.from_user_string(upstream_asset_key) if upstream_asset_key else None
                if _upstream_key:
                    _lineage_deps = {}
                    for out_col, in_cols in _effective_lineage.items():
                        _lineage_deps[out_col] = [
                            TableColumnDep(asset_key=_upstream_key, column_name=ic)
                            for ic in in_cols
                        ]
                    _metadata["dagster/column_lineage"] = MetadataValue.column_lineage(
                        TableColumnLineage(_lineage_deps)
                    )
            context.add_output_metadata(_metadata)
            return df

        from dagster import build_column_schema_change_checks


        _schema_checks = build_column_schema_change_checks(assets=[_asset])


        return Definitions(assets=[_asset], asset_checks=list(_schema_checks))
