"""Image Similarity Scorer Component.

Score visual similarity between images using CLIP embeddings.
"""

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
    MetadataValue,
    Model,
    Resolvable,
    asset,
)
from pydantic import Field


@dataclass
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

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        image_column_a = self.image_column_a
        image_column_b = self.image_column_b
        reference_image = self.reference_image
        output_column = self.output_column
        model_name = self.model_name
        device = self.device

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
            return df

        return Definitions(assets=[_asset])
