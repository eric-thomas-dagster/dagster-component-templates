"""Document Layout Analyzer Component.

Analyze the layout of document images to detect regions: text blocks, tables, figures, headers.
"""

from dataclasses import dataclass
from typing import Optional, List
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
class DocumentLayoutAnalyzerComponent(Component, Model, Resolvable):
    """Component for analyzing the layout of document images.

    Detects regions such as text blocks, tables, figures, and headers using
    document layout analysis models (LayoutLM or similar HuggingFace models).

    Features:
    - Detect text/table/figure/header regions with bounding boxes
    - Optional region type filtering
    - HuggingFace pipeline integration
    - CPU, CUDA, and MPS device support

    Use Cases:
    - PDF document structure extraction
    - Table detection for data extraction pipelines
    - Document digitization quality assessment
    - Automated document classification
    """

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(
        description="Upstream asset key providing a DataFrame"
    )
    image_column: str = Field(description="Column with document image file paths")
    output_column: str = Field(
        default="layout_regions",
        description="Column for list of {type, bbox, confidence} dicts",
    )
    model_name: str = Field(
        default="microsoft/layoutlmv3-base",
        description="HuggingFace layout analysis model ID",
    )
    region_types: Optional[List[str]] = Field(
        default=None,
        description="Filter to these region types e.g. ['table','figure']",
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
        image_column = self.image_column
        output_column = self.output_column
        model_name = self.model_name
        region_types = self.region_types
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
                from transformers import pipeline
            except ImportError:
                raise ImportError("transformers required: pip install transformers")
            try:
                from PIL import Image
            except ImportError:
                raise ImportError("Pillow required: pip install Pillow")

            df = upstream.copy()
            context.log.info(f"Loading document layout model {model_name}")

            # Use object-detection pipeline which works for document layout models
            device_id = 0 if device == "cuda" else -1
            try:
                layout_pipeline = pipeline(
                    "object-detection",
                    model=model_name,
                    device=device_id,
                )
            except Exception as e:
                context.log.warning(
                    f"Could not load model {model_name} as object-detection pipeline: {e}. "
                    "Falling back to image-classification."
                )
                layout_pipeline = pipeline(
                    "image-classification",
                    model=model_name,
                    device=device_id,
                )

            all_regions = []
            region_counts = []

            for path in df[image_column]:
                try:
                    img = Image.open(str(path)).convert("RGB")
                    raw = layout_pipeline(img)
                    regions = []

                    if isinstance(raw, list) and raw and "box" in raw[0]:
                        # object-detection format
                        for det in raw:
                            region_type = det.get("label", "unknown")
                            if region_types and region_type not in region_types:
                                continue
                            regions.append(
                                {
                                    "type": region_type,
                                    "confidence": round(float(det["score"]), 4),
                                    "bbox": {
                                        k: round(float(v), 2)
                                        for k, v in det["box"].items()
                                    },
                                }
                            )
                    else:
                        # classification fallback: return top label as single region
                        if isinstance(raw, list) and raw:
                            top = raw[0]
                            region_type = top.get("label", "unknown")
                            if not region_types or region_type in region_types:
                                regions.append(
                                    {
                                        "type": region_type,
                                        "confidence": round(float(top.get("score", 0.0)), 4),
                                        "bbox": None,
                                    }
                                )

                    all_regions.append(regions)
                    region_counts.append(len(regions))
                except Exception as e:
                    context.log.warning(f"Layout analysis failed for {path}: {e}")
                    all_regions.append([])
                    region_counts.append(0)

            df[output_column] = all_regions
            df["region_count"] = region_counts

            context.add_output_metadata(
                {
                    "row_count": MetadataValue.int(len(df)),
                    "model": model_name,
                    "total_regions": MetadataValue.int(sum(region_counts)),
                    "preview": MetadataValue.md(df[[image_column, "region_count"]].head(5).to_markdown()),
                }
            )
            return df

        return Definitions(assets=[_asset])
