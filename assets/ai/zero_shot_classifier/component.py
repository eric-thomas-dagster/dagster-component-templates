"""Zero Shot Classifier Component.

Classify text into arbitrary categories using HuggingFace zero-shot classification models.
No training data required.
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
class ZeroShotClassifierComponent(Component, Model, Resolvable):
    """Component for zero-shot text classification using HuggingFace transformers.

    Classifies each text row into one or more of the provided candidate labels
    without requiring any fine-tuning or labelled training data.

    Features:
    - Any HuggingFace zero-shot-classification model
    - Multi-label classification support
    - Per-label confidence scores
    - Configurable batch size for throughput
    - Null-safe text handling

    Use Cases:
    - Content categorization without labelled data
    - Intent detection
    - Topic tagging
    - Urgency / priority scoring
    """

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame")
    text_column: str = Field(description="Column containing text to classify")
    candidate_labels: List[str] = Field(
        description="Categories to classify into e.g. ['positive', 'negative', 'neutral']"
    )
    model_name: str = Field(
        default="facebook/bart-large-mnli",
        description="HuggingFace zero-shot classification model",
    )
    output_column: str = Field(
        default="predicted_label", description="Column name for the top predicted label"
    )
    output_scores: bool = Field(
        default=True, description="Add a score column per candidate label"
    )
    multi_label: bool = Field(
        default=False, description="Allow multiple labels per text (sigmoid instead of softmax)"
    )
    batch_size: int = Field(default=8, description="Number of texts per inference batch")
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
        text_column = self.text_column
        candidate_labels = self.candidate_labels
        model_name = self.model_name
        output_column = self.output_column
        output_scores = self.output_scores
        multi_label = self.multi_label
        batch_size = self.batch_size

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
                raise ImportError("transformers required: pip install transformers torch")

            context.log.info(
                f"Loading zero-shot classifier '{model_name}' for {len(upstream)} rows"
            )
            classifier = pipeline("zero-shot-classification", model=model_name)

            df = upstream.copy()
            texts = df[text_column].fillna("").astype(str).tolist()
            results = []

            for i in range(0, len(texts), batch_size):
                batch = texts[i : i + batch_size]
                context.log.info(
                    f"Classifying batch {i // batch_size + 1}/{(len(texts) - 1) // batch_size + 1}"
                )
                batch_results = classifier(
                    batch, candidate_labels=candidate_labels, multi_label=multi_label
                )
                if isinstance(batch_results, dict):
                    batch_results = [batch_results]
                results.extend(batch_results)

            df[output_column] = [r["labels"][0] for r in results]

            if output_scores:
                for label in candidate_labels:
                    df[f"score_{label}"] = [
                        dict(zip(r["labels"], r["scores"])).get(label, 0.0) for r in results
                    ]

            label_counts = df[output_column].value_counts().to_dict()
            context.log.info(f"Classification complete: {label_counts}")

            context.add_output_metadata(
                {
                    "num_rows": len(df),
                    "model": model_name,
                    "labels": candidate_labels,
                    "label_distribution": label_counts,
                    "preview": MetadataValue.md(df.head(5).to_markdown()),
                }
            )
            return df

        return Definitions(assets=[_asset])
