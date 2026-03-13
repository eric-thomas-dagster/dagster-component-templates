"""Oversample Field.

Balance imbalanced datasets using SMOTE, random oversampling, or ADASYN.
"""
from dataclasses import dataclass
from typing import List, Optional, Union

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
class OversampleFieldComponent(Component, Model, Resolvable):
    """Balance imbalanced datasets using SMOTE, random oversampling, or ADASYN."""

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame")
    target_column: str = Field(description="Binary or categorical target column to balance")
    method: str = Field(default="smote", description="Oversampling method: 'smote', 'random', or 'adasyn'")
    sampling_strategy: Union[str, float] = Field(
        default="auto",
        description="Sampling strategy: 'auto', 'minority', 'majority', or a float ratio",
    )
    random_state: int = Field(default=42, description="Random seed for reproducibility")
    feature_columns: Optional[List[str]] = Field(
        default=None,
        description="Feature columns to use for resampling (None = all columns except target)",
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

    @classmethod
    def get_description(cls) -> str:
        return "Balance imbalanced datasets using SMOTE, random oversampling, or ADASYN."

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        target_column = self.target_column
        method = self.method
        sampling_strategy = self.sampling_strategy
        random_state = self.random_state
        feature_columns = self.feature_columns
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
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
            partitions_def=partitions_def,
            group_name=group_name,
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
                from imblearn.over_sampling import SMOTE, RandomOverSampler, ADASYN
            except ImportError as e:
                raise ImportError(
                    "imbalanced-learn is required: pip install imbalanced-learn"
                ) from e

            sampler_map = {
                "smote": SMOTE,
                "random": RandomOverSampler,
                "adasyn": ADASYN,
            }
            if method not in sampler_map:
                raise ValueError(f"Unknown method: {method}. Use 'smote', 'random', or 'adasyn'.")

            feat_cols = feature_columns or [c for c in upstream.columns if c != target_column]

            X = upstream[feat_cols].fillna(0)
            y = upstream[target_column]

            original_counts = y.value_counts().to_dict()

            sampler = sampler_map[method](
                sampling_strategy=sampling_strategy,
                random_state=random_state,
            )
            X_res, y_res = sampler.fit_resample(X, y)

            result = pd.DataFrame(X_res, columns=feat_cols)
            result[target_column] = y_res

            resampled_counts = pd.Series(y_res).value_counts().to_dict()

            context.add_output_metadata({
                "method": MetadataValue.text(method),
                "original_rows": MetadataValue.int(len(upstream)),
                "resampled_rows": MetadataValue.int(len(result)),
                "rows_added": MetadataValue.int(len(result) - len(upstream)),
                "original_class_distribution": MetadataValue.text(str(original_counts)),
                "resampled_class_distribution": MetadataValue.text(str(resampled_counts)),
            })

            return result

        return Definitions(assets=[_asset])
