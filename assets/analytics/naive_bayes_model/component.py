"""Naive Bayes Model.

Fit a Naive Bayes classifier and output predictions and/or class probabilities.
"""
from dataclasses import dataclass
from typing import List, Optional

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
class NaiveBayesModelComponent(Component, Model, Resolvable):
    """Fit a Naive Bayes classifier and output predictions and/or class probabilities."""

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame")
    target_column: str = Field(description="Target column to predict")
    feature_columns: List[str] = Field(description="Feature columns to use")
    variant: str = Field(
        default="gaussian",
        description="Naive Bayes variant: 'gaussian' (continuous), 'multinomial' (counts), 'bernoulli' (binary)",
    )
    test_size: float = Field(default=0.2, description="Fraction of data to hold out for evaluation")
    random_state: int = Field(default=42, description="Random seed for reproducibility")
    output_predictions: bool = Field(default=True, description="Append predicted_class column to output")
    output_probabilities: bool = Field(default=True, description="Append per-class probability columns to output")
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
        return "Fit a Naive Bayes classifier and output predictions and/or class probabilities."

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        target_column = self.target_column
        feature_columns = self.feature_columns
        variant = self.variant
        test_size = self.test_size
        random_state = self.random_state
        output_predictions = self.output_predictions
        output_probabilities = self.output_probabilities
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
                from sklearn.naive_bayes import GaussianNB, MultinomialNB, BernoulliNB
                from sklearn.model_selection import train_test_split
                from sklearn.metrics import accuracy_score
            except ImportError as e:
                raise ImportError("scikit-learn is required: pip install scikit-learn") from e

            nb_map = {
                "gaussian": GaussianNB,
                "multinomial": MultinomialNB,
                "bernoulli": BernoulliNB,
            }
            if variant not in nb_map:
                raise ValueError(f"Unknown variant: {variant}. Use 'gaussian', 'multinomial', or 'bernoulli'.")

            X = upstream[feature_columns].fillna(0)
            y = upstream[target_column]

            X_train, X_test, y_train, y_test = train_test_split(
                X, y, test_size=test_size, random_state=random_state
            )

            model = nb_map[variant]()
            model.fit(X_train, y_train)

            df = upstream.copy()
            if output_predictions:
                df["predicted_class"] = model.predict(X)
            if output_probabilities:
                proba = model.predict_proba(X)
                for i, cls in enumerate(model.classes_):
                    df[f"proba_{cls}"] = proba[:, i]

            acc = accuracy_score(y_test, model.predict(X_test))
            context.add_output_metadata({
                "accuracy": MetadataValue.float(float(acc)),
                "variant": MetadataValue.text(variant),
                "n_classes": MetadataValue.int(len(model.classes_)),
                "train_rows": MetadataValue.int(len(X_train)),
                "test_rows": MetadataValue.int(len(X_test)),
            })

            return df

        return Definitions(assets=[_asset])
