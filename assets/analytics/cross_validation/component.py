"""Cross Validation.

Run k-fold cross-validation on a dataset and return per-fold train/test scores.
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
class CrossValidationComponent(Component, Model, Resolvable):
    """Run k-fold cross-validation on a dataset and return per-fold train/test scores."""

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame")
    target_column: str = Field(description="Target column name")
    feature_columns: List[str] = Field(description="Feature columns to use")
    model_type: str = Field(
        default="random_forest",
        description="Model to evaluate: 'random_forest', 'logistic_regression', 'gradient_boosting', 'decision_tree', 'linear_regression'",
    )
    task_type: str = Field(default="classification", description="'classification' or 'regression'")
    cv_folds: int = Field(default=5, description="Number of cross-validation folds")
    scoring: Optional[str] = Field(
        default=None,
        description="sklearn scoring string (None = 'accuracy' for classification, 'r2' for regression)",
    )
    random_state: int = Field(default=42, description="Random seed for reproducibility")
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
        return "Run k-fold cross-validation on a dataset and return per-fold train/test scores."

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        target_column = self.target_column
        feature_columns = self.feature_columns
        model_type = self.model_type
        task_type = self.task_type
        cv_folds = self.cv_folds
        scoring = self.scoring
        random_state = self.random_state
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
                from sklearn.model_selection import cross_validate, KFold
                from sklearn.ensemble import (
                    RandomForestClassifier,
                    RandomForestRegressor,
                    GradientBoostingClassifier,
                    GradientBoostingRegressor,
                )
                from sklearn.linear_model import LogisticRegression, LinearRegression
                from sklearn.tree import DecisionTreeClassifier, DecisionTreeRegressor
            except ImportError as e:
                raise ImportError("scikit-learn is required: pip install scikit-learn") from e

            model_map = {
                "random_forest": RandomForestClassifier if task_type == "classification" else RandomForestRegressor,
                "logistic_regression": LogisticRegression if task_type == "classification" else LinearRegression,
                "gradient_boosting": GradientBoostingClassifier if task_type == "classification" else GradientBoostingRegressor,
                "decision_tree": DecisionTreeClassifier if task_type == "classification" else DecisionTreeRegressor,
                "linear_regression": LinearRegression,
            }

            if model_type not in model_map:
                raise ValueError(
                    f"Unknown model_type: {model_type}. "
                    "Use 'random_forest', 'logistic_regression', 'gradient_boosting', "
                    "'decision_tree', or 'linear_regression'."
                )

            model_cls = model_map[model_type]
            try:
                model = model_cls(random_state=random_state)
            except TypeError:
                # Some estimators (e.g. LinearRegression) don't accept random_state
                model = model_cls()

            X = upstream[feature_columns].fillna(0)
            y = upstream[target_column]

            cv = KFold(n_splits=cv_folds, shuffle=True, random_state=random_state)
            scoring_metric = scoring or ("accuracy" if task_type == "classification" else "r2")

            scores = cross_validate(
                model, X, y, cv=cv, scoring=scoring_metric, return_train_score=True
            )

            result = pd.DataFrame({
                "fold": range(1, cv_folds + 1),
                "train_score": scores["train_score"],
                "test_score": scores["test_score"],
                "fit_time": scores["fit_time"],
            })

            context.add_output_metadata({
                "mean_test_score": MetadataValue.float(float(result["test_score"].mean())),
                "std_test_score": MetadataValue.float(float(result["test_score"].std())),
                "mean_train_score": MetadataValue.float(float(result["train_score"].mean())),
                "scoring_metric": MetadataValue.text(scoring_metric),
                "cv_folds": MetadataValue.int(cv_folds),
                "model_type": MetadataValue.text(model_type),
            })

            return result

        return Definitions(assets=[_asset])
