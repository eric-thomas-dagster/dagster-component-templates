"""Random Forest Model.

Fit a random forest ensemble model for classification or regression.
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
class RandomForestModelComponent(Component, Model, Resolvable):
    """Fit a random forest ensemble model for classification or regression."""

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame")
    target_column: str = Field(description="Column name of the target variable")
    feature_columns: List[str] = Field(description="List of column names to use as features")
    task_type: str = Field(default="classification", description="Task type: 'classification' or 'regression'")
    n_estimators: int = Field(default=100, description="Number of trees in the forest")
    max_depth: Optional[int] = Field(default=None, description="Maximum depth of each tree (None = unlimited)")
    test_size: float = Field(default=0.2, description="Fraction of data to hold out for evaluation")
    random_state: int = Field(default=42, description="Random seed for reproducibility")
    output_mode: str = Field(default="predictions", description="Output mode: 'predictions' or 'feature_importance'")
    n_jobs: int = Field(default=-1, description="Number of parallel jobs (-1 = use all CPUs)")
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
        return "Fit a random forest ensemble model for classification or regression."

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        target_column = self.target_column
        feature_columns = self.feature_columns
        task_type = self.task_type
        n_estimators = self.n_estimators
        max_depth = self.max_depth
        test_size = self.test_size
        random_state = self.random_state
        output_mode = self.output_mode
        n_jobs = self.n_jobs
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
                from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
                from sklearn.model_selection import train_test_split
            except ImportError as e:
                raise ImportError("scikit-learn is required: pip install scikit-learn") from e

            df = upstream.copy()
            X = df[feature_columns].fillna(0)
            y = df[target_column]

            X_train, X_test, y_train, y_test = train_test_split(
                X, y, test_size=test_size, random_state=random_state
            )

            if task_type == "classification":
                from sklearn.metrics import accuracy_score, classification_report
                model = RandomForestClassifier(
                    n_estimators=n_estimators,
                    max_depth=max_depth,
                    random_state=random_state,
                    n_jobs=n_jobs,
                )
                model.fit(X_train, y_train)
                y_pred = model.predict(X_test)
                accuracy = accuracy_score(y_test, y_pred)
                report = classification_report(y_test, y_pred)
                context.add_output_metadata({
                    "accuracy": MetadataValue.float(float(accuracy)),
                    "classification_report": MetadataValue.text(report),
                    "n_estimators": MetadataValue.int(n_estimators),
                    "train_rows": MetadataValue.int(len(X_train)),
                    "test_rows": MetadataValue.int(len(X_test)),
                })
            elif task_type == "regression":
                from sklearn.metrics import mean_absolute_error, r2_score
                model = RandomForestRegressor(
                    n_estimators=n_estimators,
                    max_depth=max_depth,
                    random_state=random_state,
                    n_jobs=n_jobs,
                )
                model.fit(X_train, y_train)
                y_pred = model.predict(X_test)
                r2 = r2_score(y_test, y_pred)
                mae = mean_absolute_error(y_test, y_pred)
                context.add_output_metadata({
                    "r2_score": MetadataValue.float(float(r2)),
                    "mean_absolute_error": MetadataValue.float(float(mae)),
                    "n_estimators": MetadataValue.int(n_estimators),
                    "train_rows": MetadataValue.int(len(X_train)),
                    "test_rows": MetadataValue.int(len(X_test)),
                })
            else:
                raise ValueError(f"Unknown task_type: {task_type}. Use 'classification' or 'regression'.")

            if output_mode == "feature_importance":
                return pd.DataFrame({
                    "feature": feature_columns,
                    "importance": model.feature_importances_,
                }).sort_values("importance", ascending=False).reset_index(drop=True)
            elif output_mode == "predictions":
                df["predicted"] = model.predict(X)
                return df
            else:
                raise ValueError(f"Unknown output_mode: {output_mode}. Use 'predictions' or 'feature_importance'.")

        return Definitions(assets=[_asset])
