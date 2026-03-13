"""Neural Network Model.

Fit a multi-layer perceptron (MLP) neural network for classification or regression.
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
class NeuralNetworkModelComponent(Component, Model, Resolvable):
    """Fit a multi-layer perceptron (MLP) neural network for classification or regression."""

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame")
    target_column: str = Field(description="Target column to predict")
    feature_columns: List[str] = Field(description="Feature columns to use")
    task_type: str = Field(default="classification", description="'classification' or 'regression'")
    hidden_layer_sizes: List[int] = Field(
        default_factory=lambda: [100],
        description="Neurons per hidden layer, e.g. [128, 64] for two hidden layers",
    )
    activation: str = Field(default="relu", description="Activation function: 'relu', 'tanh', or 'logistic'")
    max_iter: int = Field(default=500, description="Maximum number of training iterations")
    learning_rate_init: float = Field(default=0.001, description="Initial learning rate")
    normalize: bool = Field(default=True, description="Apply StandardScaler to features before training")
    test_size: float = Field(default=0.2, description="Fraction of data to hold out for evaluation")
    random_state: int = Field(default=42, description="Random seed for reproducibility")
    output_mode: str = Field(
        default="predictions",
        description="'predictions' or 'probabilities' (probabilities only for classification)",
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
        return "Fit a multi-layer perceptron (MLP) neural network for classification or regression."

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        target_column = self.target_column
        feature_columns = self.feature_columns
        task_type = self.task_type
        hidden_layer_sizes = self.hidden_layer_sizes
        activation = self.activation
        max_iter = self.max_iter
        learning_rate_init = self.learning_rate_init
        normalize = self.normalize
        test_size = self.test_size
        random_state = self.random_state
        output_mode = self.output_mode
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
                from sklearn.neural_network import MLPClassifier, MLPRegressor
                from sklearn.preprocessing import StandardScaler
                from sklearn.model_selection import train_test_split
                from sklearn.metrics import accuracy_score, r2_score
            except ImportError as e:
                raise ImportError("scikit-learn is required: pip install scikit-learn") from e

            X = upstream[feature_columns].fillna(0).values
            y = upstream[target_column].values

            X_train, X_test, y_train, y_test = train_test_split(
                X, y, test_size=test_size, random_state=random_state
            )

            if normalize:
                scaler = StandardScaler()
                X_train = scaler.fit_transform(X_train)
                X_test = scaler.transform(X_test)
                X_all = scaler.transform(X)
            else:
                X_all = X

            if task_type == "classification":
                model = MLPClassifier(
                    hidden_layer_sizes=tuple(hidden_layer_sizes),
                    activation=activation,
                    max_iter=max_iter,
                    learning_rate_init=learning_rate_init,
                    random_state=random_state,
                )
            else:
                model = MLPRegressor(
                    hidden_layer_sizes=tuple(hidden_layer_sizes),
                    activation=activation,
                    max_iter=max_iter,
                    learning_rate_init=learning_rate_init,
                    random_state=random_state,
                )

            model.fit(X_train, y_train)

            df = upstream.copy()
            df["predicted"] = model.predict(X_all)

            if output_mode == "probabilities" and task_type == "classification":
                for i, cls in enumerate(model.classes_):
                    df[f"proba_{cls}"] = model.predict_proba(X_all)[:, i]

            if task_type == "classification":
                score = accuracy_score(y_test, model.predict(X_test))
                context.add_output_metadata({
                    "accuracy": MetadataValue.float(float(score)),
                    "n_iter": MetadataValue.int(model.n_iter_),
                    "train_rows": MetadataValue.int(len(X_train)),
                    "test_rows": MetadataValue.int(len(X_test)),
                })
            else:
                score = r2_score(y_test, model.predict(X_test))
                context.add_output_metadata({
                    "r2_score": MetadataValue.float(float(score)),
                    "n_iter": MetadataValue.int(model.n_iter_),
                    "train_rows": MetadataValue.int(len(X_train)),
                    "test_rows": MetadataValue.int(len(X_test)),
                })

            return df

        return Definitions(assets=[_asset])
