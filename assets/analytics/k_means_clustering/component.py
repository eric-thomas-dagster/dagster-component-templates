"""K-Means Clustering.

Cluster records using K-means and assign cluster IDs to each row.
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
class KMeansClusteringComponent(Component, Model, Resolvable):
    """Cluster records using K-means and assign cluster IDs."""

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame")
    feature_columns: List[str] = Field(description="List of column names to use as clustering features")
    n_clusters: int = Field(default=5, description="Number of clusters (K)")
    output_column: str = Field(default="cluster", description="Column name to store cluster assignment")
    normalize: bool = Field(default=True, description="Standardize features with StandardScaler before clustering")
    random_state: int = Field(default=42, description="Random seed for reproducibility")
    max_iter: int = Field(default=300, description="Maximum number of K-means iterations")
    include_distance: bool = Field(default=False, description="Add a column with distance to the nearest centroid")
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
        return "Cluster records using K-means and assign cluster IDs to each row."

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        feature_columns = self.feature_columns
        n_clusters = self.n_clusters
        output_column = self.output_column
        normalize = self.normalize
        random_state = self.random_state
        max_iter = self.max_iter
        include_distance = self.include_distance
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
                from sklearn.cluster import KMeans
                from sklearn.preprocessing import StandardScaler
            except ImportError as e:
                raise ImportError("scikit-learn is required: pip install scikit-learn") from e

            df = upstream.copy()
            X = df[feature_columns].fillna(0)

            if normalize:
                scaler = StandardScaler()
                X_scaled = scaler.fit_transform(X)
            else:
                X_scaled = X.values

            km = KMeans(n_clusters=n_clusters, random_state=random_state, max_iter=max_iter)
            df[output_column] = km.fit_predict(X_scaled)

            if include_distance:
                distances = km.transform(X_scaled)
                df[f"{output_column}_distance"] = distances.min(axis=1)

            cluster_sizes = df[output_column].value_counts().to_dict()
            context.add_output_metadata({
                "n_clusters": MetadataValue.int(n_clusters),
                "inertia": MetadataValue.float(float(km.inertia_)),
                "n_iterations": MetadataValue.int(int(km.n_iter_)),
                "cluster_sizes": MetadataValue.json(cluster_sizes),
                "rows": MetadataValue.int(len(df)),
            })

            return df

        return Definitions(assets=[_asset])
