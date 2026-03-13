"""Spatial Cluster Component.

Cluster geographic points using DBSCAN (density-based, handles noise and
arbitrary shapes) or K-means (fixed number of spherical clusters).
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
class SpatialClusterComponent(Component, Model, Resolvable):
    """Cluster geographic points using DBSCAN or K-means.

    Assigns a cluster label to each row based on lat/lng proximity. DBSCAN
    uses a haversine distance metric and can label noise points as -1.
    K-means partitions into a fixed number of clusters.

    Example:
        ```yaml
        type: dagster_component_templates.SpatialClusterComponent
        attributes:
          asset_name: events_clustered
          upstream_asset_key: events
          lat_column: latitude
          lng_column: longitude
          algorithm: dbscan
          eps_km: 0.5
          min_samples: 5
          output_column: spatial_cluster
          group_name: geospatial
        ```
    """

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame with lat/lng columns")
    lat_column: str = Field(default="latitude", description="Column name containing latitude values")
    lng_column: str = Field(default="longitude", description="Column name containing longitude values")
    algorithm: str = Field(
        default="dbscan",
        description="Clustering algorithm: 'dbscan' (density-based, finds noise) or 'kmeans' (fixed k clusters)"
    )
    eps_km: float = Field(
        default=1.0,
        description="DBSCAN only: maximum distance in km between neighborhood points"
    )
    min_samples: int = Field(
        default=3,
        description="DBSCAN only: minimum number of points to form a dense region"
    )
    n_clusters: int = Field(
        default=5,
        description="K-means only: number of clusters to form"
    )
    output_column: str = Field(
        default="spatial_cluster",
        description="Column name for cluster labels (-1 indicates noise in DBSCAN)"
    )
    include_cluster_center: bool = Field(
        default=False,
        description="If True, add cluster centroid lat/lng columns (cluster_center_lat, cluster_center_lng)"
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
        return "Cluster geographic points using DBSCAN or K-means."

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        lat_column = self.lat_column
        lng_column = self.lng_column
        algorithm = self.algorithm
        eps_km = self.eps_km
        min_samples = self.min_samples
        n_clusters = self.n_clusters
        output_column = self.output_column
        include_cluster_center = self.include_cluster_center
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
                import numpy as np
                from sklearn.cluster import DBSCAN, KMeans
            except ImportError:
                raise ImportError("scikit-learn and numpy are required: pip install scikit-learn numpy")

            coords = upstream[[lat_column, lng_column]].values

            if algorithm == "dbscan":
                eps_rad = eps_km / 6371.0
                coords_rad = np.radians(coords)
                db = DBSCAN(
                    eps=eps_rad,
                    min_samples=min_samples,
                    algorithm="ball_tree",
                    metric="haversine",
                )
                labels = db.fit_predict(coords_rad)
                n_found = len(set(labels)) - (1 if -1 in labels else 0)
                noise_count = int((labels == -1).sum())
                context.log.info(
                    f"DBSCAN: {n_found} clusters found, {noise_count} noise points "
                    f"(eps={eps_km}km, min_samples={min_samples})"
                )
            else:
                km = KMeans(n_clusters=n_clusters, random_state=42)
                labels = km.fit_predict(coords)
                context.log.info(f"K-means: {n_clusters} clusters assigned to {len(upstream)} points")

            df = upstream.copy()
            df[output_column] = labels

            if include_cluster_center:
                center_lats = []
                center_lngs = []
                label_to_center = {}
                for label in set(labels):
                    if label == -1:
                        label_to_center[label] = (None, None)
                    else:
                        mask = labels == label
                        label_to_center[label] = (
                            float(coords[mask, 0].mean()),
                            float(coords[mask, 1].mean()),
                        )
                for label in labels:
                    clat, clng = label_to_center[label]
                    center_lats.append(clat)
                    center_lngs.append(clng)
                df["cluster_center_lat"] = center_lats
                df["cluster_center_lng"] = center_lngs

            cluster_sizes = pd.Series(labels).value_counts().to_dict()
            unique_clusters = len(set(labels)) - (1 if -1 in labels else 0)
            context.add_output_metadata({
                "rows": MetadataValue.int(len(df)),
                "algorithm": MetadataValue.text(algorithm),
                "unique_clusters": MetadataValue.int(unique_clusters),
                "cluster_sizes": MetadataValue.json({str(k): v for k, v in cluster_sizes.items()}),
            })
            return df

        return Definitions(assets=[_asset])
