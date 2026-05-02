"""AppendClusterComponent.

Trains a k-means model in-place on the input DataFrame and appends a `cluster` column with the assigned cluster ID per row. Lighter alternative to `k_means_clustering` when you don't need the centroid distances and just want the label as a feature.
"""
from typing import Dict, List, Optional

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


class AppendClusterComponent(Component, Model, Resolvable):
    """Cluster rows with k-means and append the cluster label as a new column."""

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame")
    feature_columns: List[str] = Field(description="Numeric feature columns to cluster on.")
    n_clusters: int = Field(default=4, description="Number of clusters.")
    output_column: str = Field(default="cluster", description="Output column for cluster ID.")
    random_state: int = Field(default=42, description="Random seed.")
    normalize: bool = Field(default=True, description="StandardScaler features before clustering.")

    include_preview_metadata: bool = Field(
        default=False,
        description="Include a preview of the output DataFrame in metadata (for builder UIs).",
    )
    preview_rows: int = Field(
        default=25,
        ge=1,
        le=500,
        description="Rows in the preview when include_preview_metadata=True.",
    )
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    asset_tags: Optional[Dict[str, str]] = Field(default=None, description="Asset tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds.")
    deps: Optional[List[str]] = Field(default=None, description="Lineage-only upstream asset keys.")

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows
        group_name = self.group_name
        _self = self

        ins = {"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))}
        tags_dict = dict(self.asset_tags or {})
        for k in (self.kinds or ["python"]):
            tags_dict[f"dagster/kind/{k}"] = ""

        @asset(
            name=asset_name,
            ins=ins,
            group_name=group_name,
            description=self.description or "Cluster rows with k-means and append the cluster label as a new column.",
            tags=tags_dict,
            owners=self.owners or [],
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def _asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
            df = upstream
            from sklearn.cluster import KMeans
            from sklearn.preprocessing import StandardScaler
            X = df[_self.feature_columns].astype(float).to_numpy()
            if _self.normalize:
                X = StandardScaler().fit_transform(X)
            km = KMeans(n_clusters=_self.n_clusters, random_state=_self.random_state, n_init=10)
            labels = km.fit_predict(X)
            df = df.copy()
            df[_self.output_column] = labels
            out_df = df
            context.add_output_metadata({
                "n_clusters": MetadataValue.int(int(_self.n_clusters)),
                "inertia": MetadataValue.float(float(km.inertia_)),
            })

            if include_preview and len(out_df) > 0:
                try:
                    _prev = out_df.sample(min(preview_rows, len(out_df))) if len(out_df) > preview_rows * 10 else out_df.head(preview_rows)
                    context.add_output_metadata({"preview": MetadataValue.md(_prev.to_markdown(index=False))})
                except Exception as _e:
                    context.log.warning(f"preview emission failed: {_e}")
            return out_df

        return Definitions(assets=[_asset])
