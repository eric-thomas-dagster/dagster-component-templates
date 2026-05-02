"""MultidimensionalScalingComponent.

Applies Multidimensional Scaling (sklearn.manifold.MDS) to compress a high-dimensional feature space into 2 or 3 components, preserving pairwise distances as much as possible. Output keeps the original rows with new MDS columns appended.
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


class MultidimensionalScalingComponent(Component, Model, Resolvable):
    """Reduce feature dimensions to 2D/3D via metric MDS for visualization."""

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame")
    feature_columns: List[str] = Field(description="Numeric feature columns.")
    n_components: int = Field(default=2, description="Output dimensions (2 or 3).")
    random_state: int = Field(default=42, description="Random seed.")
    metric: bool = Field(default=True, description="True = metric MDS, False = non-metric.")

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
            description=self.description or "Reduce feature dimensions to 2D/3D via metric MDS for visualization.",
            tags=tags_dict,
            owners=self.owners or [],
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def _asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
            df = upstream
            from sklearn.manifold import MDS
            X = df[_self.feature_columns].astype(float).to_numpy()
            mds = MDS(n_components=_self.n_components, random_state=_self.random_state, metric=_self.metric, normalized_stress="auto")
            coords = mds.fit_transform(X)
            df = df.copy()
            for i in range(_self.n_components):
                df[f"mds_{i+1}"] = coords[:, i]
            out_df = df
            context.add_output_metadata({"stress": MetadataValue.float(float(mds.stress_))})

            if include_preview and len(out_df) > 0:
                try:
                    _prev = out_df.sample(min(preview_rows, len(out_df))) if len(out_df) > preview_rows * 10 else out_df.head(preview_rows)
                    context.add_output_metadata({"preview": MetadataValue.md(_prev.to_markdown(index=False))})
                except Exception as _e:
                    context.log.warning(f"preview emission failed: {_e}")
            return out_df

        return Definitions(assets=[_asset])
