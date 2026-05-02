"""SplineModelComponent.

Fits ordinary least squares using sklearn's SplineTransformer to allow smooth non-linear feature effects (alternative to a hand-engineered polynomial). Emits per-row predictions.
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


class SplineModelComponent(Component, Model, Resolvable):
    """Fit an OLS regression with cubic-spline transformed features for smooth non-linear effects."""

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame")
    target_column: str = Field(description="Continuous target column.")
    feature_columns: List[str] = Field(description="Numeric feature columns to transform via splines.")
    n_knots: int = Field(default=5, description="Number of spline knots per feature.")
    degree: int = Field(default=3, description="Spline degree (3 = cubic).")

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
            description=self.description or "Fit an OLS regression with cubic-spline transformed features for smooth non-linear effects.",
            tags=tags_dict,
            owners=self.owners or [],
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def _asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
            df = upstream
            from sklearn.linear_model import LinearRegression
            from sklearn.preprocessing import SplineTransformer
            from sklearn.pipeline import Pipeline
            import numpy as np
            X = df[_self.feature_columns].astype(float).to_numpy()
            y = df[_self.target_column].astype(float).to_numpy()
            pipe = Pipeline([
                ("spline", SplineTransformer(n_knots=_self.n_knots, degree=_self.degree)),
                ("ols", LinearRegression()),
            ])
            pipe.fit(X, y)
            df = df.copy()
            df["predicted"] = pipe.predict(X)
            out_df = df

            if include_preview and len(out_df) > 0:
                try:
                    _prev = out_df.sample(min(preview_rows, len(out_df))) if len(out_df) > preview_rows * 10 else out_df.head(preview_rows)
                    context.add_output_metadata({"preview": MetadataValue.md(_prev.to_markdown(index=False))})
                except Exception as _e:
                    context.log.warning(f"preview emission failed: {_e}")
            return out_df

        return Definitions(assets=[_asset])
