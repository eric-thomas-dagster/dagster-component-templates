"""ModelScoreComponent.

Loads a pickled scikit-learn estimator from disk and runs `predict` (and `predict_proba` for classifiers) on the input DataFrame. Lets you keep model fitting and scoring as separate Dagster assets — fit once, score many times.
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


class ModelScoreComponent(Component, Model, Resolvable):
    """Apply a trained pickled sklearn model to new rows and emit predictions."""

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame")
    model_path: str = Field(description="Path to a pickled sklearn estimator (joblib or pickle).")
    feature_columns: List[str] = Field(description="Feature columns to feed the model.")
    output_column: str = Field(default="predicted", description="Output column for predictions.")
    include_proba: bool = Field(default=False, description="For classifiers: also emit predict_proba columns.")

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
            description=self.description or "Apply a trained pickled sklearn model to new rows and emit predictions.",
            tags=tags_dict,
            owners=self.owners or [],
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def _asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
            df = upstream
            import os
            import pickle
            try:
                import joblib
            except ImportError:
                joblib = None
            path = os.path.expanduser(_self.model_path)
            if joblib is not None and path.endswith((".joblib", ".pkl")):
                try:
                    model = joblib.load(path)
                except Exception:
                    with open(path, "rb") as f:
                        model = pickle.load(f)
            else:
                with open(path, "rb") as f:
                    model = pickle.load(f)
            X = df[_self.feature_columns].astype(float)
            df = df.copy()
            df[_self.output_column] = model.predict(X)
            if _self.include_proba and hasattr(model, "predict_proba"):
                proba = model.predict_proba(X)
                for i, cls in enumerate(getattr(model, "classes_", range(proba.shape[1]))):
                    df[f"proba_{cls}"] = proba[:, i]
            out_df = df

            if include_preview and len(out_df) > 0:
                try:
                    _prev = out_df.sample(min(preview_rows, len(out_df))) if len(out_df) > preview_rows * 10 else out_df.head(preview_rows)
                    context.add_output_metadata({"preview": MetadataValue.md(_prev.to_markdown(index=False))})
                except Exception as _e:
                    context.log.warning(f"preview emission failed: {_e}")
            return out_df

        return Definitions(assets=[_asset])
