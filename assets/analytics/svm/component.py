"""SVMComponent.

Fits a Support Vector Classifier or Regressor (sklearn). Configurable kernel, regularization (C), and gamma. Emits per-row predictions plus, for classification, predicted probabilities when the kernel supports them.
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


class SVMComponent(Component, Model, Resolvable):
    """Fit a Support Vector Machine for classification or regression and emit predictions."""

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame")
    target_column: str = Field(description="Target column.")
    feature_columns: List[str] = Field(description="Feature columns.")
    task_type: str = Field(default="classification", description="'classification' or 'regression'")
    kernel: str = Field(default="rbf", description="'linear', 'poly', 'rbf', or 'sigmoid'")
    C: float = Field(default=1.0, description="Regularization parameter")
    gamma: str = Field(default="scale", description="'scale', 'auto', or a float")
    test_size: float = Field(default=0.2, description="Holdout fraction for evaluation.")
    random_state: int = Field(default=42, description="Random seed.")

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
            description=self.description or "Fit a Support Vector Machine for classification or regression and emit predictions.",
            tags=tags_dict,
            owners=self.owners or [],
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def _asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
            df = upstream
            from sklearn.model_selection import train_test_split
            X = df[_self.feature_columns].astype(float)
            y = df[_self.target_column]
            X_train, X_test, y_train, y_test = train_test_split(
                X, y, test_size=_self.test_size, random_state=_self.random_state,
            )
            try:
                gamma = float(_self.gamma)
            except (TypeError, ValueError):
                gamma = _self.gamma
            if _self.task_type == "classification":
                from sklearn.svm import SVC
                m = SVC(kernel=_self.kernel, C=_self.C, gamma=gamma, probability=True, random_state=_self.random_state)
                m.fit(X_train, y_train)
                df = df.copy()
                df["predicted"] = m.predict(X)
                out_df = df
                from sklearn.metrics import accuracy_score
                context.add_output_metadata({
                    "test_accuracy": MetadataValue.float(float(accuracy_score(y_test, m.predict(X_test)))),
                    "kernel": MetadataValue.text(_self.kernel),
                })
            else:
                from sklearn.svm import SVR
                m = SVR(kernel=_self.kernel, C=_self.C, gamma=gamma)
                m.fit(X_train, y_train.astype(float))
                df = df.copy()
                df["predicted"] = m.predict(X)
                out_df = df
                from sklearn.metrics import r2_score
                context.add_output_metadata({
                    "test_r2": MetadataValue.float(float(r2_score(y_test, m.predict(X_test)))),
                    "kernel": MetadataValue.text(_self.kernel),
                })

            if include_preview and len(out_df) > 0:
                try:
                    _prev = out_df.sample(min(preview_rows, len(out_df))) if len(out_df) > preview_rows * 10 else out_df.head(preview_rows)
                    context.add_output_metadata({"preview": MetadataValue.md(_prev.to_markdown(index=False))})
                except Exception as _e:
                    context.log.warning(f"preview emission failed: {_e}")
            return out_df

        return Definitions(assets=[_asset])
