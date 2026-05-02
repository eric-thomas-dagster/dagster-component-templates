"""StepwiseComponent.

Performs sequential forward selection using sklearn's SequentialFeatureSelector. Returns the chosen subset of features along with each candidate's relative score, so a downstream model can fit on a smaller feature set.
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


class StepwiseComponent(Component, Model, Resolvable):
    """Sequential forward feature selection — pick K best features by cross-validated score."""

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame")
    target_column: str = Field(description="Target column.")
    feature_columns: List[str] = Field(description="Candidate feature columns.")
    n_features_to_select: int = Field(default=5, description="Number of features to keep.")
    task_type: str = Field(default="classification", description="'classification' or 'regression'")
    direction: str = Field(default="forward", description="'forward' or 'backward'")
    cv: int = Field(default=5, description="Cross-validation folds.")
    scoring: Optional[str] = Field(default=None, description="sklearn scoring string (e.g. 'r2', 'accuracy').")

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
            description=self.description or "Sequential forward feature selection — pick K best features by cross-validated score.",
            tags=tags_dict,
            owners=self.owners or [],
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def _asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
            df = upstream
            from sklearn.feature_selection import SequentialFeatureSelector
            X = df[_self.feature_columns].astype(float)
            y = df[_self.target_column]
            if _self.task_type == "classification":
                from sklearn.linear_model import LogisticRegression
                est = LogisticRegression(max_iter=1000)
                scoring = _self.scoring or "accuracy"
            else:
                from sklearn.linear_model import LinearRegression
                est = LinearRegression()
                scoring = _self.scoring or "r2"
            sel = SequentialFeatureSelector(est, n_features_to_select=_self.n_features_to_select, direction=_self.direction, cv=_self.cv, scoring=scoring)
            sel.fit(X, y)
            mask = sel.get_support()
            chosen = [c for c, m in zip(_self.feature_columns, mask) if m]
            out_df = pd.DataFrame([{"feature": c, "selected": bool(m), "rank": int(i)+1} for i, (c, m) in enumerate(zip(_self.feature_columns, mask))])
            out_df = out_df.sort_values(["selected", "rank"], ascending=[False, True]).reset_index(drop=True)
            context.add_output_metadata({"selected_features": MetadataValue.text(", ".join(chosen))})

            if include_preview and len(out_df) > 0:
                try:
                    _prev = out_df.sample(min(preview_rows, len(out_df))) if len(out_df) > preview_rows * 10 else out_df.head(preview_rows)
                    context.add_output_metadata({"preview": MetadataValue.md(_prev.to_markdown(index=False))})
                except Exception as _e:
                    context.log.warning(f"preview emission failed: {_e}")
            return out_df

        return Definitions(assets=[_asset])
