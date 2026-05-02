"""ModelCoefficientsComponent.

Fits an sklearn linear or logistic regression and emits a tidy DataFrame of coefficients with intercept. For OLS it also reports standard errors and p-values via statsmodels. Useful for explainability and quick model diagnostics.
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


class ModelCoefficientsComponent(Component, Model, Resolvable):
    """Fit a linear or logistic regression and emit one row per coefficient (β + std error + p-value)."""

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame")
    target_column: str = Field(description="Target column.")
    feature_columns: List[str] = Field(description="Feature columns.")
    model_kind: str = Field(default="ols", description="'ols' or 'logistic'")

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
            description=self.description or "Fit a linear or logistic regression and emit one row per coefficient (β + std error + p-value).",
            tags=tags_dict,
            owners=self.owners or [],
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def _asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
            df = upstream
            import numpy as np
            X = df[_self.feature_columns].astype(float)
            y = df[_self.target_column].astype(float)
            if _self.model_kind == "logistic":
                from sklearn.linear_model import LogisticRegression
                m = LogisticRegression(max_iter=1000)
                m.fit(X, y.astype(int))
                rows = [{"feature": "intercept", "coef": float(m.intercept_[0]), "std_err": float("nan"), "p_value": float("nan")}]
                for f, c in zip(_self.feature_columns, m.coef_[0]):
                    rows.append({"feature": f, "coef": float(c), "std_err": float("nan"), "p_value": float("nan")})
            else:
                import statsmodels.api as sm
                Xc = sm.add_constant(X)
                res = sm.OLS(y, Xc).fit()
                rows = []
                for name, coef, se, p in zip(res.params.index, res.params.values, res.bse.values, res.pvalues.values):
                    rows.append({"feature": "intercept" if name == "const" else name,
                                 "coef": float(coef), "std_err": float(se), "p_value": float(p)})
            out_df = pd.DataFrame(rows)

            if include_preview and len(out_df) > 0:
                try:
                    _prev = out_df.sample(min(preview_rows, len(out_df))) if len(out_df) > preview_rows * 10 else out_df.head(preview_rows)
                    context.add_output_metadata({"preview": MetadataValue.md(_prev.to_markdown(index=False))})
                except Exception as _e:
                    context.log.warning(f"preview emission failed: {_e}")
            return out_df

        return Definitions(assets=[_asset])
