"""GammaRegressionComponent.

Fits a Gamma GLM (log link) for strictly-positive continuous outcomes like dollar amounts or durations. Emits per-row predictions. Better than OLS when residual variance scales with the mean.
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


class GammaRegressionComponent(Component, Model, Resolvable):
    """Fit a Gamma generalized linear model for positive continuous outcomes."""

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame")
    target_column: str = Field(description="Strictly-positive continuous target column.")
    feature_columns: List[str] = Field(description="Feature columns.")
    output_mode: str = Field(default="predictions", description="'predictions' or 'coefficients'")

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
            description=self.description or "Fit a Gamma generalized linear model for positive continuous outcomes.",
            tags=tags_dict,
            owners=self.owners or [],
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def _asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
            df = upstream
            import statsmodels.api as sm
            X = df[_self.feature_columns].astype(float)
            y = df[_self.target_column].astype(float)
            Xc = sm.add_constant(X)
            res = sm.GLM(y, Xc, family=sm.families.Gamma(link=sm.families.links.Log())).fit()
            if _self.output_mode == "coefficients":
                rows = []
                for name, coef, se, p in zip(res.params.index, res.params.values, res.bse.values, res.pvalues.values):
                    rows.append({"feature": "intercept" if name == "const" else name,
                                 "coef": float(coef), "std_err": float(se), "p_value": float(p)})
                out_df = pd.DataFrame(rows)
            else:
                df = df.copy()
                df["predicted"] = res.predict(Xc).values
                out_df = df

            if include_preview and len(out_df) > 0:
                try:
                    _prev = out_df.sample(min(preview_rows, len(out_df))) if len(out_df) > preview_rows * 10 else out_df.head(preview_rows)
                    context.add_output_metadata({"preview": MetadataValue.md(_prev.to_markdown(index=False))})
                except Exception as _e:
                    context.log.warning(f"preview emission failed: {_e}")
            return out_df

        return Definitions(assets=[_asset])
