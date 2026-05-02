"""ABControlsComponent.

Given a baseline conversion rate, minimum detectable effect (MDE), significance level, and statistical power, computes the required per-variant sample size for a proportions z-test. Output is a one-row DataFrame summarizing the calc — surface in the catalog before you run the experiment.
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


class ABControlsComponent(Component, Model, Resolvable):
    """Compute required sample size for an A/B test given baseline rate, MDE, and power."""

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame")
    baseline_rate: float = Field(description="Current conversion rate (e.g. 0.10).")
    mde: float = Field(description="Minimum detectable effect, relative (e.g. 0.05 = 5%).")
    alpha: float = Field(default=0.05, description="Significance level.")
    power: float = Field(default=0.8, description="Desired statistical power.")
    test_type: str = Field(default="two_sided", description="'two_sided' or 'one_sided'")

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
            description=self.description or "Compute required sample size for an A/B test given baseline rate, MDE, and power.",
            tags=tags_dict,
            owners=self.owners or [],
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def _asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
            df = upstream
            from scipy import stats
            import math
            p1 = float(_self.baseline_rate)
            p2 = p1 * (1.0 + float(_self.mde))
            z_alpha = stats.norm.ppf(1 - _self.alpha / (2 if _self.test_type == "two_sided" else 1))
            z_beta = stats.norm.ppf(_self.power)
            p_bar = (p1 + p2) / 2
            num = (z_alpha * math.sqrt(2 * p_bar * (1 - p_bar)) + z_beta * math.sqrt(p1 * (1 - p1) + p2 * (1 - p2))) ** 2
            n_per_variant = math.ceil(num / max((p2 - p1) ** 2, 1e-12))
            out_df = pd.DataFrame([{
                "baseline_rate": p1,
                "treatment_rate_target": p2,
                "mde": float(_self.mde),
                "alpha": float(_self.alpha),
                "power": float(_self.power),
                "n_per_variant": int(n_per_variant),
                "n_total": int(n_per_variant * 2),
            }])

            if include_preview and len(out_df) > 0:
                try:
                    _prev = out_df.sample(min(preview_rows, len(out_df))) if len(out_df) > preview_rows * 10 else out_df.head(preview_rows)
                    context.add_output_metadata({"preview": MetadataValue.md(_prev.to_markdown(index=False))})
                except Exception as _e:
                    context.log.warning(f"preview emission failed: {_e}")
            return out_df

        return Definitions(assets=[_asset])
