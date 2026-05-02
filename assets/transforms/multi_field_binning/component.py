"""MultiFieldBinningComponent.

Apply the same binning logic to many columns in one shot. Each column gets a sibling `_bin` column (e.g. `age_bin`, `income_bin`) holding the bin label. Quantile bins use n equal-frequency tiles; width bins use equal-range cuts.
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


class MultiFieldBinningComponent(Component, Model, Resolvable):
    """Bin multiple numeric columns at once into quantile or fixed-width bins."""

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame")
    columns: List[str] = Field(description="Numeric columns to bin.")
    n_bins: int = Field(default=4, description="Number of bins per column.")
    method: str = Field(default="quantile", description="'quantile' (qcut) or 'width' (cut)")
    suffix: str = Field(default="_bin", description="Suffix for the new bin columns.")
    labels: Optional[List[str]] = Field(default=None, description="Custom bin labels. None = integer 0..n-1.")

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
            description=self.description or "Bin multiple numeric columns at once into quantile or fixed-width bins.",
            tags=tags_dict,
            owners=self.owners or [],
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def _asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
            df = upstream
            df = df.copy()
            labels = _self.labels if _self.labels else False
            for c in _self.columns:
                if _self.method == "width":
                    df[f"{c}{_self.suffix}"] = pd.cut(df[c], bins=_self.n_bins, labels=labels, include_lowest=True)
                else:
                    df[f"{c}{_self.suffix}"] = pd.qcut(df[c], q=_self.n_bins, labels=labels, duplicates="drop")
            out_df = df

            if include_preview and len(out_df) > 0:
                try:
                    _prev = out_df.sample(min(preview_rows, len(out_df))) if len(out_df) > preview_rows * 10 else out_df.head(preview_rows)
                    context.add_output_metadata({"preview": MetadataValue.md(_prev.to_markdown(index=False))})
                except Exception as _e:
                    context.log.warning(f"preview emission failed: {_e}")
            return out_df

        return Definitions(assets=[_asset])
