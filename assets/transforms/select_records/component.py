"""SelectRecordsComponent.

Pick a contiguous range of rows from a DataFrame: by absolute index, by head/tail count, or by specific indices. A simple cousin of `filter` that doesn't require a predicate — useful for paginated extracts and quick previews.
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


class SelectRecordsComponent(Component, Model, Resolvable):
    """Select rows by index range, head/tail, or specific row indices."""

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame")
    mode: str = Field(default="range", description="'range', 'head', 'tail', or 'indices'")
    start: int = Field(default=0, description="Starting row index for 'range' mode.")
    end: Optional[int] = Field(default=None, description="Ending row index (exclusive) for 'range' mode.")
    n: int = Field(default=10, description="Row count for 'head' or 'tail' mode.")
    indices: Optional[List[int]] = Field(default=None, description="Specific row indices for 'indices' mode.")

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
            description=self.description or "Select rows by index range, head/tail, or specific row indices.",
            tags=tags_dict,
            owners=self.owners or [],
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def _asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
            df = upstream
            if _self.mode == "head":
                out_df = df.head(_self.n)
            elif _self.mode == "tail":
                out_df = df.tail(_self.n)
            elif _self.mode == "indices":
                out_df = df.iloc[_self.indices or []]
            else:
                end = _self.end if _self.end is not None else len(df)
                out_df = df.iloc[_self.start : end]
            out_df = out_df.reset_index(drop=True)

            if include_preview and len(out_df) > 0:
                try:
                    _prev = out_df.sample(min(preview_rows, len(out_df))) if len(out_df) > preview_rows * 10 else out_df.head(preview_rows)
                    context.add_output_metadata({"preview": MetadataValue.md(_prev.to_markdown(index=False))})
                except Exception as _e:
                    context.log.warning(f"preview emission failed: {_e}")
            return out_df

        return Definitions(assets=[_asset])
