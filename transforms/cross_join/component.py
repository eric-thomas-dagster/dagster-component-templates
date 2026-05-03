"""CrossJoinComponent.

Cartesian product of two DataFrames. Optional row-count guard to prevent memory blowups.
"""

import os
from typing import Optional

import dagster as dg
import pandas as pd
from pydantic import Field


class CrossJoinComponent(dg.Component, dg.Model, dg.Resolvable):
    """Cartesian product of two DataFrames. Optional row-count guard to prevent memory blowups."""

    asset_name: str = Field(description="Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream DataFrame asset key")
    upstream_right_key: str = Field(description="Secondary upstream DataFrame key (see body)")

    upstream_right_key: str = Field(description="Right-side DataFrame")
    suffixes: list = Field(default_factory=lambda: ["_left", "_right"], description="Suffixes for overlapping column names")
    max_result_rows: Optional[int] = Field(default=10_000_000, description="Fail if result would exceed this row count (None = no limit)")

    description: Optional[str] = Field(default=None)
    group_name: str = Field(default="transforms")
    deps: Optional[list[str]] = Field(default=None)
    owners: Optional[list[str]] = Field(default=None)
    asset_tags: Optional[dict] = Field(default=None)
    kinds: Optional[list[str]] = Field(default=None)
    include_preview_metadata: bool = Field(default=True)
    preview_rows: int = Field(default=20)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        @dg.asset(
            name=self.asset_name,
            description=self.description or "Cartesian product of two DataFrames. Optional row-count guard to prevent memory blowups.",
            group_name=self.group_name,
            kinds=set(self.kinds or ['cross-join', 'cartesian']),
            deps=[dg.AssetKey.from_user_string(self.upstream_asset_key)] + [dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
            ins={"df": dg.AssetIn(key=dg.AssetKey.from_user_string(self.upstream_asset_key)), "right_df": dg.AssetIn(key=dg.AssetKey.from_user_string(self.upstream_right_key))},
            owners=self.owners or None,
            tags=self.asset_tags or None,
        )
        def _asset(context: dg.AssetExecutionContext, df: pd.DataFrame, right_df: pd.DataFrame) -> pd.DataFrame:
            self = _self
            result_rows = len(df) * len(right_df)
            if self.max_result_rows and result_rows > self.max_result_rows:
                raise ValueError(f"cross_join would produce {result_rows} rows, exceeds max_result_rows={self.max_result_rows}")
            df = df.merge(right_df, how="cross", suffixes=tuple(self.suffixes))
            context.add_output_metadata({
                "dagster/row_count": dg.MetadataValue.int(len(df)),
            })
            if _self.include_preview_metadata and len(df) > 0:
                try:
                    sample = df.sample(min(_self.preview_rows, len(df))) if len(df) > _self.preview_rows * 10 else df.head(_self.preview_rows)
                    context.add_output_metadata({"preview": dg.MetadataValue.md(sample.to_markdown(index=False))})
                except Exception:
                    pass
            return df

        return dg.Definitions(assets=[_asset])
