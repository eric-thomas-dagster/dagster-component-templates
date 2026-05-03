"""PivotComponent.

Pivot a DataFrame from long to wide — rotate row values into column headers with a chosen aggregation.
"""

import os
from typing import Optional

import dagster as dg
import pandas as pd
from pydantic import Field


class PivotComponent(dg.Component, dg.Model, dg.Resolvable):
    """Pivot a DataFrame from long to wide — rotate row values into column headers with a chosen aggregation."""

    asset_name: str = Field(description="Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream DataFrame asset key")

    index_columns: list = Field(description="Columns that stay as rows (group keys)")
    pivot_column: str = Field(description="Column whose values become new column headers")
    value_column: str = Field(description="Column whose values fill the pivoted cells")
    agg_func: str = Field(default="sum", description="Aggregation when (index, pivot) collides: sum | mean | count | min | max | first | last")
    fill_value: Optional[float] = Field(default=None, description="Fill NaN cells with this value")

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
            description=self.description or "Pivot a DataFrame from long to wide — rotate row values into column headers with a chosen aggregation.",
            group_name=self.group_name,
            kinds=set(self.kinds or ['pivot']),
            deps=[dg.AssetKey.from_user_string(self.upstream_asset_key)] + [dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
            ins={"df": dg.AssetIn(key=dg.AssetKey.from_user_string(self.upstream_asset_key))},
            owners=self.owners or None,
            tags=self.asset_tags or None,
        )
        def _asset(context: dg.AssetExecutionContext, df: pd.DataFrame) -> pd.DataFrame:
            self = _self
            out = df.pivot_table(
                index=self.index_columns, columns=self.pivot_column,
                values=self.value_column, aggfunc=self.agg_func,
                fill_value=self.fill_value,
            ).reset_index()
            out.columns = [str(c) for c in out.columns]
            df = out
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
