"""UnpivotComponent.

Unpivot a DataFrame from wide to long — melt N value columns into 2 columns (variable, value).
"""

import os
from typing import Optional

import dagster as dg
import pandas as pd
from pydantic import Field


class UnpivotComponent(dg.Component, dg.Model, dg.Resolvable):
    """Unpivot a DataFrame from wide to long — melt N value columns into 2 columns (variable, value)."""

    asset_name: str = Field(description="Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream DataFrame asset key")

    id_columns: list = Field(description="Columns to keep as identifiers (not melted)")
    value_columns: Optional[list] = Field(default=None, description="Columns to melt (None = all non-id columns)")
    var_name: str = Field(default="variable", description="Name for the new 'variable' column")
    value_name: str = Field(default="value", description="Name for the new 'value' column")
    drop_null_values: bool = Field(default=False, description="Drop rows where the melted value is NaN")

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
            description=self.description or "Unpivot a DataFrame from wide to long — melt N value columns into 2 columns (variable, value).",
            group_name=self.group_name,
            kinds=set(self.kinds or ['unpivot', 'melt']),
            deps=[dg.AssetKey.from_user_string(self.upstream_asset_key)] + [dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
            ins={"df": dg.AssetIn(key=dg.AssetKey.from_user_string(self.upstream_asset_key))},
            owners=self.owners or None,
            tags=self.asset_tags or None,
        )
        def _asset(context: dg.AssetExecutionContext, df: pd.DataFrame) -> pd.DataFrame:
            self = _self
            out = df.melt(
                id_vars=self.id_columns,
                value_vars=self.value_columns,
                var_name=self.var_name,
                value_name=self.value_name,
            )
            if self.drop_null_values:
                out = out.dropna(subset=[self.value_name])
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
