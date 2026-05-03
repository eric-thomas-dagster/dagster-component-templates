"""MapValuesComponent.

Replace values in a column according to a lookup dict — country code → country name, status code → status text, etc.
"""

import os
from typing import Optional

import dagster as dg
import pandas as pd
from pydantic import Field


class MapValuesComponent(dg.Component, dg.Model, dg.Resolvable):
    """Replace values in a column according to a lookup dict — country code → country name, status code → status text, etc."""

    asset_name: str = Field(description="Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream DataFrame asset key")

    column: str = Field(description="Column whose values to remap")
    mapping: dict = Field(description="{old: new} value mapping")
    output_column: Optional[str] = Field(default=None, description="Write to a new column (None = overwrite)")
    default_value: Optional[str] = Field(default=None, description="Use this when no mapping match (None = keep original)")

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
            description=self.description or "Replace values in a column according to a lookup dict — country code → country name, status code → status text, etc.",
            group_name=self.group_name,
            kinds=set(self.kinds or ['map-values', 'replace']),
            deps=[dg.AssetKey.from_user_string(self.upstream_asset_key)] + [dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
            ins={"df": dg.AssetIn(key=dg.AssetKey.from_user_string(self.upstream_asset_key))},
            owners=self.owners or None,
            tags=self.asset_tags or None,
        )
        def _asset(context: dg.AssetExecutionContext, df: pd.DataFrame) -> pd.DataFrame:
            self = _self
            df = df.copy()
            target = self.output_column or self.column
            if self.default_value is not None:
                df[target] = df[self.column].map(self.mapping).fillna(self.default_value)
            else:
                mapped = df[self.column].map(self.mapping)
                df[target] = mapped.where(mapped.notna(), df[self.column])
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
