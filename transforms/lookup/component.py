"""LookupComponent.

Inline lookup — left-join an upstream DataFrame against a smaller reference DataFrame, optionally caching the reference per run.
"""

import os
from typing import Optional

import dagster as dg
import pandas as pd
from pydantic import Field


class LookupComponent(dg.Component, dg.Model, dg.Resolvable):
    """Inline lookup — left-join an upstream DataFrame against a smaller reference DataFrame, optionally caching the reference per run."""

    asset_name: str = Field(description="Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream DataFrame asset key")
    upstream_lookup_key: str = Field(description="Secondary upstream DataFrame key (see body)")

    upstream_lookup_key: str = Field(description="Reference (small) DataFrame asset key")
    on: list = Field(description="Columns used for the join (must exist in both)")
    columns_to_add: Optional[list] = Field(default=None, description="Columns from the lookup to attach (None = all non-join columns)")
    multiple_match: str = Field(default="first", description="When the lookup has multiple matches: 'first' | 'last' | 'all' (cartesian)")
    rename: Optional[dict] = Field(default=None, description="Rename added columns: {original: new}")

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
            description=self.description or "Inline lookup — left-join an upstream DataFrame against a smaller reference DataFrame, optionally caching the reference per run.",
            group_name=self.group_name,
            kinds=set(self.kinds or ['lookup', 'join']),
            deps=[dg.AssetKey.from_user_string(self.upstream_asset_key)] + [dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
            ins={"df": dg.AssetIn(key=dg.AssetKey.from_user_string(self.upstream_asset_key)), "lookup_df": dg.AssetIn(key=dg.AssetKey.from_user_string(self.upstream_lookup_key))},
            owners=self.owners or None,
            tags=self.asset_tags or None,
        )
        def _asset(context: dg.AssetExecutionContext, df: pd.DataFrame, lookup_df: pd.DataFrame) -> pd.DataFrame:
            self = _self
            # df is the main upstream; lookup_df is the small reference table
            join_keys = self.on
            cols_to_add = self.columns_to_add or [c for c in lookup_df.columns if c not in join_keys]
            lookup_subset = lookup_df[join_keys + cols_to_add].copy()
            if self.multiple_match == "first":
                lookup_subset = lookup_subset.drop_duplicates(subset=join_keys, keep="first")
            elif self.multiple_match == "last":
                lookup_subset = lookup_subset.drop_duplicates(subset=join_keys, keep="last")
            if self.rename:
                lookup_subset = lookup_subset.rename(columns=self.rename)
            df = df.merge(lookup_subset, on=join_keys, how="left")
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
