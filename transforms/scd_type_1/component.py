"""ScdType1Component.

Slowly Changing Dimension Type 1 — overwrite in place. Merge incoming rows into a target on a business key, replacing changed attributes.
"""

import os
from typing import Optional

import dagster as dg
import pandas as pd
from pydantic import Field


class ScdType1Component(dg.Component, dg.Model, dg.Resolvable):
    """Slowly Changing Dimension Type 1 — overwrite in place. Merge incoming rows into a target on a business key, replacing changed attributes."""

    asset_name: str = Field(description="Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream DataFrame asset key")
    upstream_target_key: str = Field(description="Secondary upstream DataFrame key (see body)")

    upstream_target_key: str = Field(description="Existing dimension snapshot (target table)")
    business_key_columns: list = Field(description="Natural-key columns used to match incoming rows to existing")
    track_columns: Optional[list] = Field(default=None, description="Columns to overwrite when changed (None = all non-key)")

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
            description=self.description or "Slowly Changing Dimension Type 1 — overwrite in place. Merge incoming rows into a target on a business key, replacing changed attributes.",
            group_name=self.group_name,
            kinds=set(self.kinds or ['scd', 'scd-1', 'dimension']),
            deps=[dg.AssetKey.from_user_string(self.upstream_asset_key)] + [dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
            ins={"df": dg.AssetIn(key=dg.AssetKey.from_user_string(self.upstream_asset_key)), "target_df": dg.AssetIn(key=dg.AssetKey.from_user_string(self.upstream_target_key))},
            owners=self.owners or None,
            tags=self.asset_tags or None,
        )
        def _asset(context: dg.AssetExecutionContext, df: pd.DataFrame, target_df: pd.DataFrame) -> pd.DataFrame:
            self = _self
            target = target_df
            # The asset receives df (incoming) and target (existing dimension)
            keys = self.business_key_columns
            track = self.track_columns or [c for c in df.columns if c not in keys]
            # Outer-join incoming over existing on keys; incoming wins on tracked cols.
            merged = target.merge(df, on=keys, how="outer", suffixes=("_old", ""))
            for c in track:
                old, new = f"{c}_old", c
                if old in merged.columns and new in merged.columns:
                    merged[new] = merged[new].combine_first(merged[old])
                    merged = merged.drop(columns=[old])
            df = merged
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
