"""DetectChangesComponent.

Diff incoming DataFrame against a prior snapshot — emit rows with a change_type column (insert/update/delete/unchanged).
"""

import os
from typing import Optional

import dagster as dg
import pandas as pd
from pydantic import Field


class DetectChangesComponent(dg.Component, dg.Model, dg.Resolvable):
    """Diff incoming DataFrame against a prior snapshot — emit rows with a change_type column (insert/update/delete/unchanged)."""

    asset_name: str = Field(description="Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream DataFrame asset key")
    upstream_prior_key: str = Field(description="Secondary upstream DataFrame key (see body)")

    upstream_prior_key: str = Field(description="Prior-snapshot DataFrame asset to diff against")
    business_key_columns: list = Field(description="Natural-key columns")
    compare_columns: Optional[list] = Field(default=None, description="Columns whose values are compared for 'update' (None = all non-key)")
    include_unchanged: bool = Field(default=False, description="Include unchanged rows in the output")
    change_type_column: str = Field(default="change_type")

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
            description=self.description or "Diff incoming DataFrame against a prior snapshot — emit rows with a change_type column (insert/update/delete/unchanged).",
            group_name=self.group_name,
            kinds=set(self.kinds or ['change-detection', 'diff']),
            deps=[dg.AssetKey.from_user_string(self.upstream_asset_key)] + [dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
            ins={"df": dg.AssetIn(key=dg.AssetKey.from_user_string(self.upstream_asset_key)), "prior_df": dg.AssetIn(key=dg.AssetKey.from_user_string(self.upstream_prior_key))},
            owners=self.owners or None,
            tags=self.asset_tags or None,
        )
        def _asset(context: dg.AssetExecutionContext, df: pd.DataFrame, prior_df: pd.DataFrame) -> pd.DataFrame:
            self = _self
            prior = prior_df
            keys = self.business_key_columns
            compare = self.compare_columns or [c for c in df.columns if c not in keys]

            merged = df.merge(prior, on=keys, how="outer", suffixes=("", "_prior"), indicator=True)
            rows = []
            for _, row in merged.iterrows():
                if row["_merge"] == "right_only":
                    rec = {**{k: row[k] for k in keys}, **{c: row.get(f"{c}_prior") for c in compare}}
                    rec[self.change_type_column] = "delete"
                    rows.append(rec)
                elif row["_merge"] == "left_only":
                    rec = {**{k: row[k] for k in keys}, **{c: row[c] for c in compare}}
                    rec[self.change_type_column] = "insert"
                    rows.append(rec)
                else:
                    changed = any(
                        not (pd.isna(row.get(c)) and pd.isna(row.get(f"{c}_prior")))
                        and row.get(c) != row.get(f"{c}_prior")
                        for c in compare
                    )
                    rec = {**{k: row[k] for k in keys}, **{c: row[c] for c in compare}}
                    rec[self.change_type_column] = "update" if changed else "unchanged"
                    if changed or self.include_unchanged:
                        rows.append(rec)
            df = pd.DataFrame(rows)
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
