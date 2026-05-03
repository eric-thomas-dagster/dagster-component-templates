"""ScdType2Component.

Slowly Changing Dimension Type 2 — keep history. Detect changed rows, expire prior versions, insert new versions with effective_from / effective_to / is_current.
"""

import os
from typing import Optional

import dagster as dg
import pandas as pd
from pydantic import Field


class ScdType2Component(dg.Component, dg.Model, dg.Resolvable):
    """Slowly Changing Dimension Type 2 — keep history. Detect changed rows, expire prior versions, insert new versions with effective_from / effective_to / is_current."""

    asset_name: str = Field(description="Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream DataFrame asset key")
    upstream_target_key: str = Field(description="Secondary upstream DataFrame key (see body)")

    upstream_target_key: str = Field(description="Existing SCD2 dimension (target)")
    business_key_columns: list = Field(description="Natural-key columns")
    track_columns: list = Field(description="Columns whose changes trigger a new history row")
    effective_from_column: str = Field(default="effective_from", description="Column for version start")
    effective_to_column: str = Field(default="effective_to", description="Column for version end (NULL for current)")
    is_current_column: str = Field(default="is_current")
    as_of_timestamp: Optional[str] = Field(default=None, description="ISO timestamp for the new version's effective_from (default: now UTC)")

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
            description=self.description or "Slowly Changing Dimension Type 2 — keep history. Detect changed rows, expire prior versions, insert new versions with effective_from / effective_to / is_current.",
            group_name=self.group_name,
            kinds=set(self.kinds or ['scd', 'scd-2', 'dimension', 'history']),
            deps=[dg.AssetKey.from_user_string(self.upstream_asset_key)] + [dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
            ins={"df": dg.AssetIn(key=dg.AssetKey.from_user_string(self.upstream_asset_key)), "target_df": dg.AssetIn(key=dg.AssetKey.from_user_string(self.upstream_target_key))},
            owners=self.owners or None,
            tags=self.asset_tags or None,
        )
        def _asset(context: dg.AssetExecutionContext, df: pd.DataFrame, target_df: pd.DataFrame) -> pd.DataFrame:
            self = _self
            target = target_df
            import datetime as dt
            as_of = pd.Timestamp(self.as_of_timestamp) if self.as_of_timestamp else pd.Timestamp.utcnow()
            keys = self.business_key_columns
            track = self.track_columns

            target = target.copy()
            df = df.copy()
            # Find current rows in target
            current = target[target[self.is_current_column] == True] if self.is_current_column in target.columns else target
            history = target[target[self.is_current_column] == False] if self.is_current_column in target.columns else target.iloc[0:0]
            # Merge incoming with current on keys
            merged = current.merge(df, on=keys, how="outer", suffixes=("_old", ""), indicator=True)

            new_versions, expired_versions, unchanged = [], [], []
            for _, row in merged.iterrows():
                if row["_merge"] == "right_only":
                    # net-new dimension key
                    new = {**{k: row[k] for k in keys}, **{c: row[c] for c in df.columns if c not in keys}}
                    new[self.effective_from_column] = as_of
                    new[self.effective_to_column] = pd.NaT
                    new[self.is_current_column] = True
                    new_versions.append(new)
                elif row["_merge"] == "left_only":
                    # row absent from incoming — keep as-is (could mark deleted)
                    old = {c: row[c.replace("_old", "")] for c in row.index if c.endswith("_old")}
                    old.update({k: row[k] for k in keys})
                    old[self.effective_from_column] = row.get(self.effective_from_column)
                    old[self.effective_to_column] = row.get(self.effective_to_column)
                    old[self.is_current_column] = True
                    unchanged.append(old)
                else:
                    # both — check track changes
                    changed = any(row.get(t) != row.get(f"{t}_old") for t in track if f"{t}_old" in row.index and t in row.index)
                    if changed:
                        # expire old
                        old = {c.replace("_old", ""): row[c] for c in row.index if c.endswith("_old")}
                        old.update({k: row[k] for k in keys})
                        old[self.effective_to_column] = as_of
                        old[self.is_current_column] = False
                        expired_versions.append(old)
                        # insert new
                        new = {**{k: row[k] for k in keys}, **{c: row[c] for c in df.columns if c not in keys}}
                        new[self.effective_from_column] = as_of
                        new[self.effective_to_column] = pd.NaT
                        new[self.is_current_column] = True
                        new_versions.append(new)
                    else:
                        # unchanged — keep current
                        keep = {c.replace("_old", ""): row[c] for c in row.index if c.endswith("_old")}
                        keep.update({k: row[k] for k in keys})
                        keep[self.effective_from_column] = row.get(self.effective_from_column)
                        keep[self.is_current_column] = True
                        unchanged.append(keep)

            out = pd.concat([
                history,
                pd.DataFrame(expired_versions),
                pd.DataFrame(unchanged),
                pd.DataFrame(new_versions),
            ], ignore_index=True)
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
