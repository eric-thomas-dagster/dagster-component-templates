"""HashComponent.

Compute MD5/SHA-1/SHA-256 of one or more columns (or the whole row) — useful for change detection, anonymization, surrogate keys.
"""

import os
from typing import Optional

import dagster as dg
import pandas as pd
from pydantic import Field


class HashComponent(dg.Component, dg.Model, dg.Resolvable):
    """Compute MD5/SHA-1/SHA-256 of one or more columns (or the whole row) — useful for change detection, anonymization, surrogate keys."""

    asset_name: str = Field(description="Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream DataFrame asset key")

    columns: Optional[list] = Field(default=None, description="Columns to include in the hash (None = all)")
    output_column: str = Field(default="row_hash", description="Output column name")
    algorithm: str = Field(default="sha256", description="md5 | sha1 | sha256")
    separator: str = Field(default="|", description="Joiner between column values")
    null_token: str = Field(default="\\N", description="Token substituted for NaN/None")

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
            description=self.description or "Compute MD5/SHA-1/SHA-256 of one or more columns (or the whole row) — useful for change detection, anonymization, surrogate keys.",
            group_name=self.group_name,
            kinds=set(self.kinds or ['hash', 'checksum']),
            deps=[dg.AssetKey.from_user_string(self.upstream_asset_key)] + [dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
            ins={"df": dg.AssetIn(key=dg.AssetKey.from_user_string(self.upstream_asset_key))},
            owners=self.owners or None,
            tags=self.asset_tags or None,
        )
        def _asset(context: dg.AssetExecutionContext, df: pd.DataFrame) -> pd.DataFrame:
            self = _self
            import hashlib
            algo = {"md5": hashlib.md5, "sha1": hashlib.sha1, "sha256": hashlib.sha256}[self.algorithm]
            cols = self.columns or list(df.columns)
            def _hash_row(row):
                parts = [self.null_token if pd.isna(row[c]) else str(row[c]) for c in cols]
                return algo(self.separator.join(parts).encode("utf-8")).hexdigest()
            df = df.copy()
            df[self.output_column] = df.apply(_hash_row, axis=1)
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
