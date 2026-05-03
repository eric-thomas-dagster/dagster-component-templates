"""SurrogateKeyComponent.

Generate stable surrogate keys — deterministic SHA-256 hash of business-key columns, or sequential integers.
"""

import os
from typing import Optional

import dagster as dg
import pandas as pd
from pydantic import Field


class SurrogateKeyComponent(dg.Component, dg.Model, dg.Resolvable):
    """Generate stable surrogate keys — deterministic SHA-256 hash of business-key columns, or sequential integers."""

    asset_name: str = Field(description="Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream DataFrame asset key")

    business_key_columns: list = Field(description="Columns that uniquely identify a record")
    output_column: str = Field(default="sk_id", description="Name for the generated surrogate key column")
    method: str = Field(default="sha256", description="'sha256' (deterministic) | 'md5' | 'sequential' (1..N)")
    sequential_start: int = Field(default=1, description="Start value for sequential mode")
    truncate_chars: Optional[int] = Field(default=None, description="Truncate hash to this many hex chars (e.g. 16)")

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
            description=self.description or "Generate stable surrogate keys — deterministic SHA-256 hash of business-key columns, or sequential integers.",
            group_name=self.group_name,
            kinds=set(self.kinds or ['surrogate-key', 'hash']),
            deps=[dg.AssetKey.from_user_string(self.upstream_asset_key)] + [dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
            ins={"df": dg.AssetIn(key=dg.AssetKey.from_user_string(self.upstream_asset_key))},
            owners=self.owners or None,
            tags=self.asset_tags or None,
        )
        def _asset(context: dg.AssetExecutionContext, df: pd.DataFrame) -> pd.DataFrame:
            self = _self
            import hashlib
            if self.method == "sequential":
                df = df.copy()
                df[self.output_column] = range(self.sequential_start, self.sequential_start + len(df))
            else:
                algo = hashlib.sha256 if self.method == "sha256" else hashlib.md5
                def _hash_row(row):
                    payload = "|".join(str(row[c]) for c in self.business_key_columns)
                    h = algo(payload.encode("utf-8")).hexdigest()
                    return h[:self.truncate_chars] if self.truncate_chars else h
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
