"""AlterRowComponent.

Mark each row with a CDC operation (insert / update / delete / upsert) based on conditions — same semantics as ADF Alter Row.
"""

import os
from typing import Optional

import dagster as dg
import pandas as pd
from pydantic import Field


class AlterRowComponent(dg.Component, dg.Model, dg.Resolvable):
    """Mark each row with a CDC operation (insert / update / delete / upsert) based on conditions — same semantics as ADF Alter Row."""

    asset_name: str = Field(description="Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream DataFrame asset key")

    rules: list = Field(description="Ordered list of {operation, condition} — first matching rule wins. operation: insert | update | delete | upsert")
    default_operation: str = Field(default="insert", description="Operation when no rule matches")
    operation_column: str = Field(default="cdc_op", description="Output column holding the operation marker")

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
            description=self.description or "Mark each row with a CDC operation (insert / update / delete / upsert) based on conditions — same semantics as ADF Alter Row.",
            group_name=self.group_name,
            kinds=set(self.kinds or ['cdc', 'alter-row']),
            deps=[dg.AssetKey.from_user_string(self.upstream_asset_key)] + [dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
            ins={"df": dg.AssetIn(key=dg.AssetKey.from_user_string(self.upstream_asset_key))},
            owners=self.owners or None,
            tags=self.asset_tags or None,
        )
        def _asset(context: dg.AssetExecutionContext, df: pd.DataFrame) -> pd.DataFrame:
            self = _self
            df = df.copy()
            df[self.operation_column] = self.default_operation
            for rule in self.rules:
                cond = rule["condition"]
                op = rule["operation"]
                mask = df.eval(cond).fillna(False)
                # Only set on rows still at default — first-matching-wins semantics
                still_default = df[self.operation_column] == self.default_operation
                df.loc[mask & still_default, self.operation_column] = op
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
