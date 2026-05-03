"""AuditColumnsComponent.

Add system-audit columns to every row — run_id, dagster_run_id, asset_key, materialization_time, optional username/git_sha.
"""

import os
from typing import Optional

import dagster as dg
import pandas as pd
from pydantic import Field


class AuditColumnsComponent(dg.Component, dg.Model, dg.Resolvable):
    """Add system-audit columns to every row — run_id, dagster_run_id, asset_key, materialization_time, optional username/git_sha."""

    asset_name: str = Field(description="Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream DataFrame asset key")

    include_run_id: bool = Field(default=True)
    include_asset_key: bool = Field(default=True)
    include_materialization_time: bool = Field(default=True)
    include_run_partition: bool = Field(default=False)
    static_columns: Optional[dict] = Field(default=None, description="Constant columns to add: {col: value} (e.g. {'source_system': 'salesforce'})")
    git_sha_env: Optional[str] = Field(default=None, description="If set, attaches the git SHA from this env var")
    user_env: Optional[str] = Field(default=None, description="If set, attaches the username from this env var (default USER)")

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
            description=self.description or "Add system-audit columns to every row — run_id, dagster_run_id, asset_key, materialization_time, optional username/git_sha.",
            group_name=self.group_name,
            kinds=set(self.kinds or ['audit', 'system-metadata']),
            deps=[dg.AssetKey.from_user_string(self.upstream_asset_key)] + [dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
            ins={"df": dg.AssetIn(key=dg.AssetKey.from_user_string(self.upstream_asset_key))},
            owners=self.owners or None,
            tags=self.asset_tags or None,
        )
        def _asset(context: dg.AssetExecutionContext, df: pd.DataFrame) -> pd.DataFrame:
            self = _self
            import datetime as dt, os
            df = df.copy()
            if self.include_run_id:
                df["dagster_run_id"] = context.run_id
            if self.include_asset_key:
                df["dagster_asset_key"] = str(context.asset_key.to_user_string()) if hasattr(context, "asset_key") else None
            if self.include_materialization_time:
                df["materialization_time"] = dt.datetime.utcnow().isoformat() + "Z"
            if self.include_run_partition and getattr(context, "partition_key", None):
                df["dagster_partition_key"] = context.partition_key
            if self.git_sha_env:
                df["git_sha"] = os.environ.get(self.git_sha_env)
            if self.user_env:
                df["materialized_by"] = os.environ.get(self.user_env, os.environ.get("USER", ""))
            if self.static_columns:
                for k, v in self.static_columns.items():
                    df[k] = v
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
