"""ABTreatmentsComponent.

Takes a DataFrame of users and assigns each to a variant using a stable hash of (user_id, experiment_id). The split is deterministic — re-running on the same input gives the same assignments — so you can rebuild the asset without reshuffling users mid-experiment.
"""
from typing import Dict, List, Optional

import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    MetadataValue,
    Model,
    Resolvable,
    asset,
)
from pydantic import Field


class ABTreatmentsComponent(Component, Model, Resolvable):
    """Deterministically assign each user to a control/treatment variant via hashing."""

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame")
    user_id_column: str = Field(description="Column with user IDs.")
    experiment_id: str = Field(description="Experiment identifier (used in the hash so the same user can be in different splits across experiments).")
    variants: List[str] = Field(default=["control", "treatment"], description="Variant labels.")
    weights: Optional[List[float]] = Field(default=None, description="Per-variant weights (must sum to 1). Equal split if None.")
    output_column: str = Field(default="variant", description="Output column for the variant label.")

    include_preview_metadata: bool = Field(
        default=False,
        description="Include a preview of the output DataFrame in metadata (for builder UIs).",
    )
    preview_rows: int = Field(
        default=25,
        ge=1,
        le=500,
        description="Rows in the preview when include_preview_metadata=True.",
    )
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    asset_tags: Optional[Dict[str, str]] = Field(default=None, description="Asset tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds.")
    deps: Optional[List[str]] = Field(default=None, description="Lineage-only upstream asset keys.")

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows
        group_name = self.group_name
        _self = self

        ins = {"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))}
        tags_dict = dict(self.asset_tags or {})
        for k in (self.kinds or ["python"]):
            tags_dict[f"dagster/kind/{k}"] = ""

        @asset(
            name=asset_name,
            ins=ins,
            group_name=group_name,
            description=self.description or "Deterministically assign each user to a control/treatment variant via hashing.",
            tags=tags_dict,
            owners=self.owners or [],
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def _asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
            df = upstream
            import hashlib
            variants = _self.variants
            n = len(variants)
            weights = _self.weights or [1.0 / n] * n
            cum = []
            s = 0.0
            for w in weights:
                s += w
                cum.append(s)
            def assign(uid):
                h = int(hashlib.md5(f"{_self.experiment_id}:{uid}".encode()).hexdigest(), 16) / (1 << 128)
                for v, c in zip(variants, cum):
                    if h <= c:
                        return v
                return variants[-1]
            df = df.copy()
            df[_self.output_column] = df[_self.user_id_column].astype(str).map(assign)
            df["experiment_id"] = _self.experiment_id
            out_df = df

            if include_preview and len(out_df) > 0:
                try:
                    _prev = out_df.sample(min(preview_rows, len(out_df))) if len(out_df) > preview_rows * 10 else out_df.head(preview_rows)
                    context.add_output_metadata({"preview": MetadataValue.md(_prev.to_markdown(index=False))})
                except Exception as _e:
                    context.log.warning(f"preview emission failed: {_e}")
            return out_df

        return Definitions(assets=[_asset])
