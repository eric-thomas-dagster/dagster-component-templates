"""PearsonCorrelation.

Compute the Pearson correlation matrix across selected numeric columns and
emit the result as a DataFrame. Two output shapes:

- `long` (default) — one row per (field1, field2) pair with the correlation.
  Matches the Pearson Correlation step's typical output.
- `wide` — the correlation matrix as a square DataFrame.
"""
from typing import Any, Dict, List, Optional, Union

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


class PearsonCorrelationComponent(Component, Model, Resolvable):
    """Pearson correlation matrix across selected numeric columns."""

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame")
    columns: Optional[List[Union[str, int]]] = Field(
        default=None,
        description=(
            "Columns to correlate. If None, uses all numeric columns from "
            "the upstream DataFrame."
        ),
    )
    output_shape: str = Field(
        default="long",
        description=(
            "Output shape: 'long' (default — one row per (field1, field2) pair) "
            "or 'wide' (the correlation matrix as a square DataFrame)."
        ),
    )
    method: str = Field(
        default="pearson",
        description=(
            "Correlation method passed to `pd.DataFrame.corr`. 'pearson' "
            "(default), 'spearman' (rank), or 'kendall' (Kendall tau)."
        ),
    )
    min_periods: int = Field(
        default=1,
        ge=1,
        description="Minimum number of observations required per pair of columns.",
    )

    group_name: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    asset_tags: Optional[Dict[str, str]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        cols_filter = self.columns
        output_shape = self.output_shape.lower()
        if output_shape not in ("long", "wide"):
            raise ValueError(f"output_shape must be 'long' or 'wide', got {output_shape!r}")
        method = self.method.lower()
        if method not in ("pearson", "spearman", "kendall"):
            raise ValueError(f"method must be pearson/spearman/kendall, got {method!r}")
        min_periods = self.min_periods

        tags = dict(self.asset_tags or {})
        for k in (self.kinds or ["python", "statistics"]):
            tags[f"dagster/kind/{k}"] = ""

        @asset(
            key=AssetKey.from_user_string(asset_name),
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
            group_name=self.group_name,
            description=self.description or f"{method.title()} correlation matrix ({output_shape} form).",
            tags=tags,
            owners=self.owners or [],
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def _asset(context: AssetExecutionContext, upstream: Any) -> pd.DataFrame:
            # Defensive Output/MaterializeResult unwrap — see summarize for the rationale.
            # Tolerates upstream authors who annotate `-> Output` or
            # return `Output(value=df, ...)` / `MaterializeResult(value=df)`.
            if hasattr(upstream, "value") and hasattr(upstream, "metadata"):
                upstream = upstream.value
            # partition bridge dict-concat: when an unpartitioned
            # asset consumes a partitioned upstream, Dagster's IO
            # manager loads ALL partitions as a dict; concat to
            # a single DataFrame before any DataFrame ops.
            if isinstance(upstream, dict):
                _frames = [v for v in upstream.values() if isinstance(v, pd.DataFrame)]
                upstream = pd.concat(_frames, ignore_index=True) if _frames else pd.DataFrame()
            cols = cols_filter or list(upstream.select_dtypes(include="number").columns)
            if not cols:
                raise ValueError("No numeric columns to correlate.")
            missing = [c for c in cols if c not in upstream.columns]
            if missing:
                raise ValueError(f"columns {missing!r} not in upstream DataFrame")
            matrix = upstream[cols].corr(method=method, min_periods=min_periods)
            context.add_output_metadata({
                "dagster/row_count": MetadataValue.int(len(matrix)),
                "method": MetadataValue.text(method),
                "n_columns": MetadataValue.int(len(cols)),
            })
            if output_shape == "wide":
                return matrix.reset_index().rename(columns={"index": "field"})
            # long form
            long = matrix.reset_index().melt(
                id_vars="index", var_name="field2", value_name="correlation"
            )
            return long.rename(columns={"index": "field1"})

        return Definitions(assets=[_asset])
