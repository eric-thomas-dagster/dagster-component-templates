"""TestOfMeansComponent.

Two-sample t-test (independent or paired) using scipy.stats. Returns a single-row DataFrame with mean of each group, t-statistic, p-value, degrees of freedom, and significance flag. Useful as a stat-test gate downstream of an experiment exposure asset.
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


class TestOfMeansComponent(Component, Model, Resolvable):
    """Run a two-sample t-test on a numeric column grouped by a binary group column."""

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame")
    value_column: str = Field(description="Numeric column to compare across groups.")
    group_column: str = Field(description="Binary group column (control vs treatment, A vs B, etc).")
    group_a: Optional[str] = Field(default=None, description="Label for group A. Auto-detected if None.")
    group_b: Optional[str] = Field(default=None, description="Label for group B. Auto-detected if None.")
    test_type: str = Field(default="independent", description="'independent' or 'paired'")
    alpha: float = Field(default=0.05, description="Significance threshold")
    equal_var: bool = Field(default=False, description="Welch's t-test if False (default)")

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


    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale. Builds a FreshnessPolicy when set.",
    )

    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5' (weekdays 9am).",
    )

    column_lineage: Optional[Dict[str, List[str]]] = Field(
        default=None,
        description="Column-level lineage: output column → list of upstream columns it derives from, e.g. {'revenue': ['price', 'quantity']}.",
    )

    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', or None for unpartitioned.",
    )

    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format (e.g. '2024-01-01'). Required for time-based partition types.",
    )

    partition_date_column: Optional[str] = Field(
        default=None,
        description="Column used to filter the upstream DataFrame to the current date partition key.",
    )

    partition_values: Optional[str] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning, e.g. 'acme,globex,initech'.",
    )

    partition_static_dim: Optional[str] = Field(
        default=None,
        description="Dimension name for the static axis in multi-partitioning, e.g. 'customer'.",
    )

    partition_static_column: Optional[str] = Field(
        default=None,
        description="Column used to filter the upstream DataFrame to the current static partition value.",
    )

    retry_policy_max_retries: Optional[int] = Field(
        default=None,
        description="Max retries on failure. Defines a RetryPolicy when set.",
    )

    retry_policy_delay_seconds: Optional[int] = Field(
        default=None,
        description="Seconds between retries (default 1).",
    )

    retry_policy_backoff: str = Field(
        default="exponential",
        description="Backoff strategy: 'linear' or 'exponential'.",
    )

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
            description=self.description or "Run a two-sample t-test on a numeric column grouped by a binary group column.",
            tags=tags_dict,
            owners=self.owners or [],
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def _asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
            df = upstream
            from scipy import stats
            value_col = _self.value_column
            group_col = _self.group_column
            groups = sorted(df[group_col].dropna().unique().tolist())
            if _self.group_a and _self.group_b:
                a_label, b_label = _self.group_a, _self.group_b
            elif len(groups) >= 2:
                a_label, b_label = groups[0], groups[1]
            else:
                raise ValueError(f"Need at least 2 groups in {group_col}; found: {groups}")
            a = df[df[group_col] == a_label][value_col].dropna()
            b = df[df[group_col] == b_label][value_col].dropna()
            if _self.test_type == "paired":
                t_stat, p = stats.ttest_rel(a, b)
                dof = len(a) - 1
            else:
                t_stat, p = stats.ttest_ind(a, b, equal_var=_self.equal_var)
                dof = len(a) + len(b) - 2
            out_df = pd.DataFrame([{
                "value_column": value_col,
                "group_a": a_label, "group_b": b_label,
                "n_a": int(len(a)), "n_b": int(len(b)),
                "mean_a": float(a.mean()), "mean_b": float(b.mean()),
                "diff_means": float(b.mean() - a.mean()),
                "t_statistic": float(t_stat),
                "p_value": float(p),
                "dof": int(dof),
                "alpha": _self.alpha,
                "is_significant": bool(p < _self.alpha),
            }])

            if include_preview and len(out_df) > 0:
                try:
                    _prev = out_df.sample(min(preview_rows, len(out_df))) if len(out_df) > preview_rows * 10 else out_df.head(preview_rows)
                    context.add_output_metadata({"preview": MetadataValue.md(_prev.to_markdown(index=False))})
                except Exception as _e:
                    context.log.warning(f"preview emission failed: {_e}")
            return out_df

        return Definitions(assets=[_asset])
