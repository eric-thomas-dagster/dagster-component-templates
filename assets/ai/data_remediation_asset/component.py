"""Data Remediation Asset — apply an agent-produced remediation plan to a DataFrame.

This is the "action-half" of an agentic data-quality pipeline. An LLM (usually
via `langchain_chain_asset`) diagnoses a DataFrame's quality issues and emits
a plan of remediation actions per column. This component reads that plan and
applies the picked actions from a bounded, safe action space.

The bounded action set matters: the agent picks actions by name (e.g.
"drop_nulls" or "clip_outliers"), it does NOT write arbitrary code. Every
action executed is auditable and reproducible.

Action space (fixed):
  - drop_nulls              — drop rows where the column is null
  - fill_nulls              — fill nulls with a constant value (params: {value})
  - fill_nulls_with_median  — fill nulls with the column's median
  - fill_nulls_with_mean    — fill nulls with the column's mean
  - fill_nulls_with_mode    — fill nulls with the most common value
  - cast_type               — cast to a numpy/pandas dtype (params: {dtype})
  - dedup                   — drop duplicate rows (optionally on subset of columns)
  - clip_outliers           — clip values outside z-score range (params: {z_max})
  - filter_range            — keep only rows in [min, max] (params: {min, max})
  - strip_whitespace        — strip leading/trailing whitespace from string columns

Upstream plan shape:
  A DataFrame with columns: `column`, `action`, `params` (JSON string or dict),
  `reason` (optional, for logging). One row per remediation the agent chose.

Emits:
  A remediated DataFrame + metadata summarizing which actions ran, per-column
  row-count deltas, and the plan itself for full auditability.
"""
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


class DataRemediationAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Apply an agent-produced plan of remediation actions to a DataFrame.

    Example (Data Doctor pipeline — the plan comes from a LangChain LLM asset):

        ```yaml
        type: dagster_community_components.DataRemediationAssetComponent
        attributes:
          asset_name: cleaned_transactions
          upstream_data_key: raw_transactions
          plan_key: remediation_plan
          group_name: agentic_dq
        ```

    Plan DataFrame example (produced by the LLM asset):

        | column   | action           | params               | reason                       |
        |----------|------------------|----------------------|------------------------------|
        | amount   | fill_nulls       | {"value": 0}         | 3% nulls, safe default is 0  |
        | amount   | clip_outliers    | {"z_max": 3}         | 2 rows at z=4.7 look like typos |
        | email    | strip_whitespace | {}                   | 8% have trailing whitespace  |
        | email    | drop_nulls       | {}                   | 1% nulls, can't recover      |
        | (any)    | dedup            | {"subset": ["id"]}   | 5 duplicate ids              |
    """

    asset_name: str = Field(description="Output asset name.")
    upstream_data_key: str = Field(
        description="Upstream asset key producing the DataFrame to remediate.",
    )
    plan_key: str = Field(
        description=(
            "Upstream asset key producing the remediation plan. Must be a "
            "DataFrame with columns: column, action, params (dict/str/None), "
            "and optionally reason."
        ),
    )
    fail_on_unknown_action: bool = Field(
        default=False,
        description=(
            "If True, raise on any action name not in the built-in action set. "
            "If False (default), log a warning and skip — the agent occasionally "
            "hallucinates action names and the safe default is to skip them."
        ),
    )

    group_name: Optional[str] = Field(default=None, description="Asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds (auto-includes 'ai', 'dq').",
    )

    # Standard fields — partitions / freshness / retries.
    partition_type: Optional[str] = Field(
        default=None,
        description="'daily' | 'weekly' | 'monthly' | 'hourly' | 'static' | 'dynamic'",
    )
    partition_start: Optional[str] = Field(default=None, description="ISO date for time-based partitions.")
    partition_values: Optional[str] = Field(default=None, description="Comma-separated values for static partitions.")
    dynamic_partition_name: Optional[str] = Field(default=None, description="Name for DynamicPartitionsDefinition.")
    freshness_max_lag_minutes: Optional[int] = Field(default=None, description="FreshnessPolicy max lag minutes.")
    freshness_cron: Optional[str] = Field(default=None, description="FreshnessPolicy cron schedule.")
    retry_policy_max_retries: Optional[int] = Field(default=None, description="RetryPolicy max retries.")
    retry_policy_delay_seconds: Optional[int] = Field(default=None, description="Seconds between retries.")
    retry_policy_backoff: str = Field(default="exponential", description="'linear' or 'exponential'.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        _kinds = set(self.kinds or [])
        _kinds.update({"ai", "dq"})

        # Build partitions / freshness / retry policies from the standard fields.
        partitions_def = None
        if self.partition_type:
            from dagster import (
                DailyPartitionsDefinition, WeeklyPartitionsDefinition,
                MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
                StaticPartitionsDefinition, DynamicPartitionsDefinition,
            )
            _pt = self.partition_type
            _vals = [v.strip() for v in (self.partition_values or "").split(",") if v.strip()]
            if _pt in ("daily", "weekly", "monthly", "hourly") and not self.partition_start:
                raise ValueError(f"partition_type={_pt!r} requires partition_start (ISO date).")
            if _pt == "daily":
                partitions_def = DailyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "weekly":
                partitions_def = WeeklyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "monthly":
                partitions_def = MonthlyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "hourly":
                partitions_def = HourlyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "static":
                if not _vals:
                    raise ValueError("partition_type='static' requires partition_values.")
                partitions_def = StaticPartitionsDefinition(_vals)
            elif _pt == "dynamic":
                if not self.dynamic_partition_name:
                    raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
                partitions_def = DynamicPartitionsDefinition(name=self.dynamic_partition_name)
        freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from datetime import timedelta
            from dagster import FreshnessPolicy
            if self.freshness_cron:
                freshness_policy = FreshnessPolicy.cron(
                    deadline_cron=self.freshness_cron,
                    lower_bound_delta=timedelta(minutes=self.freshness_max_lag_minutes),
                )
            else:
                freshness_policy = FreshnessPolicy.time_window(
                    fail_window=timedelta(minutes=self.freshness_max_lag_minutes),
                )
        retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )
        std_kwargs: Dict[str, Any] = {}
        if partitions_def is not None: std_kwargs["partitions_def"] = partitions_def
        if freshness_policy is not None: std_kwargs["freshness_policy"] = freshness_policy
        if retry_policy is not None: std_kwargs["retry_policy"] = retry_policy
        if self.owners: std_kwargs["owners"] = list(self.owners)
        if self.tags: std_kwargs["tags"] = dict(self.tags)

        upstream_key = dg.AssetKey.from_user_string(_self.upstream_data_key)
        plan_key_ = dg.AssetKey.from_user_string(_self.plan_key)

        @dg.asset(
            key=dg.AssetKey.from_user_string(_self.asset_name),
            group_name=_self.group_name,
            kinds=_kinds,
            description=_self.description or (
                f"Apply agent remediation plan from {_self.plan_key} to "
                f"{_self.upstream_data_key}"
            ),
            ins={
                "upstream_data": dg.AssetIn(key=upstream_key),
                "plan": dg.AssetIn(key=plan_key_),
            },
            **std_kwargs,
        )
        def _asset(context: dg.AssetExecutionContext, upstream_data, plan):
            import json
            import pandas as pd

            df = upstream_data.copy() if hasattr(upstream_data, "copy") else pd.DataFrame(upstream_data).copy()
            plan_df = plan if isinstance(plan, pd.DataFrame) else pd.DataFrame(plan)
            rows_before = len(df)

            def _parse_params(p):
                if p is None or (isinstance(p, float) and pd.isna(p)):
                    return {}
                if isinstance(p, dict):
                    return p
                if isinstance(p, str):
                    try:
                        return json.loads(p) if p.strip() else {}
                    except json.JSONDecodeError:
                        return {}
                return {}

            summary: List[Dict[str, Any]] = []

            for _, row in plan_df.iterrows():
                col = row.get("column")
                action = row.get("action")
                params = _parse_params(row.get("params"))
                reason = row.get("reason", "")
                rows_before_op = len(df)

                try:
                    if action in ("none", None, ""):
                        context.log.info(f"[remediation] no action for {col!r}")
                        summary.append({"column": col, "action": "none", "status": "no_action", "rows_delta": 0, "reason": reason})
                        continue
                    elif action == "drop_nulls":
                        df = df.dropna(subset=[col]) if col else df.dropna()
                    elif action == "fill_nulls":
                        value = params.get("value", 0)
                        df[col] = df[col].fillna(value)
                    elif action == "fill_nulls_with_median":
                        df[col] = df[col].fillna(df[col].median())
                    elif action == "fill_nulls_with_mean":
                        df[col] = df[col].fillna(df[col].mean())
                    elif action == "fill_nulls_with_mode":
                        mode_val = df[col].mode()
                        if not mode_val.empty:
                            df[col] = df[col].fillna(mode_val.iloc[0])
                    elif action == "cast_type":
                        dtype = params.get("dtype", "float64")
                        df[col] = df[col].astype(dtype, errors="ignore")
                    elif action == "dedup":
                        subset = params.get("subset")
                        df = df.drop_duplicates(subset=subset)
                    elif action == "clip_outliers":
                        z_max = float(params.get("z_max", 3.0))
                        s = pd.to_numeric(df[col], errors="coerce")
                        mu, sd = s.mean(), s.std()
                        if sd and sd > 0:
                            lo, hi = mu - z_max * sd, mu + z_max * sd
                            df[col] = s.clip(lower=lo, upper=hi)
                    elif action == "filter_range":
                        lo = params.get("min")
                        hi = params.get("max")
                        s = pd.to_numeric(df[col], errors="coerce")
                        mask = pd.Series([True] * len(df), index=df.index)
                        if lo is not None:
                            mask &= (s >= float(lo))
                        if hi is not None:
                            mask &= (s <= float(hi))
                        df = df[mask]
                    elif action == "strip_whitespace":
                        if pd.api.types.is_string_dtype(df[col]) or df[col].dtype == object:
                            df[col] = df[col].astype(str).str.strip()
                    else:
                        msg = f"Unknown action {action!r} for column {col!r}"
                        if _self.fail_on_unknown_action:
                            raise ValueError(msg)
                        context.log.warning(f"[remediation] SKIP: {msg}")
                        summary.append({"column": col, "action": action, "status": "skipped_unknown", "rows_delta": 0, "reason": reason})
                        continue

                    delta = len(df) - rows_before_op
                    context.log.info(
                        f"[remediation] {action} on {col!r} — rows Δ={delta:+d} — reason: {reason}"
                    )
                    summary.append({"column": col, "action": action, "status": "applied", "rows_delta": delta, "reason": reason})

                except Exception as e:  # noqa: BLE001
                    context.log.warning(f"[remediation] FAILED {action} on {col!r}: {e}")
                    summary.append({"column": col, "action": action, "status": f"failed:{e}", "rows_delta": 0, "reason": reason})

            rows_after = len(df)
            summary_df = pd.DataFrame(summary)

            context.add_output_metadata({
                "rows_before": dg.MetadataValue.int(rows_before),
                "rows_after": dg.MetadataValue.int(rows_after),
                "rows_delta": dg.MetadataValue.int(rows_after - rows_before),
                "actions_applied": dg.MetadataValue.int(
                    int((summary_df["status"] == "applied").sum()) if not summary_df.empty else 0
                ),
                "actions_skipped": dg.MetadataValue.int(
                    int((summary_df["status"].str.startswith("skipped") | summary_df["status"].str.startswith("failed")).sum())
                    if not summary_df.empty else 0
                ),
                "plan_summary": dg.MetadataValue.md(
                    summary_df.to_markdown(index=False) if not summary_df.empty else "_no plan rows_"
                ),
                "cleaned_preview": dg.MetadataValue.md(
                    df.head(10).to_markdown(index=False)
                ),
            })
            return df

        return dg.Definitions(assets=[_asset])
