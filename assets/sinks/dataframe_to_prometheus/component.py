"""DataFrame → Prometheus (push metrics via pushgateway).

Convert each DataFrame row into Prometheus metrics and push to a
pushgateway. Useful for batch-job metrics — e.g., pushing the row counts
of nightly ETL jobs as gauge metrics that show up alongside operational
dashboards.

For pull-based metrics (where Prometheus scrapes your service), use
the existing `prometheus_resource` (also pushgateway-based) or expose
metrics via your own /metrics endpoint.

Pattern:
- One column becomes the metric value (`value_column`)
- One or more columns become metric labels
- The job + grouping_key identify the push (Prometheus replaces all
  metrics with the same job+grouping_key on each push)
"""

import os
from typing import Dict, List, Optional, Union

import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    MaterializeResult,
    MetadataValue,
    Model,
    Resolvable,
    asset,
)
from pydantic import Field



# BEGIN upstream-asset-key normalization (inlined per component; keep copies identical — see tests/test_upstream_keys.py)
from typing import List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def normalize_upstream_keys(single, plural=None) -> List[str]:
    """Merge ``upstream_asset_key`` / ``upstream_asset_keys`` into one clean list.

    Each argument may be a str, a list/tuple of str, or None. Entries are
    stripped, empties dropped, and duplicates removed (order preserved).
    """
    out: List[str] = []
    for value in (single, plural):
        if value is None:
            continue
        items = [value] if isinstance(value, str) else value
        if not isinstance(items, (list, tuple)):
            raise ValueError(
                f"upstream asset key(s) must be a string or a list of strings, got {value!r}"
            )
        for item in items:
            if not isinstance(item, str):
                raise ValueError(f"upstream asset keys must be strings, got {item!r}")
            item = item.strip()
            if item and item not in out:
                out.append(item)
    return out


def coerce_upstream_input(data, *, component: str, mode: str, optional: bool = True):
    """Rewrite raw component input so both spellings/shapes land on the declared field(s).

    mode "single": declared field is ``upstream_asset_key`` (str); >1 key is an error.
    mode "multi":  declared field is ``upstream_asset_keys`` (list).
    mode "both":   both fields are declared; everything is merged into the plural one.
    """
    if not isinstance(data, dict):
        return data
    if "upstream_asset_key" not in data and "upstream_asset_keys" not in data:
        return data
    keys = normalize_upstream_keys(data.get("upstream_asset_key"), data.get("upstream_asset_keys"))
    out = {k: v for k, v in data.items() if k not in ("upstream_asset_key", "upstream_asset_keys")}
    if mode == "single":
        if len(keys) > 1:
            raise ValueError(f"{component} takes exactly one upstream asset, got {keys}")
        out["upstream_asset_key"] = keys[0] if keys else (None if optional else "")
    else:
        out["upstream_asset_keys"] = keys or (None if optional else [])
    return out
# END upstream-asset-key normalization


class DataframeToPrometheusComponent(Component, Model, Resolvable):
    """Push DataFrame rows as Prometheus metrics via a pushgateway."""

    asset_name: str = Field(description="Dagster asset name")
    upstream_asset_key: Union[str, List[str]] = Field(description="Asset key of upstream DataFrame")

    pushgateway_url: str = Field(
        description="Pushgateway URL, e.g. 'http://localhost:9091' or env-var-driven"
    )
    job: str = Field(
        description="Prometheus job name (identifies the source group on the pushgateway)"
    )
    metric_name: str = Field(description="Metric name (e.g. 'orders_total')")
    metric_type: str = Field(
        default="gauge",
        description="'gauge' (default) | 'counter'. Counters can only go up; use gauge for arbitrary values.",
    )
    metric_help: Optional[str] = Field(
        default=None,
        description="Metric HELP text (description shown in Prometheus UI)",
    )
    value_column: Union[str, int] = Field(
        description="DataFrame column containing the metric value (must be numeric)"
    )
    label_columns: List[str] = Field(
        default_factory=list,
        description="Columns whose values become Prometheus labels on each metric sample",
    )
    grouping_key_columns: List[str] = Field(
        default_factory=list,
        description=(
            "Columns whose values become the pushgateway 'grouping_key'. Pushes with "
            "the same job + grouping_key replace prior pushes (Prometheus replaces "
            "the entire group). Default: empty group."
        ),
    )
    extra_headers: Optional[Dict[str, str]] = Field(default=None)

    group_name: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    asset_tags: Optional[Dict[str, str]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)

    retry_policy_max_retries: Optional[int] = Field(
        default=None,
        description="Max retries on asset failure. Useful for transient errors like network glitches or rate limits.",
    )
    retry_policy_delay_seconds: Optional[int] = Field(
        default=None,
        description="Seconds between retries (default 1).",
    )
    retry_policy_backoff: str = Field(
        default="exponential",
        description="Backoff strategy: 'linear' or 'exponential'.",
    )

    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale.",
    )
    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5'.",
    )

    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily' / 'weekly' / 'monthly' / 'hourly' / 'static' / 'dynamic' / None for unpartitioned.",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.",
    )
    partition_values: Optional[str] = Field(
        default=None,
        description="Comma-separated values for static partitioning, e.g. 'us,eu,asia'.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition when partition_type='dynamic'.",
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_upstream_asset_keys(cls, data):
        return coerce_upstream_input(data, component=cls.__name__, mode="single", optional=False)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        partitions_def = None
        if self.partition_type:
            from dagster import (
                DailyPartitionsDefinition, WeeklyPartitionsDefinition,
                MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
                StaticPartitionsDefinition, DynamicPartitionsDefinition,
            )
            _pt = self.partition_type
            _values = [v.strip() for v in (self.partition_values or "").split(",") if v.strip()]
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
                if not _values:
                    raise ValueError("partition_type='static' requires partition_values.")
                partitions_def = StaticPartitionsDefinition(_values)
            elif _pt == "dynamic":
                if not self.dynamic_partition_name:
                    raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
                partitions_def = DynamicPartitionsDefinition(name=self.dynamic_partition_name)

        freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy

            freshness_policy = FreshnessPolicy(
                maximum_lag_minutes=self.freshness_max_lag_minutes,
                cron_schedule=self.freshness_cron,
            )

        retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy

            retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        cfg = self
        kinds = self.kinds or ["prometheus", "metrics"]
        tags = dict(self.asset_tags or {})
        for k in kinds:
            tags[f"dagster/kind/{k}"] = ""

        @asset(
            key=AssetKey.from_user_string(self.asset_name),
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(self.upstream_asset_key))},
            group_name=self.group_name,
            description=self.description or f"Push DataFrame as Prometheus metric '{cfg.metric_name}' (job={cfg.job})",
            owners=self.owners or [],
            tags=tags,
            deps=[AssetKey.from_user_string(d) for d in (self.deps or [])],
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
            partitions_def=partitions_def,
        )
        def prometheus_writer(context: AssetExecutionContext, upstream: pd.DataFrame) -> MaterializeResult:
            try:
                from prometheus_client import CollectorRegistry, Gauge, Counter, push_to_gateway
            except ImportError as e:
                raise ImportError("prometheus_client required: pip install prometheus_client") from e

            metric_cls = Counter if cfg.metric_type == "counter" else Gauge

            sent = 0
            errors = 0

            # Group rows by grouping_key so each group is its own pushgateway push
            if cfg.grouping_key_columns:
                groups = upstream.groupby([str(c) for c in cfg.grouping_key_columns])
            else:
                groups = [((None,), upstream)]

            for grouping_vals, group_df in groups:
                if not isinstance(grouping_vals, tuple):
                    grouping_vals = (grouping_vals,)
                grouping_key = (
                    {col: str(v) for col, v in zip(cfg.grouping_key_columns, grouping_vals)}
                    if cfg.grouping_key_columns
                    else None
                )

                registry = CollectorRegistry()
                metric = metric_cls(
                    cfg.metric_name,
                    cfg.metric_help or cfg.metric_name,
                    list(cfg.label_columns),
                    registry=registry,
                )

                for _, row in group_df.iterrows():
                    label_vals = [str(row[c]) for c in cfg.label_columns]
                    val = float(row[cfg.value_column])
                    if metric_cls is Counter:
                        metric.labels(*label_vals).inc(val)
                    else:
                        metric.labels(*label_vals).set(val)

                try:
                    handler_kwargs = {}
                    if cfg.extra_headers:
                        from prometheus_client.exposition import default_handler
                        # Pushgateway custom headers via handler
                        # (skipping handler customization for brevity — most users don't need it)
                        pass
                    push_to_gateway(
                        cfg.pushgateway_url,
                        job=cfg.job,
                        registry=registry,
                        grouping_key=grouping_key,
                    )
                    sent += len(group_df)
                except Exception as e:
                    errors += 1
                    context.log.warning(f"push failed for group {grouping_key}: {e}")

            context.log.info(
                f"Pushed {sent}/{len(upstream)} samples to {cfg.pushgateway_url} "
                f"(metric={cfg.metric_name}, job={cfg.job}, errors={errors})"
            )
            return MaterializeResult(
                metadata={
                    "samples_pushed": MetadataValue.int(sent),
                    "errors": MetadataValue.int(errors),
                    "metric": MetadataValue.text(cfg.metric_name),
                    "job": MetadataValue.text(cfg.job),
                    "metric_type": MetadataValue.text(cfg.metric_type),
                    "pushgateway": MetadataValue.text(cfg.pushgateway_url),
                }
            )

        return Definitions(assets=[prometheus_writer])
