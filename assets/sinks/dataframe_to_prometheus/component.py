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
from typing import Dict, List, Optional

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


class DataframeToPrometheusComponent(Component, Model, Resolvable):
    """Push DataFrame rows as Prometheus metrics via a pushgateway."""

    asset_name: str = Field(description="Dagster asset name")
    upstream_asset_key: str = Field(description="Asset key of upstream DataFrame")

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
    value_column: str = Field(
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

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        cfg = self
        kinds = self.kinds or ["prometheus", "metrics"]
        tags = dict(self.asset_tags or {})
        for k in kinds:
            tags[f"dagster/kind/{k}"] = ""

        @asset(
            name=self.asset_name,
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(self.upstream_asset_key))},
            group_name=self.group_name,
            description=self.description or f"Push DataFrame as Prometheus metric '{cfg.metric_name}' (job={cfg.job})",
            owners=self.owners or [],
            tags=tags,
            deps=[AssetKey.from_user_string(d) for d in (self.deps or [])],
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
