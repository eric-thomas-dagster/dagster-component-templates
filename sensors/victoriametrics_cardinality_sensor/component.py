"""VictoriaMetricsCardinalitySensorComponent.

Poll VictoriaMetrics's `/api/v1/status/tsdb` cardinality stats and emit a
RunRequest (or SkipReason with detail) when total series cardinality
crosses a configurable threshold.

VictoriaMetrics, like Prometheus, performs poorly past ~10M active
series — runaway cardinality is typically the first sign of a misnamed
label (think `user_id` tag instead of field). This sensor surfaces
that *before* it bites query latency.

Endpoint:
  GET {base_url}/api/v1/status/tsdb?topN={topN}
  → {
      "status": "ok",
      "data": {
        "totalSeries": <int>,
        "totalLabelValuePairs": <int>,
        "seriesCountByMetricName": [{"name":..., "value":...}, ...],
        "labelValueCountByLabelName": [{"name":..., "value":...}, ...]
      }
    }

Sensor states:
  - cardinality < threshold_series  → SkipReason with current count
  - cardinality ≥ threshold_series  → RunRequest (if target_job_name set)
                                       OR SkipReason with WARN-prefix
                                       (so it's visible in the UI even
                                       without a target job).

Docs:
  https://docs.victoriametrics.com/single-server-victoriametrics/#tsdb-stats
"""
import json
import os
from typing import Optional

import dagster as dg
from pydantic import Field


class VictoriaMetricsCardinalitySensorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Watch VM series cardinality; emit RunRequest or warning SkipReason past threshold.

    Example (watch + alert without a target job):

        ```yaml
        type: dagster_community_components.VictoriaMetricsCardinalitySensorComponent
        attributes:
          sensor_name: vm_high_cardinality_watch
          base_url_env_var: VM_BASE_URL
          threshold_series: 1000000      # warn past 1M series
          interval_seconds: 300          # poll every 5 min
        ```

    Example (trigger a remediation job when cardinality spikes):

        ```yaml
        type: dagster_community_components.VictoriaMetricsCardinalitySensorComponent
        attributes:
          sensor_name: vm_cardinality_alert
          base_url_env_var: VM_BASE_URL
          threshold_series: 5000000
          target_job_name: my_cardinality_drop_job
          interval_seconds: 60
        ```
    """

    sensor_name: str = Field(description="Dagster sensor name.")
    interval_seconds: int = Field(default=300, ge=10, description="Sensor poll interval in seconds.")
    default_status: str = Field(default="STOPPED", description="STOPPED | RUNNING")

    base_url_env_var: str = Field(
        default="VM_BASE_URL",
        description="Env var with VictoriaMetrics base URL (e.g. http://localhost:8428).",
    )
    bearer_token_env_var: Optional[str] = Field(
        default=None,
        description="Env var with bearer token (for vmauth-fronted clusters).",
    )

    threshold_series: int = Field(
        default=1_000_000,
        ge=1,
        description="Series-count threshold. When totalSeries crosses this, fire RunRequest (or warning SkipReason).",
    )
    top_n_metrics: int = Field(
        default=10,
        ge=1,
        le=100,
        description="topN metrics by cardinality to surface in the sensor evaluation. Surfaced via SkipReason/RunRequest tags.",
    )

    target_job_name: Optional[str] = Field(
        default=None,
        description="If set, emit a RunRequest against this job when threshold is breached. If None, just SkipReason with the warning detail.",
    )
    target_job_tags: Optional[dict] = Field(
        default=None,
        description="Tags to attach to the RunRequest's run config.",
    )

    request_timeout_seconds: int = Field(default=30, ge=1)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        @dg.sensor(
            name=self.sensor_name,
            minimum_interval_seconds=self.interval_seconds,
            default_status=(
                dg.DefaultSensorStatus.RUNNING
                if self.default_status.upper() == "RUNNING"
                else dg.DefaultSensorStatus.STOPPED
            ),
            job_name=self.target_job_name,
        )
        def _the_sensor(context: dg.SensorEvaluationContext):
            try:
                import requests
            except ImportError:
                raise ImportError("victoriametrics_cardinality_sensor requires `requests`")

            base_url = os.environ.get(_self.base_url_env_var, "")
            if not base_url:
                return dg.SkipReason(
                    f"{_self.base_url_env_var} not set — cannot poll VM."
                )
            base_url = base_url.rstrip("/")
            headers = {}
            if _self.bearer_token_env_var and os.environ.get(_self.bearer_token_env_var):
                headers["Authorization"] = f"Bearer {os.environ[_self.bearer_token_env_var]}"

            try:
                resp = requests.get(
                    f"{base_url}/api/v1/status/tsdb",
                    params={"topN": _self.top_n_metrics},
                    headers=headers,
                    timeout=_self.request_timeout_seconds,
                )
                resp.raise_for_status()
                payload = resp.json()
            except Exception as exc:
                return dg.SkipReason(f"VM cardinality poll failed: {exc}")

            data = payload.get("data") or {}
            total_series = int(data.get("totalSeries") or 0)
            total_label_pairs = int(data.get("totalLabelValuePairs") or 0)
            top_metrics = data.get("seriesCountByMetricName") or []

            # Persist last-seen total in cursor for delta visibility.
            prior = json.loads(context.cursor) if context.cursor else {}
            prior_total = int(prior.get("totalSeries") or 0)
            new_cursor = json.dumps({"totalSeries": total_series})

            # Use top metric names + counts as compact context.
            top_summary = ", ".join(
                f"{m.get('name')}={m.get('value')}"
                for m in top_metrics[: min(5, _self.top_n_metrics)]
            )
            common = (
                f"totalSeries={total_series:,} "
                f"(Δ={total_series - prior_total:+,} vs last poll) "
                f"totalLabelPairs={total_label_pairs:,} "
                f"top5: {top_summary}"
            )

            if total_series < _self.threshold_series:
                context.update_cursor(new_cursor)
                return dg.SkipReason(f"OK below threshold ({_self.threshold_series:,}). {common}")

            context.update_cursor(new_cursor)
            if _self.target_job_name:
                run_key = f"cardinality_breach_{total_series}"
                return dg.RunRequest(
                    run_key=run_key,
                    tags={
                        **(_self.target_job_tags or {}),
                        "vm/cardinality_total_series": str(total_series),
                        "vm/cardinality_total_label_pairs": str(total_label_pairs),
                    },
                )
            # No target job — surface as a warning SkipReason (UI-visible).
            return dg.SkipReason(f"WARN: cardinality ≥ threshold ({_self.threshold_series:,}). {common}")

        return dg.Definitions(sensors=[_the_sensor])
