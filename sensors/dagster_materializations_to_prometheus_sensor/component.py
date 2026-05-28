"""DagsterMaterializationsToPrometheusSensorComponent.

Listen for AssetMaterialization events across the workspace (or a
filtered set of asset keys) and push per-asset materialization metrics
to a Prometheus-compatible endpoint (Push Gateway / VictoriaMetrics
import / Mimir-via-gateway).

Counter: `dagster_asset_materialization_count{asset_key, location}`
Gauge:   `dagster_asset_last_materialization_timestamp{asset_key, location}`

This complements `dagster_runs_to_prometheus_sensor` (which is
job-level) — this one is asset-level. Useful for dashboards like:
"which assets were last materialized > 24h ago" (freshness over PromQL).

Uses Dagster's `@multi_asset_sensor` so a single sensor evaluation can
fan out across many asset keys per tick.
"""
import os
from typing import List, Optional

import dagster as dg
from pydantic import Field


class DagsterMaterializationsToPrometheusSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Push per-asset materialization metrics to Prometheus / VM / Push Gateway.

    Example (watch all assets, push to local Push Gateway):

        ```yaml
        type: dagster_community_components.DagsterMaterializationsToPrometheusSensorComponent
        attributes:
          sensor_name: dagster_asset_materializations
          base_url_env_var: PROM_PUSHGATEWAY_URL
          target_kind: push_gateway
          push_job: dagster_assets
          location_label: prod-east
        ```

    Example (watch only specific assets):

        ```yaml
        type: dagster_community_components.DagsterMaterializationsToPrometheusSensorComponent
        attributes:
          sensor_name: critical_asset_freshness
          base_url_env_var: VM_BASE_URL
          target_kind: vm_import
          watch_asset_keys:
            - orders_clean
            - revenue_daily_summary
          location_label: prod-east
        ```
    """

    sensor_name: str = Field(description="Dagster sensor name.")
    base_url_env_var: str = Field(description="Env var with target base URL.")
    target_kind: str = Field(
        default="push_gateway",
        description="One of: push_gateway | vm_import. Mimir support via Push Gateway pattern.",
    )
    push_job: str = Field(default="dagster_assets", description="Push Gateway 'job' grouping key.")
    location_label: str = Field(default="default", description="Deployment / region label.")
    watch_asset_keys: Optional[List[str]] = Field(
        default=None,
        description="Slash-joined asset keys to watch. None = watch all assets in the workspace.",
    )
    bearer_token_env_var: Optional[str] = Field(default=None, description="Env var with bearer token.")
    default_status: str = Field(default="STOPPED", description="STOPPED | RUNNING")
    interval_seconds: int = Field(default=60, ge=10, description="Sensor poll interval.")
    request_timeout_seconds: int = Field(default=15, ge=1)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        # When watch_asset_keys is None, we want to watch the workspace. The
        # cleanest way is to use a generic @sensor that pulls from the instance's
        # event log; multi_asset_sensor requires explicit AssetKey arguments.
        explicit_keys = [dg.AssetKey.from_user_string(k) for k in (self.watch_asset_keys or [])]

        if explicit_keys:
            @dg.multi_asset_sensor(
                name=self.sensor_name,
                monitored_assets=explicit_keys,
                minimum_interval_seconds=self.interval_seconds,
                default_status=(
                    dg.DefaultSensorStatus.RUNNING
                    if self.default_status.upper() == "RUNNING"
                    else dg.DefaultSensorStatus.STOPPED
                ),
            )
            def _the_sensor(context: dg.MultiAssetSensorEvaluationContext):
                latest = context.latest_materialization_records_by_key()
                pushed = 0
                for asset_key, record in latest.items():
                    if record is None:
                        continue
                    if _self._push_materialization(context, asset_key.to_user_string(), record):
                        pushed += 1
                if pushed:
                    context.advance_all_cursors()
                return dg.SkipReason(f"Pushed {pushed} materializations.")
            sensor = _the_sensor
        else:
            # Workspace-wide: poll event log via the instance.
            @dg.sensor(
                name=self.sensor_name,
                minimum_interval_seconds=self.interval_seconds,
                default_status=(
                    dg.DefaultSensorStatus.RUNNING
                    if self.default_status.upper() == "RUNNING"
                    else dg.DefaultSensorStatus.STOPPED
                ),
            )
            def _the_sensor(context: dg.SensorEvaluationContext):
                import json
                instance = context.instance
                cursor_data = json.loads(context.cursor) if context.cursor else {}
                last_event_id = int(cursor_data.get("last_event_id") or 0)

                from dagster import EventRecordsFilter, DagsterEventType
                records = instance.get_event_records(
                    event_records_filter=EventRecordsFilter(
                        event_type=DagsterEventType.ASSET_MATERIALIZATION,
                        after_cursor=last_event_id or None,
                    ),
                    limit=200,
                )
                pushed = 0
                new_last_id = last_event_id
                for r in records:
                    asset_mat = r.asset_materialization
                    if asset_mat is None:
                        continue
                    if _self._push_materialization(context, asset_mat.asset_key.to_user_string(), r):
                        pushed += 1
                    new_last_id = max(new_last_id, r.storage_id)
                if new_last_id > last_event_id:
                    context.update_cursor(json.dumps({"last_event_id": new_last_id}))
                return dg.SkipReason(f"Pushed {pushed} materializations.")
            sensor = _the_sensor

        return dg.Definitions(sensors=[sensor])

    def _push_materialization(self, context, asset_key_str: str, record) -> bool:
        try:
            import requests
        except ImportError:
            return False
        base_url = os.environ.get(self.base_url_env_var, "")
        if not base_url:
            return False

        ts = int(getattr(record, "timestamp", 0) or 0)
        body = (
            f'# HELP dagster_asset_materialization_count Asset materialization events.\n'
            f'# TYPE dagster_asset_materialization_count counter\n'
            f'dagster_asset_materialization_count{{asset_key="{asset_key_str}",location="{self.location_label}"}} 1\n'
            f'# HELP dagster_asset_last_materialization_timestamp Unix timestamp of last materialization.\n'
            f'# TYPE dagster_asset_last_materialization_timestamp gauge\n'
            f'dagster_asset_last_materialization_timestamp{{asset_key="{asset_key_str}",location="{self.location_label}"}} {ts}\n'
        )

        base = base_url.rstrip("/")
        headers = {"Content-Type": "text/plain; version=0.0.4"}
        if self.bearer_token_env_var and os.environ.get(self.bearer_token_env_var):
            headers["Authorization"] = f"Bearer {os.environ[self.bearer_token_env_var]}"
        if self.target_kind == "vm_import":
            url = f"{base}/api/v1/import/prometheus"
        else:
            # Sanitize for Push Gateway URL — slashes in asset keys break the URL.
            safe = asset_key_str.replace("/", "__")
            url = f"{base}/metrics/job/{self.push_job}/asset/{safe}"
        try:
            resp = requests.post(url, data=body.encode("utf-8"), headers=headers, timeout=self.request_timeout_seconds)
            resp.raise_for_status()
            return True
        except Exception as exc:
            context.log.warning(f"Failed to push materialization for {asset_key_str}: {exc}")
            return False
