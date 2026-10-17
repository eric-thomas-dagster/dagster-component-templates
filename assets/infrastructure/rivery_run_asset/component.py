"""Rivery Run Asset Component.

Triggers a Rivery river (pipeline) run on demand and waits for completion.
Dagster owns the schedule; Rivery executes the ELT.

Includes RiveryResource for shared connection config across components.

Rivery API: https://{region}.rivery.io/api/v1
"""
import time
from typing import Dict, List, Optional, Type, Union
import dagster as dg
from dagster import AssetExecutionContext, ConfigurableResource, MaterializeResult
from pydantic import Field


class RiveryResource(ConfigurableResource):
    """Resource for connecting to the Rivery REST API.

    Example:
        ```python
        RiveryResource(
            api_token=EnvVar("RIVERY_API_TOKEN"),
            region="us2",
        )
        ```
    """

    api_token: str = Field(description="Rivery API token")
    region: str = Field(default="us2", description="Rivery region (e.g. us2, eu1, ap1)")

    def _base(self) -> str:
        return f"https://{self.region}.rivery.io/api/v1"

    def _headers(self) -> dict:
        return {"Authorization": f"Bearer {self.api_token}", "Accept": "application/json", "Content-Type": "application/json"}

    def run_river(self, river_id: str) -> str:
        """Trigger a river run and return the run ID."""
        import requests
        resp = requests.post(f"{self._base()}/rivers/{river_id}/run", headers=self._headers(), timeout=30)
        resp.raise_for_status()
        data = resp.json()
        return str(data.get("runId", data.get("id", "")))

    def get_run_status(self, river_id: str, run_id: str) -> dict:
        import requests
        resp = requests.get(f"{self._base()}/rivers/{river_id}/runs/{run_id}", headers=self._headers(), timeout=30)
        resp.raise_for_status()
        return resp.json()

    def get_latest_run(self, river_id: str) -> dict | None:
        import requests
        resp = requests.get(f"{self._base()}/rivers/{river_id}/runs", headers=self._headers(), params={"limit": 1}, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        runs = data.get("runs", data if isinstance(data, list) else [])
        return runs[0] if runs else None



# BEGIN field-shape normalization (inlined per component; keep copies identical — see tests/test_field_shapes.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def coerce_field_shapes(data, *, lists=(), csv=(), tags=()):
    """Accept the shapes people (and LLMs) naturally write for common boilerplate fields."""
    if not isinstance(data, dict):
        return data
    out = dict(data)
    for k in lists:  # List[str]: "a" / "a, b" -> ["a", "b"]; strip + drop empties
        v = out.get(k)
        if isinstance(v, str):
            v = v.split(",")
        if isinstance(v, (list, tuple)):
            out[k] = [x.strip() if isinstance(x, str) else x for x in v if not (isinstance(x, str) and not x.strip())]
    for k in csv:  # comma-separated str given as a list -> "a,b"
        v = out.get(k)
        if isinstance(v, (list, tuple, set)):
            out[k] = ",".join(str(x).strip() for x in v)
    for k in tags:  # Dict[str, str]: scalars -> str, ["k=v"] / ["k:v"] -> {k: v}
        v = out.get(k)
        if isinstance(v, (list, tuple)) and v and all(isinstance(x, str) and ("=" in x or ":" in x) for x in v):
            v = dict((p.strip() for p in (x.split("=", 1) if "=" in x else x.split(":", 1))) for x in v)
        if isinstance(v, dict):
            out[k] = {str(a).strip(): (str(b).lower() if isinstance(b, bool) else str(b)) for a, b in v.items() if b is not None}
    return out
# END field-shape normalization


class RiveryRunAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Trigger a Rivery river run on demand and surface results as a Dagster asset.

    Example (env vars):
        ```yaml
        type: dagster_component_templates.RiveryRunAssetComponent
        attributes:
          asset_key: rivery/ingest/salesforce_accounts
          river_id: "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
          api_token_env_var: RIVERY_API_TOKEN
          region: us2
        ```

    Example (resource):
        ```yaml
        type: dagster_component_templates.RiveryRunAssetComponent
        attributes:
          asset_key: rivery/ingest/salesforce_accounts
          river_id: "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
          resource_key: rivery
        ```
    """

    asset_key: str = Field(description="Dagster asset key (e.g. 'rivery/ingest/salesforce_accounts')")
    river_id: str = Field(description="Rivery river (pipeline) ID. Find in Rivery UI: River → Settings → River ID.")
    api_token_env_var: Optional[str] = Field(default=None, description="Env var with Rivery API token")
    region: str = Field(default="us2", description="Rivery region (e.g. us2, eu1, ap1)")
    resource_key: Optional[str] = Field(default=None, description="Key of a RiveryResource")
    poll_interval_seconds: float = Field(default=10.0, description="Seconds between status polls")
    timeout_seconds: int = Field(default=3600, description="Max seconds to wait for river run")
    group_name: Optional[str] = Field(default="rivery", description="Dagster asset group name")
    deps: Optional[list[str]] = Field(default=None, description="Upstream asset keys this asset depends on (e.g. ['raw_orders', 'schema/asset'])")


    description: Optional[str] = Field(
        default=None,
        description="Asset description shown in the Dagster catalog.",
    )

    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners — team names ('team:analytics') or email addresses.",
    )

    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(
        default=None,
        description="Additional key-value tags applied to the asset in the Dagster catalog.",
    )

    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds for the catalog (e.g. ['snowflake', 'python']). Auto-inferred from component name when unset.",
    )

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

    partition_date_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Not supported by this component: a Rivery river is triggered as configured and takes no partition-dependent input here, so partitioning is ignored and the asset stays unpartitioned.",
    )

    partition_values: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning, e.g. 'acme,globex,initech'.",
    )

    partition_static_dim: Optional[str] = Field(
        default=None,
        description="Dimension name for the static axis in multi-partitioning, e.g. 'customer'.",
    )

    partition_static_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Not supported by this component: a Rivery river is triggered as configured and takes no partition-dependent input here, so partitioning is ignored and the asset stays unpartitioned.",
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

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('deps', 'owners', 'kinds'), csv=('partition_values',), tags=('asset_tags',))

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        # freshness_max_lag_minutes / freshness_cron -> FreshnessPolicy (applied to the asset(s) below)
        # retry_policy_* settings -> RetryPolicy (applied to the asset(s) below)
        if self.partition_type:
            import warnings
        
            warnings.warn(
                "rivery_run_asset ignores partition_type: a Rivery river takes no partition-dependent input here, "
                "so the asset is not partitioned.",
                UserWarning,
                stacklevel=2,
            )

        _retry_policy_obj = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
        
            _retry_policy_obj = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )
        _freshness_policy_obj = (
            _freshness_policy_compat(self.freshness_max_lag_minutes, self.freshness_cron)
            if self.freshness_max_lag_minutes is not None else None
        )
        _self = self

        @dg.asset(retry_policy=_retry_policy_obj, freshness_policy=_freshness_policy_obj, 
            key=dg.AssetKey(self.asset_key.split("/")),
            description=f"Run Rivery river {self.river_id}",
            group_name=self.group_name,
            kinds={"rivery"},
            required_resource_keys={self.resource_key} if self.resource_key else set(),
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def rivery_run_asset(context: AssetExecutionContext) -> MaterializeResult:
            import os, requests

            if _self.resource_key:
                resource: RiveryResource = getattr(context.resources, _self.resource_key)
                run_id = resource.run_river(_self.river_id)
                base = resource._base()
                headers = resource._headers()
            else:
                token = os.environ.get(_self.api_token_env_var or "", "")
                base = f"https://{_self.region}.rivery.io/api/v1"
                headers = {"Authorization": f"Bearer {token}", "Accept": "application/json", "Content-Type": "application/json"}
                resp = requests.post(f"{base}/rivers/{_self.river_id}/run", headers=headers, timeout=30)
                resp.raise_for_status()
                data = resp.json()
                run_id = str(data.get("runId", data.get("id", "")))

            context.log.info(f"Rivery river triggered. river_id={_self.river_id} run_id={run_id}")

            elapsed = 0.0
            while elapsed < _self.timeout_seconds:
                time.sleep(_self.poll_interval_seconds)
                elapsed += _self.poll_interval_seconds
                try:
                    resp = requests.get(f"{base}/rivers/{_self.river_id}/runs/{run_id}", headers=headers, timeout=30)
                    resp.raise_for_status()
                    status_data = resp.json()
                    status = (status_data.get("status") or status_data.get("state") or "").lower()
                    context.log.info(f"Run {run_id} status: {status}")
                except Exception as e:
                    context.log.warning(f"Poll error: {e}")
                    continue

                if status in ("success", "succeeded", "completed"):
                    return MaterializeResult(metadata={
                        "run_id": run_id,
                        "river_id": _self.river_id,
                        "status": status,
                        "rows_loaded": status_data.get("rowsLoaded", status_data.get("rows_loaded", 0)),
                        "start_time": status_data.get("startTime", ""),
                        "end_time": status_data.get("endTime", ""),
                    })
                elif status in ("failed", "error", "cancelled"):
                    raise Exception(f"Rivery run {run_id} {status}. message={status_data.get('message', '')}")

            raise Exception(f"Rivery run {run_id} timed out after {_self.timeout_seconds}s")

        return dg.Definitions(assets=[rivery_run_asset])


# BEGIN freshness-policy compat (inlined per component; keep copies identical, see tests/test_freshness_policy_compat.py)
def _freshness_policy_compat(max_lag_minutes, cron_schedule=None):
    """Build a FreshnessPolicy on the current Dagster API (time_window / cron) and on the older constructor."""
    from datetime import timedelta

    from dagster import FreshnessPolicy

    lag = timedelta(minutes=max_lag_minutes)
    if hasattr(FreshnessPolicy, "time_window"):
        if cron_schedule:
            return FreshnessPolicy.cron(deadline_cron=cron_schedule, lower_bound_delta=lag)
        return FreshnessPolicy.time_window(fail_window=lag)
    return FreshnessPolicy(maximum_lag_minutes=max_lag_minutes, cron_schedule=cron_schedule)
# END freshness-policy compat
