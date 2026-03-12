"""Rivery Run Asset Component.

Triggers a Rivery river (pipeline) run on demand and waits for completion.
Dagster owns the schedule; Rivery executes the ELT.

Includes RiveryResource for shared connection config across components.

Rivery API: https://{region}.rivery.io/api/v1
"""
import time
from typing import Optional
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

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        @dg.asset(
            key=dg.AssetKey(self.asset_key.split("/")),
            description=f"Run Rivery river {self.river_id}",
            group_name=self.group_name,
            kinds={"rivery"},
            required_resource_keys={self.resource_key} if self.resource_key else set(),
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
