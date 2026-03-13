"""Precisely Run Asset Component.

Triggers a Precisely Connect (formerly Syncsort DMX / Precisely Data Integration)
job on demand and waits for completion. Dagster owns the schedule; Precisely executes.

Includes PreciselyResource for shared connection config across components.

Precisely Connect REST API: https://{host}/api
"""
import time
from typing import Optional
import dagster as dg
from dagster import AssetExecutionContext, ConfigurableResource, MaterializeResult
from pydantic import Field


class PreciselyResource(ConfigurableResource):
    """Resource for connecting to the Precisely Connect REST API.

    Example:
        ```python
        PreciselyResource(
            host=EnvVar("PRECISELY_HOST"),
            api_token=EnvVar("PRECISELY_API_TOKEN"),
        )
        ```
    """

    host: str = Field(description="Precisely Connect host URL (e.g. https://precisely.mycompany.com)")
    api_token: str = Field(description="Precisely API token (Bearer)")

    def _base(self) -> str:
        return self.host.rstrip("/")

    def _headers(self) -> dict:
        return {"Authorization": f"Bearer {self.api_token}", "Accept": "application/json", "Content-Type": "application/json"}

    def run_job(self, job_id: str, parameters: dict | None = None) -> str:
        """Trigger a job run and return the run ID."""
        import requests
        resp = requests.post(
            f"{self._base()}/api/v1/jobs/{job_id}/run",
            headers=self._headers(),
            json={"parameters": parameters or {}},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        return str(data.get("runId", data.get("id", "")))

    def get_run_status(self, job_id: str, run_id: str) -> dict:
        import requests
        resp = requests.get(f"{self._base()}/api/v1/jobs/{job_id}/runs/{run_id}", headers=self._headers(), timeout=30)
        resp.raise_for_status()
        return resp.json()

    def get_latest_run(self, job_id: str) -> dict | None:
        import requests
        resp = requests.get(
            f"{self._base()}/api/v1/jobs/{job_id}/runs",
            headers=self._headers(),
            params={"limit": 1, "sort": "-startTime"},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        runs = data.get("runs", data if isinstance(data, list) else [])
        return runs[0] if runs else None


class PreciselyRunAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Trigger a Precisely Connect job on demand and surface results as a Dagster asset.

    Example (env vars):
        ```yaml
        type: dagster_component_templates.PreciselyRunAssetComponent
        attributes:
          asset_key: precisely/etl/load_customers
          job_id: "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
          host_env_var: PRECISELY_HOST
          api_token_env_var: PRECISELY_API_TOKEN
          parameters:
            inputPath: /data/inbound/customers
            outputSchema: analytics
        ```

    Example (resource):
        ```yaml
        type: dagster_component_templates.PreciselyRunAssetComponent
        attributes:
          asset_key: precisely/etl/load_customers
          job_id: "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
          resource_key: precisely
        ```
    """

    asset_key: str = Field(description="Dagster asset key (e.g. 'precisely/etl/load_customers')")
    job_id: str = Field(description="Precisely Connect job ID. Find in Precisely UI: Jobs → Job Details → ID.")
    host_env_var: Optional[str] = Field(default=None, description="Env var with Precisely Connect host URL")
    api_token_env_var: Optional[str] = Field(default=None, description="Env var with Precisely API token")
    resource_key: Optional[str] = Field(default=None, description="Key of a PreciselyResource")
    parameters: Optional[dict] = Field(default=None, description="Job parameters passed at runtime")
    poll_interval_seconds: float = Field(default=10.0, description="Seconds between status polls")
    timeout_seconds: int = Field(default=3600, description="Max seconds to wait for job completion")
    group_name: Optional[str] = Field(default="precisely", description="Dagster asset group name")
    deps: Optional[list[str]] = Field(default=None, description="Upstream asset keys this asset depends on (e.g. ['raw_orders', 'schema/asset'])")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        @dg.asset(
            key=dg.AssetKey(self.asset_key.split("/")),
            description=f"Run Precisely Connect job {self.job_id}",
            group_name=self.group_name,
            kinds={"precisely"},
            required_resource_keys={self.resource_key} if self.resource_key else set(),
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def precisely_run_asset(context: AssetExecutionContext) -> MaterializeResult:
            import os, requests

            if _self.resource_key:
                resource: PreciselyResource = getattr(context.resources, _self.resource_key)
                run_id = resource.run_job(_self.job_id, _self.parameters)
                base = resource._base()
                headers = resource._headers()
            else:
                host = os.environ.get(_self.host_env_var or "", "").rstrip("/")
                token = os.environ.get(_self.api_token_env_var or "", "")
                base = host
                headers = {"Authorization": f"Bearer {token}", "Accept": "application/json", "Content-Type": "application/json"}
                resp = requests.post(
                    f"{base}/api/v1/jobs/{_self.job_id}/run",
                    headers=headers,
                    json={"parameters": _self.parameters or {}},
                    timeout=30,
                )
                resp.raise_for_status()
                data = resp.json()
                run_id = str(data.get("runId", data.get("id", "")))

            context.log.info(f"Precisely job triggered. job_id={_self.job_id} run_id={run_id}")

            elapsed = 0.0
            while elapsed < _self.timeout_seconds:
                time.sleep(_self.poll_interval_seconds)
                elapsed += _self.poll_interval_seconds
                try:
                    resp = requests.get(f"{base}/api/v1/jobs/{_self.job_id}/runs/{run_id}", headers=headers, timeout=30)
                    resp.raise_for_status()
                    status_data = resp.json()
                    status = (status_data.get("status") or status_data.get("state") or "").upper()
                    context.log.info(f"Run {run_id} status: {status}")
                except Exception as e:
                    context.log.warning(f"Poll error: {e}")
                    continue

                if status in ("SUCCESS", "COMPLETED", "SUCCEEDED"):
                    return MaterializeResult(metadata={
                        "run_id": run_id,
                        "job_id": _self.job_id,
                        "status": status,
                        "records_processed": status_data.get("recordsProcessed", 0),
                        "records_failed": status_data.get("recordsFailed", 0),
                        "start_time": status_data.get("startTime", ""),
                        "end_time": status_data.get("endTime", ""),
                    })
                elif status in ("FAILED", "ERROR", "ABORTED", "CANCELLED"):
                    raise Exception(f"Precisely run {run_id} {status}. message={status_data.get('message', '')}")

            raise Exception(f"Precisely run {run_id} timed out after {_self.timeout_seconds}s")

        return dg.Definitions(assets=[precisely_run_asset])
