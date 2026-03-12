"""Ab Initio Run Asset Component.

Submits an Ab Initio graph or job to the EME REST API on demand and waits
for it to complete. Dagster owns the schedule; Ab Initio executes the work.

Ab Initio graphs are used in large enterprises for ETL, batch processing,
and complex data transformations. This component lets Dagster orchestrate
them as first-class assets — triggering execution, tracking lineage, and
surfacing run metadata.

Shares AbInitioResource with abinitio_job_sensor for connection config.

EME REST API: https://{host}/abinitio/eme-rest-api/v1
"""
import time
from typing import Optional
import dagster as dg
from dagster import AssetExecutionContext, ConfigurableResource, MaterializeResult
from pydantic import Field


class AbInitioResource(ConfigurableResource):
    """Resource for connecting to Ab Initio EME REST API.

    Define once and share across AbInitioRunAssetComponent and
    AbInitioJobSensorComponent instances.

    Example:
        ```python
        from dagster import EnvVar, Definitions
        from dagster_component_templates.assets.abinitio_run_asset.component import AbInitioResource

        defs = Definitions(
            resources={
                "abinitio": AbInitioResource(
                    eme_url=EnvVar("ABINITIO_EME_URL"),
                    username=EnvVar("ABINITIO_USERNAME"),
                    password=EnvVar("ABINITIO_PASSWORD"),
                )
            }
        )
        ```
    """

    eme_url: str = Field(description="Ab Initio EME base URL (e.g. https://eme.mycompany.com)")
    username: str = Field(description="Ab Initio EME username")
    password: str = Field(description="Ab Initio EME password")

    def _headers(self) -> dict:
        import base64
        credentials = base64.b64encode(f"{self.username}:{self.password}".encode()).decode()
        return {"Authorization": f"Basic {credentials}", "Accept": "application/json"}

    def get_latest_run(self, job_path: str) -> dict | None:
        import requests, urllib.parse
        encoded = urllib.parse.quote(job_path, safe="")
        resp = requests.get(
            f"{self.eme_url.rstrip('/')}/abinitio/eme-rest-api/v1/jobs/{encoded}/runs",
            headers=self._headers(),
            params={"limit": 1, "sort": "-startTime"},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        runs = data.get("runs", data if isinstance(data, list) else [])
        return runs[0] if runs else None

    def trigger_job(self, job_path: str, parameters: dict | None = None) -> str:
        """Trigger a job run and return the run ID."""
        import requests, urllib.parse
        encoded = urllib.parse.quote(job_path, safe="")
        body = parameters or {}
        resp = requests.post(
            f"{self.eme_url.rstrip('/')}/abinitio/eme-rest-api/v1/jobs/{encoded}/runs",
            headers={**self._headers(), "Content-Type": "application/json"},
            json=body,
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        return str(data.get("runId", data.get("id", "")))

    def get_run_status(self, job_path: str, run_id: str) -> dict:
        """Return the status dict for a specific run."""
        import requests, urllib.parse
        encoded = urllib.parse.quote(job_path, safe="")
        resp = requests.get(
            f"{self.eme_url.rstrip('/')}/abinitio/eme-rest-api/v1/jobs/{encoded}/runs/{run_id}",
            headers=self._headers(),
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()


class AbInitioRunAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Submit an Ab Initio graph or job and surface it as a Dagster asset materialization.

    Dagster owns the schedule. When the asset is materialized, Dagster calls the
    Ab Initio EME API to trigger the job, waits for completion, and records the
    result. Supports passing graph parameters (date ranges, input paths, flags, etc.)
    that control Ab Initio graph behavior.

    Use a resource_key pointing to an AbInitioResource for shared connection config,
    or provide env vars directly.

    Example (env vars):
        ```yaml
        type: dagster_component_templates.AbInitioRunAssetComponent
        attributes:
          asset_key: abinitio/etl/load_orders
          job_path: /prod/etl/load_orders
          eme_url_env_var: ABINITIO_EME_URL
          username_env_var: ABINITIO_USERNAME
          password_env_var: ABINITIO_PASSWORD
          parameters:
            AI_START_DATE: "2024-01-01"
            AI_END_DATE: "2024-01-31"
        ```

    Example (resource):
        ```yaml
        type: dagster_component_templates.AbInitioRunAssetComponent
        attributes:
          asset_key: abinitio/etl/load_orders
          job_path: /prod/etl/load_orders
          resource_key: abinitio
          parameters:
            AI_START_DATE: "2024-01-01"
        ```
    """

    asset_key: str = Field(description="Dagster asset key (e.g. 'abinitio/etl/load_orders')")
    job_path: str = Field(description="Ab Initio EME job/graph path (e.g. /prod/etl/load_orders)")
    # Connection via env vars
    eme_url_env_var: Optional[str] = Field(default=None, description="Env var with Ab Initio EME base URL")
    username_env_var: Optional[str] = Field(default=None, description="Env var with Ab Initio EME username")
    password_env_var: Optional[str] = Field(default=None, description="Env var with Ab Initio EME password")
    # Connection via resource
    resource_key: Optional[str] = Field(
        default=None,
        description="Key of an AbInitioResource. When set, env var fields are ignored.",
    )
    # Graph execution parameters (passed to Ab Initio as job parameters)
    parameters: Optional[dict] = Field(
        default=None,
        description="Key-value parameters passed to the Ab Initio graph (e.g. AI_START_DATE, AI_INPUT_PATH)",
    )
    poll_interval_seconds: float = Field(default=15.0, description="Seconds between status polls")
    timeout_seconds: int = Field(default=3600, description="Max seconds to wait for job completion")
    group_name: Optional[str] = Field(default="abinitio", description="Dagster asset group name")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        @dg.asset(
            key=dg.AssetKey(self.asset_key.split("/")),
            description=f"Run Ab Initio graph {self.job_path}",
            group_name=self.group_name,
            kinds={"abinitio"},
            required_resource_keys={self.resource_key} if self.resource_key else set(),
        )
        def abinitio_run_asset(context: AssetExecutionContext) -> MaterializeResult:
            import os, base64, urllib.parse
            try:
                import requests
            except ImportError:
                raise Exception("requests not installed. Run: pip install requests")

            # Get resource or build inline client
            if _self.resource_key:
                resource: AbInitioResource = getattr(context.resources, _self.resource_key)
                run_id = resource.trigger_job(_self.job_path, _self.parameters)
            else:
                eme_url = os.environ.get(_self.eme_url_env_var or "", "").rstrip("/")
                username = os.environ.get(_self.username_env_var or "", "")
                password = os.environ.get(_self.password_env_var or "", "")
                credentials = base64.b64encode(f"{username}:{password}".encode()).decode()
                headers = {
                    "Authorization": f"Basic {credentials}",
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                }
                encoded_path = urllib.parse.quote(_self.job_path, safe="")
                base = eme_url
                resp = requests.post(
                    f"{base}/abinitio/eme-rest-api/v1/jobs/{encoded_path}/runs",
                    headers=headers,
                    json=_self.parameters or {},
                    timeout=30,
                )
                resp.raise_for_status()
                data = resp.json()
                run_id = str(data.get("runId", data.get("id", "")))

            context.log.info(f"Ab Initio job triggered. job_path={_self.job_path} run_id={run_id}")

            # Poll for completion
            elapsed = 0.0
            while elapsed < _self.timeout_seconds:
                time.sleep(_self.poll_interval_seconds)
                elapsed += _self.poll_interval_seconds
                try:
                    if _self.resource_key:
                        resource = getattr(context.resources, _self.resource_key)
                        status_data = resource.get_run_status(_self.job_path, run_id)
                    else:
                        encoded_path = urllib.parse.quote(_self.job_path, safe="")
                        eme_url = os.environ.get(_self.eme_url_env_var or "", "").rstrip("/")
                        credentials = base64.b64encode(
                            f"{os.environ.get(_self.username_env_var or '', '')}:{os.environ.get(_self.password_env_var or '', '')}".encode()
                        ).decode()
                        status_resp = requests.get(
                            f"{eme_url}/abinitio/eme-rest-api/v1/jobs/{encoded_path}/runs/{run_id}",
                            headers={"Authorization": f"Basic {credentials}", "Accept": "application/json"},
                            timeout=30,
                        )
                        status_resp.raise_for_status()
                        status_data = status_resp.json()

                    # Ab Initio run statuses: ok, failed, running, aborted, queued
                    status = (status_data.get("status") or "").lower()
                    context.log.info(f"Run {run_id} status: {status}")
                except Exception as e:
                    context.log.warning(f"Poll error: {e}")
                    continue

                if status == "ok":
                    return MaterializeResult(
                        metadata={
                            "run_id": run_id,
                            "job_path": _self.job_path,
                            "status": status,
                            "start_time": status_data.get("startTime", ""),
                            "end_time": status_data.get("endTime", ""),
                            "duration_seconds": status_data.get("durationSeconds", 0),
                            "records_processed": status_data.get("recordsProcessed", 0),
                        }
                    )
                elif status in ("failed", "aborted", "error"):
                    error_msg = status_data.get("errorMessage", status_data.get("message", ""))
                    raise Exception(
                        f"Ab Initio run {run_id} {status}. job_path={_self.job_path}. {error_msg}"
                    )
                # running / queued — keep polling

            raise Exception(
                f"Ab Initio run {run_id} timed out after {_self.timeout_seconds}s. job_path={_self.job_path}"
            )

        return dg.Definitions(assets=[abinitio_run_asset])
