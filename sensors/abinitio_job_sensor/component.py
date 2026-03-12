"""Ab Initio Job Sensor Component.

Polls the Ab Initio EME (Enterprise Meta Environment) REST API and triggers
a Dagster job when a specified Ab Initio graph or job run completes successfully.

Includes an AbInitioResource for reuse across components.

EME REST API: https://{host}/abinitio/eme-rest-api/v1
"""
from typing import Optional
import dagster as dg
from dagster import RunRequest, SensorEvaluationContext, SensorResult, ConfigurableResource, sensor
from dagster._core.definitions.sensor_definition import DefaultSensorStatus
from pydantic import Field


class AbInitioResource(ConfigurableResource):
    """Resource for connecting to Ab Initio EME REST API.

    Example (dagster.yaml or Definitions):
        ```python
        AbInitioResource(
            eme_url=EnvVar("ABINITIO_EME_URL"),
            username=EnvVar("ABINITIO_USERNAME"),
            password=EnvVar("ABINITIO_PASSWORD"),
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
        """Return the most recent run for a job path, or None if no runs exist."""
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


class AbInitioJobSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Trigger a Dagster job when an Ab Initio EME job run completes.

    Use a resource_key pointing to an AbInitioResource for shared connection config,
    or provide env vars directly.

    Example (env vars):
        ```yaml
        type: dagster_component_templates.AbInitioJobSensorComponent
        attributes:
          sensor_name: abinitio_etl_done
          eme_url_env_var: ABINITIO_EME_URL
          username_env_var: ABINITIO_USERNAME
          password_env_var: ABINITIO_PASSWORD
          job_path: /prod/etl/load_orders
          job_name: downstream_processing_job
        ```

    Example (resource):
        ```yaml
        type: dagster_component_templates.AbInitioJobSensorComponent
        attributes:
          sensor_name: abinitio_etl_done
          resource_key: abinitio
          job_path: /prod/etl/load_orders
          job_name: downstream_processing_job
        ```
    """

    sensor_name: str = Field(description="Unique sensor name")
    job_path: str = Field(description="EME job/graph path to monitor (e.g. /prod/etl/load_orders)")
    job_name: str = Field(description="Dagster job to trigger when Ab Initio run completes")
    # Connection via env vars
    eme_url_env_var: Optional[str] = Field(default=None, description="Env var with Ab Initio EME base URL")
    username_env_var: Optional[str] = Field(default=None, description="Env var with Ab Initio EME username")
    password_env_var: Optional[str] = Field(default=None, description="Env var with Ab Initio EME password")
    # Connection via resource
    resource_key: Optional[str] = Field(
        default=None,
        description="Key of an AbInitioResource. When set, env var fields are ignored.",
    )
    minimum_interval_seconds: int = Field(default=120, description="Seconds between polls")
    default_status: str = Field(default="running", description="running or stopped")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        required_resource_keys = {self.resource_key} if self.resource_key else set()
        default_status = (
            DefaultSensorStatus.RUNNING if self.default_status == "running"
            else DefaultSensorStatus.STOPPED
        )

        @sensor(
            name=_self.sensor_name,
            minimum_interval_seconds=_self.minimum_interval_seconds,
            default_status=default_status,
            job_name=_self.job_name,
            required_resource_keys=required_resource_keys,
        )
        def abinitio_job_sensor(context: SensorEvaluationContext):
            import os, base64, urllib.parse
            try:
                import requests
            except ImportError:
                return SensorResult(skip_reason="requests not installed")

            # Use resource or env vars
            if _self.resource_key:
                resource = getattr(context.resources, _self.resource_key)
                try:
                    latest = resource.get_latest_run(_self.job_path)
                except Exception as e:
                    return SensorResult(skip_reason=f"Ab Initio EME API error: {e}")
            else:
                eme_url = os.environ.get(_self.eme_url_env_var or "", "").rstrip("/")
                username = os.environ.get(_self.username_env_var or "", "")
                password = os.environ.get(_self.password_env_var or "", "")
                credentials = base64.b64encode(f"{username}:{password}".encode()).decode()
                headers = {"Authorization": f"Basic {credentials}", "Accept": "application/json"}
                encoded_path = urllib.parse.quote(_self.job_path, safe="")
                try:
                    resp = requests.get(
                        f"{eme_url}/abinitio/eme-rest-api/v1/jobs/{encoded_path}/runs",
                        headers=headers,
                        params={"limit": 1, "sort": "-startTime"},
                        timeout=30,
                    )
                    resp.raise_for_status()
                    data = resp.json()
                    runs = data.get("runs", data if isinstance(data, list) else [])
                    latest = runs[0] if runs else None
                except Exception as e:
                    return SensorResult(skip_reason=f"Ab Initio EME API error: {e}")

            if not latest:
                return SensorResult(skip_reason="No runs found for this job")

            run_id = str(latest.get("runId", latest.get("id", "")))
            status = (latest.get("status") or "").lower()  # ok, failed, running, aborted
            end_time = latest.get("endTime", "")

            cursor = context.cursor or ""
            if status == "ok" and run_id != cursor:
                return SensorResult(
                    run_requests=[RunRequest(
                        run_key=run_id,
                        run_config={"ops": {"config": {
                            "job_path": _self.job_path,
                            "run_id": run_id,
                            "end_time": end_time,
                        }}},
                    )],
                    cursor=run_id,
                )

            return SensorResult(skip_reason=f"Run {run_id} status: {status}")

        return dg.Definitions(sensors=[abinitio_job_sensor])
