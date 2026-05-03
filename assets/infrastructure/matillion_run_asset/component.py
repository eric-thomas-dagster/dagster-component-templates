"""Matillion Run Asset Component.

Triggers a Matillion ETL job on demand and waits for it to complete.
Dagster owns the schedule; Matillion executes the transformation.

Includes MatillionResource for shared connection config across components.

Matillion ETL REST API: https://{instance}/rest/v1
"""
import time
from typing import Optional
import dagster as dg
from dagster import AssetExecutionContext, ConfigurableResource, MaterializeResult
from pydantic import Field


class MatillionResource(ConfigurableResource):
    """Resource for connecting to the Matillion ETL REST API.

    Example:
        ```python
        MatillionResource(
            instance_url=EnvVar("MATILLION_INSTANCE_URL"),
            username=EnvVar("MATILLION_USERNAME"),
            password=EnvVar("MATILLION_PASSWORD"),
        )
        ```
    """

    instance_url: str = Field(description="Matillion instance URL (e.g. https://matillion.mycompany.com)")
    username: str = Field(description="Matillion username")
    password: str = Field(description="Matillion password")

    def _headers(self) -> dict:
        import base64
        credentials = base64.b64encode(f"{self.username}:{self.password}".encode()).decode()
        return {"Authorization": f"Basic {credentials}", "Accept": "application/json"}

    def run_job(self, project: str, version: str, job_name: str, variables: dict | None = None) -> int:
        """Trigger a job run and return the run ID."""
        import requests
        base = self.instance_url.rstrip("/")
        resp = requests.post(
            f"{base}/rest/v1/project/name/{project}/version/name/{version}/job/name/{job_name}/run",
            headers={**self._headers(), "Content-Type": "application/json"},
            json={"variables": variables or {}},
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json().get("id", resp.json())  # returns run ID (int)

    def get_run_status(self, project: str, version: str, job_name: str, run_id: int) -> dict:
        """Return status dict for a specific run."""
        import requests
        base = self.instance_url.rstrip("/")
        resp = requests.get(
            f"{base}/rest/v1/project/name/{project}/version/name/{version}/job/name/{job_name}/run/id/{run_id}",
            headers=self._headers(),
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()

    def get_latest_run(self, project: str, version: str, job_name: str) -> dict | None:
        """Return the most recent run for a job."""
        import requests
        base = self.instance_url.rstrip("/")
        resp = requests.get(
            f"{base}/rest/v1/project/name/{project}/version/name/{version}/job/name/{job_name}/runs",
            headers=self._headers(),
            params={"limit": 1},
            timeout=30,
        )
        resp.raise_for_status()
        runs = resp.json()
        if isinstance(runs, list) and runs:
            return runs[0]
        return runs.get("runs", [None])[0] if isinstance(runs, dict) else None


class MatillionRunAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Trigger a Matillion ETL job on demand and surface results as a Dagster asset.

    Example (env vars):
        ```yaml
        type: dagster_component_templates.MatillionRunAssetComponent
        attributes:
          asset_key: matillion/transforms/load_orders
          project: MyProject
          version: default
          job_name: Load Orders
          instance_url_env_var: MATILLION_INSTANCE_URL
          username_env_var: MATILLION_USERNAME
          password_env_var: MATILLION_PASSWORD
        ```

    Example (resource):
        ```yaml
        type: dagster_component_templates.MatillionRunAssetComponent
        attributes:
          asset_key: matillion/transforms/load_orders
          project: MyProject
          version: default
          job_name: Load Orders
          resource_key: matillion
        ```
    """

    asset_key: str = Field(description="Dagster asset key (e.g. 'matillion/transforms/load_orders')")
    project: str = Field(description="Matillion project name")
    version: str = Field(default="default", description="Matillion project version name")
    job_name: str = Field(description="Matillion job name to run")
    instance_url_env_var: Optional[str] = Field(default=None, description="Env var with Matillion instance URL")
    username_env_var: Optional[str] = Field(default=None, description="Env var with Matillion username")
    password_env_var: Optional[str] = Field(default=None, description="Env var with Matillion password")
    resource_key: Optional[str] = Field(default=None, description="Key of a MatillionResource")
    variables: Optional[dict] = Field(default=None, description="Job variable overrides passed at runtime")
    poll_interval_seconds: float = Field(default=10.0, description="Seconds between status polls")
    timeout_seconds: int = Field(default=3600, description="Max seconds to wait for job completion")
    group_name: Optional[str] = Field(default="matillion", description="Dagster asset group name")
    deps: Optional[list[str]] = Field(default=None, description="Upstream asset keys this asset depends on (e.g. ['raw_orders', 'schema/asset'])")


    description: Optional[str] = Field(
        default=None,
        description="Asset description shown in the Dagster catalog.",
    )

    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners — team names ('team:analytics') or email addresses.",
    )

    asset_tags: Optional[Dict[str, str]] = Field(
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

    partition_date_column: Optional[str] = Field(
        default=None,
        description="Column used to filter the upstream DataFrame to the current date partition key.",
    )

    partition_values: Optional[str] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning, e.g. 'acme,globex,initech'.",
    )

    partition_static_dim: Optional[str] = Field(
        default=None,
        description="Dimension name for the static axis in multi-partitioning, e.g. 'customer'.",
    )

    partition_static_column: Optional[str] = Field(
        default=None,
        description="Column used to filter the upstream DataFrame to the current static partition value.",
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

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        @dg.asset(
            key=dg.AssetKey(self.asset_key.split("/")),
            description=f"Run Matillion job '{self.job_name}' in project {self.project}",
            group_name=self.group_name,
            kinds={"matillion"},
            required_resource_keys={self.resource_key} if self.resource_key else set(),
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def matillion_run_asset(context: AssetExecutionContext) -> MaterializeResult:
            import os, base64, requests

            if _self.resource_key:
                resource: MatillionResource = getattr(context.resources, _self.resource_key)
                run_id = resource.run_job(_self.project, _self.version, _self.job_name, _self.variables)
            else:
                instance_url = os.environ.get(_self.instance_url_env_var or "", "").rstrip("/")
                username = os.environ.get(_self.username_env_var or "", "")
                password = os.environ.get(_self.password_env_var or "", "")
                credentials = base64.b64encode(f"{username}:{password}".encode()).decode()
                headers = {"Authorization": f"Basic {credentials}", "Content-Type": "application/json", "Accept": "application/json"}
                resp = requests.post(
                    f"{instance_url}/rest/v1/project/name/{_self.project}/version/name/{_self.version}/job/name/{_self.job_name}/run",
                    headers=headers,
                    json={"variables": _self.variables or {}},
                    timeout=30,
                )
                resp.raise_for_status()
                run_id = resp.json().get("id", resp.json())

            context.log.info(f"Matillion job triggered. project={_self.project} job={_self.job_name} run_id={run_id}")

            elapsed = 0.0
            while elapsed < _self.timeout_seconds:
                time.sleep(_self.poll_interval_seconds)
                elapsed += _self.poll_interval_seconds
                try:
                    if _self.resource_key:
                        resource = getattr(context.resources, _self.resource_key)
                        status_data = resource.get_run_status(_self.project, _self.version, _self.job_name, run_id)
                    else:
                        instance_url = os.environ.get(_self.instance_url_env_var or "", "").rstrip("/")
                        credentials = base64.b64encode(
                            f"{os.environ.get(_self.username_env_var or '', '')}:{os.environ.get(_self.password_env_var or '', '')}".encode()
                        ).decode()
                        resp = requests.get(
                            f"{instance_url}/rest/v1/project/name/{_self.project}/version/name/{_self.version}/job/name/{_self.job_name}/run/id/{run_id}",
                            headers={"Authorization": f"Basic {credentials}", "Accept": "application/json"},
                            timeout=30,
                        )
                        resp.raise_for_status()
                        status_data = resp.json()
                    status = (status_data.get("state") or status_data.get("status") or "").upper()
                    context.log.info(f"Run {run_id} status: {status}")
                except Exception as e:
                    context.log.warning(f"Poll error: {e}")
                    continue

                if status == "SUCCESS":
                    return MaterializeResult(metadata={
                        "run_id": run_id,
                        "project": _self.project,
                        "job_name": _self.job_name,
                        "status": status,
                        "start_time": status_data.get("startTime", ""),
                        "end_time": status_data.get("endTime", ""),
                        "row_count": status_data.get("rowCount", 0),
                    })
                elif status in ("FAILED", "CANCELLED", "ABORTED"):
                    raise Exception(f"Matillion run {run_id} {status}. message={status_data.get('message', '')}")

            raise Exception(f"Matillion run {run_id} timed out after {_self.timeout_seconds}s")

        return dg.Definitions(assets=[matillion_run_asset])
