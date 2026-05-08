"""Precisely Run Asset Component.

Triggers a Precisely Connect ETL (formerly Syncsort DMX / DMExpress) job on
demand and waits for completion. Dagster owns the schedule; Precisely executes.

Includes PreciselyResource for shared connection config across components.

Precisely Connect ETL REST API:
- Job Status (verified): GET {base}/projects/{jobRunId}/status
  Returns a plain-text status string from this enum:
    WAITING | RUNNING | COMPLETED | COMPLETED_WITH_WARNINGS |
    COMPLETED_WITH_ERRORS | CANCELLED | ERRORED | LOST_CONTACT
  See https://help.precisely.com/r/Connect-ETL/pub/Latest/en-US/Connect-ETL-Rest-API-Reference/Job-Status
- Submit / trigger endpoint: not publicly documented. The default below
  (POST {base}/projects/{job_id}/run) is a best-guess RESTful shape and may
  need adjustment when validated against actual customer docs. Override
  with `submit_path_template` if your install uses a different path.
"""
import time
from typing import Any, Dict, List, Optional

import dagster as dg
from dagster import AssetExecutionContext, ConfigurableResource, MaterializeResult
from pydantic import Field


# Status values from the Connect ETL Job Status endpoint:
# https://help.precisely.com/r/Connect-ETL/pub/Latest/en-US/Connect-ETL-Rest-API-Reference/Job-Status
PRECISELY_TERMINAL_SUCCESS = {"COMPLETED", "COMPLETED_WITH_WARNINGS"}
PRECISELY_TERMINAL_FAIL = {"COMPLETED_WITH_ERRORS", "CANCELLED", "ERRORED", "LOST_CONTACT"}
PRECISELY_TERMINAL = PRECISELY_TERMINAL_SUCCESS | PRECISELY_TERMINAL_FAIL
PRECISELY_IN_PROGRESS = {"WAITING", "RUNNING"}


class PreciselyResource(ConfigurableResource):
    """Resource for connecting to the Precisely Connect ETL REST API.

    Example:
        ```python
        PreciselyResource(
            host=EnvVar("PRECISELY_HOST"),
            api_token=EnvVar("PRECISELY_API_TOKEN"),
        )
        ```
    """

    host: str = Field(description="Precisely Connect ETL host URL (e.g. https://precisely.mycompany.com)")
    api_token: str = Field(description="Precisely API token (Bearer)")
    submit_path_template: str = Field(
        default="/projects/{job_id}/run",
        description=(
            "Template for the job-submit endpoint. Default is a best-guess "
            "RESTful shape; replace if your Connect ETL install uses a "
            "different path. Available placeholder: {job_id}."
        ),
    )

    def _base(self) -> str:
        return self.host.rstrip("/")

    def _headers(self) -> Dict[str, str]:
        return {"Authorization": f"Bearer {self.api_token}", "Accept": "application/json"}

    def run_job(self, job_id: str, parameters: Optional[dict] = None) -> str:
        """Submit a job run and return the run ID returned by Precisely."""
        import requests
        path = self.submit_path_template.format(job_id=job_id)
        resp = requests.post(
            f"{self._base()}{path}",
            headers={**self._headers(), "Content-Type": "application/json"},
            json={"parameters": parameters or {}},
            timeout=30,
        )
        resp.raise_for_status()
        # Submit response shape varies by install; tolerate both JSON {runId|id|jobRunId}
        # and plain-text run-id responses.
        try:
            data = resp.json()
            return str(data.get("jobRunId") or data.get("runId") or data.get("id") or "")
        except ValueError:
            return resp.text.strip()

    def get_run_status(self, job_run_id: str) -> str:
        """Fetch the current status of a Connect ETL job run.

        Returns one of: WAITING | RUNNING | COMPLETED | COMPLETED_WITH_WARNINGS |
        COMPLETED_WITH_ERRORS | CANCELLED | ERRORED | LOST_CONTACT.

        Per Precisely Connect ETL docs, this endpoint returns a plain-text
        status string, not JSON.
        """
        import requests
        resp = requests.get(
            f"{self._base()}/projects/{job_run_id}/status",
            headers=self._headers(),
            timeout=30,
        )
        resp.raise_for_status()
        return resp.text.strip().upper()


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

    submit_path_template: str = Field(
        default="/projects/{job_id}/run",
        description=(
            "Template for the job-submit endpoint when not using a resource. "
            "Default is a best-guess RESTful shape; replace if your Connect ETL "
            "install uses a different path. Available placeholder: {job_id}. "
            "The status-poll path is fixed at /projects/{jobRunId}/status per "
            "Precisely's documented Connect ETL Job Status endpoint."
        ),
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        @dg.asset(
            key=dg.AssetKey(self.asset_key.split("/")),
            description=f"Run Precisely Connect ETL job {self.job_id}",
            group_name=self.group_name,
            kinds={"precisely"},
            required_resource_keys={self.resource_key} if self.resource_key else set(),
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def precisely_run_asset(context: AssetExecutionContext) -> MaterializeResult:
            import os
            import requests

            if _self.resource_key:
                resource: PreciselyResource = getattr(context.resources, _self.resource_key)
                run_id = resource.run_job(_self.job_id, _self.parameters)
                base = resource._base()
                headers = resource._headers()
            else:
                host = os.environ.get(_self.host_env_var or "", "").rstrip("/")
                token = os.environ.get(_self.api_token_env_var or "", "")
                base = host
                headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
                submit_path = _self.submit_path_template.format(job_id=_self.job_id)
                resp = requests.post(
                    f"{base}{submit_path}",
                    headers={**headers, "Content-Type": "application/json"},
                    json={"parameters": _self.parameters or {}},
                    timeout=30,
                )
                resp.raise_for_status()
                # Submit response shape varies; tolerate JSON or plain text run-id.
                try:
                    data = resp.json()
                    run_id = str(data.get("jobRunId") or data.get("runId") or data.get("id") or "")
                except ValueError:
                    run_id = resp.text.strip()

            if not run_id:
                raise Exception(
                    "Precisely submit returned no job-run ID. Check submit_path_template "
                    "matches your install's API and the response shape."
                )

            context.log.info(f"Precisely job triggered. job_id={_self.job_id} run_id={run_id}")

            # Poll the documented Job Status endpoint until terminal status.
            # Returns a plain-text status string from this enum:
            #   WAITING | RUNNING | COMPLETED | COMPLETED_WITH_WARNINGS |
            #   COMPLETED_WITH_ERRORS | CANCELLED | ERRORED | LOST_CONTACT
            elapsed = 0.0
            status = ""
            while elapsed < _self.timeout_seconds:
                time.sleep(_self.poll_interval_seconds)
                elapsed += _self.poll_interval_seconds
                try:
                    resp = requests.get(
                        f"{base}/projects/{run_id}/status",
                        headers=headers,
                        timeout=30,
                    )
                    resp.raise_for_status()
                    status = resp.text.strip().upper()
                    context.log.info(f"Run {run_id} status: {status}")
                except Exception as e:
                    context.log.warning(f"Poll error: {e}")
                    continue

                if status in PRECISELY_TERMINAL_SUCCESS:
                    return MaterializeResult(metadata={
                        "run_id": run_id,
                        "job_id": _self.job_id,
                        "status": status,
                    })
                if status in PRECISELY_TERMINAL_FAIL:
                    raise Exception(f"Precisely run {run_id} terminal status: {status}")
                # Otherwise: WAITING or RUNNING — keep polling.

            raise Exception(
                f"Precisely run {run_id} timed out after {_self.timeout_seconds}s "
                f"(last status: {status or 'unknown'})"
            )

        return dg.Definitions(assets=[precisely_run_asset])
