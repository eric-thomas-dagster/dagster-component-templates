"""FabricPipelineTriggerJobComponent.

Trigger a Microsoft Fabric Data Pipeline / Notebook / Dataflow Gen2 run
from Dagster. Op-job — pair with a schedule for periodic runs, or invoke
from another job.

For workspace-wide imports (one Dagster asset per Fabric pipeline), use
the `fabric_workspace` integration component instead. Use this job
component when you want to fire off ONE specific Fabric item run as part
of a Dagster job, with no asset-graph involvement.

Fabric REST API:
  POST .../workspaces/{ws}/items/{item}/jobs/instances?jobType={Pipeline|RunNotebook|Refresh}
  GET <Location header from POST response> — to poll status

Reference: https://learn.microsoft.com/rest/api/fabric/core/job-scheduler
"""

import os
import time
from typing import Optional

import dagster as dg
from pydantic import Field


_FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"
_FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"

# Fabric REST jobType values per item type
_JOB_TYPE_MAP = {
    "DataPipeline": "Pipeline",
    "Notebook": "RunNotebook",
    "Dataflow": "Refresh",
}


class FabricPipelineTriggerJobComponent(dg.Component, dg.Model, dg.Resolvable):
    """Op-job that triggers a Fabric pipeline / notebook / dataflow run."""

    job_name: str = Field(description="Dagster job name")

    workspace_id: str = Field(description="Fabric workspace ID (GUID)")
    item_id: str = Field(description="Fabric item ID (GUID) — the pipeline/notebook/dataflow")
    item_type: str = Field(
        default="DataPipeline",
        description="'DataPipeline' | 'Notebook' | 'Dataflow'",
    )
    parameters: Optional[dict] = Field(
        default=None,
        description="Optional parameters to pass to the pipeline/notebook (forwarded as JSON body).",
    )

    wait_for_completion: bool = Field(default=True)
    max_wait_seconds: int = Field(default=1800)
    poll_interval_seconds: int = Field(default=15)

    tenant_id_env_var: Optional[str] = Field(default=None)
    client_id_env_var: Optional[str] = Field(default=None)
    client_secret_env_var: Optional[str] = Field(default=None)

    schedule: Optional[str] = Field(default=None, description="Cron (e.g. '0 */6 * * *')")
    schedule_default_status: str = Field(default="STOPPED", description="STOPPED | RUNNING")
    tags: Optional[dict] = Field(default=None)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        @dg.op(name=f"{self.job_name}_op")
        def _the_op(context: dg.OpExecutionContext):
            self = _self
            try:
                from azure.identity import DefaultAzureCredential, ClientSecretCredential
            except ImportError as e:
                raise ImportError("azure-identity required: pip install azure-identity") from e
            import requests

            tenant = os.environ.get(self.tenant_id_env_var) if self.tenant_id_env_var else None
            client = os.environ.get(self.client_id_env_var) if self.client_id_env_var else None
            secret = os.environ.get(self.client_secret_env_var) if self.client_secret_env_var else None
            if tenant and client and secret:
                cred = ClientSecretCredential(tenant_id=tenant, client_id=client, client_secret=secret)
            else:
                cred = DefaultAzureCredential()

            token = cred.get_token(_FABRIC_SCOPE).token
            job_type = _JOB_TYPE_MAP.get(self.item_type, self.item_type)
            run_url = (
                f"{_FABRIC_API_BASE}/workspaces/{self.workspace_id}"
                f"/items/{self.item_id}/jobs/instances?jobType={job_type}"
            )
            body = {"executionData": {"parameters": self.parameters}} if self.parameters else None
            resp = requests.post(
                run_url,
                json=body,
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                timeout=60,
            )
            if resp.status_code >= 300:
                raise Exception(f"Fabric trigger failed: {resp.status_code} {resp.text[:300]}")

            instance_url = resp.headers.get("Location")
            if not instance_url:
                context.log.warning("Fabric: no Location header on trigger response; cannot poll")
                return

            context.log.info(f"Fabric job triggered. Instance: {instance_url}")

            if not self.wait_for_completion:
                return

            elapsed = 0
            while elapsed < self.max_wait_seconds:
                state_resp = requests.get(
                    instance_url,
                    headers={"Authorization": f"Bearer {token}"},
                    timeout=60,
                )
                state_resp.raise_for_status()
                state = state_resp.json()
                status = state.get("status", "Unknown")
                context.log.info(f"Fabric job status={status} elapsed={elapsed}s")
                if status in {"Completed", "Failed", "Cancelled"}:
                    if status != "Completed":
                        raise Exception(f"Fabric job ended with status: {status}. Details: {state}")
                    context.log.info(f"Fabric job completed.")
                    return
                time.sleep(self.poll_interval_seconds)
                elapsed += self.poll_interval_seconds

            raise Exception(f"Fabric job timed out after {self.max_wait_seconds}s")

        @dg.job(name=self.job_name, tags=self.tags or None)
        def _the_job():
            _the_op()

        defs_kwargs = {"jobs": [_the_job]}
        if self.schedule:
            status = (
                dg.DefaultScheduleStatus.RUNNING
                if self.schedule_default_status.upper() == "RUNNING"
                else dg.DefaultScheduleStatus.STOPPED
            )
            sched = dg.ScheduleDefinition(
                name=f"{self.job_name}_schedule",
                cron_schedule=self.schedule,
                job=_the_job,
                default_status=status,
            )
            defs_kwargs["schedules"] = [sched]
        return dg.Definitions(**defs_kwargs)
