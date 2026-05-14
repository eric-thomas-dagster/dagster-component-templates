"""FabricCapacityAdminJobComponent.

Pause / resume / scale a Microsoft Fabric capacity via the Azure
management REST API. Op-job (not an asset) — same shape as
synapse_sql_pool_admin_job, but for Fabric capacity lifecycle.

Why this matters: Fabric capacity bills hourly even when idle. F2 is
~$0.21/hr ($154/mo if always on). Pausing nightly + on weekends
roughly halves Fabric spend.

Typical schedule pair:
  - Pause: weekday 19:00 + Friday 19:00 (stays paused all weekend)
  - Resume: weekday 06:00 (Mon-Fri)

The Fabric capacity Management REST API:
  POST .../capacities/{name}/suspend?api-version=2023-11-01
  POST .../capacities/{name}/resume?api-version=2023-11-01
  PATCH .../capacities/{name}?api-version=2023-11-01   (for scale via SKU change)
"""

import os
from typing import Optional

import dagster as dg
from pydantic import Field


class FabricCapacityAdminJobComponent(dg.Component, dg.Model, dg.Resolvable):
    """Pause/resume/scale a Microsoft Fabric capacity."""

    job_name: str = Field(description="Dagster job name (e.g. fabric_capacity_pause_nightly)")
    action: str = Field(
        default="suspend",
        description="'suspend' (pause billing) | 'resume' | 'scale' (change SKU)",
    )
    target_sku: Optional[str] = Field(
        default=None,
        description="Required when action='scale'. Fabric SKU like 'F2', 'F4', 'F8', 'F16', 'F32', 'F64', 'F128', 'F256'.",
    )

    subscription_id: str = Field(description="Azure subscription ID")
    resource_group_name: str = Field(description="Resource group containing the capacity")
    capacity_name: str = Field(description="Fabric capacity name")

    tenant_id_env_var: Optional[str] = Field(default=None)
    client_id_env_var: Optional[str] = Field(default=None)
    client_secret_env_var: Optional[str] = Field(default=None)

    schedule: Optional[str] = Field(
        default=None,
        description="Cron schedule (e.g. '0 19 * * *' for 7pm daily, '0 6 * * 1-5' for 6am weekdays)",
    )
    schedule_default_status: str = Field(default="STOPPED")
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

            token = cred.get_token("https://management.azure.com/.default").token
            base = (
                f"https://management.azure.com/subscriptions/{self.subscription_id}"
                f"/resourceGroups/{self.resource_group_name}"
                f"/providers/Microsoft.Fabric/capacities/{self.capacity_name}"
            )
            api_version = "?api-version=2023-11-01"
            headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

            if self.action == "suspend":
                resp = requests.post(f"{base}/suspend{api_version}", headers=headers, timeout=60)
            elif self.action == "resume":
                resp = requests.post(f"{base}/resume{api_version}", headers=headers, timeout=60)
            elif self.action == "scale":
                if not self.target_sku:
                    raise ValueError("action='scale' requires target_sku, e.g. 'F4'")
                resp = requests.patch(
                    f"{base}{api_version}",
                    json={"sku": {"name": self.target_sku, "tier": "Fabric"}},
                    headers=headers,
                    timeout=60,
                )
            else:
                raise ValueError(f"Invalid action '{self.action}'. Use 'suspend', 'resume', or 'scale'.")

            if resp.status_code >= 300:
                raise Exception(
                    f"Fabric capacity {self.action} failed: {resp.status_code} {resp.text[:300]}"
                )

            op_url = resp.headers.get("azure-asyncoperation") or resp.headers.get("Location")
            context.log.info(
                f"Fabric capacity '{self.capacity_name}': {self.action} accepted "
                f"(status={resp.status_code}). Async URL: {op_url or '(none)'}"
            )

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
