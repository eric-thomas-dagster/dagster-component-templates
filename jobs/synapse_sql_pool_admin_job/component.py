"""SynapseSqlPoolAdminJobComponent.

Pause / resume / scale a Synapse dedicated SQL pool via the management
REST API. Op-job (not an asset) because pause/resume produces no data —
it's an operational lifecycle action you want to schedule (e.g. pause
nightly, resume at 6am Mon-Fri to save DWU costs).

Typical use:
  - Schedule a pause at 19:00 weekdays + 23:00 weekends
  - Schedule a resume at 06:00 weekdays
  - Roughly halves dedicated SQL pool spend

The Synapse Management REST API:
  POST .../sqlPools/{name}/pause?api-version=2021-06-01
  POST .../sqlPools/{name}/resume?api-version=2021-06-01
  PATCH .../sqlPools/{name}?api-version=2021-06-01   (for scale via SKU change)
"""

import os
from typing import Optional

import dagster as dg
from pydantic import Field



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


class SynapseSqlPoolAdminJobComponent(dg.Component, dg.Model, dg.Resolvable):
    """Pause/resume/scale a Synapse dedicated SQL pool."""

    job_name: str = Field(description="Dagster job name (e.g. synapse_pool_pause_nightly)")
    action: str = Field(
        default="pause",
        description="'pause' | 'resume' | 'scale'. Scale requires `target_sku`.",
    )
    target_sku: Optional[str] = Field(
        default=None,
        description="Required when action='scale'. Synapse DWU SKU, e.g. 'DW100c', 'DW200c', 'DW500c'.",
    )

    subscription_id: str = Field(description="Azure subscription ID")
    resource_group_name: str = Field(description="Resource group containing the workspace")
    workspace_name: str = Field(description="Synapse workspace name")
    sql_pool_name: str = Field(description="Dedicated SQL pool name")

    tenant_id_env_var: Optional[str] = Field(default=None)
    client_id_env_var: Optional[str] = Field(default=None)
    client_secret_env_var: Optional[str] = Field(default=None)

    schedule: Optional[str] = Field(
        default=None,
        description="Cron schedule. Examples: '0 19 * * 1-5' (pause 7pm Mon-Fri), '0 6 * * 1-5' (resume 6am Mon-Fri).",
    )
    schedule_default_status: str = Field(default="STOPPED", description="STOPPED | RUNNING")
    tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None)

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=(), csv=(), tags=('tags',))

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
                f"/providers/Microsoft.Synapse/workspaces/{self.workspace_name}"
                f"/sqlPools/{self.sql_pool_name}"
            )
            api_version = "?api-version=2021-06-01"
            headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

            if self.action == "pause":
                resp = requests.post(f"{base}/pause{api_version}", headers=headers, timeout=60)
            elif self.action == "resume":
                resp = requests.post(f"{base}/resume{api_version}", headers=headers, timeout=60)
            elif self.action == "scale":
                if not self.target_sku:
                    raise ValueError("action='scale' requires target_sku, e.g. 'DW200c'")
                resp = requests.patch(
                    f"{base}{api_version}",
                    json={"sku": {"name": self.target_sku}},
                    headers=headers,
                    timeout=60,
                )
            else:
                raise ValueError(f"Invalid action '{self.action}'. Use 'pause', 'resume', or 'scale'.")

            if resp.status_code >= 300:
                raise Exception(
                    f"Synapse SQL pool {self.action} failed: {resp.status_code} {resp.text[:300]}"
                )

            # Pause/resume return 202 Accepted with an Azure-AsyncOperation header
            op_url = resp.headers.get("azure-asyncoperation") or resp.headers.get("Location")
            context.log.info(
                f"Synapse SQL pool '{self.sql_pool_name}': {self.action} accepted "
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
