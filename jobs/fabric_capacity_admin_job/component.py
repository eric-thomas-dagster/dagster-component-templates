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


def _wait_for_arm_operation(resp, headers, max_wait_seconds: int, poll_interval_seconds: int, log=None) -> None:
    """Poll an Azure Resource Manager long-running operation to completion.

    Standard ARM async-operation contract (learn.microsoft.com/azure/
    azure-resource-manager/management/async-operations): prefer the
    `Azure-AsyncOperation` header URL over `Location` when both are present;
    a GET on it returns JSON with a `status` field whose terminal values are
    `Succeeded`/`Failed`/`Canceled` (any other value, e.g. a resource-provider-
    specific one, means still in progress). Use the response's own
    `Retry-After` header for poll cadence when present. Confirmed this
    applies to Fabric capacity suspend/resume/scale specifically via
    learn.microsoft.com/rest/api/microsoftfabric/fabric-capacities/suspend's
    own documented 202 response (both headers present in its example).

    Without this, a 202 Accepted was treated as "done" -- for `suspend`
    specifically, that means Dagster reports success while the capacity may
    still be billing for a while longer, defeating the whole point of a
    pause-nightly schedule.
    """
    import time as _time
    import requests

    op_url = resp.headers.get("azure-asyncoperation") or resp.headers.get("Location")
    if not op_url:
        if log:
            log.info("No Azure-AsyncOperation/Location header on the response -- nothing to poll.")
        return

    elapsed = 0
    while elapsed < max_wait_seconds:
        poll_resp = requests.get(op_url, headers=headers, timeout=60)
        poll_resp.raise_for_status()
        try:
            body = poll_resp.json()
        except Exception:
            body = {}
        status = body.get("status") or ("Succeeded" if poll_resp.status_code in (200, 204) else None)
        if log:
            log.info(f"Fabric capacity operation: status={status!r} elapsed={elapsed}s")
        if status == "Succeeded" or (status is None and poll_resp.status_code in (200, 204)):
            return
        if status in ("Failed", "Canceled"):
            raise dg.Failure(
                description=f"Fabric capacity operation ended with status {status!r}: {body}",
                metadata={"azure_operation_status": status},
            )
        wait = int(poll_resp.headers.get("Retry-After") or poll_interval_seconds)
        _time.sleep(wait)
        elapsed += wait

    raise dg.Failure(
        description=f"Fabric capacity operation timed out after {elapsed}s waiting for completion.",
        metadata={"azure_async_operation_url": op_url},
    )


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

    wait_for_completion: bool = Field(
        default=True,
        description=(
            "Poll the Azure Resource Manager async operation (Azure-AsyncOperation "
            "header, falling back to Location) until it reports Succeeded/Failed/"
            "Canceled, instead of returning as soon as the request is accepted "
            "(202). Without this, 'suspend' completing successfully in Dagster "
            "doesn't mean the capacity has actually stopped billing yet -- the "
            "whole point of pausing nightly is to stop the hourly charge "
            "immediately, not some unknown time later."
        ),
    )
    max_wait_seconds: int = Field(default=600, description="Only used when wait_for_completion=True.")
    poll_interval_seconds: int = Field(
        default=15,
        description="Fallback poll interval when wait_for_completion=True and the response has no Retry-After header.",
    )

    schedule: Optional[str] = Field(
        default=None,
        description="Cron schedule (e.g. '0 19 * * *' for 7pm daily, '0 6 * * 1-5' for 6am weekdays)",
    )
    schedule_default_status: str = Field(default="STOPPED")
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
                raise dg.Failure(
                    description=f"Fabric capacity {self.action} failed: {resp.status_code} {resp.text[:300]}",
                    metadata={"azure_status_code": resp.status_code},
                )

            op_url = resp.headers.get("azure-asyncoperation") or resp.headers.get("Location")
            context.log.info(
                f"Fabric capacity '{self.capacity_name}': {self.action} accepted "
                f"(status={resp.status_code}). Async URL: {op_url or '(none)'}"
            )

            if self.wait_for_completion:
                _wait_for_arm_operation(
                    resp, headers, self.max_wait_seconds, self.poll_interval_seconds, log=context.log,
                )
                context.log.info(f"Fabric capacity '{self.capacity_name}': {self.action} completed.")

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
