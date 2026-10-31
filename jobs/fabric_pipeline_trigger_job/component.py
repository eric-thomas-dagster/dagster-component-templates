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

# Fabric's own Job Scheduler status enum (verified against the current REST
# API docs, learn.microsoft.com/rest/api/fabric/core/job-scheduler/
# get-item-job-instance): NotStarted, InProgress, Completed, Failed,
# Cancelled, Deduped. "Deduped" means another instance of the same job type
# was already running and this one was skipped by Fabric -- a real terminal
# outcome, not "still in progress," handled explicitly below.


def _infer_fabric_parameter_type(value):
    if isinstance(value, bool):
        return "Boolean"
    if isinstance(value, int):
        return "Integer"
    if isinstance(value, float):
        return "Number"
    return "Text"


def _to_fabric_parameters(params):
    """Convert a plain {name: value} dict into Fabric's documented Job
    Scheduler request shape: a top-level `parameters` list of
    {name, value, type} objects -- verified against the current API docs
    (learn.microsoft.com/rest/api/fabric/core/job-scheduler/run-on-demand-item-job).
    This was previously sent as `executionData.parameters` with the raw
    dict value, which doesn't match the documented request shape on either
    count: `parameters` is a top-level field, not nested under
    `executionData` (that's for fixed, item-type-specific static config,
    a different thing), and it must be a typed Parameter[] list, not a bare
    dict.

    CORRECTION: this top-level shape is for DataPipeline/Dataflow, NOT
    Notebook -- see `_to_notebook_execution_data` below. An earlier version
    of this docstring claimed this shape was used "across item types";
    that was wrong, confirmed via Microsoft's own
    apache-airflow-providers-microsoft-fabric (`MSFabricNotebookJobParameters`)
    and the official `fab` CLI's job-run docs, both of which build a
    completely different shape for Notebook specifically.
    """
    if not params:
        return None
    return [
        {"name": str(k), "value": v, "type": _infer_fabric_parameter_type(v)}
        for k, v in params.items()
    ]


def _infer_notebook_parameter_type(value):
    """Fabric Notebook executionData parameter type: 'string'/'int'/'float'/
    'bool' -- a different, lowercase vocabulary from _infer_fabric_parameter_type's
    ItemJobParameterType (Text/Boolean/Number/Integer)."""
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "float"
    return "string"


def _to_notebook_execution_data(params):
    """Build the executionData shape a Fabric Notebook's RunNotebook job
    actually reads parameters from: {"parameters": {<name>: {"value": ...,
    "type": ...}}} -- confirmed via Microsoft's own
    MSFabricNotebookJobParameters dataclass and the official `fab` CLI's
    job-run docs, NOT the generic top-level Parameter[] list."""
    if not params:
        return None
    return {
        "parameters": {
            str(k): {"value": v, "type": _infer_notebook_parameter_type(v)}
            for k, v in params.items()
        }
    }


def _extract_fabric_error(resp):
    """Best-effort parse of Fabric's documented ErrorResponse shape
    (errorCode / message / isRetriable) from a non-2xx response."""
    try:
        body = resp.json()
    except Exception:
        return {"errorCode": "", "message": resp.text[:300], "isRetriable": None}
    if isinstance(body, dict):
        return {
            "errorCode": body.get("errorCode") or "",
            "message": body.get("message") or resp.text[:300],
            "isRetriable": body.get("isRetriable"),
        }
    return {"errorCode": "", "message": resp.text[:300], "isRetriable": None}



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
        description=(
            "Optional {name: value} parameters to pass to the pipeline/notebook/dataflow. "
            "For DataPipeline/Dataflow, converted to Fabric's documented Parameter[] request "
            "shape ({name, value, type}, type inferred from the Python value). For Notebook, "
            "sent instead as executionData.parameters (a dict keyed by name, lowercase "
            "string/int/float/bool types) -- a Notebook reads its own executionData, not the "
            "generic Parameter[] list. Not all item/job types accept parameters -- Fabric's "
            "own API returns errorCode FeatureNotAvailable for ones that don't, surfaced as a "
            "clear Failure."
        ),
    )

    wait_for_completion: bool = Field(default=True)
    max_wait_seconds: int = Field(default=1800)
    poll_interval_seconds: int = Field(default=15)

    tenant_id_env_var: Optional[str] = Field(default=None)
    client_id_env_var: Optional[str] = Field(default=None)
    client_secret_env_var: Optional[str] = Field(default=None)

    schedule: Optional[str] = Field(default=None, description="Cron (e.g. '0 */6 * * *')")
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

            try:
                token = cred.get_token(_FABRIC_SCOPE).token
            except Exception as e:
                raise dg.Failure(description=f"Fabric authentication failed: {e}") from e

            job_type = _JOB_TYPE_MAP.get(self.item_type, self.item_type)
            run_url = (
                f"{_FABRIC_API_BASE}/workspaces/{self.workspace_id}"
                f"/items/{self.item_id}/jobs/instances?jobType={job_type}"
            )
            headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
            if self.item_type == "Notebook":
                execution_data = _to_notebook_execution_data(self.parameters)
                body = {"executionData": execution_data} if execution_data else None
            else:
                fabric_params = _to_fabric_parameters(self.parameters)
                body = {"parameters": fabric_params} if fabric_params else None
            resp = requests.post(run_url, json=body, headers=headers, timeout=60)
            if resp.status_code >= 300:
                error = _extract_fabric_error(resp)
                raise dg.Failure(
                    description=f"Fabric run trigger failed ({resp.status_code}): {error['message']}",
                    metadata={"fabric_error_code": error["errorCode"], "fabric_status_code": resp.status_code},
                )

            instance_url = resp.headers.get("Location")
            if not instance_url:
                context.log.warning("Fabric: no Location header on trigger response; cannot poll")
                return

            context.log.info(f"Fabric job triggered. Instance: {instance_url}")

            if not self.wait_for_completion:
                return

            elapsed = 0
            reached_fabric_terminal_state = False
            try:
                while elapsed < self.max_wait_seconds:
                    state_resp = requests.get(instance_url, headers=headers, timeout=60)
                    state_resp.raise_for_status()
                    state = state_resp.json()
                    status = state.get("status", "Unknown")
                    context.log.info(f"Fabric job status={status} elapsed={elapsed}s")

                    if status == "Completed":
                        reached_fabric_terminal_state = True
                        context.log.info("Fabric job completed.")
                        return

                    if status == "Deduped":
                        reached_fabric_terminal_state = True
                        raise dg.Failure(
                            description=(
                                f"Fabric job for item {self.item_id} was Deduped: another instance "
                                "of the same job type was already running, so Fabric skipped this "
                                "one. Not a failure of this run's own logic -- a scheduling collision."
                            ),
                        )

                    if status in ("Failed", "Cancelled"):
                        reached_fabric_terminal_state = True
                        failure = state.get("failureReason") or {}
                        error_code = failure.get("errorCode") or ""
                        message = failure.get("message") or f"Fabric job ended with status {status}"
                        is_retriable = failure.get("isRetriable")
                        md = {"fabric_status": status, "fabric_error_code": error_code}
                        failure_exc = dg.Failure(description=f"Fabric job {status.lower()}: {message}", metadata=md)
                        if is_retriable:
                            context.log.warning(f"Fabric job failed with a retriable error ({error_code}): {message}")
                            raise dg.RetryRequested(max_retries=1, seconds_to_wait=30) from failure_exc
                        raise failure_exc

                    time.sleep(self.poll_interval_seconds)
                    elapsed += self.poll_interval_seconds

                context.log.warning(f"Fabric job timed out after {elapsed}s.")
                raise dg.Failure(description=f"Fabric job for item {self.item_id} timed out after {elapsed}s.")
            finally:
                # Cancel on timeout or any other interrupting exception -- not when
                # Fabric's own job already reached a terminal state on its own,
                # where there's nothing left running to cancel.
                if not reached_fabric_terminal_state:
                    try:
                        cancel_resp = requests.post(f"{instance_url}/cancel", headers=headers, timeout=30)
                        if cancel_resp.status_code >= 300:
                            context.log.warning(f"Fabric: cancel request failed ({cancel_resp.status_code}): {cancel_resp.text[:200]}")
                        else:
                            context.log.info("Fabric: cancellation requested.")
                    except Exception as e:
                        context.log.warning(f"Fabric: cancel request raised: {e}")

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
