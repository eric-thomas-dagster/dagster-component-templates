"""Microsoft Fabric workspace resource.

Exposes a thin REST API client + token caching for ad-hoc Fabric API
calls from custom ops. Use cases:
  - Custom ops that read/write workspace items
  - Discovering item IDs at runtime
  - Triggering jobs from a generic op (when the dedicated trigger job
    component is too narrow)

For most asset workloads, use the dedicated components instead:
  - fabric_workspace      — discover items as Dagster assets
  - dataframe_to_fabric_lakehouse — write DataFrames to OneLake
  - fabric_pipeline_trigger_job — schedule one specific item run
"""

import os
import time
from typing import Optional

import dagster as dg
from pydantic import Field


_FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"
_FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"


def _infer_fabric_parameter_type(value) -> str:
    """Map a plain Python value to Fabric's documented ItemJobParameterType
    (Integer/Number/Text/Boolean) for the top-level Parameter[] list, used
    by DataPipeline/Dataflow -- NOT Notebook, see _to_notebook_execution_data."""
    if isinstance(value, bool):
        return "Boolean"
    if isinstance(value, int):
        return "Integer"
    if isinstance(value, float):
        return "Number"
    return "Text"


def _to_fabric_parameters(params: Optional[dict]):
    """Convert a plain {name: value} dict into Fabric's documented top-level
    `parameters` list shape ({name, value, type}) -- for DataPipeline/
    Dataflow. Verified against learn.microsoft.com/rest/api/fabric/core/
    job-scheduler/run-on-demand-item-job."""
    if not params:
        return None
    return [
        {"name": str(k), "value": v, "type": _infer_fabric_parameter_type(v)}
        for k, v in params.items()
    ]


def _infer_notebook_parameter_type(value) -> str:
    """Fabric Notebook executionData parameter type: 'string'/'int'/'float'/
    'bool' -- a different, lowercase vocabulary from the top-level
    Parameter[] list's ItemJobParameterType."""
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "float"
    return "string"


def _to_notebook_execution_data(params: Optional[dict]):
    """Build the executionData shape a Fabric Notebook's RunNotebook job
    actually reads parameters from: {"parameters": {<name>: {"value": ...,
    "type": ...}}} -- a dict keyed by name with its own lowercase type
    vocabulary, confirmed via Microsoft's own
    apache-airflow-providers-microsoft-fabric (MSFabricNotebookJobParameters)
    and the official `fab` CLI's job-run docs. This resource previously sent
    `{"executionData": {"parameters": <raw dict>}}` for EVERY item type --
    wrong for DataPipeline/Dataflow (should be the top-level list above) and
    wrong even for Notebook (needed the typed {value, type} shape, not a
    bare value)."""
    if not params:
        return None
    return {
        "parameters": {
            str(k): {"value": v, "type": _infer_notebook_parameter_type(v)}
            for k, v in params.items()
        }
    }


class FabricWorkspaceResource(dg.ConfigurableResource):
    """Reusable Fabric REST API client. Cached token, simple GET/POST helpers."""

    workspace_id: str = Field(description="Fabric workspace ID (GUID)")
    tenant_id_env_var: Optional[str] = Field(default=None)
    client_id_env_var: Optional[str] = Field(default=None)
    client_secret_env_var: Optional[str] = Field(default=None)

    def _credential(self):
        from azure.identity import DefaultAzureCredential, ClientSecretCredential
        tenant = os.environ.get(self.tenant_id_env_var) if self.tenant_id_env_var else None
        client = os.environ.get(self.client_id_env_var) if self.client_id_env_var else None
        secret = os.environ.get(self.client_secret_env_var) if self.client_secret_env_var else None
        if tenant and client and secret:
            return ClientSecretCredential(tenant_id=tenant, client_id=client, client_secret=secret)
        return DefaultAzureCredential()

    def _get_token(self) -> str:
        cred = self._credential()
        return cred.get_token(_FABRIC_SCOPE).token

    @property
    def base_url(self) -> str:
        return f"{_FABRIC_API_BASE}/workspaces/{self.workspace_id}"

    def get(self, path: str) -> dict:
        """GET <workspace>/<path> and return JSON."""
        import requests
        url = f"{self.base_url}/{path.lstrip('/')}"
        resp = requests.get(url, headers={"Authorization": f"Bearer {self._get_token()}"}, timeout=60)
        resp.raise_for_status()
        return resp.json()

    def post(self, path: str, body: Optional[dict] = None) -> "requests.Response":
        """POST <workspace>/<path> with optional JSON body."""
        import requests
        url = f"{self.base_url}/{path.lstrip('/')}"
        resp = requests.post(
            url,
            json=body,
            headers={
                "Authorization": f"Bearer {self._get_token()}",
                "Content-Type": "application/json",
            },
            timeout=60,
        )
        resp.raise_for_status()
        return resp

    def list_items(self, item_type: Optional[str] = None) -> list:
        """List items in the workspace, optionally filtered by type."""
        items = []
        path = "items"
        if item_type:
            path = f"items?type={item_type}"
        page = self.get(path)
        items.extend(page.get("value", []))
        next_link = page.get("continuationUri")
        while next_link:
            import requests
            resp = requests.get(next_link, headers={"Authorization": f"Bearer {self._get_token()}"}, timeout=60)
            resp.raise_for_status()
            page = resp.json()
            items.extend(page.get("value", []))
            next_link = page.get("continuationUri")
        return items

    def trigger_item_run(self, item_id: str, item_type: str, parameters: Optional[dict] = None) -> str:
        """Trigger a Fabric item run. Returns the job instance URL for polling.

        `parameters` (a plain {name: value} dict) is sent in a shape that
        depends on `item_type` -- see `_to_fabric_parameters`/
        `_to_notebook_execution_data` module functions for why these differ.
        """
        job_type_map = {
            "DataPipeline": "Pipeline",
            "Notebook": "RunNotebook",
            "Dataflow": "Refresh",
        }
        job_type = job_type_map.get(item_type, item_type)
        if item_type == "Notebook":
            execution_data = _to_notebook_execution_data(parameters)
            body = {"executionData": execution_data} if execution_data else None
        else:
            fabric_params = _to_fabric_parameters(parameters)
            body = {"parameters": fabric_params} if fabric_params else None
        resp = self.post(f"items/{item_id}/jobs/instances?jobType={job_type}", body)
        return resp.headers.get("Location", "")

    def wait_for_run(self, instance_url: str, max_wait_seconds: int = 1800, poll_interval: int = 15, log=None) -> dict:
        """Poll a job instance URL until it completes/fails/times out.

        Returns the raw Fabric state dict (including its `status` field) in
        every case -- this is a thin client for custom ops, so the CALLER
        decides what a Failed/Cancelled/Deduped/Timeout status means (unlike
        `fabric_workspace`/`fabric_pipeline_trigger_job`, which raise
        dg.Failure/dg.RetryRequested themselves since they own the
        materialization). Always check `state["status"]` -- a dict being
        returned does not by itself mean the run succeeded.

        `Deduped` (another instance of the same job type was already
        running; Fabric skipped this one) is treated as terminal so it
        returns immediately instead of polling until `max_wait_seconds`.
        """
        import requests
        elapsed = 0
        while elapsed < max_wait_seconds:
            resp = requests.get(
                instance_url,
                headers={"Authorization": f"Bearer {self._get_token()}"},
                timeout=60,
            )
            resp.raise_for_status()
            state = resp.json()
            status = state.get("status", "Unknown")
            if log:
                log.info(f"Fabric job: status={status} elapsed={elapsed}s")
            if status in {"Completed", "Failed", "Cancelled", "Deduped"}:
                return state
            time.sleep(poll_interval)
            elapsed += poll_interval
        return {"status": "Timeout", "instance_url": instance_url}


class FabricWorkspaceResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a FabricWorkspaceResource for use by other components / ops."""

    resource_key: str = Field(default="fabric", description="Dagster resource key")
    workspace_id: str = Field(description="Fabric workspace ID (GUID)")
    tenant_id_env_var: Optional[str] = Field(default=None)
    client_id_env_var: Optional[str] = Field(default=None)
    client_secret_env_var: Optional[str] = Field(default=None)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = FabricWorkspaceResource(
            workspace_id=self.workspace_id,
            tenant_id_env_var=self.tenant_id_env_var,
            client_id_env_var=self.client_id_env_var,
            client_secret_env_var=self.client_secret_env_var,
        )
        return dg.Definitions(resources={self.resource_key: resource})
