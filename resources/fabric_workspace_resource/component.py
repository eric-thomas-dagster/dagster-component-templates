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
        """Trigger a Fabric item run. Returns the job instance URL for polling."""
        job_type_map = {
            "DataPipeline": "Pipeline",
            "Notebook": "RunNotebook",
            "Dataflow": "Refresh",
        }
        job_type = job_type_map.get(item_type, item_type)
        body = {"executionData": {"parameters": parameters}} if parameters else None
        resp = self.post(f"items/{item_id}/jobs/instances?jobType={job_type}", body)
        return resp.headers.get("Location", "")

    def wait_for_run(self, instance_url: str, max_wait_seconds: int = 1800, poll_interval: int = 15, log=None) -> dict:
        """Poll a job instance URL until it completes/fails/times out."""
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
            if status in {"Completed", "Failed", "Cancelled"}:
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
