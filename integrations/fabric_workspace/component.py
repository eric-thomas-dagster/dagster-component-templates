"""Microsoft Fabric Workspace integration component.

Imports a Fabric workspace's items as Dagster external assets. Mirrors
azure_synapse / azure_data_factory in shape but uses the Fabric REST API
(not the Synapse management/artifacts SDKs) since Fabric is a separate
product family with a different API surface.

Items imported:
  - Lakehouse  (DataSet external asset)
  - Warehouse  (DataSet external asset)
  - Notebook   (job-trigger asset; runs as a notebook job)
  - DataPipeline   (job-trigger asset; runs the pipeline)
  - Dataflow Gen2  (job-trigger asset; refreshes the dataflow)
  - SemanticModel  (DataSet external asset)
  - Report     (DataSet external asset)

Auth: Microsoft Entra ID OAuth via DefaultAzureCredential. The Fabric
REST API scopes are:
  https://api.fabric.microsoft.com/.default

Reference: https://learn.microsoft.com/rest/api/fabric/
"""

import os
import time
from typing import Dict, List, Optional

import dagster as dg
from pydantic import Field


_FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"
_FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"

# Fabric item types we know how to import. Lakehouse/warehouse become source-style
# external assets; notebook/datapipeline/dataflow become trigger assets.
_RUNNABLE_ITEM_TYPES = {"Notebook", "DataPipeline", "Dataflow"}
_DATASET_ITEM_TYPES = {"Lakehouse", "Warehouse", "SemanticModel", "Report"}


class FabricWorkspaceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Import a Fabric workspace's items as Dagster assets.

    Example:
        ```yaml
        type: dagster_component_templates.FabricWorkspaceComponent
        attributes:
          workspace_id: "12345678-1234-1234-1234-123456789012"
          tenant_id_env_var: AZURE_TENANT_ID
          client_id_env_var: AZURE_CLIENT_ID
          client_secret_env_var: AZURE_CLIENT_SECRET
          import_lakehouses: true
          import_warehouses: true
          import_notebooks: true
          import_pipelines: true
          import_dataflows: true
          group_name: fabric
        ```
    """

    workspace_id: str = Field(description="Fabric workspace ID (GUID)")

    tenant_id_env_var: Optional[str] = Field(default=None)
    client_id_env_var: Optional[str] = Field(default=None)
    client_secret_env_var: Optional[str] = Field(default=None)

    import_lakehouses: bool = Field(default=True)
    import_warehouses: bool = Field(default=True)
    import_notebooks: bool = Field(default=False, description="Notebooks become trigger assets — materializing them runs the notebook job.")
    import_pipelines: bool = Field(default=True, description="Data Pipelines become trigger assets.")
    import_dataflows: bool = Field(default=False, description="Dataflow Gen2 become trigger assets.")
    import_semantic_models: bool = Field(default=False)
    import_reports: bool = Field(default=False)

    filter_by_name_pattern: Optional[str] = Field(default=None, description="Regex to filter items by name")
    exclude_name_pattern: Optional[str] = Field(default=None)

    max_wait_seconds: int = Field(default=1800, description="Max wait when polling a triggered job for completion.")
    poll_interval_seconds: int = Field(default=15)

    group_name: str = Field(default="fabric")
    upstream_asset_keys: Optional[List[str]] = Field(default=None, description="Asset keys that all imported assets wait for (lineage-only).")

    def _credential(self):
        try:
            from azure.identity import DefaultAzureCredential, ClientSecretCredential
        except ImportError as e:
            raise ImportError("azure-identity required: pip install azure-identity") from e
        tenant = os.environ.get(self.tenant_id_env_var) if self.tenant_id_env_var else None
        client = os.environ.get(self.client_id_env_var) if self.client_id_env_var else None
        secret = os.environ.get(self.client_secret_env_var) if self.client_secret_env_var else None
        if tenant and client and secret:
            return ClientSecretCredential(tenant_id=tenant, client_id=client, client_secret=secret)
        return DefaultAzureCredential()

    def _list_items(self, log) -> List[dict]:
        import requests
        cred = self._credential()
        token = cred.get_token(_FABRIC_SCOPE).token
        headers = {"Authorization": f"Bearer {token}"}
        url = f"{_FABRIC_API_BASE}/workspaces/{self.workspace_id}/items"

        items = []
        next_link = url
        while next_link:
            resp = requests.get(next_link, headers=headers, timeout=60)
            resp.raise_for_status()
            data = resp.json()
            items.extend(data.get("value", []))
            next_link = data.get("continuationUri") or data.get("@odata.nextLink")
        log.info(f"Fabric: discovered {len(items)} items in workspace {self.workspace_id}")
        return items

    def _matches_filter(self, name: str) -> bool:
        if self.filter_by_name_pattern:
            import re
            if not re.search(self.filter_by_name_pattern, name):
                return False
        if self.exclude_name_pattern:
            import re
            if re.search(self.exclude_name_pattern, name):
                return False
        return True

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        component_self = self

        def _trigger_item_run(item_id: str, item_type: str, asset_log) -> dict:
            """Trigger a Fabric job for the given item and poll until done."""
            import requests
            cred = component_self._credential()
            token = cred.get_token(_FABRIC_SCOPE).token
            headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

            # Fabric "Run on demand item job" endpoint
            # POST /workspaces/{ws}/items/{id}/jobs/instances?jobType={Pipeline|Notebook|RefreshDataflow}
            job_type_map = {
                "DataPipeline": "Pipeline",
                "Notebook": "RunNotebook",
                "Dataflow": "Refresh",
            }
            job_type = job_type_map.get(item_type, "Pipeline")
            run_url = (
                f"{_FABRIC_API_BASE}/workspaces/{component_self.workspace_id}"
                f"/items/{item_id}/jobs/instances?jobType={job_type}"
            )
            resp = requests.post(run_url, headers=headers, timeout=60)
            if resp.status_code >= 300:
                raise Exception(f"Fabric run trigger failed: {resp.status_code} {resp.text[:300]}")

            # Job instance URL is in the Location header
            instance_url = resp.headers.get("Location")
            if not instance_url:
                asset_log.warning("Fabric: no Location header on job trigger response; cannot poll")
                return {"status": "Triggered", "instance_url": None}

            asset_log.info(f"Fabric job triggered. Instance: {instance_url}")
            elapsed = 0
            while elapsed < component_self.max_wait_seconds:
                state_resp = requests.get(instance_url, headers=headers, timeout=60)
                state_resp.raise_for_status()
                state = state_resp.json()
                status = state.get("status", "Unknown")
                asset_log.info(f"Fabric job: status={status} elapsed={elapsed}s")
                if status in {"Completed", "Failed", "Cancelled"}:
                    return state
                time.sleep(component_self.poll_interval_seconds)
                elapsed += component_self.poll_interval_seconds

            return {"status": "Timeout", "instance_url": instance_url}

        # Discovery happens at component load time
        items = self._list_items(context.log if hasattr(context, "log") else _NoopLog())

        upstream_keys = [dg.AssetKey.from_user_string(k) for k in (self.upstream_asset_keys or [])]

        assets = []

        # External-only assets (Lakehouse, Warehouse, SemanticModel, Report)
        for item in items:
            item_type = item.get("type", "")
            name = item.get("displayName") or item.get("name") or "unnamed"
            if not self._matches_filter(name):
                continue

            if item_type == "Lakehouse" and self.import_lakehouses:
                assets.append(self._make_external_asset(item, item_type, "lakehouse"))
            elif item_type == "Warehouse" and self.import_warehouses:
                assets.append(self._make_external_asset(item, item_type, "warehouse"))
            elif item_type == "SemanticModel" and self.import_semantic_models:
                assets.append(self._make_external_asset(item, item_type, "semantic_model"))
            elif item_type == "Report" and self.import_reports:
                assets.append(self._make_external_asset(item, item_type, "report"))
            elif item_type == "Notebook" and self.import_notebooks:
                assets.append(self._make_runnable_asset(item, item_type, _trigger_item_run, upstream_keys))
            elif item_type == "DataPipeline" and self.import_pipelines:
                assets.append(self._make_runnable_asset(item, item_type, _trigger_item_run, upstream_keys))
            elif item_type == "Dataflow" and self.import_dataflows:
                assets.append(self._make_runnable_asset(item, item_type, _trigger_item_run, upstream_keys))

        return dg.Definitions(assets=assets)

    def _make_external_asset(self, item: dict, item_type: str, kind: str):
        item_id = item.get("id")
        name = item.get("displayName") or item.get("name") or "unnamed"
        asset_key = f"fabric_{item_type.lower()}_{name}"
        gn = self.group_name

        @dg.observable_source_asset(
            name=asset_key,
            group_name=gn,
            metadata={
                "fabric_item_id": item_id,
                "fabric_item_type": item_type,
                "workspace_id": self.workspace_id,
            },
            description=f"Fabric {item_type}: {name}",
        )
        def _ext():
            return dg.DataVersion(item_id or "unknown")

        return _ext

    def _make_runnable_asset(self, item: dict, item_type: str, trigger_fn, upstream_keys):
        item_id = item.get("id")
        name = item.get("displayName") or item.get("name") or "unnamed"
        asset_key = f"fabric_{item_type.lower()}_{name}"
        gn = self.group_name

        def _make(_item_id=item_id, _item_type=item_type, _name=name, _asset_key=asset_key):
            @dg.asset(
                name=_asset_key,
                group_name=gn,
                deps=upstream_keys,
                metadata={
                    "fabric_item_id": _item_id,
                    "fabric_item_type": _item_type,
                    "workspace_id": self.workspace_id,
                },
                description=f"Fabric {_item_type} run: {_name}",
            )
            def _runnable(context: dg.AssetExecutionContext):
                state = trigger_fn(_item_id, _item_type, context.log)
                return state
            return _runnable
        return _make()


class _NoopLog:
    def info(self, *a, **k): pass
    def warning(self, *a, **k): pass
    def error(self, *a, **k): pass
