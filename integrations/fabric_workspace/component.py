"""Microsoft Fabric Workspace integration component.

Imports a Fabric workspace's items as Dagster external assets. Uses the
Fabric REST API (`https://api.fabric.microsoft.com/v1`) and Microsoft
Entra ID OAuth via a service principal (client_id / client_secret /
tenant_id) or ``DefaultAzureCredential``.

Items imported:
  - Lakehouse       (observable_source_asset)
  - Warehouse       (observable_source_asset)
  - Notebook        (materializable @asset that triggers a notebook job)
  - DataPipeline    (materializable @asset that triggers the pipeline)
  - Dataflow Gen2   (materializable @asset that refreshes the dataflow)
  - SemanticModel   (observable_source_asset)
  - Report          (observable_source_asset)

Aligns with the canonical `workspace:` convention shared by
``SnowflakeWorkspaceComponent`` / ``MLflowWorkspaceComponent`` /
``dagster_databricks.DatabricksWorkspaceComponent`` /
``dagster_powerbi.PowerBIWorkspaceComponent``:

- ``@public`` class
- ``translation:`` callable field
- ``@public get_asset_spec(props)`` hook
- ``polling_sensor`` (alias ``generate_sensor``) opt-in
- ``defs_state`` + ``defs_state_config`` property
- ``StateBackedComponent`` inheritance with ``write_state_to_path`` +
  ``build_defs_from_state`` -- Fabric REST enumeration lives entirely in
  the state-write path so no HTTP fires at Dagster load time
- ``FabricObjectProps`` @record + ``DagsterFabricTranslator`` +
  ``FabricComponentTranslator``

Fabric shares the Microsoft Entra auth stack with Power BI --
``FabricResource`` accepts the same service-principal triple
(``client_id`` / ``client_secret`` / ``tenant_id``) as
``dagster_powerbi.PowerBIServicePrincipal``. A future PR could collapse
these onto a shared ``EntraServicePrincipal`` resource; today we keep
them separate so ``FabricResource`` can be installed without pulling in
the Power BI SDK.

Reference: https://learn.microsoft.com/rest/api/fabric/
"""

import hashlib
import json
import time
from pathlib import Path
from typing import Annotated, Any, Dict, List, Optional

import dagster as dg
from dagster import (
    AssetKey,
    AssetSpec,
    ComponentLoadContext,
    Definitions,
    Model,
    Resolvable,
    Resolver,
)
from dagster._annotations import public
from dagster.components.component.state_backed_component import StateBackedComponent
from dagster.components.resolved.base import resolve_fields
from dagster.components.utils.defs_state import (
    DefsStateConfig,
    DefsStateConfigArgs,
    ResolvedDefsStateConfig,
)
from dagster.components.utils.translation import (
    ComponentTranslator,
    TranslationFn,
    TranslationFnResolver,
    create_component_translator_cls,
)
from dagster_shared.record import record
from pydantic import Field


_FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"
_FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"

# Fabric item types we know how to import. Lakehouse/Warehouse/SemanticModel/
# Report become observable source assets; Notebook/DataPipeline/Dataflow
# become materializable trigger assets.
_RUNNABLE_ITEM_TYPES = {"Notebook", "DataPipeline", "Dataflow"}
_DATASET_ITEM_TYPES = {"Lakehouse", "Warehouse", "SemanticModel", "Report"}


@record
class FabricObjectProps:
    """Data passed to translation callables for each imported Fabric object.

    Mirrors the shape of ``SnowflakeObjectProps`` / ``MLflowObjectProps`` --
    a single record describing the object plus its parent workspace so
    ``translation:`` callables can filter, rename, add tags, etc.

    Attributes:
        object_kind: One of 'lakehouse' / 'warehouse' / 'notebook' /
            'data_pipeline' / 'dataflow' / 'semantic_model' / 'report'.
        object_name: The Fabric item's display name.
        workspace_id: The Fabric workspace GUID the item lives in.
        extra: Kind-specific metadata (item_id, item type as returned by
            the Fabric REST API, description, workspace metadata, etc.).
    """
    object_kind: str
    object_name: str
    workspace_id: str
    extra: Optional[Dict[str, Any]] = None


class FabricResource(dg.ConfigurableResource):
    """Microsoft Fabric workspace connection.

    Shape mirrors ``dagster_powerbi.PowerBIServicePrincipal`` -- same Entra
    ID auth surface (client_id / client_secret / tenant_id), same
    ``login.microsoftonline.com`` token exchange. If any of the three
    service-principal fields are unset, falls back to
    ``DefaultAzureCredential`` (env vars, Azure CLI, managed identity,
    etc.) so local dev "just works" via `az login`.

    Named ``FabricResource`` (not ``FabricWorkspace``) so it reads
    naturally alongside the community ``mlflow_resource`` /
    ``snowflake_resource`` components and doesn't shadow the
    ``workspace:`` field on the component itself.
    """

    workspace_id: str = Field(
        description="Fabric workspace GUID (from Fabric portal, workspace settings).",
    )
    tenant_id: Optional[str] = Field(
        default=None,
        description="Entra tenant ID for service-principal auth. When set with client_id + client_secret, uses ClientSecretCredential. When any of the three is missing, falls back to DefaultAzureCredential.",
    )
    client_id: Optional[str] = Field(
        default=None,
        description="Entra application (service principal) client ID.",
    )
    client_secret: Optional[str] = Field(
        default=None,
        description="Entra application (service principal) client secret.",
    )
    max_wait_seconds: int = Field(
        default=1800,
        description="Max wait when polling a triggered notebook/pipeline/dataflow job for completion.",
    )
    poll_interval_seconds: int = Field(
        default=15,
        description="Poll interval when waiting on a triggered job.",
    )

    def _credential(self):
        try:
            from azure.identity import ClientSecretCredential, DefaultAzureCredential
        except ImportError as e:
            raise ImportError(
                "azure-identity is required for FabricResource -- install with `pip install azure-identity`."
            ) from e
        if self.tenant_id and self.client_id and self.client_secret:
            return ClientSecretCredential(
                tenant_id=self.tenant_id,
                client_id=self.client_id,
                client_secret=self.client_secret,
            )
        return DefaultAzureCredential()

    def get_token(self) -> str:
        return self._credential().get_token(_FABRIC_SCOPE).token


# Backward-compat alias -- the resource may also be referenced as
# ``FabricWorkspace`` (matches ``PowerBIWorkspace`` naming).
FabricWorkspace = FabricResource


@public
class FabricWorkspaceComponent(StateBackedComponent, Model, Resolvable):
    """Import a Microsoft Fabric workspace's items as Dagster assets.

    Discovery runs once at state-write time and caches the workspace
    contents to disk. On subsequent Dagster loads no Fabric REST calls
    fire until an asset is materialized. Runtime triggering
    (notebook/pipeline/dataflow jobs) still happens inside asset compute
    functions.

    Example (canonical `workspace:` block, mirrors dagster-powerbi):
        ```yaml
        type: dagster_component_templates.FabricWorkspaceComponent
        attributes:
          workspace:
            workspace_id: "{{ env.FABRIC_WORKSPACE_ID }}"
            tenant_id:     "{{ env.AZURE_TENANT_ID }}"
            client_id:     "{{ env.AZURE_CLIENT_ID }}"
            client_secret: "{{ env.AZURE_CLIENT_SECRET }}"
          import_lakehouses: true
          import_warehouses: true
          import_notebooks:  true
          import_pipelines:  true
        ```
    """

    workspace: Annotated[
        FabricResource,
        Resolver(
            lambda context, model: FabricResource(
                **resolve_fields(model, FabricResource, context)  # ty: ignore[invalid-argument-type]
            ),
        ),
    ] = Field(
        description=(
            "Fabric connection as a FabricResource (workspace_id + optional "
            "tenant_id/client_id/client_secret for service-principal auth). "
            "Secrets typically arrive via `{{ env.XXX }}` Jinja templating "
            "in defs.yaml. Falls back to DefaultAzureCredential when the "
            "service-principal triple is unset."
        ),
    )

    translation: Annotated[
        Optional[TranslationFn[FabricObjectProps]],
        TranslationFnResolver(template_vars_for_translation_fn=lambda data: {"props": data}),
    ] = Field(
        default=None,
        description=(
            "Function used to translate Fabric object properties into "
            "Dagster asset specs. Called for each imported lakehouse / "
            "warehouse / notebook / pipeline / dataflow / semantic model / "
            "report. If unset, the base translator's default AssetSpec is used."
        ),
    )

    import_lakehouses: bool = Field(default=True)
    import_warehouses: bool = Field(default=True)
    import_notebooks: bool = Field(
        default=False,
        description="Notebooks become materializable @assets -- materializing them runs the notebook job.",
    )
    import_pipelines: bool = Field(
        default=True,
        description="Data Pipelines become materializable @assets.",
    )
    import_dataflows: bool = Field(
        default=False,
        description="Dataflow Gen2 become materializable @assets.",
    )
    import_semantic_models: bool = Field(default=False)
    import_reports: bool = Field(default=False)

    filter_by_name_pattern: Optional[str] = Field(
        default=None,
        description="Regex applied to Fabric item display names for inclusion.",
    )
    exclude_name_pattern: Optional[str] = Field(
        default=None,
        description="Regex applied to Fabric item display names for exclusion.",
    )

    group_name: str = Field(default="fabric")
    upstream_asset_keys: Optional[List[str]] = Field(
        default=None,
        description="Asset keys that all imported assets wait for (lineage-only).",
    )

    polling_sensor: bool = Field(
        default=False,
        description=(
            "If true, adds a polling sensor that detects new Fabric item job "
            "completions and emits AssetObservation events. Matches the "
            "`polling_sensor` convention on FivetranAccountComponent and "
            "SnowflakeWorkspaceComponent. Off by default -- opt in explicitly."
        ),
        alias="generate_sensor",
    )

    defs_state: ResolvedDefsStateConfig = Field(
        default_factory=DefsStateConfigArgs.local_filesystem,
        description=(
            "State backend for cached workspace discovery. Local filesystem by "
            "default. Overridden per-deploy for prod runs against Dagster Cloud."
        ),
    )

    @public
    def get_asset_spec(self, props: FabricObjectProps) -> AssetSpec:
        """Generates an AssetSpec for a given Fabric object.

        This method can be overridden in a subclass to customize how Fabric
        objects are converted to Dagster asset specs. By default, it delegates
        to the configured translator (which respects the ``translation:`` field).

        Args:
            props: The FabricObjectProps carrying object kind, name, workspace
                ID, and any kind-specific metadata.

        Returns:
            An AssetSpec that represents the Fabric object as a Dagster asset.

        Example:
            Override this method to add custom tags based on the object kind:

            .. code-block:: python

                from dagster_community_components import FabricWorkspaceComponent

                class CustomFabricWorkspaceComponent(FabricWorkspaceComponent):
                    def get_asset_spec(self, props):
                        base_spec = super().get_asset_spec(props)
                        return base_spec.replace_attributes(
                            tags={
                                **base_spec.tags,
                                "fabric_object_kind": props.object_kind,
                            }
                        )
        """
        return self._base_translator.get_asset_spec(props)

    @property
    def _base_translator(self) -> "FabricComponentTranslator":
        cached = getattr(self, "__base_translator_cached", None)
        if cached is None:
            cached = FabricComponentTranslator(self)
            object.__setattr__(self, "__base_translator_cached", cached)
        return cached

    @property
    def defs_state_config(self) -> DefsStateConfig:
        wsid_hash = hashlib.sha256(self.workspace.workspace_id.encode()).hexdigest()[:12]
        default_key = f"{self.__class__.__name__}[{wsid_hash}]"
        return DefsStateConfig.from_args(self.defs_state, default_key=default_key)

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

    def _apply_translation(
        self,
        kwargs: Dict[str, Any],
        kind: str,
        name: str,
        extra: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Fold the translation callable into per-asset kwargs.

        Builds a ``FabricObjectProps`` and calls ``self.get_asset_spec(props)``,
        which delegates to ``FabricComponentTranslator`` (base spec + optional
        user ``translation:`` callable).

        Backward-compat: when no ``translation:`` callable is set, the base
        translator returns the default AssetSpec and this method is a no-op --
        all pre-existing per-asset kwargs (name, key, group_name, metadata,
        tags, kinds, deps) win. When a ``translation:`` callable IS set, its
        AssetSpec's key / tags / metadata / kinds / owners flow into the
        kwargs (translation-provided values win over inferred ones).
        """
        if self.translation is None:
            return kwargs

        props = FabricObjectProps(
            object_kind=kind,
            object_name=name,
            workspace_id=self.workspace.workspace_id,
            extra=extra,
        )
        base_spec = self.get_asset_spec(props)
        merged = dict(kwargs)
        merged.pop("name", None)
        merged["key"] = base_spec.key
        if base_spec.metadata:
            existing_meta = dict(merged.get("metadata") or {})
            existing_meta.update(base_spec.metadata)
            merged["metadata"] = existing_meta
        if base_spec.tags:
            existing_tags = dict(merged.get("tags") or {})
            existing_tags.update(base_spec.tags)
            merged["tags"] = existing_tags
        if base_spec.kinds:
            existing_kinds = set(merged.get("kinds") or set())
            existing_kinds.update(base_spec.kinds)
            merged["kinds"] = existing_kinds
        if base_spec.owners:
            merged["owners"] = list(base_spec.owners)
        if base_spec.group_name and "group_name" not in kwargs:
            merged["group_name"] = base_spec.group_name
        return merged

    def _list_items(self) -> List[dict]:
        """Enumerate all items in the Fabric workspace via the REST API."""
        import requests
        token = self.workspace.get_token()
        headers = {"Authorization": f"Bearer {token}"}
        url = f"{_FABRIC_API_BASE}/workspaces/{self.workspace.workspace_id}/items"

        items: List[dict] = []
        next_link: Optional[str] = url
        while next_link:
            resp = requests.get(next_link, headers=headers, timeout=60)
            resp.raise_for_status()
            data = resp.json()
            items.extend(data.get("value", []))
            next_link = data.get("continuationUri") or data.get("@odata.nextLink")
        return items

    async def write_state_to_path(self, state_path: Path) -> None:
        """Enumerate Fabric workspace items and cache them.

        Runs the Fabric REST discovery (`GET /workspaces/{ws}/items`),
        applies the ``filter_by_name_pattern`` / ``exclude_name_pattern``
        regexes, and writes the surviving rows to ``state_path`` as a JSON
        dict bucketed by item type. ``build_defs_from_state`` re-hydrates
        from this snapshot so no Fabric HTTP calls fire at Dagster load
        time.
        """
        items = self._list_items()
        buckets: Dict[str, List[dict]] = {
            "lakehouses": [],
            "warehouses": [],
            "notebooks": [],
            "pipelines": [],
            "dataflows": [],
            "semantic_models": [],
            "reports": [],
        }
        for item in items:
            item_type = item.get("type", "")
            name = item.get("displayName") or item.get("name") or "unnamed"
            if not self._matches_filter(name):
                continue
            row = {
                "id": item.get("id"),
                "displayName": name,
                "type": item_type,
                "description": item.get("description"),
                "workspaceId": item.get("workspaceId") or self.workspace.workspace_id,
            }
            if item_type == "Lakehouse":
                buckets["lakehouses"].append(row)
            elif item_type == "Warehouse":
                buckets["warehouses"].append(row)
            elif item_type == "Notebook":
                buckets["notebooks"].append(row)
            elif item_type == "DataPipeline":
                buckets["pipelines"].append(row)
            elif item_type == "Dataflow":
                buckets["dataflows"].append(row)
            elif item_type == "SemanticModel":
                buckets["semantic_models"].append(row)
            elif item_type == "Report":
                buckets["reports"].append(row)

        state = {
            "workspace_id": self.workspace.workspace_id,
            **buckets,
        }
        state_path.write_text(json.dumps(state, indent=2, default=str))

    def build_defs_from_state(
        self,
        context: ComponentLoadContext,
        state_path: Optional[Path],
    ) -> Definitions:
        """Build Dagster definitions from cached Fabric workspace state.

        Reads the JSON dict written by ``write_state_to_path`` and turns
        each item row into an ``observable_source_asset`` (lakehouse /
        warehouse / semantic model / report) or a materializable ``@asset``
        (notebook / pipeline / dataflow). Runtime Fabric REST calls
        (job triggers, polling) still fire on each materialization -- only
        the discovery moved to state.
        """
        if state_path is None or not state_path.exists():
            return Definitions()
        state: Dict[str, Any] = json.loads(state_path.read_text())

        upstream_keys = [dg.AssetKey.from_user_string(k) for k in (self.upstream_asset_keys or [])]
        assets = []

        if self.import_lakehouses:
            for row in state.get("lakehouses", []):
                assets.append(self._build_dataset_asset(row, "Lakehouse", "lakehouse"))
        if self.import_warehouses:
            for row in state.get("warehouses", []):
                assets.append(self._build_dataset_asset(row, "Warehouse", "warehouse"))
        if self.import_semantic_models:
            for row in state.get("semantic_models", []):
                assets.append(self._build_dataset_asset(row, "SemanticModel", "semantic_model"))
        if self.import_reports:
            for row in state.get("reports", []):
                assets.append(self._build_dataset_asset(row, "Report", "report"))

        if self.import_notebooks:
            for row in state.get("notebooks", []):
                assets.append(
                    self._build_runnable_asset(row, "Notebook", "notebook", upstream_keys)
                )
        if self.import_pipelines:
            for row in state.get("pipelines", []):
                assets.append(
                    self._build_runnable_asset(row, "DataPipeline", "data_pipeline", upstream_keys)
                )
        if self.import_dataflows:
            for row in state.get("dataflows", []):
                assets.append(
                    self._build_runnable_asset(row, "Dataflow", "dataflow", upstream_keys)
                )

        return Definitions(assets=assets)

    def _build_dataset_asset(self, row: dict, item_type: str, kind: str):
        """Build an observable_source_asset for a Lakehouse / Warehouse /
        SemanticModel / Report -- items with no direct run-on-demand
        semantics.
        """
        item_id = row.get("id")
        name = row.get("displayName") or "unnamed"
        default_name = f"fabric_{item_type.lower()}_{name}"

        base_kwargs: Dict[str, Any] = dict(
            name=default_name,
            group_name=self.group_name,
            metadata={
                "fabric_item_id": item_id,
                "fabric_item_type": item_type,
                "workspace_id": self.workspace.workspace_id,
            },
            description=f"Fabric {item_type}: {name}",
        )
        asset_kwargs = self._apply_translation(
            base_kwargs,
            kind=kind,
            name=name,
            extra={
                "item_id": item_id,
                "item_type": item_type,
                "description": row.get("description"),
                "workspace_id": row.get("workspaceId"),
            },
        )

        @dg.observable_source_asset(**asset_kwargs)
        def _ext():
            return dg.DataVersion(item_id or "unknown")

        return _ext

    def _build_runnable_asset(self, row: dict, item_type: str, kind: str, upstream_keys):
        """Build a materializable @asset that triggers a Fabric notebook /
        pipeline / dataflow job and polls it to completion.
        """
        item_id = row.get("id")
        name = row.get("displayName") or "unnamed"
        default_name = f"fabric_{item_type.lower()}_{name}"
        _self = self

        base_kwargs: Dict[str, Any] = dict(
            key=dg.AssetKey.from_user_string(default_name),
            group_name=self.group_name,
            deps=upstream_keys,
            metadata={
                "fabric_item_id": item_id,
                "fabric_item_type": item_type,
                "workspace_id": self.workspace.workspace_id,
            },
            description=f"Fabric {item_type} run: {name}",
        )
        asset_kwargs = self._apply_translation(
            base_kwargs,
            kind=kind,
            name=name,
            extra={
                "item_id": item_id,
                "item_type": item_type,
                "description": row.get("description"),
                "workspace_id": row.get("workspaceId"),
            },
        )

        @dg.asset(**asset_kwargs)
        def _runnable(context: dg.AssetExecutionContext):
            return _self._trigger_item_run(item_id, item_type, context.log)

        return _runnable

    def _trigger_item_run(self, item_id: str, item_type: str, asset_log) -> dict:
        """Trigger a Fabric on-demand item job and poll until done.

        POST /workspaces/{ws}/items/{id}/jobs/instances?jobType=<T>
        The response Location header carries the job instance URL we poll.
        """
        import requests
        token = self.workspace.get_token()
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

        job_type_map = {
            "DataPipeline": "Pipeline",
            "Notebook": "RunNotebook",
            "Dataflow": "Refresh",
        }
        job_type = job_type_map.get(item_type, "Pipeline")
        run_url = (
            f"{_FABRIC_API_BASE}/workspaces/{self.workspace.workspace_id}"
            f"/items/{item_id}/jobs/instances?jobType={job_type}"
        )
        resp = requests.post(run_url, headers=headers, timeout=60)
        if resp.status_code >= 300:
            raise Exception(f"Fabric run trigger failed: {resp.status_code} {resp.text[:300]}")

        instance_url = resp.headers.get("Location")
        if not instance_url:
            asset_log.warning("Fabric: no Location header on job trigger response; cannot poll")
            return {"status": "Triggered", "instance_url": None}

        asset_log.info(f"Fabric job triggered. Instance: {instance_url}")
        elapsed = 0
        while elapsed < self.workspace.max_wait_seconds:
            state_resp = requests.get(instance_url, headers=headers, timeout=60)
            state_resp.raise_for_status()
            state = state_resp.json()
            status = state.get("status", "Unknown")
            asset_log.info(f"Fabric job: status={status} elapsed={elapsed}s")
            if status in {"Completed", "Failed", "Cancelled"}:
                return state
            time.sleep(self.workspace.poll_interval_seconds)
            elapsed += self.workspace.poll_interval_seconds

        return {"status": "Timeout", "instance_url": instance_url}


class DagsterFabricTranslator:
    """Base translator for Fabric workspace objects to AssetSpec.

    Follows the shape of ``DagsterSnowflakeTranslator`` /
    ``DagsterMLflowTranslator`` / ``DagsterPowerBITranslator``. Subclass
    this and override ``get_asset_spec()`` to fully customize how Fabric
    objects become Dagster assets -- an alternative to the runtime
    ``translation:`` callable on the component.
    """

    def get_asset_spec(self, props: FabricObjectProps) -> AssetSpec:
        """Default AssetSpec for a Fabric object.

        Key = ["fabric", <object_kind>, <object_name>] (lowercased for
        consistency with the rest of the Dagster catalog). Kind carries
        both "fabric" and the specific item kind. Metadata carries the
        item kind + name + workspace ID.
        """
        return AssetSpec(
            key=AssetKey(["fabric", props.object_kind, props.object_name.lower()]),
            kinds={"fabric", props.object_kind},
            metadata={
                "fabric/object_kind": props.object_kind,
                "fabric/object_name": props.object_name,
                "fabric/workspace_id": props.workspace_id,
            },
        )


class FabricComponentTranslator(
    create_component_translator_cls(FabricWorkspaceComponent, DagsterFabricTranslator),  # ty: ignore[unsupported-base]
    ComponentTranslator[FabricWorkspaceComponent],
):
    """Bridges ``FabricWorkspaceComponent.translation`` (runtime callable)
    with the base ``DagsterFabricTranslator`` (class-level override).

    Mirrors ``SnowflakeComponentTranslator`` / ``MLflowComponentTranslator``
    / ``FivetranComponentTranslator`` / ``PowerBIComponentTranslator``.
    """

    def __init__(self, component: "FabricWorkspaceComponent"):
        self._component = component

    def get_asset_spec(self, props: FabricObjectProps) -> AssetSpec:
        base_asset_spec = super().get_asset_spec(props)
        if self.component.translation is None:
            return base_asset_spec
        return self.component.translation(base_asset_spec, props)
