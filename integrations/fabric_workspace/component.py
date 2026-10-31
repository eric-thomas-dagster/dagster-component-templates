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
from pydantic import ConfigDict, Field


_FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"
_FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"

# Fabric item types we know how to import. Lakehouse/Warehouse/SemanticModel/
# Report become observable source assets; Notebook/DataPipeline/Dataflow
# become materializable trigger assets.
_RUNNABLE_ITEM_TYPES = {"Notebook", "DataPipeline", "Dataflow"}
_DATASET_ITEM_TYPES = {"Lakehouse", "Warehouse", "SemanticModel", "Report"}

# Fabric's own Job Scheduler status enum (verified against the current REST
# API docs, learn.microsoft.com/rest/api/fabric/core/job-scheduler/
# get-item-job-instance): NotStarted, InProgress, Completed, Failed,
# Cancelled, Deduped. All but the first two are terminal. "Deduped" means
# another instance of the same job type was already running and this one
# was skipped -- it is NOT the same as a normal completion, and was
# previously treated as "still in progress" here, so a run that collided
# with an already-running instance just polled until it hit the timeout.


def _infer_fabric_parameter_type(value: Any) -> str:
    """Map a plain Python value to one of Fabric's documented
    ItemJobParameterType values (Integer / Number / Text / Boolean --
    DateTime/Guid/VariableReference/Automatic are also valid but not
    inferable from a bare Python value)."""
    if isinstance(value, bool):
        return "Boolean"
    if isinstance(value, int):
        return "Integer"
    if isinstance(value, float):
        return "Number"
    return "Text"


def _to_fabric_parameters(params: Optional[Dict[str, Any]]) -> Optional[List[Dict[str, Any]]]:
    """Convert a plain {name: value} dict into Fabric's documented Job
    Scheduler request shape: a list of {name, value, type} objects.

    Verified against the current Fabric REST API docs (learn.microsoft.com/
    rest/api/fabric/core/job-scheduler/run-on-demand-item-job): this `Parameter[]`
    shape is the SAME across item types (Notebook/DataPipeline/Dataflow) --
    there is no separate per-item-type dict shape. The API itself warns that
    `parameters` is "not broadly supported": an item/job type that doesn't
    support it returns errorCode `FeatureNotAvailable`, surfaced as a clear
    Failure by `_trigger_item_run` rather than a generic HTTP error.
    """
    if not params:
        return None
    return [
        {"name": str(k), "value": v, "type": _infer_fabric_parameter_type(v)}
        for k, v in params.items()
    ]


def _extract_fabric_error(resp) -> Dict[str, Any]:
    """Best-effort parse of Fabric's documented ErrorResponse shape
    (errorCode / message / isRetriable / moreDetails) from a non-2xx
    response. Falls back to the raw response text if the body isn't JSON
    or doesn't match the expected shape."""
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



# BEGIN upstream-asset-key normalization (inlined per component; keep copies identical — see tests/test_upstream_keys.py)
from typing import List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def normalize_upstream_keys(single, plural=None) -> List[str]:
    """Merge ``upstream_asset_key`` / ``upstream_asset_keys`` into one clean list.

    Each argument may be a str, a list/tuple of str, or None. Entries are
    stripped, empties dropped, and duplicates removed (order preserved).
    """
    out: List[str] = []
    for value in (single, plural):
        if value is None:
            continue
        items = [value] if isinstance(value, str) else value
        if not isinstance(items, (list, tuple)):
            raise ValueError(
                f"upstream asset key(s) must be a string or a list of strings, got {value!r}"
            )
        for item in items:
            if not isinstance(item, str):
                raise ValueError(f"upstream asset keys must be strings, got {item!r}")
            item = item.strip()
            if item and item not in out:
                out.append(item)
    return out


def coerce_upstream_input(data, *, component: str, mode: str, optional: bool = True):
    """Rewrite raw component input so both spellings/shapes land on the declared field(s).

    mode "single": declared field is ``upstream_asset_key`` (str); >1 key is an error.
    mode "multi":  declared field is ``upstream_asset_keys`` (list).
    mode "both":   both fields are declared; everything is merged into the plural one.
    """
    if not isinstance(data, dict):
        return data
    if "upstream_asset_key" not in data and "upstream_asset_keys" not in data:
        return data
    keys = normalize_upstream_keys(data.get("upstream_asset_key"), data.get("upstream_asset_keys"))
    out = {k: v for k, v in data.items() if k not in ("upstream_asset_key", "upstream_asset_keys")}
    if mode == "single":
        if len(keys) > 1:
            raise ValueError(f"{component} takes exactly one upstream asset, got {keys}")
        out["upstream_asset_key"] = keys[0] if keys else (None if optional else "")
    else:
        out["upstream_asset_keys"] = keys or (None if optional else [])
    return out
# END upstream-asset-key normalization


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
        type: dagster_community_components.FabricWorkspaceComponent
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

    # `polling_sensor` carries `alias="generate_sensor"` below for backcompat
    # with the pre-rename field name -- populate_by_name lets BOTH the current
    # field name and the old alias validate. Without it, Pydantic accepts only
    # the alias and rejects the current field name as an unknown extra input.
    model_config = ConfigDict(populate_by_name=True)

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
    upstream_asset_keys: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Asset keys that all imported assets wait for (lineage-only).",
    )

    key: Optional[str] = Field(
        default=None,
        description=(
            "Disambiguator for the cached discovery-state key. Without this, two "
            "FabricWorkspaceComponent instances pointed at the SAME workspace_id but "
            "different import_*/filter settings (e.g. one importing only notebooks "
            "matching 'part_a', another only pipelines) would hash to the same state "
            "key and silently share one state file. Set a distinct `key:` per instance "
            "when you split one workspace across multiple component instances."
        ),
    )

    default_parameters: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Parameters passed to every triggered Notebook/DataPipeline/Dataflow job "
            "(e.g. a load date), merged with `item_parameters` below. A plain "
            "{name: value} dict -- converted internally to Fabric's documented "
            "Parameter[] request shape ({name, value, type}, type inferred from the "
            "Python value). Not all item/job types accept parameters -- Fabric's own "
            "API returns errorCode FeatureNotAvailable for ones that don't, surfaced "
            "as a clear Failure rather than a generic HTTP error."
        ),
    )
    item_parameters: Optional[Dict[str, Dict[str, Any]]] = Field(
        default=None,
        description=(
            "Per-item parameter overrides, keyed by the Fabric item's display name "
            "(e.g. {'my_notebook': {'load_date': '2024-01-01'}}). Merged OVER "
            "default_parameters for that specific item -- lets one notebook wait for "
            "a different input than another from the same component instance."
        ),
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
    sensor_check_interval_seconds: int = Field(
        default=300,
        description=(
            "Seconds between polling-sensor ticks. Only used when polling_sensor=True. "
            "Distinct from workspace.poll_interval_seconds, which governs the in-asset "
            "trigger-and-wait loop for runs Dagster itself kicks off."
        ),
    )

    defs_state: ResolvedDefsStateConfig = Field(
        default_factory=DefsStateConfigArgs.local_filesystem,
        description=(
            "State backend for cached workspace discovery. Local filesystem by "
            "default. Overridden per-deploy for prod runs against Dagster Cloud."
        ),
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_upstream_asset_keys(cls, data):
        return coerce_upstream_input(data, component=cls.__name__, mode="multi", optional=True)

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
        # Hash workspace_id + the optional instance disambiguator (`key`), not
        # workspace_id alone -- otherwise two instances over the same workspace
        # with different import_*/filter settings silently share one state file.
        hash_input = self.workspace.workspace_id + (self.key or "")
        wsid_hash = hashlib.sha256(hash_input.encode()).hexdigest()[:12]
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
        AssetSpec's key / tags / metadata / kinds / owners / deps /
        automation_condition / freshness_policy / description flow into the
        kwargs (translation-provided values win over inferred ones; `deps`
        and `tags`/`metadata`/`kinds` are merged rather than replaced, so a
        `translation:` can ADD a per-item dependency without having to
        re-declare `upstream_asset_keys`'s own deps).

        Before this fix, only key/metadata/tags/kinds/owners/group_name were
        copied from the translated AssetSpec -- setting `deps` or
        `automation_condition` inside a `translation:` callable silently did
        nothing, with no error raised, and `upstream_asset_keys` had no way
        to vary per item as a result (component-feedback: translation
        silently drops deps and automation_condition).
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
        if base_spec.deps:
            existing_deps = list(merged.get("deps") or [])
            existing_dep_keys = {getattr(d, "asset_key", d) for d in existing_deps}
            for dep in base_spec.deps:
                dep_key = getattr(dep, "asset_key", dep)
                if dep_key not in existing_dep_keys:
                    existing_deps.append(dep)
                    existing_dep_keys.add(dep_key)
            merged["deps"] = existing_deps
        if base_spec.automation_condition is not None:
            merged["automation_condition"] = base_spec.automation_condition
        if base_spec.freshness_policy is not None:
            merged["freshness_policy"] = base_spec.freshness_policy
        if base_spec.description:
            merged["description"] = base_spec.description
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

    async def refresh_state(self, project_root: Path) -> str:
        """Refresh LOCAL_FILESYSTEM state atomically.

        The stock ``StateBackedComponent.refresh_state`` does ``shutil.rmtree(state_path)`` and only then
        runs ``write_state_to_path`` (network calls, seconds long), so for the whole fetch -- and again
        while the file is half-written -- a concurrent reader (run worker, step subprocess, ``dg list defs``;
        every ``dg`` command refreshes in dev mode) sees missing/partial state: empty Definitions or a
        JSONDecodeError. Here the new state is built beside the old and swapped in with a rename, so the
        previous state stays readable until the new one is complete (and survives a failed refresh)."""
        import os
        import shutil
        import uuid

        from dagster._core.storage.defs_state.base import DefsStateStorage
        from dagster.components.utils.project_paths import get_local_state_path
        from dagster_shared.serdes.objects.models.defs_state_info import (
            LOCAL_STATE_VERSION,
            DefsStateManagementType,
        )

        config = self.defs_state_config
        if config.management_type != DefsStateManagementType.LOCAL_FILESYSTEM:
            return await super().refresh_state(project_root)
        state_path = get_local_state_path(config.key, project_root)
        state_path.parent.mkdir(parents=True, exist_ok=True)
        token = f"{os.getpid()}-{uuid.uuid4().hex[:8]}"
        work = state_path.parent / f".{state_path.name}.refresh-{token}"
        old = state_path.parent / f".{state_path.name}.old-{token}"
        try:
            await self._write_state_to_path_async(work)
            if work.exists():
                if work.is_dir() and state_path.is_dir():
                    try:
                        os.rename(state_path, old)
                        os.rename(work, state_path)
                    except OSError:  # e.g. Windows: directory held open by another process
                        shutil.rmtree(state_path, ignore_errors=True)
                        os.rename(work, state_path)
                else:
                    if state_path.is_dir():
                        shutil.rmtree(state_path, ignore_errors=True)
                    elif work.is_dir() and state_path.exists():
                        state_path.unlink()
                    os.replace(work, state_path)
        finally:
            for _p in (work, old):
                if _p.is_dir():
                    shutil.rmtree(_p, ignore_errors=True)
                else:
                    _p.unlink(missing_ok=True)
        state_storage = DefsStateStorage.get()
        if state_storage:
            state_storage.set_latest_version(config.key, LOCAL_STATE_VERSION)
        return LOCAL_STATE_VERSION

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
        # Populated by _build_runnable_asset below with the FINAL,
        # translation-resolved AssetKey each runnable item actually got (not
        # a re-derived default) -- consumed by the polling sensor further
        # down so sensor/asset identity never splits, the same class of bug
        # found in azure_data_factory's and ssis_workspace's observation
        # sensors (component-feedback/2026-09-03-azure-data-factory-demo-mode-seam.md,
        # component-feedback/2026-09-24-ssis-workspace-gaps.md).
        runnable_items: List[Dict[str, Any]] = []

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
                    self._build_runnable_asset(row, "Notebook", "notebook", upstream_keys, runnable_items)
                )
        if self.import_pipelines:
            for row in state.get("pipelines", []):
                assets.append(
                    self._build_runnable_asset(row, "DataPipeline", "data_pipeline", upstream_keys, runnable_items)
                )
        if self.import_dataflows:
            for row in state.get("dataflows", []):
                assets.append(
                    self._build_runnable_asset(row, "Dataflow", "dataflow", upstream_keys, runnable_items)
                )

        sensors = []
        if self.polling_sensor and runnable_items:
            sensors.append(self._build_polling_sensor(runnable_items))

        return Definitions(assets=assets, sensors=sensors)

    def _build_polling_sensor(self, runnable_items: List[Dict[str, Any]]):
        """Poll every imported Notebook/DataPipeline/Dataflow's Fabric job-run
        history and emit AssetObservation for completions Dagster didn't itself
        trigger (another scheduler, or a user running it manually in Fabric).

        One token fetch per tick, shared across every item. Cursor is a JSON
        dict of {item_id: last-seen COMPLETED job instance id}, so a rerun of
        the same instance id (job history keeps ~100 recent entries per
        Fabric's own API limit) is never re-emitted.
        """
        _self = self

        @dg.sensor(
            name=f"{self.group_name}_fabric_job_observation_sensor",
            minimum_interval_seconds=self.sensor_check_interval_seconds,
            asset_selection=dg.AssetSelection.assets(*[i["key"] for i in runnable_items]),
        )
        def _fabric_job_observation_sensor(context: dg.SensorEvaluationContext):
            import requests

            try:
                prev_seen: Dict[str, str] = json.loads(context.cursor) if context.cursor else {}
            except Exception:
                prev_seen = {}
            new_seen = dict(prev_seen)

            try:
                token = _self.workspace.get_token()
            except Exception as e:
                return dg.SensorResult(skip_reason=f"Fabric auth failed: {e}")
            headers = {"Authorization": f"Bearer {token}"}

            events = []
            for item in runnable_items:
                url = (
                    f"{_FABRIC_API_BASE}/workspaces/{_self.workspace.workspace_id}"
                    f"/items/{item['item_id']}/jobs/instances"
                )
                try:
                    resp = requests.get(url, headers=headers, timeout=30)
                    resp.raise_for_status()
                    instances = resp.json().get("value", [])
                except Exception as e:
                    context.log.warning(f"Fabric job-instance poll failed for {item['item_id']!r}: {e}")
                    continue

                completed = [i for i in instances if i.get("status") == "Completed"]
                if not completed:
                    continue
                # Instances aren't guaranteed sorted; pick the most recently started.
                latest = max(completed, key=lambda i: i.get("startTimeUtc") or "")
                if prev_seen.get(item["item_id"]) == latest["id"]:
                    continue
                new_seen[item["item_id"]] = latest["id"]
                events.append(dg.AssetObservation(
                    asset_key=item["key"],
                    metadata={
                        "fabric_job_instance_id": latest["id"],
                        "fabric_invoke_type": latest.get("invokeType"),
                        "fabric_start_time_utc": latest.get("startTimeUtc"),
                        "fabric_end_time_utc": latest.get("endTimeUtc"),
                    },
                    tags={"dagster/data_version": str(latest["id"])},
                ))

            if not events:
                return dg.SensorResult(skip_reason="No new Fabric job completions", cursor=json.dumps(new_seen))
            return dg.SensorResult(asset_events=events, cursor=json.dumps(new_seen))

        return _fabric_job_observation_sensor

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
            # Per-item kinds: `fabric` + the specific workload type
            # (`lakehouse` / `warehouse` / `semantic_model` / `report`).
            # Renders as badges in the Dagster catalog and enables
            # `kind:lakehouse` filtering. No first-class Dagster icons
            # for these — they render as text-only badges, still useful.
            kinds={"fabric", kind},
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

    def _build_runnable_asset(self, row: dict, item_type: str, kind: str, upstream_keys, runnable_items: Optional[List[Dict[str, Any]]] = None):
        """Build a materializable @asset that triggers a Fabric notebook /
        pipeline / dataflow job and polls it to completion.

        When `runnable_items` is passed, appends this item's FINAL,
        translation-resolved {item_id, item_type, key} -- consumed by the
        polling sensor so it observes against the asset's real key, not a
        re-derived default that can drift from a translation: override.
        """
        item_id = row.get("id")
        name = row.get("displayName") or "unnamed"
        default_name = f"fabric_{item_type.lower()}_{name}"
        _self = self

        base_kwargs: Dict[str, Any] = dict(
            key=dg.AssetKey.from_user_string(default_name),
            group_name=self.group_name,
            deps=upstream_keys,
            # Per-item kinds: `fabric` + the specific workload type
            # (`notebook` / `data_pipeline` / `dataflow`).
            # Renders as badges in the Dagster catalog and enables
            # `kind:notebook` filtering across the whole workspace.
            kinds={"fabric", kind},
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

        if runnable_items is not None:
            runnable_items.append({"item_id": item_id, "item_type": item_type, "key": asset_kwargs["key"]})

        resolved_parameters: Dict[str, Any] = dict(self.default_parameters or {})
        resolved_parameters.update((self.item_parameters or {}).get(name) or {})

        @dg.asset(**asset_kwargs)
        def _runnable(context: dg.AssetExecutionContext):
            return _self._trigger_item_run(context, item_id, item_type, resolved_parameters or None)

        return _runnable

    def _cancel_job_instance(self, instance_url: str, headers: dict, asset_log) -> None:
        """Best-effort cancellation of a still-running Fabric job instance.

        POST {instance_url}/cancel -- https://learn.microsoft.com/rest/api/fabric/core/job-scheduler/cancel-item-job-instance
        Used on timeout and when polling is interrupted by an unexpected
        exception, so a Dagster-side timeout/interrupt doesn't leave an
        orphaned Fabric job running. Never called once the job has already
        reached one of its own terminal states (Completed/Failed/Cancelled/
        Deduped) -- Fabric would just answer with a benign `JobAlreadyCompleted`
        error in that case, so there's nothing useful to cancel.
        """
        import requests
        try:
            resp = requests.post(f"{instance_url}/cancel", headers=headers, timeout=30)
            if resp.status_code >= 300:
                asset_log.warning(f"Fabric: cancel request failed ({resp.status_code}): {resp.text[:200]}")
            else:
                asset_log.info("Fabric: cancellation requested.")
        except Exception as e:
            asset_log.warning(f"Fabric: cancel request raised: {e}")

    def _trigger_item_run(
        self,
        context: dg.AssetExecutionContext,
        item_id: str,
        item_type: str,
        parameters: Optional[Dict[str, Any]] = None,
    ) -> dict:
        """Trigger a Fabric on-demand item job and poll until done.

        POST /workspaces/{ws}/items/{id}/jobs/instances?jobType=<T>
        The response Location header carries the job instance URL we poll.

        Raises `dg.Failure` for Failed / Cancelled / Deduped / Timeout, and
        `dg.RetryRequested` when Fabric's own `failureReason.isRetriable`
        flag says the failure is transient. Previously this method RETURNED
        the state dict in every case including failure, so a failed Fabric
        run showed up as a green Dagster materialization, with nothing
        downstream any the wiser (component-feedback: a failed Fabric run
        is a green Dagster materialization).

        Accepts the full `context` (not just a logger) so a subclass can
        read the partition key / retry number / run config, matching the
        same `context` a subclassed `execute()` hook gets on
        `FivetranAccountComponent`.
        """
        import requests
        asset_log = context.log

        try:
            token = self.workspace.get_token()
        except Exception as e:
            # Auth failures previously escaped as a raw azure.core/azure.identity
            # exception; wrap them so the Dagster UI shows a clear description.
            raise dg.Failure(
                description=f"Fabric authentication failed: {e}",
                metadata={"fabric_item_id": item_id, "fabric_item_type": item_type},
            ) from e
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
        fabric_params = _to_fabric_parameters(parameters)
        body = {"parameters": fabric_params} if fabric_params else None
        resp = requests.post(run_url, json=body, headers=headers, timeout=60)
        if resp.status_code >= 300:
            error = _extract_fabric_error(resp)
            raise dg.Failure(
                description=f"Fabric run trigger failed ({resp.status_code}): {error['message']}",
                metadata={
                    "fabric_item_id": item_id,
                    "fabric_item_type": item_type,
                    "fabric_error_code": error["errorCode"],
                    "fabric_status_code": resp.status_code,
                },
            )

        instance_url = resp.headers.get("Location")
        if not instance_url:
            asset_log.warning("Fabric: no Location header on job trigger response; cannot poll")
            return {"status": "Triggered", "instance_url": None}

        asset_log.info(f"Fabric job triggered. Instance: {instance_url}")
        elapsed = 0
        reached_fabric_terminal_state = False
        try:
            while elapsed < self.workspace.max_wait_seconds:
                state_resp = requests.get(instance_url, headers=headers, timeout=60)
                state_resp.raise_for_status()
                state = state_resp.json()
                status = state.get("status", "Unknown")
                asset_log.info(f"Fabric job: status={status} elapsed={elapsed}s")

                if status == "Completed":
                    reached_fabric_terminal_state = True
                    return state

                if status == "Deduped":
                    reached_fabric_terminal_state = True
                    raise dg.Failure(
                        description=(
                            f"Fabric job for item {item_id} was Deduped: another instance of "
                            "the same job type was already running, so Fabric skipped this one. "
                            "Not a failure of this run's own logic -- a scheduling collision."
                        ),
                        metadata={
                            "fabric_item_id": item_id,
                            "fabric_item_type": item_type,
                            "fabric_job_instance": state.get("id", ""),
                        },
                    )

                if status in ("Failed", "Cancelled"):
                    reached_fabric_terminal_state = True
                    failure = state.get("failureReason") or {}
                    error_code = failure.get("errorCode") or ""
                    message = failure.get("message") or f"Fabric job ended with status {status}"
                    is_retriable = failure.get("isRetriable")
                    md = {
                        "fabric_item_id": item_id,
                        "fabric_item_type": item_type,
                        "fabric_status": status,
                        "fabric_error_code": error_code,
                        "fabric_job_instance": state.get("id", ""),
                    }
                    failure_exc = dg.Failure(
                        description=f"Fabric job {status.lower()}: {message}",
                        metadata=md,
                    )
                    if is_retriable:
                        # Fabric's own failureReason.isRetriable flag, not a guess
                        # based on matching error-code/message substrings -- the
                        # authoritative signal the API itself provides for whether
                        # re-triggering the job is worth attempting.
                        asset_log.warning(f"Fabric job failed with a retriable error ({error_code}): {message}")
                        raise dg.RetryRequested(max_retries=1, seconds_to_wait=30) from failure_exc
                    raise failure_exc

                time.sleep(self.workspace.poll_interval_seconds)
                elapsed += self.workspace.poll_interval_seconds

            asset_log.warning(f"Fabric job timed out after {elapsed}s.")
            raise dg.Failure(
                description=f"Fabric job for item {item_id} timed out after {elapsed}s waiting for completion.",
                metadata={
                    "fabric_item_id": item_id,
                    "fabric_item_type": item_type,
                    "fabric_job_instance_url": instance_url,
                },
            )
        finally:
            # Cancel on timeout, or on any other exception interrupting the poll
            # loop -- but not when Fabric's own job already reached a terminal
            # state on its own (Completed/Failed/Cancelled/Deduped), where
            # there's nothing left running to cancel.
            if not reached_fabric_terminal_state:
                self._cancel_job_instance(instance_url, headers, asset_log)


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
