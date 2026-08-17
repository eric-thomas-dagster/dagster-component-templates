"""TM1 Workspace Component.

StateBackedComponent that auto-enumerates every TM1 Cube, Process, and Chore
via the TM1 REST API and emits one Dagster asset per object. Discovery is
cached to disk on `write_state_to_path`; every subsequent `build_defs_from_state`
reads the cache without hitting the API.

Refresh the catalog via `dg utils refresh-defs-state` (or Dagster+ auto-refresh)
— same pattern as FivetranWorkspace.

Follows the canonical `workspace: <Resource>` pattern used by
dagster-databricks / dagster-fivetran / dagster-powerbi / snowflake_workspace /
mlflow_workspace — secrets travel inline in the `workspace:` block via
`{{ env.XXX }}` Jinja templating, and the runtime component reads them off
`self.workspace.<attr>`.

Aligns with the same convention as `SnowflakeWorkspaceComponent`:
- `@public` class
- `translation:` callable field
- `@public get_asset_spec(props)` hook
- `polling_sensor` (alias `generate_sensor`) opt-in
- `defs_state` + `defs_state_config` property
- `StateBackedComponent` inheritance with `write_state_to_path` +
  `build_defs_from_state`
- `TM1ObjectProps` @record + `DagsterTM1Translator` +
  `TM1ComponentTranslator`
"""

import hashlib
import json
from dataclasses import dataclass
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

# Reuse the existing TM1Resource (base_url + username/password + optional
# cam_namespace + verify_ssl) so the workspace: block matches the
# `tm1_resource` component and the rest of the TM1 toolkit
# (tm1_process_trigger_job, tm1_cube_data_ingestion, etc.).
from dagster_community_components.resources.tm1_resource.component import TM1Resource


@record
class TM1ObjectProps:
    """Data passed to translation callables for each imported TM1 object.

    Mirrors the shape of `FivetranConnectorTableProps` / `SnowflakeObjectProps` /
    `MLflowObjectProps` — a single record describing the object so
    `translation:` callables can filter, rename, add tags, etc.

    Attributes:
        object_kind: One of 'cube' / 'process' / 'chore'.
        object_name: The TM1 object's name.
        extra: Kind-specific metadata (reserved for future use — e.g. Cube
            dimensionality, Chore schedule, etc.).
    """
    object_kind: str
    object_name: str
    extra: Optional[Dict[str, Any]] = None


@dataclass
class TM1ObjectSelector(dg.Resolvable):
    """Selector shape for filtering TM1 objects (Cubes / Processes / Chores).

    Mirrors FivetranWorkspace's `connector_selector` shape:

        cube_selector:
          by_name: [Sales, Finance]
          by_pattern: [Actual_*]
          exclude_by_name: [test_cube]
          exclude_by_pattern: [*_deprecated]

    Empty `by_name` + empty `by_pattern` = include everything.
    `exclude_by_*` always wins over `by_*`.
    """
    by_name: Optional[List[str]] = None
    by_pattern: Optional[List[str]] = None
    exclude_by_name: Optional[List[str]] = None
    exclude_by_pattern: Optional[List[str]] = None

    def matches(self, name: str) -> bool:
        import fnmatch
        if self.exclude_by_name and name in self.exclude_by_name:
            return False
        if self.exclude_by_pattern and any(fnmatch.fnmatch(name, p) for p in self.exclude_by_pattern):
            return False
        if not self.by_name and not self.by_pattern:
            return True
        if self.by_name and name in self.by_name:
            return True
        if self.by_pattern and any(fnmatch.fnmatch(name, p) for p in self.by_pattern):
            return True
        return False


def _enumerate_tm1(base_url, username, password, cam_namespace, verify_ssl) -> dict:
    """Return {cubes: [...], processes: [...], chores: [...]}."""
    import base64
    import requests

    session = requests.Session()
    session.verify = verify_ssl
    api_base = f"{base_url.rstrip('/')}/api/v1"

    headers = {"Accept": "application/json"}
    if cam_namespace:
        token = base64.b64encode(f"{username}:{password}:{cam_namespace}".encode()).decode()
        headers["Authorization"] = f"CAMNamespace {token}"
    else:
        token = base64.b64encode(f"{username}:{password}".encode()).decode()
        headers["Authorization"] = f"Basic {token}"

    out: dict = {"cubes": [], "processes": [], "chores": []}

    for kind, url_suffix, key in [
        ("cubes", "Cubes", "cubes"),
        ("processes", "Processes", "processes"),
        ("chores", "Chores", "chores"),
    ]:
        try:
            r = session.get(f"{api_base}/{url_suffix}", headers=headers, timeout=30)
            r.raise_for_status()
            body = r.json() or {}
            items = body.get("value") or []
            for item in items:
                name = item.get("Name")
                if not name:
                    continue
                out[key].append({"name": name})
        except Exception:  # noqa: BLE001 — enumeration best-effort per kind
            continue

    return out


@public
class TM1WorkspaceComponent(StateBackedComponent, Model, Resolvable):
    """Component for importing TM1 Cubes / Processes / Chores as Dagster assets.

    Supports importing:
    - Cubes (as observation assets — carry cube presence metadata; use
      `tm1_cube_data_ingestion` for actual data extraction)
    - Processes (as executable assets — materializing kicks off the TI
      process via TM1 REST API)
    - Chores (as executable assets — materializing kicks off the chore)

    Example (canonical `workspace:` block, mirrors dagster-databricks):

        ```yaml
        type: dagster_community_components.TM1WorkspaceComponent
        attributes:
          workspace:
            base_url: "{{ env.TM1_URL }}"             # e.g. https://tm1.acme.com:5495
            username: "{{ env.TM1_USER }}"
            password: "{{ env.TM1_PASSWORD }}"
            cam_namespace: "{{ env.TM1_CAM_NS }}"     # optional (CAM SSO)
            verify_ssl: true
          cube_selector:
            by_name: [Sales, Finance]
          process_selector:
            by_pattern: [Load_*]
          chore_selector:
            exclude_by_pattern: [*_deprecated]
          group_name: tm1_planning
          defs_state:
            management_type: LOCAL_FILESYSTEM
            refresh_if_dev: true
        ```
    """

    # ── Connection: workspace: block IS a TM1Resource ──────────────────
    # Canonical shape — mirrors dagster-databricks / dagster-fivetran /
    # dagster-powerbi / snowflake_workspace / mlflow_workspace workspace
    # components (all have `workspace: <Resource>`).
    workspace: Annotated[
        TM1Resource,
        Resolver(
            lambda context, model: TM1Resource(
                **resolve_fields(model, TM1Resource, context)  # ty: ignore[invalid-argument-type]
            ),
        ),
    ] = Field(
        description=(
            "TM1 connection as a TM1Resource (base_url + optional "
            "username/password/cam_namespace + verify_ssl). Secrets typically "
            "arrive via `{{ env.XXX }}` Jinja templating in defs.yaml."
        ),
    )

    # Optional user-side customization hook. Matches the convention used by
    # FivetranAccountComponent / PowerBIWorkspaceComponent /
    # SnowflakeWorkspaceComponent / MLflowWorkspaceComponent — a callable
    # that takes (base_spec, props) and returns a modified AssetSpec.
    # Applied to each imported TM1 object; wired via `TM1ComponentTranslator`.
    translation: Annotated[
        Optional[TranslationFn[TM1ObjectProps]],
        TranslationFnResolver(template_vars_for_translation_fn=lambda data: {"props": data}),
    ] = Field(
        default=None,
        description=(
            "Function used to translate TM1 object properties into Dagster "
            "asset specs. Called for each imported cube / process / chore. "
            "If unset, the base translator's default AssetSpec is used."
        ),
    )

    cube_selector: Optional[TM1ObjectSelector] = Field(
        default=None,
        description="Optional inclusion/exclusion filter for cube names.",
    )
    process_selector: Optional[TM1ObjectSelector] = Field(
        default=None,
        description="Optional inclusion/exclusion filter for process names.",
    )
    chore_selector: Optional[TM1ObjectSelector] = Field(
        default=None,
        description="Optional inclusion/exclusion filter for chore names.",
    )

    group_name: Optional[str] = Field(
        default=None,
        description="Group name for all imported assets.",
    )
    asset_key_prefix: List[str] = Field(
        default_factory=lambda: ["tm1"],
        description="Key prefix used for all emitted AssetKeys.",
    )
    compute_kind: str = Field(
        default="tm1",
        description="Compute kind tag for all imported assets.",
    )

    # For process / chore assets — action on materialize.
    wait_for_completion: bool = Field(
        default=True,
        description=(
            "If true, wait for TM1's synchronous execute call to return "
            "before finishing the asset run. TM1 REST executes synchronously "
            "by default — flag is retained for future async support."
        ),
    )
    timeout_seconds: int = Field(
        default=1800,
        description="HTTP timeout for TM1 process/chore execute calls (seconds).",
    )

    polling_sensor: bool = Field(
        default=False,
        description=(
            "Reserved: if true, add a polling sensor that detects new TM1 "
            "process runs and emits AssetObservation events. Matches the "
            "`polling_sensor` convention on FivetranAccountComponent and "
            "SnowflakeWorkspaceComponent. Off by default — TM1 has no "
            "cheap change-signal, so opt in explicitly."
        ),
        alias="generate_sensor",  # backward-compat: old YAML still resolves
    )

    defs_state: ResolvedDefsStateConfig = Field(
        default_factory=DefsStateConfigArgs.local_filesystem,
        description=(
            "State backend for cached workspace discovery. Local filesystem by "
            "default. Overridden per-deploy for prod runs against Dagster Cloud."
        ),
    )

    @public
    def get_asset_spec(self, props: TM1ObjectProps) -> AssetSpec:
        """Generates an AssetSpec for a given TM1 object.

        This method can be overridden in a subclass to customize how TM1
        objects are converted to Dagster asset specs. By default, it delegates
        to the configured translator (which respects the `translation:` field).

        Args:
            props: The TM1ObjectProps carrying object kind, name, and any
                kind-specific metadata.

        Returns:
            An AssetSpec that represents the TM1 object as a Dagster asset.

        Example:
            Override this method to add custom tags based on the object kind:

            .. code-block:: python

                from dagster_community_components import TM1WorkspaceComponent

                class CustomTM1WorkspaceComponent(TM1WorkspaceComponent):
                    def get_asset_spec(self, props):
                        base_spec = super().get_asset_spec(props)
                        return base_spec.replace_attributes(
                            tags={
                                **base_spec.tags,
                                "tm1_object_kind": props.object_kind,
                            }
                        )
        """
        return self._base_translator.get_asset_spec(props)

    @property
    def _base_translator(self) -> "TM1ComponentTranslator":
        # Cached lazily so subclasses can still override get_asset_spec cleanly.
        cached = getattr(self, "__base_translator_cached", None)
        if cached is None:
            cached = TM1ComponentTranslator(self)
            object.__setattr__(self, "__base_translator_cached", cached)
        return cached

    @property
    def defs_state_config(self) -> DefsStateConfig:
        # Key on base_url so multiple TM1 servers don't collide in the
        # shared local-filesystem state dir. Hashed to keep the key
        # filesystem-safe.
        url_hash = hashlib.sha256(self.workspace.base_url.encode()).hexdigest()[:12]
        default_key = f"{self.__class__.__name__}[{url_hash}]"
        return DefsStateConfig.from_args(self.defs_state, default_key=default_key)

    def _apply_translation(
        self,
        kwargs: Dict[str, Any],
        kind: str,
        name: str,
        extra: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Fold the translation callable into per-asset kwargs.

        Builds a ``TM1ObjectProps`` and calls ``self.get_asset_spec(props)``,
        which delegates to ``TM1ComponentTranslator`` (base spec + optional
        user ``translation:`` callable).

        Backward-compat: when no ``translation:`` callable is set, the base
        translator returns the default AssetSpec and this method is a no-op —
        all pre-existing per-asset kwargs win. When a ``translation:`` callable
        IS set, its AssetSpec's key / tags / metadata / kinds / owners flow
        into the kwargs (translation-provided values win over inferred ones).
        """
        if self.translation is None:
            return kwargs

        props = TM1ObjectProps(
            object_kind=kind,
            object_name=name,
            extra=extra,
        )
        base_spec = self.get_asset_spec(props)
        merged = dict(kwargs)
        # Translation callable can rename the key. Drop `name=` when key is
        # supplied so `@asset` doesn't reject the double-declaration.
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

    async def write_state_to_path(self, state_path: Path) -> None:
        """Enumerate TM1 cubes/processes/chores and cache them to disk.

        Runs the same REST calls the previous inline discovery used, applies
        the ``cube_selector`` / ``process_selector`` / ``chore_selector``
        filters, and writes the surviving rows to ``state_path`` as a JSON
        dict keyed by object kind. ``build_defs_from_state`` re-hydrates from
        this snapshot so no TM1 HTTP calls fire at Dagster load time.
        """
        snapshot = _enumerate_tm1(
            self.workspace.base_url,
            self.workspace.username or "",
            self.workspace.password or "",
            self.workspace.cam_namespace,
            self.workspace.verify_ssl,
        )

        # Apply selectors.
        if self.cube_selector is not None:
            snapshot["cubes"] = [
                c for c in snapshot["cubes"] if self.cube_selector.matches(c["name"])
            ]
        if self.process_selector is not None:
            snapshot["processes"] = [
                p for p in snapshot["processes"] if self.process_selector.matches(p["name"])
            ]
        if self.chore_selector is not None:
            snapshot["chores"] = [
                c for c in snapshot["chores"] if self.chore_selector.matches(c["name"])
            ]

        state_path.write_text(json.dumps(snapshot, indent=2, default=str))

    def build_defs_from_state(
        self,
        context: ComponentLoadContext,
        state_path: Optional[Path],
    ) -> Definitions:
        """Build Dagster definitions from cached TM1 workspace state.

        Reads the JSON dict written by ``write_state_to_path`` and turns each
        cube/process/chore entry into a materializable ``@asset``. Runtime TM1
        REST calls (executing processes/chores) still fire on each
        materialization — only the discovery moved to state.
        """
        if state_path is None or not state_path.exists():
            return Definitions()
        state = json.loads(state_path.read_text())

        assets = []
        for c in state.get("cubes", []):
            assets.append(self._build_cube_asset(c["name"]))
        for p in state.get("processes", []):
            assets.append(self._build_process_asset(p["name"], target_type="process"))
        for c in state.get("chores", []):
            assets.append(self._build_process_asset(c["name"], target_type="chore"))
        return Definitions(assets=assets)

    def _build_cube_asset(self, cube: str):
        key = AssetKey([*self.asset_key_prefix, "cube", cube])

        base_kwargs: Dict[str, Any] = dict(
            key=key,
            group_name=self.group_name,
            compute_kind=self.compute_kind,
            metadata={"tm1_object": dg.MetadataValue.text(f"cube:{cube}")},
        )
        asset_kwargs = self._apply_translation(
            base_kwargs,
            kind="cube",
            name=cube,
            extra=None,
        )

        @dg.asset(**asset_kwargs)
        def _cube_asset(context: dg.AssetExecutionContext):
            # Cube assets are OBSERVATIONAL — materialize just records
            # that the cube exists; use tm1_cube_data_ingestion for
            # actual data extraction.
            context.log.info(f"TM1 cube {cube!r} present.")

        return _cube_asset

    def _build_process_asset(self, name: str, target_type: str):
        _self = self
        key = AssetKey([*self.asset_key_prefix, target_type, name])

        base_kwargs: Dict[str, Any] = dict(
            key=key,
            group_name=self.group_name,
            compute_kind=self.compute_kind,
            metadata={"tm1_object": dg.MetadataValue.text(f"{target_type}:{name}")},
        )
        asset_kwargs = self._apply_translation(
            base_kwargs,
            kind=target_type,
            name=name,
            extra=None,
        )

        @dg.asset(**asset_kwargs)
        def _asset(context: dg.AssetExecutionContext):
            import base64
            try:
                import requests
            except ImportError as e:
                raise Exception("requests library not installed") from e

            base_url = _self.workspace.base_url
            username = _self.workspace.username or ""
            password = _self.workspace.password or ""
            cam_ns = _self.workspace.cam_namespace

            session = requests.Session()
            session.verify = _self.workspace.verify_ssl
            api_base = f"{base_url.rstrip('/')}/api/v1"

            headers = {"Accept": "application/json", "Content-Type": "application/json"}
            if cam_ns:
                token = base64.b64encode(f"{username}:{password}:{cam_ns}".encode()).decode()
                headers["Authorization"] = f"CAMNamespace {token}"
            else:
                token = base64.b64encode(f"{username}:{password}".encode()).decode()
                headers["Authorization"] = f"Basic {token}"

            if target_type == "process":
                exec_url = f"{api_base}/Processes('{name}')/tm1.ExecuteProcess"
            else:
                exec_url = f"{api_base}/Chores('{name}')/tm1.Execute"

            r = session.post(exec_url, json={}, headers=headers, timeout=_self.timeout_seconds)
            if r.status_code >= 400:
                raise Exception(
                    f"TM1 execute failed: {r.status_code} {r.text[:200]} ({target_type}:{name})"
                )
            context.log.info(f"TM1: {target_type} {name!r} executed (status={r.status_code})")

            try:
                body = r.json() or {}
                status = body.get("ProcessExecuteStatusCode") or body.get("Status")
                if status and status not in ("CompletedSuccessfully", "Success"):
                    raise Exception(f"TM1 {target_type} ended with status {status}")
                if status:
                    context.add_output_metadata({"tm1_status": status})
            except ValueError:
                pass  # bare 200 is fine

        return _asset


class DagsterTM1Translator:
    """Base translator for TM1 workspace objects → AssetSpec.

    Follows the shape of `DagsterFivetranTranslator` / `DagsterPowerBITranslator` /
    `DagsterSnowflakeTranslator` / `DagsterMLflowTranslator`. Subclass this
    and override `get_asset_spec()` to fully customize how TM1 objects become
    Dagster assets — an alternative to the runtime `translation:` callable
    on the component.
    """

    def get_asset_spec(self, props: TM1ObjectProps) -> AssetSpec:
        """Default AssetSpec for a TM1 object.

        Key = ["tm1", <object_kind>, <object_name>] (lowercased for
        consistency with the rest of the Dagster catalog). Kind is set to
        the TM1 object type. Metadata carries the object kind + name.
        """
        return AssetSpec(
            key=AssetKey(["tm1", props.object_kind, props.object_name.lower()]),
            kinds={"tm1", props.object_kind},
            metadata={
                "tm1/object_kind": props.object_kind,
                "tm1/object_name": props.object_name,
            },
        )


class TM1ComponentTranslator(
    create_component_translator_cls(TM1WorkspaceComponent, DagsterTM1Translator),  # ty: ignore[unsupported-base]
    ComponentTranslator[TM1WorkspaceComponent],
):
    """Bridges `TM1WorkspaceComponent.translation` (runtime callable)
    with the base `DagsterTM1Translator` (class-level override).

    Mirrors `FivetranComponentTranslator` / `PowerBIComponentTranslator` /
    `SnowflakeComponentTranslator` / `MLflowComponentTranslator`.
    """

    def __init__(self, component: "TM1WorkspaceComponent"):
        self._component = component

    def get_asset_spec(self, props: TM1ObjectProps) -> AssetSpec:
        base_asset_spec = super().get_asset_spec(props)
        if self.component.translation is None:
            return base_asset_spec
        return self.component.translation(base_asset_spec, props)
