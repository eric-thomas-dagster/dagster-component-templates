"""Qlik Compose Workspace Component.

StateBackedComponent that auto-enumerates every Qlik Compose project
(data warehouse) x workflow x data mart via the Compose REST API and
emits one Dagster asset per object.

Follows the canonical `workspace: <Resource>` pattern used by
dagster-databricks / dagster-fivetran / dagster-powerbi -- secrets travel
inline in the `workspace:` block via `{{ env.XXX }}` Jinja templating, and
the runtime component reads them off `self.workspace.<attr>`.

Aligns with the same convention as `SnowflakeWorkspaceComponent` and
`MLflowWorkspaceComponent`:
- `@public` class
- `translation:` callable field
- `@public get_asset_spec(props)` hook
- `polling_sensor` (alias `generate_sensor`) opt-in
- `defs_state` + `defs_state_config` property
- `StateBackedComponent` inheritance with `write_state_to_path` +
  `build_defs_from_state`
- `QlikComposeObjectProps` @record + `DagsterQlikComposeTranslator` +
  `QlikComposeComponentTranslator`
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


@record
class QlikComposeObjectProps:
    """Data passed to translation callables for each imported Compose object.

    Mirrors the shape of `FivetranConnectorTableProps` / `SnowflakeObjectProps` /
    `MLflowObjectProps` -- a single record describing the object so
    `translation:` callables can filter, rename, add tags, etc.

    Attributes:
        object_kind: One of 'workflow' / 'data_mart' / 'project'.
        object_name: The Compose object's name (workflow / data mart / project).
        extra: Kind-specific metadata (parent project name for workflow /
            data_mart, etc.).
    """
    object_kind: str
    object_name: str
    extra: Optional[Dict[str, Any]] = None


class QlikComposeResource(dg.ConfigurableResource):
    """Qlik Compose workspace connection.

    Mirrors the shape of dagster_databricks.DatabricksWorkspace /
    dagster_fivetran.FivetranWorkspace / dagster_powerbi.PowerBIWorkspace --
    a `ConfigurableResource` holding the connection fields. Values typically
    arrive via `{{ env.XXX }}` templating from YAML.

    Named `QlikComposeResource` so it matches the community
    `qlik_compose_resource` component and reads naturally next to the
    official `dagster_snowflake.SnowflakeResource` /
    `dagster_databricks.DatabricksWorkspace` conventions.

    Auth is either API token (preferred for prod) OR session-based
    (username + password login via `/api/v1/login`).
    """

    base_url: str = Field(
        description=(
            "Compose base URL, e.g. https://qlikcompose.acme.com "
            "(no /qlikcompose/api/... path suffix)."
        ),
    )
    username: Optional[str] = Field(
        default=None,
        description="Optional session-auth username. Pair with password.",
    )
    password: Optional[str] = Field(
        default=None,
        description="Optional session-auth password. Pair with username.",
    )
    api_token: Optional[str] = Field(
        default=None,
        description="Optional Bearer API token. Preferred for prod.",
    )
    verify_ssl: bool = Field(
        default=True,
        description=(
            "TLS cert verification. Set false for self-signed dev "
            "environments only."
        ),
    )


@dataclass
class ComposeObjectSelector(dg.Resolvable):
    """Selector for filtering Compose objects (workflows / data marts).

    Same shape as Fivetran's connector_selector.
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


def _enumerate_compose(base_url, username, password, api_token, verify_ssl, projects_filter) -> dict:
    """Return {projects: [{name, workflows: [...], data_marts: [...]}]}."""
    import requests

    session = requests.Session()
    session.verify = verify_ssl
    api_base = f"{base_url.rstrip('/')}/qlikcompose/api/v1"

    headers = {"Accept": "application/json", "Content-Type": "application/json"}
    if api_token:
        headers["Authorization"] = f"Bearer {api_token}"
    else:
        try:
            r = session.post(
                f"{api_base}/login",
                json={"username": username, "password": password},
                timeout=30,
            )
            r.raise_for_status()
        except Exception:  # noqa: BLE001
            return {"projects": []}

    out: dict = {"projects": []}
    try:
        r = session.get(f"{api_base}/projects", headers=headers, timeout=30)
        r.raise_for_status()
        body = r.json() or {}
        proj_list = body.get("projects") or body.get("value") or []
    except Exception:  # noqa: BLE001
        return out

    for p in proj_list:
        pname = p.get("name") if isinstance(p, dict) else str(p)
        if not pname:
            continue
        if projects_filter is not None and pname not in projects_filter:
            continue

        wfs, marts = [], []
        try:
            wr = session.get(f"{api_base}/projects/{pname}/workflows", headers=headers, timeout=30)
            wr.raise_for_status()
            wbody = wr.json() or {}
            wf_items = wbody.get("workflows") or wbody.get("value") or wbody
            if isinstance(wf_items, list):
                for w in wf_items:
                    wname = w.get("name") if isinstance(w, dict) else str(w)
                    if wname:
                        wfs.append({"name": wname})
        except Exception:  # noqa: BLE001
            pass

        try:
            dr = session.get(f"{api_base}/projects/{pname}/data_marts", headers=headers, timeout=30)
            dr.raise_for_status()
            dbody = dr.json() or {}
            mart_items = dbody.get("data_marts") or dbody.get("value") or dbody
            if isinstance(mart_items, list):
                for m in mart_items:
                    mname = m.get("name") if isinstance(m, dict) else str(m)
                    if mname:
                        marts.append({"name": mname})
        except Exception:  # noqa: BLE001
            pass

        out["projects"].append({"name": pname, "workflows": wfs, "data_marts": marts})
    return out


@public
class QlikComposeWorkspaceComponent(StateBackedComponent, Model, Resolvable):
    """Component for importing Qlik Compose workflows and data marts
    as Dagster assets.

    Auto-enumerates every Compose project (data warehouse) x workflow x
    data mart via the Compose REST API and emits one Dagster asset per
    object. Workflow assets trigger a Compose run (and optionally wait
    for terminal state); data-mart assets are observational (a companion
    workflow rebuilds them).

    Example (canonical `workspace:` block, mirrors dagster-databricks):
        ```yaml
        type: dagster_community_components.QlikComposeWorkspaceComponent
        attributes:
          workspace:
            base_url: "{{ env.QLIK_COMPOSE_URL }}"
            api_token: "{{ env.QLIK_COMPOSE_TOKEN }}"    # OR
            # username: "{{ env.QLIK_COMPOSE_USER }}"
            # password: "{{ env.QLIK_COMPOSE_PASSWORD }}"
            verify_ssl: true
          projects: [FinanceDW, SalesDW]
          workflow_selector:
            by_pattern: ["FullBuild*", "Incremental*"]
          data_mart_selector:
            exclude_by_pattern: ["*_deprecated"]
        ```
    """

    # -- Connection: workspace: block IS a QlikComposeResource ------------
    # Canonical shape -- mirrors dagster-databricks / dagster-fivetran /
    # dagster-powerbi workspace components (all have `workspace: <Resource>`).
    workspace: Annotated[
        QlikComposeResource,
        Resolver(
            lambda context, model: QlikComposeResource(
                **resolve_fields(model, QlikComposeResource, context)  # ty: ignore[invalid-argument-type]
            ),
        ),
    ] = Field(
        description=(
            "Qlik Compose connection as a QlikComposeResource (base_url + "
            "either api_token OR username/password + verify_ssl). Secrets "
            "typically arrive via `{{ env.XXX }}` Jinja templating in defs.yaml."
        ),
    )

    # Optional user-side customization hook. Matches the convention used by
    # FivetranAccountComponent / PowerBIWorkspaceComponent /
    # SnowflakeWorkspaceComponent / MLflowWorkspaceComponent -- a callable
    # that takes (base_spec, props) and returns a modified AssetSpec.
    # Applied to each imported Compose object; wired via
    # `QlikComposeComponentTranslator`.
    translation: Annotated[
        Optional[TranslationFn[QlikComposeObjectProps]],
        TranslationFnResolver(template_vars_for_translation_fn=lambda data: {"props": data}),
    ] = Field(
        default=None,
        description=(
            "Function used to translate Compose object properties into "
            "Dagster asset specs. Called for each imported workflow / "
            "data mart. If unset, the base translator's default AssetSpec "
            "is used."
        ),
    )

    projects: Optional[List[str]] = Field(
        default=None,
        description=(
            "Optional whitelist of Compose project (data-warehouse) names. "
            "None means all discoverable projects."
        ),
    )
    workflow_selector: Optional[ComposeObjectSelector] = Field(
        default=None,
        description="Optional inclusion/exclusion filter for workflow names.",
    )
    data_mart_selector: Optional[ComposeObjectSelector] = Field(
        default=None,
        description="Optional inclusion/exclusion filter for data-mart names.",
    )

    group_name: Optional[str] = Field(
        default=None,
        description="Group name for all imported assets.",
    )
    asset_key_prefix: List[str] = Field(
        default_factory=lambda: ["qlik_compose"],
        description="Key prefix used for all emitted AssetKeys.",
    )
    compute_kind: str = Field(
        default="qlik_compose",
        description="Compute kind tag for all imported assets.",
    )

    wait_for_completion: bool = Field(
        default=True,
        description=(
            "When true, workflow-asset materializations block until the "
            "Compose workflow reaches a terminal state (COMPLETED / STOPPED "
            "/ ERROR / FAILED). When false, fire-and-forget."
        ),
    )
    poll_interval_seconds: int = Field(
        default=30,
        description="Interval between Compose workflow-state polls when waiting.",
    )
    timeout_seconds: int = Field(
        default=3600,
        description="Hard timeout for wait_for_completion polling.",
    )

    polling_sensor: bool = Field(
        default=False,
        description=(
            "If true, opts in to a polling sensor that detects new Compose "
            "workflow runs and emits AssetObservation events into Dagster's "
            "event log. Matches the `polling_sensor` convention on "
            "FivetranAccountComponent / SnowflakeWorkspaceComponent. Off by "
            "default -- opt in explicitly."
        ),
        alias="generate_sensor",  # backward-compat: old YAML still resolves
    )

    assets_by_name: Optional[Dict[str, Dict[str, Any]]] = Field(
        default=None,
        description=(
            "Per-object overrides keyed by the imported object's name "
            "(workflow name / data-mart name). Values are dicts of "
            "@asset kwargs applied on top of the auto-generated ones "
            "(group_name, tags, metadata, description, etc.)."
        ),
    )

    defs_state: ResolvedDefsStateConfig = Field(
        default_factory=DefsStateConfigArgs.local_filesystem,
        description=(
            "State backend for cached workspace discovery. Local filesystem by "
            "default. Overridden per-deploy for prod runs against Dagster Cloud."
        ),
    )

    @public
    def get_asset_spec(self, props: QlikComposeObjectProps) -> AssetSpec:
        """Generates an AssetSpec for a given Compose object.

        This method can be overridden in a subclass to customize how Compose
        objects are converted to Dagster asset specs. By default, it delegates
        to the configured translator (which respects the `translation:` field).

        Args:
            props: The QlikComposeObjectProps carrying object kind, name, and
                any kind-specific metadata.

        Returns:
            An AssetSpec that represents the Compose object as a Dagster asset.

        Example:
            Override this method to add custom tags based on the object kind:

            .. code-block:: python

                from dagster_community_components import QlikComposeWorkspaceComponent

                class CustomQlikComposeWorkspaceComponent(QlikComposeWorkspaceComponent):
                    def get_asset_spec(self, props):
                        base_spec = super().get_asset_spec(props)
                        return base_spec.replace_attributes(
                            tags={
                                **base_spec.tags,
                                "qlik_compose_object_kind": props.object_kind,
                            }
                        )
        """
        return self._base_translator.get_asset_spec(props)

    @property
    def _base_translator(self) -> "QlikComposeComponentTranslator":
        # Cached lazily so subclasses can still override get_asset_spec cleanly.
        cached = getattr(self, "__base_translator_cached", None)
        if cached is None:
            cached = QlikComposeComponentTranslator(self)
            object.__setattr__(self, "__base_translator_cached", cached)
        return cached

    @property
    def defs_state_config(self) -> DefsStateConfig:
        # Key on base_url so multiple Compose servers don't collide in the
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

        Builds a ``QlikComposeObjectProps`` and calls
        ``self.get_asset_spec(props)``, which delegates to
        ``QlikComposeComponentTranslator`` (base spec + optional user
        ``translation:`` callable).

        Backward-compat: when no ``translation:`` callable is set, the base
        translator returns the default AssetSpec and this method is a no-op --
        all pre-existing per-asset kwargs (name, key, group_name, metadata,
        tags, kinds) win. When a ``translation:`` callable IS set, its
        AssetSpec's key / tags / metadata / kinds / owners flow into the
        kwargs (translation-provided values win over inferred ones).
        """
        if self.translation is None:
            return kwargs

        props = QlikComposeObjectProps(
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
        """Enumerate Compose projects / workflows / data marts and cache them.

        Runs the same REST discovery calls the previous inline path used,
        applies the ``projects`` / ``workflow_selector`` / ``data_mart_selector``
        filters, and writes the surviving rows to ``state_path`` as a JSON
        snapshot. ``build_defs_from_state`` re-hydrates from this snapshot so
        no Compose HTTP calls fire at Dagster load time.
        """
        snapshot = _enumerate_compose(
            self.workspace.base_url,
            self.workspace.username,
            self.workspace.password,
            self.workspace.api_token,
            self.workspace.verify_ssl,
            self.projects,
        )

        if self.workflow_selector is not None or self.data_mart_selector is not None:
            for proj in snapshot["projects"]:
                if self.workflow_selector is not None:
                    proj["workflows"] = [
                        w for w in proj["workflows"]
                        if self.workflow_selector.matches(w["name"])
                    ]
                if self.data_mart_selector is not None:
                    proj["data_marts"] = [
                        m for m in proj["data_marts"]
                        if self.data_mart_selector.matches(m["name"])
                    ]

        state_path.write_text(json.dumps(snapshot, indent=2))

    def build_defs_from_state(
        self,
        context: ComponentLoadContext,
        state_path: Optional[Path],
    ) -> Definitions:
        """Build Dagster definitions from cached Compose workspace state.

        Reads the JSON snapshot written by ``write_state_to_path`` and turns
        each workflow / data-mart entry into a materializable ``@asset``.
        Runtime Compose calls (workflow run, poll for terminal state) still
        fire on each materialization -- only the discovery moved to state.
        """
        if state_path is None or not state_path.exists():
            return Definitions()
        state = json.loads(state_path.read_text())

        assets = []
        for proj in state.get("projects", []):
            pname = proj["name"]
            for w in proj.get("workflows", []):
                assets.append(self._build_workflow_asset(pname, w["name"]))
            for m in proj.get("data_marts", []):
                assets.append(self._build_data_mart_asset(pname, m["name"]))
        return Definitions(assets=assets)

    def _build_workflow_asset(self, project: str, workflow: str):
        _self = self
        key = AssetKey([*self.asset_key_prefix, project, "workflow", workflow])

        base_kwargs: Dict[str, Any] = dict(
            key=key,
            group_name=self.group_name,
            compute_kind=self.compute_kind,
            metadata={
                "qlik_project": dg.MetadataValue.text(project),
                "qlik_workflow": dg.MetadataValue.text(workflow),
                "compose_object": dg.MetadataValue.text("workflow"),
            },
        )
        # Per-object overrides (assets_by_name["<workflow>"] wins over defaults).
        if self.assets_by_name and workflow in self.assets_by_name:
            override = self.assets_by_name[workflow] or {}
            for k, v in override.items():
                base_kwargs[k] = v

        asset_kwargs = self._apply_translation(
            base_kwargs,
            kind="workflow",
            name=workflow,
            extra={"project": project},
        )

        @dg.asset(**asset_kwargs)
        def _asset(context: dg.AssetExecutionContext):
            import time
            try:
                import requests
            except ImportError as e:
                raise Exception("requests library not installed") from e

            session = requests.Session()
            session.verify = _self.workspace.verify_ssl
            api_base = f"{_self.workspace.base_url.rstrip('/')}/qlikcompose/api/v1"

            headers = {"Accept": "application/json", "Content-Type": "application/json"}
            if _self.workspace.api_token:
                headers["Authorization"] = f"Bearer {_self.workspace.api_token}"
            else:
                r = session.post(
                    f"{api_base}/login",
                    json={
                        "username": _self.workspace.username,
                        "password": _self.workspace.password,
                    },
                    timeout=30,
                )
                if r.status_code >= 300:
                    raise Exception(f"Compose login failed: {r.status_code}")

            workflow_url = f"{api_base}/projects/{project}/workflows/{workflow}"
            r = session.post(f"{workflow_url}?action=run", headers=headers, timeout=60)
            if r.status_code >= 400:
                raise Exception(
                    f"Compose workflow run failed: {r.status_code} {r.text[:200]}"
                )
            context.log.info(f"Qlik Compose: run sent to {project}/{workflow}")

            if _self.wait_for_completion:
                deadline = time.time() + _self.timeout_seconds
                terminal = {"COMPLETED", "STOPPED", "ERROR", "FAILED"}
                last_state = None
                while time.time() < deadline:
                    time.sleep(_self.poll_interval_seconds)
                    sr = session.get(workflow_url, headers=headers, timeout=30)
                    if sr.status_code >= 300:
                        continue
                    body = sr.json() or {}
                    state = (body.get("workflow", {}) or {}).get("state") or body.get("state") or ""
                    if state and state != last_state:
                        context.log.info(f"workflow state: {state}")
                        last_state = state
                    if state and state.upper() in terminal:
                        if state.upper() in ("ERROR", "FAILED"):
                            raise Exception(
                                f"Compose workflow ended in {state} "
                                f"({project}/{workflow})"
                            )
                        context.add_output_metadata({"final_state": state})
                        return
                raise Exception(
                    f"Workflow did not reach terminal state within "
                    f"{_self.timeout_seconds}s ({project}/{workflow})"
                )

        return _asset

    def _build_data_mart_asset(self, project: str, data_mart: str):
        _self = self
        key = AssetKey([*self.asset_key_prefix, project, "data_mart", data_mart])

        base_kwargs: Dict[str, Any] = dict(
            key=key,
            group_name=self.group_name,
            compute_kind=self.compute_kind,
            metadata={
                "qlik_project": dg.MetadataValue.text(project),
                "qlik_data_mart": dg.MetadataValue.text(data_mart),
                "compose_object": dg.MetadataValue.text("data_mart"),
            },
        )
        if self.assets_by_name and data_mart in self.assets_by_name:
            override = self.assets_by_name[data_mart] or {}
            for k, v in override.items():
                base_kwargs[k] = v

        asset_kwargs = self._apply_translation(
            base_kwargs,
            kind="data_mart",
            name=data_mart,
            extra={"project": project},
        )

        @dg.asset(**asset_kwargs)
        def _asset(context: dg.AssetExecutionContext):
            # Data-mart assets are observational -- record presence.
            # A separate qlik_compose_workflow_trigger_job can rebuild.
            context.log.info(f"Compose data mart {project}/{data_mart} present.")

        return _asset


class DagsterQlikComposeTranslator:
    """Base translator for Compose workspace objects -> AssetSpec.

    Follows the shape of `DagsterFivetranTranslator` / `DagsterPowerBITranslator` /
    `DagsterSnowflakeTranslator` / `DagsterMLflowTranslator`. Subclass this and
    override `get_asset_spec()` to fully customize how Compose objects become
    Dagster assets -- an alternative to the runtime `translation:` callable on
    the component.
    """

    def get_asset_spec(self, props: QlikComposeObjectProps) -> AssetSpec:
        """Default AssetSpec for a Compose object.

        Key = ["qlik_compose", <object_kind>, <object_name>] (lowercased for
        consistency with the rest of the Dagster catalog). Kind is set to the
        Compose object type. Metadata carries the object kind + name.
        """
        return AssetSpec(
            key=AssetKey(["qlik_compose", props.object_kind, props.object_name.lower()]),
            kinds={"qlik_compose", props.object_kind},
            metadata={
                "qlik_compose/object_kind": props.object_kind,
                "qlik_compose/object_name": props.object_name,
            },
        )


class QlikComposeComponentTranslator(
    create_component_translator_cls(QlikComposeWorkspaceComponent, DagsterQlikComposeTranslator),  # ty: ignore[unsupported-base]
    ComponentTranslator[QlikComposeWorkspaceComponent],
):
    """Bridges `QlikComposeWorkspaceComponent.translation` (runtime callable)
    with the base `DagsterQlikComposeTranslator` (class-level override).

    Mirrors `FivetranComponentTranslator` / `PowerBIComponentTranslator` /
    `SnowflakeComponentTranslator` / `MLflowComponentTranslator`.
    """

    def __init__(self, component: "QlikComposeWorkspaceComponent"):
        self._component = component

    def get_asset_spec(self, props: QlikComposeObjectProps) -> AssetSpec:
        base_asset_spec = super().get_asset_spec(props)
        if self.component.translation is None:
            return base_asset_spec
        return self.component.translation(base_asset_spec, props)
