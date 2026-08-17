"""Qlik Replicate Workspace Component.

StateBackedComponent that auto-enumerates every Qlik Replicate task across
one or more Enterprise Manager servers and emits one Dagster asset per task.
The list of tasks is cached to disk on `write_state_to_path`; every subsequent
`build_defs_from_state` reads the cache without hitting the API — so cold
starts are fast and independent of Enterprise Manager availability.

Refresh the catalog explicitly via `dg utils refresh-defs-state` (or the
Dagster+ auto-refresh) — same pattern as the FivetranWorkspace shape.

Each emitted asset is materializable: it triggers the underlying Replicate
task (reload / resume) and polls until terminal state. So Dagster becomes
the imperative control plane over your entire Replicate fleet, with zero
per-task YAML.

Aligns with the same convention as `SnowflakeWorkspaceComponent` /
`MLflowWorkspaceComponent`:
- `@public` class
- `workspace: <Resource>` inline auth via `{{ env.XXX }}` templating
- `translation:` callable field
- `@public get_asset_spec(props)` hook
- `polling_sensor` (alias `generate_sensor`) opt-in
- `defs_state` + `defs_state_config` property
- `StateBackedComponent` inheritance with `write_state_to_path` +
  `build_defs_from_state`
- `QlikReplicateObjectProps` @record + `DagsterQlikReplicateTranslator` +
  `QlikReplicateComponentTranslator`
"""

import fnmatch
import hashlib
import json
import time
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
class QlikReplicateObjectProps:
    """Data passed to translation callables for each imported Qlik Replicate object.

    Mirrors the shape of `FivetranConnectorTableProps` / `SnowflakeObjectProps` /
    `MLflowObjectProps` — a single record describing the object so
    `translation:` callables can filter, rename, add tags, etc.

    Attributes:
        object_kind: One of 'task' (currently the only enumerated kind).
        object_name: The Qlik Replicate task name.
        server: The Enterprise Manager server the task lives on.
        extra: Kind-specific metadata (state / stage at discovery, etc.).
    """
    object_kind: str
    object_name: str
    server: Optional[str] = None
    extra: Optional[Dict[str, Any]] = None


class QlikReplicateResource(dg.ConfigurableResource):
    """Qlik Replicate Enterprise Manager connection.

    Mirrors the shape of dagster_databricks.DatabricksWorkspace /
    dagster_fivetran.FivetranWorkspace / dagster_powerbi.PowerBIWorkspace /
    dagster_community_components.MLflowResource — a `ConfigurableResource`
    holding just the connection fields. Values typically arrive via
    `{{ env.XXX }}` templating from YAML.

    Supports either API-token or basic-auth (username / password), matching
    both the newer Qlik EM API-key flow and the legacy session-login flow.
    """

    base_url: str = Field(
        description=(
            "Base URL of the Qlik Enterprise Manager instance, e.g. "
            "https://em.acme.com."
        ),
    )
    api_token: Optional[str] = Field(
        default=None,
        description="Optional API token / bearer token for the newer key flow.",
    )
    username: Optional[str] = Field(
        default=None,
        description="Optional basic-auth username for the session-login flow.",
    )
    password: Optional[str] = Field(
        default=None,
        description="Optional basic-auth password for the session-login flow.",
    )
    verify_ssl: bool = Field(
        default=True,
        description="TLS cert verification. Set false for self-signed dev environments.",
    )


def _login_if_needed(session, base_url: str, username: Optional[str], password: Optional[str]) -> None:
    """POST /login for session-based auth. No-op for API-token flows."""
    if not (username and password):
        return
    api_base = f"{base_url.rstrip('/')}/attunityenterprisemanager/api/v1"
    r = session.post(
        f"{api_base}/login",
        json={"username": username, "password": password},
        headers={"Accept": "application/json", "Content-Type": "application/json"},
        timeout=30,
    )
    if r.status_code >= 300:
        raise Exception(f"Qlik EM login failed: {r.status_code} {r.text[:200]}")


def _api_headers(api_token: Optional[str]) -> dict:
    h = {"Accept": "application/json", "Content-Type": "application/json"}
    if api_token:
        h["Authorization"] = f"Bearer {api_token}"
    return h


def _enumerate_workspace(
    base_url: str,
    username: Optional[str],
    password: Optional[str],
    api_token: Optional[str],
    verify_ssl: bool,
    servers_filter: Optional[List[str]],
) -> dict:
    """Return {servers: [{name, tasks: [{name, state, stage}]}]} from Qlik EM."""
    import requests
    session = requests.Session()
    session.verify = verify_ssl
    api_base = f"{base_url.rstrip('/')}/attunityenterprisemanager/api/v1"
    _login_if_needed(session, base_url, username, password)

    headers = _api_headers(api_token)

    servers: list = []
    r = session.get(f"{api_base}/servers", headers=headers, timeout=30)
    r.raise_for_status()
    server_body = r.json() or {}
    server_list = server_body.get("serverList") or server_body.get("servers") or []
    for sv in server_list:
        sv_name = sv.get("name") if isinstance(sv, dict) else str(sv)
        if not sv_name:
            continue
        if servers_filter is not None and sv_name not in servers_filter:
            continue

        tr = session.get(f"{api_base}/servers/{sv_name}/tasks", headers=headers, timeout=30)
        if tr.status_code >= 300:
            continue
        task_body = tr.json() or {}
        task_list = task_body.get("taskList") or task_body.get("tasks") or []

        server_tasks: list = []
        for t in task_list:
            t_name = t.get("name") if isinstance(t, dict) else str(t)
            if not t_name:
                continue
            # Fetch detail for state / stage.
            dr = session.get(f"{api_base}/servers/{sv_name}/tasks/{t_name}", headers=headers, timeout=15)
            detail = dr.json() or {} if dr.status_code < 300 else {}
            task_obj = detail.get("task") or detail
            server_tasks.append({
                "name": t_name,
                "state": task_obj.get("state"),
                "stage": task_obj.get("stage") or task_obj.get("current_stage"),
            })
        servers.append({"name": sv_name, "tasks": server_tasks})
    return {"servers": servers, "polled_at": time.time()}


@dataclass
class TaskSelector(dg.Resolvable):
    """Selector for filtering Qlik Replicate tasks.

    Mirrors the FivetranWorkspace `connector_selector` shape:

        task_selector:
          by_name: [orders_cdc, customers_cdc]       # exact names to include
          by_pattern: [orders_*]                      # globs to include
          exclude_by_name: [test_task]                # exact names to exclude
          exclude_by_pattern: [*_deprecated, *_test]  # globs to exclude

    Empty `by_name` + empty `by_pattern` = include all tasks.
    `exclude_by_*` always wins over `by_*`.
    """
    by_name: Optional[List[str]] = None
    by_pattern: Optional[List[str]] = None
    exclude_by_name: Optional[List[str]] = None
    exclude_by_pattern: Optional[List[str]] = None

    def matches(self, task_name: str) -> bool:
        # Exclusions win.
        if self.exclude_by_name and task_name in self.exclude_by_name:
            return False
        if self.exclude_by_pattern and any(fnmatch.fnmatch(task_name, p) for p in self.exclude_by_pattern):
            return False
        # If no include filters, include everything.
        if not self.by_name and not self.by_pattern:
            return True
        if self.by_name and task_name in self.by_name:
            return True
        if self.by_pattern and any(fnmatch.fnmatch(task_name, p) for p in self.by_pattern):
            return True
        return False


@public
class QlikReplicateWorkspaceComponent(StateBackedComponent, Model, Resolvable):
    """Auto-emit one Dagster asset per Qlik Replicate task.

    On `write_state_to_path`, enumerate every task across every server
    (optionally filtered) via the Qlik Enterprise Manager REST API. On
    `build_defs_from_state`, read the cached snapshot and emit one asset
    per (server, task). Materializing an asset triggers the underlying
    Replicate task and polls to completion.

    Example (canonical `workspace:` block, mirrors dagster-databricks):

        ```yaml
        type: dagster_community_components.QlikReplicateWorkspaceComponent
        attributes:
          workspace:
            base_url: "{{ env.QLIK_EM_URL }}"
            api_token: "{{ env.QLIK_EM_API_TOKEN }}"
            verify_ssl: true
          servers: [prod-replicate-01]
          task_selector:
            by_name: [orders_sqlserver_to_snowflake]
          group_name: qlik_replicate
          action: reload           # what to do on materialize
          wait_for_completion: true
        ```
    """

    # ── Connection: workspace: block IS a QlikReplicateResource ─────────
    # Canonical shape — mirrors dagster-databricks / dagster-fivetran /
    # dagster-powerbi workspace components (all have `workspace: <Resource>`).
    workspace: Annotated[
        QlikReplicateResource,
        Resolver(
            lambda context, model: QlikReplicateResource(
                **resolve_fields(model, QlikReplicateResource, context)  # ty: ignore[invalid-argument-type]
            ),
        ),
    ] = Field(
        description=(
            "Qlik Replicate Enterprise Manager connection as a "
            "QlikReplicateResource (base_url + optional api_token / basic-auth "
            "+ verify_ssl). Secrets typically arrive via `{{ env.XXX }}` Jinja "
            "templating in defs.yaml."
        ),
    )

    # Optional user-side customization hook. Matches the convention used by
    # FivetranAccountComponent / PowerBIWorkspaceComponent /
    # SnowflakeWorkspaceComponent / MLflowWorkspaceComponent — a callable
    # that takes (base_spec, props) and returns a modified AssetSpec. Applied
    # to each imported Qlik Replicate task; wired via
    # `QlikReplicateComponentTranslator`.
    translation: Annotated[
        Optional[TranslationFn[QlikReplicateObjectProps]],
        TranslationFnResolver(template_vars_for_translation_fn=lambda data: {"props": data}),
    ] = Field(
        default=None,
        description=(
            "Function used to translate Qlik Replicate object properties into "
            "Dagster asset specs. Called for each imported task. If unset, "
            "the base translator's default AssetSpec is used."
        ),
    )

    servers: Optional[List[str]] = Field(
        default=None,
        description="Optional server-name filter. None = all servers under the EM instance.",
    )
    task_selector: Optional[TaskSelector] = Field(
        default=None,
        description="Optional inclusion/exclusion filter for task names.",
    )

    group_name: Optional[str] = Field(
        default=None,
        description="Group name for all imported assets.",
    )
    asset_key_prefix: List[str] = Field(
        default_factory=lambda: ["qlik_replicate"],
        description="Key prefix used for all emitted AssetKeys.",
    )
    compute_kind: str = Field(
        default="qlik_replicate",
        description="Compute kind tag for all imported assets.",
    )

    # What each asset does when materialized.
    action: str = Field(
        default="run",
        description="Action sent to Qlik EM on materialize: run | reload | stop.",
    )
    run_option: str = Field(
        default="RESUME_PROCESSING",
        description="`option` query-string arg for the `run` action.",
    )
    wait_for_completion: bool = Field(
        default=True,
        description="If true, poll Qlik EM until the task reaches a terminal state.",
    )
    poll_interval_seconds: int = Field(
        default=15,
        description="Poll interval while waiting for terminal state.",
    )
    timeout_seconds: int = Field(
        default=3600,
        description="Give up waiting after this many seconds (asset fails).",
    )

    polling_sensor: bool = Field(
        default=False,
        description=(
            "If true, adds a polling sensor that detects Qlik Replicate task "
            "state changes and emits AssetObservation events into Dagster's "
            "event log. Matches the `polling_sensor` convention on "
            "FivetranAccountComponent / SnowflakeWorkspaceComponent / "
            "MLflowWorkspaceComponent. Off by default — opt in explicitly."
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
    def get_asset_spec(self, props: QlikReplicateObjectProps) -> AssetSpec:
        """Generates an AssetSpec for a given Qlik Replicate object.

        This method can be overridden in a subclass to customize how Qlik
        Replicate objects are converted to Dagster asset specs. By default,
        it delegates to the configured translator (which respects the
        `translation:` field).

        Args:
            props: The QlikReplicateObjectProps carrying object kind, name,
                server, and any kind-specific metadata.

        Returns:
            An AssetSpec that represents the Qlik Replicate object as a
            Dagster asset.

        Example:
            Override this method to add custom tags based on the source server:

            .. code-block:: python

                from dagster_community_components import QlikReplicateWorkspaceComponent

                class CustomQlikReplicateWorkspaceComponent(QlikReplicateWorkspaceComponent):
                    def get_asset_spec(self, props):
                        base_spec = super().get_asset_spec(props)
                        return base_spec.replace_attributes(
                            tags={
                                **base_spec.tags,
                                "qlik_server": props.server or "unknown",
                            }
                        )
        """
        return self._base_translator.get_asset_spec(props)

    @property
    def _base_translator(self) -> "QlikReplicateComponentTranslator":
        # Cached lazily so subclasses can still override get_asset_spec cleanly.
        cached = getattr(self, "__base_translator_cached", None)
        if cached is None:
            cached = QlikReplicateComponentTranslator(self)
            object.__setattr__(self, "__base_translator_cached", cached)
        return cached

    @property
    def defs_state_config(self) -> DefsStateConfig:
        # Key on the EM base URL so multiple Qlik EM servers don't collide in
        # the shared local-filesystem state dir. Hashed to keep the key
        # filesystem-safe.
        url_hash = hashlib.sha256(self.workspace.base_url.encode()).hexdigest()[:12]
        default_key = f"{self.__class__.__name__}[{url_hash}]"
        return DefsStateConfig.from_args(self.defs_state, default_key=default_key)

    def _apply_translation(
        self,
        kwargs: Dict[str, Any],
        kind: str,
        name: str,
        server: Optional[str] = None,
        extra: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Fold the translation callable into per-asset kwargs.

        Builds a ``QlikReplicateObjectProps`` and calls
        ``self.get_asset_spec(props)``, which delegates to
        ``QlikReplicateComponentTranslator`` (base spec + optional user
        ``translation:`` callable).

        Backward-compat: when no ``translation:`` callable is set, the base
        translator returns the default AssetSpec and this method is a no-op —
        all pre-existing per-asset kwargs (name, key, group_name, metadata,
        tags, kinds) win. When a ``translation:`` callable IS set, its
        AssetSpec's key / tags / metadata / kinds / owners flow into the
        kwargs (translation-provided values win over inferred ones).
        """
        if self.translation is None:
            return kwargs

        props = QlikReplicateObjectProps(
            object_kind=kind,
            object_name=name,
            server=server,
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
        """Enumerate Qlik Replicate tasks across every EM server and cache them.

        Runs the same Qlik EM REST calls that the previous inline discovery
        used, applies the ``servers`` / ``task_selector`` filters, and writes
        the surviving rows to ``state_path`` as a JSON dict.
        ``build_defs_from_state`` re-hydrates from this snapshot so no Qlik
        EM HTTP calls fire at Dagster load time.
        """
        snapshot = _enumerate_workspace(
            base_url=self.workspace.base_url,
            username=self.workspace.username,
            password=self.workspace.password,
            api_token=self.workspace.api_token,
            verify_ssl=self.workspace.verify_ssl,
            servers_filter=self.servers,
        )
        # Apply task_selector filtering.
        if self.task_selector is not None:
            for sv in snapshot["servers"]:
                sv["tasks"] = [t for t in sv["tasks"] if self.task_selector.matches(t["name"])]
        state_path.write_text(json.dumps(snapshot, indent=2))

    def build_defs_from_state(
        self,
        context: ComponentLoadContext,
        state_path: Optional[Path],
    ) -> Definitions:
        """Build Dagster definitions from cached Qlik Replicate workspace state.

        Reads the JSON dict written by ``write_state_to_path`` and turns each
        (server, task) into a materializable ``@asset``. Runtime Qlik EM calls
        (POST action + polling for terminal state) still fire on each
        materialization — only the discovery moved to state.
        """
        if state_path is None or not state_path.exists():
            return Definitions()
        state = json.loads(state_path.read_text())
        assets = []
        for sv in state.get("servers", []):
            sv_name = sv["name"]
            for t in sv["tasks"]:
                t_name = t["name"]
                assets.append(self._build_asset(sv_name, t_name, t))
        return Definitions(assets=assets)

    def _build_asset(self, server: str, task: str, task_snapshot: dict):
        _self = self
        key = AssetKey([*self.asset_key_prefix, server, task])

        base_kwargs: Dict[str, Any] = dict(
            key=key,
            group_name=self.group_name,
            compute_kind=self.compute_kind,
            metadata={
                "qlik_server": dg.MetadataValue.text(server),
                "qlik_task": dg.MetadataValue.text(task),
                "state_at_discovery": dg.MetadataValue.text(str(task_snapshot.get("state"))),
                "stage_at_discovery": dg.MetadataValue.text(str(task_snapshot.get("stage"))),
            },
        )
        asset_kwargs = self._apply_translation(
            base_kwargs,
            kind="task",
            name=task,
            server=server,
            extra={
                "state": task_snapshot.get("state"),
                "stage": task_snapshot.get("stage"),
            },
        )

        @dg.asset(**asset_kwargs)
        def _asset(context: dg.AssetExecutionContext):
            try:
                import requests
            except ImportError as e:
                raise Exception("requests library not installed") from e

            base_url = _self.workspace.base_url
            username = _self.workspace.username
            password = _self.workspace.password
            api_token = _self.workspace.api_token

            session = requests.Session()
            session.verify = _self.workspace.verify_ssl
            api_base = f"{base_url.rstrip('/')}/attunityenterprisemanager/api/v1"
            _login_if_needed(session, base_url, username, password)
            headers = _api_headers(api_token)

            task_url = f"{api_base}/servers/{server}/tasks/{task}"
            action_url = f"{task_url}?action={_self.action}"
            if _self.action == "run":
                action_url += f"&option={_self.run_option}"

            r = session.post(action_url, headers=headers, timeout=60)
            if r.status_code >= 300:
                raise Exception(
                    f"Qlik EM task action failed: {r.status_code} {r.text[:200]} "
                    f"(server={server} task={task} action={_self.action})"
                )
            context.log.info(f"Qlik Replicate: {_self.action} sent to {server}/{task}")

            if _self.wait_for_completion:
                deadline = time.time() + _self.timeout_seconds
                terminal = {"STOPPED", "ERROR"}
                last_state = None
                while time.time() < deadline:
                    time.sleep(_self.poll_interval_seconds)
                    sr = session.get(task_url, headers=headers, timeout=30)
                    if sr.status_code >= 300:
                        continue
                    body = sr.json() or {}
                    state = (body.get("task", {}) or {}).get("state") or body.get("state")
                    if state and state != last_state:
                        context.log.info(f"task state: {state}")
                        last_state = state
                    if state and state.upper() in terminal:
                        if state.upper() == "ERROR":
                            raise Exception(f"Task ended in ERROR (server={server}, task={task})")
                        context.add_output_metadata({
                            "final_state": state,
                            "duration_seconds": round(time.time() - (deadline - _self.timeout_seconds), 2),
                        })
                        return
                raise Exception(
                    f"Task did not reach terminal state within {_self.timeout_seconds}s "
                    f"(server={server}, task={task}, last state={last_state})"
                )

        return _asset


class DagsterQlikReplicateTranslator:
    """Base translator for Qlik Replicate workspace objects → AssetSpec.

    Follows the shape of `DagsterFivetranTranslator` / `DagsterPowerBITranslator` /
    `DagsterSnowflakeTranslator` / `DagsterMLflowTranslator`. Subclass this
    and override `get_asset_spec()` to fully customize how Qlik Replicate
    objects become Dagster assets — an alternative to the runtime
    `translation:` callable on the component.
    """

    def get_asset_spec(self, props: QlikReplicateObjectProps) -> AssetSpec:
        """Default AssetSpec for a Qlik Replicate object.

        Key = ["qlik_replicate", <object_kind>, <object_name>] (lowercased
        for consistency with the rest of the Dagster catalog). Kind is set
        to the Qlik Replicate object type. Metadata carries the object kind,
        name, and source server.
        """
        return AssetSpec(
            key=AssetKey(["qlik_replicate", props.object_kind, props.object_name.lower()]),
            kinds={"qlik_replicate", props.object_kind},
            metadata={
                "qlik_replicate/object_kind": props.object_kind,
                "qlik_replicate/object_name": props.object_name,
                "qlik_replicate/server": props.server or "",
            },
        )


class QlikReplicateComponentTranslator(
    create_component_translator_cls(QlikReplicateWorkspaceComponent, DagsterQlikReplicateTranslator),  # ty: ignore[unsupported-base]
    ComponentTranslator[QlikReplicateWorkspaceComponent],
):
    """Bridges `QlikReplicateWorkspaceComponent.translation` (runtime callable)
    with the base `DagsterQlikReplicateTranslator` (class-level override).

    Mirrors `FivetranComponentTranslator` / `PowerBIComponentTranslator` /
    `SnowflakeComponentTranslator` / `MLflowComponentTranslator`.
    """

    def __init__(self, component: "QlikReplicateWorkspaceComponent"):
        self._component = component

    def get_asset_spec(self, props: QlikReplicateObjectProps) -> AssetSpec:
        base_asset_spec = super().get_asset_spec(props)
        if self.component.translation is None:
            return base_asset_spec
        return self.component.translation(base_asset_spec, props)
