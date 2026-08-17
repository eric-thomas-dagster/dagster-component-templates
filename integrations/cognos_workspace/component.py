"""Cognos Workspace Component.

Auto-enumerates every IBM Cognos Analytics report and emits one Dagster
asset per report. Materializing an asset runs the report via the Cognos
REST API (session-based auth against `/api/v1/session`).

Follows the canonical `workspace: <Resource>` pattern used by
dagster-databricks / dagster-fivetran / dagster-powerbi — secrets travel
inline in the `workspace:` block via `{{ env.XXX }}` Jinja templating, and
the runtime component reads them off `self.workspace.<attr>`.

Aligns with the same convention as `SnowflakeWorkspaceComponent` /
`MLflowWorkspaceComponent`:
- `@public` class
- `translation:` callable field
- `@public get_asset_spec(props)` hook
- `polling_sensor` (alias `generate_sensor`) opt-in
- `defs_state` + `defs_state_config` property
- `StateBackedComponent` inheritance with `write_state_to_path` +
  `build_defs_from_state`
- `CognosObjectProps` @record + `DagsterCognosTranslator` +
  `CognosComponentTranslator`

Cognos auth quirk: login requires three parameters bundled together —
`CAMNamespace` (security namespace, typically `LDAP` / `CognosEx`),
`CAMUsername`, `CAMPassword` — POSTed as a `parameters:` array to
`/api/v1/session`. Session cookie is cached on the `requests.Session`.
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

# Reuse the community `CognosResource` so users don't have to redeclare its
# fields inline. `CognosResource` already exposes `base_url` / `username` /
# `password` / `namespace` / `verify_ssl` + REST helper methods.
from dagster_community_components.resources.cognos_resource.component import CognosResource


@record
class CognosObjectProps:
    """Data passed to translation callables for each imported Cognos object.

    Mirrors the shape of `SnowflakeObjectProps` / `MLflowObjectProps` — a
    single record describing the object so `translation:` callables can
    filter, rename, add tags, etc.

    Attributes:
        object_kind: Cognos object type — `report` / `dashboard` / `package`
            / etc. Currently the workspace only enumerates reports; the
            field is present so future kinds slot in without a signature
            change.
        object_name: The Cognos object's `defaultName` (falls back to
            `name`).
        extra: Kind-specific metadata — e.g. `{"id": ..., "folder": ...}`
            for reports.
    """
    object_kind: str
    object_name: str
    extra: Optional[Dict[str, Any]] = None


@dataclass
class CognosReportSelector(dg.Resolvable):
    """Selector for filtering Cognos reports. Same shape as Fivetran's connector_selector."""
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


def _enumerate_cognos(base_url, username, password, namespace, verify_ssl, folder_ids) -> dict:
    """Return {reports: [{id, name, folder}]}.

    Session-based auth: POST `/api/v1/session` with a `parameters:` array
    bundling `CAMNamespace` / `CAMUsername` / `CAMPassword`. The session
    cookie sticks on the `requests.Session` for subsequent
    `/content{folder}/items?type=report` walks.

    Response shape varies between Cognos versions — items land in one of
    `data` / `items` / `value`; IDs land in one of `id` / `storeID` /
    `searchPath`. All three paths are probed.
    """
    import requests

    session = requests.Session()
    session.verify = verify_ssl
    api_base = f"{base_url.rstrip('/')}/api/v1"

    # Login.
    login_body = {
        "parameters": [
            {"name": "CAMNamespace", "value": namespace or ""},
            {"name": "CAMUsername", "value": username or ""},
            {"name": "CAMPassword", "value": password or ""},
        ]
    }
    try:
        r = session.post(f"{api_base}/session", json=login_body, timeout=30)
        r.raise_for_status()
    except Exception:
        return {"reports": []}

    reports: list = []
    folders_to_walk = folder_ids or ["/"]

    for folder in folders_to_walk:
        try:
            r = session.get(f"{api_base}/content{folder}/items?type=report", timeout=30)
            r.raise_for_status()
            body = r.json() or {}
            items = body.get("data") or body.get("items") or body.get("value") or []
            for it in items:
                rid = it.get("id") or it.get("storeID") or it.get("searchPath")
                rname = it.get("defaultName") or it.get("name") or ""
                if rid and rname:
                    reports.append({"id": rid, "name": rname, "folder": folder})
        except Exception:  # noqa: BLE001
            continue

    return {"reports": reports}


@public
class CognosWorkspaceComponent(StateBackedComponent, Model, Resolvable):
    """Component for importing IBM Cognos Analytics reports as Dagster assets.

    Supports importing:
    - Reports (as materializable assets that run the underlying Cognos
      report via `POST /api/v1/reports/{id}/data`)

    Example (canonical `workspace:` block, mirrors dagster-databricks):
        ```yaml
        type: dagster_community_components.CognosWorkspaceComponent
        attributes:
          workspace:
            base_url: "{{ env.COGNOS_URL }}"
            username: "{{ env.COGNOS_USER }}"
            password: "{{ env.COGNOS_PASSWORD }}"
            namespace: "{{ env.COGNOS_NAMESPACE }}"  # LDAP / CognosEx / etc.
            verify_ssl: false
          folder_ids:
            - "/content/folder[@name='Finance']"
            - "/content/folder[@name='Ops']"
          report_selector:
            by_pattern: ["Monthly*", "Daily*"]
            exclude_by_pattern: ["*_deprecated"]
          group_name: cognos_reports
          output_format: CSV
        ```
    """

    # ── Connection: workspace: block IS a CognosResource ──────────────
    # Canonical shape — mirrors dagster-databricks / dagster-fivetran /
    # dagster-powerbi workspace components (all have `workspace: <Resource>`).
    # `resolve_fields()` lets `{{ env.XXX }}` templating fill the resource
    # fields at parse time.
    workspace: Annotated[
        CognosResource,
        Resolver(
            lambda context, model: CognosResource(
                **resolve_fields(model, CognosResource, context)  # ty: ignore[invalid-argument-type]
            ),
        ),
    ] = Field(
        description=(
            "Cognos connection as a CognosResource (base_url + username / "
            "password / namespace + verify_ssl). Secrets typically arrive "
            "via `{{ env.XXX }}` Jinja templating in defs.yaml."
        ),
    )

    # Optional user-side customization hook. Matches the convention used by
    # FivetranAccountComponent / SnowflakeWorkspaceComponent /
    # MLflowWorkspaceComponent — a callable that takes (base_spec, props)
    # and returns a modified AssetSpec. Applied to each imported Cognos
    # object; wired via `CognosComponentTranslator`.
    translation: Annotated[
        Optional[TranslationFn[CognosObjectProps]],
        TranslationFnResolver(template_vars_for_translation_fn=lambda data: {"props": data}),
    ] = Field(
        default=None,
        description=(
            "Function used to translate Cognos object properties into "
            "Dagster asset specs. Called for each imported report. If "
            "unset, the base translator's default AssetSpec is used."
        ),
    )

    folder_ids: Optional[List[str]] = Field(
        default=None,
        description=(
            "Cognos searchPath-style folder paths to walk. Omit for root "
            "(`/`). Example: `/content/folder[@name='Finance']`."
        ),
    )

    report_selector: Optional[CognosReportSelector] = Field(
        default=None,
        description="Optional inclusion/exclusion filter for report names.",
    )

    group_name: Optional[str] = Field(
        default=None,
        description="Group name for all imported assets.",
    )
    asset_key_prefix: List[str] = Field(
        default_factory=lambda: ["cognos", "report"],
        description="Key prefix used for all emitted AssetKeys.",
    )
    compute_kind: str = Field(
        default="cognos",
        description="Compute kind tag for all imported assets.",
    )

    output_format: str = Field(
        default="CSV",
        description="Cognos report output format (CSV / PDF / XLSX / JSON).",
    )
    wait_for_completion: bool = Field(
        default=True,
        description="If true, block until the report run completes.",
    )
    timeout_seconds: int = Field(
        default=600,
        description="Max seconds to wait for a report run to complete.",
    )

    polling_sensor: bool = Field(
        default=False,
        description=(
            "If true, adds a polling sensor that detects new Cognos report "
            "runs and emits AssetObservation events into Dagster's event "
            "log. Matches the `polling_sensor` convention on "
            "FivetranAccountComponent and SnowflakeWorkspaceComponent. Off "
            "by default — Cognos has no cheap change-signal, so opt in "
            "explicitly."
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
    def get_asset_spec(self, props: CognosObjectProps) -> AssetSpec:
        """Generates an AssetSpec for a given Cognos object.

        This method can be overridden in a subclass to customize how Cognos
        objects are converted to Dagster asset specs. By default, it delegates
        to the configured translator (which respects the `translation:` field).

        Args:
            props: The CognosObjectProps carrying object kind, name, and any
                kind-specific metadata (id / folder).

        Returns:
            An AssetSpec that represents the Cognos object as a Dagster asset.

        Example:
            Override this method to add custom tags based on the object kind:

            .. code-block:: python

                from dagster_community_components import CognosWorkspaceComponent

                class CustomCognosWorkspaceComponent(CognosWorkspaceComponent):
                    def get_asset_spec(self, props):
                        base_spec = super().get_asset_spec(props)
                        return base_spec.replace_attributes(
                            tags={
                                **base_spec.tags,
                                "cognos_object_kind": props.object_kind,
                            }
                        )
        """
        return self._base_translator.get_asset_spec(props)

    @property
    def _base_translator(self) -> "CognosComponentTranslator":
        # Cached lazily so subclasses can still override get_asset_spec cleanly.
        cached = getattr(self, "__base_translator_cached", None)
        if cached is None:
            cached = CognosComponentTranslator(self)
            object.__setattr__(self, "__base_translator_cached", cached)
        return cached

    @property
    def defs_state_config(self) -> DefsStateConfig:
        # Key on base URL so multiple Cognos servers don't collide in the
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

        Builds a ``CognosObjectProps`` and calls ``self.get_asset_spec(props)``,
        which delegates to ``CognosComponentTranslator`` (base spec + optional
        user ``translation:`` callable).

        Backward-compat: when no ``translation:`` callable is set, the base
        translator returns the default AssetSpec and this method is a no-op —
        all pre-existing per-asset kwargs (name, key, group_name, metadata,
        tags, kinds) win. When a ``translation:`` callable IS set, its
        AssetSpec's key / tags / metadata / kinds / owners flow into the
        kwargs (translation-provided values win over inferred ones).
        """
        if self.translation is None:
            return kwargs

        props = CognosObjectProps(
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
        """Enumerate Cognos reports and cache them as JSON.

        Runs the same session-login + paginated folder-walk that the
        previous inline discovery used, applies the ``report_selector``
        filter, and writes the surviving rows to ``state_path``.
        ``build_defs_from_state`` re-hydrates from this snapshot so no
        Cognos HTTP calls fire at Dagster load time.
        """
        snapshot = _enumerate_cognos(
            self.workspace.base_url,
            self.workspace.username,
            self.workspace.password,
            self.workspace.namespace,
            self.workspace.verify_ssl,
            self.folder_ids,
        )
        if self.report_selector is not None:
            snapshot["reports"] = [
                r for r in snapshot["reports"]
                if self.report_selector.matches(r["name"])
            ]
        state_path.write_text(json.dumps(snapshot, indent=2))

    def build_defs_from_state(
        self,
        context: ComponentLoadContext,
        state_path: Optional[Path],
    ) -> Definitions:
        """Build Dagster definitions from cached Cognos workspace state.

        Reads the JSON dict written by ``write_state_to_path`` and turns
        each report entry into a materializable ``@asset``. Runtime Cognos
        calls (session login + `POST /reports/{id}/data`) still fire on
        each materialization — only the discovery moved to state.
        """
        if state_path is None or not state_path.exists():
            return Definitions()
        state = json.loads(state_path.read_text())
        assets = []
        assets_by_name: Dict[str, Any] = {}
        for r in state.get("reports", []):
            asset = self._build_report_asset(r["id"], r["name"], r.get("folder"))
            assets.append(asset)
            assets_by_name[r["name"]] = asset
        # Preserve the pre-refactor `assets_by_name` handle so any downstream
        # code inspecting the component instance continues to work.
        object.__setattr__(self, "assets_by_name", assets_by_name)
        return Definitions(assets=assets)

    def _build_report_asset(self, report_id: str, name: str, folder: Optional[str] = None):
        _self = self
        # Sanitize name → valid asset-key segment.
        safe_name = "".join(c if c.isalnum() or c == "_" else "_" for c in name)
        key = AssetKey([*self.asset_key_prefix, safe_name])

        base_kwargs: Dict[str, Any] = dict(
            key=key,
            group_name=self.group_name,
            compute_kind=self.compute_kind,
            metadata={
                "cognos_report_id": dg.MetadataValue.text(report_id),
                "cognos_report_name": dg.MetadataValue.text(name),
            },
        )
        if folder:
            base_kwargs["metadata"]["cognos_folder"] = dg.MetadataValue.text(folder)

        asset_kwargs = self._apply_translation(
            base_kwargs,
            kind="report",
            name=name,
            extra={"id": report_id, "folder": folder},
        )

        @dg.asset(**asset_kwargs)
        def _asset(context: dg.AssetExecutionContext):
            try:
                import requests
            except ImportError as e:
                raise Exception("requests library not installed") from e

            base_url = _self.workspace.base_url
            username = _self.workspace.username or ""
            password = _self.workspace.password or ""
            namespace = _self.workspace.namespace or ""

            session = requests.Session()
            session.verify = _self.workspace.verify_ssl
            api_base = f"{base_url.rstrip('/')}/api/v1"

            r = session.post(f"{api_base}/session", json={"parameters": [
                {"name": "CAMNamespace", "value": namespace},
                {"name": "CAMUsername", "value": username},
                {"name": "CAMPassword", "value": password},
            ]}, timeout=30)
            if r.status_code >= 300:
                raise Exception(f"Cognos login failed: {r.status_code}")

            run_url = f"{api_base}/reports/{report_id}/data"
            body = {"format": _self.output_format}
            rr = session.post(run_url, json=body, timeout=_self.timeout_seconds)
            if rr.status_code >= 400:
                raise Exception(f"Cognos report run failed: {rr.status_code} {rr.text[:200]}")
            context.log.info(f"Cognos: report {report_id} ({name}) executed (status={rr.status_code})")

        return _asset


class DagsterCognosTranslator:
    """Base translator for Cognos workspace objects → AssetSpec.

    Follows the shape of `DagsterFivetranTranslator` / `DagsterSnowflakeTranslator` /
    `DagsterMLflowTranslator`. Subclass this and override `get_asset_spec()`
    to fully customize how Cognos objects become Dagster assets — an
    alternative to the runtime `translation:` callable on the component.
    """

    def get_asset_spec(self, props: CognosObjectProps) -> AssetSpec:
        """Default AssetSpec for a Cognos object.

        Key = ["cognos", <object_kind>, <object_name>] (lowercased for
        consistency with the rest of the Dagster catalog). Kind is set to
        the Cognos object type. Metadata carries the object kind + name.
        """
        return AssetSpec(
            key=AssetKey(["cognos", props.object_kind, props.object_name.lower()]),
            kinds={"cognos", props.object_kind},
            metadata={
                "cognos/object_kind": props.object_kind,
                "cognos/object_name": props.object_name,
            },
        )


class CognosComponentTranslator(
    create_component_translator_cls(CognosWorkspaceComponent, DagsterCognosTranslator),  # ty: ignore[unsupported-base]
    ComponentTranslator[CognosWorkspaceComponent],
):
    """Bridges `CognosWorkspaceComponent.translation` (runtime callable)
    with the base `DagsterCognosTranslator` (class-level override).

    Mirrors `FivetranComponentTranslator` / `SnowflakeComponentTranslator` /
    `MLflowComponentTranslator`.
    """

    def __init__(self, component: "CognosWorkspaceComponent"):
        self._component = component

    def get_asset_spec(self, props: CognosObjectProps) -> AssetSpec:
        base_asset_spec = super().get_asset_spec(props)
        if self.component.translation is None:
            return base_asset_spec
        return self.component.translation(base_asset_spec, props)
