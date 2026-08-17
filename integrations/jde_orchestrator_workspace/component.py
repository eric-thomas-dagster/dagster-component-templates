"""JDE Orchestrator Workspace Component.

Auto-enumerates JD Edwards EnterpriseOne Orchestrator objects (orchestrations,
and optionally reports / UBE / XML requests exposed through AIS) and emits
one Dagster asset per object. Discovery is cached to disk on
``write_state_to_path``; every subsequent load reads from the JSON cache and
runtime materialization (POST-ing to AIS, optional async job-status polling)
stays inside the individual ``@asset`` compute functions.

Follows the canonical ``workspace: <Resource>`` pattern used by
dagster-databricks / dagster-fivetran / dagster-powerbi — auth travels
inline in the ``workspace:`` block via ``{{ env.XXX }}`` Jinja templating,
and the runtime component reads them off ``self.workspace.<attr>``.

Aligns with the same convention as ``SnowflakeWorkspaceComponent`` /
``MLflowWorkspaceComponent``:
- ``@public`` class
- ``translation:`` callable field
- ``@public get_asset_spec(props)`` hook
- ``polling_sensor`` (alias ``generate_sensor``) opt-in placeholder
- ``defs_state`` + ``defs_state_config`` property
- ``StateBackedComponent`` inheritance with ``write_state_to_path`` +
  ``build_defs_from_state``
- ``JDEOrchestratorObjectProps`` @record + ``DagsterJDEOrchestratorTranslator``
  + ``JDEOrchestratorComponentTranslator``

The JDE side reuses ``JDEOrchestratorResource`` from the community
``jde_orchestrator_resource`` component so the workspace: block is the same
shape used by every other JDE component in the registry.
"""

import fnmatch
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

# Reuse the community JDE Orchestrator resource so the workspace: block is
# the same shape used by every other JDE component in the registry.
from dagster_community_components.resources.jde_orchestrator_resource.component import (
    JDEOrchestratorResource,
)


@record
class JDEOrchestratorObjectProps:
    """Data passed to translation callables for each imported JDE object.

    Mirrors the shape of ``SnowflakeObjectProps`` / ``MLflowObjectProps`` — a
    single record describing the object so ``translation:`` callables can
    filter, rename, add tags, etc.

    Attributes:
        object_kind: One of ``orchestration`` (default; AIS-registered
            orchestrations), ``report`` (UBE reports exposed via AIS), or
            ``xmlrequest`` (XML request definitions). Additional kinds may
            be surfaced in the future as AIS enumeration endpoints expand.
        object_name: The JDE object's registered name (e.g. an orchestration
            like ``JDE_CustomerMasterExtract``).
        extra: Kind-specific metadata (async_mode flag, environment, etc.).
    """
    object_kind: str
    object_name: str
    extra: Optional[Dict[str, Any]] = None


@dataclass
class OrchestrationSelector(Resolvable):
    """Selector for filtering JDE orchestrations. Same shape as Fivetran's connector_selector."""
    by_name: Optional[List[str]] = None
    by_pattern: Optional[List[str]] = None
    exclude_by_name: Optional[List[str]] = None
    exclude_by_pattern: Optional[List[str]] = None

    def matches(self, name: str) -> bool:
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


def _enumerate_jde(resource: JDEOrchestratorResource) -> dict:
    """Return ``{"orchestrations": [{"name": ...}]}``.

    JDE Tools 9.2.7+ exposes ``GET /jderest/v3/orchestrator`` which lists the
    orchestrations registered on the AIS server. Older releases don't ship
    the listing endpoint — in that case the response is silently swallowed
    and an empty list is returned (customers pin explicit names via the
    selector instead).
    """
    import requests

    session = requests.Session()
    session.verify = resource.verify_ssl
    api_base = resource.api_base
    headers = resource.get_auth_headers()

    out: dict = {"orchestrations": []}
    try:
        r = session.get(f"{api_base}", headers=headers, timeout=30)
        r.raise_for_status()
        body = r.json() or {}
        items = body.get("orchestrations") or body.get("value") or []
    except Exception:  # noqa: BLE001
        return out

    for item in items:
        name = item.get("name") if isinstance(item, dict) else str(item)
        if name:
            out["orchestrations"].append({"name": name})
    return out


@public
class JDEOrchestratorWorkspaceComponent(StateBackedComponent, Model, Resolvable):
    """Component for importing JD Edwards EnterpriseOne Orchestrator
    objects as Dagster assets.

    Supports importing:
    - Orchestrations (AIS-registered composed automation flows)

    Reports (UBE) and XML request definitions are surfaced through the same
    ``object_kind`` axis so future enumeration endpoints can drop in
    without breaking translation callables.

    Example (canonical ``workspace:`` block, mirrors dagster-databricks):
        ```yaml
        type: dagster_community_components.JDEOrchestratorWorkspaceComponent
        attributes:
          workspace:
            base_url: "{{ env.JDE_AIS_URL }}"
            username: "{{ env.JDE_USER }}"
            password: "{{ env.JDE_PASSWORD }}"
            api_path_prefix: /jderest/v3/orchestrator
            verify_ssl: true
          orchestration_selector:
            by_pattern: ["JDE_*"]
            exclude_by_pattern: ["*_deprecated"]
          async_mode: true
          wait_for_completion: true
        ```
    """

    # ── Connection: workspace: block IS a JDEOrchestratorResource ─────
    # Canonical shape — mirrors dagster-databricks / dagster-fivetran /
    # dagster-powerbi workspace components (all have `workspace: <Resource>`).
    workspace: Annotated[
        JDEOrchestratorResource,
        Resolver(
            lambda context, model: JDEOrchestratorResource(
                **resolve_fields(model, JDEOrchestratorResource, context)  # ty: ignore[invalid-argument-type]
            ),
        ),
    ] = Field(
        description=(
            "JDE Orchestrator connection as a JDEOrchestratorResource. "
            "Carries base_url (AIS server), username/password (Basic auth), "
            "api_path_prefix (v3 for JDE Tools 9.2.7+, v2 for older) and "
            "verify_ssl. Secrets typically arrive via `{{ env.XXX }}` Jinja "
            "templating in defs.yaml."
        ),
    )

    # Optional user-side customization hook. Matches the convention used by
    # FivetranAccountComponent / PowerBIWorkspaceComponent / SnowflakeWorkspaceComponent
    # — a callable that takes (base_spec, props) and returns a modified
    # AssetSpec. Applied to each imported JDE object; wired via
    # `JDEOrchestratorComponentTranslator`.
    translation: Annotated[
        Optional[TranslationFn[JDEOrchestratorObjectProps]],
        TranslationFnResolver(template_vars_for_translation_fn=lambda data: {"props": data}),
    ] = Field(
        default=None,
        description=(
            "Function used to translate JDE object properties into "
            "Dagster asset specs. Called for each imported orchestration / "
            "report / xmlrequest. If unset, the base translator's default "
            "AssetSpec is used."
        ),
    )

    orchestration_selector: Optional[OrchestrationSelector] = Field(
        default=None,
        description="Optional inclusion/exclusion filter for orchestration names.",
    )

    group_name: Optional[str] = Field(
        default=None,
        description="Group name for all imported assets.",
    )
    asset_key_prefix: List[str] = Field(
        default_factory=lambda: ["jde", "orchestration"],
        description="Key prefix used for all emitted AssetKeys.",
    )
    compute_kind: str = Field(
        default="jde",
        description="Compute kind tag for all imported assets.",
    )

    async_mode: bool = Field(
        default=False,
        description=(
            "If true, submit orchestrations with ``?asynchronous=true`` so AIS "
            "returns a jobId immediately. Pairs with ``wait_for_completion`` to "
            "poll the status endpoint until terminal."
        ),
    )
    wait_for_completion: bool = Field(
        default=True,
        description=(
            "When ``async_mode`` is true, block on the AIS status endpoint until "
            "the orchestration reaches a terminal state (SUCCESS/COMPLETED/FAILED/"
            "ERROR/CANCELED). Ignored in synchronous mode."
        ),
    )
    poll_interval_seconds: int = Field(
        default=15,
        description="How often (in seconds) to poll the AIS job-status endpoint in async mode.",
    )
    timeout_seconds: int = Field(
        default=1800,
        description=(
            "Overall deadline (seconds) for both the initial POST and the "
            "async job-status polling loop."
        ),
    )

    polling_sensor: bool = Field(
        default=False,
        description=(
            "If true, adds a polling sensor that detects newly completed "
            "AIS orchestration runs and emits AssetObservation events into "
            "Dagster's event log. Matches the ``polling_sensor`` convention "
            "on FivetranAccountComponent / SnowflakeWorkspaceComponent. Off "
            "by default — JDE AIS doesn't push change events, so opt in "
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
    def get_asset_spec(self, props: JDEOrchestratorObjectProps) -> AssetSpec:
        """Generates an AssetSpec for a given JDE Orchestrator object.

        This method can be overridden in a subclass to customize how JDE
        objects are converted to Dagster asset specs. By default, it delegates
        to the configured translator (which respects the ``translation:`` field).

        Args:
            props: The JDEOrchestratorObjectProps carrying object kind, name,
                and any kind-specific metadata.

        Returns:
            An AssetSpec that represents the JDE object as a Dagster asset.

        Example:
            Override this method to add custom tags based on the object kind:

            .. code-block:: python

                from dagster_community_components import JDEOrchestratorWorkspaceComponent

                class CustomJDEOrchestratorWorkspaceComponent(JDEOrchestratorWorkspaceComponent):
                    def get_asset_spec(self, props):
                        base_spec = super().get_asset_spec(props)
                        return base_spec.replace_attributes(
                            tags={
                                **base_spec.tags,
                                "jde_object_kind": props.object_kind,
                            }
                        )
        """
        return self._base_translator.get_asset_spec(props)

    @property
    def _base_translator(self) -> "JDEOrchestratorComponentTranslator":
        # Cached lazily so subclasses can still override get_asset_spec cleanly.
        cached = getattr(self, "__base_translator_cached", None)
        if cached is None:
            cached = JDEOrchestratorComponentTranslator(self)
            object.__setattr__(self, "__base_translator_cached", cached)
        return cached

    @property
    def defs_state_config(self) -> DefsStateConfig:
        # Key on AIS hostname so multiple JDE servers don't collide in the
        # shared local-filesystem state dir. Hashed to keep the key
        # filesystem-safe.
        host_hash = hashlib.sha256(self.workspace.base_url.encode()).hexdigest()[:12]
        default_key = f"{self.__class__.__name__}[{host_hash}]"
        return DefsStateConfig.from_args(self.defs_state, default_key=default_key)

    def _apply_translation(
        self,
        kwargs: Dict[str, Any],
        kind: str,
        name: str,
        extra: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Fold the translation callable into per-asset kwargs.

        Builds a ``JDEOrchestratorObjectProps`` and calls
        ``self.get_asset_spec(props)``, which delegates to
        ``JDEOrchestratorComponentTranslator`` (base spec + optional user
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

        props = JDEOrchestratorObjectProps(
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
        """Enumerate JDE orchestrations via AIS and cache them.

        Runs the same ``GET /jderest/v3/orchestrator`` (or ``v2``) discovery
        the previous inline enumeration used, applies the
        ``orchestration_selector`` filter, and writes the surviving rows to
        ``state_path`` as a JSON dict keyed by object kind.
        ``build_defs_from_state`` re-hydrates from this snapshot so no AIS
        HTTP calls fire at Dagster load time.
        """
        snapshot = _enumerate_jde(self.workspace)
        if self.orchestration_selector is not None:
            snapshot["orchestrations"] = [
                o for o in snapshot["orchestrations"]
                if self.orchestration_selector.matches(o["name"])
            ]
        state_path.write_text(json.dumps(snapshot, indent=2))

    def build_defs_from_state(
        self,
        context: ComponentLoadContext,
        state_path: Optional[Path],
    ) -> Definitions:
        """Build Dagster definitions from cached JDE workspace state.

        Reads the JSON dict written by ``write_state_to_path`` and turns each
        orchestration entry into a materializable ``@asset``. Runtime AIS
        calls (POST-ing the orchestration, optional async status polling)
        still fire on each materialization — only the discovery moved to
        state.
        """
        if state_path is None or not state_path.exists():
            return Definitions()
        state = json.loads(state_path.read_text())
        assets = []
        for o in state.get("orchestrations", []):
            assets.append(self._build_orchestration_asset(o["name"]))
        return Definitions(assets=assets)

    def _build_orchestration_asset(self, orchestration: str):
        _self = self
        key = AssetKey([*self.asset_key_prefix, orchestration])

        base_kwargs: Dict[str, Any] = dict(
            key=key,
            group_name=self.group_name,
            compute_kind=self.compute_kind,
            metadata={"jde_orchestration": dg.MetadataValue.text(orchestration)},
        )
        asset_kwargs = self._apply_translation(
            base_kwargs,
            kind="orchestration",
            name=orchestration,
            extra={"async_mode": self.async_mode},
        )

        @dg.asset(**asset_kwargs)
        def _asset(context: dg.AssetExecutionContext):
            import time
            try:
                import requests
            except ImportError as e:
                raise Exception("requests library not installed") from e

            resource = _self.workspace
            session = requests.Session()
            session.verify = resource.verify_ssl
            headers = resource.get_auth_headers()

            url = resource.orchestration_url(orchestration)
            if _self.async_mode:
                url = f"{url}?asynchronous=true"

            r = session.post(url, json={}, headers=headers, timeout=_self.timeout_seconds)
            if r.status_code >= 400:
                raise Exception(
                    f"JDE orchestration failed: {r.status_code} {r.text[:200]} ({orchestration})"
                )
            context.log.info(f"JDE: {orchestration} submitted (status={r.status_code})")

            if not (_self.async_mode and _self.wait_for_completion):
                return

            try:
                job_id = (r.json() or {}).get("jobId")
            except ValueError:
                job_id = None
            if not job_id:
                return

            deadline = time.time() + _self.timeout_seconds
            terminal = {"SUCCESS", "COMPLETED", "FAILED", "ERROR", "CANCELED"}
            while time.time() < deadline:
                time.sleep(_self.poll_interval_seconds)
                sr = session.get(resource.status_url(str(job_id)), headers=headers, timeout=30)
                if sr.status_code >= 300:
                    continue
                state = ((sr.json() or {}).get("status") or "").upper()
                if state in terminal:
                    if state in ("FAILED", "ERROR", "CANCELED"):
                        raise Exception(
                            f"Orchestration ended in {state} ({orchestration})"
                        )
                    context.add_output_metadata({"final_state": state})
                    return
            raise Exception(
                f"Orchestration did not reach terminal state within "
                f"{_self.timeout_seconds}s ({orchestration})"
            )

        return _asset


class DagsterJDEOrchestratorTranslator:
    """Base translator for JDE Orchestrator workspace objects → AssetSpec.

    Follows the shape of ``DagsterFivetranTranslator`` /
    ``DagsterPowerBITranslator`` / ``DagsterSnowflakeTranslator``. Subclass
    this and override ``get_asset_spec()`` to fully customize how JDE
    objects become Dagster assets — an alternative to the runtime
    ``translation:`` callable on the component.
    """

    def get_asset_spec(self, props: JDEOrchestratorObjectProps) -> AssetSpec:
        """Default AssetSpec for a JDE Orchestrator object.

        Key = ["jde", <object_kind>, <object_name.lower()>] (lowercased for
        consistency with the rest of the Dagster catalog). Kinds carry both
        ``jde_orchestrator`` and the specific object kind. Metadata carries
        the object kind + name.
        """
        return AssetSpec(
            key=AssetKey(["jde", props.object_kind, props.object_name.lower()]),
            kinds={"jde_orchestrator", props.object_kind},
            metadata={
                "jde/object_kind": props.object_kind,
                "jde/object_name": props.object_name,
            },
        )


class JDEOrchestratorComponentTranslator(
    create_component_translator_cls(JDEOrchestratorWorkspaceComponent, DagsterJDEOrchestratorTranslator),  # ty: ignore[unsupported-base]
    ComponentTranslator[JDEOrchestratorWorkspaceComponent],
):
    """Bridges ``JDEOrchestratorWorkspaceComponent.translation`` (runtime
    callable) with the base ``DagsterJDEOrchestratorTranslator`` (class-level
    override).

    Mirrors ``FivetranComponentTranslator`` / ``PowerBIComponentTranslator``
    / ``SnowflakeComponentTranslator`` / ``MLflowComponentTranslator``.
    """

    def __init__(self, component: "JDEOrchestratorWorkspaceComponent"):
        self._component = component

    def get_asset_spec(self, props: JDEOrchestratorObjectProps) -> AssetSpec:
        base_asset_spec = super().get_asset_spec(props)
        if self.component.translation is None:
            return base_asset_spec
        return self.component.translation(base_asset_spec, props)
