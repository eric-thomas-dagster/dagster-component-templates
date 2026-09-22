"""Prefect Workspace Component.

Auto-enumerates Prefect deployments via the Prefect API and emits one
triggerable Dagster asset per deployment. This exists because prefect_flow_run
requires one hand-written YAML block per deployment — a real tax for "I just
want Dagster to see everything already deployed in my Prefect instance." A
workspace-style discovery component removes that tax the same way
mlflow_workspace / snowflake_workspace do for their own targets.

Every discovered deployment gets prefect_flow_run's FULL richness by
default: forward_termination, stream_logs, stream_artifacts all apply
uniformly (they're generic — no per-flow convention needed). check_names and
Dagster-side scheduling do NOT apply globally, on purpose — both are opt-in
PER DEPLOYMENT via `assets_by_name`, not a single setting shared across every
discovered deployment:

- check_names: a global check name would be required on every deployment,
  most of which won't implement that exact artifact convention — declared
  checks that never get a result crash the whole step (see check_names'
  field docs). `assets_by_name.<flow>/<deployment>.check_names` scopes it to
  one deployment at a time.
- Scheduling (`auto_schedule` + `assets_by_name.<name>.schedule: true`):
  deliberately NOT inferred from anything about the deployment's own Prefect
  schedule state (e.g. whether it happens to be paused) — that's a side
  signal someone could flip for an unrelated reason and silently start
  Dagster firing on a schedule nobody asked it to own. Scheduling must be
  named explicitly, per deployment, every time. The one thing Prefect's own
  paused state IS still used for: a safety check that raises if you opt a
  deployment in while its Prefect-side cron is still active (both would
  fire it on the same tick) — see auto_schedule's field docs.

Follows the canonical `workspace: <Resource>` + StateBackedComponent
pattern used by MLflowWorkspaceComponent / SnowflakeWorkspaceComponent:
- `@public` class
- `translation:` callable field
- `@public get_asset_spec(props)` hook
- `assets_by_name` per-deployment override dict, mirroring
  SnowflakeWorkspaceComponent.assets_by_name / the official
  dagster-databricks `assets_by_task_key` pattern
- `polling_sensor` (alias `generate_sensor`) opt-in, cross-deployment
- `defs_state` + `defs_state_config` property
- `StateBackedComponent` inheritance with `write_state_to_path` +
  `build_defs_from_state`
- `PrefectWorkspaceObjectProps` @record + `DagsterPrefectWorkspaceTranslator`
  + `PrefectWorkspaceComponentTranslator`

Scheduling: only CronSchedule can be mirrored into a Dagster
ScheduleDefinition. IntervalSchedule / RRuleSchedule deployments are still
discovered and get a triggerable asset — they just can't be opted into
auto_schedule, since Dagster's ScheduleDefinition is cron-shaped and
converting interval/RRule cleanly isn't a small problem — trigger those
manually, via a sensor, or via `prefect_flow_run_sensor` in the meantime.
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
from pydantic import ConfigDict, Field


@record
class PrefectWorkspaceObjectProps:
    """Data passed to translation callables for each imported deployment.

    Mirrors MLflowObjectProps / SnowflakeObjectProps' shape.

    Attributes:
        object_kind: Always 'deployment' (the only kind this component
            discovers today — see module docstring on scope).
        object_name: 'flow_name/deployment_name', Prefect's own addressing
            convention (matches prefect_flow_run's deployment_name field).
        extra: flow_name, deployment_name, work_pool_name, tags, paused.
    """
    object_kind: str
    object_name: str
    extra: Optional[Dict[str, Any]] = None


class PrefectWorkspaceResource(dg.ConfigurableResource):
    """Prefect workspace connection.

    Self-contained (duplicated, not imported from resources/prefect_resource)
    per this registry's convention — see FIELD_CONVENTIONS.md on
    `_build_partitions_def`. Mirrors dagster_databricks.DatabricksWorkspace /
    dagster_fivetran.FivetranWorkspace's `workspace:` connection-only shape.
    """

    api_url: str = Field(
        default="http://127.0.0.1:4200/api",
        description="Prefect API URL. Default is local server at :4200.",
    )
    api_key_env_var: Optional[str] = Field(
        default=None,
        description="Env var holding a Prefect Cloud API key. Leave unset for local server.",
    )
    ui_url: Optional[str] = Field(
        default=None,
        description=(
            "Base URL of the Prefect UI, for the 'Prefect Run URL' metadata link. "
            "Defaults to api_url with its trailing '/api' stripped. Prefect Cloud "
            "needs this set explicitly."
        ),
    )


@dataclass
class PrefectDeploymentSelector(dg.Resolvable):
    """Inclusion/exclusion filter on 'flow_name/deployment_name' strings.
    Same shape as MLflowSelector / SnowflakeWorkspaceComponent's selectors."""
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


async def _enumerate_prefect_deployments(api_url: str, api_key: Optional[str]) -> List[Dict[str, Any]]:
    """Enumerate Prefect deployments + resolve each one's flow name.

    Verified fields against a live server: DeploymentResponse has `name`,
    `flow_id` (NOT flow_name — resolved separately per unique flow_id below),
    `schedules` (list of DeploymentSchedule, each `.schedule` a
    CronSchedule/IntervalSchedule/RRuleSchedule union), `parameters`, `tags`,
    `paused`, `work_pool_name`.
    """
    from prefect.client.orchestration import get_client
    from prefect.client.schemas.schedules import CronSchedule

    async with get_client() as client:
        # 200 is the server's actual enforced max (verified: limit=500 gets a
        # 422 "must be less than or equal to 200") — paginate via offset for
        # instances with more deployments than that.
        deployments: List[Any] = []
        offset = 0
        while True:
            page = await client.read_deployments(limit=200, offset=offset)
            deployments.extend(page)
            if len(page) < 200:
                break
            offset += 200

        flow_name_by_id: Dict[str, str] = {}
        for dep in deployments:
            fid = str(dep.flow_id)
            if fid not in flow_name_by_id:
                flow = await client.read_flow(dep.flow_id)
                flow_name_by_id[fid] = flow.name

        out: List[Dict[str, Any]] = []
        for dep in deployments:
            flow_name = flow_name_by_id[str(dep.flow_id)]
            # Track cron presence separately from whether it's still ACTIVE in
            # Prefect. auto_schedule only mirrors a deployment's schedule into
            # Dagster once every one of its cron schedules is paused
            # (ds.active=False) on the Prefect side — that pause is the
            # explicit, per-deployment signal that Dagster should take over
            # triggering it, not something this component decides unilaterally
            # or applies as a blanket default. A deployment with any cron
            # schedule still active in Prefect is left alone entirely, so
            # enabling auto_schedule can never cause a flow to fire twice on
            # the same tick (once from Prefect's own scheduler, once from
            # Dagster's mirrored schedule).
            cron: Optional[str] = None
            cron_timezone = "UTC"
            any_active = False
            for ds in dep.schedules or []:
                if isinstance(ds.schedule, CronSchedule):
                    if cron is None:
                        cron = ds.schedule.cron
                        cron_timezone = ds.schedule.timezone or "UTC"
                    if ds.active:
                        any_active = True
            schedule_paused = cron is not None and not any_active
            out.append({
                "deployment_id": str(dep.id),
                "flow_name": flow_name,
                "deployment_name": dep.name,
                "full_name": f"{flow_name}/{dep.name}",
                "cron": cron,
                "cron_timezone": cron_timezone,
                "schedule_paused": schedule_paused,
                "parameters": dict(dep.parameters or {}),
                "tags": list(dep.tags or []),
                "work_pool_name": dep.work_pool_name,
                "paused": bool(dep.paused),
            })
        return out


@public
class PrefectWorkspaceComponent(StateBackedComponent, Model, Resolvable):
    """Auto-discover Prefect deployments and emit one triggerable Dagster
    asset per deployment — with prefect_flow_run's full observability
    (forward_termination, stream_logs, stream_artifacts) applied uniformly.
    check_names and Dagster-side scheduling are opt-in per deployment via
    assets_by_name — see the module docstring for why those two can't be
    global settings the way the others are.

    Example (local Prefect server, all deployments, full observability):

        ```yaml
        type: dagster_community_components.PrefectWorkspaceComponent
        attributes:
          workspace:
            api_url: http://127.0.0.1:4200/api
          stream_logs: true
          stream_artifacts: true
        ```

    Example (Prefect Cloud, only production deployments, cross-deployment
    completions sensor for anything triggered outside Dagster):

        ```yaml
        type: dagster_community_components.PrefectWorkspaceComponent
        attributes:
          workspace:
            api_url: https://api.prefect.cloud/api/accounts/<acct>/workspaces/<ws>
            api_key_env_var: PREFECT_API_KEY
          deployment_selector:
            by_pattern: ["*/production"]
          polling_sensor: true
        ```

    Example (one deployment opted into a check + Dagster-side scheduling —
    both require naming the deployment explicitly in assets_by_name; the
    schedule opt-in also requires pausing "nightly-report/prod"'s own cron
    schedule in Prefect first, or this raises at build time):

        ```yaml
        type: dagster_community_components.PrefectWorkspaceComponent
        attributes:
          workspace:
            api_url: http://127.0.0.1:4200/api
          stream_artifacts: true
          auto_schedule: true
          assets_by_name:
            nightly-report/prod:
              check_names: [row_count_check]  # matches artifact key "row-count-check"
              schedule: true
        ```
    """

    model_config = ConfigDict(populate_by_name=True)

    workspace: Annotated[
        PrefectWorkspaceResource,
        Resolver(
            lambda context, model: PrefectWorkspaceResource(
                **resolve_fields(model, PrefectWorkspaceResource, context)  # ty: ignore[invalid-argument-type]
            ),
        ),
    ] = Field(
        description=(
            "Prefect connection as a PrefectWorkspaceResource (api_url + optional "
            "api_key_env_var + ui_url)."
        ),
    )

    translation: Annotated[
        Optional[TranslationFn[PrefectWorkspaceObjectProps]],
        TranslationFnResolver(template_vars_for_translation_fn=lambda data: {"props": data}),
    ] = Field(
        default=None,
        description=(
            "Function used to translate deployment properties into Dagster asset "
            "specs. Called for each discovered deployment. If unset, the base "
            "translator's default AssetSpec is used."
        ),
    )

    deployment_selector: Optional[PrefectDeploymentSelector] = Field(
        default=None,
        description="Optional inclusion/exclusion filter on 'flow_name/deployment_name' strings.",
    )

    # Per-deployment trigger behavior — applies uniformly across every
    # discovered deployment.
    wait_for_result: bool = Field(
        default=True,
        description="Wait for each triggered flow run to reach a terminal state before materializing.",
    )
    timeout_seconds: Optional[int] = Field(default=None)
    poll_interval_seconds: float = Field(default=5.0)
    fail_on_flow_run_failure: bool = Field(default=True)
    forward_termination: bool = Field(
        default=True,
        description=(
            "Cancel the Prefect flow run if the Dagster run is terminated while "
            "waiting — same mechanism as prefect_flow_run.forward_termination."
        ),
    )
    stream_logs: bool = Field(
        default=False,
        description=(
            "Forward each deployment's own Prefect logs into the Dagster run log "
            "while waiting, via read_logs — no shared filesystem or blob store "
            "required. Applies uniformly to every discovered deployment. Off by "
            "default. Same mechanism as prefect_flow_run.stream_logs."
        ),
    )
    stream_artifacts: bool = Field(
        default=False,
        description=(
            "Forward Prefect artifacts each deployment's flow creates as "
            "AssetObservation events. Applies uniformly to every discovered "
            "deployment. Off by default. Required (validated at build time) if "
            "any assets_by_name entry sets check_names. Same mechanism as "
            "prefect_flow_run.stream_artifacts."
        ),
    )

    assets_by_name: Optional[Dict[str, Dict[str, Any]]] = Field(
        default=None,
        description=(
            "Per-deployment overrides, keyed by 'flow_name/deployment_name' "
            "(same string deployment_selector matches against). Mirrors "
            "SnowflakeWorkspaceComponent.assets_by_name / the official "
            "dagster-databricks assets_by_task_key pattern — nothing here is "
            "applied globally across deployments; every key below is opt-in, "
            "per deployment, by name.\n\n"
            "Standard @asset-kwarg overrides (all optional): key, group_name, "
            "description, deps, metadata, tags, kinds, owners — same merge "
            "semantics as SnowflakeWorkspaceComponent.assets_by_name.\n\n"
            "check_names (List[str]): declares AssetCheckSpecs for THIS ONE "
            "deployment only — avoids the footgun of a single global check "
            "name being required on every discovered deployment, most of which "
            "won't implement that exact check. Requires stream_artifacts=True "
            "globally (validated at build time). Same convention as "
            "prefect_flow_run.check_names, including the dash/underscore "
            "translation between Prefect artifact keys and Dagster check "
            "names.\n\n"
            "schedule (bool): explicit, per-deployment opt-in for auto_schedule "
            "— see that field's docs for why this can't be inferred from "
            "Prefect's own schedule state."
        ),
    )

    auto_schedule: bool = Field(
        default=False,
        description=(
            "Master switch for Dagster-side scheduling — even when True, a "
            "deployment only gets a Dagster ScheduleDefinition if it's ALSO "
            "explicitly named in assets_by_name.<flow>/<deployment>.schedule: "
            "true. Deliberately NOT inferred from whether the deployment's own "
            "Prefect schedule happens to be paused — that's a side signal "
            "someone could flip for an unrelated reason (debugging, "
            "maintenance) and silently start Dagster firing on a schedule "
            "nobody asked it to own. Scheduling must be named explicitly, "
            "every time.\n\n"
            "Safety check (still enforced, just not the trigger): if a "
            "deployment IS explicitly opted in but its own Prefect cron "
            "schedule is still active (not paused), build_defs_from_state "
            "raises rather than silently letting Prefect and Dagster both "
            "fire it on the same tick — pause the Prefect-side schedule first.\n\n"
            "IntervalSchedule/RRuleSchedule deployments always get a "
            "triggerable asset, just never an automatic Dagster schedule "
            "(Dagster's ScheduleDefinition is cron-shaped)."
        ),
    )

    group_name: Optional[str] = Field(default=None, description="Group name for all imported assets.")
    asset_key_prefix: List[str] = Field(
        default_factory=lambda: ["prefect"],
        description="Key prefix used for all emitted AssetKeys.",
    )
    compute_kind: str = Field(default="prefect", description="Compute kind tag for all imported assets.")

    poll_interval_seconds_sensor: int = Field(
        default=60,
        description="Minimum seconds between polling-sensor evaluations. Only consulted when polling_sensor: true.",
    )
    polling_sensor: bool = Field(
        default=False,
        description=(
            "If true, adds a sensor watching ALL discovered deployments for flow "
            "runs entering a terminal state and emits an AssetObservation on the "
            "matching deployment's asset. Use when some deployments are triggered "
            "outside Dagster (Prefect's own schedule, other application code) and "
            "you want the catalog to reflect those runs too — the auto-discovery "
            "equivalent of hand-configuring prefect_flow_run_sensor per "
            "deployment. Off by default."
        ),
        alias="generate_sensor",
    )

    defs_state: ResolvedDefsStateConfig = Field(
        default_factory=DefsStateConfigArgs.local_filesystem,
        description="State backend for cached workspace discovery. Local filesystem by default.",
    )

    @public
    def get_asset_spec(self, props: PrefectWorkspaceObjectProps) -> AssetSpec:
        """Generates an AssetSpec for a given Prefect deployment. Override in
        a subclass to customize; see MLflowWorkspaceComponent.get_asset_spec
        for the override pattern this mirrors."""
        return self._base_translator.get_asset_spec(props)

    @property
    def _base_translator(self) -> "PrefectWorkspaceComponentTranslator":
        cached = getattr(self, "__base_translator_cached", None)
        if cached is None:
            cached = PrefectWorkspaceComponentTranslator(self)
            object.__setattr__(self, "__base_translator_cached", cached)
        return cached

    @property
    def defs_state_config(self) -> DefsStateConfig:
        # Key on api_url so multiple Prefect instances don't collide in the
        # shared local-filesystem state dir — same convention as
        # MLflowWorkspaceComponent keying on tracking_uri.
        uri_hash = hashlib.sha256(self.workspace.api_url.encode()).hexdigest()[:12]
        default_key = f"{self.__class__.__name__}[{uri_hash}]"
        return DefsStateConfig.from_args(self.defs_state, default_key=default_key)

    def _apply_translation(
        self, kwargs: Dict[str, Any], name: str, extra: Dict[str, Any],
    ) -> Dict[str, Any]:
        if self.translation is None:
            return kwargs
        props = PrefectWorkspaceObjectProps(object_kind="deployment", object_name=name, extra=extra)
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

    def _apply_asset_overrides(self, full_name: str, base_kwargs: Dict[str, Any]) -> Dict[str, Any]:
        """Merge per-deployment overrides from `assets_by_name` into @asset
        kwargs. Identical merge semantics to
        SnowflakeWorkspaceComponent._apply_asset_overrides — applied AFTER
        _apply_translation, so an explicit assets_by_name entry wins over
        whatever the translation callable computed. Only the standard
        @asset-kwarg keys are handled here; check_names/schedule are consumed
        separately by the caller since they aren't @asset kwargs."""
        if not self.assets_by_name:
            return base_kwargs
        override = self.assets_by_name.get(full_name)
        if not override:
            return base_kwargs
        result = dict(base_kwargs)
        if "key" in override:
            result.pop("name", None)
            result["key"] = AssetKey.from_user_string(str(override["key"]))
        if "group_name" in override:
            result["group_name"] = override["group_name"]
        if "description" in override:
            result["description"] = override["description"]
        if "deps" in override:
            existing = list(result.get("deps") or [])
            existing.extend(AssetKey.from_user_string(d) for d in override["deps"])
            result["deps"] = existing
        if "metadata" in override:
            existing = dict(result.get("metadata") or {})
            existing.update(override["metadata"])
            result["metadata"] = existing
        if "tags" in override:
            existing = dict(result.get("tags") or {})
            existing.update(override["tags"])
            result["tags"] = existing
        if "kinds" in override:
            result["kinds"] = set(override["kinds"])
        if "owners" in override:
            result["owners"] = override["owners"]
        return result

    async def write_state_to_path(self, state_path: Path) -> None:
        """Enumerate Prefect deployments and cache them. build_defs_from_state
        re-hydrates from this snapshot so no Prefect API calls fire at
        Dagster defs-load time."""
        api_key = None
        if self.workspace.api_key_env_var:
            import os
            api_key = os.environ.get(self.workspace.api_key_env_var)

        deployments = await _enumerate_prefect_deployments(self.workspace.api_url, api_key)
        if self.deployment_selector is not None:
            deployments = [d for d in deployments if self.deployment_selector.matches(d["full_name"])]
        state_path.write_text(json.dumps(deployments, indent=2))

    def build_defs_from_state(
        self, context: ComponentLoadContext, state_path: Optional[Path],
    ) -> Definitions:
        if state_path is None or not state_path.exists():
            return Definitions()
        deployments = json.loads(state_path.read_text())
        overrides = self.assets_by_name or {}

        # check_names requires stream_artifacts globally — that's the only
        # way a check result is ever collected. Validated once, up front,
        # rather than per-deployment, since it's a global field either way.
        if any(o.get("check_names") for o in overrides.values()) and not self.stream_artifacts:
            raise ValueError(
                "PrefectWorkspaceComponent: an assets_by_name entry sets "
                "check_names, which requires stream_artifacts=True — that's "
                "the only way a check result is ever collected."
            )

        assets = []
        schedules = []
        deployment_asset_keys: List[tuple] = []
        for d in deployments:
            override = overrides.get(d["full_name"], {})
            asset_def, asset_key = self._build_deployment_asset(d, override)
            assets.append(asset_def)
            deployment_asset_keys.append((d["deployment_id"], asset_key))

            if self.auto_schedule and override.get("schedule"):
                if not d["cron"]:
                    raise ValueError(
                        f"PrefectWorkspaceComponent: {d['full_name']!r} is opted into "
                        f"auto_schedule via assets_by_name but has no cron schedule in "
                        f"Prefect to mirror (IntervalSchedule/RRuleSchedule aren't "
                        f"supported for this)."
                    )
                if not d.get("schedule_paused"):
                    raise ValueError(
                        f"PrefectWorkspaceComponent: {d['full_name']!r} is opted into "
                        f"auto_schedule via assets_by_name, but its own Prefect cron "
                        f"schedule is still active — both would fire it on the same "
                        f"tick. Pause the schedule in Prefect (UI, CLI, or API) first."
                    )
                schedules.append(dg.ScheduleDefinition(
                    name=f"{self.compute_kind}_{_safe(d['full_name'])}_schedule",
                    cron_schedule=d["cron"],
                    execution_timezone=d["cron_timezone"],
                    target=asset_def,  # AssetsDefinition, not a bare AssetKey — see below
                ))

        sensors = []
        if self.polling_sensor and deployment_asset_keys:
            sensors.append(self._build_completions_polling_sensor(deployment_asset_keys))

        return Definitions(
            assets=assets,
            schedules=schedules if schedules else None,
            sensors=sensors if sensors else None,
        )

    def _build_deployment_asset(self, d: Dict[str, Any], override: Dict[str, Any]):
        _self = self
        full_name = d["full_name"]
        safe = _safe(full_name)
        key = AssetKey([*self.asset_key_prefix, safe])

        tags = {f"dagster/kind/{self.compute_kind}": ""}
        for t in d.get("tags") or []:
            tags[f"prefect_tag_{t}"] = ""

        base_kwargs: Dict[str, Any] = dict(
            key=key,
            group_name=self.group_name,
            tags=tags,
            description=f"Prefect deployment {full_name} (auto-discovered)",
            metadata={
                "prefect_flow_name": dg.MetadataValue.text(d["flow_name"]),
                "prefect_deployment_name": dg.MetadataValue.text(d["deployment_name"]),
                "prefect_work_pool": dg.MetadataValue.text(d.get("work_pool_name") or ""),
                "prefect_paused": dg.MetadataValue.bool(d.get("paused", False)),
            },
        )
        asset_kwargs = self._apply_translation(
            base_kwargs, name=full_name,
            extra={
                "flow_name": d["flow_name"], "deployment_name": d["deployment_name"],
                "work_pool_name": d.get("work_pool_name"), "tags": d.get("tags"),
                "paused": d.get("paused"),
            },
        )
        # assets_by_name wins over the translation callable's computed defaults.
        asset_kwargs = self._apply_asset_overrides(full_name, asset_kwargs)
        key = asset_kwargs["key"]  # may have been renamed by an override

        check_names_list: List[str] = list(override.get("check_names") or [])
        check_names_set = set(check_names_list)
        check_specs = (
            [dg.AssetCheckSpec(name=n, asset=key) for n in check_names_list]
            if check_names_list else None
        )
        asset_kwargs["check_specs"] = check_specs

        @dg.asset(**asset_kwargs)
        def _asset(context: dg.AssetExecutionContext):
            # No return-type annotation, and this is a generator (yield
            # below): identical reasoning to prefect_flow_run's asset
            # function — Dagster requires check results to be yielded from
            # the function's own body when check_specs is declared, and
            # yielding is harmless when check_specs is None (the common case
            # for this component), so the shape is unconditional.
            _apply_prefect_env(_self.workspace)
            from prefect.deployments import run_deployment

            context.log.info(
                f"[prefect_workspace] Triggering {full_name} "
                f"(wait_for_result={_self.wait_for_result})"
            )
            flow_run = run_deployment(name=full_name, timeout=0, poll_interval=_self.poll_interval_seconds)

            collected_checks: List[Any] = []
            if _self.wait_for_result:
                flow_run, collected_checks = _poll_flow_run_until_terminal(
                    context=context,
                    asset_key=key,
                    flow_run_id=flow_run.id,
                    timeout_seconds=_self.timeout_seconds,
                    poll_interval_seconds=_self.poll_interval_seconds,
                    forward_termination=_self.forward_termination,
                    stream_logs=_self.stream_logs,
                    stream_artifacts=_self.stream_artifacts,
                    check_names=check_names_set,
                )

            reported_names = {c.check_name for c in collected_checks}
            for missing_name in check_names_set - reported_names:
                # An unreported declared check crashes the whole step
                # (DagsterStepOutputNotFoundError) otherwise — see
                # prefect_flow_run's identical fallback.
                collected_checks.append(dg.AssetCheckResult(
                    check_name=missing_name,
                    passed=False,
                    description=(
                        f"No Prefect table artifact with key={missing_name!r} and "
                        f"data containing 'passed' was reported for this flow run."
                    ),
                ))
            yield from collected_checks

            state = flow_run.state
            state_name = getattr(state, "name", "unknown") if state else "unknown"
            state_type = getattr(getattr(state, "type", None), "value", "unknown")
            terminal_failure = state_type in {"FAILED", "CRASHED", "CANCELLED"}

            context.add_output_metadata({
                "flow_run_id": dg.MetadataValue.text(str(flow_run.id)),
                "state_name": dg.MetadataValue.text(state_name),
                "state_type": dg.MetadataValue.text(state_type),
                "prefect_run_url": dg.MetadataValue.url(
                    _prefect_run_url(_self.workspace.api_url, _self.workspace.ui_url, flow_run.id)
                ),
            }, output_name="result")

            if _self.wait_for_result and _self.fail_on_flow_run_failure and terminal_failure:
                raise dg.Failure(description=f"Prefect flow run ended in {state_name} state")

            yield dg.Output({"flow_run_id": str(flow_run.id), "state_type": state_type})

        return _asset, key

    def _build_completions_polling_sensor(self, deployment_asset_keys: List[tuple]):
        """Sensor watching ALL discovered deployments for terminal-state flow
        runs and emitting an AssetObservation on the matching asset — the
        auto-discovery equivalent of hand-configuring prefect_flow_run_sensor
        once per deployment. Cursor: latest end_time seen, same shape as
        prefect_flow_run_sensor's own cursor.

        Matches on `FlowRun.deployment_id` (a UUID) — verified this is the
        only deployment linkage FlowRun carries; there's no inline
        flow_name/deployment_name string on the run itself, so the state
        snapshot stores deployment_id specifically so this sensor can key
        off it directly instead of re-deriving names.
        """
        _self = self
        asset_key_by_deployment_id = dict(deployment_asset_keys)

        @dg.sensor(
            name=f"{self.compute_kind}_workspace_completions_sensor",
            minimum_interval_seconds=self.poll_interval_seconds_sensor,
            default_status=dg.DefaultSensorStatus.STOPPED,
        )
        def _prefect_workspace_completions_sensor(context: dg.SensorEvaluationContext):
            import asyncio
            from datetime import datetime, timedelta, timezone

            from prefect.client.orchestration import get_client
            from prefect.client.schemas.filters import (
                FlowRunFilter, FlowRunFilterEndTime, FlowRunFilterState, FlowRunFilterStateType,
            )
            from prefect.client.schemas.objects import StateType

            _apply_prefect_env(_self.workspace)

            if context.cursor:
                try:
                    cursor_dt = datetime.fromisoformat(context.cursor)
                except Exception:
                    cursor_dt = datetime.now(timezone.utc) - timedelta(minutes=60)
            else:
                cursor_dt = datetime.now(timezone.utc) - timedelta(minutes=60)

            async def _fetch():
                async with get_client() as client:
                    return await client.read_flow_runs(
                        flow_run_filter=FlowRunFilter(
                            state=FlowRunFilterState(
                                type=FlowRunFilterStateType(any_=[
                                    StateType.COMPLETED, StateType.FAILED,
                                    StateType.CRASHED, StateType.CANCELLED,
                                ]),
                            ),
                            end_time=FlowRunFilterEndTime(after_=cursor_dt),
                        ),
                        # END_TIME_ASC is not a valid sort value on the real
                        # server (verified: 422 "Input should be 'ID_DESC',
                        # ... 'END_TIME_DESC'" — no ASC variant exists for
                        # end_time). Sort DESC instead; latest_end below is
                        # computed via max() so it doesn't depend on order.
                        limit=200, sort="END_TIME_DESC",
                    )

            flow_runs = asyncio.run(_fetch())
            if not flow_runs:
                return dg.SensorResult(skip_reason="no new terminal flow runs")

            observations = []
            latest_end = cursor_dt
            for fr in flow_runs:
                if fr.end_time and fr.end_time > latest_end:
                    latest_end = fr.end_time
                if fr.deployment_id is None:
                    continue  # ad-hoc flow run with no deployment — nothing to key off
                asset_key = asset_key_by_deployment_id.get(str(fr.deployment_id))
                if asset_key is None:
                    continue  # deployment not in this component's discovered set
                observations.append(dg.AssetObservation(
                    asset_key=asset_key,
                    metadata={
                        "flow_run_id": dg.MetadataValue.text(str(fr.id)),
                        "state_type": dg.MetadataValue.text(
                            fr.state_type.value if fr.state_type else ""
                        ),
                    },
                ))

            if not observations:
                return dg.SensorResult(cursor=latest_end.isoformat(), skip_reason="no matching discovered deployments")
            return dg.SensorResult(asset_events=observations, cursor=latest_end.isoformat())

        return _prefect_workspace_completions_sensor


def _safe(name: str) -> str:
    return "".join(c if c.isalnum() or c == "_" else "_" for c in name)[:60] or "deployment"


def _apply_prefect_env(workspace: PrefectWorkspaceResource) -> None:
    import os
    os.environ["PREFECT_API_URL"] = workspace.api_url
    if workspace.api_key_env_var:
        key = os.environ.get(workspace.api_key_env_var)
        if key:
            os.environ["PREFECT_API_KEY"] = key


def _prefect_run_url(api_url: str, ui_url: Optional[str], flow_run_id: Any) -> str:
    base = (ui_url or api_url).rstrip("/").removesuffix("/api")
    return f"{base}/runs/flow-run/{flow_run_id}"


def _forward_prefect_log(dagster_log: Any, entry: Any) -> None:
    """Re-emit one Prefect `Log` record as a Dagster run-log line — identical,
    independently-tested logic to prefect_flow_run.component's version."""
    level = entry.level or 20
    prefix = f"[prefect:{entry.name}]" if getattr(entry, "name", None) else "[prefect]"
    message = f"{prefix} {entry.message}"
    if level >= 50:
        dagster_log.critical(message)
    elif level >= 40:
        dagster_log.error(message)
    elif level >= 30:
        dagster_log.warning(message)
    elif level >= 20:
        dagster_log.info(message)
    else:
        dagster_log.debug(message)


def _parse_table_artifact_data(data: Any) -> Any:
    """Prefect table artifacts store `data` as a JSON-encoded STRING (verified
    against a live server) — identical logic to prefect_flow_run.component's
    version."""
    if isinstance(data, str):
        try:
            return json.loads(data)
        except (TypeError, ValueError):
            return data
    return data


def _first_table_row(parsed: Any) -> Dict[str, Any]:
    """Unwrap a Prefect table artifact's first row — identical logic to
    prefect_flow_run.component's version."""
    if isinstance(parsed, list) and parsed and isinstance(parsed[0], dict):
        return parsed[0]
    if isinstance(parsed, dict):
        return parsed
    return {}


def _artifact_metadata_value(artifact: Any) -> Any:
    """Map a Prefect Artifact's native `type` onto the matching Dagster
    MetadataValue — identical, independently-tested logic to
    prefect_flow_run.component's version."""
    data = artifact.data
    if artifact.type == "markdown":
        return dg.MetadataValue.md(str(data))
    if artifact.type == "table":
        return dg.MetadataValue.json(_parse_table_artifact_data(data))
    if artifact.type == "progress":
        try:
            return dg.MetadataValue.float(float(data))
        except (TypeError, ValueError):
            return dg.MetadataValue.text(str(data))
    if artifact.type == "image":
        return dg.MetadataValue.url(str(data))
    return dg.MetadataValue.text(str(data))


def _forward_prefect_artifact(
    context: Any, asset_key: Any, artifact: Any, check_names: set,
) -> Optional[Any]:
    """Forward one Prefect Artifact. Returns an AssetCheckResult if the
    artifact's key (dashes read as underscores) matches a declared
    check_names entry for THIS deployment (per-deployment, via
    assets_by_name — not a global list, see the module docstring for why);
    the CALLER must yield it. Plain artifacts are logged immediately as an
    AssetObservation and this returns None. Identical, independently-tested
    logic to prefect_flow_run.component's version."""
    translated_key = artifact.key.replace("-", "_") if artifact.key else None
    if translated_key and translated_key in check_names and artifact.type == "table":
        row = _first_table_row(_parse_table_artifact_data(artifact.data))
        passed = bool(row.get("passed"))
        extra_metadata = {k: v for k, v in row.items() if k != "passed"}
        return dg.AssetCheckResult(
            check_name=translated_key,
            passed=passed,
            description=artifact.description,
            metadata=extra_metadata or None,
        )

    key = artifact.key or f"prefect_artifact_{artifact.id}"
    context.log_event(dg.AssetObservation(
        asset_key=asset_key,
        metadata={key: _artifact_metadata_value(artifact)},
    ))
    return None


def _poll_flow_run_until_terminal(
    context: Any, asset_key: Any, flow_run_id: Any, timeout_seconds: Optional[int],
    poll_interval_seconds: float, forward_termination: bool,
    stream_logs: bool, stream_artifacts: bool, check_names: set,
):
    """Poll until terminal, forwarding Dagster termination as a Prefect
    cancellation, and (when enabled) streaming the flow's own logs/artifacts
    into the Dagster run log / event history each tick — identical,
    independently-tested logic to prefect_flow_run.component's version.

    Returns (flow_run, collected_check_results) — the caller must yield each
    collected check result itself (see `_forward_prefect_artifact`'s
    docstring for why that can't happen from in here).
    """
    import asyncio
    import time as _time
    from datetime import timedelta

    from dagster import DagsterExecutionInterruptedError
    from prefect.client.orchestration import get_client

    log_cursor: Dict[str, Any] = {"after": None}
    seen_artifact_ids: set = set()
    collected_checks: List[Any] = []

    async def _tick():
        async with get_client() as client:
            fr = await client.read_flow_run(flow_run_id)

            new_logs: List[Any] = []
            if stream_logs:
                from prefect.client.schemas.filters import (
                    LogFilter, LogFilterFlowRunId, LogFilterTimestamp,
                )
                from prefect.client.schemas.sorting import LogSort

                ts_filter = (
                    LogFilterTimestamp(after_=log_cursor["after"])
                    if log_cursor["after"] is not None else None
                )
                new_logs = await client.read_logs(
                    log_filter=LogFilter(
                        flow_run_id=LogFilterFlowRunId(any_=[flow_run_id]),
                        timestamp=ts_filter,
                    ),
                    sort=LogSort.TIMESTAMP_ASC,
                )
                if new_logs:
                    log_cursor["after"] = new_logs[-1].timestamp + timedelta(microseconds=1)

            new_artifacts: List[Any] = []
            if stream_artifacts:
                from prefect.client.schemas.filters import ArtifactFilter, ArtifactFilterFlowRunId

                all_artifacts = await client.read_artifacts(
                    artifact_filter=ArtifactFilter(
                        flow_run_id=ArtifactFilterFlowRunId(any_=[flow_run_id]),
                    ),
                )
                new_artifacts = [a for a in all_artifacts if a.id not in seen_artifact_ids]
                seen_artifact_ids.update(a.id for a in new_artifacts)

            return fr, new_logs, new_artifacts

    async def _cancel():
        from prefect.states import Cancelling
        async with get_client() as client:
            await client.set_flow_run_state(flow_run_id, Cancelling())

    start = _time.monotonic()
    try:
        while True:
            fr, new_logs, new_artifacts = asyncio.run(_tick())
            for entry in new_logs:
                _forward_prefect_log(context.log, entry)
            for artifact in new_artifacts:
                check_result = _forward_prefect_artifact(context, asset_key, artifact, check_names)
                if check_result is not None:
                    collected_checks.append(check_result)
            if fr.state is not None and fr.state.is_final():
                return fr, collected_checks
            if timeout_seconds is not None and (_time.monotonic() - start) > timeout_seconds:
                raise TimeoutError(f"Timed out after {timeout_seconds}s waiting for Prefect flow run {flow_run_id}")
            _time.sleep(poll_interval_seconds)
    except DagsterExecutionInterruptedError:
        if forward_termination:
            context.log.info(f"Dagster run terminated — cancelling Prefect flow run {flow_run_id}")
            try:
                asyncio.run(_cancel())
            except Exception as cancel_err:
                context.log.warning(f"Failed to cancel Prefect flow run {flow_run_id}: {cancel_err}")
        raise


class DagsterPrefectWorkspaceTranslator:
    """Base translator for Prefect workspace deployments → AssetSpec.
    Follows DagsterMLflowTranslator's shape."""

    def get_asset_spec(self, props: PrefectWorkspaceObjectProps) -> AssetSpec:
        parts = props.object_name.split("/", 1)
        return AssetSpec(
            key=AssetKey(["prefect", *[_safe(p) for p in parts]]),
            kinds={"prefect"},
            metadata={
                "prefect/flow_name": (props.extra or {}).get("flow_name", ""),
                "prefect/deployment_name": (props.extra or {}).get("deployment_name", ""),
            },
        )


class PrefectWorkspaceComponentTranslator(
    create_component_translator_cls(PrefectWorkspaceComponent, DagsterPrefectWorkspaceTranslator),  # ty: ignore[unsupported-base]
    ComponentTranslator[PrefectWorkspaceComponent],
):
    """Bridges PrefectWorkspaceComponent.translation with the base
    DagsterPrefectWorkspaceTranslator. Mirrors MLflowComponentTranslator."""

    def __init__(self, component: "PrefectWorkspaceComponent"):
        self._component = component

    def get_asset_spec(self, props: PrefectWorkspaceObjectProps) -> AssetSpec:
        base_asset_spec = super().get_asset_spec(props)
        if self.component.translation is None:
            return base_asset_spec
        return self.component.translation(base_asset_spec, props)
