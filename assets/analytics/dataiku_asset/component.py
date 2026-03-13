"""Dataiku DSS Scenario Asset Component.

Extends StateBackedComponent so the Dataiku DSS API is called once at prepare time
(write_state_to_path) and the scenario list is cached on disk. build_defs_from_state
creates one Dagster asset per scenario with zero network calls, keeping code-server
reloads fast even for large Dataiku instances with many projects.

On first load (state_path is None) returns empty Definitions — run
`dg utils refresh-defs-state` or `dagster dev` to populate the cache.

The Dataiku Python SDK is NOT required. All API calls use the Dataiku DSS REST API
directly via `requests`, with HTTP Basic Auth (api_key, "").
"""
from __future__ import annotations

import json
import os
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import dagster as dg
import requests

try:
    from dagster.components.component.state_backed_component import StateBackedComponent
    from dagster.components.utils.defs_state import (
        DefsStateConfig,
        DefsStateConfigArgs,
        ResolvedDefsStateConfig,
    )
    _HAS_STATE_BACKED = True
except ImportError:
    StateBackedComponent = None  # type: ignore[assignment,misc]
    _HAS_STATE_BACKED = False


# ── Helpers ───────────────────────────────────────────────────────────────────

def _sanitize(name: str) -> str:
    """Lowercase and replace non-alphanumeric characters with underscores."""
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


def _asset_key(project_key: str, scenario_id: str, key_prefix: Optional[str]) -> dg.AssetKey:
    parts: list[str] = []
    if key_prefix:
        parts.append(_sanitize(key_prefix))
    parts.append(_sanitize(project_key))
    parts.append(_sanitize(scenario_id))
    return dg.AssetKey(parts)


def _merge_spec(base: dg.AssetSpec, ov: dict) -> dg.AssetSpec:
    """Merge an override dict into a base AssetSpec."""
    extra_deps = [dg.AssetKey.from_user_string(d) for d in ov.get("deps", [])]
    return dg.AssetSpec(
        key=dg.AssetKey.from_user_string(ov["key"]) if "key" in ov else base.key,
        description=ov.get("description", base.description),
        group_name=ov.get("group_name", base.group_name),
        metadata={**(base.metadata or {}), **(ov.get("metadata") or {})},
        tags={**(base.tags or {}), **(ov.get("tags") or {})},
        kinds=set(ov["kinds"]) if "kinds" in ov else base.kinds,
        deps=list(base.deps or []) + extra_deps,
    )


def _apply_item_overrides(
    default_spec: dg.AssetSpec,
    item_name: str,
    overrides: Optional[dict],
) -> list[dg.AssetSpec]:
    """Apply assets_by_X overrides. Returns list (usually 1, but >1 if one item → multiple assets)."""
    if not overrides or item_name not in overrides:
        return [default_spec]
    ov = overrides[item_name]
    if isinstance(ov, list):
        return [_merge_spec(default_spec, o) for o in ov]
    return [_merge_spec(default_spec, ov)]


# ── Core build logic (shared between StateBackedComponent and fallback) ────────

def _build_dataiku_defs(
    scenarios_by_project: dict[str, list[dict]],
    host_env_var: str,
    api_key_env_var: str,
    group_name: str,
    key_prefix: Optional[str],
    wait_for_completion: bool,
    poll_interval_seconds: int,
    scenario_timeout_seconds: int,
    assets_by_scenario_name: Optional[dict] = None,
) -> dg.Definitions:
    """Build Definitions from a cached scenario map — no network calls at asset-spec time."""

    assets: list = []

    for project_key, scenarios in scenarios_by_project.items():
        for scenario in scenarios:
            scenario_id: str = scenario["id"]
            scenario_name: str = scenario.get("name", scenario_id)
            computed_asset_key = _asset_key(project_key, scenario_id, key_prefix)

            default_spec = dg.AssetSpec(
                key=computed_asset_key,
                description=f"Dataiku scenario '{scenario_name}' in project '{project_key}'",
                group_name=group_name,
                kinds={"dataiku"},
                metadata={
                    "dataiku/project_key": dg.MetadataValue.text(project_key),
                    "dataiku/scenario_id": dg.MetadataValue.text(scenario_id),
                    "dataiku/scenario_name": dg.MetadataValue.text(scenario_name),
                },
            )
            expanded_specs = _apply_item_overrides(default_spec, scenario_name, assets_by_scenario_name)

            # Capture loop variables in default args to avoid closure issues
            def _make_asset(
                _project_key: str = project_key,
                _scenario_id: str = scenario_id,
                _scenario_name: str = scenario_name,
                _expanded_specs: list = expanded_specs,
            ):
                if len(_expanded_specs) == 1:
                    spec = _expanded_specs[0]

                    @dg.asset(
                        key=spec.key,
                        group_name=spec.group_name,
                        description=spec.description,
                        metadata=dict(spec.metadata or {}),
                        tags=dict(spec.tags or {}),
                        kinds=spec.kinds or {"dataiku"},
                        deps=list(spec.deps or []),
                    )
                    def _dataiku_scenario_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
                        host = os.environ[host_env_var].rstrip("/")
                        api_key = os.environ[api_key_env_var]
                        auth = (api_key, "")

                        # Trigger the scenario
                        run_url = (
                            f"{host}/api/projects/{_project_key}/scenarios/{_scenario_id}/run/"
                        )
                        context.log.info(
                            f"Triggering Dataiku scenario '{_scenario_name}' "
                            f"(project={_project_key}, id={_scenario_id})"
                        )
                        run_resp = requests.post(run_url, auth=auth, timeout=30)
                        run_resp.raise_for_status()
                        run_data = run_resp.json()
                        context.log.info(f"Scenario run triggered: {run_data}")

                        outcome = "triggered"

                        if wait_for_completion:
                            status_url = (
                                f"{host}/api/projects/{_project_key}/scenarios/"
                                f"{_scenario_id}/last-finished-run/"
                            )
                            elapsed = 0
                            # Give the scenario a moment to start before first poll
                            time.sleep(min(poll_interval_seconds, 5))
                            while elapsed < scenario_timeout_seconds:
                                status_resp = requests.get(status_url, auth=auth, timeout=30)
                                status_resp.raise_for_status()
                                status_data = status_resp.json()
                                outcome = status_data.get("result", {}).get("outcome", "RUNNING")
                                context.log.info(
                                    f"Scenario '{_scenario_name}' status: {outcome} "
                                    f"(elapsed={elapsed}s)"
                                )
                                if outcome == "SUCCESS":
                                    break
                                if outcome in ("FAILED", "ABORTED"):
                                    raise Exception(
                                        f"Dataiku scenario '{_scenario_name}' "
                                        f"(project={_project_key}) ended with outcome: {outcome}"
                                    )
                                time.sleep(poll_interval_seconds)
                                elapsed += poll_interval_seconds
                            else:
                                raise Exception(
                                    f"Dataiku scenario '{_scenario_name}' timed out after "
                                    f"{scenario_timeout_seconds}s"
                                )

                        return dg.MaterializeResult(
                            metadata={
                                "project": dg.MetadataValue.text(_project_key),
                                "scenario": dg.MetadataValue.text(_scenario_id),
                                "scenario_name": dg.MetadataValue.text(_scenario_name),
                                "outcome": dg.MetadataValue.text(outcome),
                            }
                        )

                    return _dataiku_scenario_asset

                else:
                    # One scenario → multiple Dagster assets
                    _safe_scenario = _sanitize(_scenario_name)

                    @dg.multi_asset(specs=_expanded_specs, name=f"{_safe_scenario}_multi")
                    def _dataiku_scenario_multi_asset(context: dg.AssetExecutionContext):
                        host = os.environ[host_env_var].rstrip("/")
                        api_key = os.environ[api_key_env_var]
                        auth = (api_key, "")

                        # Trigger the scenario
                        run_url = (
                            f"{host}/api/projects/{_project_key}/scenarios/{_scenario_id}/run/"
                        )
                        context.log.info(
                            f"Triggering Dataiku scenario '{_scenario_name}' "
                            f"(project={_project_key}, id={_scenario_id})"
                        )
                        run_resp = requests.post(run_url, auth=auth, timeout=30)
                        run_resp.raise_for_status()
                        run_data = run_resp.json()
                        context.log.info(f"Scenario run triggered: {run_data}")

                        outcome = "triggered"

                        if wait_for_completion:
                            status_url = (
                                f"{host}/api/projects/{_project_key}/scenarios/"
                                f"{_scenario_id}/last-finished-run/"
                            )
                            elapsed = 0
                            time.sleep(min(poll_interval_seconds, 5))
                            while elapsed < scenario_timeout_seconds:
                                status_resp = requests.get(status_url, auth=auth, timeout=30)
                                status_resp.raise_for_status()
                                status_data = status_resp.json()
                                outcome = status_data.get("result", {}).get("outcome", "RUNNING")
                                context.log.info(
                                    f"Scenario '{_scenario_name}' status: {outcome} "
                                    f"(elapsed={elapsed}s)"
                                )
                                if outcome == "SUCCESS":
                                    break
                                if outcome in ("FAILED", "ABORTED"):
                                    raise Exception(
                                        f"Dataiku scenario '{_scenario_name}' "
                                        f"(project={_project_key}) ended with outcome: {outcome}"
                                    )
                                time.sleep(poll_interval_seconds)
                                elapsed += poll_interval_seconds
                            else:
                                raise Exception(
                                    f"Dataiku scenario '{_scenario_name}' timed out after "
                                    f"{scenario_timeout_seconds}s"
                                )

                        for spec in _expanded_specs:
                            yield dg.MaterializeResult(
                                asset_key=spec.key,
                                metadata={
                                    "project": dg.MetadataValue.text(_project_key),
                                    "scenario": dg.MetadataValue.text(_scenario_id),
                                    "scenario_name": dg.MetadataValue.text(_scenario_name),
                                    "outcome": dg.MetadataValue.text(outcome),
                                },
                            )

                    return _dataiku_scenario_multi_asset

            assets.append(_make_asset())

    if not assets:
        return dg.Definitions()

    return dg.Definitions(assets=assets)


def _fetch_scenarios(
    host: str,
    api_key: str,
    project_keys: Optional[list[str]],
    exclude_project_keys: Optional[list[str]],
    scenario_name_prefix: Optional[str],
    exclude_scenario_ids: Optional[list[str]],
) -> dict[str, list[dict]]:
    """Fetch scenarios from Dataiku DSS REST API. Returns {project_key: [scenario, ...]}."""
    auth = (api_key, "")
    host = host.rstrip("/")

    # List projects
    if project_keys:
        project_list = [{"projectKey": pk} for pk in project_keys]
    else:
        projects_resp = requests.get(f"{host}/api/projects/", auth=auth, timeout=30)
        projects_resp.raise_for_status()
        project_list = projects_resp.json()

    exclude_set = set(exclude_project_keys or [])
    exclude_scenario_set = set(exclude_scenario_ids or [])

    result: dict[str, list[dict]] = {}

    for project in project_list:
        pk = project.get("projectKey") or project.get("key", "")
        if not pk or pk in exclude_set:
            continue

        scenarios_resp = requests.get(
            f"{host}/api/projects/{pk}/scenarios/", auth=auth, timeout=30
        )
        scenarios_resp.raise_for_status()
        scenarios = scenarios_resp.json()

        filtered: list[dict] = []
        for scenario in scenarios:
            sid = scenario.get("id", "")
            sname = scenario.get("name", "")
            if sid in exclude_scenario_set:
                continue
            if scenario_name_prefix and not sname.startswith(scenario_name_prefix):
                continue
            filtered.append({
                "id": sid,
                "name": sname,
                "active": scenario.get("active", True),
                "type": scenario.get("type", ""),
            })

        if filtered:
            result[pk] = filtered

    return result


# ── StateBackedComponent variant ───────────────────────────────────────────────

if _HAS_STATE_BACKED:
    @dataclass
    class DataikuAssetComponent(StateBackedComponent, dg.Resolvable):
        """Dataiku DSS scenario component — one Dagster asset per Dataiku scenario.

        Uses StateBackedComponent to cache the scenario list from the Dataiku DSS API
        at prepare time (write_state_to_path). build_defs_from_state creates asset specs
        from the cache with zero network calls, so code-server reloads are instant even
        for large Dataiku instances with many projects.

        Populate or refresh the cache with:
          dagster dev                       (automatic in dev)
          dg utils refresh-defs-state       (CI/CD / image build)

        Example:
            ```yaml
            type: dagster_component_templates.DataikuAssetComponent
            attributes:
              host_env_var: DATAIKU_HOST
              api_key_env_var: DATAIKU_API_KEY
              project_keys:
                - CUSTOMER_ANALYTICS
                - ML_MODELS
              group_name: dataiku
              wait_for_completion: true
            ```
        """

        host_env_var: str = dg.Field(
            description="Env var containing the Dataiku DSS host URL (e.g. https://dataiku.company.com)"
        )
        api_key_env_var: str = dg.Field(
            description="Env var containing the Dataiku DSS API key"
        )
        project_keys: Optional[list[str]] = dg.Field(
            default=None,
            description="Specific project keys to include. Includes all projects if None.",
        )
        exclude_project_keys: Optional[list[str]] = dg.Field(
            default=None,
            description="Project keys to exclude (applied after project_keys filter).",
        )
        scenario_name_prefix: Optional[str] = dg.Field(
            default=None,
            description="Only include scenarios whose name starts with this prefix.",
        )
        exclude_scenario_ids: Optional[list[str]] = dg.Field(
            default=None,
            description="Scenario IDs to exclude.",
        )
        group_name: str = dg.Field(
            default="dataiku",
            description="Dagster asset group name for all generated assets.",
        )
        key_prefix: Optional[str] = dg.Field(
            default=None,
            description="Optional prefix segment for all generated asset keys.",
        )
        wait_for_completion: bool = dg.Field(
            default=True,
            description="If True, poll until the scenario finishes before returning.",
        )
        poll_interval_seconds: int = dg.Field(
            default=10,
            description="Seconds between status polls when wait_for_completion=True.",
        )
        scenario_timeout_seconds: int = dg.Field(
            default=7200,
            description="Maximum seconds to wait for a scenario to finish.",
        )
        assets_by_scenario_name: Optional[dict] = dg.Field(
            default=None,
            description="Override AssetSpec per scenario name. Value can be a single override dict or a list of dicts (one scenario → multiple assets).",
        )
        defs_state: ResolvedDefsStateConfig = field(
            default_factory=ResolvedDefsStateConfig
        )

        @property
        def defs_state_config(self) -> DefsStateConfig:
            return DefsStateConfig.from_args(
                self.defs_state,
                default_key=f"DataikuAssetComponent[{self.host_env_var}]",
            )

        def write_state_to_path(self, state_path: Path) -> None:
            """Fetch all matching scenarios from Dataiku DSS API and cache to disk."""
            host = os.environ[self.host_env_var]
            api_key = os.environ[self.api_key_env_var]
            scenarios_by_project = _fetch_scenarios(
                host=host,
                api_key=api_key,
                project_keys=self.project_keys,
                exclude_project_keys=self.exclude_project_keys,
                scenario_name_prefix=self.scenario_name_prefix,
                exclude_scenario_ids=self.exclude_scenario_ids,
            )
            state_path.write_text(json.dumps(scenarios_by_project, indent=2))

        def build_defs_from_state(
            self, context: dg.ComponentLoadContext, state_path: Optional[Path]
        ) -> dg.Definitions:
            """Build asset specs from cached scenario list — no network calls."""
            if state_path is None or not state_path.exists():
                if hasattr(context, "log"):
                    context.log.warning(  # type: ignore[attr-defined]
                        "DataikuAssetComponent: no cached state found. "
                        "Run `dg utils refresh-defs-state` or `dagster dev` to populate."
                    )
                return dg.Definitions()

            scenarios_by_project: dict[str, list[dict]] = json.loads(state_path.read_text())
            return _build_dataiku_defs(
                scenarios_by_project=scenarios_by_project,
                host_env_var=self.host_env_var,
                api_key_env_var=self.api_key_env_var,
                group_name=self.group_name,
                key_prefix=self.key_prefix,
                wait_for_completion=self.wait_for_completion,
                poll_interval_seconds=self.poll_interval_seconds,
                scenario_timeout_seconds=self.scenario_timeout_seconds,
                assets_by_scenario_name=self.assets_by_scenario_name,
            )

else:
    # Fallback: StateBackedComponent not available in this Dagster version.
    # Falls back to calling the API on every build_defs (no caching).
    class DataikuAssetComponent(dg.Component, dg.Model, dg.Resolvable):  # type: ignore[no-redef]
        """Dataiku DSS scenario component (fallback: no state caching).

        Upgrade to dagster>=1.8 to get StateBackedComponent caching.
        """

        host_env_var: str = dg.Field(
            description="Env var containing the Dataiku DSS host URL"
        )
        api_key_env_var: str = dg.Field(
            description="Env var containing the Dataiku DSS API key"
        )
        project_keys: Optional[list[str]] = dg.Field(default=None)
        exclude_project_keys: Optional[list[str]] = dg.Field(default=None)
        scenario_name_prefix: Optional[str] = dg.Field(default=None)
        exclude_scenario_ids: Optional[list[str]] = dg.Field(default=None)
        group_name: str = dg.Field(default="dataiku")
        key_prefix: Optional[str] = dg.Field(default=None)
        wait_for_completion: bool = dg.Field(default=True)
        poll_interval_seconds: int = dg.Field(default=10)
        scenario_timeout_seconds: int = dg.Field(default=7200)
        assets_by_scenario_name: Optional[dict] = dg.Field(default=None)

        def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
            host = os.environ[self.host_env_var]
            api_key = os.environ[self.api_key_env_var]
            scenarios_by_project = _fetch_scenarios(
                host=host,
                api_key=api_key,
                project_keys=self.project_keys,
                exclude_project_keys=self.exclude_project_keys,
                scenario_name_prefix=self.scenario_name_prefix,
                exclude_scenario_ids=self.exclude_scenario_ids,
            )
            return _build_dataiku_defs(
                scenarios_by_project=scenarios_by_project,
                host_env_var=self.host_env_var,
                api_key_env_var=self.api_key_env_var,
                group_name=self.group_name,
                key_prefix=self.key_prefix,
                wait_for_completion=self.wait_for_completion,
                poll_interval_seconds=self.poll_interval_seconds,
                scenario_timeout_seconds=self.scenario_timeout_seconds,
                assets_by_scenario_name=self.assets_by_scenario_name,
            )
