"""HCP Terraform (Terraform Cloud) Asset Component.

Extends StateBackedComponent so the HCP Terraform API is called once at prepare time
(write_state_to_path) and cached on disk. build_defs_from_state builds one asset per
workspace from the cached workspace list with zero network calls, keeping code-server
reloads fast.

On first load (state_path is None) returns empty Definitions — run
`dg utils refresh-defs-state` or `dagster dev` to populate the cache.
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
    StateBackedComponent = None
    _HAS_STATE_BACKED = False


# ── Constants ──────────────────────────────────────────────────────────────────

_TFC_BASE_URL = "https://app.terraform.io/api/v2"
_TFC_CONTENT_TYPE = "application/vnd.api+json"

_TERMINAL_STATUSES = frozenset({
    "applied",
    "planned_and_finished",
    "errored",
    "canceled",
    "force_canceled",
    "discarded",
})

_SUCCESS_STATUSES = frozenset({"applied", "planned_and_finished"})


# ── Helpers ────────────────────────────────────────────────────────────────────

def _tfc_headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": _TFC_CONTENT_TYPE,
    }


def _list_workspaces(token: str, organization: str) -> list[dict]:
    """Paginate through all workspaces in an HCP Terraform organization."""
    headers = _tfc_headers(token)
    workspaces: list[dict] = []
    page = 1

    while True:
        url = f"{_TFC_BASE_URL}/organizations/{organization}/workspaces"
        resp = requests.get(
            url,
            headers=headers,
            params={"page[size]": 100, "page[number]": page},
            timeout=30,
        )
        resp.raise_for_status()
        body = resp.json()
        workspaces.extend(body.get("data", []))

        next_page = (
            body.get("meta", {})
            .get("pagination", {})
            .get("next-page")
        )
        if not next_page:
            break
        page = next_page

    return workspaces


def _workspace_to_record(ws: dict) -> dict:
    attrs = ws.get("attributes", {})
    return {
        "id": ws["id"],
        "name": attrs.get("name", ws["id"]),
        "description": attrs.get("description"),
        "terraform_version": attrs.get("terraform-version"),
        "working_directory": attrs.get("working-directory"),
        "tags": [t.get("name") for t in attrs.get("tag-names", [])],
        "auto_apply": attrs.get("auto-apply", False),
        "locked": attrs.get("locked", False),
    }


def _apply_filters(
    workspaces: list[dict],
    workspace_filter: Optional[str],
    exclude_workspaces: Optional[list[str]],
    tags: Optional[list[str]],
) -> list[dict]:
    result = workspaces
    if workspace_filter:
        result = [w for w in result if workspace_filter in w["name"]]
    if exclude_workspaces:
        excluded = set(exclude_workspaces)
        result = [w for w in result if w["name"] not in excluded]
    if tags:
        required = set(tags)
        result = [w for w in result if required.issubset(set(w["tags"]))]
    return result


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


def _apply_workspace_overrides(
    default_spec: dg.AssetSpec,
    workspace_name: str,
    overrides: Optional[dict],
) -> list[dg.AssetSpec]:
    """Apply assets_by_workspace_name overrides. Returns list (1 usually, >1 if one workspace -> multiple assets)."""
    if not overrides or workspace_name not in overrides:
        return [default_spec]
    ov = overrides[workspace_name]
    if isinstance(ov, list):
        return [_merge_spec(default_spec, o) for o in ov]
    return [_merge_spec(default_spec, ov)]


# ── Core execution helper ──────────────────────────────────────────────────────

def _build_terraform_defs(
    workspaces: list[dict],
    api_token_env_var: str,
    group_name: str,
    key_prefix: Optional[str],
    plan_only: bool,
    auto_apply: bool,
    message: str,
    poll_interval_seconds: int,
    run_timeout_seconds: int,
    assets_by_workspace_name: Optional[dict] = None,
) -> dg.Definitions:
    """Build Definitions from a list of workspace records (no network calls)."""
    from dagster import AssetExecutionContext

    if not workspaces:
        return dg.Definitions()

    assets: list = []

    for ws in workspaces:
        ws_id = ws["id"]
        ws_name = ws["name"]

        key_parts = [key_prefix, ws_name] if key_prefix else [ws_name]
        asset_key = dg.AssetKey([p for p in key_parts if p])

        default_spec = dg.AssetSpec(
            key=asset_key,
            description=ws.get("description") or f"HCP Terraform workspace: {ws_name}",
            group_name=group_name,
            kinds={"terraform", "infrastructure"},
            metadata={
                "terraform/workspace_id": dg.MetadataValue.text(ws_id),
                "terraform/workspace_name": dg.MetadataValue.text(ws_name),
                "terraform/terraform_version": dg.MetadataValue.text(
                    ws.get("terraform_version") or ""
                ),
                "terraform/tags": dg.MetadataValue.text(
                    ", ".join(ws.get("tags") or [])
                ),
            },
        )

        expanded = _apply_workspace_overrides(default_spec, ws_name, assets_by_workspace_name)

        # Capture loop variables in closure
        _ws_id = ws_id
        _ws_name = ws_name
        _api_token_env_var = api_token_env_var
        _plan_only = plan_only
        _auto_apply = auto_apply
        _message = message
        _poll_interval = poll_interval_seconds
        _timeout = run_timeout_seconds

        if len(expanded) == 1:
            # Single spec — use @dg.asset with the (possibly overridden) spec
            _spec = expanded[0]

            @dg.asset(spec=_spec)
            def _terraform_workspace_asset(
                context: AssetExecutionContext,
                *,
                __ws_id=_ws_id,
                __ws_name=_ws_name,
                __api_token_env_var=_api_token_env_var,
                __plan_only=_plan_only,
                __auto_apply=_auto_apply,
                __message=_message,
                __poll_interval=_poll_interval,
                __timeout=_timeout,
            ) -> dg.MaterializeResult:
                token = os.environ[__api_token_env_var]
                headers = _tfc_headers(token)

                # Create a run
                run_url = f"{_TFC_BASE_URL}/workspaces/{__ws_id}/runs"
                run_body = {
                    "data": {
                        "type": "runs",
                        "attributes": {
                            "message": __message,
                            "plan-only": __plan_only,
                            "auto-apply": __auto_apply,
                        },
                        "relationships": {
                            "workspace": {
                                "data": {"type": "workspaces", "id": __ws_id}
                            }
                        },
                    }
                }
                context.log.info(
                    f"Creating Terraform run for workspace '{__ws_name}' ({__ws_id})"
                )
                run_resp = requests.post(run_url, headers=headers, json=run_body, timeout=30)
                run_resp.raise_for_status()
                run_id = run_resp.json()["data"]["id"]
                context.log.info(f"Run created: {run_id}")

                # Poll for completion
                elapsed = 0
                last_status: Optional[str] = None
                while elapsed < __timeout:
                    time.sleep(__poll_interval)
                    elapsed += __poll_interval

                    status_resp = requests.get(
                        f"{_TFC_BASE_URL}/runs/{run_id}",
                        headers=headers,
                        timeout=30,
                    )
                    status_resp.raise_for_status()
                    status = status_resp.json()["data"]["attributes"]["status"]

                    if status != last_status:
                        context.log.info(f"Run {run_id} status: {status}")
                        last_status = status

                    if status in _TERMINAL_STATUSES:
                        break
                else:
                    raise Exception(
                        f"Terraform run {run_id} timed out after {__timeout}s "
                        f"(last status: {last_status})"
                    )

                if status not in _SUCCESS_STATUSES:
                    raise Exception(
                        f"Terraform run {run_id} finished with status '{status}' "
                        f"for workspace '{__ws_name}'"
                    )

                # Fetch apply metadata on success
                apply_id: Optional[str] = None
                if status == "applied":
                    apply_resp = requests.get(
                        f"{_TFC_BASE_URL}/runs/{run_id}/apply",
                        headers=headers,
                        timeout=30,
                    )
                    if apply_resp.ok:
                        apply_id = apply_resp.json().get("data", {}).get("id")

                return dg.MaterializeResult(
                    metadata={
                        "run_id": dg.MetadataValue.text(run_id),
                        "status": dg.MetadataValue.text(status),
                        "workspace": dg.MetadataValue.text(__ws_name),
                        "apply_id": dg.MetadataValue.text(apply_id or ""),
                    }
                )

            assets.append(_terraform_workspace_asset)

        else:
            # Multiple specs — one workspace run yields MaterializeResult for each spec key
            _expanded_specs = expanded
            _safe_name = re.sub(r"[^a-zA-Z0-9_]", "_", ws_name)

            @dg.multi_asset(
                specs=_expanded_specs,
                name=f"tfc_{_safe_name}_multi",
            )
            def _terraform_workspace_multi_asset(
                context: AssetExecutionContext,
                *,
                __ws_id=_ws_id,
                __ws_name=_ws_name,
                __api_token_env_var=_api_token_env_var,
                __plan_only=_plan_only,
                __auto_apply=_auto_apply,
                __message=_message,
                __poll_interval=_poll_interval,
                __timeout=_timeout,
                __specs=_expanded_specs,
            ):
                token = os.environ[__api_token_env_var]
                headers = _tfc_headers(token)

                # Create a run
                run_url = f"{_TFC_BASE_URL}/workspaces/{__ws_id}/runs"
                run_body = {
                    "data": {
                        "type": "runs",
                        "attributes": {
                            "message": __message,
                            "plan-only": __plan_only,
                            "auto-apply": __auto_apply,
                        },
                        "relationships": {
                            "workspace": {
                                "data": {"type": "workspaces", "id": __ws_id}
                            }
                        },
                    }
                }
                context.log.info(
                    f"Creating Terraform run for workspace '{__ws_name}' ({__ws_id})"
                )
                run_resp = requests.post(run_url, headers=headers, json=run_body, timeout=30)
                run_resp.raise_for_status()
                run_id = run_resp.json()["data"]["id"]
                context.log.info(f"Run created: {run_id}")

                # Poll for completion
                elapsed = 0
                last_status: Optional[str] = None
                while elapsed < __timeout:
                    time.sleep(__poll_interval)
                    elapsed += __poll_interval

                    status_resp = requests.get(
                        f"{_TFC_BASE_URL}/runs/{run_id}",
                        headers=headers,
                        timeout=30,
                    )
                    status_resp.raise_for_status()
                    status = status_resp.json()["data"]["attributes"]["status"]

                    if status != last_status:
                        context.log.info(f"Run {run_id} status: {status}")
                        last_status = status

                    if status in _TERMINAL_STATUSES:
                        break
                else:
                    raise Exception(
                        f"Terraform run {run_id} timed out after {__timeout}s "
                        f"(last status: {last_status})"
                    )

                if status not in _SUCCESS_STATUSES:
                    raise Exception(
                        f"Terraform run {run_id} finished with status '{status}' "
                        f"for workspace '{__ws_name}'"
                    )

                # Fetch apply metadata on success
                apply_id: Optional[str] = None
                if status == "applied":
                    apply_resp = requests.get(
                        f"{_TFC_BASE_URL}/runs/{run_id}/apply",
                        headers=headers,
                        timeout=30,
                    )
                    if apply_resp.ok:
                        apply_id = apply_resp.json().get("data", {}).get("id")

                # Yield MaterializeResult for each output spec key
                for spec in __specs:
                    yield dg.MaterializeResult(
                        asset_key=spec.key,
                        metadata={
                            "run_id": dg.MetadataValue.text(run_id),
                            "status": dg.MetadataValue.text(status),
                            "workspace": dg.MetadataValue.text(__ws_name),
                            "apply_id": dg.MetadataValue.text(apply_id or ""),
                        },
                    )

            assets.append(_terraform_workspace_multi_asset)

    return dg.Definitions(assets=assets)


# ── Component ─────────────────────────────────────────────────────────────────

if _HAS_STATE_BACKED:
    @dataclass
    class TerraformCloudAssetComponent(StateBackedComponent, dg.Resolvable):
        """HCP Terraform (Terraform Cloud) asset component.

        Discovers all workspaces in an HCP Terraform organization at prepare time
        (StateBackedComponent) and creates one Dagster asset per workspace. At
        materialization, each asset triggers a remote run and polls for completion.

        Uses StateBackedComponent to cache the workspace list from the HCP Terraform API,
        so code-server reloads are fast. Populate the cache with:
          dagster dev   (automatic in dev)
          dg utils refresh-defs-state   (CI/CD/image build)

        Example:
            ```yaml
            type: dagster_component_templates.TerraformCloudAssetComponent
            attributes:
              api_token_env_var: TFC_TOKEN
              organization: my-company
              workspace_filter: "data-platform-"
              group_name: infrastructure
              auto_apply: true
              message: "Triggered by Dagster pipeline"
            ```
        """

        api_token_env_var: str
        organization: str
        workspace_filter: Optional[str] = None
        exclude_workspaces: Optional[list[str]] = None
        tags: Optional[list[str]] = None
        group_name: str = "infrastructure"
        key_prefix: Optional[str] = None
        plan_only: bool = False
        auto_apply: bool = True
        message: str = "Triggered by Dagster"
        poll_interval_seconds: int = 15
        run_timeout_seconds: int = 3600
        assets_by_workspace_name: Optional[dict] = None
        defs_state: ResolvedDefsStateConfig = field(
            default_factory=DefsStateConfigArgs.local_filesystem
        )

        @property
        def defs_state_config(self) -> DefsStateConfig:
            return DefsStateConfig.from_args(
                self.defs_state,
                default_key=f"TerraformCloudAssetComponent[{self.organization}]",
            )

        def write_state_to_path(self, state_path: Path) -> None:
            """Fetch all workspaces from HCP Terraform API and cache to disk."""
            token = os.environ[self.api_token_env_var]
            raw_workspaces = _list_workspaces(token, self.organization)
            records = [_workspace_to_record(ws) for ws in raw_workspaces]
            filtered = _apply_filters(
                records,
                self.workspace_filter,
                self.exclude_workspaces,
                self.tags,
            )
            state_path.write_text(json.dumps(filtered))

        def build_defs_from_state(
            self, context: dg.ComponentLoadContext, state_path: Optional[Path]
        ) -> dg.Definitions:
            """Build one asset per workspace from cached state — no network calls."""
            if state_path is None or not state_path.exists():
                if hasattr(context, "log"):
                    context.log.warning(  # type: ignore
                        "TerraformCloudAssetComponent: no cached state found. "
                        "Run `dg utils refresh-defs-state` or `dagster dev` to populate."
                    )
                return dg.Definitions()

            workspaces = json.loads(state_path.read_text())
            return _build_terraform_defs(
                workspaces=workspaces,
                api_token_env_var=self.api_token_env_var,
                group_name=self.group_name,
                key_prefix=self.key_prefix,
                plan_only=self.plan_only,
                auto_apply=self.auto_apply,
                message=self.message,
                poll_interval_seconds=self.poll_interval_seconds,
                run_timeout_seconds=self.run_timeout_seconds,
                assets_by_workspace_name=self.assets_by_workspace_name,
            )

else:
    # Fallback: StateBackedComponent not available in this dagster version.
    # Calls the HCP Terraform API on every build_defs (no caching).
    class TerraformCloudAssetComponent(dg.Component, dg.Model, dg.Resolvable):  # type: ignore[no-redef]
        """HCP Terraform asset component (fallback: no state caching).

        Upgrade to dagster>=1.8 to get StateBackedComponent caching.
        """

        api_token_env_var: str = dg.Field(
            description="Env var containing the HCP Terraform API token"
        )
        organization: str = dg.Field(
            description="HCP Terraform organization name"
        )
        workspace_filter: Optional[str] = dg.Field(
            default=None,
            description="Substring filter on workspace name",
        )
        exclude_workspaces: Optional[list[str]] = dg.Field(
            default=None,
            description="Workspace names to exclude",
        )
        tags: Optional[list[str]] = dg.Field(
            default=None,
            description="Only include workspaces with ALL these tags",
        )
        group_name: str = dg.Field(default="infrastructure")
        key_prefix: Optional[str] = dg.Field(default=None)
        plan_only: bool = dg.Field(
            default=False,
            description="Create plan-only runs (no apply)",
        )
        auto_apply: bool = dg.Field(default=True)
        message: str = dg.Field(default="Triggered by Dagster")
        poll_interval_seconds: int = dg.Field(default=15)
        run_timeout_seconds: int = dg.Field(default=3600)
        assets_by_workspace_name: Optional[dict] = dg.Field(
            default=None,
            description="Override AssetSpec for specific HCP Terraform workspaces by name",
        )

        def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
            token = os.environ[self.api_token_env_var]
            raw_workspaces = _list_workspaces(token, self.organization)
            records = [_workspace_to_record(ws) for ws in raw_workspaces]
            filtered = _apply_filters(
                records,
                self.workspace_filter,
                self.exclude_workspaces,
                self.tags,
            )
            return _build_terraform_defs(
                workspaces=filtered,
                api_token_env_var=self.api_token_env_var,
                group_name=self.group_name,
                key_prefix=self.key_prefix,
                plan_only=self.plan_only,
                auto_apply=self.auto_apply,
                message=self.message,
                poll_interval_seconds=self.poll_interval_seconds,
                run_timeout_seconds=self.run_timeout_seconds,
                assets_by_workspace_name=self.assets_by_workspace_name,
            )
