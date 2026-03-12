"""Coalesce Project Component.

Extends StateBackedComponent so the Coalesce API is called once at prepare time
(write_state_to_path) and cached on disk. build_defs_from_state builds asset specs
from the cached node list with zero network calls, keeping code-server reloads fast.

On first load (state_path is None) returns empty Definitions — run
`dg utils refresh-defs-state` or `dagster dev` to populate the cache.
"""
from __future__ import annotations

import json
import os
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


# ── Resource ──────────────────────────────────────────────────────────────────

class CoalesceResource(dg.ConfigurableResource):
    """Shared Coalesce connection config.

    Example:
        ```python
        resources = {
            "coalesce": CoalesceResource(
                api_token_env_var="COALESCE_API_TOKEN",
                environment_id="env_abc123",
            )
        }
        ```
    """
    api_token_env_var: str = dg.Field(description="Env var with Coalesce API token")
    environment_id: str = dg.Field(description="Coalesce environment ID")
    api_base_url: str = dg.Field(
        default="https://app.coalescesoftware.io",
        description="Coalesce API base URL",
    )

    def _headers(self) -> dict:
        return {"Authorization": f"Bearer {os.environ[self.api_token_env_var]}"}

    def list_nodes(self) -> list[dict]:
        url = f"{self.api_base_url}/api/v1/environments/{self.environment_id}/nodes"
        resp = requests.get(url, headers=self._headers(), timeout=30)
        resp.raise_for_status()
        data = resp.json()
        return data.get("data", data) if isinstance(data, dict) else data

    def start_run(self, node_ids: list[str], parameters: Optional[dict] = None) -> str:
        url = f"{self.api_base_url}/scheduler/startRun"
        payload: dict = {
            "environmentID": self.environment_id,
            "parameterOverride": {"nodeIds": node_ids},
        }
        if parameters:
            payload["parameterOverride"].update(parameters)
        resp = requests.post(url, headers=self._headers(), json=payload, timeout=30)
        resp.raise_for_status()
        return resp.json()["runCounter"]

    def get_run_status(self, run_counter: str) -> str:
        url = f"{self.api_base_url}/scheduler/runStatus"
        resp = requests.get(
            url, headers=self._headers(),
            params={"runCounter": run_counter}, timeout=30,
        )
        resp.raise_for_status()
        return resp.json().get("runStatus", "")


# ── Component ─────────────────────────────────────────────────────────────────

def _build_coalesce_defs(
    nodes: list[dict],
    environment_id: str,
    api_token_env_var: str,
    api_base_url: str,
    asset_name_prefix: Optional[str],
    group_name: Optional[str],
    poll_interval: int,
    timeout: int,
) -> dg.Definitions:
    """Build Definitions from a list of Coalesce node dicts (no network calls)."""
    from dagster import AssetExecutionContext

    # Build a flat mapping of nodeId → AssetKey for dep resolution
    node_key_map: dict[str, dg.AssetKey] = {}
    for node in nodes:
        node_id = node.get("nodeId", node.get("id", ""))
        name = node.get("name", node_id)
        parts = [asset_name_prefix, name] if asset_name_prefix else [name]
        node_key_map[node_id] = dg.AssetKey([p for p in parts if p])

    specs: list[dg.AssetSpec] = []
    for node in nodes:
        node_id = node.get("nodeId", node.get("id", ""))
        name = node.get("name", node_id)
        source_ids = node.get("sourceNodeIds", [])

        dep_keys = [node_key_map[sid] for sid in source_ids if sid in node_key_map]

        specs.append(dg.AssetSpec(
            key=node_key_map[node_id],
            description=node.get("description"),
            group_name=group_name or "coalesce",
            deps=dep_keys,
            kinds={"coalesce", "sql"},
            metadata={
                "coalesce/node_id": dg.MetadataValue.text(node_id),
                "coalesce/node_type": dg.MetadataValue.text(node.get("type", "")),
            },
        ))

    if not specs:
        return dg.Definitions()

    @dg.multi_asset(specs=specs)
    def coalesce_project(context: AssetExecutionContext):
        import time

        token = os.environ[api_token_env_var]
        headers = {"Authorization": f"Bearer {token}"}

        # Determine which node IDs to materialize (selected assets)
        selected_node_ids = []
        reverse_map = {v: k for k, v in node_key_map.items()}
        for key in context.selected_asset_keys:
            node_id = reverse_map.get(key)
            if node_id:
                selected_node_ids.append(node_id)

        context.log.info(f"Starting Coalesce run for {len(selected_node_ids)} nodes")
        run_url = f"{api_base_url}/scheduler/startRun"
        run_resp = requests.post(run_url, headers=headers, json={
            "environmentID": environment_id,
            "parameterOverride": {"nodeIds": selected_node_ids},
        }, timeout=30)
        run_resp.raise_for_status()
        run_counter = run_resp.json()["runCounter"]
        context.log.info(f"Coalesce run started: {run_counter}")

        elapsed = 0
        while elapsed < timeout:
            time.sleep(poll_interval)
            elapsed += poll_interval
            status_resp = requests.get(
                f"{api_base_url}/scheduler/runStatus",
                headers=headers,
                params={"runCounter": run_counter},
                timeout=30,
            )
            status_resp.raise_for_status()
            status = status_resp.json().get("runStatus", "")
            context.log.info(f"Coalesce run {run_counter} status: {status}")
            if status == "succeeded":
                break
            if status in ("failed", "cancelled", "error"):
                raise Exception(f"Coalesce run {run_counter} {status}")

        return dg.MaterializeResult(metadata={
            "run_counter": dg.MetadataValue.text(run_counter),
            "nodes_run": len(selected_node_ids),
        })

    return dg.Definitions(assets=[coalesce_project])


if _HAS_STATE_BACKED:
    @dataclass
    class CoalesceProjectComponent(StateBackedComponent, dg.Resolvable):
        """Coalesce project component — one Dagster asset per Coalesce node.

        Uses StateBackedComponent to cache the node list from the Coalesce API,
        so code-server reloads are fast. Populate the cache with:
          dagster dev   (automatic in dev)
          dg utils refresh-defs-state   (CI/CD/image build)

        Example:
            ```yaml
            type: dagster_component_templates.CoalesceProjectComponent
            attributes:
              environment_id: env_abc123
              api_token_env_var: COALESCE_API_TOKEN
            ```
        """

        environment_id: str
        api_token_env_var: str
        api_base_url: str = "https://app.coalescesoftware.io"
        asset_name_prefix: Optional[str] = None
        group_name: Optional[str] = "coalesce"
        poll_interval_seconds: int = 10
        timeout_seconds: int = 1800
        defs_state: ResolvedDefsStateConfig = field(
            default_factory=DefsStateConfigArgs.local_filesystem
        )

        @property
        def defs_state_config(self) -> DefsStateConfig:
            return DefsStateConfig.from_args(
                self.defs_state,
                default_key=f"CoalesceProjectComponent[{self.environment_id}]",
            )

        def write_state_to_path(self, state_path: Path) -> None:
            """Fetch all nodes from Coalesce API and cache to disk."""
            token = os.environ[self.api_token_env_var]
            headers = {"Authorization": f"Bearer {token}"}
            url = f"{self.api_base_url}/api/v1/environments/{self.environment_id}/nodes"
            resp = requests.get(url, headers=headers, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            nodes = data.get("data", data) if isinstance(data, dict) else data
            state_path.write_text(json.dumps(nodes))

        def build_defs_from_state(
            self, context: dg.ComponentLoadContext, state_path: Optional[Path]
        ) -> dg.Definitions:
            """Build asset specs from cached node list — no network calls."""
            if state_path is None or not state_path.exists():
                context.log.warning(  # type: ignore
                    "CoalesceProjectComponent: no cached state found. "
                    "Run `dg utils refresh-defs-state` or `dagster dev` to populate."
                ) if hasattr(context, "log") else None
                return dg.Definitions()

            nodes = json.loads(state_path.read_text())
            return _build_coalesce_defs(
                nodes=nodes,
                environment_id=self.environment_id,
                api_token_env_var=self.api_token_env_var,
                api_base_url=self.api_base_url,
                asset_name_prefix=self.asset_name_prefix,
                group_name=self.group_name,
                poll_interval=self.poll_interval_seconds,
                timeout=self.timeout_seconds,
            )

else:
    # Fallback: StateBackedComponent not available in this dagster version.
    # Falls back to calling the API on every build_defs (original behaviour).
    class CoalesceProjectComponent(dg.Component, dg.Model, dg.Resolvable):  # type: ignore[no-redef]
        """Coalesce project component (fallback: no state caching).

        Upgrade to dagster>=1.8 to get StateBackedComponent caching.
        """
        environment_id: str = dg.Field(description="Coalesce environment ID")
        api_token_env_var: str = dg.Field(description="Env var with Coalesce API token")
        api_base_url: str = dg.Field(default="https://app.coalescesoftware.io")
        asset_name_prefix: Optional[str] = dg.Field(default=None)
        group_name: Optional[str] = dg.Field(default="coalesce")
        poll_interval_seconds: int = dg.Field(default=10)
        timeout_seconds: int = dg.Field(default=1800)

        def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
            token = os.environ[self.api_token_env_var]
            headers = {"Authorization": f"Bearer {token}"}
            url = f"{self.api_base_url}/api/v1/environments/{self.environment_id}/nodes"
            resp = requests.get(url, headers=headers, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            nodes = data.get("data", data) if isinstance(data, dict) else data
            return _build_coalesce_defs(
                nodes=nodes,
                environment_id=self.environment_id,
                api_token_env_var=self.api_token_env_var,
                api_base_url=self.api_base_url,
                asset_name_prefix=self.asset_name_prefix,
                group_name=self.group_name,
                poll_interval=self.poll_interval_seconds,
                timeout=self.timeout_seconds,
            )
