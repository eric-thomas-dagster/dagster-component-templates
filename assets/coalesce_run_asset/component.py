"""Coalesce Project Component.

Introspects a Coalesce environment at load time and creates one Dagster asset
per Coalesce node (model), with dependencies mirroring the Coalesce DAG.

Coalesce nodes are the equivalent of dbt models — each is a SQL transformation
that can be individually selected, materialized, and tracked in Dagster.

API: https://app.coalescesoftware.io
"""
import time
from typing import Optional, Sequence
import dagster as dg
from dagster import (
    AssetExecutionContext,
    AssetSpec,
    MaterializeResult,
    ConfigurableResource,
    multi_asset,
)
from pydantic import Field


class CoalesceResource(ConfigurableResource):
    """Resource for interacting with the Coalesce Scheduler API.

    Example (dagster.yaml or Definitions):
        ```python
        CoalesceResource(api_token=EnvVar("COALESCE_API_TOKEN"))
        ```
    """

    api_token: str = Field(description="Coalesce API token")
    base_url: str = Field(
        default="https://app.coalescesoftware.io",
        description="Coalesce base URL (default: https://app.coalescesoftware.io)",
    )

    def _headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self.api_token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def list_nodes(self, environment_id: str) -> list:
        import requests
        resp = requests.get(
            f"{self.base_url}/api/v1/environments/{environment_id}/nodes",
            headers=self._headers(),
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        return data.get("nodes", data if isinstance(data, list) else [])

    def start_run(self, environment_id: str, job_id: str, node_ids: list | None = None) -> str:
        import requests
        body: dict = {"environmentID": environment_id, "jobID": job_id}
        if node_ids:
            body["parameterOverride"] = {"nodeIds": node_ids}
        resp = requests.post(
            f"{self.base_url}/scheduler/startRun",
            headers=self._headers(),
            json=body,
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        return str(data.get("runCounter", data.get("id", "")))

    def get_run_status(self, run_counter: str) -> dict:
        import requests
        resp = requests.get(
            f"{self.base_url}/scheduler/runStatus",
            headers=self._headers(),
            params={"runCounter": run_counter},
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()


class CoalesceProjectComponent(dg.Component, dg.Model, dg.Resolvable):
    """Create one Dagster asset per Coalesce node, mirroring the Coalesce DAG.

    Discovers all nodes in the specified Coalesce environment at load time.
    Each node becomes a Dagster asset with upstream dependencies matching the
    Coalesce graph. Individual nodes can be selected and materialized — Dagster
    calls the Coalesce API to run only the selected subset.

    Requires a CoalesceResource to be available (see resource_key).

    Example:
        ```yaml
        type: dagster_component_templates.CoalesceProjectComponent
        attributes:
          environment_id: "my-environment-id"
          job_id: "my-job-id"
          api_token_env_var: COALESCE_API_TOKEN
          asset_key_prefix: coalesce
          group_name: coalesce_transforms
        ```
    """

    environment_id: str = Field(description="Coalesce environment ID")
    job_id: str = Field(description="Coalesce job ID used to execute node runs")
    api_token_env_var: str = Field(description="Env var containing the Coalesce API token")
    asset_key_prefix: str = Field(
        default="coalesce",
        description="Prefix for all generated asset keys (e.g. 'coalesce' → 'coalesce/orders_stage')",
    )
    group_name: str = Field(default="coalesce", description="Dagster asset group name")
    poll_interval_seconds: float = Field(default=10.0, description="Seconds between status polls")
    timeout_seconds: int = Field(default=3600, description="Max seconds to wait for a run")
    base_url: str = Field(
        default="https://app.coalescesoftware.io",
        description="Coalesce base URL",
    )

    def _fetch_nodes(self) -> list:
        """Fetch nodes from Coalesce API at load time."""
        import os, requests
        token = os.environ.get(self.api_token_env_var, "")
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        }
        resp = requests.get(
            f"{self.base_url}/api/v1/environments/{self.environment_id}/nodes",
            headers=headers,
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        return data.get("nodes", data if isinstance(data, list) else [])

    def _node_to_asset_key(self, node: dict) -> dg.AssetKey:
        name = (node.get("name") or node.get("nodeId") or "unknown").lower().replace(" ", "_").replace("-", "_")
        return dg.AssetKey([self.asset_key_prefix, name])

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        # Discover nodes from Coalesce at load time
        try:
            nodes = self._fetch_nodes()
        except Exception as e:
            raise RuntimeError(
                f"Failed to load Coalesce nodes for environment {self.environment_id}: {e}"
            )

        if not nodes:
            return dg.Definitions(assets=[])

        # Build a lookup: nodeId → asset key
        node_id_to_key: dict = {}
        for node in nodes:
            node_id = node.get("nodeId") or node.get("id") or ""
            node_id_to_key[node_id] = self._node_to_asset_key(node)

        # Create one AssetSpec per node with upstream deps
        specs: list[AssetSpec] = []
        node_id_to_node: dict = {}
        for node in nodes:
            node_id = node.get("nodeId") or node.get("id") or ""
            node_id_to_node[node_id] = node
            upstream_ids = node.get("sourceNodeIds", node.get("upstreamNodeIds", []))
            deps = [
                node_id_to_key[uid]
                for uid in upstream_ids
                if uid in node_id_to_key
            ]
            node_type = (node.get("type") or node.get("nodeType") or "node").lower()
            specs.append(
                AssetSpec(
                    key=node_id_to_key[node_id],
                    group_name=self.group_name,
                    description=f"Coalesce {node_type}: {node.get('name', node_id)}",
                    kinds={"coalesce", "sql"},
                    metadata={
                        "coalesce/node_id": node_id,
                        "coalesce/node_type": node_type,
                        "coalesce/environment_id": self.environment_id,
                        "coalesce/name": node.get("name", ""),
                        "coalesce/location": node.get("location", node.get("folder", "")),
                    },
                    deps=deps if deps else [],
                )
            )

        # Reverse lookup: asset key string → nodeId (for execution)
        key_str_to_node_id = {
            "/".join(node_id_to_key[nid].path): nid
            for nid in node_id_to_key
        }

        multi_asset_name = f"coalesce_{self.environment_id.replace('-', '_')}_nodes"

        @multi_asset(
            specs=specs,
            name=multi_asset_name,
            required_resource_keys=set(),
        )
        def coalesce_assets(context: AssetExecutionContext) -> Sequence[MaterializeResult]:
            import os, requests as req

            token = os.environ.get(_self.api_token_env_var, "")
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
            base = _self.base_url

            # Resolve selected asset keys → Coalesce node IDs
            selected_node_ids = []
            for asset_key in context.selected_asset_keys:
                key_str = "/".join(asset_key.path)
                node_id = key_str_to_node_id.get(key_str)
                if node_id:
                    selected_node_ids.append(node_id)

            context.log.info(
                f"Materializing {len(selected_node_ids)} Coalesce node(s): {selected_node_ids}"
            )

            # Trigger Coalesce run for selected nodes
            body: dict = {"environmentID": _self.environment_id, "jobID": _self.job_id}
            if selected_node_ids:
                body["parameterOverride"] = {"nodeIds": selected_node_ids}

            try:
                resp = req.post(f"{base}/scheduler/startRun", headers=headers, json=body, timeout=30)
                resp.raise_for_status()
                data = resp.json()
                run_counter = str(data.get("runCounter", data.get("id", "")))
            except Exception as e:
                raise Exception(f"Failed to start Coalesce run: {e}")

            context.log.info(f"Coalesce run started. runCounter={run_counter}")

            # Poll for completion
            elapsed = 0.0
            while elapsed < _self.timeout_seconds:
                time.sleep(_self.poll_interval_seconds)
                elapsed += _self.poll_interval_seconds
                try:
                    status_resp = req.get(
                        f"{base}/scheduler/runStatus",
                        headers=headers,
                        params={"runCounter": run_counter},
                        timeout=30,
                    )
                    status_resp.raise_for_status()
                    status_data = status_resp.json()
                    status = (status_data.get("status") or "").lower()
                    context.log.info(f"Run {run_counter} status: {status}")
                except Exception as e:
                    context.log.warning(f"Poll error: {e}")
                    continue

                if status == "succeeded":
                    nodes_run = status_data.get("nodesRun", 0)
                    nodes_failed = status_data.get("nodesFailed", 0)
                    return [
                        MaterializeResult(
                            asset_key=ak,
                            metadata={
                                "run_counter": run_counter,
                                "nodes_run": nodes_run,
                                "nodes_failed": nodes_failed,
                                "status": status,
                            },
                        )
                        for ak in context.selected_asset_keys
                    ]
                elif status in ("failed", "cancelled", "error"):
                    raise Exception(
                        f"Coalesce run {run_counter} {status}. "
                        f"nodes_failed={status_data.get('nodesFailed', '?')}. "
                        f"{status_data.get('errorMessage', '')}"
                    )
                # running / queued — keep polling

            raise Exception(f"Coalesce run {run_counter} timed out after {_self.timeout_seconds}s")

        return dg.Definitions(assets=[coalesce_assets])
