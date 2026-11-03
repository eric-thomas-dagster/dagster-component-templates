"""Coalesce Resource component.

Shared Coalesce Scheduler API connection (environment + Bearer token) with
start/poll/results convenience methods, so `coalesce_job_sensor` and any
future Coalesce component can share one credential.

Real Coalesce API facts this file's behavior is built on (confirmed against
the live OpenAPI spec at https://docs.coalesce.io/api/coalesce-api.json and
the bundled jobs spec at https://docs.coalesce.io/api/bundled/bundled-jobs.json
on 2026-11-02 -- the same research `integrations/coalesce_workspace` is
built on, duplicated here per this repo's self-contained-component
convention rather than imported):

- The Scheduler API has exactly four endpoints: POST /scheduler/startRun,
  GET /scheduler/runStatus (query param: runCounter), POST /scheduler/rerun,
  POST /scheduler/cancelRun. There is NO endpoint to list or search runs by
  job/environment -- polling requires already having the runCounter a run
  was started with.
- `runStatus`'s real terminal values are `completed`, `failed`, `canceled`
  (one L) -- NOT `succeeded`/`cancelled`/`error`.
"""
import os
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


class CoalesceResource(dg.ConfigurableResource):
    """Coalesce Scheduler API workhorse: connection + start/poll convenience methods."""

    environment_id: str
    api_token: str
    api_base_url: str = "https://app.coalescesoftware.io"
    request_timeout_seconds: int = 30

    def _headers(self) -> Dict[str, str]:
        return {"Authorization": f"Bearer {self.api_token}", "accept": "application/json"}

    def start_run(self, node_ids: Optional[List[str]] = None, parameters: Optional[Dict[str, Any]] = None) -> str:
        """POST /scheduler/startRun. Returns the new run's runCounter."""
        import requests

        payload: Dict[str, Any] = {"environmentID": self.environment_id}
        if node_ids:
            payload["parameterOverride"] = {"nodeIds": node_ids}
        if parameters:
            payload.setdefault("parameterOverride", {}).update(parameters)
        resp = requests.post(
            f"{self.api_base_url}/scheduler/startRun",
            headers=self._headers(), json=payload, timeout=self.request_timeout_seconds,
        )
        resp.raise_for_status()
        return str(resp.json()["runCounter"])

    def get_run_status(self, run_counter: str) -> str:
        """GET /scheduler/runStatus. Real terminal values: completed/failed/canceled."""
        import requests

        resp = requests.get(
            f"{self.api_base_url}/scheduler/runStatus",
            headers=self._headers(), params={"runCounter": run_counter}, timeout=self.request_timeout_seconds,
        )
        resp.raise_for_status()
        return resp.json().get("runStatus", "")

    def get_run_results(self, run_counter: str) -> List[Dict[str, Any]]:
        """GET /api/v1/runs/{runID}/results -- per-node results, incl. hasTestFailures."""
        import requests

        resp = requests.get(
            f"{self.api_base_url}/api/v1/runs/{run_counter}/results",
            headers=self._headers(), timeout=self.request_timeout_seconds,
        )
        resp.raise_for_status()
        data = resp.json()
        return data.get("data", data) if isinstance(data, dict) else data


class CoalesceResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a Coalesce Scheduler API resource for use by other components.

    Pairs with `coalesce_job_sensor` to trigger on run completion.
    """

    resource_key: str = Field(
        default="coalesce_resource",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    environment_id: str = Field(description="Coalesce environment ID.")
    api_token_env_var: str = Field(description="Env var with the Coalesce API token.")
    api_base_url: str = Field(default="https://app.coalescesoftware.io", description="Coalesce API base URL (regional deployments may differ).")
    request_timeout_seconds: int = Field(default=30, description="Per-request timeout in seconds.")

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Coalesce Resource", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = CoalesceResource(
            environment_id=self.environment_id,
            api_token=dg.EnvVar(self.api_token_env_var),
            api_base_url=self.api_base_url,
            request_timeout_seconds=self.request_timeout_seconds,
        )
        return dg.Definitions(resources={self.resource_key: resource})
