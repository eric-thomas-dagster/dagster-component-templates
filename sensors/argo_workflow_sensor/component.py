"""Argo Workflow Sensor Component.

Polls the Argo Server REST API for the status of a specific workflow
(or the latest run of a WorkflowTemplate) and emits an
AssetMaterialization / AssetObservation on terminal success.

Argo workflow phases (terminal):
  - Succeeded  → success
  - Failed     → terminal failure
  - Error      → terminal failure
  - Skipped    → terminal (treated as success — workflow opted out)
  - Omitted    → terminal (treated as failure)
In-progress phases: Pending, Running.

API: GET /api/v1/workflows/{namespace}/{name}
     GET /api/v1/workflows/{namespace}?listOptions.labelSelector=...

Auth: Bearer token via service-account token (ARGO_TOKEN env var) OR
no auth on clusters where Argo Server is configured for anonymous read.

Pair with ``external_argo_workflow`` (Case A — Argo owns the schedule)
or ``argo_workflow_trigger`` in observation mode (Case B — Dagster
fires + observes).

Docs: https://argo-workflows.readthedocs.io/en/latest/rest-api/
"""
from typing import Dict, Optional

import dagster as dg
from dagster import (
    AssetMaterialization,
    AssetObservation,
    RunRequest,
    SensorEvaluationContext,
    SensorResult,
    sensor,
)
from dagster._core.definitions.sensor_definition import DefaultSensorStatus
from pydantic import Field

ARGO_TERMINAL_SUCCESS = {"Succeeded", "Skipped"}
ARGO_TERMINAL_FAIL = {"Failed", "Error", "Omitted"}


class ArgoWorkflowSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Fire a Dagster job when an Argo Workflow reaches a terminal success phase.

    Two modes — set exactly one of ``workflow_name`` or
    ``workflow_template_label``:

      - ``workflow_name`` — watch one specific Workflow by name. Useful
        when Dagster knows the workflow's name (e.g. it triggered it).

      - ``workflow_template_label`` — list workflows by a label selector
        like ``workflows.argoproj.io/workflow-template=nightly-agg``
        and watch the most-recently-completed one. Useful when Argo's
        scheduler creates workflows on its own cadence and Dagster
        wants to react to the latest finish.

    Example:
        ```yaml
        type: dagster_community_components.ArgoWorkflowSensorComponent
        attributes:
          sensor_name: argo_nightly_done
          workflow_template_label: workflows.argoproj.io/workflow-template=nightly-aggregation
          namespace: argo
          host_env_var: ARGO_SERVER_URL
          token_env_var: ARGO_TOKEN
          job_name: downstream_processing_job
          asset_key: argo/etl/nightly_aggregation
        ```
    """

    sensor_name: str = Field(description="Unique sensor name.")
    namespace: str = Field(
        default="argo",
        description="Kubernetes namespace the workflow runs in.",
    )
    workflow_name: Optional[str] = Field(
        default=None,
        description=(
            "Specific Argo Workflow name to watch. Set this OR "
            "workflow_template_label, not both."
        ),
    )
    workflow_template_label: Optional[str] = Field(
        default=None,
        description=(
            "Label selector to identify workflows of a given template, "
            "e.g. 'workflows.argoproj.io/workflow-template=nightly-agg'. "
            "Sensor watches the most-recently-completed matching workflow. "
            "Set this OR workflow_name, not both."
        ),
    )
    job_name: str = Field(description="Dagster job to trigger on terminal success.")
    asset_key: Optional[str] = Field(
        default=None,
        description=(
            "Optional Dagster asset key. When set, the sensor emits "
            "AssetMaterialization (default) or AssetObservation (see "
            "asset_event_type) on terminal success. Use '/' separators."
        ),
    )
    asset_event_type: str = Field(
        default="materialization",
        description=(
            "'materialization' (default) or 'observation'. Use "
            "'observation' when paired with argo_workflow_trigger to "
            "avoid double-counting materializations on the same asset."
        ),
    )

    host_env_var: str = Field(
        default="ARGO_SERVER_URL",
        description="Env var with Argo Server base URL (e.g. https://argo.mycompany.com).",
    )
    token_env_var: Optional[str] = Field(
        default="ARGO_TOKEN",
        description=(
            "Env var with the bearer token for the Argo Server. Leave "
            "empty for clusters with anonymous read enabled."
        ),
    )
    verify_ssl: bool = Field(default=True, description="Verify TLS certs (set False only for dev clusters).")

    minimum_interval_seconds: int = Field(default=60, description="Seconds between polls.")
    default_status: str = Field(default="running", description="'running' or 'stopped'.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        if bool(self.workflow_name) == bool(self.workflow_template_label):
            raise ValueError(
                f"argo_workflow_sensor {self.sensor_name!r}: set exactly one of "
                f"`workflow_name` or `workflow_template_label`."
            )

        _self = self
        default_status = (
            DefaultSensorStatus.RUNNING if self.default_status == "running"
            else DefaultSensorStatus.STOPPED
        )

        @sensor(
            name=_self.sensor_name,
            minimum_interval_seconds=_self.minimum_interval_seconds,
            default_status=default_status,
            job_name=_self.job_name,
        )
        def argo_workflow_sensor(context: SensorEvaluationContext):
            import os
            try:
                import requests
            except ImportError:
                return SensorResult(skip_reason="requests not installed")

            host = os.environ.get(_self.host_env_var, "").rstrip("/")
            if not host:
                return SensorResult(skip_reason=f"{_self.host_env_var} not set")
            headers: Dict[str, str] = {"Accept": "application/json"}
            if _self.token_env_var and os.environ.get(_self.token_env_var):
                headers["Authorization"] = f"Bearer {os.environ[_self.token_env_var]}"

            # Pick the workflow to inspect.
            if _self.workflow_name:
                url = f"{host}/api/v1/workflows/{_self.namespace}/{_self.workflow_name}"
            else:
                # List workflows matching the label selector; pick the
                # most-recently-completed one. Argo orders by creation
                # timestamp descending by default.
                url = (
                    f"{host}/api/v1/workflows/{_self.namespace}"
                    f"?listOptions.labelSelector={_self.workflow_template_label}"
                    f"&listOptions.limit=10"
                )

            try:
                resp = requests.get(url, headers=headers, timeout=30, verify=_self.verify_ssl)
                resp.raise_for_status()
                body = resp.json()
            except Exception as e:
                return SensorResult(skip_reason=f"Argo Server poll error: {e}")

            # Resolve to a single workflow object.
            if _self.workflow_name:
                wf = body
            else:
                items = body.get("items") or []
                # Pick the most-recent terminal one.
                terminal = [
                    it for it in items
                    if (it.get("status") or {}).get("phase") in ARGO_TERMINAL_SUCCESS | ARGO_TERMINAL_FAIL
                ]
                wf = terminal[0] if terminal else None

            if not wf:
                return SensorResult(skip_reason="No terminal workflow found yet.")

            metadata_blob = wf.get("metadata") or {}
            status_blob = wf.get("status") or {}
            wf_name = metadata_blob.get("name") or "(unknown)"
            phase = status_blob.get("phase")

            # De-dupe across ticks: cursor is the last-emitted workflow name.
            cursor = context.cursor or ""

            if phase in ARGO_TERMINAL_SUCCESS and wf_name != cursor:
                asset_events = []
                if _self.asset_key:
                    ak = dg.AssetKey(_self.asset_key.split("/"))
                    desc = f"Argo Workflow {wf_name} → {phase}"
                    md = {
                        "argo_workflow_name": wf_name,
                        "argo_phase": phase,
                        "argo_namespace": _self.namespace,
                        "argo_started_at": status_blob.get("startedAt"),
                        "argo_finished_at": status_blob.get("finishedAt"),
                    }
                    if status_blob.get("message"):
                        md["argo_message"] = str(status_blob["message"])
                    cls = AssetObservation if _self.asset_event_type == "observation" else AssetMaterialization
                    asset_events.append(cls(asset_key=ak, description=desc, metadata=md))

                return SensorResult(
                    run_requests=[RunRequest(
                        run_key=wf_name,
                        tags={
                            "argo/workflow_name": wf_name,
                            "argo/phase": phase,
                            "argo/namespace": _self.namespace,
                        },
                    )],
                    asset_events=asset_events or None,
                    cursor=wf_name,
                )

            if phase in ARGO_TERMINAL_FAIL:
                return SensorResult(
                    skip_reason=(
                        f"Workflow {wf_name} terminal failure: {phase} "
                        f"({status_blob.get('message') or 'no message'})"
                    ),
                )

            return SensorResult(skip_reason=f"Workflow {wf_name} phase: {phase}")

        return dg.Definitions(sensors=[argo_workflow_sensor])
