"""Temporal Workflow Sensor Component.

Polls Temporal via the ``temporalio`` SDK for the terminal status of a
Workflow (by workflow_id) or the most-recently-closed workflow matching a
List filter, and emits an AssetMaterialization / AssetObservation on
terminal success. Fires a Dagster job on the same terminal-success event.

Workflow status codes (temporalio.client.WorkflowExecutionStatus):
  - COMPLETED   → success
  - FAILED      → terminal failure
  - CANCELED    → terminal failure
  - TERMINATED  → terminal failure
  - TIMED_OUT   → terminal failure
  - CONTINUED_AS_NEW → treat as success (chained run)
In-progress: RUNNING.

Auth: local dev-server (no auth), Temporal Cloud API key, or mTLS. Same
as ``temporal_workflow_trigger``.

Pair with ``external_temporal_workflow`` (Case A — Temporal owns the schedule)
or ``temporal_workflow_trigger`` with ``asset_event_type=observation``
(Case B — Dagster fires + observes).

Docs: https://docs.temporal.io/develop/python
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

TEMPORAL_SUCCESS = {"COMPLETED", "CONTINUED_AS_NEW"}
TEMPORAL_FAIL = {"FAILED", "CANCELED", "TERMINATED", "TIMED_OUT"}


class TemporalWorkflowSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Fire a Dagster job when a Temporal Workflow reaches a terminal state.

    Two modes — set exactly one of ``workflow_id`` or ``list_filter``:

      - ``workflow_id`` — watch one specific workflow by ID. Useful when
        Dagster knows the workflow_id (e.g. it triggered it or it's fixed
        per Temporal Schedule).

      - ``list_filter`` — Temporal Visibility List filter, e.g.
        ``WorkflowType='NightlyAggregationWorkflow' AND ExecutionStatus!='Running'``
        — sensor picks the most-recently-closed matching workflow.

    Example:
        ```yaml
        type: dagster_community_components.TemporalWorkflowSensorComponent
        attributes:
          sensor_name: temporal_nightly_done
          list_filter: "WorkflowType='NightlyAggregationWorkflow' AND ExecutionStatus!='Running'"
          target_host: localhost:7233
          namespace: default
          job_name: downstream_processing_job
          asset_key: temporal/etl/nightly_aggregation
        ```
    """

    sensor_name: str = Field(description="Unique sensor name.")
    workflow_id: Optional[str] = Field(
        default=None,
        description="Specific Temporal Workflow ID to watch. Set this OR list_filter.",
    )
    list_filter: Optional[str] = Field(
        default=None,
        description=(
            "Temporal Visibility SQL-ish filter, e.g. "
            "'WorkflowType=\"NightlyAggWorkflow\" AND ExecutionStatus!=\"Running\"'. "
            "Sensor watches the most-recently-closed matching workflow. Set this OR workflow_id."
        ),
    )
    job_name: str = Field(description="Dagster job to trigger on terminal success.")
    asset_key: Optional[str] = Field(
        default=None,
        description=(
            "Optional Dagster asset key. When set, sensor emits AssetMaterialization "
            "(default) or AssetObservation on terminal success. '/'-separated."
        ),
    )
    asset_event_type: str = Field(
        default="materialization",
        description=(
            "'materialization' (default) or 'observation'. Use 'observation' when "
            "paired with temporal_workflow_trigger to avoid double-counting materializations."
        ),
    )

    target_host: str = Field(
        default="localhost:7233",
        description="Temporal frontend host:port.",
    )
    namespace: str = Field(default="default", description="Temporal namespace.")
    api_key_env_var: Optional[str] = Field(
        default=None,
        description="Env var with Temporal Cloud API key.",
    )
    tls_cert_env_var: Optional[str] = Field(
        default=None,
        description="mTLS client cert env var (pair with tls_key_env_var).",
    )
    tls_key_env_var: Optional[str] = Field(
        default=None,
        description="mTLS client private key env var.",
    )
    tls_server_root_ca_env_var: Optional[str] = Field(
        default=None,
        description="Env var with a PEM-encoded custom root CA.",
    )

    minimum_interval_seconds: int = Field(default=60, description="Seconds between polls.")
    default_status: str = Field(default="running", description="'running' or 'stopped'.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        if bool(self.workflow_id) == bool(self.list_filter):
            raise ValueError(
                f"temporal_workflow_sensor {self.sensor_name!r}: set exactly one of "
                f"`workflow_id` or `list_filter`."
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
        def temporal_workflow_sensor(context: SensorEvaluationContext):
            import asyncio
            import os

            try:
                from temporalio.client import Client
                from temporalio.service import TLSConfig
            except ImportError:
                return SensorResult(skip_reason="temporalio not installed")

            async def _poll():
                tls_cfg = None
                if _self.tls_cert_env_var and _self.tls_key_env_var:
                    cert = os.environ.get(_self.tls_cert_env_var, "").encode()
                    key = os.environ.get(_self.tls_key_env_var, "").encode()
                    root = None
                    if _self.tls_server_root_ca_env_var:
                        root = os.environ.get(_self.tls_server_root_ca_env_var, "").encode() or None
                    tls_cfg = TLSConfig(
                        client_cert=cert or None,
                        client_private_key=key or None,
                        server_root_ca_cert=root,
                    )
                elif _self.api_key_env_var:
                    tls_cfg = True

                client_kwargs = {"target_host": _self.target_host, "namespace": _self.namespace}
                if tls_cfg is not None:
                    client_kwargs["tls"] = tls_cfg
                if _self.api_key_env_var:
                    ak = os.environ.get(_self.api_key_env_var)
                    if not ak:
                        return None, f"api_key_env_var {_self.api_key_env_var!r} not set"
                    client_kwargs["api_key"] = ak

                client = await Client.connect(**client_kwargs)

                if _self.workflow_id:
                    handle = client.get_workflow_handle(_self.workflow_id)
                    desc = await handle.describe()
                    return desc, None

                # List filter — pick the most-recently-closed matching workflow.
                latest = None
                async for wf in client.list_workflows(query=_self.list_filter, limit=10):
                    if latest is None or (wf.close_time and (latest.close_time is None or wf.close_time > latest.close_time)):
                        latest = wf
                return latest, None

            try:
                wf, err = asyncio.run(_poll())
            except Exception as e:  # noqa: BLE001
                return SensorResult(skip_reason=f"Temporal poll error: {e}")

            if err:
                return SensorResult(skip_reason=err)
            if not wf:
                return SensorResult(skip_reason="No matching workflow yet.")

            # WorkflowExecutionStatus is an enum; .name is the string.
            status_name = getattr(getattr(wf, "status", None), "name", None) or str(getattr(wf, "status", ""))
            wf_id = getattr(wf, "id", None) or getattr(wf, "workflow_id", None) or "(unknown)"
            run_id = getattr(wf, "run_id", None) or ""
            close_time = getattr(wf, "close_time", None)

            cursor = context.cursor or ""
            cursor_key = f"{wf_id}:{run_id}"

            if status_name in TEMPORAL_SUCCESS and cursor_key != cursor:
                asset_events = []
                if _self.asset_key:
                    ak = dg.AssetKey(_self.asset_key.split("/"))
                    md = {
                        "temporal_workflow_id": wf_id,
                        "temporal_run_id": run_id,
                        "temporal_status": status_name,
                        "temporal_namespace": _self.namespace,
                    }
                    if close_time is not None:
                        md["temporal_close_time"] = str(close_time)
                    cls = AssetObservation if _self.asset_event_type == "observation" else AssetMaterialization
                    asset_events.append(
                        cls(
                            asset_key=ak,
                            description=f"Temporal workflow {wf_id} → {status_name}",
                            metadata=md,
                        )
                    )
                return SensorResult(
                    run_requests=[RunRequest(
                        run_key=cursor_key,
                        tags={
                            "temporal/workflow_id": wf_id,
                            "temporal/run_id": run_id,
                            "temporal/status": status_name,
                            "temporal/namespace": _self.namespace,
                        },
                    )],
                    asset_events=asset_events or None,
                    cursor=cursor_key,
                )

            if status_name in TEMPORAL_FAIL:
                return SensorResult(skip_reason=f"Workflow {wf_id} terminal failure: {status_name}")

            return SensorResult(skip_reason=f"Workflow {wf_id} status: {status_name}")

        return dg.Definitions(sensors=[temporal_workflow_sensor])
