"""Prefect Flow Run Sensor — Dagster sensor watching completed Prefect flow runs.

Polls the Prefect API for flow runs that entered a terminal state since
the last tick and launches a Dagster job for each. Use it when Prefect
owns some upstream work (durable execution, runtime task graph) and
Dagster wants to consume the result into its asset catalog.

Works against local Prefect server or Prefect Cloud.

Docs: https://docs.prefect.io/latest/develop/read-flows/
"""
import os
from datetime import datetime, timedelta, timezone
from typing import List, Optional

import dagster as dg
from pydantic import Field


class PrefectFlowRunSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Sensor that fires when Prefect flow runs enter a terminal state.

    Example — fire a Dagster job for every completed run of a specific deployment:

        ```yaml
        type: dagster_community_components.PrefectFlowRunSensorComponent
        attributes:
          sensor_name: on_document_parsed
          job_name: index_parsed_document_job
          deployment_name: "document-parser/main"    # filter
          states: [COMPLETED]                        # only successes; add FAILED to catch failures
          api_url: http://127.0.0.1:4200/api
          minimum_interval_seconds: 15
          partition_mode: dynamic_partition
          dynamic_partitions_name: prefect_flow_runs
          # partition_key_template — how to derive the Dagster partition_key
          # from a flow_run. Available: {flow_run_id}, {flow_name}, {deployment_name}.
          partition_key_template: "{flow_run_id}"
        ```
    """

    sensor_name: str = Field(description="Sensor name.")
    job_name: str = Field(description="Dagster job launched when a flow run finishes.")

    # Filters
    flow_name: Optional[str] = Field(
        default=None,
        description="Only fire for runs of this flow name. Combined with deployment_name AND-style.",
    )
    deployment_name: Optional[str] = Field(
        default=None,
        description="Only fire for runs of this deployment. Combined with flow_name AND-style.",
    )
    states: List[str] = Field(
        default_factory=lambda: ["COMPLETED"],
        description=(
            "Prefect state types to react to. Choose from COMPLETED, FAILED, "
            "CRASHED, CANCELLED. Default: only COMPLETED (success)."
        ),
    )
    look_back_minutes: int = Field(
        default=60,
        description=(
            "How far back to scan Prefect for terminal flow runs on the FIRST "
            "tick (before a cursor is established). Subsequent ticks use the "
            "cursor timestamp."
        ),
    )

    # Connection
    api_url: str = Field(default="http://127.0.0.1:4200/api")
    api_key_env_var: Optional[str] = Field(default=None)

    # Cadence
    minimum_interval_seconds: int = Field(default=30)
    default_status: str = Field(default="running", description="'running' | 'stopped'.")

    # Partition mode — same pattern as filesystem_monitor
    partition_mode: str = Field(
        default="run_config",
        description=(
            "'run_config' → emit flow_run info as run_config (legacy); "
            "'static_partition' → RunRequest(partition_key=<from template>); "
            "'dynamic_partition' → also registers each key on the given "
            "DynamicPartitionsDefinition first."
        ),
    )
    partition_key_template: str = Field(
        default="{flow_run_id}",
        description=(
            "Template for the partition_key per flow run. Available fields: "
            "{flow_run_id}, {flow_name}, {deployment_name}."
        ),
    )
    dynamic_partitions_name: Optional[str] = Field(default=None)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        from dagster._core.definitions.sensor_definition import DefaultSensorStatus
        default_status = (
            DefaultSensorStatus.RUNNING if self.default_status.lower() == "running"
            else DefaultSensorStatus.STOPPED
        )

        if self.partition_mode == "dynamic_partition" and not self.dynamic_partitions_name:
            raise ValueError(
                "PrefectFlowRunSensorComponent: partition_mode='dynamic_partition' "
                "requires dynamic_partitions_name."
            )

        def _apply_env():
            os.environ["PREFECT_API_URL"] = _self.api_url
            if _self.api_key_env_var:
                key = os.environ.get(_self.api_key_env_var)
                if key:
                    os.environ["PREFECT_API_KEY"] = key

        @dg.sensor(
            name=self.sensor_name,
            minimum_interval_seconds=self.minimum_interval_seconds,
            default_status=default_status,
            job_name=self.job_name,
        )
        def _prefect_sensor(context: dg.SensorEvaluationContext):
            _apply_env()
            from prefect.client.orchestration import get_client
            from prefect.client.schemas.filters import (
                FlowRunFilter, FlowRunFilterState, FlowRunFilterStateType,
                FlowRunFilterEndTime, FlowFilter, FlowFilterName,
                DeploymentFilter, DeploymentFilterName,
            )
            from prefect.client.schemas.objects import StateType

            # Cursor = latest end_time seen. Fall back to look_back_minutes.
            if context.cursor:
                try:
                    cursor_dt = datetime.fromisoformat(context.cursor)
                except Exception:
                    cursor_dt = datetime.now(timezone.utc) - timedelta(minutes=_self.look_back_minutes)
            else:
                cursor_dt = datetime.now(timezone.utc) - timedelta(minutes=_self.look_back_minutes)

            wanted_states = []
            for s in _self.states:
                try:
                    wanted_states.append(StateType(s))
                except Exception:
                    context.log.warning(f"unknown Prefect state {s!r}, skipping")

            import asyncio

            async def _fetch():
                async with get_client() as client:
                    flow_filter = None
                    if _self.flow_name:
                        flow_filter = FlowFilter(name=FlowFilterName(any_=[_self.flow_name]))
                    dep_filter = None
                    if _self.deployment_name:
                        dep_filter = DeploymentFilter(name=DeploymentFilterName(any_=[_self.deployment_name]))
                    run_filter = FlowRunFilter(
                        state=FlowRunFilterState(
                            type=FlowRunFilterStateType(any_=wanted_states),
                        ) if wanted_states else None,
                        end_time=FlowRunFilterEndTime(after_=cursor_dt),
                    )
                    return await client.read_flow_runs(
                        flow_filter=flow_filter,
                        deployment_filter=dep_filter,
                        flow_run_filter=run_filter,
                        limit=200,
                        sort="END_TIME_ASC",
                    )

            flow_runs = asyncio.run(_fetch())
            if not flow_runs:
                return dg.SensorResult(skip_reason="no new terminal flow runs")

            # Build a name lookup for flow + deployment (needed for the template).
            # For MVP, we just use the flow_run's own fields.
            run_requests = []
            new_partition_keys: List[str] = []
            latest_end = cursor_dt
            for fr in flow_runs:
                if fr.end_time and fr.end_time > latest_end:
                    latest_end = fr.end_time
                key = _self.partition_key_template.format(
                    flow_run_id=str(fr.id),
                    flow_name=str(getattr(fr, "flow_name", "") or ""),
                    deployment_name=(_self.deployment_name or ""),
                )
                if _self.partition_mode in ("static_partition", "dynamic_partition"):
                    new_partition_keys.append(key)
                    run_requests.append(dg.RunRequest(
                        run_key=str(fr.id),
                        partition_key=key,
                    ))
                else:
                    run_requests.append(dg.RunRequest(
                        run_key=str(fr.id),
                        run_config={"ops": {"config": {
                            "flow_run_id": str(fr.id),
                            "flow_run_name": str(getattr(fr, "name", "")),
                            "state_type": getattr(getattr(fr, "state_type", None), "value", ""),
                            "deployment_name": _self.deployment_name or "",
                            "end_time": fr.end_time.isoformat() if fr.end_time else "",
                        }}},
                    ))

            dynamic_requests = []
            if new_partition_keys and _self.partition_mode == "dynamic_partition" and _self.dynamic_partitions_name:
                dynamic_requests = [dg.AddDynamicPartitionsRequest(
                    partitions_def_name=_self.dynamic_partitions_name,
                    partition_keys=new_partition_keys,
                )]

            context.log.info(
                f"picked up {len(run_requests)} completed Prefect flow run(s); "
                f"new cursor={latest_end.isoformat()}"
            )
            return dg.SensorResult(
                run_requests=run_requests,
                cursor=latest_end.isoformat(),
                dynamic_partitions_requests=dynamic_requests,
            )

        return dg.Definitions(sensors=[_prefect_sensor])
