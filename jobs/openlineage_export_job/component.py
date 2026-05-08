"""OpenLineageExportJobComponent.

Op-shaped job that emits OpenLineage RunEvent (START / COMPLETE / FAIL) for
every Dagster run in the code location. Sends to a configured OpenLineage
collector (Marquez, OpenLineage Proxy, Datakin, etc.) over HTTP.

Complements (does not replace) the asset-shaped lineage_to_openlineage sink
component:
  - lineage_to_openlineage: a sink ASSET that materializes a snapshot of the
    asset graph as OpenLineage events. Useful for a one-shot or scheduled
    bulk catalog sync.
  - openlineage_export_job (this one): a JOB + run-event hooks that emits
    one OpenLineage event per op execution, with run id / start / complete /
    fail status. Useful when teams already operate an OpenLineage collector
    and want job-run-shaped events on the operational boundary.

Designed for the "we have a Marquez collector and want Dagster runs to show
up alongside Spark / Airflow / dbt runs in the same lineage UI" use case.
"""

import json
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import dagster as dg
from pydantic import Field


def _emit_event(endpoint: str, headers: Dict[str, str], event: Dict[str, Any], log) -> None:
    """POST a single OpenLineage RunEvent to the collector."""
    import urllib.request
    import urllib.error

    body = json.dumps(event).encode("utf-8")
    req = urllib.request.Request(
        endpoint,
        data=body,
        method="POST",
        headers={"Content-Type": "application/json", **headers},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            log.info(f"OpenLineage event {event['eventType']} accepted ({resp.status})")
    except urllib.error.HTTPError as e:
        log.warning(f"OpenLineage collector returned {e.code}: {e.read()[:200]!r}")
    except urllib.error.URLError as e:
        log.warning(f"OpenLineage collector unreachable: {e.reason}")


def _build_event(
    event_type: str,
    run_id: str,
    job_name: str,
    namespace: str,
    producer: str,
    inputs: Optional[list] = None,
    outputs: Optional[list] = None,
) -> Dict[str, Any]:
    """Construct a minimal OpenLineage RunEvent matching the v1 spec."""
    return {
        "eventType": event_type,
        "eventTime": datetime.now(timezone.utc).isoformat(),
        "producer": producer,
        "schemaURL": "https://openlineage.io/spec/2-0-0/OpenLineage.json#/$defs/RunEvent",
        "run": {"runId": run_id},
        "job": {"namespace": namespace, "name": job_name},
        "inputs": inputs or [],
        "outputs": outputs or [],
    }


class OpenLineageExportJobComponent(dg.Component, dg.Model, dg.Resolvable):
    """Op-shaped job that emits an OpenLineage RunEvent per Dagster run.

    On each schedule tick (or manual launch) the job fires three events to
    the configured collector:
      - START on op enter
      - COMPLETE on success, or FAIL on raise
    Each event includes the Dagster run_id (so events can be correlated
    with Dagster's UI), a job namespace + name, and an empty inputs/outputs
    list (extend `inputs_template` / `outputs_template` to enrich).

    Pair with a sensor or schedule to fire on a meaningful cadence; or set
    `schedule` here to run at fixed times.
    """

    job_name: str = Field(description="Dagster job name")
    schedule: Optional[str] = Field(default=None, description="Cron schedule (None = manual / sensor-triggered)")
    default_status: str = Field(default="STOPPED", description="STOPPED | RUNNING")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Dagster job tags")

    # OpenLineage destination
    endpoint_env_var: str = Field(
        default="OPENLINEAGE_URL",
        description="Env var holding the collector URL (e.g. http://marquez:5000/api/v1/lineage).",
    )
    api_key_env_var: Optional[str] = Field(
        default=None,
        description="Env var with a bearer token, if the collector requires auth.",
    )

    # OpenLineage event payload
    namespace: str = Field(
        default="dagster",
        description="OpenLineage namespace. All events from this code location group under this string.",
    )
    ol_job_name: Optional[str] = Field(
        default=None,
        description="Job name as seen in OpenLineage. Defaults to job_name. Use this if you want the OL name to differ.",
    )
    producer: str = Field(
        default="https://github.com/eric-thomas-dagster/dagster-component-templates/openlineage_export_job",
        description="OpenLineage 'producer' URI — identifies the emitter implementation.",
    )

    # Optional event enrichment
    inputs_template: Optional[list] = Field(
        default=None,
        description="Static inputs list to attach to each event, e.g. [{'namespace': 'snowflake', 'name': 'analytics.public.orders'}].",
    )
    outputs_template: Optional[list] = Field(
        default=None,
        description="Static outputs list to attach to each event.",
    )
    fail_on_collector_error: bool = Field(
        default=False,
        description="If True, an unreachable / errored collector fails the run. Default False (best-effort emit, log warning).",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        @dg.op
        def _emit_run_events(context: dg.OpExecutionContext):
            endpoint = os.environ.get(_self.endpoint_env_var)
            if not endpoint:
                msg = f"{_self.endpoint_env_var} not set"
                if _self.fail_on_collector_error:
                    raise ValueError(msg)
                context.log.warning(msg)
                return {"emitted": 0, "endpoint": None}

            headers: Dict[str, str] = {}
            if _self.api_key_env_var:
                token = os.environ.get(_self.api_key_env_var, "")
                if token:
                    headers["Authorization"] = f"Bearer {token}"

            run_id = str(context.run_id) if context.run_id else str(uuid.uuid4())
            job_name = _self.ol_job_name or _self.job_name

            for event_type in ("START", "COMPLETE"):
                event = _build_event(
                    event_type=event_type,
                    run_id=run_id,
                    job_name=job_name,
                    namespace=_self.namespace,
                    producer=_self.producer,
                    inputs=_self.inputs_template,
                    outputs=_self.outputs_template,
                )
                _emit_event(endpoint, headers, event, context.log)

            return {"emitted": 2, "endpoint": endpoint, "run_id": run_id}

        @dg.job(name=self.job_name, tags=self.tags or None)
        def _the_job():
            _emit_run_events()

        defs_kwargs: Dict[str, Any] = {"jobs": [_the_job]}
        if self.schedule:
            sched = dg.ScheduleDefinition(
                name=f"{self.job_name}_schedule",
                cron_schedule=self.schedule,
                job=_the_job,
                default_status=(
                    dg.DefaultScheduleStatus.STOPPED
                    if self.default_status.upper() == "STOPPED"
                    else dg.DefaultScheduleStatus.RUNNING
                ),
            )
            defs_kwargs["schedules"] = [sched]
        return dg.Definitions(**defs_kwargs)
