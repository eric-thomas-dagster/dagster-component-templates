"""DbtCloudTriggerJobComponent.

Trigger a dbt Cloud job run from a Dagster op-job — the missing "Dagster
kicks off dbt Cloud" primitive.

The community registry today has:

  - `dbt_cloud_resource` — registers `dagster_dbt.DbtCloudResource`.
  - `dbt_cloud_job_sensor` — polls dbt Cloud → triggers a DAGSTER job
     when a dbt Cloud run completes (dbt Cloud → Dagster direction).
  - `dbt_run_job` — runs dbt CORE via subprocess. Not dbt Cloud.

None of those cover the case of "Dagster observes an upstream event
(e.g. a Snowpipe load), and wants to kick off a dbt Cloud job." This
component wraps `DbtCloudResource.run_job_and_poll(job_id, ...)` behind
a Dagster op-job so YAML gets you:

    type: dagster_community_components.DbtCloudTriggerJobComponent
    attributes:
      job_name: dbt_build_scenario1_job
      dbt_cloud_job_id: 67890
      dbt_cloud_resource_key: dbt_cloud_resource
      wait_for_completion: true
      cause: "Triggered by Dagster on Snowpipe load event"

Downstream, any Dagster mechanism that targets an op-job by name can
invoke this — sensors, schedules, run-launcher, human triggers in the
UI. Combined with `snowflake_workspace polling_sensor: true` on the
Snowpipe side, this closes the "trigger dbt Cloud on Snowpipe load
evidence" pattern with pure YAML.

Emits per-model `AssetMaterialization` events for keys listed in
`emit_materializations_for` so the dbt Cloud run participates in the
Dagster asset graph even though the job shape doesn't declare assets.
"""

from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


class DbtCloudTriggerJobComponent(dg.Component, dg.Model, dg.Resolvable):
    """Kick off a dbt Cloud job from a Dagster op-job.

    Wraps `DbtCloudResource.run_job_and_poll` — synchronous by default
    (waits until the dbt Cloud run terminates before the Dagster op
    completes). Set `wait_for_completion: false` for async fire-and-
    forget behavior (returns as soon as dbt Cloud accepts the trigger).
    """

    job_name: str = Field(
        description="Dagster op-job name. Use this in sensor/schedule "
        "`job_name:` fields to target this job for triggering."
    )
    dbt_cloud_job_id: int = Field(
        description="dbt Cloud job ID to trigger (numeric — from the dbt "
        "Cloud UI URL or the dbt Cloud REST API)."
    )
    dbt_cloud_resource_key: str = Field(
        default="dbt_cloud_resource",
        description="Resource key of a registered `dagster_dbt.DbtCloudResource` "
        "(commonly via the `dbt_cloud_resource` community component).",
    )
    wait_for_completion: bool = Field(
        default=True,
        description="If true, the op blocks until the dbt Cloud run "
        "terminates (uses `run_job_and_poll`). If false, the op returns "
        "as soon as dbt Cloud accepts the trigger (uses `run_job`).",
    )
    poll_interval_seconds: float = Field(
        default=10.0,
        description="Seconds between polls to dbt Cloud for run status. "
        "Ignored when `wait_for_completion: false`.",
    )
    poll_timeout_seconds: Optional[float] = Field(
        default=None,
        description="Overall timeout (seconds) for a synchronous run. "
        "None = no timeout (dbt Cloud runs can be long). Ignored when "
        "`wait_for_completion: false`.",
    )
    cause: str = Field(
        default="Triggered by Dagster",
        description="dbt Cloud's `cause` field on the run — a short string "
        "shown in the dbt Cloud UI explaining WHY the run started.",
    )
    steps_override: Optional[List[str]] = Field(
        default=None,
        description="dbt Cloud per-run `steps_override` — replaces the job's "
        "configured steps for this run only. E.g. `['dbt build --select tag:hourly']`.",
    )
    schema_override: Optional[str] = Field(
        default=None,
        description="dbt Cloud per-run `schema_override` — target schema for "
        "this run only. Useful for branch-deployment isolation.",
    )
    git_branch: Optional[str] = Field(
        default=None,
        description="dbt Cloud per-run `git_branch` — run against this branch "
        "instead of the job's configured branch.",
    )
    git_sha: Optional[str] = Field(
        default=None,
        description="dbt Cloud per-run `git_sha` — run against this exact commit.",
    )
    emit_materializations_for: Optional[List[str]] = Field(
        default=None,
        description="Optional list of asset keys (slash-separated form, e.g. "
        "'mart/daily_summary'). After the dbt Cloud run succeeds, the op "
        "emits one `AssetMaterialization` event per key so the run edges "
        "into Dagster's asset graph. Requires `wait_for_completion: true`.",
    )
    schedule: Optional[str] = Field(
        default=None,
        description="Optional cron string (e.g. '0 3 * * *'). If set, "
        "attaches a `ScheduleDefinition` that triggers this job on cron.",
    )
    default_status: str = Field(
        default="STOPPED",
        description="Initial schedule status: STOPPED or RUNNING.",
    )
    tags: Optional[Dict[str, str]] = Field(
        default=None,
        description="Dagster job tags — applied to every run of this job.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        resource_key = self.dbt_cloud_resource_key

        @dg.op(
            name=f"{self.job_name}_op",
            required_resource_keys={resource_key},
        )
        def _trigger_op(context: dg.OpExecutionContext) -> None:
            resource = getattr(context.resources, resource_key)

            # Build **kwargs for the dbt Cloud API's `run_job` /
            # `run_job_and_poll` — only forward fields the user actually
            # set, so dbt Cloud's own defaults win otherwise.
            kwargs: Dict[str, Any] = {"cause": _self.cause}
            if _self.steps_override is not None:
                kwargs["steps_override"] = _self.steps_override
            if _self.schema_override is not None:
                kwargs["schema_override"] = _self.schema_override
            if _self.git_branch is not None:
                kwargs["git_branch"] = _self.git_branch
            if _self.git_sha is not None:
                kwargs["git_sha"] = _self.git_sha

            if _self.wait_for_completion:
                context.log.info(
                    f"Triggering dbt Cloud job {_self.dbt_cloud_job_id} "
                    f"(waiting up to {_self.poll_timeout_seconds or 'unlimited'}s)"
                )
                output = resource.run_job_and_poll(
                    job_id=_self.dbt_cloud_job_id,
                    poll_interval=_self.poll_interval_seconds,
                    poll_timeout=_self.poll_timeout_seconds,
                    **kwargs,
                )
                # `output` is a DbtCloudOutput with .run_details (dict) +
                # .result (parsed run_results.json when available).
                run_details = getattr(output, "run_details", {}) or {}
                run_id = run_details.get("id")
                run_url = (
                    run_details.get("href")
                    or resource.build_url_for_job(_self.dbt_cloud_job_id)
                )
                status = run_details.get("status_humanized", "unknown")
                context.log.info(
                    f"dbt Cloud run {run_id} finished: status={status!r}"
                )

                context.add_output_metadata(
                    {
                        "dbt_cloud/run_id": run_id,
                        "dbt_cloud/run_url": (
                            dg.MetadataValue.url(run_url) if run_url else "n/a"
                        ),
                        "dbt_cloud/job_id": _self.dbt_cloud_job_id,
                        "dbt_cloud/status": status,
                    }
                )

                # Emit per-model materialization events so this run edges
                # into the asset graph. `context.log_event` doesn't require
                # declaring these as outputs on the op — cleaner than
                # yielding AssetMaterialization from a plain @op.
                for key_str in _self.emit_materializations_for or []:
                    context.log_event(
                        dg.AssetMaterialization(
                            asset_key=dg.AssetKey.from_user_string(key_str),
                            description=(
                                f"Materialized via dbt Cloud job "
                                f"{_self.dbt_cloud_job_id} (run {run_id})"
                            ),
                            metadata={
                                "dbt_cloud/run_id": run_id,
                                "dbt_cloud/run_url": (
                                    dg.MetadataValue.url(run_url)
                                    if run_url
                                    else "n/a"
                                ),
                                "dbt_cloud/status": status,
                            },
                        )
                    )
            else:
                context.log.info(
                    f"Triggering dbt Cloud job {_self.dbt_cloud_job_id} "
                    f"(async — not waiting)"
                )
                run = resource.run_job(job_id=_self.dbt_cloud_job_id, **kwargs)
                run_id = run.get("id") if isinstance(run, dict) else None
                context.add_output_metadata(
                    {
                        "dbt_cloud/run_id": run_id,
                        "dbt_cloud/job_id": _self.dbt_cloud_job_id,
                        "dbt_cloud/mode": "async",
                    }
                )

        @dg.job(name=self.job_name, tags=self.tags or None)
        def _the_job() -> None:
            _trigger_op()

        defs_kwargs: Dict[str, Any] = {"jobs": [_the_job]}

        if self.schedule:
            default_status = (
                dg.DefaultScheduleStatus.RUNNING
                if self.default_status.upper() == "RUNNING"
                else dg.DefaultScheduleStatus.STOPPED
            )
            defs_kwargs["schedules"] = [
                dg.ScheduleDefinition(
                    name=f"{self.job_name}_schedule",
                    cron_schedule=self.schedule,
                    job=_the_job,
                    default_status=default_status,
                )
            ]

        return dg.Definitions(**defs_kwargs)
