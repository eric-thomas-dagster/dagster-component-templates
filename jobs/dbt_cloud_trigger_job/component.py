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



# BEGIN field-shape normalization (inlined per component; keep copies identical — see tests/test_field_shapes.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def coerce_field_shapes(data, *, lists=(), csv=(), tags=(), strs=()):
    """Accept the shapes people (and LLMs) naturally write for common boilerplate fields."""
    if not isinstance(data, dict):
        return data
    out = dict(data)
    for k in lists:  # List[str]: "a" / "a, b" -> ["a", "b"]; strip + drop empties
        v = out.get(k)
        if isinstance(v, str):
            v = v.split(",")
        if isinstance(v, (list, tuple)):
            out[k] = [x.strip() if isinstance(x, str) else x for x in v if not (isinstance(x, str) and not x.strip())]
    for k in csv:  # comma-separated str given as a list -> "a,b"
        v = out.get(k)
        if isinstance(v, (list, tuple, set)):
            out[k] = ",".join(str(x).strip() for x in v)
    for k in tags:  # Dict[str, str]: scalars -> str, ["k=v"] / ["k:v"] -> {k: v}
        v = out.get(k)
        if isinstance(v, (list, tuple)) and v and all(isinstance(x, str) and ("=" in x or ":" in x) for x in v):
            v = dict((p.strip() for p in (x.split("=", 1) if "=" in x else x.split(":", 1))) for x in v)
        if isinstance(v, dict):
            out[k] = {str(a).strip(): (str(b).lower() if isinstance(b, bool) else str(b)) for a, b in v.items() if b is not None}
    for k in strs:  # str (or Dict[str, str]) fields: a numeric-looking YAML scalar resolves to int/float/bool
        v = out.get(k)
        if isinstance(v, bool):
            out[k] = "true" if v else "false"
        elif isinstance(v, (int, float)):
            out[k] = str(v)
        elif isinstance(v, dict):
            out[k] = {str(a): (("true" if b is True else "false") if isinstance(b, bool) else (str(b) if isinstance(b, (int, float)) else b)) for a, b in v.items()}
    return out
# END field-shape normalization


class DbtCloudTriggerJobComponent(dg.Component, dg.Model, dg.Resolvable):
    """Kick off a dbt Cloud job from a Dagster op-job.

    Wraps `DbtCloudResource.run_job_and_poll` — synchronous by default
    (waits until the dbt Cloud run terminates before the Dagster op
    completes). Set `wait_for_completion: false` for async fire-and-
    forget behavior (returns as soon as dbt Cloud accepts the trigger).
    """

    job_name: Optional[str] = Field(
        default=None,
        description="Dagster op-job name. Use this in sensor/schedule "
        "`job_name:` fields to target this job for triggering. Required "
        "unless `asset_name` is set.",
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
        "attaches a `ScheduleDefinition` that triggers this job on cron. "
        "Ignored when `asset_name` is set (use an automation condition or "
        "a separate schedule/sensor targeting the asset instead).",
    )
    default_status: str = Field(
        default="STOPPED",
        description="Initial schedule status: STOPPED or RUNNING.",
    )
    tags: Optional[Union[Dict[str, Any], List[str]]] = Field(
        default=None,
        description="Dagster job tags — applied to every run of this job.",
    )
    asset_name: Optional[str] = Field(
        default=None,
        description="If set, this component emits a real materializable "
        "`@dg.asset` (with `deps`) instead of a plain op-job — so the "
        "triggered dbt Cloud run can sit in the asset graph with upstream "
        "dependencies, be targeted by automation conditions, etc. `job_name` "
        "is ignored when this is set (no op-job/schedule is created). Richer "
        "per-run trigger fields (`cause`/`schema_override`/`git_branch`/"
        "`git_sha`) are still forwarded via the real dbt Cloud REST API's "
        "trigger-run endpoint; `enriched_dbt_cloud_workspace`'s own "
        "`mirror_jobs: executable` mode only forwards `steps_override` to "
        "the newer Cloud v2 client, so use this instead when you need those.",
    )
    deps: Optional[List[str]] = Field(
        default=None,
        description="Upstream asset keys (slash-separated form, e.g. "
        "'raw/snowpipe_landing'). Only used when `asset_name` is set.",
    )
    group_name: Optional[str] = Field(
        default=None, description="Asset group name. Only used when `asset_name` is set.",
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('deps',), csv=(), tags=('tags',))

    @model_validator(mode="after")
    def _require_job_name_or_asset_name(self):
        if not self.asset_name and not self.job_name:
            raise ValueError("Set either `job_name` (op-job shape) or `asset_name` (asset shape).")
        return self

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='Dbt Cloud Trigger Job', editable=True)

    def _run_trigger(self, context, resource) -> Dict[str, Any]:
        """Trigger + (optionally) poll the dbt Cloud job; return a metadata dict.
        Shared by the op-job shape and the asset shape -- same trigger logic either way."""
        _self = self

        # Build **kwargs for the dbt Cloud API's `run_job` / `run_job_and_poll`
        # -- only forward fields the user actually set, so dbt Cloud's own
        # defaults win otherwise.
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
            context.log.info(f"dbt Cloud run {run_id} finished: status={status!r}")

            metadata = {
                "dbt_cloud/run_id": run_id,
                "dbt_cloud/run_url": dg.MetadataValue.url(run_url) if run_url else "n/a",
                "dbt_cloud/job_id": _self.dbt_cloud_job_id,
                "dbt_cloud/status": status,
            }

            # Emit per-model materialization events so this run edges into
            # the asset graph. `context.log_event` doesn't require declaring
            # these as outputs on the op -- cleaner than yielding
            # AssetMaterialization from a plain @op. (Only meaningful in the
            # op-job shape; the asset shape already IS an asset.)
            for key_str in _self.emit_materializations_for or []:
                context.log_event(
                    dg.AssetMaterialization(
                        asset_key=dg.AssetKey.from_user_string(key_str),
                        description=(
                            f"Materialized via dbt Cloud job {_self.dbt_cloud_job_id} "
                            f"(run {run_id})"
                        ),
                        metadata={
                            "dbt_cloud/run_id": run_id,
                            "dbt_cloud/run_url": dg.MetadataValue.url(run_url) if run_url else "n/a",
                            "dbt_cloud/status": status,
                        },
                    )
                )
            return metadata
        else:
            context.log.info(
                f"Triggering dbt Cloud job {_self.dbt_cloud_job_id} (async — not waiting)"
            )
            run = resource.run_job(job_id=_self.dbt_cloud_job_id, **kwargs)
            run_id = run.get("id") if isinstance(run, dict) else None
            return {
                "dbt_cloud/run_id": run_id,
                "dbt_cloud/job_id": _self.dbt_cloud_job_id,
                "dbt_cloud/mode": "async",
            }

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        resource_key = self.dbt_cloud_resource_key

        if self.asset_name:
            @dg.asset(
                key=dg.AssetKey.from_user_string(self.asset_name),
                group_name=self.group_name,
                deps=[dg.AssetKey.from_user_string(d) for d in (self.deps or [])],
                required_resource_keys={resource_key},
                description=f"Triggers dbt Cloud job {self.dbt_cloud_job_id} and waits for completion.",
            )
            def _dbt_cloud_trigger_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
                resource = getattr(context.resources, resource_key)
                metadata = _self._run_trigger(context, resource)
                return dg.MaterializeResult(metadata=metadata)

            return dg.Definitions(assets=[_dbt_cloud_trigger_asset])

        @dg.op(
            name=f"{self.job_name}_op",
            required_resource_keys={resource_key},
        )
        def _trigger_op(context: dg.OpExecutionContext) -> None:
            resource = getattr(context.resources, resource_key)
            metadata = _self._run_trigger(context, resource)
            context.add_output_metadata(metadata)

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
