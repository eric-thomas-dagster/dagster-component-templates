"""OpenaiBatchStatusSensorComponent.

Watches an `openai_batch_submit`-produced asset's materialization metadata
for a `batch_id`, polls OpenAI's Batch API for that batch's LIVE current
status (never trusting the possibly-stale status stamped in the watched
asset's own metadata -- same principle as qlik_compose_workflow_status_sensor
re-polling live state rather than trusting cached state), and fires a
RunRequest against `job_name` once the batch reaches a terminal state.

Structural template: sensors/qlik_compose_workflow_status_sensor/component.py
(same shape: @sensor + SensorResult + cursor-based dedup on a fingerprint).

A note on `results_asset_key` and why it exists (a deliberate addition beyond
the bare qlik template): a naive port of qlik's sensor emits
`run_config={"ops": {"config": {"batch_id": batch_id}}}` -- but `"config"` is
only a valid key there if the target op is literally named `"config"`. For
an `@asset`-backed op (like openai_batch_results), the real op name is the
asset's key rendered through `AssetKey.to_python_identifier()` (e.g.
`"ai__openai_batch_results"` for key `ai/openai_batch_results`), not the
literal string `"config"`. Using the literal-`"config"` shape here would
silently produce a run where `context.op_config`/the asset's `config` param
is always empty -- the precise bug this repo already found and fixed in
`sensors/precisely_job_sensor/component.py` (see its comment: "The old
run_config={'ops': {'config': {...}}} treated the literal string 'config' as
an op name, so downstream ops' context.op_config was always empty"). Rather
than resurrect that bug, `results_asset_key` carries the actual target
asset's key so the correct op name can be derived automatically.
"""
import os
from typing import Any, Optional

import dagster as dg
from dagster._core.definitions.sensor_definition import DefaultSensorStatus
from pydantic import Field

_TERMINAL_STATUSES = {"completed", "failed", "expired", "cancelled"}


def _build_openai_client(api_key: str) -> Any:
    """Isolated client construction -- tests monkeypatch
    `openai_batch_status_sensor_component._build_openai_client` to return a
    fake client instead of calling the real (paid) API."""
    from openai import OpenAI

    return OpenAI(api_key=api_key)


class OpenaiBatchStatusSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Trigger a Dagster job once a watched OpenAI batch reaches a terminal state.

    Example:
        ```yaml
        type: dagster_component_templates.OpenaiBatchStatusSensorComponent
        attributes:
          sensor_name: support_reply_batch_done
          watch_asset_key: support_reply_batch
          job_name: fetch_support_reply_results
          results_asset_key: support_reply_results
        ```
    """

    sensor_name: str = Field(description="Unique sensor name.")
    watch_asset_key: str = Field(description="The openai_batch_submit asset to watch for a batch_id in its materialization metadata.")
    job_name: str = Field(description="Job to trigger once the watched batch reaches a terminal state.")
    results_asset_key: str = Field(
        description=(
            "Asset key of the openai_batch_results asset this sensor's RunRequest targets. Used only "
            "to derive the correct op name (via AssetKey.to_python_identifier()) for run_config -- not "
            "to look up anything at sensor-eval time."
        ),
    )
    api_key_env_var: str = Field(default="OPENAI_API_KEY", description="Env var holding the OpenAI API key.")
    minimum_interval_seconds: int = Field(default=60, description="Minimum seconds between sensor evaluations.")
    default_status: str = Field(default="running", description="'running' or 'stopped' -- initial sensor status.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        default_status = (
            DefaultSensorStatus.RUNNING if self.default_status == "running"
            else DefaultSensorStatus.STOPPED
        )
        watch_key = dg.AssetKey.from_user_string(self.watch_asset_key)
        op_name = dg.AssetKey.from_user_string(self.results_asset_key).to_python_identifier()

        @dg.sensor(
            name=_self.sensor_name,
            minimum_interval_seconds=_self.minimum_interval_seconds,
            default_status=default_status,
            job_name=_self.job_name,
        )
        def openai_batch_status_sensor(context: dg.SensorEvaluationContext):
            event = context.instance.get_latest_materialization_event(watch_key)
            if not event or not event.asset_materialization:
                return dg.SensorResult(skip_reason=f"No materialization found for {_self.watch_asset_key}.")

            md = event.asset_materialization.metadata
            batch_id_mv = md.get("batch_id")
            if not batch_id_mv:
                return dg.SensorResult(skip_reason=f"Latest materialization of {_self.watch_asset_key} has no batch_id metadata.")
            batch_id = batch_id_mv.text
            if not batch_id:
                return dg.SensorResult(skip_reason=f"Latest materialization of {_self.watch_asset_key} has an empty batch_id.")

            api_key = os.environ.get(_self.api_key_env_var)
            if not api_key:
                return dg.SensorResult(skip_reason=f"{_self.api_key_env_var} not set.")

            client = _build_openai_client(api_key)
            try:
                batch = client.batches.retrieve(batch_id)
            except Exception as e:
                return dg.SensorResult(skip_reason=f"batches.retrieve({batch_id!r}) failed: {e}")

            status = batch.status
            if status not in _TERMINAL_STATUSES:
                return dg.SensorResult(skip_reason=f"Batch {batch_id} status={status!r} (not terminal yet).")

            fingerprint = f"{batch_id}|{status}"
            cursor = context.cursor or ""
            if fingerprint == cursor:
                return dg.SensorResult(skip_reason=f"Already processed {fingerprint}.")

            return dg.SensorResult(
                run_requests=[
                    dg.RunRequest(
                        run_key=fingerprint,
                        run_config={"ops": {op_name: {"config": {"batch_id": batch_id}}}},
                    )
                ],
                cursor=fingerprint,
            )

        return dg.Definitions(sensors=[openai_batch_status_sensor])
