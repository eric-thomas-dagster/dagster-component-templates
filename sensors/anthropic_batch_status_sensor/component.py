"""Anthropic Batch Status Sensor Component.

Watches the materialization metadata of an anthropic_batch_submit asset,
re-checks the LIVE processing_status of its batch directly against the
Anthropic API (never trusting the possibly-stale metadata snapshot), and
fires a run once the batch has ended.

Structural template: sensors/qlik_compose_workflow_status_sensor/component.py
(same shape: @sensor + SensorResult + cursor-based dedup on a fingerprint).

A note on `results_asset_key` and why it exists (a deliberate addition beyond
the bare qlik template): a naive port of qlik's sensor emits
`run_config={"ops": {"config": {"batch_id": batch_id}}}` — but `"config"` is
only a valid key there if the target op is literally named `"config"`. For
an `@asset`-backed op (like anthropic_batch_results), the real op name is the
asset's key rendered through `AssetKey.to_python_identifier()` (e.g.
`"ai__anthropic_batch_results"` for key `ai/anthropic_batch_results`), not
the literal string `"config"`. Using the literal-`"config"` shape here would
silently produce a run where `context.op_config`/the asset's `config` param
is always empty — the precise bug this repo already found and fixed in
`sensors/precisely_job_sensor/component.py` (see its comment) and that the
parallel `openai_batch_status_sensor` component independently hit and fixed
the same way. Rather than resurrect that bug, `results_asset_key` carries
the actual target asset's key so the correct op name is derived automatically.
"""
import os
from typing import Optional

import dagster as dg
from dagster import RunRequest, SensorEvaluationContext, SensorResult, sensor
from dagster._core.definitions.sensor_definition import DefaultSensorStatus
from pydantic import Field


class AnthropicBatchStatusSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Trigger a job once a watched Anthropic Message Batch has ended.

    Example:
        ```yaml
        type: dagster_component_templates.AnthropicBatchStatusSensorComponent
        attributes:
          sensor_name: support_ticket_batch_done
          watch_asset_key: support_ticket_batch
          job_name: support_ticket_batch_results
          results_asset_key: support_ticket_batch_results
          api_key_env_var: ANTHROPIC_API_KEY
          minimum_interval_seconds: 60
          default_status: running
        ```
    """

    sensor_name: str = Field(description="Unique sensor name.")
    watch_asset_key: str = Field(description="The anthropic_batch_submit asset to watch for a batch_id in its materialization metadata.")
    job_name: str = Field(description="Job to trigger once the watched batch reaches processing_status == 'ended'.")
    results_asset_key: str = Field(
        description=(
            "Asset key of the anthropic_batch_results asset this sensor's RunRequest targets. "
            "Used only to derive the correct op name (via AssetKey.to_python_identifier()) for "
            "run_config — not to look up anything at sensor-eval time."
        ),
    )
    api_key_env_var: str = Field(default="ANTHROPIC_API_KEY", description="Env var holding the Anthropic API key")
    minimum_interval_seconds: int = Field(default=60, description="Minimum seconds between sensor evaluations.")
    default_status: str = Field(default="running", description="Sensor default status: 'running' or 'stopped'.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        default_status = (
            DefaultSensorStatus.RUNNING if self.default_status == "running"
            else DefaultSensorStatus.STOPPED
        )
        op_name = dg.AssetKey.from_user_string(self.results_asset_key).to_python_identifier()

        @sensor(
            name=_self.sensor_name,
            minimum_interval_seconds=_self.minimum_interval_seconds,
            default_status=default_status,
            job_name=_self.job_name,
        )
        def anthropic_batch_status_sensor(context: SensorEvaluationContext):
            watch_key = dg.AssetKey.from_user_string(_self.watch_asset_key)
            event = context.instance.get_latest_materialization_event(watch_key)
            if event is None or event.asset_materialization is None:
                return SensorResult(skip_reason=f"No materialization found for {_self.watch_asset_key!r}")

            md = event.asset_materialization.metadata or {}
            batch_id_mv = md.get("batch_id")
            if batch_id_mv is None:
                return SensorResult(
                    skip_reason=f"Latest materialization of {_self.watch_asset_key!r} has no batch_id metadata"
                )
            batch_id: str = batch_id_mv.text

            try:
                import anthropic
            except ImportError:
                return SensorResult(skip_reason="anthropic package not installed")

            api_key = os.environ.get(_self.api_key_env_var)
            if not api_key:
                return SensorResult(skip_reason=f"{_self.api_key_env_var} not set")

            client = anthropic.Anthropic(api_key=api_key)
            try:
                batch = client.messages.batches.retrieve(batch_id)
            except Exception as e:
                return SensorResult(skip_reason=f"Failed to retrieve batch {batch_id}: {e}")

            # Always re-check the LIVE status — never trust the metadata snapshot.
            if batch.processing_status != "ended":
                return SensorResult(
                    skip_reason=f"Batch {batch_id} processing_status={batch.processing_status!r} (not yet ended)"
                )

            fingerprint = f"{batch_id}|ended"
            cursor = context.cursor or ""
            if fingerprint == cursor:
                return SensorResult(skip_reason=f"Already processed {fingerprint}")

            return SensorResult(
                run_requests=[
                    RunRequest(
                        run_key=fingerprint,
                        run_config={"ops": {op_name: {"config": {"batch_id": batch_id}}}},
                    )
                ],
                cursor=fingerprint,
            )

        return dg.Definitions(sensors=[anthropic_batch_status_sensor])
