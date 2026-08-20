"""RejectionFeedbackLoopComponent.

Sensor that watches an approval-token directory for `approved: false`
tokens with a `feedback: "..."` field, and re-triggers an upstream job
with that feedback captured as new input — letting the agent revise
its output based on the human's specific rejection reason. Bounded by
`max_iterations` so a stubborn agent can't loop forever.

Composes with HumanApprovalGate / SlackApprovalGate / TeamsApprovalGate
via the shared `approval_dir`. No coupling to the approval-gate
components — reads tokens by file convention only.

## The loop

1. Agent generates draft → HumanApprovalGate writes asset
2. Human reviews in Slack/Teams/UI/webhook, drops a JSON token:
      {"approved": false, "approver": "...", "reason": "too vague",
       "feedback": "Cover the API-stability implication explicitly"}
3. This sensor sees the rejected token, writes the feedback text to
   `{approval_dir}/.feedback/{partition_key}.txt`, bumps iteration
   counter, moves consumed token to `.consumed/`, yields RunRequest
   to re-materialize the target job
4. Target job (typically PartitionedAssetLauncherJob + AgenticPipeline)
   re-runs with the same partition_key; pipeline reads
   `{approval_dir}/.feedback/{partition_key}.txt` as an additional
   source, agent revises
5. New approval attempt; loop until approved OR max_iterations reached

## Reading feedback from the pipeline

Wire your AgenticPipeline to include a `feedback_hint` step early on:

    - id: feedback_hint
      op: llm_call
      source: source                  # the pipeline's initial source
      # Note: agent tools that can read files, OR a synthesize step
      # merging source + feedback file, are the common patterns.

Or simpler: read the feedback file into the pipeline's source via a
`synthesize` step that joins original source + feedback file (typed
inputs, port names `original` and `feedback`).
"""
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


class RejectionFeedbackLoopComponent(dg.Component, dg.Model, dg.Resolvable):
    """Watch approval_dir for rejected+feedback tokens; re-trigger target job with feedback captured.

    Bounded by max_iterations so a stubborn agent can't infinite-loop.
    Emits ONE sensor. Optionally emits a state DataFrame asset for
    observability into which partitions are mid-loop.
    """

    sensor_name: str = Field(
        description="Unique sensor name (e.g. `mir_feedback_loop`).",
    )
    approval_dir: str = Field(
        description=(
            "Shared approval token directory — same as HumanApprovalGate / "
            "SlackApprovalGate / TeamsApprovalGate write into."
        ),
    )
    target_job_name: str = Field(
        description=(
            "Job to re-materialize when a rejection-with-feedback token is "
            "detected. Typically your PartitionedAssetLauncherJob or a plain "
            "asset job that re-runs the whole pipeline for one partition."
        ),
    )

    # Bounds
    max_iterations: int = Field(
        default=3,
        description=(
            "Cap on how many revise cycles per partition_key. Once reached, "
            "the sensor STOPS re-triggering — subsequent rejections write "
            "a `.exhausted.json` marker and are logged but not retried. "
            "Prevents infinite loops when the agent can't satisfy the human."
        ),
    )
    minimum_interval_seconds: int = Field(
        default=30,
        description="Sensor polling cadence.",
    )
    default_status: str = Field(
        default="running",
        description="'running' (default) or 'stopped'.",
    )

    # Composite partition key parser — same shape as PartitionedAssetLauncherJob
    dynamic_partitions_name: Optional[str] = Field(
        default=None,
        description=(
            "If target assets are dynamic-partitioned, name of the "
            "DynamicPartitionsDefinition. Sensor uses `partition_key` "
            "(filename stem) as the key directly."
        ),
    )

    # Feedback file config
    feedback_subdir: str = Field(
        default=".feedback",
        description=(
            "Subdirectory under `approval_dir` where feedback text files "
            "are written for the pipeline to read. Default `.feedback`."
        ),
    )
    consumed_subdir: str = Field(
        default=".consumed",
        description=(
            "Subdirectory where consumed rejection tokens are moved after "
            "triggering a re-run. Prevents double-processing."
        ),
    )
    state_subdir: str = Field(
        default=".state",
        description=(
            "Subdirectory for per-partition iteration state JSON files."
        ),
    )

    # Optional companion asset that surfaces mid-loop partitions
    emit_state_asset: bool = Field(
        default=False,
        description=(
            "If true, emit a DataFrame asset `{sensor_name}_state` with one "
            "row per partition currently in a feedback loop (iteration count, "
            "last feedback timestamp). Useful for the Insights view."
        ),
    )

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Rejection Feedback Loop", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        default_status = (
            dg.DefaultSensorStatus.RUNNING
            if self.default_status == "running"
            else dg.DefaultSensorStatus.STOPPED
        )

        @dg.sensor(
            name=_self.sensor_name,
            job_name=_self.target_job_name,
            minimum_interval_seconds=_self.minimum_interval_seconds,
            default_status=default_status,
            description=(
                f"Watches {_self.approval_dir!r} for rejected+feedback tokens. "
                f"Re-triggers {_self.target_job_name!r} up to {_self.max_iterations} "
                f"times per partition_key, capturing feedback text so the pipeline "
                f"can revise. Composes with HumanApprovalGate / SlackApprovalGate / "
                f"TeamsApprovalGate via shared approval_dir."
            ),
        )
        def _feedback_loop_sensor(context: dg.SensorEvaluationContext):
            root = Path(_self.approval_dir).expanduser().resolve()
            if not root.exists():
                return dg.SkipReason(f"approval_dir does not exist yet: {root}")

            feedback_dir = root / _self.feedback_subdir
            consumed_dir = root / _self.consumed_subdir
            state_dir = root / _self.state_subdir
            feedback_dir.mkdir(parents=True, exist_ok=True)
            consumed_dir.mkdir(parents=True, exist_ok=True)
            state_dir.mkdir(parents=True, exist_ok=True)

            run_requests: List[dg.RunRequest] = []
            observations: List[str] = []
            n_exhausted = 0
            n_triggered = 0

            for token_path in sorted(root.glob("*.json")):
                if not token_path.is_file():
                    continue
                try:
                    token = json.loads(token_path.read_text())
                except (json.JSONDecodeError, OSError) as e:
                    context.log.warning(f"skip malformed {token_path}: {e}")
                    continue

                # Only act on rejections that carry feedback.
                if token.get("approved") is not False:
                    continue
                feedback = token.get("feedback")
                if not feedback:
                    continue

                partition_key = token_path.stem
                safe_key = partition_key.replace("/", "_").replace("\\", "_")

                # Load / init iteration state.
                state_file = state_dir / f"{safe_key}.json"
                if state_file.exists():
                    try:
                        state = json.loads(state_file.read_text())
                    except (json.JSONDecodeError, OSError):
                        state = {"iterations": 0, "history": []}
                else:
                    state = {"iterations": 0, "history": []}

                if state["iterations"] >= _self.max_iterations:
                    # Bounded — mark exhausted, move token, do NOT re-trigger.
                    exhausted_marker = consumed_dir / f"{safe_key}.exhausted.json"
                    exhausted_marker.write_text(json.dumps({
                        **token,
                        "loop_status": "max_iterations_reached",
                        "iterations_done": state["iterations"],
                        "exhausted_at": _now_iso(),
                    }, indent=2))
                    try:
                        token_path.unlink()
                    except OSError:
                        pass
                    context.log.warning(
                        f"[{_self.sensor_name}] partition {partition_key!r} hit "
                        f"max_iterations={_self.max_iterations}; NOT re-triggering. "
                        f"Marker: {exhausted_marker}"
                    )
                    n_exhausted += 1
                    continue

                # Write feedback for the pipeline to read next iteration.
                feedback_file = feedback_dir / f"{safe_key}.txt"
                feedback_body = _format_feedback(feedback, state["iterations"] + 1, token)
                feedback_file.write_text(feedback_body)

                # Bump iteration state.
                state["iterations"] += 1
                state["history"].append({
                    "iteration": state["iterations"],
                    "rejected_by": token.get("approver"),
                    "reason": token.get("reason"),
                    "feedback": feedback,
                    "at": _now_iso(),
                })
                state["last_updated"] = _now_iso()
                state_file.write_text(json.dumps(state, indent=2))

                # Move the rejection token so it's not re-processed.
                consumed_file = consumed_dir / f"{safe_key}.iter{state['iterations']}.json"
                shutil.move(str(token_path), str(consumed_file))

                # Register dynamic partition if needed (safe if already registered).
                if _self.dynamic_partitions_name:
                    context.instance.add_dynamic_partitions(
                        _self.dynamic_partitions_name, [partition_key]
                    )

                # Yield a RunRequest re-materializing the target job for this key.
                run_requests.append(dg.RunRequest(
                    run_key=f"{safe_key}-feedback-iter{state['iterations']}",
                    partition_key=partition_key,
                    tags={
                        "feedback_loop": _self.sensor_name,
                        "feedback_iteration": str(state["iterations"]),
                        "rejected_by": str(token.get("approver") or ""),
                    },
                ))
                observations.append(
                    f"triggered iter {state['iterations']}/{_self.max_iterations} "
                    f"for {partition_key!r}"
                )
                n_triggered += 1

            if run_requests:
                context.log.info(
                    f"[{_self.sensor_name}] {n_triggered} re-run(s) triggered, "
                    f"{n_exhausted} exhausted"
                )
                return dg.SensorResult(run_requests=run_requests)

            if n_exhausted:
                return dg.SkipReason(f"{n_exhausted} partition(s) hit max_iterations")
            return dg.SkipReason("no rejection-with-feedback tokens")

        defs_kwargs: Dict[str, Any] = {"sensors": [_feedback_loop_sensor]}

        # Optional companion state DataFrame asset.
        if _self.emit_state_asset:
            defs_kwargs["assets"] = [_build_state_asset(_self)]

        return dg.Definitions(**defs_kwargs)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _format_feedback(feedback: str, iteration: int, token: Dict[str, Any]) -> str:
    """Format the feedback text file body — the pipeline reads this via
    `source: {kind: file, path: ...}` or a synthesize step's typed input."""
    header = (
        f"# Human feedback (iteration {iteration})\n"
        f"Rejected by: {token.get('approver') or 'unknown'}\n"
        f"Reason: {token.get('reason') or '(no reason given)'}\n"
        f"At: {_now_iso()}\n\n"
        f"## Feedback\n"
    )
    return header + str(feedback).strip() + "\n"


def _build_state_asset(comp: "RejectionFeedbackLoopComponent"):
    """One DataFrame row per partition currently mid-loop. Refresh on
    materialize by scanning `state_subdir`."""
    asset_name = f"{comp.sensor_name}_state"

    @dg.asset(
        key=dg.AssetKey.from_user_string(asset_name),
        description=f"Feedback-loop state for {comp.sensor_name} (partitions currently mid-loop).",
        group_name="governance",
        tags={"dagster/kind/audit": "", "dagster/kind/approval": ""},
    )
    def _state_asset(context: dg.AssetExecutionContext):
        import pandas as pd
        state_dir = Path(comp.approval_dir).expanduser().resolve() / comp.state_subdir
        rows: List[Dict[str, Any]] = []
        if state_dir.exists():
            for f in sorted(state_dir.glob("*.json")):
                try:
                    s = json.loads(f.read_text())
                except (json.JSONDecodeError, OSError):
                    continue
                rows.append({
                    "partition_key": f.stem,
                    "iterations_done": s.get("iterations", 0),
                    "max_iterations": comp.max_iterations,
                    "last_updated": s.get("last_updated", ""),
                    "n_feedback_rounds": len(s.get("history", [])),
                    "state_file": str(f),
                })
        df = pd.DataFrame(rows, columns=[
            "partition_key", "iterations_done", "max_iterations",
            "last_updated", "n_feedback_rounds", "state_file",
        ])
        return dg.MaterializeResult(
            value=df,
            metadata={
                "n_partitions_in_loop": len(df),
                "state_dir": str(state_dir),
                "preview": dg.MetadataValue.md(
                    df.head(20).to_markdown(index=False) if not df.empty else "_(no partitions in loop)_"
                ),
            },
        )
    return _state_asset
