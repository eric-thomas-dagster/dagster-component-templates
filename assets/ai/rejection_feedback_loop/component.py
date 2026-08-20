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


# ── Inline fs helper: local + cloud (s3://, gs://, abfs://) via fsspec ──
# Kept inline per DCC's self-contained convention.
class _ApprovalFS:
    """Uniform read/write/list/move over `approval_dir`. Plain paths use
    pathlib (no fsspec dependency); URIs (`s3://`, `gs://`, `abfs://`)
    route through fsspec + the appropriate driver."""
    def __init__(self, root: str):
        if "://" in root:
            import fsspec
            fs, rt = fsspec.core.url_to_fs(root)
            self.fs, self.root, self.is_uri = fs, rt.rstrip("/"), True
        else:
            self.fs, self.root, self.is_uri = None, str(Path(root).expanduser().resolve()), False
    def path(self, *parts: str) -> str:
        pieces = [self.root, *[p.strip("/") for p in parts if p]]
        return "/".join(pieces) if self.is_uri else str(Path(*pieces))
    def exists(self, p: str) -> bool:
        return bool(self.fs.exists(p)) if self.is_uri else Path(p).exists()
    def mkdir(self, subdir: str = "") -> None:
        target = self.path(subdir) if subdir else self.root
        if self.is_uri:
            try: self.fs.makedirs(target, exist_ok=True)
            except Exception: pass
        else:
            Path(target).mkdir(parents=True, exist_ok=True)
    def read_json(self, p: str) -> Any:
        if self.is_uri:
            with self.fs.open(p, "r") as f: return json.loads(f.read())
        return json.loads(Path(p).read_text())
    def write_text(self, p: str, content: str) -> None:
        if self.is_uri:
            try: self.fs.makedirs("/".join(p.split("/")[:-1]), exist_ok=True)
            except Exception: pass
            with self.fs.open(p, "w") as f: f.write(content)
        else:
            Path(p).parent.mkdir(parents=True, exist_ok=True)
            Path(p).write_text(content)
    def write_json(self, p: str, obj: Any) -> None:
        self.write_text(p, json.dumps(obj, indent=2, default=str))
    def glob(self, pattern: str) -> List[str]:
        if self.is_uri:
            proto = self.fs.protocol if isinstance(self.fs.protocol, str) else self.fs.protocol[0]
            return [f"{proto}://{m}" for m in self.fs.glob(self.path(pattern))]
        return [str(p) for p in Path(self.root).glob(pattern)]
    def move(self, src: str, dst: str) -> None:
        if self.is_uri:
            self.fs.mv(src, dst)
        else:
            Path(dst).rsplit("/", 1) if "/" in dst else None
            Path(dst).parent.mkdir(parents=True, exist_ok=True)
            shutil.move(src, dst)
    def delete(self, p: str) -> None:
        if self.is_uri:
            try: self.fs.rm(p)
            except FileNotFoundError: pass
        else:
            try: Path(p).unlink()
            except FileNotFoundError: pass
    def stem(self, path: str) -> str:
        base = path.rstrip("/").split("/")[-1]
        return base.rsplit(".", 1)[0] if "." in base else base


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
            fs = _ApprovalFS(_self.approval_dir)
            # Local: skip if root missing. Cloud: prefix stores return [] gracefully.
            if not fs.is_uri and not Path(fs.root).exists():
                return dg.SkipReason(f"approval_dir does not exist yet: {fs.root}")

            fs.mkdir(_self.feedback_subdir)
            fs.mkdir(_self.consumed_subdir)
            fs.mkdir(_self.state_subdir)

            run_requests: List[dg.RunRequest] = []
            observations: List[str] = []
            n_exhausted = 0
            n_triggered = 0

            for token_path in sorted(fs.glob("*.json")):
                try:
                    token = fs.read_json(token_path)
                except (json.JSONDecodeError, OSError, IsADirectoryError) as e:
                    context.log.warning(f"skip malformed {token_path}: {e}")
                    continue

                # Only act on rejections that carry feedback.
                if token.get("approved") is not False:
                    continue
                feedback = token.get("feedback")
                if not feedback:
                    continue

                partition_key = fs.stem(token_path)
                safe_key = partition_key.replace("/", "_").replace("\\", "_")

                # Load / init iteration state.
                state_path = fs.path(_self.state_subdir, f"{safe_key}.json")
                if fs.exists(state_path):
                    try:
                        state = fs.read_json(state_path)
                    except (json.JSONDecodeError, OSError):
                        state = {"iterations": 0, "history": []}
                else:
                    state = {"iterations": 0, "history": []}

                if state["iterations"] >= _self.max_iterations:
                    # Bounded — mark exhausted, delete original token, do NOT re-trigger.
                    exhausted_marker = fs.path(_self.consumed_subdir, f"{safe_key}.exhausted.json")
                    fs.write_json(exhausted_marker, {
                        **token,
                        "loop_status": "max_iterations_reached",
                        "iterations_done": state["iterations"],
                        "exhausted_at": _now_iso(),
                    })
                    fs.delete(token_path)
                    context.log.warning(
                        f"[{_self.sensor_name}] partition {partition_key!r} hit "
                        f"max_iterations={_self.max_iterations}; NOT re-triggering. "
                        f"Marker: {exhausted_marker}"
                    )
                    n_exhausted += 1
                    continue

                # Write feedback for the pipeline to read next iteration.
                feedback_path = fs.path(_self.feedback_subdir, f"{safe_key}.txt")
                feedback_body = _format_feedback(feedback, state["iterations"] + 1, token)
                fs.write_text(feedback_path, feedback_body)

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
                fs.write_json(state_path, state)

                # Move the rejection token so it's not re-processed.
                consumed_path = fs.path(
                    _self.consumed_subdir, f"{safe_key}.iter{state['iterations']}.json"
                )
                fs.move(token_path, consumed_path)

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
        fs = _ApprovalFS(comp.approval_dir)
        state_dir_display = fs.path(comp.state_subdir)
        rows: List[Dict[str, Any]] = []
        state_files: List[str] = []
        try:
            state_files = sorted(fs.glob(f"{comp.state_subdir}/*.json"))
        except FileNotFoundError:
            state_files = []
        for f in state_files:
            try:
                s = fs.read_json(f)
            except (json.JSONDecodeError, OSError, IsADirectoryError):
                continue
            rows.append({
                "partition_key": fs.stem(f),
                "iterations_done": s.get("iterations", 0),
                "max_iterations": comp.max_iterations,
                "last_updated": s.get("last_updated", ""),
                "n_feedback_rounds": len(s.get("history", [])),
                "state_file": f,
            })
        df = pd.DataFrame(rows, columns=[
            "partition_key", "iterations_done", "max_iterations",
            "last_updated", "n_feedback_rounds", "state_file",
        ])
        return dg.MaterializeResult(
            value=df,
            metadata={
                "n_partitions_in_loop": len(df),
                "state_dir": state_dir_display,
                "preview": dg.MetadataValue.md(
                    df.head(20).to_markdown(index=False) if not df.empty else "_(no partitions in loop)_"
                ),
            },
        )
    return _state_asset
