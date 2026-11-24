"""AwsBatchWorkspaceComponent -- AWS Batch via Dagster Pipes, workspace-shaped.

## Why this exists

No component in this registry wrapped AWS Batch before this one. `dagster_aws.pipes`
(confirmed: dagster-aws 0.29.x installed) ships `PipesGlueClient`, `PipesECSClient`,
`PipesEMRClient`, `PipesEMRContainersClient`, `PipesEMRServerlessClient`, and
`PipesLambdaClient` -- no Batch client (confirmed by reading
`dagster_aws/pipes/clients/__init__.py` directly). `PipesBatchClient` below is
modeled closely on the real, installed `PipesECSClient` (read directly from
`dagster_aws/pipes/clients/ecs.py`, not guessed), adapted for AWS Batch's real,
different API:

  - `SubmitJob` against a job queue + job definition (not ECS's `RunTask` against
    a cluster + task definition) -- job queue placement, Spot capacity, and
    retries are owned by Batch itself, not this component.
  - `DescribeJobs` to poll status (not ECS's `DescribeTasks`); AWS Batch's boto3
    client ships NO waiter (confirmed: `boto3.client("batch").waiter_names == []`),
    unlike ECS's `tasks_stopped` waiter, so this polls manually.
  - `TerminateJob` on interrupt (not ECS's `StopTask`).
  - Bootstrap env vars go into `containerOverrides.environment` (the real
    `SubmitJob` parameter shape, confirmed against the installed botocore Batch
    service model: `containerOverrides: {environment: [{name, value}], ...}`,
    identical in shape to ECS's container overrides).
  - Batch jobs log to the `aws/batch/job` CloudWatch log group by default when no
    `logConfiguration` override is set -- confirmed via the AWS Batch user guide's
    "Use the awslogs log driver" page, which states the default literally as
    `aws/batch/job` (no leading slash). `DescribeJobs`'s response conveniently
    includes `container.logStreamName` and `container.logConfiguration.options`
    directly (no reconstruction needed, unlike ECS where `PipesECSClient` has to
    build the stream name itself from a stream prefix + container name + task id).
  - Batch's `tags` SubmitJob parameter is a plain `{str: str}` map (not a list of
    `{key, value}` dicts like ECS resource tags), confirmed via the installed
    botocore service model (`tags: map (TagrisTagsMap)`) -- so
    `session.default_remote_invocation_info` (which includes `dagster/run-id`)
    merges straight into it.

## Demo mode

`demo_mode: true` does NOT swap `PipesBatchClient` for a different transport and
does NOT abstractly fake Batch API responses. It builds the exact same
`PipesBatchClient`/`PipesCloudWatchMessageReader` real classes used in production,
injecting a pair of internal fake boto3 `batch`/`logs` clients instead of real
ones -- the same convention `ecs_pipes_asset`'s own demo mode uses (read directly
from `assets/infrastructure/ecs_pipes_asset/component.py` and mirrored here). The
fake `submit_job` call launches `local_script_path` as a real local subprocess
with the container-override environment Batch would inject, and writes its
stdout to a file; the fake `logs` client serves that growing file as paginated
CloudWatch log events, so Pipes messages and live logs stream back exactly as
they do from a real Batch job. Only the actual AWS API calls are faked --
`PipesBatchClient`'s own request-building, polling, and message-reading code
runs unmodified in both modes.

## Workspace shape

Follows this registry's established workspace-component convention (see
`ssis_workspace`/`snowflake_workspace`): `StateBackedComponent` enumeration via
`DescribeJobDefinitions` in the state-write path, an `assets_by_job_definition`
per-entry override table (`_merge_spec`-equivalent), a `@public get_asset_spec
(props)` hook (the base translator wrapped by an optional `translation:`
callable), and an opt-in `generate_sensor` polling `ListJobs` that skips jobs
carrying a `dagster/run-id` tag (jobs Dagster itself launched -- see
`PipesBatchClient`'s tag-merging above) so it doesn't double-report them.
"""
import json
import re
import time
from pathlib import Path
from typing import Annotated, Any, Dict, List, Optional, Union

import dagster as dg
from dagster import (
    AssetKey,
    AssetSpec,
    ComponentLoadContext,
    Definitions,
    Model,
    Resolvable,
)
from dagster._annotations import public
from dagster.components.component.state_backed_component import StateBackedComponent
from dagster.components.utils.defs_state import (
    DefsStateConfig,
    DefsStateConfigArgs,
    ResolvedDefsStateConfig,
)
from dagster.components.utils.translation import TranslationFn, TranslationFnResolver
from dagster_shared.record import record
from pydantic import Field, model_validator


# ---------------------------------------------------------------------------
# PipesBatchClient -- modeled closely on the real, installed
# dagster_aws.pipes.PipesECSClient, adapted for AWS Batch's own API (see the
# module docstring above for exactly what's different and why).
# ---------------------------------------------------------------------------

class PipesBatchClient:
    """A Pipes client for running AWS Batch jobs.

    Modeled on `dagster_aws.pipes.PipesECSClient` -- same constructor/`run()`
    shape (`submit_job_params` in place of `run_task_params`) -- but targets
    AWS Batch's own `SubmitJob`/`DescribeJobs`/`TerminateJob` API instead of
    ECS's `RunTask`/`DescribeTasks`/`StopTask`.

    Args:
        client (Any): The boto Batch client used to submit the job.
        context_injector: A context injector to use to inject context into the
            Batch job. Defaults to `PipesEnvContextInjector`.
        message_reader: A message reader to use to read messages from the Batch
            job. Defaults to `PipesCloudWatchMessageReader`.
        forward_termination (bool): Whether to terminate the Batch job when the
            Dagster process receives a termination signal.
        poll_interval_seconds (float): Seconds between `DescribeJobs` polls.
            AWS Batch's boto3 client ships no waiter for job completion
            (confirmed: `boto3.client("batch").waiter_names == []`), so this
            client polls manually.
        default_log_group (str): CloudWatch log group to fall back to when a
            job's own `containerProperties.logConfiguration` doesn't specify
            one. Batch's real default is `aws/batch/job` (no leading slash) --
            confirmed via the AWS Batch user guide's "Use the awslogs log
            driver" page, not assumed.
    """

    def __init__(
        self,
        client: Optional[Any] = None,
        context_injector: Optional[Any] = None,
        message_reader: Optional[Any] = None,
        forward_termination: bool = True,
        poll_interval_seconds: float = 6.0,
        default_log_group: str = "aws/batch/job",
    ):
        import boto3
        from dagster._core.pipes.utils import PipesEnvContextInjector
        from dagster_aws.pipes import PipesCloudWatchMessageReader

        self._client = client or boto3.client("batch")
        self._context_injector = context_injector or PipesEnvContextInjector()
        self._message_reader = message_reader or PipesCloudWatchMessageReader()
        self.forward_termination = forward_termination
        self._poll_interval_seconds = poll_interval_seconds
        self._default_log_group = default_log_group

    @classmethod
    def _is_dagster_maintained(cls) -> bool:
        return True

    def run(
        self,
        *,
        context,
        submit_job_params: Dict[str, Any],
        extras: Optional[Dict[str, Any]] = None,
    ):
        from dagster._core.errors import DagsterExecutionInterruptedError
        from dagster._core.pipes.utils import open_pipes_session
        from dagster_aws.pipes import PipesCloudWatchLogReader, PipesCloudWatchMessageReader

        with open_pipes_session(
            context=context,
            message_reader=self._message_reader,
            context_injector=self._context_injector,
            extras=extras,
        ) as session:
            params = dict(submit_job_params)

            # Inject the Pipes bootstrap env vars into containerOverrides.environment --
            # the real SubmitJob parameter shape, confirmed against the installed
            # botocore Batch service model (identical shape to ECS's container
            # overrides: a list of {name, value} dicts).
            overrides = dict(params.get("containerOverrides") or {})
            environment = list(overrides.get("environment") or [])
            pipes_args = session.get_bootstrap_env_vars()
            environment.extend({"name": k, "value": v} for k, v in pipes_args.items())
            overrides["environment"] = environment
            params["containerOverrides"] = overrides

            # Batch's `tags` SubmitJob parameter is a plain {str: str} map (unlike
            # ECS's [{key, value}] list) -- confirmed via the installed botocore
            # service model -- so this merges straight in, including
            # `dagster/run-id`, which the observation sensor's own job-skip logic
            # (see the component's generate_sensor) reads back out.
            tags = dict(params.get("tags") or {})
            tags.update(session.default_remote_invocation_info)
            params["tags"] = tags

            response = self._client.submit_job(**params)
            job_id = response["jobId"]
            job_name = response.get("jobName") or params.get("jobName")
            context.log.info(f"[pipes] submitted AWS Batch job {job_id} ({job_name})")

            terminal_states = {"SUCCEEDED", "FAILED"}
            log_attached = not isinstance(self._message_reader, PipesCloudWatchMessageReader)
            job_detail: Dict[str, Any] = {}
            try:
                while True:
                    job_detail = self._client.describe_jobs(jobs=[job_id])["jobs"][0]
                    status = job_detail["status"]

                    if not log_attached:
                        container = job_detail.get("container") or {}
                        log_stream = container.get("logStreamName")
                        if log_stream:
                            log_group = (
                                (container.get("logConfiguration") or {}).get("options", {}).get("awslogs-group")
                                or self._default_log_group
                            )
                            session.report_launched({"extras": {"log_group": log_group, "log_stream": log_stream}})
                            self._message_reader.add_log_reader(
                                PipesCloudWatchLogReader(
                                    client=self._message_reader.client,
                                    log_group=log_group,
                                    log_stream=log_stream,
                                    start_time=int(session.created_at.timestamp() * 1000),
                                    debug_info=f"reader for batch job {job_id}",
                                ),
                            )
                            log_attached = True

                    if status in terminal_states:
                        break
                    time.sleep(self._poll_interval_seconds)
            except DagsterExecutionInterruptedError:
                if self.forward_termination:
                    context.log.warning(f"[pipes] Dagster process interrupted, terminating AWS Batch job {job_id}")
                    self._terminate(context=context, job_id=job_id)
                raise

            if job_detail.get("status") == "FAILED":
                raise RuntimeError(
                    f"AWS Batch job {job_id} failed: {job_detail.get('statusReason')}"
                )

        context.log.info(f"[pipes] AWS Batch job {job_id} completed")
        from dagster._core.pipes.client import PipesClientCompletedInvocation
        return PipesClientCompletedInvocation(session, metadata=self._extract_dagster_metadata(job_detail, job_id))

    def _terminate(self, context, job_id: str) -> None:
        try:
            self._client.terminate_job(jobId=job_id, reason="Dagster process was interrupted")
        except Exception as exc:  # noqa: BLE001
            context.log.warning(f"[pipes] Couldn't terminate AWS Batch job {job_id}: {exc}")

    def _extract_dagster_metadata(self, job_detail: Dict[str, Any], job_id: str) -> Dict[str, Any]:
        from dagster import MetadataValue
        metadata: Dict[str, Any] = {"AWS Batch Job ID": job_id}
        region = getattr(getattr(self._client, "meta", None), "region_name", None)
        if region and region != "demo":
            metadata["AWS Batch Job URL"] = MetadataValue.url(
                f"https://{region}.console.aws.amazon.com/batch/home?region={region}#jobs/detail/{job_id}"
            )
        status_reason = job_detail.get("statusReason")
        if status_reason:
            metadata["AWS Batch Status Reason"] = MetadataValue.text(status_reason)
        return metadata


# ---------------------------------------------------------------------------
# Demo-mode fakes: the boto3 batch/logs client boundary ONLY. Mirrors
# ecs_pipes_asset's own _DemoEcsClient/_DemoLogsClient exactly, adapted for
# Batch's SubmitJob/DescribeJobs/TerminateJob instead of ECS's
# RunTask/DescribeTasks/StopTask.
# ---------------------------------------------------------------------------

class _DemoLogsClient:
    """Serves a local file as CloudWatch log streams (`awslogs` driver semantics)."""

    class exceptions:  # noqa: N801 -- mirrors boto3's client.exceptions namespace
        class ResourceNotFoundException(Exception):
            pass

        class ThrottlingException(Exception):
            pass

    def __init__(self) -> None:
        import threading
        self._streams: Dict[str, Path] = {}
        self._lock = threading.Lock()

    def register(self, stream: str, file: Path) -> None:
        with self._lock:
            self._streams[stream] = file

    def describe_log_streams(self, logGroupName: str, logStreamNamePrefix: str) -> dict:
        with self._lock:
            known = [name for name in self._streams if name.startswith(logStreamNamePrefix)]
        return {"logStreams": [{"logStreamName": name} for name in known]}

    def get_log_events(self, logGroupName: str, logStreamName: str, nextToken: Optional[str] = None, **_ignored) -> dict:
        offset = int(nextToken.removeprefix("f/")) if nextToken else 0
        data = self._streams[logStreamName].read_bytes()
        complete = data.rfind(b"\n") + 1
        lines = data[offset:complete].decode(errors="replace").splitlines()
        if not lines:
            time.sleep(0.1)  # a real poll returns at once; this just avoids a hot loop
        return {
            "events": [{"message": line, "timestamp": int(time.time() * 1000)} for line in lines],
            "nextForwardToken": f"f/{complete}",
        }


class _DemoBatchClient:
    """Answers the handful of Batch calls `PipesBatchClient.run` makes, by
    launching `local_script_path` as a real local subprocess."""

    def __init__(self, logs: _DemoLogsClient, local_script_path: str, log_group: str):
        import subprocess  # noqa: F401 -- for the type below
        self._logs = logs
        self._script = local_script_path
        self._log_group = log_group
        self._processes: Dict[str, Any] = {}

    @property
    def meta(self):
        import types
        return types.SimpleNamespace(region_name="demo")

    def submit_job(self, **params) -> dict:
        import os
        import subprocess
        import sys
        import uuid

        job_id = uuid.uuid4().hex
        job_name = params.get("jobName", "demo-job")
        overrides = params.get("containerOverrides") or {}
        environment = {item["name"]: item["value"] for item in overrides.get("environment", [])}
        log_dir = Path(os.environ.get("TMPDIR", "/tmp")) / "aws_batch_workspace_demo_logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / f"{job_id}.log"
        env = {**os.environ, **environment, "PYTHONUNBUFFERED": "1"}
        handle = log_file.open("wb")
        process = subprocess.Popen(
            [sys.executable, self._script], env=env, stdout=handle, stderr=subprocess.STDOUT,
        )
        handle.close()
        log_stream = f"{job_name}/default/{job_id}"
        self._logs.register(log_stream, log_file)
        self._processes[job_id] = {"process": process, "log_stream": log_stream}
        return {
            "jobArn": f"arn:aws:batch:demo:000000000000:job/{job_id}",
            "jobName": job_name,
            "jobId": job_id,
        }

    def describe_jobs(self, jobs: List[str]) -> dict:
        job_id = jobs[0]
        entry = self._processes[job_id]
        process, log_stream = entry["process"], entry["log_stream"]
        exit_code = process.poll()
        status = "RUNNING" if exit_code is None else ("SUCCEEDED" if exit_code == 0 else "FAILED")
        return {
            "jobs": [
                {
                    "jobId": job_id,
                    "status": status,
                    "statusReason": None if status != "FAILED" else f"container exited with code {exit_code}",
                    "container": {
                        "logStreamName": log_stream,
                        "logConfiguration": {"options": {"awslogs-group": self._log_group}},
                        "exitCode": exit_code,
                    },
                }
            ]
        }

    def terminate_job(self, jobId: str, reason: str) -> dict:
        entry = self._processes.get(jobId)
        if entry:
            entry["process"].terminate()
        return {}


# ---------------------------------------------------------------------------
# Field-shape normalization (inlined per component; keep copies identical across
# this repo's components -- see tests/test_field_shapes.py elsewhere)
# ---------------------------------------------------------------------------

def coerce_field_shapes(data, *, lists=(), csv=(), tags=()):
    """Accept the shapes people (and LLMs) naturally write for common boilerplate fields."""
    if not isinstance(data, dict):
        return data
    out = dict(data)
    for k in lists:
        v = out.get(k)
        if isinstance(v, str):
            v = v.split(",")
        if isinstance(v, (list, tuple)):
            out[k] = [x.strip() if isinstance(x, str) else x for x in v if not (isinstance(x, str) and not x.strip())]
    for k in csv:
        v = out.get(k)
        if isinstance(v, (list, tuple, set)):
            out[k] = ",".join(str(x).strip() for x in v)
    for k in tags:
        v = out.get(k)
        if isinstance(v, (list, tuple)) and v and all(isinstance(x, str) and ("=" in x or ":" in x) for x in v):
            v = dict((p.strip() for p in (x.split("=", 1) if "=" in x else x.split(":", 1))) for x in v)
        if isinstance(v, dict):
            out[k] = {str(a).strip(): (str(b).lower() if isinstance(b, bool) else str(b)) for a, b in v.items() if b is not None}
    return out


def _build_partitions_def(
    partition_type, partition_start, partition_values, dynamic_partition_name,
    cron_schedule=None, cron_fmt="%Y-%m-%d", end_offset=0, timezone=None,
):
    """Construct a Dagster partitions_def from the canonical flat partition fields.
    Same shape as ecs_pipes_asset/ssis_workspace/step_functions_asset/aws_glue's
    own `_build_partitions_def` -- inlined here per this registry's no-shared-
    helper-files convention."""
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, DynamicPartitionsDefinition,
    )

    if not partition_type:
        return None
    if partition_type in ("daily", "weekly", "monthly", "hourly", "cron") and not partition_start:
        raise ValueError(f"partition_type={partition_type!r} requires partition_start (ISO date, e.g. '2024-01-01').")
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "cron":
        from dagster import TimeWindowPartitionsDefinition
        if not cron_schedule:
            raise ValueError("partition_type='cron' requires cron_schedule, e.g. '0 0 * * 1-5' for weekdays only.")
        return TimeWindowPartitionsDefinition(start=partition_start, fmt=cron_fmt, cron_schedule=cron_schedule, end_offset=end_offset, timezone=timezone)
    if partition_type == "static":
        if isinstance(partition_values, (list, tuple)):
            values = [str(v).strip() for v in partition_values if str(v).strip()]
        else:
            values = [v.strip() for v in (str(partition_values) if partition_values else "").split(",") if v.strip()]
        if not values:
            raise ValueError("partition_type='static' requires partition_values (comma-separated).")
        return StaticPartitionsDefinition(values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    raise ValueError(f"unknown partition_type: {partition_type!r}")


def _sanitize(name: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_]", "_", name.lower())


# ---------------------------------------------------------------------------
# Props (@record) for the translator callable.
# ---------------------------------------------------------------------------

@record
class BatchJobDefinitionProps:
    """Data passed to `translation:` callables for each imported Batch job definition.

    Mirrors `SsisPackageProps`/`SnowflakeObjectProps` -- a single record describing
    the job definition so `translation:` callables can filter, rename, add tags, etc.

    Attributes:
        job_definition_name: The Batch job definition's name.
        revision: The job definition's revision number.
        job_definition_arn: The job definition's full ARN.
        image: Container image, if present.
        vcpus: Container vCPU request, if present.
        memory: Container memory (MiB) request, if present.
    """

    job_definition_name: str
    revision: int
    job_definition_arn: str = ""
    image: Optional[str] = None
    vcpus: Optional[float] = None
    memory: Optional[int] = None

    @property
    def qualified_name(self) -> str:
        return f"{self.job_definition_name}:{self.revision}"


# ---------------------------------------------------------------------------
# Base translator: BatchJobDefinitionProps -> AssetSpec.
# ---------------------------------------------------------------------------

class AwsBatchComponentTranslator:
    """Base translator. User's `translation:` callable wraps this via TranslationFn."""

    def __init__(self, component: "AwsBatchWorkspaceComponent"):
        self._component = component

    def get_asset_spec(self, props: BatchJobDefinitionProps) -> AssetSpec:
        prefix = self._component.key_prefix or ["aws_batch"]
        if isinstance(prefix, str):
            prefix = [prefix]
        kinds = set(self._component.kinds or []) | {"batch", "aws", "pipes"}
        kwargs: Dict[str, Any] = dict(
            key=AssetKey([*prefix, _sanitize(props.job_definition_name)]),
            description=f"AWS Batch job definition `{props.job_definition_name}` (revision {props.revision}).",
            group_name=self._component.group_name,
            kinds=kinds,
            tags=dict(self._component.tags or {}),
            owners=list(self._component.owners or []),
            deps=[],
            metadata={
                "batch/job_definition_name": props.job_definition_name,
                "batch/revision": props.revision,
                "batch/job_definition_arn": props.job_definition_arn,
                "batch/image": props.image or "",
                "batch/vcpus": props.vcpus or 0.0,
                "batch/memory_mib": props.memory or 0,
            },
            partitions_def=self._component._partitions_def,
        )
        kwargs = self._component._apply_asset_overrides(props.job_definition_name, kwargs)
        return AssetSpec(**kwargs)


# ---------------------------------------------------------------------------
# Component
# ---------------------------------------------------------------------------

@public
class AwsBatchWorkspaceComponent(StateBackedComponent, Model, Resolvable):
    """AWS Batch jobs, run via Dagster Pipes, as a Dagster workspace.

    Discovers job definitions via `DescribeJobDefinitions`, emits one
    materializable AssetSpec per job definition, and submits a real AWS Batch
    job (through the real `SubmitJob`/`DescribeJobs`/`TerminateJob` API, wrapped
    in the Pipes protocol by this component's own `PipesBatchClient`) on
    materialize.

    Full workspace-pattern shape:
      - `StateBackedComponent` caching -- refresh via `dg utils refresh-defs-state`
      - `assets_by_job_definition:` per-entry AssetSpec overrides
      - `translation:` callable for per-asset customization
      - `@public get_asset_spec(props)` hook (same convention as `ssis_workspace`)
      - `generate_sensor:` opt-in `ListJobs` observation sensor, skipping jobs
        Dagster itself launched

    Example:

    ```yaml
    type: dagster_community_components.AwsBatchWorkspaceComponent
    attributes:
      region_name: us-east-1
      job_queue: my-job-queue
      log_group: /ecs/my-batch-jobs
      demo_mode: true
      local_script_path: batch_task/run_batch.py
      generate_sensor: true
    ```
    """

    region_name: str = Field(description="AWS region for the batch/logs clients.")
    aws_access_key_id: Optional[str] = Field(default=None)
    aws_secret_access_key: Optional[str] = Field(default=None)
    aws_session_token: Optional[str] = Field(default=None)

    # ── Discovery / filtering ───────────────────────────────────────────────
    job_definition_name_prefix: Optional[str] = Field(default=None, description="Only include job definitions whose name starts with this prefix.")
    exclude_job_definitions: Optional[List[str]] = Field(default=None, description="Explicit list of job definition names to skip.")
    filter_by_name_pattern: Optional[str] = Field(default=None, description="Regex -- only include job definitions whose name matches.")
    exclude_name_pattern: Optional[str] = Field(default=None, description="Regex -- exclude job definitions whose name matches.")
    status_filter: str = Field(default="ACTIVE", description="DescribeJobDefinitions status filter (e.g. 'ACTIVE').")

    # ── Submission ───────────────────────────────────────────────────────────
    job_queue: str = Field(description="Default AWS Batch job queue jobs submit to. Per-entry override via assets_by_job_definition.<name>.job_queue.")
    container_command: Optional[List[str]] = Field(default=None, description="Command override for the container.")
    container_environment: Optional[Dict[str, str]] = Field(
        default=None,
        description="Environment variable overrides for the container, as a plain {name: value} dict -- converted internally to Batch's [{name, value}] list shape.",
    )
    vcpus_override: Optional[float] = Field(default=None, description="containerOverrides.vcpus.")
    memory_override: Optional[int] = Field(default=None, description="containerOverrides.memory (MiB).")
    retry_attempts: Optional[int] = Field(default=None, description="retryStrategy.attempts -- number of times Batch itself retries a failed job.")
    timeout_seconds: Optional[int] = Field(default=None, description="timeout.attemptDurationSeconds.")
    submit_job_params_overrides: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Raw overrides deep-merged on top of the submit_job params this component builds, for anything not covered by an explicit field above.",
    )

    # ── Pipes ────────────────────────────────────────────────────────────────
    log_group: Optional[str] = Field(
        default=None,
        description="Fallback CloudWatch log group when a job definition's own logConfiguration doesn't specify one. Batch's real default is 'aws/batch/job' (used when this is unset).",
    )
    extras: Optional[Dict[str, Any]] = Field(default=None, description="Arbitrary extra data passed to PipesBatchClient.run(extras=...).")
    poll_interval_seconds: float = Field(default=6.0, description="Seconds between DescribeJobs polls (Batch's boto3 client ships no completion waiter).")

    # ── Per-entry overrides + translation ───────────────────────────────────
    assets_by_job_definition: Optional[Dict[str, Dict[str, Any]]] = Field(
        default=None,
        description=(
            "Override the asset spec (and job_queue/arguments) for specific job definitions, keyed by "
            "job definition name. Mirrors aws_glue's assets_by_job_name / the official dagster-databricks "
            "assets_by_task_key shape.\n\n"
            "Per-entry keys (all optional): key, group_name, description, deps, metadata, tags, kinds, "
            "owners, job_queue (submission override), container_environment (merged over the global "
            "container_environment template)."
        ),
    )
    translation: Annotated[
        Optional[TranslationFn[BatchJobDefinitionProps]],
        TranslationFnResolver(template_vars_for_translation_fn=lambda data: {"props": data}),
    ] = Field(
        default=None,
        description=(
            "Optional per-asset translation callable. Receives a BatchJobDefinitionProps and returns a "
            "dict of AssetSpec kwargs (key / group_name / tags / owners / kinds / metadata / description). "
            "Follows the same convention as ssis_workspace / dagster-fivetran / dagster-snowflake."
        ),
    )

    # ── Catalog / governance ─────────────────────────────────────────────────
    group_name: Optional[str] = Field(default="aws_batch", description="Dagster asset group name.")
    key_prefix: Optional[Union[str, List[str]]] = Field(default=None, description="Prefix for every emitted asset key. Default: ['aws_batch'].")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds. Default always includes ['batch', 'aws', 'pipes'].")
    tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Additional asset tags.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")

    # ── Demo mode ────────────────────────────────────────────────────────────
    demo_mode: bool = Field(
        default=True,
        description="Run jobs as a real local subprocess against fake Batch/CloudWatch clients instead of a real Batch job queue. The real PipesBatchClient/PipesCloudWatchMessageReader code runs unmodified in both modes -- only the boto3 API calls are faked. Set false for real AWS Batch.",
    )
    local_script_path: Optional[str] = Field(
        default=None,
        description="Script demo mode launches locally (via sys.executable) in place of a real Batch job. Required when demo_mode=true.",
    )

    # ── Observation sensor ───────────────────────────────────────────────────
    generate_sensor: bool = Field(default=False, description="Opt-in sensor polling ListJobs for jobs Dagster did not itself submit (no dagster/run-id tag).")
    observation_interval_seconds: int = Field(default=300, description="Sensor tick interval. Only used when generate_sensor=true.")
    job_queues_to_poll: Optional[List[str]] = Field(default=None, description="Job queues the observation sensor polls via ListJobs. Default: [job_queue].")

    # ── Partitioning (same canonical shape as ecs_pipes_asset/ssis_workspace) ──
    partition_type: Optional[str] = Field(default=None, description="'daily'|'weekly'|'monthly'|'hourly'|'cron'|'static'|'dynamic', applied to every job definition's asset.")
    partition_start: Optional[str] = Field(default=None)
    partition_values: Optional[Any] = Field(default=None)
    dynamic_partition_name: Optional[str] = Field(default=None)
    partition_cron_schedule: Optional[str] = Field(default=None)
    partition_cron_fmt: Optional[str] = Field(default="%Y-%m-%d")
    partition_end_offset: int = Field(default=0)
    partition_timezone: Optional[str] = Field(default=None)
    backfill_policy: Optional[str] = Field(default=None, description="'single_run' or 'multi_run'. Only honored when the asset is actually partitioned.")
    backfill_policy_max_partitions_per_run: Optional[int] = Field(default=None)

    # ── State backing ────────────────────────────────────────────────────────
    defs_state: ResolvedDefsStateConfig = Field(default_factory=DefsStateConfigArgs.local_filesystem)

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=("owners", "kinds", "container_command", "exclude_job_definitions", "job_queues_to_poll"), csv=(), tags=("tags",))

    @model_validator(mode="after")
    def _validate_demo_mode(self):
        if self.demo_mode and not self.local_script_path:
            raise ValueError("local_script_path is required when demo_mode=true (nothing for the demo-mode fake to launch otherwise).")
        return self

    @property
    def _partitions_def(self):
        return _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values, self.dynamic_partition_name,
            cron_schedule=self.partition_cron_schedule, cron_fmt=self.partition_cron_fmt or "%Y-%m-%d",
            end_offset=self.partition_end_offset, timezone=self.partition_timezone,
        )

    @property
    def _backfill_policy_obj(self):
        pdef = self._partitions_def
        if pdef is None:
            return None
        if self.backfill_policy == "single_run":
            return dg.BackfillPolicy.single_run()
        if self.backfill_policy == "multi_run":
            return dg.BackfillPolicy.multi_run(max_partitions_per_run=self.backfill_policy_max_partitions_per_run or 1)
        return None

    def _apply_asset_overrides(self, job_definition_name: str, base_kwargs: dict) -> dict:
        """Merge per-entry overrides from `assets_by_job_definition` into AssetSpec kwargs.
        Same convention as ssis_workspace._apply_asset_overrides / aws_glue._merge_spec."""
        if not self.assets_by_job_definition:
            return base_kwargs
        override = self.assets_by_job_definition.get(job_definition_name)
        if not override:
            return base_kwargs
        result = dict(base_kwargs)
        if "key" in override:
            result["key"] = AssetKey.from_user_string(str(override["key"]))
        if "group_name" in override:
            result["group_name"] = override["group_name"]
        if "description" in override:
            result["description"] = override["description"]
        if "deps" in override:
            existing = list(result.get("deps") or [])
            existing.extend(AssetKey.from_user_string(d) for d in override["deps"])
            result["deps"] = existing
        if "metadata" in override:
            existing = dict(result.get("metadata") or {})
            existing.update(override["metadata"])
            result["metadata"] = existing
        if "tags" in override:
            existing = dict(result.get("tags") or {})
            existing.update(override["tags"])
            result["tags"] = existing
        if "kinds" in override:
            result["kinds"] = set(override["kinds"])
        if "owners" in override:
            result["owners"] = override["owners"]
        return result

    def _resolve_job_queue(self, job_definition_name: str) -> str:
        override = (self.assets_by_job_definition or {}).get(job_definition_name) or {}
        return override.get("job_queue") or self.job_queue

    def _resolve_container_environment(self, job_definition_name: str) -> Dict[str, str]:
        merged = dict(self.container_environment or {})
        override = (self.assets_by_job_definition or {}).get(job_definition_name) or {}
        if isinstance(override, dict) and override.get("container_environment"):
            merged.update(override["container_environment"])
        return merged

    # ── Translator (cached per instance) ────────────────────────────────────
    @property
    def _base_translator(self) -> AwsBatchComponentTranslator:
        cached = getattr(self, "__base_translator_cached", None)
        if cached is None:
            cached = AwsBatchComponentTranslator(self)
            object.__setattr__(self, "__base_translator_cached", cached)
        return cached

    @public
    def get_asset_spec(self, props: BatchJobDefinitionProps) -> AssetSpec:
        """Public hook -- user's `translation:` callable wraps this."""
        base_spec = self._base_translator.get_asset_spec(props)
        if self.translation is None:
            return base_spec
        overrides = self.translation(base_spec, props) or {}
        if isinstance(overrides, AssetSpec):
            return overrides
        return base_spec._replace(**overrides) if hasattr(base_spec, "_replace") else base_spec

    # ── boto3 client builders (override points for a subclass or test) ─────
    def _session_kwargs(self) -> Dict[str, Any]:
        kwargs: Dict[str, Any] = {"region_name": self.region_name}
        if self.aws_access_key_id and self.aws_secret_access_key:
            kwargs["aws_access_key_id"] = self.aws_access_key_id
            kwargs["aws_secret_access_key"] = self.aws_secret_access_key
        if self.aws_session_token:
            kwargs["aws_session_token"] = self.aws_session_token
        return kwargs

    def _get_batch_client(self):
        import boto3
        return boto3.Session(**self._session_kwargs()).client("batch")

    def _get_logs_client(self):
        import boto3
        return boto3.Session(**self._session_kwargs()).client("logs")

    def _build_pipes_client(self) -> PipesBatchClient:
        from dagster_aws.pipes import PipesCloudWatchMessageReader

        if self.demo_mode:
            logs = _DemoLogsClient()
            batch = _DemoBatchClient(logs, self.local_script_path, self.log_group or "aws/batch/job")
            return PipesBatchClient(
                client=batch,
                message_reader=PipesCloudWatchMessageReader(client=logs),
                poll_interval_seconds=0.05,
                default_log_group=self.log_group or "aws/batch/job",
            )
        return PipesBatchClient(
            client=self._get_batch_client(),
            message_reader=PipesCloudWatchMessageReader(client=self._get_logs_client()),
            poll_interval_seconds=self.poll_interval_seconds,
            default_log_group=self.log_group or "aws/batch/job",
        )

    def _build_submit_job_params(self, job_definition_name: str, job_definition_arn: str) -> Dict[str, Any]:
        import uuid
        container_environment = self._resolve_container_environment(job_definition_name)
        params: Dict[str, Any] = {
            "jobName": f"{_sanitize(job_definition_name)[:100]}-{uuid.uuid4().hex[:8]}",
            "jobQueue": self._resolve_job_queue(job_definition_name),
            "jobDefinition": job_definition_arn or job_definition_name,
        }
        container_override: Dict[str, Any] = {}
        if self.container_command:
            container_override["command"] = self.container_command
        if container_environment:
            container_override["environment"] = [{"name": k, "value": v} for k, v in container_environment.items()]
        if self.vcpus_override is not None:
            container_override["vcpus"] = self.vcpus_override
        if self.memory_override is not None:
            container_override["memory"] = self.memory_override
        if container_override:
            params["containerOverrides"] = container_override
        if self.retry_attempts is not None:
            params["retryStrategy"] = {"attempts": self.retry_attempts}
        if self.timeout_seconds is not None:
            params["timeout"] = {"attemptDurationSeconds": self.timeout_seconds}
        if self.submit_job_params_overrides:
            params = _deep_merge(params, self.submit_job_params_overrides)
        return params

    # ── StateBackedComponent contract ───────────────────────────────────────
    @property
    def defs_state_config(self) -> DefsStateConfig:
        return DefsStateConfig.from_args(
            self.defs_state,
            default_key=f"AwsBatchWorkspaceComponent[{self.region_name}]",
        )

    async def refresh_state(self, project_root: Path) -> str:
        """Refresh LOCAL_FILESYSTEM state atomically.

        The stock ``StateBackedComponent.refresh_state`` does ``shutil.rmtree(state_path)`` and only then
        runs ``write_state_to_path`` (network calls, seconds long), so for the whole fetch -- and again
        while the file is half-written -- a concurrent reader (run worker, step subprocess, ``dg list defs``;
        every ``dg`` command refreshes in dev mode) sees missing/partial state: empty Definitions or a
        JSONDecodeError. Here the new state is built beside the old and swapped in with a rename, so the
        previous state stays readable until the new one is complete (and survives a failed refresh)."""
        import os
        import shutil
        import uuid

        from dagster._core.storage.defs_state.base import DefsStateStorage
        from dagster.components.utils.project_paths import get_local_state_path
        from dagster_shared.serdes.objects.models.defs_state_info import (
            LOCAL_STATE_VERSION,
            DefsStateManagementType,
        )

        config = self.defs_state_config
        if config.management_type != DefsStateManagementType.LOCAL_FILESYSTEM:
            return await super().refresh_state(project_root)
        state_path = get_local_state_path(config.key, project_root)
        state_path.parent.mkdir(parents=True, exist_ok=True)
        token = f"{os.getpid()}-{uuid.uuid4().hex[:8]}"
        work = state_path.parent / f".{state_path.name}.refresh-{token}"
        old = state_path.parent / f".{state_path.name}.old-{token}"
        try:
            await self._write_state_to_path_async(work)
            if work.exists():
                if work.is_dir() and state_path.is_dir():
                    try:
                        os.rename(state_path, old)
                        os.rename(work, state_path)
                    except OSError:
                        shutil.rmtree(state_path, ignore_errors=True)
                        os.rename(work, state_path)
                else:
                    if state_path.is_dir():
                        shutil.rmtree(state_path, ignore_errors=True)
                    elif work.is_dir() and state_path.exists():
                        state_path.unlink()
                    os.replace(work, state_path)
        finally:
            for _p in (work, old):
                if _p.is_dir():
                    shutil.rmtree(_p, ignore_errors=True)
                else:
                    _p.unlink(missing_ok=True)
        state_storage = DefsStateStorage.get()
        if state_storage:
            state_storage.set_latest_version(config.key, LOCAL_STATE_VERSION)
        return LOCAL_STATE_VERSION

    def _should_include(self, name: str) -> bool:
        if self.job_definition_name_prefix and not name.startswith(self.job_definition_name_prefix):
            return False
        if self.exclude_job_definitions and name in self.exclude_job_definitions:
            return False
        if self.exclude_name_pattern and re.search(self.exclude_name_pattern, name, re.IGNORECASE):
            return False
        if self.filter_by_name_pattern and not re.search(self.filter_by_name_pattern, name, re.IGNORECASE):
            return False
        return True

    def write_state_to_path(self, state_path: Path) -> None:
        """Enumerate AWS Batch job definitions via DescribeJobDefinitions and cache to disk."""
        job_definitions: List[Dict[str, Any]] = []
        if self.demo_mode:
            # No real AWS Batch account to enumerate in demo mode -- a single synthetic
            # job definition named after local_script_path's stem, so the whole
            # workspace shape (assets_by_job_definition overrides, the sensor, Pipes
            # execution) can be smoke-tested with no real Batch account available.
            name = _sanitize(Path(self.local_script_path or "demo_task").stem) or "demo_task"
            job_definitions.append({
                "name": name, "revision": 1,
                "arn": f"arn:aws:batch:{self.region_name}:000000000000:job-definition/{name}:1",
                "image": None, "vcpus": None, "memory": None,
            })
        else:
            batch = self._get_batch_client()
            paginator = batch.get_paginator("describe_job_definitions")
            seen_names: set = set()
            for page in paginator.paginate(status=self.status_filter):
                for jd in page.get("jobDefinitions", []):
                    name = jd["jobDefinitionName"]
                    if name in seen_names or not self._should_include(name):
                        continue
                    seen_names.add(name)
                    container = jd.get("containerProperties") or {}
                    job_definitions.append({
                        "name": name,
                        "revision": jd.get("revision", 1),
                        "arn": jd.get("jobDefinitionArn", ""),
                        "image": container.get("image"),
                        "vcpus": container.get("vcpus"),
                        "memory": container.get("memory"),
                    })
        state_path.write_text(json.dumps({"job_definitions": job_definitions}, default=str))

    def build_defs_from_state(self, context: ComponentLoadContext, state_path: Optional[Path]) -> Definitions:
        if state_path is None or not state_path.exists():
            if hasattr(context, "log"):
                context.log.warning(  # type: ignore[union-attr]
                    "AwsBatchWorkspaceComponent: no cached state found. "
                    "Run `dg utils refresh-defs-state` or `dagster dev` to populate."
                )
            return Definitions()

        state = json.loads(state_path.read_text())
        job_definitions = state.get("job_definitions", [])
        if not job_definitions:
            return Definitions()

        assets: List[Any] = []
        job_metadata: Dict[AssetKey, Dict[str, Any]] = {}

        for jd in job_definitions:
            props = BatchJobDefinitionProps(
                job_definition_name=jd["name"],
                revision=int(jd.get("revision", 1)),
                job_definition_arn=jd.get("arn", ""),
                image=jd.get("image"),
                vcpus=jd.get("vcpus"),
                memory=jd.get("memory"),
            )
            spec = self.get_asset_spec(props)
            job_metadata[spec.key] = {"job_definition_name": jd["name"]}
            assets.append(self._make_batch_job_asset(spec, jd["name"], jd.get("arn", "")))

        sensors: List[Any] = []
        if self.generate_sensor and job_metadata:
            sensors.append(self._build_observation_sensor(job_metadata))

        return Definitions(assets=assets, sensors=sensors or None)

    # ── Executable asset factory ────────────────────────────────────────────
    def _make_batch_job_asset(self, spec: AssetSpec, job_definition_name: str, job_definition_arn: str):
        """Factory function, not a default-argument trick: Dagster's asset
        machinery treats any non-`context` parameter on an `@dg.asset` function
        as a real input to resolve against upstream deps, even with a default
        value (same gap documented -- and fixed -- in ssis_workspace/
        step_functions_asset/aws_glue). One asset per job definition, so
        re-executing one failed job doesn't resubmit every selected job."""
        component = self
        partitions_def = spec.partitions_def
        backfill_policy_obj = self._backfill_policy_obj if partitions_def is not None else None

        @dg.asset(
            key=spec.key, group_name=spec.group_name, description=spec.description,
            metadata=dict(spec.metadata or {}), tags=dict(spec.tags or {}),
            kinds=spec.kinds or {"batch", "aws", "pipes"}, deps=list(spec.deps or []),
            owners=list(spec.owners or []), partitions_def=partitions_def,
            backfill_policy=backfill_policy_obj,
        )
        def _batch_job_asset(context: dg.AssetExecutionContext):
            is_range = (not context.has_partition_key) and context.has_partition_key_range
            if context.has_partition_key or is_range:
                partition_keys: List[Optional[str]] = [str(k) for k in context.partition_keys]
            else:
                partition_keys = [None]

            def _run_one(partition_key: Optional[str]):
                extras = dict(component.extras or {})
                if partition_key:
                    extras.setdefault("partition_key", partition_key)
                client = component._build_pipes_client()
                return client.run(
                    context=context,
                    submit_job_params=component._build_submit_job_params(job_definition_name, job_definition_arn),
                    extras=extras or None,
                )

            if not is_range:
                result = _run_one(partition_keys[0]).get_materialize_result()
                yield result
                return

            # Range: one Batch job submission per partition, folded into one MaterializeResult.
            merged_metadata: Dict[str, Any] = {"partition_keys": dg.MetadataValue.json(partition_keys)}
            for pk in partition_keys:
                invocation = _run_one(pk)
                for res in invocation.get_results():
                    for mk, mv in (getattr(res, "metadata", None) or {}).items():
                        merged_metadata[f"{pk}/{mk}"] = mv
            yield dg.MaterializeResult(asset_key=spec.key, metadata=merged_metadata)

        return _batch_job_asset

    # ── Observation sensor ───────────────────────────────────────────────────
    def _build_observation_sensor(self, job_metadata: Dict[AssetKey, Dict[str, Any]]):
        """Poll ListJobs per job queue for jobs Dagster did NOT itself submit (no
        `dagster/run-id` tag -- see PipesBatchClient.run's tag-merging), and emit
        an AssetObservation under the component's OWN resolved (post-override)
        asset key for each job definition those external jobs ran against."""
        component = self
        job_queues = self.job_queues_to_poll or [self.job_queue]
        definition_name_to_key = {meta["job_definition_name"]: key for key, meta in job_metadata.items()}

        @dg.sensor(
            name=f"{_sanitize(self.group_name or 'aws_batch')}_observation_sensor",
            minimum_interval_seconds=self.observation_interval_seconds,
            default_status=dg.DefaultSensorStatus.STOPPED,
            asset_selection=dg.AssetSelection.assets(*job_metadata.keys()),
        )
        def _aws_batch_observation_sensor(context: dg.SensorEvaluationContext):
            cursor = json.loads(context.cursor) if context.cursor else {}
            client = component._get_batch_client() if not component.demo_mode else None
            if client is None:
                return dg.SensorResult(skip_reason="demo_mode=true -- no real AWS Batch jobs to observe.")

            events = []
            new_cursor = dict(cursor)
            for job_queue in job_queues:
                watermark = cursor.get(job_queue)
                latest_seen = watermark
                try:
                    paginator = client.get_paginator("list_jobs")
                    for page in paginator.paginate(jobQueue=job_queue, jobStatus="SUCCEEDED"):
                        for summary in page.get("jobSummaryList", []):
                            tags = summary.get("tags") or {}
                            if "dagster/run-id" in tags:
                                continue  # Dagster launched this one itself -- already materialized.
                            created_at = summary.get("createdAt")
                            if watermark and created_at is not None and created_at <= watermark:
                                continue
                            if created_at is not None and (latest_seen is None or created_at > latest_seen):
                                latest_seen = created_at
                            job_def_arn_or_name = summary.get("jobDefinition", "")
                            job_def_name = job_def_arn_or_name.split("/")[-1].split(":")[0]
                            asset_key = definition_name_to_key.get(job_def_name)
                            if asset_key is None:
                                continue
                            events.append(dg.AssetObservation(
                                asset_key=asset_key,
                                metadata={
                                    "batch/job_id": summary.get("jobId"),
                                    "batch/job_name": summary.get("jobName"),
                                    "batch/status": summary.get("status"),
                                    "batch/job_queue": job_queue,
                                },
                            ))
                except Exception as exc:  # noqa: BLE001
                    context.log.error(f"Error polling AWS Batch jobs for queue {job_queue!r}: {exc}")
                if latest_seen is not None:
                    new_cursor[job_queue] = latest_seen

            return dg.SensorResult(asset_events=events, cursor=json.dumps(new_cursor))

        return _aws_batch_observation_sensor


def _deep_merge(base: Dict[str, Any], overrides: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(base)
    for k, v in overrides.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out
