"""EcsPipesAssetComponent — launch a containerized batch job on AWS ECS via Dagster Pipes.

Wraps `dagster_aws.pipes.PipesECSClient` (the real, upstream client -- not
reimplemented here) to run a task on ECS (including GPU-backed capacity
providers), stream its CloudWatch logs and Pipes metadata back into the
Dagster event log, and materialize the result as a partitioned asset with
optional metadata-driven checks.

## Why this exists

No component in this registry wrapped ECS + Pipes before this one (confirmed
via a dedicated search: `aws ecs`, `pipes ecs`, `ecs task` all came back
empty or off-target). `modal_pipes_asset` is the closest existing shape --
same partition/backfill-policy/field conventions -- but launches Modal
serverless functions via `PipesSubprocessClient`, not an ECS task via
`PipesECSClient`. This component follows the same conventions for a
different (and more general-purpose) compute platform.

## Demo mode

`demo_mode: true` does NOT swap `PipesECSClient` for a different transport
and does NOT abstractly fake ECS API responses. It builds the exact same
`PipesECSClient`/`PipesCloudWatchMessageReader` real classes used in
production, injecting a pair of internal fake boto3 `ecs`/`logs` clients
instead of real ones. The fake `run_task` call launches `local_script_path`
as a real local subprocess with the container-override environment ECS would
inject, and writes its stdout to a file; the fake `logs` client serves that
growing file as paginated CloudWatch log events (`nextToken`/
`nextForwardToken` semantics), so Pipes messages and live logs stream back
exactly as they do from a real ECS task. Only the actual AWS API calls are
faked -- `PipesECSClient`'s and `PipesCloudWatchMessageReader`'s own
request-building, polling, and message-reading code all run unmodified in
both modes.
"""

import json
import os
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Union

import dagster as dg
from pydantic import Field



# BEGIN field-shape normalization (inlined per component; keep copies identical — see tests/test_field_shapes.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def coerce_field_shapes(data, *, lists=(), csv=(), tags=()):
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
    return out
# END field-shape normalization


# ---------------------------------------------------------------------------
# Demo-mode fakes: the boto3 ecs/logs client boundary ONLY.
# ---------------------------------------------------------------------------

class _DemoLogsClient:
    """Serves a local file as CloudWatch log streams (`awslogs` driver semantics)."""

    class exceptions:  # noqa: N801 -- mirrors boto3's client.exceptions namespace
        class ResourceNotFoundException(Exception):
            pass

        class ThrottlingException(Exception):
            pass

    def __init__(self) -> None:
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


class _DemoEcsClient:
    """Answers the handful of ECS calls `PipesECSClient.run` makes, by
    launching `local_script_path` as a real local subprocess."""

    def __init__(self, logs: _DemoLogsClient, local_script_path: str, log_group: str, stream_prefix: str, container_name: str):
        self._logs = logs
        self._script = local_script_path
        self._log_group = log_group
        self._stream_prefix = stream_prefix
        self._container_name = container_name
        self._processes: Dict[str, subprocess.Popen] = {}

    @property
    def meta(self):
        import types
        return types.SimpleNamespace(region_name="demo")

    def describe_task_definition(self, taskDefinition: str) -> dict:
        return {
            "taskDefinition": {
                "containerDefinitions": [
                    {
                        "name": self._container_name,
                        "logConfiguration": {
                            "logDriver": "awslogs",
                            "options": {
                                "awslogs-group": self._log_group,
                                "awslogs-stream-prefix": self._stream_prefix,
                                "awslogs-region": "demo",
                            },
                        },
                    }
                ]
            }
        }

    def run_task(self, **params) -> dict:
        task_id = uuid.uuid4().hex
        overrides = (params.get("overrides") or {}).get("containerOverrides", [{}])[0]
        environment = {item["name"]: item["value"] for item in overrides.get("environment", [])}
        log_dir = Path(os.environ.get("TMPDIR", "/tmp")) / "ecs_pipes_asset_demo_logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / f"{task_id}.log"
        env = {**os.environ, **environment, "PYTHONUNBUFFERED": "1"}
        handle = log_file.open("wb")
        process = subprocess.Popen(
            [sys.executable, self._script], env=env, stdout=handle, stderr=subprocess.STDOUT,
        )
        handle.close()
        arn = f"arn:aws:ecs:demo:000000000000:task/{params.get('cluster', 'cluster')}/{task_id}"
        self._processes[arn] = process
        self._logs.register(f"{self._stream_prefix}/{self._container_name}/{task_id}", log_file)
        return {"tasks": [{"taskArn": arn, "containers": [{"name": self._container_name}]}]}

    class _Waiter:
        def __init__(self, client: "_DemoEcsClient"):
            self._client = client

        def wait(self, tasks: List[str], **_ignored) -> None:
            self._client._processes[tasks[0]].wait()

    def get_waiter(self, name: str):
        assert name == "tasks_stopped", name
        return self._Waiter(self)

    def describe_tasks(self, tasks: List[str], cluster: Optional[str] = None) -> dict:
        arn = tasks[0]
        code = self._processes[arn].wait()
        return {
            "tasks": [
                {
                    "taskArn": arn,
                    "clusterArn": f"arn:aws:ecs:demo:000000000000:cluster/{cluster}",
                    "stopCode": "EssentialContainerExited",
                    "containers": [{"name": self._container_name, "exitCode": code, "runtimeId": arn[-12:]}],
                }
            ]
        }


# ---------------------------------------------------------------------------
# Configurable metadata-driven checks.
# ---------------------------------------------------------------------------

_COMPARATORS = {
    "gte": lambda v, t: v >= t,
    "lte": lambda v, t: v <= t,
    "eq": lambda v, t: v == t,
    "gt": lambda v, t: v > t,
    "lt": lambda v, t: v < t,
}


def _build_check_specs(asset_key: dg.AssetKey, metadata_checks: List[Dict[str, Any]]) -> List[dg.AssetCheckSpec]:
    specs = []
    for chk in metadata_checks:
        specs.append(dg.AssetCheckSpec(
            name=chk["name"],
            asset=asset_key,
            blocking=(chk.get("severity", "ERROR").upper() == "ERROR"),
        ))
    return specs


def _evaluate_checks(result: dg.MaterializeResult, asset_key: dg.AssetKey, metadata_checks: List[Dict[str, Any]]):
    for chk in metadata_checks:
        metadata_key = chk["metadata_key"]
        comparator = _COMPARATORS[chk.get("comparator", "gte")]
        threshold = chk["threshold"]
        raw = result.metadata.get(metadata_key)
        value = raw.value if hasattr(raw, "value") else raw
        passed = value is not None and comparator(value, threshold)
        severity = dg.AssetCheckSeverity.ERROR if chk.get("severity", "ERROR").upper() == "ERROR" else dg.AssetCheckSeverity.WARN
        yield dg.AssetCheckResult(
            asset_key=asset_key,
            check_name=chk["name"],
            passed=passed,
            severity=severity,
            metadata={"value": value, "threshold": threshold, "comparator": chk.get("comparator", "gte")},
        )


class EcsPipesAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Launch a containerized batch job on AWS ECS via Dagster Pipes and materialize the result.

    Wraps the real `dagster_aws.pipes.PipesECSClient` -- requires the task's
    own container to open a `dagster_pipes.open_dagster_pipes()` context and
    report back via `ctx.log`/`ctx.report_asset_materialization()`, the same
    protocol every other Pipes component in this registry uses.

    Example:
        ```yaml
        type: dagster_community_components.EcsPipesAssetComponent
        attributes:
          asset_name: gpu_batch_inference
          task_definition: my-inference-task:3
          container_name: inference
          cluster: my-gpu-cluster
          subnets: [subnet-0123456789abcdef0]
          aws_region: us-east-2
          capacity_provider: gpu-g6e
          log_group: /ecs/my-inference-task
          metadata_checks:
            - {name: schema_valid_rate, metadata_key: schema_valid_rate, comparator: gte, threshold: 0.98, severity: ERROR}
          demo_mode: true
          local_script_path: inference_task/run_batch.py
        ```
    """

    asset_name: str = Field(description="Dagster asset key emitted by this component.")

    # Required, AWS-account-specific -- no sensible defaults exist for any of these.
    task_definition: str = Field(description="ECS task definition family:revision (or ARN) to run.")
    container_name: str = Field(description="Name of the container (within the task definition) that opens the Pipes context.")
    cluster: str = Field(description="ECS cluster name or ARN to run the task in.")
    subnets: List[str] = Field(description="Subnet IDs for the task's awsvpc network configuration.")
    aws_region: str = Field(description="AWS region for the ecs/logs clients.")

    # Launch configuration.
    security_groups: Optional[List[str]] = Field(default=None, description="Security group IDs for the task's awsvpc network configuration.")
    assign_public_ip: Optional[str] = Field(
        default=None,
        description="ECS awsvpcConfiguration.assignPublicIp -- 'ENABLED' or 'DISABLED' (the literal strings ECS's API expects, not a boolean).",
    )
    capacity_provider: Optional[str] = Field(
        default=None,
        description="Single capacity provider name (e.g. a GPU capacity provider). Sugar for a one-entry capacityProviderStrategy. Mutually exclusive with capacity_provider_strategy and launch_type.",
    )
    capacity_provider_strategy: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Full ECS capacityProviderStrategy (list of {capacityProvider, weight, base}) for a weighted multi-provider strategy. Mutually exclusive with capacity_provider and launch_type.",
    )
    launch_type: Optional[str] = Field(
        default=None,
        description="ECS launchType ('FARGATE' or 'EC2') for accounts not using capacity providers. Mutually exclusive with capacity_provider and capacity_provider_strategy.",
    )
    container_command: Optional[List[str]] = Field(default=None, description="Command override for the container, e.g. ['python', 'run_batch.py'].")
    container_environment: Optional[Dict[str, str]] = Field(
        default=None,
        description="Environment variable overrides for the container, as a plain {name: value} dict -- converted internally to ECS's [{name, value}] list shape.",
    )
    run_task_params_overrides: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Raw overrides deep-merged on top of the run_task_params this component builds, for anything not covered by an explicit field above.",
    )

    # Pipes configuration.
    log_group: str = Field(description="CloudWatch log group the task's awslogs driver writes to.")
    log_stream_prefix: Optional[str] = Field(default=None, description="CloudWatch log stream prefix, if the task definition's awslogs-stream-prefix isn't the default.")
    extras: Optional[Dict[str, Any]] = Field(default=None, description="Arbitrary extra data passed to PipesECSClient.run(extras=...) and available to the task via the Pipes context.")
    pipes_container_name: Optional[str] = Field(default=None, description="Container name Pipes reads messages from, if different from container_name.")

    metadata_checks: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description=(
            "Configurable AssetCheckSpec/AssetCheckResult pairs driven by the Pipes result's own "
            "metadata, e.g. [{name: schema_valid_rate, metadata_key: schema_valid_rate, "
            "comparator: gte, threshold: 0.98, severity: ERROR}]. comparator is one of "
            "gte/lte/eq/gt/lt; severity is ERROR (blocking) or WARN. Only evaluated for a "
            "single-partition (or unpartitioned) run -- a single_run backfill range folds "
            "metadata from multiple task launches into one MaterializeResult and does not "
            "emit per-partition checks."
        ),
    )

    # Demo mode.
    demo_mode: bool = Field(
        default=True,
        description="Run the task as a real local subprocess against fake ECS/CloudWatch clients instead of a real ECS cluster. The real PipesECSClient/PipesCloudWatchMessageReader code runs unmodified in both modes -- only the boto3 API calls are faked. Set false for the real cluster.",
    )
    local_script_path: Optional[str] = Field(
        default=None,
        description="Script demo mode launches locally (via sys.executable) in place of the real ECS task. Required when demo_mode=true.",
    )

    # AWS credentials (optional if the environment's default credential chain already works).
    aws_access_key_id: Optional[str] = Field(default=None)
    aws_secret_access_key: Optional[str] = Field(default=None)
    aws_session_token: Optional[str] = Field(default=None)

    # Catalog / governance.
    group_name: Optional[str] = Field(default=None, description="Asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Asset tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds. Default: ['ecs', 'pipes', 'aws'].")

    # Partitioning -- same shape as modal_pipes_asset, this repo's shared convention.
    partition_type: Optional[str] = Field(default=None)
    partition_cron_schedule: Optional[str] = Field(default=None)
    partition_cron_fmt: Optional[str] = Field(default="%Y-%m-%d")
    partition_end_offset: int = Field(default=0)
    partition_timezone: Optional[str] = Field(default=None)
    partition_start: Optional[str] = Field(default=None)
    partition_values: Optional[Any] = Field(default=None)
    dynamic_partition_name: Optional[str] = Field(default=None)
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(default=None)
    backfill_policy: Optional[Literal["single_run", "multi_run"]] = Field(default=None)
    backfill_policy_max_partitions_per_run: Optional[int] = Field(default=None)

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=("owners", "kinds", "subnets", "security_groups", "container_command"), csv=(), tags=("tags",))

    @model_validator(mode="after")
    def _validate_launch_config(self):
        set_count = sum(bool(x) for x in (self.capacity_provider, self.capacity_provider_strategy, self.launch_type))
        if set_count > 1:
            raise ValueError("Set at most one of capacity_provider, capacity_provider_strategy, launch_type.")
        if self.demo_mode and not self.local_script_path:
            raise ValueError("local_script_path is required when demo_mode=true (nothing for the demo-mode fake to launch otherwise).")
        return self

    def _get_ecs_client(self):
        """Real-mode boto3 ecs client. Override point for a subclass or test."""
        import boto3
        session_kwargs: Dict[str, Any] = {"region_name": self.aws_region}
        if self.aws_access_key_id and self.aws_secret_access_key:
            session_kwargs["aws_access_key_id"] = self.aws_access_key_id
            session_kwargs["aws_secret_access_key"] = self.aws_secret_access_key
        if self.aws_session_token:
            session_kwargs["aws_session_token"] = self.aws_session_token
        return boto3.Session(**session_kwargs).client("ecs")

    def _get_logs_client(self):
        """Real-mode boto3 logs client. Override point for a subclass or test."""
        import boto3
        session_kwargs: Dict[str, Any] = {"region_name": self.aws_region}
        if self.aws_access_key_id and self.aws_secret_access_key:
            session_kwargs["aws_access_key_id"] = self.aws_access_key_id
            session_kwargs["aws_secret_access_key"] = self.aws_secret_access_key
        if self.aws_session_token:
            session_kwargs["aws_session_token"] = self.aws_session_token
        return boto3.Session(**session_kwargs).client("logs")

    def _build_pipes_client(self):
        from dagster_aws.pipes import PipesCloudWatchMessageReader, PipesECSClient

        if self.demo_mode:
            logs = _DemoLogsClient()
            ecs = _DemoEcsClient(logs, self.local_script_path, self.log_group, "ecs", self.container_name)
            return PipesECSClient(client=ecs, message_reader=PipesCloudWatchMessageReader(client=logs))
        return PipesECSClient(
            client=self._get_ecs_client(),
            message_reader=PipesCloudWatchMessageReader(
                client=self._get_logs_client(), log_group=self.log_group, log_stream=self.log_stream_prefix,
            ),
        )

    def _build_run_task_params(self) -> Dict[str, Any]:
        params: Dict[str, Any] = {
            "taskDefinition": self.task_definition,
            "cluster": self.cluster,
        }
        if self.capacity_provider:
            params["capacityProviderStrategy"] = [{"capacityProvider": self.capacity_provider, "weight": 1}]
        elif self.capacity_provider_strategy:
            params["capacityProviderStrategy"] = self.capacity_provider_strategy
        elif self.launch_type:
            params["launchType"] = self.launch_type

        network_config: Dict[str, Any] = {"subnets": self.subnets}
        if self.security_groups:
            network_config["securityGroups"] = self.security_groups
        if self.assign_public_ip:
            network_config["assignPublicIp"] = self.assign_public_ip
        params["networkConfiguration"] = {"awsvpcConfiguration": network_config}

        if self.container_command or self.container_environment:
            container_override: Dict[str, Any] = {"name": self.container_name}
            if self.container_command:
                container_override["command"] = self.container_command
            if self.container_environment:
                container_override["environment"] = [{"name": k, "value": v} for k, v in self.container_environment.items()]
            params["overrides"] = {"containerOverrides": [container_override]}

        if self.run_task_params_overrides:
            params = _deep_merge(params, self.run_task_params_overrides)
        return params

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="ECS Pipes Asset", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        component = self
        asset_key = dg.AssetKey.from_user_string(self.asset_name)
        kinds_set = set(self.kinds or []) | {"ecs", "pipes", "aws"}
        tag_map = dict(self.tags or {})
        for k in kinds_set:
            tag_map[f"dagster/kind/{k}"] = ""

        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values,
            self.dynamic_partition_name, self.partition_dimensions,
            cron_schedule=self.partition_cron_schedule,
            cron_fmt=self.partition_cron_fmt or "%Y-%m-%d",
            end_offset=self.partition_end_offset, timezone=self.partition_timezone,
        )

        _backfill_policy_obj = None
        if partitions_def is not None and self.backfill_policy == "single_run":
            _backfill_policy_obj = dg.BackfillPolicy.single_run()
        elif partitions_def is not None and self.backfill_policy == "multi_run":
            _backfill_policy_obj = dg.BackfillPolicy.multi_run(
                max_partitions_per_run=self.backfill_policy_max_partitions_per_run or 1
            )

        check_specs = _build_check_specs(asset_key, self.metadata_checks or [])

        @dg.multi_asset(
            specs=[dg.AssetSpec(
                asset_key, description=self.description or f"ECS task {self.task_definition} via Dagster Pipes",
                group_name=self.group_name, owners=self.owners or [], tags=tag_map, kinds=kinds_set,
                partitions_def=partitions_def,
            )],
            check_specs=check_specs,
            backfill_policy=_backfill_policy_obj,
            name=self.asset_name,
        )
        def _ecs_pipes_asset(context: dg.AssetExecutionContext):
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
                    run_task_params=component._build_run_task_params(),
                    extras=extras or None,
                    pipes_container_name=component.pipes_container_name or component.container_name,
                )

            if not is_range:
                result = _run_one(partition_keys[0]).get_materialize_result()
                yield result
                yield from _evaluate_checks(result, asset_key, component.metadata_checks or [])
                return

            # Range: one ECS task launch per partition, folded into one MaterializeResult.
            # Metadata checks are not evaluated per-partition in this path -- see metadata_checks' own docstring.
            merged_metadata: Dict[str, Any] = {"partition_keys": dg.MetadataValue.json(partition_keys)}
            for pk in partition_keys:
                invocation = _run_one(pk)
                for res in invocation.get_results():
                    for mk, mv in (getattr(res, "metadata", None) or {}).items():
                        merged_metadata[f"{pk}/{mk}"] = mv
            yield dg.MaterializeResult(asset_key=asset_key, metadata=merged_metadata)

        return dg.Definitions(assets=[_ecs_pipes_asset])


def _deep_merge(base: Dict[str, Any], overrides: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(base)
    for k, v in overrides.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def _build_partitions_def(
    partition_type, partition_start, partition_values,
    dynamic_partition_name, partition_dimensions,
    cron_schedule=None,
    cron_fmt="%Y-%m-%d",
    end_offset=0,
    timezone=None,
):
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )
    if partition_dimensions and partition_type:
        raise ValueError("Set either partition_type or partition_dimensions, not both.")

    def _build_axis(spec):
        t = spec.get("type")
        _end_offset = spec.get("end_offset", 0)
        _timezone = spec.get("timezone")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily": return DailyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "weekly": return WeeklyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "monthly": return MonthlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "hourly": return HourlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "cron":
            from dagster import TimeWindowPartitionsDefinition
            if not spec.get("start"):
                raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
            if not spec.get("cron_schedule"):
                raise ValueError("partition dimension type='cron' requires 'cron_schedule'")
            return TimeWindowPartitionsDefinition(start=spec["start"], fmt=spec.get("fmt") or "%Y-%m-%d", cron_schedule=spec["cron_schedule"], end_offset=_end_offset, timezone=_timezone)
        if t == "static":
            vals = spec.get("values") or []
            if isinstance(vals, str):
                vals = [v.strip() for v in vals.split(",") if v.strip()]
            if not vals:
                raise ValueError("static partition requires non-empty 'values'")
            return StaticPartitionsDefinition(list(vals))
        if t == "dynamic":
            name = spec.get("dynamic_partition_name") or spec.get("name")
            if not name:
                raise ValueError("dynamic partition requires a name")
            return DynamicPartitionsDefinition(name=name)
        raise ValueError(f"unknown partition type: {t!r}")

    if partition_dimensions:
        if len(partition_dimensions) == 1:
            return _build_axis(partition_dimensions[0])
        return MultiPartitionsDefinition({d["name"]: _build_axis(d) for d in partition_dimensions})

    if not partition_type:
        return None
    if isinstance(partition_values, (list, tuple)):
        _values = [str(v).strip() for v in partition_values if str(v).strip()]
    else:
        _values = [v.strip() for v in (str(partition_values) if partition_values else "").split(",") if v.strip()]
    if partition_type in ("daily", "weekly", "monthly", "hourly", "cron") and not partition_start:
        raise ValueError(f"partition_type={partition_type!r} requires partition_start.")
    if partition_type == "daily": return DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "weekly": return WeeklyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "monthly": return MonthlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "hourly": return HourlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "cron":
        from dagster import TimeWindowPartitionsDefinition
        if not cron_schedule:
            raise ValueError("partition_type='cron' requires cron_schedule, e.g. '0 0 * * 1-5' for weekdays only.")
        return TimeWindowPartitionsDefinition(start=partition_start, fmt=cron_fmt, cron_schedule=cron_schedule, end_offset=end_offset, timezone=timezone)
    if partition_type == "static":
        if not _values: raise ValueError("static requires values")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError("dynamic requires dynamic_partition_name")
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    raise ValueError(f"unknown partition_type: {partition_type!r}")
