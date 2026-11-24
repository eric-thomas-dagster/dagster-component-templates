"""AWS Step Functions Asset Component.

Extends StateBackedComponent so the AWS Step Functions API is called once at
prepare time (write_state_to_path) and the discovered state machine list is cached
on disk. build_defs_from_state builds one asset per state machine with zero network
calls, keeping code-server reloads instant.

On first load (state_path is None) returns empty Definitions — run
`dg utils refresh-defs-state` or `dagster dev` to populate the cache.
"""
import json
import re
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import dagster as dg

# Execution names Dagster itself starts carry this prefix -- the observation
# sensor (ListExecutions) skips any execution whose name starts with it, so a
# run Dagster already knows about doesn't get double-reported as an external
# observation. Step Functions' StartExecution has no `tags` parameter (unlike
# ECS/Batch/Glue), so a tag-based convention isn't available here -- confirmed
# via the real boto3 `stepfunctions` service model (StartExecution only takes
# stateMachineArn/name/input/traceHeader). The execution `name` is the only
# per-execution identifier Step Functions exposes, so it's the correlation key.
DAGSTER_EXECUTION_NAME_PREFIX = "dagster-"

try:
    from dagster.components.component.state_backed_component import StateBackedComponent
    from dagster.components.utils.defs_state import (
        DefsStateConfig,
        DefsStateConfigArgs,
        ResolvedDefsStateConfig,
    )
    _HAS_STATE_BACKED = True
except ImportError:
    StateBackedComponent = None
    _HAS_STATE_BACKED = False


# ── Helpers ───────────────────────────────────────────────────────────────────

def _sanitize_name(name: str) -> str:
    """Convert a state machine name to a valid Dagster asset key segment."""
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


def _make_boto3_client(region_name: str, aws_profile: Optional[str], role_arn: Optional[str]):
    """Return a boto3 SFN client, optionally assuming a cross-account role."""
    import boto3

    session = boto3.Session(profile_name=aws_profile, region_name=region_name)

    if role_arn:
        sts = session.client("sts")
        creds = sts.assume_role(
            RoleArn=role_arn,
            RoleSessionName="DagsterStepFunctionsComponent",
        )["Credentials"]
        session = boto3.Session(
            aws_access_key_id=creds["AccessKeyId"],
            aws_secret_access_key=creds["SecretAccessKey"],
            aws_session_token=creds["SessionToken"],
            region_name=region_name,
        )

    return session.client("stepfunctions")


def _list_state_machines(
    sfn,
    name_prefix: Optional[str],
    exclude_names: Optional[list[str]],
) -> list[dict]:
    """Page through ListStateMachines and apply filters."""
    exclude_set = set(exclude_names or [])
    machines = []
    paginator = sfn.get_paginator("list_state_machines")
    for page in paginator.paginate():
        for sm in page.get("stateMachines", []):
            name = sm["name"]
            if name_prefix and not name.startswith(name_prefix):
                continue
            if name in exclude_set:
                continue
            machines.append({
                "name": name,
                "arn": sm["stateMachineArn"],
                "type": sm.get("type", "STANDARD"),
                "creation_date_str": sm["creationDate"].isoformat()
                if hasattr(sm["creationDate"], "isoformat")
                else str(sm["creationDate"]),
            })
    return machines


def _build_partitions_def(
    partition_type, partition_start, partition_values, dynamic_partition_name,
    cron_schedule=None, cron_fmt="%Y-%m-%d", end_offset=0, timezone=None,
):
    """Construct a Dagster partitions_def from the canonical flat partition fields.

    Strict: raises ValueError on misconfigured combinations rather than silently
    picking a default (time-based types need partition_start; static needs
    partition_values; dynamic needs dynamic_partition_name). Same shape as
    ssis_workspace/ecs_pipes_asset's own `_build_partitions_def` -- inlined here
    per this registry's no-shared-helper-files convention.
    """
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, DynamicPartitionsDefinition,
    )

    if not partition_type:
        return None
    if partition_type in ("daily", "weekly", "monthly", "hourly", "cron") and not partition_start:
        raise ValueError(
            f"partition_type={partition_type!r} requires partition_start (ISO date, e.g. '2024-01-01')."
        )
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


def _build_execution_input(input_template: Optional[str], context) -> str:
    """Render the StartExecution `input` JSON string, substituting {partition_key}
    when the run is partitioned. Defaults to '{}' (the previous hardcoded value)
    when no template is configured, so existing users see no behavior change."""
    template = input_template if input_template is not None else "{}"
    if "{partition_key}" in template and getattr(context, "has_partition_key", False):
        template = template.replace("{partition_key}", context.partition_key)
    # Validate it's well-formed JSON before handing it to StartExecution -- a bad
    # template should fail fast with a clear message, not a cryptic boto3 error.
    json.loads(template)
    return template


def _merge_spec(base: dg.AssetSpec, ov: dict) -> dg.AssetSpec:
    """Merge an override dict into a base AssetSpec."""
    extra_deps = [dg.AssetKey.from_user_string(d) for d in ov.get("deps", [])]
    return dg.AssetSpec(
        key=dg.AssetKey.from_user_string(ov["key"]) if "key" in ov else base.key,
        description=ov.get("description", base.description),
        group_name=ov.get("group_name", base.group_name),
        metadata={**(base.metadata or {}), **(ov.get("metadata") or {})},
        tags={**(base.tags or {}), **(ov.get("tags") or {})},
        kinds=set(ov["kinds"]) if "kinds" in ov else base.kinds,
        deps=list(base.deps or []) + extra_deps,
        owners=list(ov["owners"]) if "owners" in ov else (list(base.owners) if base.owners else None),
        partitions_def=base.partitions_def,
    )


def _apply_item_overrides(
    default_spec: dg.AssetSpec,
    item_name: str,
    overrides: Optional[dict],
) -> list[dg.AssetSpec]:
    """Apply assets_by_X overrides. Returns list (usually 1, but >1 if one item → multiple assets)."""
    if not overrides or item_name not in overrides:
        return [default_spec]
    ov = overrides[item_name]
    if isinstance(ov, list):
        return [_merge_spec(default_spec, o) for o in ov]
    return [_merge_spec(default_spec, ov)]


def _output_metadata(desc: dict) -> dict:
    """Surface DescribeExecution's `output` (present once a SUCCEEDED execution's
    result is available) as materialization metadata. Previously discarded entirely."""
    raw_output = desc.get("output")
    if raw_output is None:
        return {}
    try:
        return {"step_functions/output": dg.MetadataValue.json(json.loads(raw_output))}
    except (TypeError, ValueError):
        return {"step_functions/output": dg.MetadataValue.text(str(raw_output))}


def _make_single_asset(
    spec, sm_arn, sm_name, wait_for_completion, poll_interval_seconds,
    execution_timeout_seconds, region_name, aws_profile, role_arn,
    input_template, partitions_def, terminal_statuses,
):
    """Factory function, not a default-argument trick (`def _asset(_arn=sm_arn, ...)`):
    Dagster's asset machinery treats any non-`context` parameter on an `@dg.asset`
    function as a real input to load via the project's default IO manager, even
    when it carries a default value (same gap documented -- and fixed -- in
    ssis_workspace's `_build_freshness_checks`). The default-argument form only
    dodges Python's closure late-binding bug for the LOOP variable; it doesn't
    stop Dagster from treating `_poll`/`_region`/etc. as asset dependencies. This
    was invisible before because no test previously exercised `dg.materialize()`
    against this component's execute path -- confirmed by the exact
    `DagsterInvalidDefinitionError: Input asset ["_poll"] is not produced...`
    this function's own tests catch pre-fix."""
    import dagster as _dg

    @_dg.asset(
        key=spec.key,
        group_name=spec.group_name,
        description=spec.description,
        metadata=dict(spec.metadata or {}),
        tags=dict(spec.tags or {}),
        kinds=spec.kinds or {"aws", "step_functions"},
        deps=list(spec.deps or []),
        owners=list(spec.owners or []),
        partitions_def=partitions_def,
    )
    def _asset(context: _dg.AssetExecutionContext) -> _dg.MaterializeResult:
        sfn = _make_boto3_client(region_name, aws_profile, role_arn)

        execution_input = _build_execution_input(input_template, context)
        execution_name = f"{DAGSTER_EXECUTION_NAME_PREFIX}{context.run.run_id[:8]}-{uuid.uuid4().hex[:8]}"
        context.log.info(f"Starting Step Functions execution for: {sm_name}")
        start_resp = sfn.start_execution(
            stateMachineArn=sm_arn,
            name=execution_name,
            input=execution_input,
        )
        execution_arn = start_resp["executionArn"]
        context.log.info(f"Execution started: {execution_arn}")

        if not wait_for_completion:
            return _dg.MaterializeResult(
                metadata={
                    "execution_arn": _dg.MetadataValue.text(execution_arn),
                    "status": _dg.MetadataValue.text("RUNNING"),
                }
            )

        elapsed = 0
        status = "RUNNING"
        desc: dict = {}
        while elapsed < execution_timeout_seconds:
            time.sleep(poll_interval_seconds)
            elapsed += poll_interval_seconds
            desc = sfn.describe_execution(executionArn=execution_arn)
            status = desc["status"]
            context.log.info(f"Execution {execution_arn} status: {status} (elapsed {elapsed}s)")
            if status in terminal_statuses:
                break

        if status != "SUCCEEDED":
            raise Exception(f"Step Functions execution {execution_arn} ended with status: {status}")

        return _dg.MaterializeResult(
            metadata={
                "execution_arn": _dg.MetadataValue.text(execution_arn),
                "status": _dg.MetadataValue.text(status),
                **_output_metadata(desc),
            }
        )

    return _asset


def _make_multi_asset(
    specs, safe_name, sm_arn, sm_name, wait_for_completion, poll_interval_seconds,
    execution_timeout_seconds, region_name, aws_profile, role_arn,
    input_template, terminal_statuses,
):
    """Factory function -- same closure/IO-manager-input reason documented on
    `_make_single_asset` above."""
    import dagster as _dg

    @_dg.multi_asset(specs=specs, name=f"sfn_{safe_name}_multi")
    def _multi_asset(context: _dg.AssetExecutionContext):
        sfn = _make_boto3_client(region_name, aws_profile, role_arn)

        execution_input = _build_execution_input(input_template, context)
        execution_name = f"{DAGSTER_EXECUTION_NAME_PREFIX}{context.run.run_id[:8]}-{uuid.uuid4().hex[:8]}"
        context.log.info(f"Starting Step Functions execution for: {sm_name}")
        start_resp = sfn.start_execution(
            stateMachineArn=sm_arn,
            name=execution_name,
            input=execution_input,
        )
        execution_arn = start_resp["executionArn"]
        context.log.info(f"Execution started: {execution_arn}")

        if not wait_for_completion:
            for spec in specs:
                yield _dg.MaterializeResult(
                    asset_key=spec.key,
                    metadata={
                        "execution_arn": _dg.MetadataValue.text(execution_arn),
                        "status": _dg.MetadataValue.text("RUNNING"),
                    },
                )
            return

        elapsed = 0
        status = "RUNNING"
        desc: dict = {}
        while elapsed < execution_timeout_seconds:
            time.sleep(poll_interval_seconds)
            elapsed += poll_interval_seconds
            desc = sfn.describe_execution(executionArn=execution_arn)
            status = desc["status"]
            context.log.info(f"Execution {execution_arn} status: {status} (elapsed {elapsed}s)")
            if status in terminal_statuses:
                break

        if status != "SUCCEEDED":
            raise Exception(f"Step Functions execution {execution_arn} ended with status: {status}")

        for spec in specs:
            yield _dg.MaterializeResult(
                asset_key=spec.key,
                metadata={
                    "execution_arn": _dg.MetadataValue.text(execution_arn),
                    "status": _dg.MetadataValue.text(status),
                    **_output_metadata(desc),
                },
            )

    return _multi_asset


def _build_step_functions_defs(
    machines: list[dict],
    region_name: str,
    aws_profile: Optional[str],
    role_arn: Optional[str],
    group_name: str,
    key_prefix: Optional[str],
    wait_for_completion: bool,
    poll_interval_seconds: int,
    execution_timeout_seconds: int,
    assets_by_state_machine_name: Optional[dict] = None,
    input_template: Optional[str] = None,
    partitions_def=None,
    generate_sensor: bool = False,
    observation_interval_seconds: int = 300,
) -> dg.Definitions:
    """Build one @dg.asset per state machine (no network calls at def-build time)."""
    terminal_statuses = {"SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED"}

    assets = []
    # sm_name -> resolved (post-override) AssetKeys, so the observation sensor
    # reports under the SAME keys the executable assets above actually use --
    # see aws_glue/ssis_workspace/snowflake_workspace for the same bug class
    # (sensor independently re-deriving a pre-override key).
    resolved_keys_by_sm_name: dict[str, list[dg.AssetKey]] = {}
    sm_arn_by_name: dict[str, str] = {}

    for sm in machines:
        safe_name = _sanitize_name(sm["name"])
        key_parts = [key_prefix, safe_name] if key_prefix else [safe_name]
        asset_key = dg.AssetKey(key_parts)
        sm_arn = sm["arn"]
        sm_name = sm["name"]
        sm_arn_by_name[sm_name] = sm_arn

        default_spec = dg.AssetSpec(
            key=asset_key,
            description=f"AWS Step Functions state machine: {sm_name}",
            group_name=group_name,
            kinds={"aws", "step_functions"},
            metadata={
                "step_functions/state_machine_arn": dg.MetadataValue.text(sm_arn),
                "step_functions/state_machine_type": dg.MetadataValue.text(sm.get("type", "STANDARD")),
                "step_functions/region": dg.MetadataValue.text(region_name),
            },
            partitions_def=partitions_def,
        )
        expanded_specs = _apply_item_overrides(default_spec, sm_name, assets_by_state_machine_name)
        resolved_keys_by_sm_name[sm_name] = [s.key for s in expanded_specs]

        if len(expanded_specs) == 1:
            spec = expanded_specs[0]
            assets.append(_make_single_asset(
                spec=spec, sm_arn=sm_arn, sm_name=sm_name,
                wait_for_completion=wait_for_completion, poll_interval_seconds=poll_interval_seconds,
                execution_timeout_seconds=execution_timeout_seconds, region_name=region_name,
                aws_profile=aws_profile, role_arn=role_arn, input_template=input_template,
                partitions_def=partitions_def, terminal_statuses=terminal_statuses,
            ))
        else:
            # One state machine → multiple Dagster assets
            assets.append(_make_multi_asset(
                specs=expanded_specs, safe_name=safe_name, sm_arn=sm_arn, sm_name=sm_name,
                wait_for_completion=wait_for_completion, poll_interval_seconds=poll_interval_seconds,
                execution_timeout_seconds=execution_timeout_seconds, region_name=region_name,
                aws_profile=aws_profile, role_arn=role_arn, input_template=input_template,
                terminal_statuses=terminal_statuses,
            ))

    sensors = []
    if generate_sensor and resolved_keys_by_sm_name:
        sensors.append(_build_observation_sensor(
            resolved_keys_by_sm_name=resolved_keys_by_sm_name,
            sm_arn_by_name=sm_arn_by_name,
            region_name=region_name,
            aws_profile=aws_profile,
            role_arn=role_arn,
            observation_interval_seconds=observation_interval_seconds,
        ))

    if not assets:
        return dg.Definitions()

    return dg.Definitions(assets=assets, sensors=sensors or None)


def _build_observation_sensor(
    resolved_keys_by_sm_name: dict,
    sm_arn_by_name: dict,
    region_name: str,
    aws_profile: Optional[str],
    role_arn: Optional[str],
    observation_interval_seconds: int,
):
    """Poll ListExecutions per state machine for completions Dagster didn't
    itself start (any execution name NOT carrying DAGSTER_EXECUTION_NAME_PREFIX),
    and emit AssetObservation under the component's OWN resolved (post-override)
    asset keys -- never independently reconstructed ones."""
    all_keys = [k for keys in resolved_keys_by_sm_name.values() for k in keys]

    @dg.sensor(
        name="step_functions_observation_sensor",
        minimum_interval_seconds=observation_interval_seconds,
        default_status=dg.DefaultSensorStatus.STOPPED,
        asset_selection=dg.AssetSelection.assets(*all_keys),
    )
    def _observation_sensor(context: dg.SensorEvaluationContext):
        import json as _json

        cursor = _json.loads(context.cursor) if context.cursor else {}
        sfn = _make_boto3_client(region_name, aws_profile, role_arn)
        observations = []
        new_cursor = dict(cursor)

        for sm_name, keys in resolved_keys_by_sm_name.items():
            sm_arn = sm_arn_by_name[sm_name]
            watermark = cursor.get(sm_name)
            latest_seen = watermark
            try:
                paginator = sfn.get_paginator("list_executions")
                for page in paginator.paginate(stateMachineArn=sm_arn, statusFilter="SUCCEEDED"):
                    for execn in page.get("executions", []):
                        name = execn.get("name") or ""
                        if name.startswith(DAGSTER_EXECUTION_NAME_PREFIX):
                            continue  # Dagster launched this one itself -- already materialized.
                        stop_date = execn.get("stopDate")
                        stop_iso = stop_date.isoformat() if hasattr(stop_date, "isoformat") else str(stop_date)
                        if watermark and stop_iso <= watermark:
                            continue
                        if latest_seen is None or stop_iso > latest_seen:
                            latest_seen = stop_iso
                        for key in keys:
                            observations.append(dg.AssetObservation(
                                asset_key=key,
                                metadata={
                                    "step_functions/execution_arn": execn.get("executionArn"),
                                    "step_functions/execution_name": name,
                                    "step_functions/status": execn.get("status"),
                                    "step_functions/stop_date": stop_iso,
                                },
                            ))
            except Exception as exc:
                context.log.error(f"Error polling executions for state machine {sm_name!r}: {exc}")
            if latest_seen is not None:
                new_cursor[sm_name] = latest_seen

        return dg.SensorResult(asset_events=observations, cursor=_json.dumps(new_cursor))

    return _observation_sensor


# ── StateBackedComponent variant ──────────────────────────────────────────────

if _HAS_STATE_BACKED:
    @dataclass
    class StepFunctionsAssetComponent(StateBackedComponent, dg.Resolvable):
        """AWS Step Functions Asset Component.

        Connects to AWS Step Functions at prepare time, discovers all matching
        state machines, and creates one Dagster asset per state machine. At
        execution time each asset starts a Step Functions execution and (optionally)
        waits for it to reach a terminal status.

        Uses StateBackedComponent so the state machine list is fetched once and
        cached — code-server reloads are instant. Refresh with:
          dagster dev   (automatic in dev)
          dg utils refresh-defs-state   (CI/CD / image build)

        Example:
            ```yaml
            type: dagster_community_components.StepFunctionsAssetComponent
            attributes:
              region_name: us-east-1
              name_prefix: "data-pipeline-"
              group_name: step_functions
              wait_for_completion: true
            ```
        """

        region_name: str
        aws_profile: Optional[str] = None
        role_arn: Optional[str] = None
        name_prefix: Optional[str] = None
        exclude_names: Optional[list[str]] = None
        group_name: str = "step_functions"
        key_prefix: Optional[str] = None
        wait_for_completion: bool = True
        poll_interval_seconds: int = 10
        execution_timeout_seconds: int = 3600
        assets_by_state_machine_name: Optional[dict] = None
        input_template: Optional[str] = None
        generate_sensor: bool = False
        observation_interval_seconds: int = 300
        partition_type: Optional[str] = None
        partition_start: Optional[str] = None
        partition_values: Optional[Any] = None
        dynamic_partition_name: Optional[str] = None
        partition_cron_schedule: Optional[str] = None
        partition_cron_fmt: str = "%Y-%m-%d"
        partition_end_offset: int = 0
        partition_timezone: Optional[str] = None
        defs_state: ResolvedDefsStateConfig = field(
            default_factory=DefsStateConfigArgs.local_filesystem
        )

        @property
        def _partitions_def(self):
            return _build_partitions_def(
                self.partition_type, self.partition_start, self.partition_values, self.dynamic_partition_name,
                cron_schedule=self.partition_cron_schedule, cron_fmt=self.partition_cron_fmt or "%Y-%m-%d",
                end_offset=self.partition_end_offset, timezone=self.partition_timezone,
            )

        @property
        def defs_state_config(self) -> DefsStateConfig:
            return DefsStateConfig.from_args(
                self.defs_state,
                default_key=f"StepFunctionsAssetComponent[{self.region_name}]",
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
                        except OSError:  # e.g. Windows: directory held open by another process
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

        def write_state_to_path(self, state_path: Path) -> None:
            """Discover state machines via AWS API and cache list to disk."""
            sfn = _make_boto3_client(self.region_name, self.aws_profile, self.role_arn)
            machines = _list_state_machines(sfn, self.name_prefix, self.exclude_names)
            state_path.write_text(json.dumps(machines, default=str))

        def build_defs_from_state(
            self, context: dg.ComponentLoadContext, state_path: Optional[Path]
        ) -> dg.Definitions:
            """Build asset definitions from cached state — no network calls."""
            if state_path is None or not state_path.exists():
                if hasattr(context, "log"):
                    context.log.warning(  # type: ignore[union-attr]
                        "StepFunctionsAssetComponent: no cached state found. "
                        "Run `dg utils refresh-defs-state` or `dagster dev` to populate."
                    )
                return dg.Definitions()

            machines = json.loads(state_path.read_text())
            return _build_step_functions_defs(
                machines=machines,
                region_name=self.region_name,
                aws_profile=self.aws_profile,
                role_arn=self.role_arn,
                group_name=self.group_name,
                key_prefix=self.key_prefix,
                wait_for_completion=self.wait_for_completion,
                poll_interval_seconds=self.poll_interval_seconds,
                execution_timeout_seconds=self.execution_timeout_seconds,
                assets_by_state_machine_name=self.assets_by_state_machine_name,
                input_template=self.input_template,
                partitions_def=self._partitions_def,
                generate_sensor=self.generate_sensor,
                observation_interval_seconds=self.observation_interval_seconds,
            )


else:
    # Fallback: StateBackedComponent not available in this dagster version.
    # Calls the AWS API on every build_defs invocation (no caching).
    class StepFunctionsAssetComponent(dg.Component, dg.Model, dg.Resolvable):  # type: ignore[no-redef]
        """AWS Step Functions Asset Component (fallback: no state caching).

        Upgrade to dagster>=1.8 to get StateBackedComponent caching.
        """

        region_name: str
        aws_profile: Optional[str] = None
        role_arn: Optional[str] = None
        name_prefix: Optional[str] = None
        exclude_names: Optional[list[str]] = None
        group_name: str = "step_functions"
        key_prefix: Optional[str] = None
        wait_for_completion: bool = True
        poll_interval_seconds: int = 10
        execution_timeout_seconds: int = 3600
        assets_by_state_machine_name: Optional[dict] = None
        input_template: Optional[str] = None
        generate_sensor: bool = False
        observation_interval_seconds: int = 300
        partition_type: Optional[str] = None
        partition_start: Optional[str] = None
        partition_values: Optional[Any] = None
        dynamic_partition_name: Optional[str] = None
        partition_cron_schedule: Optional[str] = None
        partition_cron_fmt: str = "%Y-%m-%d"
        partition_end_offset: int = 0
        partition_timezone: Optional[str] = None

        @property
        def _partitions_def(self):
            return _build_partitions_def(
                self.partition_type, self.partition_start, self.partition_values, self.dynamic_partition_name,
                cron_schedule=self.partition_cron_schedule, cron_fmt=self.partition_cron_fmt or "%Y-%m-%d",
                end_offset=self.partition_end_offset, timezone=self.partition_timezone,
            )

        @classmethod
        def get_form_config(cls):
            from dagster.components.resolved.form_config import ComponentFormConfig
            return ComponentFormConfig(label='Step Functions Asset', editable=True)

        def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
            sfn = _make_boto3_client(self.region_name, self.aws_profile, self.role_arn)
            machines = _list_state_machines(sfn, self.name_prefix, self.exclude_names)
            return _build_step_functions_defs(
                machines=machines,
                region_name=self.region_name,
                aws_profile=self.aws_profile,
                role_arn=self.role_arn,
                group_name=self.group_name,
                key_prefix=self.key_prefix,
                wait_for_completion=self.wait_for_completion,
                poll_interval_seconds=self.poll_interval_seconds,
                execution_timeout_seconds=self.execution_timeout_seconds,
                assets_by_state_machine_name=self.assets_by_state_machine_name,
                input_template=self.input_template,
                partitions_def=self._partitions_def,
                generate_sensor=self.generate_sensor,
                observation_interval_seconds=self.observation_interval_seconds,
            )
