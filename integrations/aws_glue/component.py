"""AWS Glue Component.

Import AWS Glue jobs, crawlers, workflows, DataBrew recipes, and data quality rulesets
as Dagster assets with automatic lineage tracking from Data Catalog tables.

Uses StateBackedComponent so AWS API calls are made once at prepare time
(write_state_to_path) and cached on disk. build_defs_from_state builds all
asset definitions from the cached state with zero network calls, keeping
code-server reloads fast.

On first load (state_path is None) returns empty Definitions — run
`dg utils refresh-defs-state` or `dagster dev` to populate the cache.
"""
import json
import re
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import boto3
import dagster as dg
from botocore.exceptions import ClientError

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

def _sanitize(name: str) -> str:
    """Lowercase and replace non-alphanumeric characters with underscores."""
    return re.sub(r"[^a-zA-Z0-9_]", "_", name.lower())


def _make_glue_client(
    region_name: str,
    aws_access_key_id: Optional[str] = None,
    aws_secret_access_key: Optional[str] = None,
    aws_session_token: Optional[str] = None,
):
    session_kwargs: Dict[str, Any] = {"region_name": region_name}
    if aws_access_key_id and aws_secret_access_key:
        session_kwargs["aws_access_key_id"] = aws_access_key_id
        session_kwargs["aws_secret_access_key"] = aws_secret_access_key
    if aws_session_token:
        session_kwargs["aws_session_token"] = aws_session_token
    return boto3.Session(**session_kwargs).client("glue")


def _make_databrew_client(
    region_name: str,
    aws_access_key_id: Optional[str] = None,
    aws_secret_access_key: Optional[str] = None,
    aws_session_token: Optional[str] = None,
):
    session_kwargs: Dict[str, Any] = {"region_name": region_name}
    if aws_access_key_id and aws_secret_access_key:
        session_kwargs["aws_access_key_id"] = aws_access_key_id
        session_kwargs["aws_secret_access_key"] = aws_secret_access_key
    if aws_session_token:
        session_kwargs["aws_session_token"] = aws_session_token
    return boto3.Session(**session_kwargs).client("databrew")


def _make_s3_client(
    region_name: str,
    aws_access_key_id: Optional[str] = None,
    aws_secret_access_key: Optional[str] = None,
    aws_session_token: Optional[str] = None,
):
    """Used only for `use_pipes: true`'s PipesS3ContextInjector/PipesS3MessageReader --
    Glue jobs read their Pipes bootstrap context from an S3 object (there's no
    env-var or tag mechanism Glue exposes the way ECS/Batch do), confirmed via
    the real installed `dagster_aws.pipes.clients.glue.PipesGlueClient`, which
    requires an explicit `context_injector` (no default, unlike PipesECSClient)."""
    session_kwargs: Dict[str, Any] = {"region_name": region_name}
    if aws_access_key_id and aws_secret_access_key:
        session_kwargs["aws_access_key_id"] = aws_access_key_id
        session_kwargs["aws_secret_access_key"] = aws_secret_access_key
    if aws_session_token:
        session_kwargs["aws_session_token"] = aws_session_token
    return boto3.Session(**session_kwargs).client("s3")


def _build_partitions_def(
    partition_type, partition_start, partition_values, dynamic_partition_name,
    cron_schedule=None, cron_fmt="%Y-%m-%d", end_offset=0, timezone=None,
):
    """Construct a Dagster partitions_def from the canonical flat partition fields.
    Same shape as ecs_pipes_asset/ssis_workspace/step_functions_asset's own
    `_build_partitions_def` -- inlined here per this registry's no-shared-
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


def _render_arguments(template: Optional[Dict[str, str]], context) -> Dict[str, str]:
    """Render a Glue `Arguments` dict (StartJobRun's real boto3 parameter name,
    confirmed via the installed botocore Glue service model), substituting
    {partition_key} in each value when the run is partitioned."""
    if not template:
        return {}
    partition_key = context.partition_key if getattr(context, "has_partition_key", False) else None
    rendered = {}
    for k, v in template.items():
        if isinstance(v, str) and "{partition_key}" in v and partition_key is not None:
            v = v.replace("{partition_key}", partition_key)
        rendered[k] = v
    return rendered


def _should_include(
    name: str,
    tags: Optional[Dict[str, str]],
    job_name_prefix: Optional[str],
    exclude_jobs: Optional[List[str]],
    filter_by_name_pattern: Optional[str],
    exclude_name_pattern: Optional[str],
    filter_by_tags: Optional[str],
) -> bool:
    """Apply all configured filters to decide whether to include an entity."""
    if job_name_prefix and not name.startswith(job_name_prefix):
        return False
    if exclude_jobs and name in exclude_jobs:
        return False
    if exclude_name_pattern and re.search(exclude_name_pattern, name, re.IGNORECASE):
        return False
    if filter_by_name_pattern and not re.search(filter_by_name_pattern, name, re.IGNORECASE):
        return False
    if filter_by_tags and tags:
        filter_keys = [t.strip() for t in filter_by_tags.split(",")]
        entity_keys = list(tags.keys()) if isinstance(tags, dict) else []
        if not any(k in entity_keys for k in filter_keys):
            return False
    return True


def _extract_table_refs(job: Dict[str, Any], catalog_keys: set) -> List[str]:
    """Extract Data Catalog table asset keys referenced in job DefaultArguments."""
    refs = []
    default_args = job.get("DefaultArguments", {})
    db_arg = default_args.get("--database_name") or default_args.get("--database")
    if not db_arg:
        return refs
    for key, value in default_args.items():
        if "table" in key.lower() and key.startswith("--"):
            candidate = f"glue_table_{_sanitize(db_arg)}_{_sanitize(value)}"
            if candidate in catalog_keys:
                refs.append(candidate)
    return refs


# ── AssetSpec override helpers ─────────────────────────────────────────────────

def _automation_condition_from_string(expr: str):
    """Eval a short AutomationCondition expression, e.g. "eager()" or
    "on_cron('0 2 * * *')". Same config-driven eval convention already used by
    http_external_asset's own `automation_condition` field in this registry."""
    from dagster import AutomationCondition
    return eval(expr, {"__builtins__": {}}, {"AutomationCondition": AutomationCondition})  # noqa: S307


def _freshness_policy_from_dict(d: dict):
    """Build a dg.FreshnessPolicy from a small override dict:
      {type: time_window, fail_window_minutes: N, warn_window_minutes: N}
      {type: cron, deadline_cron: "...", lower_bound_delta_minutes: N, timezone: "..."}
    """
    from datetime import timedelta
    import dagster as _dg
    ptype = d.get("type", "time_window")
    if ptype == "cron":
        return _dg.FreshnessPolicy.cron(
            deadline_cron=d["deadline_cron"],
            lower_bound_delta=timedelta(minutes=d.get("lower_bound_delta_minutes", 60)),
            timezone=d.get("timezone", "UTC"),
        )
    return _dg.FreshnessPolicy.time_window(
        fail_window=timedelta(minutes=d["fail_window_minutes"]),
        warn_window=timedelta(minutes=d["warn_window_minutes"]) if d.get("warn_window_minutes") else None,
    )


def _merge_spec(base: dg.AssetSpec, ov: dict) -> dg.AssetSpec:
    """Merge an override dict into a base AssetSpec."""
    extra_deps = [dg.AssetKey.from_user_string(d) for d in ov.get("deps", [])]
    kwargs: Dict[str, Any] = dict(
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
    if "automation_condition" in ov:
        kwargs["automation_condition"] = _automation_condition_from_string(ov["automation_condition"])
    elif base.automation_condition is not None:
        kwargs["automation_condition"] = base.automation_condition
    if "freshness_policy" in ov:
        kwargs["freshness_policy"] = _freshness_policy_from_dict(ov["freshness_policy"])
    elif base.freshness_policy is not None:
        kwargs["freshness_policy"] = base.freshness_policy
    return dg.AssetSpec(**kwargs)


def _apply_job_overrides(
    default_spec: dg.AssetSpec,
    job_name: str,
    overrides: Optional[dict],
) -> list:
    """Apply assets_by_job_name overrides. Returns list (usually 1, but >1 if one job → multiple assets)."""
    if not overrides or job_name not in overrides:
        return [default_spec]
    ov = overrides[job_name]
    if isinstance(ov, list):
        return [_merge_spec(default_spec, o) for o in ov]
    return [_merge_spec(default_spec, ov)]


# ── Core defs builder (no network calls) ──────────────────────────────────────

def _make_glue_job_asset(spec, job_name, creds, poll_interval_seconds, arguments_template, use_pipes, pipes_bucket):
    """Factory function, not a default-argument trick: Dagster's asset machinery
    treats any non-`context` parameter on an `@dg.asset` function as a real
    input to resolve against upstream deps, even with a default value (same gap
    documented -- and fixed -- in ssis_workspace/step_functions_asset). One
    asset per job (not a shared @multi_asset over every job) so re-executing a
    single failed job doesn't rerun every selected job."""
    import dagster as _dg

    @_dg.asset(
        key=spec.key, group_name=spec.group_name, description=spec.description,
        metadata=dict(spec.metadata or {}), tags=dict(spec.tags or {}),
        kinds=spec.kinds or {"aws", "glue"}, deps=list(spec.deps or []),
        owners=list(spec.owners or []), partitions_def=spec.partitions_def,
        automation_condition=spec.automation_condition, freshness_policy=spec.freshness_policy,
    )
    def _glue_job_asset(context: _dg.AssetExecutionContext) -> _dg.MaterializeResult:
        client = _make_glue_client(**creds)
        rendered_args = _render_arguments(arguments_template, context)

        if use_pipes:
            from dagster_aws.pipes import PipesGlueClient, PipesS3ContextInjector, PipesS3MessageReader
            s3_client = _make_s3_client(**creds)
            pipes_client = PipesGlueClient(
                client=client,
                context_injector=PipesS3ContextInjector(bucket=pipes_bucket, client=s3_client),
                message_reader=PipesS3MessageReader(bucket=pipes_bucket, client=s3_client),
            )
            invocation = pipes_client.run(
                context=context,
                start_job_run_params={"JobName": job_name, "Arguments": rendered_args},
            )
            return invocation.get_materialize_result()

        response = client.start_job_run(JobName=job_name, Arguments=rendered_args)
        run_id: str = response["JobRunId"]
        context.log.info(f"Started Glue job '{job_name}', run ID: {run_id}")

        terminal = {"SUCCEEDED", "FAILED", "STOPPED", "TIMEOUT", "ERROR"}
        while True:
            time.sleep(poll_interval_seconds)
            run_info = client.get_job_run(JobName=job_name, RunId=run_id)
            job_run = run_info["JobRun"]
            state_val: str = job_run.get("JobRunState", "")
            context.log.info(f"Glue job '{job_name}' run {run_id} state: {state_val}")
            if state_val in terminal:
                break

        execution_time = job_run.get("ExecutionTime")
        if state_val != "SUCCEEDED":
            raise Exception(f"Glue job '{job_name}' run {run_id} ended with state: {state_val}")

        return _dg.MaterializeResult(
            metadata={
                "run_id": _dg.MetadataValue.text(run_id),
                "job_run_state": _dg.MetadataValue.text(state_val),
                "execution_time_seconds": _dg.MetadataValue.int(int(execution_time or 0)),
                "started_on": _dg.MetadataValue.text(str(job_run.get("StartedOn")) if job_run.get("StartedOn") else ""),
                "completed_on": _dg.MetadataValue.text(str(job_run.get("CompletedOn")) if job_run.get("CompletedOn") else ""),
                "arguments": _dg.MetadataValue.json(rendered_args) if rendered_args else _dg.MetadataValue.text(""),
            }
        )

    return _glue_job_asset


def _make_glue_job_multi_asset(specs, job_name, safe_name, creds, poll_interval_seconds, arguments_template, use_pipes, pipes_bucket):
    """Same per-job (not all-jobs-shared) executable as `_make_glue_job_asset`,
    for the case where one Glue job expands to MULTIPLE Dagster assets via
    `assets_by_job_name`."""
    import dagster as _dg

    @_dg.multi_asset(specs=specs, name=f"glue_job_{safe_name}_multi")
    def _glue_job_multi_asset(context: _dg.AssetExecutionContext):
        client = _make_glue_client(**creds)
        rendered_args = _render_arguments(arguments_template, context)

        if use_pipes:
            from dagster_aws.pipes import PipesGlueClient, PipesS3ContextInjector, PipesS3MessageReader
            s3_client = _make_s3_client(**creds)
            pipes_client = PipesGlueClient(
                client=client,
                context_injector=PipesS3ContextInjector(bucket=pipes_bucket, client=s3_client),
                message_reader=PipesS3MessageReader(bucket=pipes_bucket, client=s3_client),
            )
            invocation = pipes_client.run(
                context=context,
                start_job_run_params={"JobName": job_name, "Arguments": rendered_args},
            )
            result = invocation.get_materialize_result()
            for spec in specs:
                yield _dg.MaterializeResult(asset_key=spec.key, metadata=dict(result.metadata or {}))
            return

        response = client.start_job_run(JobName=job_name, Arguments=rendered_args)
        run_id: str = response["JobRunId"]
        context.log.info(f"Started Glue job '{job_name}', run ID: {run_id}")

        terminal = {"SUCCEEDED", "FAILED", "STOPPED", "TIMEOUT", "ERROR"}
        while True:
            time.sleep(poll_interval_seconds)
            run_info = client.get_job_run(JobName=job_name, RunId=run_id)
            job_run = run_info["JobRun"]
            state_val: str = job_run.get("JobRunState", "")
            context.log.info(f"Glue job '{job_name}' run {run_id} state: {state_val}")
            if state_val in terminal:
                break

        execution_time = job_run.get("ExecutionTime")
        if state_val != "SUCCEEDED":
            raise Exception(f"Glue job '{job_name}' run {run_id} ended with state: {state_val}")

        for spec in specs:
            yield _dg.MaterializeResult(
                asset_key=spec.key,
                metadata={
                    "run_id": _dg.MetadataValue.text(run_id),
                    "job_run_state": _dg.MetadataValue.text(state_val),
                    "execution_time_seconds": _dg.MetadataValue.int(int(execution_time or 0)),
                    "started_on": _dg.MetadataValue.text(str(job_run.get("StartedOn")) if job_run.get("StartedOn") else ""),
                    "completed_on": _dg.MetadataValue.text(str(job_run.get("CompletedOn")) if job_run.get("CompletedOn") else ""),
                },
            )

    return _glue_job_multi_asset


def _build_glue_defs(
    state: Dict[str, Any],
    region_name: str,
    aws_access_key_id: Optional[str],
    aws_secret_access_key: Optional[str],
    aws_session_token: Optional[str],
    group_name: str,
    key_prefix: Optional[str],
    poll_interval_seconds: int,
    generate_sensor: bool,
    import_catalog_tables: bool,
    catalog_database_filter: Optional[str],
    filter_by_name_pattern: Optional[str],
    exclude_name_pattern: Optional[str],
    filter_by_tags: Optional[str],
    assets_by_job_name: Optional[dict] = None,
    retry_policy_max_retries: Optional[int] = None,
    retry_policy_delay_seconds: Optional[int] = None,
    retry_policy_backoff: str = "exponential",
    partitions_def=None,
    arguments: Optional[Dict[str, str]] = None,
    use_pipes: bool = False,
    pipes_bucket: Optional[str] = None,
) -> dg.Definitions:
    """Build Definitions from cached state dict with zero network calls."""

    assets_list: list = []
    sensors_list: list = []

    # Credentials bundle passed into asset closures
    creds = dict(
        region_name=region_name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        aws_session_token=aws_session_token,
    )

    # ── Data Catalog Tables (observable) ──────────────────────────────────────
    catalog_table_keys: set = set()
    if import_catalog_tables:
        for t in state.get("catalog_tables", []):
            database = t["database"]
            table_name = t["table"]
            asset_key = f"glue_table_{_sanitize(database)}_{_sanitize(table_name)}"
            catalog_table_keys.add(asset_key)

            prefixed_key = ([key_prefix] + [asset_key]) if key_prefix else [asset_key]

            @dg.observable_source_asset(
                name=asset_key,
                group_name=group_name,
                description=f"Glue Data Catalog table: {database}.{table_name}",
                metadata={
                    "glue_database": database,
                    "glue_table": table_name,
                    "glue_location": dg.MetadataValue.text(t.get("location") or ""),
                    "entity_type": "catalog_table",
                },
            )
            def _catalog_asset(
                context: dg.AssetExecutionContext,
                _db=database,
                _tbl=table_name,
                _creds=creds,
            ):
                client = _make_glue_client(**_creds)
                try:
                    table_data = client.get_table(DatabaseName=_db, Name=_tbl)["Table"]
                    return {
                        "database": _db,
                        "table": _tbl,
                        "location": table_data.get("StorageDescriptor", {}).get("Location"),
                        "update_time": str(table_data.get("UpdateTime")) if table_data.get("UpdateTime") else None,
                        "row_count": table_data.get("Parameters", {}).get("numRows", "N/A"),
                    }
                except Exception as exc:
                    context.log.warning(f"Could not get table info for {_db}.{_tbl}: {exc}")
                    return {"database": _db, "table": _tbl}

            assets_list.append(_catalog_asset)

    # ── Glue Jobs (materializable) ─────────────────────────────────────────────
    # job_metadata is keyed by the FINAL (post-override) AssetKey, not the raw
    # glue_job_<name> key -- previously the observation sensor below rebuilt
    # observations under the pre-override key, so renaming an asset via
    # assets_by_job_name (its documented purpose) made the sensor report
    # materializations for an asset key nothing in the graph has. Same bug
    # class already fixed in snowflake_workspace/azure_data_factory/
    # ssis_workspace -- see component-feedback/2026-11-23-aws-batch-pipes.md.
    job_metadata: Dict[dg.AssetKey, Dict[str, Any]] = {}

    for job in state.get("jobs", []):
        job_name: str = job["name"]
        safe_name = _sanitize(job_name)
        asset_key_str = f"glue_job_{safe_name}"
        if key_prefix:
            asset_key_str = f"{_sanitize(key_prefix)}_{asset_key_str}"

        table_deps = _extract_table_refs(
            {"DefaultArguments": job.get("default_arguments", {})},
            catalog_table_keys,
        )

        default_spec = dg.AssetSpec(
            key=dg.AssetKey([asset_key_str]),
            group_name=group_name,
            description=job.get("description") or f"AWS Glue job: {job_name}",
            metadata={
                "glue_job_name": dg.MetadataValue.text(job_name),
                "glue_command": dg.MetadataValue.text(job.get("command_name") or ""),
                "glue_role": dg.MetadataValue.text(job.get("role") or ""),
                "glue_max_capacity": dg.MetadataValue.float(float(job.get("max_capacity") or 0)),
                "glue_timeout": dg.MetadataValue.int(int(job.get("timeout") or 0)),
                "entity_type": "glue_job",
                "upstream_tables": dg.MetadataValue.int(len(table_deps)),
            },
            tags=job.get("tags") or {},
            deps=[dg.AssetKey([d]) for d in table_deps] if table_deps else [],
            partitions_def=partitions_def,
        )

        expanded = _apply_job_overrides(default_spec, job_name, assets_by_job_name)
        job_override = (assets_by_job_name or {}).get(job_name) or {}
        job_arguments = dict(arguments or {})
        if isinstance(job_override, dict) and job_override.get("arguments"):
            job_arguments.update(job_override["arguments"])

        for spec in expanded:
            job_metadata[spec.key] = {"job_name": job_name}

        # Per-job (not one shared @multi_asset over every job): re-executing
        # ONE failed job must not rerun every selected job.
        if len(expanded) == 1:
            assets_list.append(_make_glue_job_asset(
                spec=expanded[0], job_name=job_name, creds=creds,
                poll_interval_seconds=poll_interval_seconds, arguments_template=job_arguments,
                use_pipes=use_pipes, pipes_bucket=pipes_bucket,
            ))
        else:
            assets_list.append(_make_glue_job_multi_asset(
                specs=expanded, job_name=job_name, safe_name=safe_name, creds=creds,
                poll_interval_seconds=poll_interval_seconds, arguments_template=job_arguments,
                use_pipes=use_pipes, pipes_bucket=pipes_bucket,
            ))

    # ── Glue Crawlers (materializable) ─────────────────────────────────────────
    crawler_metadata: Dict[str, Dict[str, Any]] = {}

    for crawler in state.get("crawlers", []):
        crawler_name: str = crawler["name"]
        asset_key = f"glue_crawler_{_sanitize(crawler_name)}"
        if key_prefix:
            asset_key = f"{_sanitize(key_prefix)}_{asset_key}"

        crawler_metadata[asset_key] = {"crawler_name": crawler_name}

        # Build retry policy (auto-generated; opt-in via retry_policy_max_retries).

        _retry_policy = None

        if retry_policy_max_retries is not None:

            from dagster import Backoff, RetryPolicy

            _retry_policy = RetryPolicy(

                max_retries=retry_policy_max_retries,

                delay=retry_policy_delay_seconds or 1,

                backoff=Backoff[retry_policy_backoff.upper()],

            )


        @dg.asset(retry_policy=_retry_policy, 
            key=dg.AssetKey.from_user_string(asset_key),
            group_name=group_name,
            description=crawler.get("description") or f"AWS Glue crawler: {crawler_name}",
            metadata={
                "glue_crawler_name": dg.MetadataValue.text(crawler_name),
                "glue_role": dg.MetadataValue.text(crawler.get("role") or ""),
                "glue_database": dg.MetadataValue.text(crawler.get("database_name") or ""),
                "entity_type": "glue_crawler",
            },
        )
        def _crawler_asset(
            context: dg.AssetExecutionContext,
            _crawler_name=crawler_name,
            _creds=creds,
        ):
            client = _make_glue_client(**_creds)
            client.start_crawler(Name=_crawler_name)
            context.log.info(f"Started Glue crawler: {_crawler_name}")

            crawler_data = client.get_crawler(Name=_crawler_name)["Crawler"]
            return dg.MaterializeResult(
                metadata={
                    "crawler_name": dg.MetadataValue.text(_crawler_name),
                    "state": dg.MetadataValue.text(crawler_data.get("State") or ""),
                    "last_crawl": dg.MetadataValue.text(
                        str(crawler_data.get("LastCrawl", {}).get("StartTime"))
                        if crawler_data.get("LastCrawl")
                        else ""
                    ),
                }
            )

        assets_list.append(_crawler_asset)

    # ── Glue Workflows (materializable) ────────────────────────────────────────
    workflow_metadata: Dict[str, Dict[str, Any]] = {}

    for workflow in state.get("workflows", []):
        workflow_name: str = workflow["name"]
        asset_key = f"glue_workflow_{_sanitize(workflow_name)}"
        if key_prefix:
            asset_key = f"{_sanitize(key_prefix)}_{asset_key}"

        workflow_metadata[asset_key] = {"workflow_name": workflow_name}

        @dg.asset(
            key=dg.AssetKey.from_user_string(asset_key),
            group_name=group_name,
            description=workflow.get("description") or f"AWS Glue workflow: {workflow_name}",
            metadata={
                "glue_workflow_name": dg.MetadataValue.text(workflow_name),
                "entity_type": "glue_workflow",
            },
        )
        def _workflow_asset(
            context: dg.AssetExecutionContext,
            _workflow_name=workflow_name,
            _creds=creds,
        ):
            client = _make_glue_client(**_creds)
            response = client.start_workflow_run(Name=_workflow_name)
            run_id: str = response["RunId"]
            context.log.info(f"Started Glue workflow '{_workflow_name}', run ID: {run_id}")

            run_info = client.get_workflow_run(Name=_workflow_name, RunId=run_id)
            workflow_run = run_info["Run"]

            return dg.MaterializeResult(
                metadata={
                    "run_id": dg.MetadataValue.text(run_id),
                    "status": dg.MetadataValue.text(workflow_run.get("Status") or ""),
                    "started_on": dg.MetadataValue.text(
                        str(workflow_run.get("StartedOn")) if workflow_run.get("StartedOn") else ""
                    ),
                }
            )

        assets_list.append(_workflow_asset)

    # ── Glue DataBrew Jobs (materializable) ────────────────────────────────────
    for databrew_job in state.get("databrew_jobs", []):
        databrew_job_name: str = databrew_job["name"]
        asset_key = f"databrew_job_{_sanitize(databrew_job_name)}"
        if key_prefix:
            asset_key = f"{_sanitize(key_prefix)}_{asset_key}"

        @dg.asset(
            key=dg.AssetKey.from_user_string(asset_key),
            group_name=group_name,
            description=databrew_job.get("description") or f"AWS Glue DataBrew job: {databrew_job_name}",
            metadata={
                "databrew_job_name": dg.MetadataValue.text(databrew_job_name),
                "databrew_type": dg.MetadataValue.text(databrew_job.get("type") or ""),
                "databrew_recipe": dg.MetadataValue.text(databrew_job.get("recipe_name") or ""),
                "entity_type": "databrew_job",
            },
        )
        def _databrew_asset(
            context: dg.AssetExecutionContext,
            _job_name=databrew_job_name,
            _creds=creds,
        ):
            client = _make_databrew_client(**_creds)
            response = client.start_job_run(Name=_job_name)
            run_id: str = response["RunId"]
            context.log.info(f"Started DataBrew job '{_job_name}', run ID: {run_id}")
            return dg.MaterializeResult(
                metadata={
                    "run_id": dg.MetadataValue.text(run_id),
                    "job_name": dg.MetadataValue.text(_job_name),
                }
            )

        assets_list.append(_databrew_asset)

    # ── Glue Data Quality Rulesets (observable) ────────────────────────────────
    for ruleset in state.get("data_quality_rulesets", []):
        ruleset_name: str = ruleset["name"]
        asset_key = f"data_quality_{_sanitize(ruleset_name)}"
        if key_prefix:
            asset_key = f"{_sanitize(key_prefix)}_{asset_key}"

        @dg.observable_source_asset(
            name=asset_key,
            group_name=group_name,
            description=f"AWS Glue Data Quality ruleset: {ruleset_name}",
            metadata={
                "ruleset_name": dg.MetadataValue.text(ruleset_name),
                "target_table": dg.MetadataValue.text(str(ruleset.get("target_table") or "")),
                "entity_type": "data_quality_ruleset",
            },
        )
        def _dq_asset(
            context: dg.AssetExecutionContext,
            _ruleset_name=ruleset_name,
            _creds=creds,
        ):
            client = _make_glue_client(**_creds)
            try:
                client.get_data_quality_ruleset(Name=_ruleset_name)
                context.log.info(f"Data quality ruleset '{_ruleset_name}' found")
            except ClientError as exc:
                context.log.warning(f"Could not get data quality ruleset '{_ruleset_name}': {exc}")

        assets_list.append(_dq_asset)

    # ── Observation Sensor ─────────────────────────────────────────────────────
    if generate_sensor and (job_metadata or crawler_metadata or workflow_metadata):
        sensor_name = f"{group_name}_observation_sensor"

        @dg.sensor(
            name=sensor_name,
            minimum_interval_seconds=poll_interval_seconds,
        )
        def glue_observation_sensor(
            context: dg.SensorEvaluationContext,
            _job_meta=job_metadata,
            _crawler_meta=crawler_metadata,
            _workflow_meta=workflow_metadata,
            _creds=creds,
        ):
            """NOTE: Sensor evaluations must return SensorResult /
            RunRequest / SkipReason. Direct `yield AssetMaterialization`
            is silently dropped by Dagster — collect + return via
            SensorResult.
            """
            client = _make_glue_client(**_creds)
            asset_events: list[dg.AssetMaterialization] = []

            for asset_key, meta in _job_meta.items():
                job_name = meta["job_name"]
                try:
                    runs = client.get_job_runs(JobName=job_name, MaxResults=5).get("JobRuns", [])
                    for run in runs:
                        if run.get("JobRunState") == "SUCCEEDED":
                            asset_events.append(dg.AssetMaterialization(
                                asset_key=asset_key,
                                metadata={
                                    "run_id": run.get("Id"),
                                    "job_run_state": run.get("JobRunState"),
                                    "started_on": str(run.get("StartedOn")) if run.get("StartedOn") else None,
                                    "completed_on": str(run.get("CompletedOn")) if run.get("CompletedOn") else None,
                                    "execution_time": run.get("ExecutionTime"),
                                    "source": "glue_observation_sensor",
                                    "entity_type": "glue_job",
                                },
                            ))
                except Exception as exc:
                    context.log.error(f"Error checking runs for Glue job '{job_name}': {exc}")

            for asset_key, meta in _crawler_meta.items():
                crawler_name = meta["crawler_name"]
                try:
                    metrics = client.get_crawler_metrics(
                        CrawlerNameList=[crawler_name]
                    ).get("CrawlerMetricsList", [])
                    if metrics and metrics[0].get("LastRuntimeSeconds", 0) > 0:
                        m = metrics[0]
                        asset_events.append(dg.AssetMaterialization(
                            asset_key=asset_key,
                            metadata={
                                "crawler_name": crawler_name,
                                "tables_created": m.get("TablesCreated", 0),
                                "tables_updated": m.get("TablesUpdated", 0),
                                "tables_deleted": m.get("TablesDeleted", 0),
                                "last_runtime_seconds": m.get("LastRuntimeSeconds"),
                                "source": "glue_observation_sensor",
                                "entity_type": "glue_crawler",
                            },
                        ))
                except Exception as exc:
                    context.log.error(f"Error checking metrics for Glue crawler '{crawler_name}': {exc}")

            for asset_key, meta in _workflow_meta.items():
                workflow_name = meta["workflow_name"]
                try:
                    runs = client.get_workflow_runs(Name=workflow_name, MaxResults=5).get("Runs", [])
                    for run in runs:
                        if run.get("Status") == "COMPLETED":
                            asset_events.append(dg.AssetMaterialization(
                                asset_key=asset_key,
                                metadata={
                                    "run_id": run.get("WorkflowRunId"),
                                    "status": run.get("Status"),
                                    "started_on": str(run.get("StartedOn")) if run.get("StartedOn") else None,
                                    "completed_on": str(run.get("CompletedOn")) if run.get("CompletedOn") else None,
                                    "source": "glue_observation_sensor",
                                    "entity_type": "glue_workflow",
                                },
                            ))
                except Exception as exc:
                    context.log.error(f"Error checking runs for Glue workflow '{workflow_name}': {exc}")

            return dg.SensorResult(asset_events=asset_events)

        sensors_list.append(glue_observation_sensor)

    if not assets_list:
        return dg.Definitions()

    return dg.Definitions(
        assets=assets_list,
        sensors=sensors_list if sensors_list else None,
    )


# ── StateBackedComponent branch ────────────────────────────────────────────────

if _HAS_STATE_BACKED:
    @dataclass
    class AWSGlueComponent(StateBackedComponent, dg.Resolvable):
        """Component for importing AWS Glue entities as Dagster assets.

        Uses StateBackedComponent to cache discovery results from the AWS Glue API
        on disk, so code-server reloads make zero network calls. Populate the cache
        with:
          dagster dev                       (automatic in dev)
          dg utils refresh-defs-state       (CI/CD / image builds)

        Supports:
        - Glue Jobs (materializable, automatic lineage to Data Catalog tables)
        - Glue Crawlers (materializable)
        - Glue Workflows (materializable)
        - Glue DataBrew Jobs (materializable)
        - Glue Data Quality Rulesets (observable)
        - Data Catalog Tables (observable, for lineage)
        - Observation Sensor (tracks external runs)

        Example:
            ```yaml
            type: dagster_community_components.AWSGlueComponent
            attributes:
              region_name: us-east-1
              aws_access_key_id: "{{ env('AWS_ACCESS_KEY_ID') }}"
              aws_secret_access_key: "{{ env('AWS_SECRET_ACCESS_KEY') }}"
              import_jobs: true
              import_catalog_tables: true
              catalog_database_filter: production,analytics
            ```
        """

        # ── Required ──────────────────────────────────────────────────────────
        region_name: str

        # ── AWS credentials (optional; falls back to IAM role / env) ──────────
        aws_access_key_id: Optional[str] = None
        aws_secret_access_key: Optional[str] = None
        aws_session_token: Optional[str] = None

        # ── Discovery toggles ──────────────────────────────────────────────────
        import_jobs: bool = True
        import_crawlers: bool = False
        import_workflows: bool = False
        import_databrew_jobs: bool = False
        import_data_quality_rulesets: bool = False
        import_catalog_tables: bool = True

        # ── Filtering ─────────────────────────────────────────────────────────
        catalog_database_filter: Optional[str] = None
        job_name_prefix: Optional[str] = None
        exclude_jobs: Optional[List[str]] = None
        filter_by_name_pattern: Optional[str] = None
        exclude_name_pattern: Optional[str] = None
        filter_by_tags: Optional[str] = None

        # ── Asset presentation ────────────────────────────────────────────────
        group_name: str = "aws_glue"
        key_prefix: Optional[str] = None

        # ── Execution ─────────────────────────────────────────────────────────
        poll_interval_seconds: int = 30
        generate_sensor: bool = True

        # ── Per-job AssetSpec overrides ────────────────────────────────────────
        assets_by_job_name: Optional[dict] = None

        # ── Partitioning (same canonical shape as ecs_pipes_asset/ssis_workspace) ──
        partition_type: Optional[str] = None
        partition_start: Optional[str] = None
        partition_values: Optional[Any] = None
        dynamic_partition_name: Optional[str] = None
        partition_cron_schedule: Optional[str] = None
        partition_cron_fmt: str = "%Y-%m-%d"
        partition_end_offset: int = 0
        partition_timezone: Optional[str] = None

        # ── Job run arguments + Pipes ───────────────────────────────────────────
        arguments: Optional[Dict[str, str]] = None
        use_pipes: bool = False
        pipes_bucket: Optional[str] = None

        # ── State backing ─────────────────────────────────────────────────────
        retry_policy_max_retries: Optional[int] = None
        retry_policy_delay_seconds: Optional[int] = None
        retry_policy_backoff: str = "exponential"
        defs_state: ResolvedDefsStateConfig = field(
            default_factory=DefsStateConfigArgs.local_filesystem
        )

        # ── StateBackedComponent interface ────────────────────────────────────

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
                default_key=f"GlueComponent[{self.region_name}]",
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
            """Call AWS Glue APIs and write discovered entities to a JSON cache file."""
            glue = _make_glue_client(
                region_name=self.region_name,
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                aws_session_token=self.aws_session_token,
            )

            filter_kwargs = dict(
                job_name_prefix=self.job_name_prefix,
                exclude_jobs=self.exclude_jobs or [],
                filter_by_name_pattern=self.filter_by_name_pattern,
                exclude_name_pattern=self.exclude_name_pattern,
                filter_by_tags=self.filter_by_tags,
            )

            # ── Jobs ──────────────────────────────────────────────────────────
            jobs: List[Dict[str, Any]] = []
            if self.import_jobs:
                paginator = glue.get_paginator("get_jobs")
                for page in paginator.paginate():
                    for job in page.get("Jobs", []):
                        name: str = job["Name"]
                        tags = job.get("Tags", {})
                        if not _should_include(name, tags, **filter_kwargs):
                            continue
                        jobs.append({
                            "name": name,
                            "description": job.get("Description"),
                            "role": job.get("Role"),
                            "command_name": job.get("Command", {}).get("Name"),
                            "max_capacity": job.get("MaxCapacity"),
                            "timeout": job.get("Timeout"),
                            "tags": tags,
                            "default_arguments": job.get("DefaultArguments", {}),
                        })

            # ── Crawlers ──────────────────────────────────────────────────────
            crawlers: List[Dict[str, Any]] = []
            if self.import_crawlers:
                paginator = glue.get_paginator("get_crawlers")
                for page in paginator.paginate():
                    for crawler in page.get("Crawlers", []):
                        name = crawler["Name"]
                        tags = crawler.get("Tags", {})
                        if not _should_include(name, tags, **filter_kwargs):
                            continue
                        crawlers.append({
                            "name": name,
                            "description": crawler.get("Description"),
                            "role": crawler.get("Role"),
                            "database_name": crawler.get("DatabaseName"),
                            "tags": tags,
                        })

            # ── Workflows ─────────────────────────────────────────────────────
            workflows: List[Dict[str, Any]] = []
            if self.import_workflows:
                paginator = glue.get_paginator("list_workflows")
                for page in paginator.paginate():
                    workflow_names: List[str] = page.get("Workflows", [])
                    if not workflow_names:
                        continue
                    batch = glue.batch_get_workflows(Names=workflow_names)
                    for wf in batch.get("Workflows", []):
                        name = wf["Name"]
                        if not _should_include(name, None, **filter_kwargs):
                            continue
                        workflows.append({
                            "name": name,
                            "description": wf.get("Description"),
                            "created_on": str(wf.get("CreatedOn")) if wf.get("CreatedOn") else None,
                        })

            # ── DataBrew Jobs ─────────────────────────────────────────────────
            databrew_jobs: List[Dict[str, Any]] = []
            if self.import_databrew_jobs:
                try:
                    databrew = _make_databrew_client(
                        region_name=self.region_name,
                        aws_access_key_id=self.aws_access_key_id,
                        aws_secret_access_key=self.aws_secret_access_key,
                        aws_session_token=self.aws_session_token,
                    )
                    paginator = databrew.get_paginator("list_jobs")
                    for page in paginator.paginate():
                        for job in page.get("Jobs", []):
                            name = job["Name"]
                            tags = job.get("Tags", {})
                            if not _should_include(name, tags, **filter_kwargs):
                                continue
                            databrew_jobs.append({
                                "name": name,
                                "description": job.get("Description"),
                                "type": job.get("Type"),
                                "recipe_name": job.get("RecipeName"),
                                "tags": tags,
                            })
                except Exception:
                    pass  # DataBrew may not be available in all regions

            # ── Data Quality Rulesets ─────────────────────────────────────────
            data_quality_rulesets: List[Dict[str, Any]] = []
            if self.import_data_quality_rulesets:
                try:
                    paginator = glue.get_paginator("list_data_quality_rulesets")
                    for page in paginator.paginate():
                        for rs in page.get("Rulesets", []):
                            name = rs.get("Name")
                            if not name or not _should_include(name, None, **filter_kwargs):
                                continue
                            data_quality_rulesets.append({
                                "name": name,
                                "target_table": rs.get("TargetTable"),
                            })
                except Exception:
                    pass

            # ── Data Catalog Tables ───────────────────────────────────────────
            catalog_tables: List[Dict[str, Any]] = []
            if self.import_catalog_tables:
                try:
                    databases_to_scan: List[str] = []
                    if self.catalog_database_filter:
                        databases_to_scan = [
                            db.strip() for db in self.catalog_database_filter.split(",")
                        ]
                    else:
                        db_pager = glue.get_paginator("get_databases")
                        for page in db_pager.paginate():
                            for db in page.get("DatabaseList", []):
                                databases_to_scan.append(db["Name"])

                    for database_name in databases_to_scan:
                        try:
                            tbl_pager = glue.get_paginator("get_tables")
                            for page in tbl_pager.paginate(DatabaseName=database_name):
                                for table in page.get("TableList", []):
                                    tbl_name = table["Name"]
                                    fqn = f"{database_name}.{tbl_name}"
                                    if not _should_include(fqn, None, **filter_kwargs):
                                        continue
                                    catalog_tables.append({
                                        "database": database_name,
                                        "table": tbl_name,
                                        "location": table.get("StorageDescriptor", {}).get("Location"),
                                        "update_time": str(table.get("UpdateTime"))
                                        if table.get("UpdateTime")
                                        else None,
                                    })
                        except Exception:
                            continue
                except Exception:
                    pass

            state_path.write_text(
                json.dumps(
                    {
                        "jobs": jobs,
                        "crawlers": crawlers,
                        "workflows": workflows,
                        "databrew_jobs": databrew_jobs,
                        "data_quality_rulesets": data_quality_rulesets,
                        "catalog_tables": catalog_tables,
                    },
                    default=str,
                )
            )

        def build_defs_from_state(
            self,
            context: dg.ComponentLoadContext,
            state_path: Optional[Path],
        ) -> dg.Definitions:
            """Build asset definitions from the cached state file — no network calls."""
            if state_path is None or not state_path.exists():
                if hasattr(context, "log"):
                    context.log.warning(  # type: ignore[attr-defined]
                        "AWSGlueComponent: no cached state found. "
                        "Run `dg utils refresh-defs-state` or `dagster dev` to populate."
                    )
                return dg.Definitions()

            state: Dict[str, Any] = json.loads(state_path.read_text())
            return _build_glue_defs(
                state=state,
                region_name=self.region_name,
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                aws_session_token=self.aws_session_token,
                group_name=self.group_name,
                key_prefix=self.key_prefix,
                poll_interval_seconds=self.poll_interval_seconds,
                generate_sensor=self.generate_sensor,
                import_catalog_tables=self.import_catalog_tables,
                catalog_database_filter=self.catalog_database_filter,
                filter_by_name_pattern=self.filter_by_name_pattern,
                exclude_name_pattern=self.exclude_name_pattern,
                filter_by_tags=self.filter_by_tags,
                assets_by_job_name=self.assets_by_job_name,
                retry_policy_max_retries=self.retry_policy_max_retries,
                retry_policy_delay_seconds=self.retry_policy_delay_seconds,
                retry_policy_backoff=self.retry_policy_backoff,
                partitions_def=self._partitions_def,
                arguments=self.arguments,
                use_pipes=self.use_pipes,
                pipes_bucket=self.pipes_bucket,
            )

else:
    # ── Fallback: StateBackedComponent not available in this Dagster version ──
    # Falls back to calling AWS APIs on every build_defs call.
    class AWSGlueComponent(dg.Component, dg.Model, dg.Resolvable):  # type: ignore[no-redef]
        """AWS Glue component (fallback: no state caching).

        Upgrade to dagster>=1.8 to get StateBackedComponent caching.

        Example:
            ```yaml
            type: dagster_community_components.AWSGlueComponent
            attributes:
              region_name: us-east-1
              import_jobs: true
            ```
        """

        region_name: str = dg.Field(description="AWS region (e.g., us-east-1)")
        aws_access_key_id: Optional[str] = dg.Field(default=None)
        aws_secret_access_key: Optional[str] = dg.Field(default=None)
        aws_session_token: Optional[str] = dg.Field(default=None)

        import_jobs: bool = dg.Field(default=True)
        import_crawlers: bool = dg.Field(default=False)
        import_workflows: bool = dg.Field(default=False)
        import_databrew_jobs: bool = dg.Field(default=False)
        import_data_quality_rulesets: bool = dg.Field(default=False)
        import_catalog_tables: bool = dg.Field(default=True)

        catalog_database_filter: Optional[str] = dg.Field(default=None)
        job_name_prefix: Optional[str] = dg.Field(default=None)
        exclude_jobs: Optional[List[str]] = dg.Field(default=None)
        filter_by_name_pattern: Optional[str] = dg.Field(default=None)
        exclude_name_pattern: Optional[str] = dg.Field(default=None)
        filter_by_tags: Optional[str] = dg.Field(default=None)

        group_name: str = dg.Field(default="aws_glue")
        key_prefix: Optional[str] = dg.Field(default=None)
        poll_interval_seconds: int = dg.Field(default=30)
        generate_sensor: bool = dg.Field(default=True)
        assets_by_job_name: Optional[dict] = dg.Field(default=None)

        retry_policy_max_retries: Optional[int] = Field(

            default=None,

            description="Max retries on asset failure. Defines a RetryPolicy. Useful for transient network failures, rate limits, etc.",

        )

        retry_policy_delay_seconds: Optional[int] = Field(

            default=None,

            description="Seconds between retries (default 1).",

        )

        retry_policy_backoff: str = Field(

            default="exponential",

            description="Backoff strategy: 'linear' or 'exponential'.",

        )

        partition_type: Optional[str] = dg.Field(default=None)
        partition_start: Optional[str] = dg.Field(default=None)
        partition_values: Optional[Any] = dg.Field(default=None)
        dynamic_partition_name: Optional[str] = dg.Field(default=None)
        partition_cron_schedule: Optional[str] = dg.Field(default=None)
        partition_cron_fmt: str = dg.Field(default="%Y-%m-%d")
        partition_end_offset: int = dg.Field(default=0)
        partition_timezone: Optional[str] = dg.Field(default=None)

        arguments: Optional[Dict[str, str]] = Field(
            default=None,
            description="StartJobRun Arguments template ({--key: value}); values support {partition_key} substitution.",
        )
        use_pipes: bool = Field(
            default=False,
            description="Run jobs through dagster_aws.pipes.PipesGlueClient for Pipes-protocol metadata/logs. Requires pipes_bucket.",
        )
        pipes_bucket: Optional[str] = Field(
            default=None, description="S3 bucket for Pipes context injection/message reading. Required when use_pipes=true.",
        )

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
            return ComponentFormConfig(label='AWS Glue', editable=True)

        def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
            """Discover AWS Glue entities at load time and build Definitions."""
            glue = _make_glue_client(
                region_name=self.region_name,
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                aws_session_token=self.aws_session_token,
            )

            filter_kwargs = dict(
                job_name_prefix=self.job_name_prefix,
                exclude_jobs=self.exclude_jobs or [],
                filter_by_name_pattern=self.filter_by_name_pattern,
                exclude_name_pattern=self.exclude_name_pattern,
                filter_by_tags=self.filter_by_tags,
            )

            jobs: List[Dict[str, Any]] = []
            if self.import_jobs:
                for page in glue.get_paginator("get_jobs").paginate():
                    for job in page.get("Jobs", []):
                        name = job["Name"]
                        if not _should_include(name, job.get("Tags", {}), **filter_kwargs):
                            continue
                        jobs.append({
                            "name": name,
                            "description": job.get("Description"),
                            "role": job.get("Role"),
                            "command_name": job.get("Command", {}).get("Name"),
                            "max_capacity": job.get("MaxCapacity"),
                            "timeout": job.get("Timeout"),
                            "tags": job.get("Tags", {}),
                            "default_arguments": job.get("DefaultArguments", {}),
                        })

            crawlers: List[Dict[str, Any]] = []
            if self.import_crawlers:
                for page in glue.get_paginator("get_crawlers").paginate():
                    for crawler in page.get("Crawlers", []):
                        name = crawler["Name"]
                        if not _should_include(name, crawler.get("Tags", {}), **filter_kwargs):
                            continue
                        crawlers.append({
                            "name": name,
                            "description": crawler.get("Description"),
                            "role": crawler.get("Role"),
                            "database_name": crawler.get("DatabaseName"),
                            "tags": crawler.get("Tags", {}),
                        })

            workflows: List[Dict[str, Any]] = []
            if self.import_workflows:
                for page in glue.get_paginator("list_workflows").paginate():
                    names = page.get("Workflows", [])
                    if names:
                        for wf in glue.batch_get_workflows(Names=names).get("Workflows", []):
                            name = wf["Name"]
                            if not _should_include(name, None, **filter_kwargs):
                                continue
                            workflows.append({
                                "name": name,
                                "description": wf.get("Description"),
                                "created_on": str(wf.get("CreatedOn")) if wf.get("CreatedOn") else None,
                            })

            databrew_jobs: List[Dict[str, Any]] = []
            if self.import_databrew_jobs:
                try:
                    databrew = _make_databrew_client(
                        region_name=self.region_name,
                        aws_access_key_id=self.aws_access_key_id,
                        aws_secret_access_key=self.aws_secret_access_key,
                        aws_session_token=self.aws_session_token,
                    )
                    for page in databrew.get_paginator("list_jobs").paginate():
                        for job in page.get("Jobs", []):
                            name = job["Name"]
                            if not _should_include(name, job.get("Tags", {}), **filter_kwargs):
                                continue
                            databrew_jobs.append({
                                "name": name,
                                "description": job.get("Description"),
                                "type": job.get("Type"),
                                "recipe_name": job.get("RecipeName"),
                                "tags": job.get("Tags", {}),
                            })
                except Exception:
                    pass

            data_quality_rulesets: List[Dict[str, Any]] = []
            if self.import_data_quality_rulesets:
                try:
                    for page in glue.get_paginator("list_data_quality_rulesets").paginate():
                        for rs in page.get("Rulesets", []):
                            name = rs.get("Name")
                            if name and _should_include(name, None, **filter_kwargs):
                                data_quality_rulesets.append({
                                    "name": name,
                                    "target_table": rs.get("TargetTable"),
                                })
                except Exception:
                    pass

            catalog_tables: List[Dict[str, Any]] = []
            if self.import_catalog_tables:
                try:
                    dbs: List[str] = []
                    if self.catalog_database_filter:
                        dbs = [d.strip() for d in self.catalog_database_filter.split(",")]
                    else:
                        for page in glue.get_paginator("get_databases").paginate():
                            for db in page.get("DatabaseList", []):
                                dbs.append(db["Name"])
                    for db_name in dbs:
                        try:
                            for page in glue.get_paginator("get_tables").paginate(DatabaseName=db_name):
                                for tbl in page.get("TableList", []):
                                    tbl_name = tbl["Name"]
                                    if not _should_include(f"{db_name}.{tbl_name}", None, **filter_kwargs):
                                        continue
                                    catalog_tables.append({
                                        "database": db_name,
                                        "table": tbl_name,
                                        "location": tbl.get("StorageDescriptor", {}).get("Location"),
                                    })
                        except Exception:
                            continue
                except Exception:
                    pass

            state = {
                "jobs": jobs,
                "crawlers": crawlers,
                "workflows": workflows,
                "databrew_jobs": databrew_jobs,
                "data_quality_rulesets": data_quality_rulesets,
                "catalog_tables": catalog_tables,
            }

            return _build_glue_defs(
                state=state,
                region_name=self.region_name,
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                aws_session_token=self.aws_session_token,
                group_name=self.group_name,
                key_prefix=self.key_prefix,
                poll_interval_seconds=self.poll_interval_seconds,
                generate_sensor=self.generate_sensor,
                import_catalog_tables=self.import_catalog_tables,
                catalog_database_filter=self.catalog_database_filter,
                filter_by_name_pattern=self.filter_by_name_pattern,
                exclude_name_pattern=self.exclude_name_pattern,
                filter_by_tags=self.filter_by_tags,
                assets_by_job_name=self.assets_by_job_name,
                retry_policy_max_retries=self.retry_policy_max_retries,
                retry_policy_delay_seconds=self.retry_policy_delay_seconds,
                retry_policy_backoff=self.retry_policy_backoff,
                partitions_def=self._partitions_def,
                arguments=self.arguments,
                use_pipes=self.use_pipes,
                pipes_bucket=self.pipes_bucket,
            )
