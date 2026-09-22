"""Prefect Background Task — Dagster asset that submits a Prefect background task.

Materializing this asset imports a `@task`-decorated function and submits it
via `.delay()` — Prefect's background-task mechanism — then, by default,
waits for it to reach a terminal state. Requires a task worker serving that
task (`prefect task serve`, or a work-pool-backed serve process); without one
the task run stays scheduled and this blocks until the Dagster run is
terminated (same caveat the official dagster-prefect docs give for Pipes).

Unlike prefect_flow_run's deployment_name (a string reference, no import
needed — the deployment lives entirely in Prefect), a background task has no
by-name trigger API: `.delay()` needs the actual Python function object, so
`task_import_path` must be importable from wherever this component builds
its defs. This mirrors a real constraint in Prefect itself, not something we
chose — the official dagster-prefect PipesPrefectTaskClient has the exact
same requirement (its docstring example passes the task object directly).

execution_mode:
  - "poll" (default) — plain Prefect SDK (`.delay()` + `get_client`). Zero
    Pipes dependency. stream_logs / stream_artifacts / check_names all work
    the same way they do on prefect_flow_run — see that component's
    module docstring for why (Prefect's own read_logs/read_artifacts APIs,
    no shared filesystem or blob store needed).
  - "pipes" — delegates to dagster-prefect's PipesPrefectTaskClient. Same
    task_import_path import requirement either way.

forward_termination is NOT a field here (unlike prefect_flow_run): Prefect's
task workers run a task to completion regardless of a cancellation request —
confirmed in dagster-prefect's own source (BasePipesPrefectClient logs a
warning and gives up for task-run kind rather than attempting a cancel).
This component does the same: it warns instead of pretending to support it.

Docs: https://docs.prefect.io/latest/develop/task-runners/
"""
import json
import os
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


def _parse_table_artifact_data(data: Any) -> Any:
    """Prefect table artifacts store `data` as a JSON-encoded STRING (verified
    against a live server — NOT already a parsed list/dict, unlike e.g.
    progress artifacts, which come back as a plain int/float) — see
    prefect_flow_run.component for the identical, independently-tested version."""
    if isinstance(data, str):
        try:
            return json.loads(data)
        except (TypeError, ValueError):
            return data
    return data


def _first_table_row(parsed: Any) -> Dict[str, Any]:
    """Unwrap a Prefect table artifact's first row — see prefect_flow_run.component
    for the identical, independently-tested version."""
    if isinstance(parsed, list) and parsed and isinstance(parsed[0], dict):
        return parsed[0]
    if isinstance(parsed, dict):
        return parsed
    return {}


def _build_partitions_def(
    partition_type, partition_start, partition_values,
    dynamic_partition_name, partition_dimensions,
):
    """Canonical partition builder shared across the registry."""
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )
    if partition_dimensions and partition_type:
        raise ValueError("Set partition_type OR partition_dimensions, not both.")

    def _axis(spec):
        t = spec.get("type")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily": return DailyPartitionsDefinition(start_date=spec["start"])
        if t == "weekly": return WeeklyPartitionsDefinition(start_date=spec["start"])
        if t == "monthly": return MonthlyPartitionsDefinition(start_date=spec["start"])
        if t == "hourly": return HourlyPartitionsDefinition(start_date=spec["start"])
        if t == "static":
            vals = spec.get("values") or []
            if isinstance(vals, str):
                vals = [v.strip() for v in vals.split(",") if v.strip()]
            return StaticPartitionsDefinition(list(vals))
        if t == "dynamic":
            n = spec.get("dynamic_partition_name") or spec.get("name")
            return DynamicPartitionsDefinition(name=n)
        raise ValueError(f"unknown partition type: {t!r}")

    if partition_dimensions:
        if len(partition_dimensions) == 1:
            return _axis(partition_dimensions[0])
        return MultiPartitionsDefinition({d["name"]: _axis(d) for d in partition_dimensions})

    if not partition_type:
        return None
    _vals = (
        [v.strip() for v in partition_values.split(",") if v.strip()]
        if isinstance(partition_values, str) else list(partition_values or [])
    )
    if partition_type in ("daily", "weekly", "monthly", "hourly") and not partition_start:
        raise ValueError(f"partition_type={partition_type!r} requires partition_start (ISO date).")
    if partition_type == "daily": return DailyPartitionsDefinition(start_date=partition_start)
    if partition_type == "weekly": return WeeklyPartitionsDefinition(start_date=partition_start)
    if partition_type == "monthly": return MonthlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "hourly": return HourlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "static":
        if not _vals: raise ValueError("partition_type='static' requires partition_values.")
        return StaticPartitionsDefinition(_vals)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    raise ValueError(f"unknown partition_type: {partition_type!r}")


def _prefect_task_run_url(api_url: str, ui_url: Optional[str], task_run_id: Any) -> str:
    """Build a link into the Prefect UI for a task run (path differs from a flow run's)."""
    base = (ui_url or api_url).rstrip("/").removesuffix("/api")
    return f"{base}/runs/task-run/{task_run_id}"


def _import_task(task_import_path: str):
    """Resolve 'module.path:function_name' to the actual @task-decorated object.

    Background tasks have no by-name trigger API the way deployments do —
    `.delay()` needs the real Python object, so the module must be importable
    from wherever this runs (installed dependency, or vendored onto the path).
    """
    module_path, sep, fn_name = task_import_path.partition(":")
    if not sep or not fn_name:
        raise ValueError(
            f"task_import_path must be 'module.path:function_name', got {task_import_path!r}"
        )
    import importlib
    module = importlib.import_module(module_path)
    fn = getattr(module, fn_name, None)
    if fn is None:
        raise ValueError(f"{fn_name!r} not found in module {module_path!r}")
    return fn


def _forward_prefect_log(dagster_log: Any, entry: Any) -> None:
    """Re-emit one Prefect `Log` record as a Dagster run-log line — see
    prefect_flow_run.component for the identical, independently-tested version."""
    level = entry.level or 20
    prefix = f"[prefect:{entry.name}]" if getattr(entry, "name", None) else "[prefect]"
    message = f"{prefix} {entry.message}"
    if level >= 50:
        dagster_log.critical(message)
    elif level >= 40:
        dagster_log.error(message)
    elif level >= 30:
        dagster_log.warning(message)
    elif level >= 20:
        dagster_log.info(message)
    else:
        dagster_log.debug(message)


def _artifact_metadata_value(artifact: Any) -> Any:
    """Map a Prefect Artifact's native `type` onto the matching Dagster MetadataValue —
    see prefect_flow_run.component for the identical, independently-tested version."""
    data = artifact.data
    if artifact.type == "markdown":
        return dg.MetadataValue.md(str(data))
    if artifact.type == "table":
        return dg.MetadataValue.json(_parse_table_artifact_data(data))
    if artifact.type == "progress":
        try:
            return dg.MetadataValue.float(float(data))
        except (TypeError, ValueError):
            return dg.MetadataValue.text(str(data))
    if artifact.type == "image":
        return dg.MetadataValue.url(str(data))
    return dg.MetadataValue.text(str(data))


def _forward_prefect_artifact(
    context: Any, asset_key: Any, artifact: Any, check_names: set,
) -> Optional[Any]:
    """Forward one Prefect Artifact. Returns an AssetCheckResult if the
    artifact's key (dashes read as underscores — see check_names field docs
    for why that translation is required) matches a declared check_names
    entry; the CALLER must yield it (Dagster requires check results to be
    yielded from the asset function's own body when check_specs is declared
    — log_event alone doesn't satisfy that, verified directly). Plain
    artifacts are logged immediately as an AssetObservation and this returns
    None — see prefect_flow_run.component for the identical, independently-
    tested version and its fuller docstring."""
    translated_key = artifact.key.replace("-", "_") if artifact.key else None
    if translated_key and translated_key in check_names and artifact.type == "table":
        row = _first_table_row(_parse_table_artifact_data(artifact.data))
        passed = bool(row.get("passed"))
        extra_metadata = {k: v for k, v in row.items() if k != "passed"}
        return dg.AssetCheckResult(
            check_name=translated_key,
            passed=passed,
            description=artifact.description,
            metadata=extra_metadata or None,
        )

    key = artifact.key or f"prefect_artifact_{artifact.id}"
    context.log_event(dg.AssetObservation(
        asset_key=asset_key,
        metadata={key: _artifact_metadata_value(artifact)},
    ))
    return None


def _poll_task_run_until_terminal(
    context: Any,
    asset_key: Any,
    task_run_id: Any,
    timeout_seconds: Optional[int],
    poll_interval_seconds: float,
    stream_logs: bool,
    stream_artifacts: bool,
    check_names: set,
):
    """Poll a Prefect task run (already submitted) until it reaches a terminal state.

    No forward_termination here: Prefect's task workers run a task to
    completion regardless of a cancellation request. We log a warning (same
    wording dagster-prefect's own client uses) instead of pretending a cancel
    call would do anything.

    Returns (task_run, collected_check_results) — the caller must yield each
    collected check result itself (see `_forward_prefect_artifact`'s
    docstring for why that can't happen from in here).
    """
    import asyncio
    import time as _time
    from datetime import timedelta

    from dagster import DagsterExecutionInterruptedError
    from prefect.client.orchestration import get_client

    log_cursor: Dict[str, Any] = {"after": None}
    seen_artifact_ids: set = set()
    collected_checks: List[Any] = []

    async def _tick():
        async with get_client() as client:
            tr = await client.read_task_run(task_run_id)

            new_logs: List[Any] = []
            if stream_logs:
                from prefect.client.schemas.filters import (
                    LogFilter, LogFilterTaskRunId, LogFilterTimestamp,
                )
                from prefect.client.schemas.sorting import LogSort

                ts_filter = (
                    LogFilterTimestamp(after_=log_cursor["after"])
                    if log_cursor["after"] is not None else None
                )
                new_logs = await client.read_logs(
                    log_filter=LogFilter(
                        task_run_id=LogFilterTaskRunId(any_=[task_run_id]),
                        timestamp=ts_filter,
                    ),
                    sort=LogSort.TIMESTAMP_ASC,
                )
                if new_logs:
                    log_cursor["after"] = new_logs[-1].timestamp + timedelta(microseconds=1)

            new_artifacts: List[Any] = []
            if stream_artifacts:
                from prefect.client.schemas.filters import ArtifactFilter, ArtifactFilterTaskRunId

                all_artifacts = await client.read_artifacts(
                    artifact_filter=ArtifactFilter(
                        task_run_id=ArtifactFilterTaskRunId(any_=[task_run_id]),
                    ),
                )
                new_artifacts = [a for a in all_artifacts if a.id not in seen_artifact_ids]
                seen_artifact_ids.update(a.id for a in new_artifacts)

            return tr, new_logs, new_artifacts

    start = _time.monotonic()
    try:
        while True:
            tr, new_logs, new_artifacts = asyncio.run(_tick())
            for entry in new_logs:
                _forward_prefect_log(context.log, entry)
            for artifact in new_artifacts:
                check_result = _forward_prefect_artifact(context, asset_key, artifact, check_names)
                if check_result is not None:
                    collected_checks.append(check_result)
            if tr.state is not None and tr.state.is_final():
                return tr, collected_checks
            if timeout_seconds is not None and (_time.monotonic() - start) > timeout_seconds:
                raise TimeoutError(
                    f"Timed out after {timeout_seconds}s waiting for Prefect task "
                    f"run {task_run_id}"
                )
            _time.sleep(poll_interval_seconds)
    except DagsterExecutionInterruptedError:
        context.log.warning(
            f"Dagster run terminated, but Prefect task run {task_run_id} cannot be "
            "cancelled: Prefect's task worker runs it to completion regardless. "
            "It will keep running."
        )
        raise


def _run_via_pipes(
    context: "dg.AssetExecutionContext",
    cfg: "PrefectBackgroundTaskAssetComponent",
    task_fn: Any,
    params: Dict[str, Any],
):
    """execution_mode='pipes' path — delegates to dagster-prefect's PipesPrefectTaskClient.

    Requires `pip install dagster-prefect` (lazily imported, never a hard
    dependency) and the task to open a Pipes session via its
    `dagster_pipes_params` argument (see dagster-prefect's docs for the
    exact task-side pattern).
    """
    try:
        from dagster_prefect import PipesPrefectTaskClient
        from dagster_prefect import PrefectResource as DagsterPrefectResource
    except ImportError as e:
        raise dg.Failure(
            description=(
                "execution_mode='pipes' requires the official `dagster-prefect` "
                "package (`pip install dagster-prefect`), and the task must accept "
                "a `dagster_pipes_params` argument and open a Pipes session with it. "
                "Use execution_mode='poll' (the default) if you don't want to "
                "modify the task's code."
            )
        ) from e

    if cfg.timeout_seconds is not None:
        context.log.warning(
            "execution_mode='pipes' polls until the task run reaches a terminal "
            "state and does not support timeout_seconds; ignoring."
        )
    if not cfg.wait_for_result:
        context.log.warning(
            "execution_mode='pipes' always waits for a terminal state; "
            "wait_for_result=False has no effect in this mode."
        )
    if cfg.stream_logs or cfg.stream_artifacts:
        context.log.warning(
            "stream_logs/stream_artifacts are only used in execution_mode='poll'; "
            "Pipes streams logs/metadata through its own message reader instead. "
            "Ignoring."
        )

    api_key = os.environ.get(cfg.api_key_env_var) if cfg.api_key_env_var else None
    prefect_resource = DagsterPrefectResource(
        api_url=cfg.api_url, api_key=api_key, ui_url=cfg.ui_url,
    )
    client = PipesPrefectTaskClient(
        prefect=prefect_resource,
        poll_interval_seconds=cfg.poll_interval_seconds,
    )
    return client.run(
        context=context,
        task=task_fn,
        parameters=params or None,
    ).get_materialize_result()


class PrefectBackgroundTaskAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Submit a Prefect background task (`.delay()`) and materialize it as a Dagster asset.

    Requires a task worker already serving the task (`prefect task serve`) —
    without one the task run stays scheduled and this blocks until the
    Dagster run is terminated. `forward_termination` isn't offered (unlike
    prefect_flow_run): Prefect's task workers ignore cancellation requests
    and run to completion regardless.

    Example — local Prefect server, streaming the task's own logs:

        ```yaml
        type: dagster_community_components.PrefectBackgroundTaskAssetComponent
        attributes:
          asset_name: summarized_doc
          task_import_path: "my_project.tasks:summarize"
          parameters:
            doc_id: "{partition_key}"
          stream_logs: true
          api_url: http://127.0.0.1:4200/api
        ```

    Example — turn a reported artifact into a real AssetCheckResult (same
    convention as prefect_flow_run's check_names, including the required
    dash/underscore translation — see that field's docs for why):

        ```yaml
        type: dagster_community_components.PrefectBackgroundTaskAssetComponent
        attributes:
          asset_name: summarized_doc
          task_import_path: "my_project.tasks:summarize"
          stream_artifacts: true
          check_names: [word_count_check]  # matches artifact key "word-count-check"
          wait_for_result: true
        ```
    """

    asset_name: str = Field(description="Dagster asset name (or '/' path).")
    task_import_path: str = Field(
        description=(
            "'module.path:function_name' of the @task-decorated function to submit. "
            "Must be importable from wherever this component builds its defs — "
            "background tasks have no by-name trigger API the way deployments do."
        ),
    )
    parameters: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Keyword arguments passed to task.delay(**parameters). String values "
            "are templated: `{partition_key}`, `{run_id}`, `{partition_window_start}` / "
            "`{partition_window_end}` (time-window partitions only). Non-string "
            "values pass through."
        ),
    )
    wait_for_result: bool = Field(
        default=True,
        description=(
            "Wait for the task run to reach a terminal state before the Dagster "
            "asset finishes materializing. When False, the asset returns "
            "immediately after submitting the task run."
        ),
    )
    timeout_seconds: Optional[int] = Field(
        default=None,
        description=(
            "Only used when wait_for_result=True. Max seconds to wait. "
            "None = wait indefinitely. If exceeded, the asset raises."
        ),
    )
    poll_interval_seconds: float = Field(
        default=5.0,
        description="Only used when wait_for_result=True. Seconds between polls.",
    )

    # Connection — inline (skip the resource for one-off usage)
    api_url: str = Field(
        default="http://127.0.0.1:4200/api",
        description="Prefect API URL. Default is local server at :4200.",
    )
    api_key_env_var: Optional[str] = Field(
        default=None,
        description="Env var holding a Prefect Cloud API key. Leave unset for local server.",
    )
    ui_url: Optional[str] = Field(
        default=None,
        description=(
            "Base URL of the Prefect UI, used to build the 'Prefect Run URL' "
            "materialization metadata link. Defaults to api_url with its trailing "
            "'/api' stripped. Prefect Cloud needs this set explicitly."
        ),
    )

    # Failure semantics
    fail_on_task_run_failure: bool = Field(
        default=True,
        description=(
            "When True and wait_for_result=True, the Dagster asset fails if the "
            "Prefect task run ends in a non-COMPLETED state. When False, the asset "
            "always materializes successfully — inspect the state in the metadata. "
            "Ignored when execution_mode='pipes', which always raises on failure."
        ),
    )

    # Observability (same shape + convention as prefect_flow_run)
    stream_logs: bool = Field(
        default=False,
        description=(
            "Only in execution_mode='poll'. Forward the task's own Prefect logs "
            "into the Dagster run log while waiting, via read_logs — no shared "
            "filesystem or blob store required. Off by default."
        ),
    )
    stream_artifacts: bool = Field(
        default=False,
        description=(
            "Only in execution_mode='poll'. Forward Prefect artifacts the task "
            "creates as AssetObservation events. Combine with check_names to turn "
            "specific artifacts into AssetCheckResults. Off by default."
        ),
    )
    check_names: Optional[List[str]] = Field(
        default=None,
        description=(
            "Requires stream_artifacts=True, wait_for_result=True, and "
            "execution_mode='poll' (validated at build time). Same convention "
            "as prefect_flow_run.check_names: a table artifact whose KEY, READ "
            "WITH DASHES AS UNDERSCORES, matches one of these names — e.g. "
            "check_names: [word_count_check] matches an artifact key of "
            "'word-count-check' (Prefect artifact keys reject underscores; "
            "Dagster check names reject dashes — see prefect_flow_run's fuller "
            "field docs for why). Pass table=[{'passed': bool, ...}] — a "
            "one-row table. An unreported declared check is reported as "
            "passed=False with an explanatory description, not silently "
            "skipped — Dagster requires every declared check to get a result "
            "every run."
        ),
    )

    # Execution
    execution_mode: str = Field(
        default="poll",
        description=(
            "'poll' (default) — trigger + poll via the plain Prefect SDK, zero task "
            "code changes required. 'pipes' — delegate to dagster-prefect's "
            "PipesPrefectTaskClient; requires `pip install dagster-prefect` and the "
            "task to accept a `dagster_pipes_params` argument."
        ),
    )

    # Standard catalog metadata
    group_name: Optional[str] = Field(default=None, description="Asset group.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    asset_tags: Optional[Dict[str, str]] = Field(default=None, description="Extra asset tags.")
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds. Defaults to ['prefect'].",
    )
    freshness_max_lag_minutes: Optional[int] = Field(default=None)
    freshness_cron: Optional[str] = Field(default=None)

    # Partitions
    partition_type: Optional[str] = Field(
        default=None,
        description="'daily'|'weekly'|'monthly'|'hourly'|'static'|'dynamic'|'multi'|None",
    )
    partition_start: Optional[str] = Field(default=None)
    partition_values: Optional[str] = Field(default=None)
    dynamic_partition_name: Optional[str] = Field(default=None)
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(default=None)

    # Deps (lineage-only)
    deps: Optional[List[str]] = Field(default=None)

    # Retry
    retry_policy_max_retries: Optional[int] = Field(default=None)
    retry_policy_delay_seconds: Optional[int] = Field(default=None)
    retry_policy_backoff: str = Field(default="exponential")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        if self.execution_mode not in ("poll", "pipes"):
            raise ValueError(
                f"PrefectBackgroundTaskAssetComponent: execution_mode must be "
                f"'poll' or 'pipes', got {self.execution_mode!r}."
            )
        if self.check_names:
            # See PrefectFlowRunAssetComponent.build_defs for why these are
            # hard requirements, not soft recommendations: a declared check
            # that never gets a result crashes the whole step.
            if not self.stream_artifacts:
                raise ValueError(
                    "PrefectBackgroundTaskAssetComponent: check_names requires "
                    "stream_artifacts=True — that's the only way a check result "
                    "is ever collected."
                )
            if not self.wait_for_result:
                raise ValueError(
                    "PrefectBackgroundTaskAssetComponent: check_names requires "
                    "wait_for_result=True — checks are collected while polling."
                )
            if self.execution_mode == "pipes":
                raise ValueError(
                    "PrefectBackgroundTaskAssetComponent: check_names is not "
                    "supported with execution_mode='pipes' — report checks "
                    "through Pipes' own report_asset_check() instead."
                )

        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values,
            self.dynamic_partition_name, self.partition_dimensions,
        )

        kinds = self.kinds or ["prefect"]
        tags = dict(self.asset_tags or {})
        for k in kinds:
            tags[f"dagster/kind/{k}"] = ""

        freshness = None
        if self.freshness_max_lag_minutes is not None:
            freshness = dg.FreshnessPolicy(
                maximum_lag_minutes=self.freshness_max_lag_minutes,
                cron_schedule=self.freshness_cron,
            )

        asset_key = dg.AssetKey.from_user_string(self.asset_name)
        check_names_set = set(self.check_names or [])
        check_specs = (
            [dg.AssetCheckSpec(name=n, asset=asset_key) for n in self.check_names]
            if self.check_names else None
        )

        retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        def _apply_env():
            os.environ["PREFECT_API_URL"] = _self.api_url
            if _self.api_key_env_var:
                key = os.environ.get(_self.api_key_env_var)
                if key:
                    os.environ["PREFECT_API_KEY"] = key

        def _template(
            value: Any, partition_key: str, run_id: str, window_start: str, window_end: str,
        ) -> Any:
            if not isinstance(value, str):
                return value
            return (
                value.replace("{partition_key}", partition_key)
                .replace("{run_id}", run_id)
                .replace("{partition_window_start}", window_start)
                .replace("{partition_window_end}", window_end)
            )

        @dg.asset(
            key=asset_key,
            description=self.description or f"Submit Prefect background task {self.task_import_path}",
            group_name=self.group_name,
            owners=self.owners or [],
            tags=tags,
            partitions_def=partitions_def,
            freshness_policy=freshness,
            retry_policy=retry_policy,
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
            check_specs=check_specs,
        )
        def _background_task_asset(context: dg.AssetExecutionContext):
            # No return-type annotation, and this is a generator (yield
            # below): see prefect_flow_run's identical comment + verification
            # for why check_specs requires this shape.
            _apply_env()
            task_fn = _import_task(_self.task_import_path)

            partition_key = str(context.partition_key) if context.has_partition_key else ""
            run_id = str(getattr(context, "run_id", "") or "")

            window_start = window_end = ""
            if context.has_partition_key:
                try:
                    time_window = context.partition_time_window
                    window_start = time_window.start.isoformat()
                    window_end = time_window.end.isoformat()
                except dg.DagsterInvariantViolationError:
                    pass

            params = {}
            for k, v in (_self.parameters or {}).items():
                params[k] = _template(v, partition_key, run_id, window_start, window_end)

            context.log.info(
                f"Submitting Prefect background task {_self.task_import_path!r} "
                f"with params={params} (wait_for_result={_self.wait_for_result}, "
                f"execution_mode={_self.execution_mode!r})"
            )

            if _self.execution_mode == "pipes":
                yield _run_via_pipes(context, _self, task_fn, params)
                return

            from prefect.settings import PREFECT_API_KEY, PREFECT_API_URL, temporary_settings

            settings_updates: Dict[Any, Any] = {PREFECT_API_URL: _self.api_url}
            if _self.api_key_env_var:
                key = os.environ.get(_self.api_key_env_var)
                if key:
                    settings_updates[PREFECT_API_KEY] = key
            with temporary_settings(settings_updates):
                future = task_fn.delay(**params)
            task_run_id = future.task_run_id

            task_run = None
            collected_checks: List[Any] = []
            if _self.wait_for_result:
                task_run, collected_checks = _poll_task_run_until_terminal(
                    context=context,
                    asset_key=asset_key,
                    task_run_id=task_run_id,
                    timeout_seconds=_self.timeout_seconds,
                    poll_interval_seconds=_self.poll_interval_seconds,
                    stream_logs=_self.stream_logs,
                    stream_artifacts=_self.stream_artifacts,
                    check_names=check_names_set,
                )

            reported_names = {c.check_name for c in collected_checks}
            for missing_name in check_names_set - reported_names:
                # See prefect_flow_run's identical fallback: an unreported
                # declared check crashes the whole step otherwise.
                collected_checks.append(dg.AssetCheckResult(
                    check_name=missing_name,
                    passed=False,
                    description=(
                        f"No Prefect table artifact with key={missing_name!r} and "
                        f"data containing 'passed' was reported for this task run."
                    ),
                ))
            yield from collected_checks

            state = task_run.state if task_run else None
            state_name = getattr(state, "name", "unknown") if state else "unknown"
            state_type = getattr(getattr(state, "type", None), "value", "unknown")
            state_message = getattr(state, "message", "") if state else ""

            result_meta: Dict[str, Any] = {
                "task_run_id": dg.MetadataValue.text(str(task_run_id)),
                "task_import_path": dg.MetadataValue.text(_self.task_import_path),
                "state_name": dg.MetadataValue.text(state_name),
                "state_type": dg.MetadataValue.text(state_type),
                "state_message": dg.MetadataValue.text(state_message[:500] if state_message else ""),
                "prefect_run_url": dg.MetadataValue.url(
                    _prefect_task_run_url(_self.api_url, _self.ui_url, task_run_id)
                ),
                "parameters": dg.MetadataValue.json(params),
                "wait_for_result": dg.MetadataValue.bool(_self.wait_for_result),
            }
            # output_name required once check_specs adds extra Out()s —
            # "result" is @dg.asset's default primary-output name either way.
            context.add_output_metadata(result_meta, output_name="result")

            context.log.info(f"Prefect task run {task_run_id} state={state_name} ({state_type})")

            terminal_success = (state_type == "COMPLETED")
            terminal_failure = (state_type in {"FAILED", "CRASHED", "CANCELLED"})

            if _self.wait_for_result and _self.fail_on_task_run_failure and terminal_failure:
                raise dg.Failure(
                    description=f"Prefect task run ended in {state_name} state: {state_message}",
                    metadata=result_meta,
                )

            yield dg.Output({
                "task_run_id": str(task_run_id),
                "state_name": state_name,
                "state_type": state_type,
                "parameters": params,
                "terminal_success": terminal_success,
            })

        return dg.Definitions(assets=[_background_task_asset])
