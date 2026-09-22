"""Prefect Flow Run — Dagster asset that triggers a Prefect deployment.

Materializing this asset creates a Prefect flow run (via
`prefect.deployments.run_deployment`) and — by default — waits for the
flow to complete before returning. Flow run id, state, state message,
and per-parameter values land in the materialization metadata so you
can inspect them from `dg dev`.

The asset works against:
  - Local Prefect server (`prefect server start` — default api_url
    'http://127.0.0.1:4200/api').
  - Prefect Cloud (set api_url + api_key_env_var to your cloud workspace).

For a per-file document processing flow (Dagster owns the partition,
Prefect owns the per-file work), pair with a partitioned upstream that
supplies the parameters — e.g. a partition_static_column pointing at a
`file_path` field in an upstream DataFrame.

Two execution modes (`execution_mode`):
  - "poll" (default) — plain Prefect SDK (`run_deployment` + `get_client`).
    Zero changes to the flow's code. Termination of the Dagster run is
    forwarded as a Prefect flow-run cancellation (`forward_termination`,
    on by default) the same way dagster-prefect's Pipes client does it,
    without requiring dagster-prefect or Pipes instrumentation. Set
    `stream_logs: true` to also forward the flow's own Prefect logs into
    the Dagster run log while it waits (see below).
  - "pipes" — delegates to the official `dagster-prefect` package's
    `PipesPrefectDeploymentClient`. Requires `pip install dagster-prefect`
    AND the flow to open a Pipes session (`open_dagster_pipes()`) so it
    can stream metadata/logs back during the run instead of only a
    terminal-state summary. Opt-in — most flows don't need this.

On `stream_logs` vs. dagster-prefect's Pipes log streaming: Pipes' default
message reader is a temp file, which only works when the Dagster step and
the Prefect worker share a filesystem — not true for Dagster+ or any
worker on separate infrastructure. Without a reachable message reader,
dagster-prefect's docs say the asset still materializes on success, but
silently *without* the metadata/logs/checks the flow reported — a quiet
failure mode, not an error. `stream_logs` sidesteps this by polling
Prefect's own `read_logs` API (the same store the Prefect UI reads from,
centrally hosted, reachable wherever the Prefect API already is) instead
of needing any shared filesystem or blob store — it costs one extra API
call per poll tick, nothing else.

Docs: https://docs.prefect.io/latest/develop/deployments/
"""
import os
from typing import Any, Dict, List, Optional, Union

import dagster as dg
from pydantic import Field


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


def _prefect_run_url(api_url: str, ui_url: Optional[str], flow_run_id: Any) -> str:
    """Build a link into the Prefect UI for a flow run.

    Mirrors dagster-prefect's `PrefectResource._run_url`: UI base is `ui_url`
    if set, else `api_url` with its trailing `/api` stripped (correct for a
    local/self-hosted server; Prefect Cloud needs `ui_url` set explicitly
    since it serves the UI from a different host).
    """
    base = (ui_url or api_url).rstrip("/").removesuffix("/api")
    return f"{base}/runs/flow-run/{flow_run_id}"


def _forward_prefect_log(dagster_log: Any, entry: Any) -> None:
    """Re-emit one Prefect `Log` record as a Dagster run-log line.

    Prefect's log levels are plain stdlib logging ints (10/20/30/40/50);
    map them onto the matching DagsterLogManager method so severity is
    preserved in the Dagster UI, not flattened to a single level.
    """
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


def _poll_flow_run_until_terminal(
    flow_run_id: Any,
    timeout_seconds: Optional[int],
    poll_interval_seconds: float,
    forward_termination: bool,
    stream_logs: bool,
    log: Any,
):
    """Poll a Prefect flow run (already submitted) until it reaches a terminal state.

    Polling happens in OUR loop rather than inside `run_deployment(timeout=...)`'s
    own internal wait, so a `DagsterExecutionInterruptedError` raised mid-poll (the
    Dagster run was terminated) still has `flow_run_id` in scope to forward the
    cancellation to Prefect — the same architecture dagster-prefect's Pipes client
    uses for `forward_termination`, but built on the plain Prefect SDK so it works
    in execution_mode='poll' with no dagster-prefect dependency.

    When `stream_logs`, each tick also pulls new Prefect log rows (via
    `client.read_logs`, cursor = last-seen timestamp) and forwards them into the
    Dagster run log — see the module docstring for why this exists instead of
    relying on dagster-prefect's Pipes message reader.
    """
    import asyncio
    import time as _time
    from datetime import timedelta

    from dagster import DagsterExecutionInterruptedError
    from prefect.client.orchestration import get_client

    log_cursor: Dict[str, Any] = {"after": None}

    async def _tick():
        async with get_client() as client:
            fr = await client.read_flow_run(flow_run_id)
            new_logs: List[Any] = []
            if stream_logs:
                from prefect.client.schemas.filters import (
                    LogFilter, LogFilterFlowRunId, LogFilterTimestamp,
                )
                from prefect.client.schemas.sorting import LogSort

                ts_filter = (
                    LogFilterTimestamp(after_=log_cursor["after"])
                    if log_cursor["after"] is not None else None
                )
                new_logs = await client.read_logs(
                    log_filter=LogFilter(
                        flow_run_id=LogFilterFlowRunId(any_=[flow_run_id]),
                        timestamp=ts_filter,
                    ),
                    sort=LogSort.TIMESTAMP_ASC,
                )
                if new_logs:
                    # +1us: `after_` is inclusive, so bump past the last-seen
                    # timestamp to avoid re-forwarding the same line next tick.
                    log_cursor["after"] = new_logs[-1].timestamp + timedelta(microseconds=1)
            return fr, new_logs

    async def _cancel():
        from prefect.states import Cancelling

        async with get_client() as client:
            await client.set_flow_run_state(flow_run_id, Cancelling())

    start = _time.monotonic()
    try:
        while True:
            fr, new_logs = asyncio.run(_tick())
            for entry in new_logs:
                _forward_prefect_log(log, entry)
            if fr.state is not None and fr.state.is_final():
                return fr
            if timeout_seconds is not None and (_time.monotonic() - start) > timeout_seconds:
                raise TimeoutError(
                    f"Timed out after {timeout_seconds}s waiting for Prefect flow "
                    f"run {flow_run_id}"
                )
            _time.sleep(poll_interval_seconds)
    except DagsterExecutionInterruptedError:
        if forward_termination:
            log.info(
                f"Dagster run terminated — cancelling Prefect flow run {flow_run_id}"
            )
            try:
                asyncio.run(_cancel())
            except Exception as cancel_err:
                log.warning(
                    f"Failed to cancel Prefect flow run {flow_run_id}: {cancel_err}"
                )
        raise


def _run_via_pipes(
    context: "dg.AssetExecutionContext",
    cfg: "PrefectFlowRunAssetComponent",
    params: Dict[str, Any],
):
    """execution_mode='pipes' path — delegates to dagster-prefect's official Pipes client.

    Requires `pip install dagster-prefect` (not a hard dependency of this component —
    imported lazily so execution_mode='poll', the default, never needs it) and the
    target flow to open a Pipes session (`open_dagster_pipes()`).
    """
    try:
        from dagster_prefect import PipesPrefectDeploymentClient
        from dagster_prefect import PrefectResource as DagsterPrefectResource
    except ImportError as e:
        raise dg.Failure(
            description=(
                "execution_mode='pipes' requires the official `dagster-prefect` "
                "package (`pip install dagster-prefect`), and the target flow must "
                "open a Pipes session (`from dagster_pipes import open_dagster_pipes`) "
                "to report results back. Use execution_mode='poll' (the default) if "
                "you don't want to modify the flow's code."
            )
        ) from e

    if cfg.timeout_seconds is not None:
        context.log.warning(
            "execution_mode='pipes' polls until the flow run reaches a terminal "
            "state and does not support timeout_seconds; ignoring."
        )
    if not cfg.fail_on_flow_run_failure:
        context.log.warning(
            "execution_mode='pipes' always raises on a non-successful flow run; "
            "fail_on_flow_run_failure=False has no effect in this mode."
        )
    if not cfg.wait_for_result:
        context.log.warning(
            "execution_mode='pipes' always waits for a terminal state; "
            "wait_for_result=False has no effect in this mode."
        )
    if cfg.stream_logs:
        context.log.warning(
            "stream_logs is only used in execution_mode='poll'; Pipes streams "
            "logs/metadata through its own message reader instead. Ignoring."
        )

    api_key = os.environ.get(cfg.api_key_env_var) if cfg.api_key_env_var else None
    prefect_resource = DagsterPrefectResource(
        api_url=cfg.api_url, api_key=api_key, ui_url=cfg.ui_url,
    )
    client = PipesPrefectDeploymentClient(
        prefect=prefect_resource,
        poll_interval_seconds=cfg.poll_interval_seconds,
        forward_termination=cfg.forward_termination,
    )
    return client.run(
        context=context,
        deployment=cfg.deployment_name,
        parameters=params or None,
        tags=list(cfg.tags or []),
    ).get_materialize_result()


class PrefectFlowRunAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Trigger a Prefect deployment and materialize the flow run as a Dagster asset.

    Example — local Prefect server, unpartitioned:

        ```yaml
        type: dagster_community_components.PrefectFlowRunAssetComponent
        attributes:
          asset_name: nightly_report
          deployment_name: "reporting/nightly"      # flow_name/deployment_name
          parameters:
            region: us-east-1
            date: "2026-07-31"
          wait_for_result: true
          api_url: http://127.0.0.1:4200/api
        ```

    Example — Prefect Cloud, dynamic-partitioned per file:

        ```yaml
        type: dagster_community_components.PrefectFlowRunAssetComponent
        attributes:
          asset_name: parsed_document
          deployment_name: "document-parser/main"
          # {partition_key} substitutes into any string parameter value.
          parameters:
            file_path: "{partition_key}"
          partition_type: dynamic
          dynamic_partition_name: incoming_files
          api_url: https://api.prefect.cloud/api/accounts/<acct>/workspaces/<ws>
          api_key_env_var: PREFECT_API_KEY
        ```

    Example — forward the flow's own Prefect logs into the Dagster run log
    while waiting (no shared filesystem / blob store needed, unlike Pipes):

        ```yaml
        type: dagster_community_components.PrefectFlowRunAssetComponent
        attributes:
          asset_name: nightly_report
          deployment_name: "reporting/nightly"
          stream_logs: true
          api_url: http://127.0.0.1:4200/api
        ```

    Example — opt into the official dagster-prefect Pipes client for a flow
    that reports its own metadata (requires `pip install dagster-prefect`
    and `open_dagster_pipes()` in the flow):

        ```yaml
        type: dagster_community_components.PrefectFlowRunAssetComponent
        attributes:
          asset_name: nightly_report
          deployment_name: "reporting/nightly"
          execution_mode: pipes
          api_url: http://127.0.0.1:4200/api
        ```
    """

    asset_name: str = Field(description="Dagster asset name (or '/' path).")
    deployment_name: str = Field(
        description="Prefect deployment name in 'flow_name/deployment_name' format.",
    )
    parameters: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Parameters passed to the flow run. String values are templated: "
            "`{partition_key}` substitutes the current partition key; "
            "`{run_id}` substitutes the Dagster run_id; `{partition_window_start}` / "
            "`{partition_window_end}` substitute the ISO 8601 start/end of the current "
            "time-window partition (daily/weekly/monthly/hourly only — empty string "
            "otherwise). Non-string values pass through."
        ),
    )
    wait_for_result: bool = Field(
        default=True,
        description=(
            "Wait for the flow run to reach a terminal state before the "
            "Dagster asset finishes materializing. When False, the asset "
            "returns immediately after submitting the flow run — use this "
            "for fire-and-forget triggers and pair with `prefect_flow_run_sensor` "
            "downstream."
        ),
    )
    timeout_seconds: Optional[int] = Field(
        default=None,
        description=(
            "Only used when wait_for_result=True. Max seconds to wait for the "
            "flow run. None = wait indefinitely. If exceeded, the asset raises."
        ),
    )
    poll_interval_seconds: float = Field(
        default=5.0,
        description="Only used when wait_for_result=True. Seconds between polls.",
    )
    tags: Optional[List[str]] = Field(
        default=None, description="Tags applied to the created flow run.",
    )
    flow_run_name: Optional[str] = Field(
        default=None,
        description=(
            "Optional flow run name. Templated with `{partition_key}` and "
            "`{run_id}` like parameter values."
        ),
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
            "'/api' stripped, which is correct for a local/self-hosted server. "
            "Prefect Cloud serves its UI from a different host — set this explicitly "
            "there, e.g. https://app.prefect.cloud/account/<acct>/workspace/<ws>."
        ),
    )

    # Failure semantics
    fail_on_flow_run_failure: bool = Field(
        default=True,
        description=(
            "When True and wait_for_result=True, the Dagster asset fails if "
            "the Prefect flow run ends in a non-COMPLETED state (FAILED, "
            "CRASHED, CANCELLED). When False, the asset always materializes "
            "successfully — inspect the state in the metadata. Ignored when "
            "execution_mode='pipes', which always raises on a non-successful run."
        ),
    )
    forward_termination: bool = Field(
        default=True,
        description=(
            "When wait_for_result=True, cancel the Prefect flow run if the Dagster "
            "run is terminated/interrupted while waiting. Uses the plain Prefect SDK "
            "(no dagster-prefect dependency) — same behavior dagster-prefect's Pipes "
            "client calls 'forward_termination'. Applies to both execution modes."
        ),
    )
    stream_logs: bool = Field(
        default=False,
        description=(
            "Only in execution_mode='poll'. Forward the flow's own Prefect logs into "
            "the Dagster run log while waiting, via Prefect's read_logs API — no "
            "shared filesystem or blob store required (unlike dagster-prefect's Pipes "
            "message reader, which silently drops metadata/logs/checks when the "
            "Dagster step and Prefect worker don't share a filesystem). Costs one "
            "extra API call per poll tick; off by default."
        ),
    )

    # Execution
    execution_mode: str = Field(
        default="poll",
        description=(
            "'poll' (default) — trigger + poll via the plain Prefect SDK, zero flow "
            "code changes required. 'pipes' — delegate to dagster-prefect's "
            "PipesPrefectDeploymentClient for in-flight metadata/log streaming; "
            "requires `pip install dagster-prefect` and the flow to call "
            "open_dagster_pipes(). See the class docstring for an example of each."
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
                f"PrefectFlowRunAssetComponent: execution_mode must be 'poll' or "
                f"'pipes', got {self.execution_mode!r}."
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
            key=dg.AssetKey.from_user_string(self.asset_name),
            description=self.description or f"Trigger Prefect deployment {self.deployment_name}",
            group_name=self.group_name,
            owners=self.owners or [],
            tags=tags,
            partitions_def=partitions_def,
            freshness_policy=freshness,
            retry_policy=retry_policy,
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def _flow_run_asset(context: dg.AssetExecutionContext) -> Any:
            # Return type is `dict` in execution_mode='poll' (see below) or a
            # `MaterializeResult` in execution_mode='pipes' (_run_via_pipes) —
            # Dagster dispatches on the runtime value either way.
            _apply_env()
            from prefect.deployments import run_deployment

            partition_key = str(context.partition_key) if context.has_partition_key else ""
            run_id = str(getattr(context, "run_id", "") or "")

            window_start = window_end = ""
            if context.has_partition_key:
                try:
                    time_window = context.partition_time_window
                    window_start = time_window.start.isoformat()
                    window_end = time_window.end.isoformat()
                except dg.DagsterInvariantViolationError:
                    pass  # static/dynamic partitions have no time window

            params = {}
            for k, v in (_self.parameters or {}).items():
                params[k] = _template(v, partition_key, run_id, window_start, window_end)

            flow_run_name = (
                _template(_self.flow_run_name, partition_key, run_id, window_start, window_end)
                if _self.flow_run_name else None
            )

            context.log.info(
                f"Triggering Prefect deployment {_self.deployment_name!r} "
                f"with params={params} (wait_for_result={_self.wait_for_result}, "
                f"execution_mode={_self.execution_mode!r})"
            )

            if _self.execution_mode == "pipes":
                return _run_via_pipes(context, _self, params)

            # execution_mode == "poll": submit without waiting (timeout=0), then
            # poll ourselves so forward_termination can reach the flow_run_id even
            # if we're interrupted mid-wait — see _poll_flow_run_until_terminal.
            flow_run = run_deployment(
                name=_self.deployment_name,
                parameters=params or None,
                flow_run_name=flow_run_name,
                tags=list(_self.tags or []),
                timeout=0,
                poll_interval=_self.poll_interval_seconds,
            )

            if _self.wait_for_result:
                flow_run = _poll_flow_run_until_terminal(
                    flow_run_id=flow_run.id,
                    timeout_seconds=_self.timeout_seconds,
                    poll_interval_seconds=_self.poll_interval_seconds,
                    forward_termination=_self.forward_termination,
                    stream_logs=_self.stream_logs,
                    log=context.log,
                )

            state = flow_run.state
            state_name = getattr(state, "name", "unknown") if state else "unknown"
            state_type = getattr(getattr(state, "type", None), "value", "unknown")
            state_message = getattr(state, "message", "") if state else ""

            result_meta: Dict[str, Any] = {
                "flow_run_id": dg.MetadataValue.text(str(flow_run.id)),
                "flow_name": dg.MetadataValue.text(str(getattr(flow_run, "name", ""))),
                "deployment_name": dg.MetadataValue.text(_self.deployment_name),
                "state_name": dg.MetadataValue.text(state_name),
                "state_type": dg.MetadataValue.text(state_type),
                "state_message": dg.MetadataValue.text(state_message[:500] if state_message else ""),
                "prefect_url": dg.MetadataValue.text(_self.api_url),
                "prefect_run_url": dg.MetadataValue.url(
                    _prefect_run_url(_self.api_url, _self.ui_url, flow_run.id)
                ),
                "parameters": dg.MetadataValue.json(params),
                "wait_for_result": dg.MetadataValue.bool(_self.wait_for_result),
            }
            context.add_output_metadata(result_meta)

            terminal_success = (state_type == "COMPLETED")
            terminal_failure = (state_type in {"FAILED", "CRASHED", "CANCELLED"})

            context.log.info(
                f"Prefect flow run {flow_run.id} state={state_name} ({state_type})"
            )

            # Note: we deliberately do NOT try to fetch state.result(). Fetching
            # the flow's return value requires Prefect result persistence to be
            # configured, which is Prefect-specific plumbing users shouldn't have
            # to worry about here. Downstream Dagster assets read the artifacts
            # the flow WROTE (to S3/GCS/local disk) — the standard Prefect-owns-
            # execution pattern. This asset's return dict is intentionally small
            # (flow_run_id + state + parameters), enough for lineage + to derive
            # where the flow's artifacts landed if the output path was passed as
            # a parameter.

            if _self.wait_for_result and _self.fail_on_flow_run_failure and terminal_failure:
                raise dg.Failure(
                    description=f"Prefect flow run ended in {state_name} state: {state_message}",
                    metadata=result_meta,
                )

            return {
                "flow_run_id": str(flow_run.id),
                "state_name": state_name,
                "state_type": state_type,
                "parameters": params,
                "terminal_success": terminal_success,
            }

        return dg.Definitions(assets=[_flow_run_asset])
