"""DbtCommandAssetComponent.

Run an arbitrary `dbt` CLI command (run, build, test, snapshot, seed, source freshness) as a
single materializable Dagster asset -- the asset-shaped counterpart to `jobs/dbt_run_job`
(job/op-shaped, explicitly "no asset modeling" per its own docstring) and a lighter-weight
alternative to `assets/dbt/enriched_dbt_project` (which requires a dbt manifest and emits one
asset PER dbt model). Use this when you just want "run this dbt command, materialize one asset
on success" without per-model asset modeling -- the same relationship `shell_command_asset` has
to a full per-step pipeline component.

Runs `dbt <command> --project-dir <dir> [--profiles-dir ...] [--target ...] [--select ...]
[--exclude ...] [--full-refresh] [--vars ...]` via `subprocess.run`, streaming stdout/stderr to
the run log. Exit code -> asset success/failure. Best-effort: if the command produced a
`target/run_results.json` (every `dbt run|build|test|snapshot|seed` invocation does), its
per-node `status` counts are attached as asset metadata -- `pass`/`error`/`skip`/`warn` node
counts and total elapsed seconds -- without requiring a full manifest-based asset graph.
"""
import json
import os
import subprocess
from datetime import datetime, timezone
from typing import Any, Dict, List, Literal, Optional, Tuple, Union

import dagster as dg
from pydantic import Field


def _build_partitions_def(
    partition_type,
    partition_start,
    partition_values,
    dynamic_partition_name,
    partition_dimensions,
    cron_schedule=None,
    cron_fmt="%Y-%m-%d",
    end_offset=0,
    timezone=None,
):
    """Construct a Dagster partitions_def from the canonical partition fields.

    Strict: raises ValueError on misconfigured combinations rather than
    silently picking a default. Specifically:
      - time-based partition_type without partition_start
      - partition_type=multi without partition_values
      - partition_type=dynamic without dynamic_partition_name
      - both partition_dimensions AND flat fields set (ambiguous intent)
    """
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )

    if partition_dimensions and partition_type:
        raise ValueError(
            "Set either partition_type (flat-fields shape) or "
            "partition_dimensions (multi-axis shape), not both."
        )

    def _build_axis(spec):
        t = spec.get("type")
        _end_offset = spec.get("end_offset", 0)
        _timezone = spec.get("timezone")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily":
            return DailyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "weekly":
            return WeeklyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "monthly":
            return MonthlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "hourly":
            return HourlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
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
                raise ValueError("partition dimension type='static' requires non-empty 'values'")
            return StaticPartitionsDefinition(list(vals))
        if t == "dynamic":
            name = spec.get("dynamic_partition_name") or spec.get("name")
            if not name:
                raise ValueError("partition dimension type='dynamic' requires a name")
            return DynamicPartitionsDefinition(name=name)
        raise ValueError(f"unknown partition type: {t!r}")

    if partition_dimensions:
        if len(partition_dimensions) == 1:
            return _build_axis(partition_dimensions[0])
        axes = {d["name"]: _build_axis(d) for d in partition_dimensions}
        return MultiPartitionsDefinition(axes)

    if not partition_type:
        return None
    if isinstance(partition_values, (list, tuple)):
        _values = [str(v).strip() for v in partition_values if str(v).strip()]
    else:
        _values = [v.strip() for v in (str(partition_values) if partition_values else "").split(",") if v.strip()]
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
            raise ValueError(
                "partition_type='cron' requires cron_schedule, e.g. '0 0 * * 1-5' for weekdays only."
            )
        return TimeWindowPartitionsDefinition(start=partition_start, fmt=cron_fmt, cron_schedule=cron_schedule, end_offset=end_offset, timezone=timezone)
    if partition_type == "static":
        if not _values:
            raise ValueError("partition_type='static' requires partition_values (comma-separated).")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    if partition_type == "multi":
        if not _values:
            raise ValueError("partition_type='multi' requires partition_values (comma-separated).")
        if not partition_start:
            raise ValueError("partition_type='multi' requires partition_start (the date axis start).")
        return MultiPartitionsDefinition({
            "date": DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone),
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")


# BEGIN freshness-policy compat (inlined per component; keep copies identical, see tests/test_freshness_policy_compat.py)
def _freshness_policy_compat(max_lag_minutes, cron_schedule=None):
    """Build a FreshnessPolicy on the current Dagster API (time_window / cron) and on the older constructor."""
    from datetime import timedelta

    from dagster import FreshnessPolicy

    lag = timedelta(minutes=max_lag_minutes)
    if hasattr(FreshnessPolicy, "time_window"):
        if cron_schedule:
            return FreshnessPolicy.cron(deadline_cron=cron_schedule, lower_bound_delta=lag)
        return FreshnessPolicy.time_window(fail_window=lag)
    return FreshnessPolicy(maximum_lag_minutes=max_lag_minutes, cron_schedule=cron_schedule)
# END freshness-policy compat


# BEGIN run_window (inlined per component; keep copies identical — ported verbatim from
# sensors/event_automation's own business_hours_only parser/checker, same format and same
# fail-open-on-malformed-input behavior, just renamed for this field's own name)
_WEEKDAY_MAP = {"mon": 0, "tue": 1, "wed": 2, "thu": 3, "fri": 4, "sat": 5, "sun": 6}


def _parse_run_window(spec: str) -> Tuple[int, int, str, Optional[List[int]]]:
    """Parse 'HH:MM-HH:MM tz [day,day,...]' into (start_min, end_min, tz, weekdays). Raises
    ValueError on malformed input."""
    parts = spec.strip().split()
    if len(parts) < 2:
        raise ValueError(f"run_window needs 'HH:MM-HH:MM tz [days]', got '{spec}'")
    window, tz_name = parts[0], parts[1]
    start_str, _, end_str = window.partition("-")
    sh, _, sm = start_str.partition(":")
    eh, _, em = end_str.partition(":")
    start_min = int(sh) * 60 + int(sm or 0)
    end_min = int(eh) * 60 + int(em or 0)
    weekdays: Optional[List[int]] = None
    if len(parts) >= 3:
        weekdays = []
        for day in parts[2].split(","):
            key = day.strip().lower()[:3]
            if key in _WEEKDAY_MAP:
                weekdays.append(_WEEKDAY_MAP[key])
    return start_min, end_min, tz_name, weekdays


def _is_in_run_window(now_ts: float, spec: str) -> bool:
    """True if now_ts falls in the daily window (and optional day-of-week list). Fails open
    (returns True -- never blocks) on a malformed spec or any unexpected error."""
    try:
        start_min, end_min, tz_name, weekdays = _parse_run_window(spec)
    except Exception:
        return True
    try:
        try:
            from zoneinfo import ZoneInfo
            tz = ZoneInfo(tz_name) if tz_name.upper() != "UTC" else timezone.utc
        except Exception:
            tz = timezone.utc
        local = datetime.fromtimestamp(now_ts, tz=tz)
        cur_min = local.hour * 60 + local.minute
        if weekdays is not None and local.weekday() not in weekdays:
            return False
        if start_min <= end_min:
            return start_min <= cur_min < end_min
        return cur_min >= start_min or cur_min < end_min  # overnight window, e.g. 22:00-06:00
    except Exception:
        return True
# END run_window


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


def _parse_run_results(project_dir: str, target_path: Optional[str]) -> Optional[Dict[str, Any]]:
    """Best-effort parse of dbt's own `target/run_results.json` artifact (written by every
    `dbt run|build|test|snapshot|seed` invocation). Returns a summary dict of per-status node
    counts + total elapsed seconds, or None if the file doesn't exist or isn't parseable --
    never raises, since this is metadata enrichment, not a correctness requirement."""
    candidate = os.path.join(project_dir, target_path or "target", "run_results.json")
    try:
        with open(candidate) as f:
            data = json.load(f)
    except (OSError, ValueError):
        return None
    results = data.get("results") or []
    counts: Dict[str, int] = {}
    for node in results:
        status = str(node.get("status", "unknown"))
        counts[status] = counts.get(status, 0) + 1
    elapsed = data.get("elapsed_time")
    return {
        "node_count": len(results),
        "status_counts": counts,
        "elapsed_seconds": float(elapsed) if elapsed is not None else None,
    }


class DbtCommandAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Run an arbitrary `dbt` CLI command as a single materializable Dagster asset."""

    asset_name: str = Field(description="Dagster asset name.")
    project_dir: str = Field(description="Path to the dbt project root (where dbt_project.yml lives).")
    profiles_dir: Optional[str] = Field(default=None, description="Path to profiles.yml (default: ~/.dbt).")
    target: Optional[str] = Field(default=None, description="dbt target name (overrides the project default).")
    command: Literal["run", "build", "test", "snapshot", "seed", "source"] = Field(
        default="run",
        description="dbt subcommand to run. 'source' runs `dbt source freshness`.",
    )
    select: Optional[str] = Field(default=None, description="--select expression. Supports `{partition_key}` when partition_type is set.")
    exclude: Optional[str] = Field(default=None, description="--exclude expression.")
    full_refresh: bool = Field(default=False, description="Pass --full-refresh.")
    vars: Optional[dict] = Field(default=None, description="--vars, passed through as JSON. Supports `{partition_key}` values when partition_type is set.")
    extra_args: Optional[List[str]] = Field(default=None, description="Additional raw CLI args appended after the built command, for flags this component doesn't have a dedicated field for.")

    cwd: Optional[str] = Field(default=None, description="Working directory to run dbt from. Defaults to project_dir when unset.")
    env_vars: Optional[Dict[str, str]] = Field(default=None, description="Additional env vars to pass to the dbt subprocess (merged with the parent process's own environment).")
    timeout_seconds: Optional[int] = Field(default=None, description="Kill the command and fail the run if it runs longer than this many seconds. Unset: no timeout.")
    dry_run: bool = Field(
        default=False,
        description="If True, log the fully-resolved dbt command (after {partition_key} substitution) and skip actually running it -- no subprocess is spawned. Output metadata always includes dry_run so a materialization history shows which runs were real.",
    )
    run_window: Optional[str] = Field(
        default=None,
        description=(
            "'HH:MM-HH:MM tz [day,day,...]' (e.g. '09:00-17:00 America/New_York "
            "mon,tue,wed,thu,fri'). Outside this window, the asset SKIPS cleanly instead of "
            "running or failing. Checked once per materialization, before dry_run or the "
            "command itself. A malformed spec fails open (never blocks). Unset (default): "
            "always runs."
        ),
    )
    pool: Optional[str] = Field(
        default=None,
        description="Dagster's native named concurrency pool. The pool's own slot count is configured at the deployment level, not here.",
    )
    parse_run_results: bool = Field(
        default=True,
        description=(
            "After a non-dry-run invocation, best-effort parse the dbt project's own "
            "target/run_results.json for per-status node counts + elapsed time and attach as "
            "asset metadata. Silently skipped if the file doesn't exist or isn't parseable -- "
            "this is enrichment, not a requirement."
        ),
    )
    target_path: Optional[str] = Field(default=None, description="dbt's target-path (default 'target') -- only used to locate run_results.json for parse_run_results, not passed to the CLI.")

    group_name: str = Field(default="dbt", description="Asset group.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    deps: Optional[List[str]] = Field(default=None, description="Upstream Dagster asset keys for lineage.")

    partition_type: Optional[str] = Field(default=None, description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'cron', 'static', 'dynamic', 'multi', or None for unpartitioned. With a partition type set, `{partition_key}` is substituted into `select`/`cwd`.")
    partition_cron_schedule: Optional[str] = Field(default=None, description="Required when partition_type='cron'.")
    partition_cron_fmt: Optional[str] = Field(default="%Y-%m-%d", description="strftime format for partition keys produced by partition_type='cron'.")
    partition_end_offset: int = Field(default=0, description="0 (default): a time-based partition set only ever holds COMPLETED windows. 1: include the current, still-in-progress window.")
    partition_timezone: Optional[str] = Field(default=None, description="IANA timezone for the partitions definition (defaults to UTC).")
    partition_start: Optional[str] = Field(default=None, description="Partition start date in ISO format, e.g. '2024-01-01'. Required when partition_type is time-based.")
    dynamic_partition_name: Optional[str] = Field(default=None, description="Name for DynamicPartitionsDefinition (partition_type='dynamic').")
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(default=None, description="Multi-axis partition spec. Overrides flat fields when set.")
    partition_values: Optional[Union[str, List[str]]] = Field(default=None, description="Comma-separated values for static or multi partitioning.")
    backfill_policy: Optional[Literal["single_run", "multi_run"]] = Field(default=None, description="Backfill execution strategy for a partitioned asset.")
    backfill_policy_max_partitions_per_run: Optional[int] = Field(default=None, description="For backfill_policy='multi_run' only.")

    retry_policy_max_retries: Optional[int] = Field(default=None, description="Max retries on asset failure. Defines a RetryPolicy.")
    retry_policy_delay_seconds: Optional[int] = Field(default=None, description="Seconds between retries (default 1).")
    retry_policy_backoff: str = Field(default="exponential", description="Backoff strategy: 'linear' or 'exponential'.")

    owners: Optional[List[str]] = Field(default=None, description="Asset owners -- team names ('team:analytics') or email addresses.")
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Additional key-value tags applied to the asset in the Dagster catalog.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds for the catalog. Defaults to ['dbt'].")
    freshness_max_lag_minutes: Optional[int] = Field(default=None, description="Maximum acceptable lag in minutes before the asset is considered stale.")
    freshness_cron: Optional[str] = Field(default=None, description="Cron schedule string for the freshness policy.")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('deps', 'owners', 'kinds', 'extra_args'), csv=('partition_values',), tags=('asset_tags',))

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='Dbt Command Asset', editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        asset_key = dg.AssetKey.from_user_string(self.asset_name)
        project_dir = self.project_dir
        profiles_dir = self.profiles_dir
        target = self.target
        command = self.command
        static_select = self.select
        exclude = self.exclude
        full_refresh = self.full_refresh
        static_vars = self.vars
        extra_args = self.extra_args or []
        cwd = self.cwd or project_dir
        env_vars = self.env_vars or {}
        timeout_seconds = self.timeout_seconds
        dry_run = self.dry_run
        run_window = self.run_window
        parse_results = self.parse_run_results
        target_path = self.target_path
        deps_keys = [dg.AssetKey.from_user_string(k) for k in (self.deps or [])]

        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values,
            self.dynamic_partition_name, self.partition_dimensions,
            cron_schedule=self.partition_cron_schedule, cron_fmt=self.partition_cron_fmt or "%Y-%m-%d",
            end_offset=self.partition_end_offset, timezone=self.partition_timezone,
        )

        _retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            _retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        _freshness_policy_obj = (
            _freshness_policy_compat(self.freshness_max_lag_minutes, self.freshness_cron)
            if self.freshness_max_lag_minutes is not None else None
        )

        _backfill_policy_obj = None
        if partitions_def is not None and self.backfill_policy == "single_run":
            _backfill_policy_obj = dg.BackfillPolicy.single_run()
        elif partitions_def is not None and self.backfill_policy == "multi_run":
            _backfill_policy_obj = dg.BackfillPolicy.multi_run(
                max_partitions_per_run=self.backfill_policy_max_partitions_per_run or 1
            )

        @dg.asset(
            key=asset_key,
            description=self.description or f"Run `dbt {command}` in {project_dir}.",
            group_name=self.group_name,
            deps=deps_keys,
            partitions_def=partitions_def,
            backfill_policy=_backfill_policy_obj,
            retry_policy=_retry_policy,
            freshness_policy=_freshness_policy_obj,
            owners=self.owners or None,
            tags=self.asset_tags or None,
            kinds=set(self.kinds) if self.kinds else {"dbt"},
            pool=self.pool,
            output_required=False,
        )
        def _dbt_command_asset(context: dg.AssetExecutionContext):
            import time

            if run_window and not _is_in_run_window(time.time(), run_window):
                context.log.info(f"run_window={run_window!r}: outside the configured window -- skipping (not failing).")
                return

            select = static_select
            resolved_vars = static_vars
            resolved_cwd = cwd
            if context.has_partition_key:
                pk = context.partition_key
                if select:
                    select = select.replace("{partition_key}", pk)
                if resolved_cwd:
                    resolved_cwd = resolved_cwd.replace("{partition_key}", pk)
                if resolved_vars:
                    resolved_vars = {
                        k: (v.replace("{partition_key}", pk) if isinstance(v, str) else v)
                        for k, v in resolved_vars.items()
                    }

            cmd = ["dbt", command, "--project-dir", project_dir]
            if profiles_dir:
                cmd += ["--profiles-dir", profiles_dir]
            if target:
                cmd += ["--target", target]
            if select:
                cmd += ["--select", select]
            if exclude:
                cmd += ["--exclude", exclude]
            if full_refresh:
                cmd.append("--full-refresh")
            if resolved_vars:
                cmd += ["--vars", json.dumps(resolved_vars)]
            cmd += extra_args

            if dry_run:
                context.log.info(f"[dry_run] would run: $ {' '.join(cmd)}")
                yield dg.MaterializeResult(metadata={"dry_run": dg.MetadataValue.bool(True), "command": dg.MetadataValue.text(" ".join(cmd))})
                return

            merged_env = {**os.environ, **env_vars}
            context.log.info(f"$ {' '.join(cmd)}")
            try:
                r = subprocess.run(
                    cmd, cwd=resolved_cwd, env=merged_env,
                    capture_output=True, text=True, timeout=timeout_seconds,
                )
            except subprocess.TimeoutExpired as e:
                raise dg.Failure(
                    description=f"dbt {command} timed out after {timeout_seconds}s.",
                    metadata={"stderr": dg.MetadataValue.text((e.stderr or "")[:2000] if e.stderr else "")},
                ) from e
            if r.stdout:
                context.log.info(r.stdout)
            if r.stderr:
                context.log.warning(r.stderr)

            metadata: Dict[str, Any] = {
                "exit_code": dg.MetadataValue.int(r.returncode),
                "dry_run": dg.MetadataValue.bool(False),
                "command": dg.MetadataValue.text(" ".join(cmd)),
                "stdout_preview": dg.MetadataValue.text((r.stdout or "")[:2000]),
            }
            if parse_results:
                summary = _parse_run_results(project_dir, target_path)
                if summary is not None:
                    metadata["dbt_node_count"] = dg.MetadataValue.int(summary["node_count"])
                    metadata["dbt_status_counts"] = dg.MetadataValue.json(summary["status_counts"])
                    if summary["elapsed_seconds"] is not None:
                        metadata["dbt_elapsed_seconds"] = dg.MetadataValue.float(summary["elapsed_seconds"])

            if r.returncode != 0:
                raise dg.Failure(
                    description=f"dbt {command} failed, exit={r.returncode}",
                    metadata=metadata,
                )
            yield dg.MaterializeResult(metadata=metadata)

        return dg.Definitions(assets=[_dbt_command_asset])
