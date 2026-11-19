"""ShellCommandAssetComponent.

Run a shell command (or multiline script) as a Dagster asset -- the Airflow `BashOperator` /
`@task.bash` equivalent. Runs via `subprocess.Popen(cmd, shell=True, executable="/bin/bash")`
(NOT `dagster-shell`'s `execute_shell_command` -- an earlier version of this docstring claimed
that; the code has always used subprocess directly, and `dagster-shell`'s last release pins
`dagster==1.9.13`, so it's not even installable alongside a current dagster). The command runs
with `bash -c` specifically (not the OS default `/bin/sh`, which on many Linux distros is
`dash` and rejects bashisms like `[[ ]]`/`${var:-default}`), matching Airflow's `BashOperator`
default executable. Captured stdout/stderr in the run log, exit code -> asset success/failure,
optional cwd + env vars (merged with the parent process's own environment, same as Airflow).
"""
from typing import Any, Dict, List, Literal, Optional, Union

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


class ShellCommandAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Run a shell command as a Dagster asset via `subprocess.Popen(..., shell=True, executable="/bin/bash")`
    -- the Airflow `BashOperator` / `@task.bash` equivalent."""

    asset_name: str = Field(description="Dagster asset name.")
    command: Optional[str] = Field(
        default=None,
        description=(
            "Shell command (or multiline script) to execute, run via `bash -c`. Supports "
            "`{partition_key}` when `partition_type` is set. Required unless `command_asset_key` "
            "is set (in which case the upstream asset's value IS the command, Airflow "
            "`@task.bash`-style -- `command` is then a fallback used only if that upstream value "
            "is falsy)."
        ),
    )
    command_asset_key: Optional[str] = Field(
        default=None,
        description=(
            "Upstream asset key whose materialized value becomes the command string to run -- "
            "mirrors Airflow's `@task.bash`, where the decorated Python callable's return value "
            "IS the command. Adds that asset as a dependency automatically."
        ),
    )
    cwd: Optional[str] = Field(default=None, description="Working directory for the command.")
    env_vars: Optional[Dict[str, str]] = Field(default=None, description="Additional env vars to pass to the shell (merged with the parent process's own environment).")
    skip_on_exit_code: Optional[int] = Field(
        default=None,
        description=(
            "Exit code treated as a no-op success instead of a failure (Airflow's "
            "BashOperator default is 99 -- not defaulted here since this component predates "
            "this field and existing YAML may rely on every nonzero exit code failing the "
            "run; set explicitly to opt in, e.g. 99 for Airflow parity). The asset still "
            "materializes successfully; metadata records `skipped: true` and the exit code."
        ),
    )
    timeout_seconds: Optional[int] = Field(
        default=None,
        description="Kill the command and fail the run if it runs longer than this many seconds. Unset: no timeout.",
    )
    success_exit_codes: Optional[List[int]] = Field(
        default=None,
        description=(
            "Additional exit codes treated as a normal, successful materialization -- unlike "
            "skip_on_exit_code, no 'skipped' metadata marker, just success. For legacy scripts "
            "that exit nonzero on 'nothing to do' and should count as success, not a no-op. "
            "Checked before skip_on_exit_code and before the exit-code!=0 failure; 0 is always "
            "success and never needs to be listed here."
        ),
    )
    dry_run: bool = Field(
        default=False,
        description="If True, log the fully-resolved command (after {partition_key} substitution) and skip actually running it -- no subprocess is spawned. Output metadata always includes dry_run so a materialization history shows which runs were real.",
    )
    pool: Optional[str] = Field(
        default=None,
        description=(
            "Dagster's native named concurrency pool (e.g. Airflow's `pool` -- a fixed "
            "number of slots shared across every asset/op using the same pool name). The "
            "pool's own slot count is configured at the deployment level (dagster.yaml's "
            "concurrency: pools:, or the Dagster+ UI), not here."
        ),
    )
    return_last_stdout_line: bool = Field(
        default=False,
        description="Return the command's last non-empty stdout line as this asset's materialized value (via the IO manager), not just metadata, so a downstream asset can take it as an input the usual way. Mirrors what a source system's last line of output is read as by its own downstream steps. Off by default.",
    )
    parse_json_output: bool = Field(
        default=False,
        description="Only with return_last_stdout_line: if that last line parses as valid JSON, return the parsed value instead of the raw string. Falls back to the raw string when it isn't valid JSON.",
    )
    group_name: str = Field(default="shell", description="Asset group.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    deps: Optional[List[str]] = Field(default=None, description="Upstream Dagster asset keys for lineage (in addition to command_asset_key, which is added automatically).")

    partition_type: Optional[str] = Field(default=None, description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'cron', 'static', 'dynamic', 'multi', or None for unpartitioned. With a partition type set, `{partition_key}` is substituted into `command`/`cwd`.")
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
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds for the catalog. Auto-inferred from component name when unset.")
    freshness_max_lag_minutes: Optional[int] = Field(default=None, description="Maximum acceptable lag in minutes before the asset is considered stale.")
    freshness_cron: Optional[str] = Field(default=None, description="Cron schedule string for the freshness policy.")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('deps', 'owners', 'kinds'), csv=('partition_values',), tags=('asset_tags',))

    @model_validator(mode="after")
    def _validate_command_source(self):
        if not self.command and not self.command_asset_key:
            raise ValueError("ShellCommandAssetComponent: set `command` or `command_asset_key` (or both).")
        return self

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='Shell Command Asset', editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        component = self
        static_cmd = self.command
        cwd = self.cwd
        env_vars = self.env_vars or {}
        skip_on_exit_code = self.skip_on_exit_code
        success_exit_codes = self.success_exit_codes
        dry_run = self.dry_run
        timeout_seconds = self.timeout_seconds
        return_last_stdout_line = self.return_last_stdout_line
        parse_json_output = self.parse_json_output
        deps_keys = [dg.AssetKey.from_user_string(k) for k in (self.deps or [])]

        ins = None
        if self.command_asset_key:
            ins = {"_command_upstream": dg.AssetIn(key=dg.AssetKey.from_user_string(self.command_asset_key))}

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
            key=dg.AssetKey.from_user_string(self.asset_name),
            ins=ins,
            description=self.description or (f"Run shell: {static_cmd[:60]}..." if static_cmd else "Run shell command from upstream asset"),
            group_name=self.group_name,
            deps=deps_keys,
            partitions_def=partitions_def,
            backfill_policy=_backfill_policy_obj,
            retry_policy=_retry_policy,
            freshness_policy=_freshness_policy_obj,
            owners=self.owners or [],
            tags=self.asset_tags or None,
            kinds=set(self.kinds) if self.kinds else None,
            pool=self.pool,
        )
        def _shell_asset(context: dg.AssetExecutionContext, **upstream_kwargs):
            import json
            import os
            import subprocess
            import sys
            import threading

            upstream_cmd = upstream_kwargs.get("_command_upstream")
            cmd = str(upstream_cmd) if upstream_cmd else static_cmd
            if not cmd:
                raise dg.Failure(description="command_asset_key's upstream value was empty/falsy and no fallback `command` was set.")
            if context.has_partition_key:
                cmd = cmd.replace("{partition_key}", context.partition_key)
                if cwd:
                    cwd_resolved = cwd.replace("{partition_key}", context.partition_key)
                else:
                    cwd_resolved = cwd
            else:
                cwd_resolved = cwd

            if dry_run:
                context.log.info(f"[dry_run] would run: $ {cmd}")
                metadata = {"dry_run": dg.MetadataValue.bool(True)}
                if return_last_stdout_line:
                    return dg.Output(None, metadata=metadata)
                return dg.MaterializeResult(metadata=metadata)

            merged_env = {**os.environ, **env_vars}
            print(f"$ {cmd}", flush=True)
            # executable="/bin/bash" (not the shell=True default of /bin/sh, which on many Linux
            # distros is dash and rejects bashisms like [[ ]] / ${var:-default}) -- matches
            # Airflow's BashOperator default executable.
            proc = subprocess.Popen(
                cmd, shell=True, cwd=cwd_resolved, env=merged_env, executable="/bin/bash",
                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                text=True, bufsize=1,
            )

            timed_out = {"flag": False}
            timer = None
            if timeout_seconds:
                def _kill():
                    timed_out["flag"] = True
                    proc.kill()
                timer = threading.Timer(timeout_seconds, _kill)
                timer.start()

            try:
                # Stream stdout/stderr through the parent process so the
                # ComputeLogManager captures them. Do NOT `capture_output=True` here
                # — that swallows the streams and starves the CLM.
                stdout_lines = []
                for line in proc.stdout or []:
                    sys.stdout.write(line)
                    sys.stdout.flush()
                    stdout_lines.append(line)
                stderr_data = (proc.stderr.read() if proc.stderr else "") or ""
                if stderr_data:
                    sys.stderr.write(stderr_data)
                    sys.stderr.flush()
                proc.wait()
            finally:
                if timer:
                    timer.cancel()

            if timed_out["flag"]:
                raise dg.Failure(
                    description=f"Shell command timed out after {timeout_seconds}s and was killed.",
                    metadata={"stderr": dg.MetadataValue.text(stderr_data[:2000])},
                )
            def _last_line_value():
                last_line = next((l.rstrip("\n") for l in reversed(stdout_lines) if l.strip()), None)
                if last_line is None:
                    return None
                if parse_json_output:
                    try:
                        return json.loads(last_line)
                    except (TypeError, ValueError):
                        pass
                return last_line

            if skip_on_exit_code is not None and proc.returncode == skip_on_exit_code:
                metadata = {
                    "exit_code": dg.MetadataValue.int(proc.returncode),
                    "skipped": dg.MetadataValue.bool(True),
                    "dry_run": dg.MetadataValue.bool(False),
                    "stdout_preview": dg.MetadataValue.text("".join(stdout_lines)[:2000]),
                }
                if return_last_stdout_line:
                    return dg.Output(_last_line_value(), metadata=metadata)
                return dg.MaterializeResult(metadata=metadata)
            if proc.returncode != 0 and not (success_exit_codes and proc.returncode in success_exit_codes):
                raise dg.Failure(
                    description=f"Shell command exited {proc.returncode}",
                    metadata={"stderr": dg.MetadataValue.text(stderr_data[:2000])},
                )
            metadata = {
                "exit_code": dg.MetadataValue.int(proc.returncode),
                "dry_run": dg.MetadataValue.bool(False),
                "stdout_preview": dg.MetadataValue.text("".join(stdout_lines)[:2000]),
            }
            if return_last_stdout_line:
                return dg.Output(_last_line_value(), metadata=metadata)
            return dg.MaterializeResult(metadata=metadata)
        return dg.Definitions(assets=[_shell_asset])
