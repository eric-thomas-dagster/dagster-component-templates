"""Terraform Asset Component.

Provisions infrastructure by running Terraform as the first step in a Dagster
asset graph. Downstream data assets declare `deps: [infrastructure]` and will
not run until Terraform has applied successfully.

Uses subprocess to call the Terraform CLI — no Python Terraform SDK required.
"""

import json
import os
import subprocess
from typing import Dict, List, Literal, Optional, Set, Union

import dagster as dg
from dagster import AssetExecutionContext, MaterializeResult
from pydantic import Field


def _run(cmd: list[str], cwd: str, extra_env: dict[str, str], log) -> subprocess.CompletedProcess:
    """Run a subprocess command, streaming logs and raising on failure."""
    env = {**os.environ, **extra_env}
    log.info(f"Running: {' '.join(cmd)}")
    result = subprocess.run(
        cmd,
        cwd=cwd,
        env=env,
        capture_output=True,
        text=True,
    )
    if result.stdout:
        log.info(result.stdout)
    if result.stderr:
        log.info(result.stderr)
    return result


def _run_checked(cmd: list[str], cwd: str, extra_env: dict[str, str], log) -> subprocess.CompletedProcess:
    """Run a subprocess command and raise on non-zero exit code."""
    result = _run(cmd, cwd, extra_env, log)
    if result.returncode != 0:
        raise Exception(
            f"Command failed (exit {result.returncode}): {' '.join(cmd)}\n"
            f"stderr: {result.stderr}\nstdout: {result.stdout}"
        )
    return result



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


class TerraformAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Provision infrastructure with Terraform as a Dagster asset.

    Runs `terraform init` (optional), `terraform plan`, and `terraform apply`
    (or `terraform destroy`) in sequence. Terraform outputs are parsed as JSON
    and surfaced as Dagster asset metadata, making them available to downstream
    assets.

    Designed to run as the *first step* in an asset graph so that downstream
    data assets can declare a dependency on provisioned infrastructure:

    ```yaml
    # downstream asset
    deps:
      - provision_data_infrastructure
    ```

    Example:
        ```yaml
        type: dagster_community_components.TerraformAssetComponent
        attributes:
          asset_name: provision_data_infrastructure
          working_dir: "{{ project_root }}/terraform/data_platform"
          workspace: production
          var_files:
            - vars/production.tfvars
          group_name: infrastructure
          description: Provision S3 buckets, IAM roles, and RDS instance before pipeline runs
        ```
    """

    asset_name: str = Field(description="Dagster asset name")
    working_dir: str = Field(description="Path to the Terraform root module directory")
    workspace: Optional[str] = Field(
        default=None,
        description="Terraform workspace to select before running. Uses the default workspace if None.",
    )
    var_files: Optional[list[str]] = Field(
        default=None,
        description="List of .tfvars files passed as -var-file arguments",
    )
    vars: Optional[dict[str, str]] = Field(
        default=None,
        description="Inline variable overrides passed as -var key=value",
    )
    targets: Optional[list[str]] = Field(
        default=None,
        description="Limit apply/plan scope to specific resources via -target",
    )
    terraform_bin: str = Field(
        default="terraform",
        description="Path to the terraform binary (default: 'terraform', assumed to be on PATH)",
    )
    init_on_run: bool = Field(
        default=True,
        description="Run 'terraform init -input=false' before plan/apply. Safe to always enable.",
    )
    plan_only: bool = Field(
        default=False,
        description="Only run 'terraform plan'; do not apply. Useful for CI drift detection.",
    )
    destroy: bool = Field(
        default=False,
        description="Run 'terraform destroy' instead of apply",
    )
    parallelism: int = Field(
        default=10,
        description="Number of concurrent operations (-parallelism flag)",
    )
    auto_approve: bool = Field(
        default=True,
        description="Pass -auto-approve to apply/destroy. Set False to require manual approval.",
    )
    env_vars: Optional[dict[str, str]] = Field(
        default=None,
        description="Extra environment variables for the Terraform subprocess (e.g. AWS_PROFILE)",
    )
    group_name: str = Field(
        default="infrastructure",
        description="Dagster asset group name",
    )
    description: Optional[str] = Field(
        default=None,
        description="Human-readable description of what this Terraform module provisions",
    )
    deps: Optional[list[str]] = Field(
        default=None,
        description="Upstream Dagster asset keys this asset depends on (e.g. ['raw_orders', 'schema/orders'])",
    )


    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners — team names ('team:analytics') or email addresses.",
    )

    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(
        default=None,
        description="Additional key-value tags applied to the asset in the Dagster catalog.",
    )

    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds for the catalog (e.g. ['snowflake', 'python']). Auto-inferred from component name when unset.",
    )

    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale. Builds a FreshnessPolicy when set.",
    )

    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5' (weekdays 9am).",
    )

    column_lineage: Optional[Dict[str, List[str]]] = Field(
        default=None,
        description="Column-level lineage, emitted as `dagster/column_lineage` metadata on every materialization: {output column: [upstream columns]}, e.g. {'revenue': ['price', 'quantity']}. The upstream asset is `upstream_asset_key`, or the only entry of `deps`; with several upstream assets, name each column '<asset_key>:<column>'. Ignored with a warning when no upstream asset can be resolved.",
    )

    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', or None for unpartitioned.",
    )

    partition_cron_schedule: Optional[str] = Field(
        default=None,
        description=(
            "Required when partition_type='cron'. A cron expression restricting the date axis to a "
            "custom cadence, e.g. '0 0 * * 1-5' for weekdays only (business-day cadence). Built on "
            "Dagster's TimeWindowPartitionsDefinition(cron_schedule=...)."
        ),
    )
    partition_cron_fmt: Optional[str] = Field(
        default="%Y-%m-%d",
        description="strftime format for partition keys produced by partition_type='cron'. Only change this for a sub-daily cron_schedule.",
    )

    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format (e.g. '2024-01-01'). Required for time-based partition types.",
    )

    partition_date_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Not used by this component, which has no input rows to filter. Use {partition_key}, {partition_date}, {partition_start} and {partition_end} in `workspace`, `vars` and `var_files` instead.",
    )

    partition_values: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning, e.g. 'acme,globex,initech'.",
    )

    partition_static_dim: Optional[str] = Field(
        default=None,
        description="Dimension name for the static axis in multi-partitioning, e.g. 'customer'.",
    )

    partition_static_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Not used by this component, which has no input rows to filter. Use {partition_key}, {partition_date}, {partition_start} and {partition_end} in `workspace`, `vars` and `var_files` instead.",
    )

    retry_policy_max_retries: Optional[int] = Field(
        default=None,
        description="Max retries on failure. Defines a RetryPolicy when set.",
    )

    retry_policy_delay_seconds: Optional[int] = Field(
        default=None,
        description="Seconds between retries (default 1).",
    )

    retry_policy_backoff: str = Field(
        default="exponential",
        description="Backoff strategy: 'linear' or 'exponential'.",
    )

    backfill_policy: Optional[Literal["single_run", "multi_run"]] = Field(
        default=None,
        description=(
            "Backfill execution strategy for a partitioned asset. 'single_run' "
            "fires ONE external call covering the whole backfill range -- the "
            "partition_key/partition_start/partition_end tokens this component "
            "already supports are comma-joined/range-spanning rather than a "
            "single key -- instead of one call per partition. 'multi_run' "
            "launches one run per partition (optionally capped via "
            "backfill_policy_max_partitions_per_run). Leave unset for Dagster's "
            "default (one run per partition, uncapped). Ignored when "
            "partition_type is not set."
        ),
    )
    backfill_policy_max_partitions_per_run: Optional[int] = Field(
        default=None,
        description="For backfill_policy='multi_run' only: maximum partitions materialized per run. Leave unset for Dagster's default (1 partition per run).",
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('deps', 'owners', 'kinds'), csv=('partition_values',), tags=('asset_tags',))

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        # freshness_max_lag_minutes / freshness_cron -> FreshnessPolicy (applied to the asset(s) below)
        # retry_policy_* settings -> RetryPolicy (applied to the asset(s) below)
        _column_lineage_md = _column_lineage_metadata(self)
        partitions_def = _build_partitions_def(
            self.partition_type,
            self.partition_start,
            self.partition_values,
            None,  # dynamic_partition_name: not a field of this component
            None,  # partition_dimensions: not a field of this component
        
    cron_schedule=self.partition_cron_schedule,
    cron_fmt=self.partition_cron_fmt or "%Y-%m-%d",
)
        partition_date_column = self.partition_date_column
        partition_static_column = self.partition_static_column
        partition_static_dim = self.partition_static_dim

        _retry_policy_obj = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
        
            _retry_policy_obj = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )
        _freshness_policy_obj = (
            _freshness_policy_compat(self.freshness_max_lag_minutes, self.freshness_cron)
            if self.freshness_max_lag_minutes is not None else None
        )
        _self = self

        backfill_policy = None
        if partitions_def is not None and self.backfill_policy == "single_run":
            backfill_policy = dg.BackfillPolicy.single_run()
        elif partitions_def is not None and self.backfill_policy == "multi_run":
            backfill_policy = dg.BackfillPolicy.multi_run(
                max_partitions_per_run=self.backfill_policy_max_partitions_per_run or 1
            )

        @dg.asset(partitions_def=partitions_def, retry_policy=_retry_policy_obj, freshness_policy=_freshness_policy_obj, backfill_policy=backfill_policy, 
            key=dg.AssetKey.from_user_string(self.asset_name),
            description=self.description or f"Provision infrastructure via Terraform in {self.working_dir}",
            group_name=self.group_name,
            kinds={"terraform"},
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def terraform_asset(context: AssetExecutionContext) -> MaterializeResult:
            if _column_lineage_md:
                context.add_output_metadata(_column_lineage_md)
            _tokens = _partition_vars(context)
            extra_env: dict[str, str] = {**(_self.env_vars or {}), **_partition_env(_tokens)}
            _workspace = _substitute_partition(_self.workspace, _tokens)
            tf = _self.terraform_bin
            cwd = _self.working_dir

            # --- 1. Workspace selection ---
            if _workspace:
                # Try to select; if the workspace doesn't exist, create it then select.
                select_result = _run(
                    [tf, "workspace", "select", _workspace],
                    cwd=cwd,
                    extra_env=extra_env,
                    log=context.log,
                )
                if select_result.returncode != 0:
                    context.log.info(
                        f"Workspace '{_workspace}' not found — creating it."
                    )
                    _run_checked(
                        [tf, "workspace", "new", _workspace],
                        cwd=cwd,
                        extra_env=extra_env,
                        log=context.log,
                    )

            # --- 2. Init ---
            if _self.init_on_run:
                _run_checked(
                    [tf, "init", "-input=false"],
                    cwd=cwd,
                    extra_env=extra_env,
                    log=context.log,
                )

            # --- 3. Build shared flags ---
            var_flags: list[str] = []
            for vf in _substitute_partition(_self.var_files or [], _tokens):
                var_flags += ["-var-file", vf]
            for k, v in _substitute_partition(_self.vars or {}, _tokens).items():
                var_flags += ["-var", f"{k}={v}"]

            target_flags: list[str] = []
            for t in (_self.targets or []):
                target_flags += ["-target", t]

            # --- 4. Plan ---
            plan_cmd = (
                [tf, "plan", "-out=tfplan", "-input=false", "-detailed-exitcode"]
                + [f"-parallelism={_self.parallelism}"]
                + var_flags
                + target_flags
            )
            plan_result = _run(
                plan_cmd,
                cwd=cwd,
                extra_env=extra_env,
                log=context.log,
            )

            # exit codes: 0 = no changes, 2 = changes present, 1 = error
            if plan_result.returncode == 1:
                raise Exception(
                    f"terraform plan failed.\nstderr: {plan_result.stderr}\nstdout: {plan_result.stdout}"
                )

            changes_present = plan_result.returncode == 2
            context.log.info(
                f"terraform plan exit code {plan_result.returncode} — "
                f"{'changes present' if changes_present else 'no changes'}"
            )

            # --- 5. Apply / Destroy ---
            if not _self.plan_only and changes_present:
                if _self.destroy:
                    destroy_cmd = [tf, "destroy", "-input=false", f"-parallelism={_self.parallelism}"]
                    if _self.auto_approve:
                        destroy_cmd.append("-auto-approve")
                    destroy_cmd += var_flags + target_flags
                    _run_checked(
                        destroy_cmd,
                        cwd=cwd,
                        extra_env=extra_env,
                        log=context.log,
                    )
                else:
                    _run_checked(
                        [tf, "apply", "tfplan"],
                        cwd=cwd,
                        extra_env=extra_env,
                        log=context.log,
                    )
            elif _self.plan_only:
                context.log.info("plan_only=True — skipping apply.")
            else:
                context.log.info("No changes detected — skipping apply.")

            # --- 6. Capture outputs ---
            outputs: dict = {}
            try:
                output_result = _run_checked(
                    [tf, "output", "-json"],
                    cwd=cwd,
                    extra_env=extra_env,
                    log=context.log,
                )
                raw_outputs = json.loads(output_result.stdout)
                # Flatten: extract the `value` field from each output object
                outputs = {k: v.get("value", v) for k, v in raw_outputs.items()}
            except Exception as exc:
                context.log.warning(f"Could not parse terraform outputs: {exc}")

            return MaterializeResult(
                metadata={
                    "outputs": dg.MetadataValue.json(outputs),
                    "working_dir": dg.MetadataValue.text(cwd),
                    "workspace": dg.MetadataValue.text(_workspace or "default"),
                    "plan_only": dg.MetadataValue.bool(_self.plan_only),
                    "changes_applied": dg.MetadataValue.bool(
                        not _self.plan_only and changes_present
                    ),
                }
            )

        return dg.Definitions(assets=[terraform_asset])


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


# BEGIN partitions-def builder (inlined per component; keep copies identical, see tests/test_partition_port.py)
def _build_partitions_def(
    partition_type,
    partition_start,
    partition_values,
    dynamic_partition_name,
    partition_dimensions,
    cron_schedule=None,
    cron_fmt="%Y-%m-%d",
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

    # Both shapes set: ambiguous. Pick one.
    if partition_dimensions and partition_type:
        raise ValueError(
            "Set either partition_type (flat-fields shape) or "
            "partition_dimensions (multi-axis shape), not both."
        )

    def _build_axis(spec):
        t = spec.get("type")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily":
            return DailyPartitionsDefinition(start_date=spec["start"])
        if t == "weekly":
            return WeeklyPartitionsDefinition(start_date=spec["start"])
        if t == "monthly":
            return MonthlyPartitionsDefinition(start_date=spec["start"])
        if t == "hourly":
            return HourlyPartitionsDefinition(start_date=spec["start"])
        if t == "cron":
            from dagster import TimeWindowPartitionsDefinition
            if not spec.get("start"):
                raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
            if not spec.get("cron_schedule"):
                raise ValueError("partition dimension type='cron' requires 'cron_schedule'")
            return TimeWindowPartitionsDefinition(
                start=spec["start"], fmt=spec.get("fmt") or "%Y-%m-%d", cron_schedule=spec["cron_schedule"],
            )
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
        return DailyPartitionsDefinition(start_date=partition_start)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "cron":
        from dagster import TimeWindowPartitionsDefinition
        if not cron_schedule:
            raise ValueError(
                "partition_type='cron' requires cron_schedule, e.g. '0 0 * * 1-5' for weekdays only."
            )
        return TimeWindowPartitionsDefinition(start=partition_start, fmt=cron_fmt, cron_schedule=cron_schedule)
    if partition_type == "static":
        if not _values:
            raise ValueError("partition_type='static' requires partition_values (comma-separated).")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError(
                "partition_type='dynamic' requires dynamic_partition_name."
            )
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    if partition_type == "multi":
        if not _values:
            raise ValueError("partition_type='multi' requires partition_values (comma-separated).")
        if not partition_start:
            raise ValueError("partition_type='multi' requires partition_start (the date axis start).")
        return MultiPartitionsDefinition({
            "date": DailyPartitionsDefinition(start_date=partition_start),
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")
# END partitions-def builder


# BEGIN partition-vars helper (inlined per component; keep copies identical, see tests/test_partition_vars.py)
def _partition_vars(context):
    """Tokens a partitioned run can use in its variables: {partition_key}, and for time partitions {partition_date}
    (YYYY-MM-DD), {partition_start} and {partition_end} (ISO datetimes); for a multi-partition also
    {partition_<dimension>}. Empty for an unpartitioned run."""
    import re as _re

    if not (context.has_partition_key or context.has_partition_key_range):
        return {}
    keys = list(context.partition_keys)
    tokens = {"partition_key": ",".join(str(k) for k in keys)}
    try:
        window = context.partition_time_window
        tokens["partition_start"] = window.start.strftime("%Y-%m-%dT%H:%M:%S")
        tokens["partition_end"] = window.end.strftime("%Y-%m-%dT%H:%M:%S")
        tokens["partition_date"] = window.start.strftime("%Y-%m-%d")
    except Exception:
        pass  # static / dynamic partitions have no time window
    if len(keys) == 1 and hasattr(keys[0], "keys_by_dimension"):
        for dimension, value in keys[0].keys_by_dimension.items():
            tokens["partition_" + _re.sub(r"\W", "_", str(dimension))] = str(value)
    return tokens


def _partition_env(tokens):
    """DAGSTER_PARTITION_* environment variables for a partitioned run (empty when unpartitioned)."""
    if not tokens:
        return {}
    env = {"DAGSTER_PARTITION_KEY": tokens["partition_key"]}
    for token, name in (("partition_date", "DAGSTER_PARTITION_DATE"), ("partition_start", "DAGSTER_PARTITION_START"),
                        ("partition_end", "DAGSTER_PARTITION_END")):
        if token in tokens:
            env[name] = tokens[token]
    return env


def _substitute_partition(value, tokens):
    """Replace {partition_*} tokens inside strings, recursively through dicts and lists. Only known tokens are
    replaced, so other braces (JSON, shell, templates) are left alone. A no-op for an unpartitioned run."""
    import re as _re

    if not tokens:
        return value
    if isinstance(value, str):
        return _re.sub(r"\{(partition_\w+)\}", lambda m: tokens.get(m.group(1), m.group(0)), value)
    if isinstance(value, dict):
        return {k: _substitute_partition(v, tokens) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return type(value)(_substitute_partition(v, tokens) for v in value)
    return value
# END partition-vars helper

# BEGIN column-lineage helper (inlined per component; keep copies identical, see tests/test_column_lineage.py)
def _column_lineage_metadata(component):
    """`dagster/column_lineage` metadata from the component's `column_lineage` mapping ({output column: [upstream columns]}).

    The upstream asset is `upstream_asset_key`, or the only entry of `deps`. With several upstream assets, name each
    column as "<asset_key>:<column>". Returns {} when the mapping is unset, or (with a warning) when no upstream asset
    can be resolved, since lineage needs an upstream asset to point at."""
    import warnings

    import dagster as _dg

    mapping = getattr(component, "column_lineage", None)
    if not mapping:
        return {}
    ups = getattr(component, "upstream_asset_key", None) or getattr(component, "deps", None) or []
    ups = [ups] if isinstance(ups, (str, _dg.AssetKey)) else list(ups)
    ups = [u.to_user_string() if isinstance(u, _dg.AssetKey) else str(u) for u in ups]
    deps = {}
    for out_col, in_cols in mapping.items():
        refs = []
        for ic in [in_cols] if isinstance(in_cols, str) else in_cols:
            key, _, col = str(ic).rpartition(":")
            if key in ups and col:
                refs.append(_dg.TableColumnDep(asset_key=_dg.AssetKey.from_user_string(key), column_name=col))
            elif len(ups) == 1:
                refs.append(_dg.TableColumnDep(asset_key=_dg.AssetKey.from_user_string(ups[0]), column_name=str(ic)))
            else:
                warnings.warn(
                    f"column_lineage ignored: cannot tell which upstream asset {ic!r} belongs to "
                    f"(upstream assets: {ups or 'none'}; use '<asset_key>:<column>' with several upstream assets)"
                )
                return {}
        deps[str(out_col)] = refs
    return {"dagster/column_lineage": _dg.MetadataValue.column_lineage(_dg.TableColumnLineage(deps))}
# END column-lineage helper
