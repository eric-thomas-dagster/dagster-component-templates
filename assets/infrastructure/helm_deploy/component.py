"""Helm Deploy Asset Component.

Deploys or upgrades a Helm chart as the first step in a Dagster asset graph.
Downstream assets declare `deps: [asset_name]` and will not run until the
release has been deployed and all pods are ready.

Uses subprocess to call the Helm CLI — no Python Helm SDK required.

Typical use case: deploy an ML model serving container before running
inference assets, or provision a database Helm chart before data loading assets.
"""

import json
import os
import subprocess
from typing import Dict, List, Literal, Optional, Union

import dagster as dg
from dagster import AssetExecutionContext, MaterializeResult
from pydantic import Field


def _run(cmd: list[str], extra_env: dict[str, str], log) -> subprocess.CompletedProcess:
    """Run a subprocess command, streaming logs."""
    env = {**os.environ, **extra_env}
    log.info(f"Running: {' '.join(cmd)}")
    result = subprocess.run(
        cmd,
        env=env,
        capture_output=True,
        text=True,
    )
    if result.stdout:
        log.info(result.stdout)
    if result.stderr:
        log.info(result.stderr)
    return result


def _run_checked(cmd: list[str], extra_env: dict[str, str], log) -> subprocess.CompletedProcess:
    """Run a subprocess command and raise on non-zero exit code."""
    result = _run(cmd, extra_env, log)
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


class HelmDeployComponent(dg.Component, dg.Model, dg.Resolvable):
    """Deploy or upgrade a Helm chart as a Dagster asset.

    Runs `helm upgrade --install` against a chart (from a repository, local
    path, or OCI registry) and surfaces the release status as a Dagster asset
    materialization. Release metadata — chart version, app version, deployed
    namespace, and release status — are recorded as asset metadata.

    Designed to run as the *first step* in an asset graph so that downstream
    assets can declare a dependency on a live Kubernetes workload:

    ```yaml
    # downstream inference asset
    deps:
      - deploy_ml_serving
    ```

    Example:
        ```yaml
        type: dagster_community_components.HelmDeployComponent
        attributes:
          asset_name: deploy_ml_serving
          release_name: ml-serving
          chart: my-org/ml-serving
          repo_url: https://charts.my-company.com
          repo_name: my-org
          namespace: ml-serving
          set_values:
            image.tag: "latest"
            replicas: "3"
          group_name: infrastructure
          description: Deploy ML model serving before inference pipeline runs
        ```
    """

    asset_name: str = Field(description="Dagster asset name")
    release_name: str = Field(description="Helm release name")
    chart: str = Field(
        description=(
            "Chart reference: repository alias + name (e.g. 'my-org/ml-serving'), "
            "local path (e.g. './charts/ml-serving'), "
            "or OCI reference (e.g. 'oci://registry.example.com/charts/ml-serving')."
        )
    )
    namespace: str = Field(
        default="default",
        description="Kubernetes namespace to deploy into.",
    )
    create_namespace: bool = Field(
        default=True,
        description="Create the namespace if it does not already exist (--create-namespace).",
    )
    values_files: Optional[list[str]] = Field(
        default=None,
        description="List of values.yaml files passed as -f to helm upgrade.",
    )
    set_values: Optional[dict[str, str]] = Field(
        default=None,
        description="Key-value pairs passed as --set. Values are coerced to their YAML type by Helm.",
    )
    set_string_values: Optional[dict[str, str]] = Field(
        default=None,
        description="Key-value pairs passed as --set-string. Values are always treated as strings.",
    )
    version: Optional[str] = Field(
        default=None,
        description="Pin a specific chart version (--version). Omit to use the latest.",
    )
    repo_url: Optional[str] = Field(
        default=None,
        description=(
            "Helm repository URL to add before installing "
            "(e.g. https://charts.my-company.com). "
            "Required when using a repo alias in `chart` and the repo is not already added."
        ),
    )
    repo_name: Optional[str] = Field(
        default=None,
        description=(
            "Name to register the repository as (e.g. 'my-org'). "
            "Must match the alias prefix in `chart` when using a repository reference."
        ),
    )
    kube_context: Optional[str] = Field(
        default=None,
        description="Kubernetes context to use (--kube-context). Uses the current context if omitted.",
    )
    kubeconfig: Optional[str] = Field(
        default=None,
        description="Path to a kubeconfig file (--kubeconfig). Uses ~/.kube/config if omitted.",
    )
    atomic: bool = Field(
        default=True,
        description=(
            "Roll back automatically on failure (--atomic). "
            "Ensures the release never gets stuck in a broken state."
        ),
    )
    wait: bool = Field(
        default=True,
        description="Wait for all pods and resources to be ready before considering the deploy successful (--wait).",
    )
    timeout: str = Field(
        default="10m",
        description="Maximum time to wait for resources to become ready (--timeout). Format: '10m', '300s', etc.",
    )
    dry_run: bool = Field(
        default=False,
        description="Perform a dry run — render templates and validate but do not apply (--dry-run).",
    )
    helm_bin: str = Field(
        default="helm",
        description="Path to the helm binary (default: 'helm', assumed to be on PATH).",
    )
    env_vars: Optional[dict[str, str]] = Field(
        default=None,
        description=(
            "Extra environment variables for the Helm subprocess "
            "(e.g. KUBECONFIG, HELM_KUBEAPISERVER, cloud provider auth vars)."
        ),
    )
    group_name: str = Field(
        default="infrastructure",
        description="Dagster asset group name.",
    )
    description: Optional[str] = Field(
        default=None,
        description="Human-readable description of what this Helm release deploys.",
    )
    deps: Optional[list[str]] = Field(
        default=None,
        description="Upstream Dagster asset keys this asset depends on (e.g. ['raw_orders', 'schema/orders']).",
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
    partition_end_offset: int = Field(
        default=0,
        description=(
            "Passed through to the underlying Daily/Weekly/Monthly/Hourly/cron "
            "partitions definition. 0 (the Dagster default) means a time-based "
            "partition set only ever holds COMPLETED windows -- e.g. a daily set "
            "never includes today until tomorrow. Set to 1 to include the "
            "current, still-in-progress window."
        ),
    )
    partition_timezone: Optional[str] = Field(
        default=None,
        description="IANA timezone for the underlying Daily/Weekly/Monthly/Hourly/cron partitions definition (defaults to UTC when unset, matching Dagster's own default).",
    )

    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format (e.g. '2024-01-01'). Required for time-based partition types.",
    )

    partition_date_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Not used by this component, which has no input rows to filter. Use {partition_key}, {partition_date}, {partition_start} and {partition_end} in `release_name`, `namespace`, `set_values` and `set_string_values` instead.",
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
        description="Not used by this component, which has no input rows to filter. Use {partition_key}, {partition_date}, {partition_start} and {partition_end} in `release_name`, `namespace`, `set_values` and `set_string_values` instead.",
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
    cron_fmt=self.partition_cron_fmt or "%Y-%m-%d", end_offset=self.partition_end_offset, timezone=self.partition_timezone,
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
            description=self.description or f"Deploy Helm release {self.release_name} ({self.chart})",
            group_name=self.group_name,
            kinds={"helm", "kubernetes"},
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def helm_deploy(context: AssetExecutionContext) -> MaterializeResult:
            if _column_lineage_md:
                context.add_output_metadata(_column_lineage_md)
            _tokens = _partition_vars(context)
            extra_env: dict[str, str] = {**(_self.env_vars or {}), **_partition_env(_tokens)}
            _release_name = _substitute_partition(_self.release_name, _tokens)
            _namespace = _substitute_partition(_self.namespace, _tokens)
            helm = _self.helm_bin

            # Shared kubeconfig/context flags used across all helm commands
            kube_flags: list[str] = []
            if _self.kube_context:
                kube_flags += ["--kube-context", _self.kube_context]
            if _self.kubeconfig:
                kube_flags += ["--kubeconfig", _self.kubeconfig]

            # --- 1. Add Helm repository (if configured) ---
            if _self.repo_url and _self.repo_name:
                _run_checked(
                    [helm, "repo", "add", _self.repo_name, _self.repo_url],
                    extra_env=extra_env,
                    log=context.log,
                )
                _run_checked(
                    [helm, "repo", "update"],
                    extra_env=extra_env,
                    log=context.log,
                )

            # --- 2. Build upgrade flags ---
            upgrade_flags: list[str] = ["--output", "json"]

            if _self.create_namespace:
                upgrade_flags.append("--create-namespace")
            if _self.atomic:
                upgrade_flags.append("--atomic")
            if _self.wait:
                upgrade_flags.append("--wait")
            if _self.dry_run:
                upgrade_flags.append("--dry-run")
            if _self.version:
                upgrade_flags += ["--version", _self.version]

            upgrade_flags += ["--timeout", _self.timeout]

            for vf in (_self.values_files or []):
                upgrade_flags += ["-f", vf]
            for k, v in _substitute_partition(_self.set_values or {}, _tokens).items():
                upgrade_flags += ["--set", f"{k}={v}"]
            for k, v in _substitute_partition(_self.set_string_values or {}, _tokens).items():
                upgrade_flags += ["--set-string", f"{k}={v}"]

            # --- 3. Run helm upgrade --install ---
            upgrade_cmd = (
                [
                    helm, "upgrade", "--install",
                    _release_name, _self.chart,
                    "--namespace", _namespace,
                ]
                + upgrade_flags
                + kube_flags
            )

            upgrade_result = _run_checked(
                upgrade_cmd,
                extra_env=extra_env,
                log=context.log,
            )

            # --- 4. Parse upgrade JSON output ---
            release_info: dict = {}
            chart_version = "unknown"
            app_version = "unknown"
            deploy_status = "unknown"

            try:
                release_info = json.loads(upgrade_result.stdout)
                chart_meta = release_info.get("chart", {}).get("metadata", {})
                chart_version = chart_meta.get("version", "unknown")
                app_version = chart_meta.get("appVersion", "unknown")
                deploy_status = release_info.get("info", {}).get("status", "unknown")
            except (json.JSONDecodeError, AttributeError):
                context.log.warning(
                    "Could not parse helm upgrade JSON output. Metadata will be minimal."
                )

            # --- 5. Fetch deployed resources via helm status ---
            resources: dict = {}
            try:
                status_result = _run_checked(
                    [
                        helm, "status", _release_name,
                        "-n", _namespace,
                        "--output", "json",
                    ] + kube_flags,
                    extra_env=extra_env,
                    log=context.log,
                )
                status_data = json.loads(status_result.stdout)
                deploy_status = status_data.get("info", {}).get("status", deploy_status)
                # Resource summary is nested under info.resources in newer Helm versions
                resources = status_data.get("info", {}).get("resources", {})
            except Exception as exc:
                context.log.warning(f"Could not fetch helm status: {exc}")

            # --- 6. Return metadata ---
            return MaterializeResult(
                metadata={
                    "release": dg.MetadataValue.text(_release_name),
                    "chart": dg.MetadataValue.text(_self.chart),
                    "chart_version": dg.MetadataValue.text(chart_version),
                    "app_version": dg.MetadataValue.text(app_version),
                    "namespace": dg.MetadataValue.text(_namespace),
                    "status": dg.MetadataValue.text(deploy_status),
                    "dry_run": dg.MetadataValue.bool(_self.dry_run),
                    "resources": dg.MetadataValue.json(resources),
                }
            )

        return dg.Definitions(assets=[helm_deploy])


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

    # Both shapes set: ambiguous. Pick one.
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
            "date": DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone),
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
