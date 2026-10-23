"""FlinkJobRunAssetComponent.

Submits (and optionally polls) a job on a self-hosted/standalone Apache Flink
cluster via its real JobManager REST API -- the generic REST API every Flink
deployment exposes (standalone, Kubernetes, YARN session, etc.), as distinct
from AWS's managed runtime (`managed_flink_application_asset`, which talks to
`kinesisanalyticsv2` instead -- a different control plane entirely, since a
self-managed cluster has no AWS API to call).

Request/response shapes verified directly against the official Flink REST API
reference (nightlies.apache.org/flink/flink-docs-stable/docs/ops/rest_api/),
not from memory:
  - `POST /jars/upload` -- multipart `jarfile`, response `{filename, status}`.
    The returned `filename` (not the local path) is the real jar id used below.
  - `POST /jars/{jarid}/run` -- JSON body `entryClass`/`programArgsList`/
    `parallelism`/`savepointPath`/`allowNonRestoredState`/`flinkConfiguration`;
    response `{jobid, applicationId}`.
  - `GET /jobs/{jobid}` -- `state` one of INITIALIZING | CREATED | RUNNING |
    FAILING | FAILED | CANCELLING | CANCELED | FINISHED | RESTARTING |
    SUSPENDED | RECONCILING.
  - `PATCH /jobs/{jobid}?mode=cancel` -- 202, `{}`.
"""
from typing import Any, Dict, List, Optional, Union

import dagster as dg
from pydantic import Field, model_validator

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


_TERMINAL_STATES = {"FINISHED", "CANCELED", "FAILED"}
_FAILURE_STATES = {"FAILED", "CANCELED"}


def _upload_jar(jobmanager_url: str, jar_path: str, headers: Dict[str, str], request_timeout_seconds: int) -> str:
    import requests

    with open(jar_path, "rb") as f:
        resp = requests.post(
            f"{jobmanager_url}/jars/upload",
            files={"jarfile": (jar_path.rsplit("/", 1)[-1], f, "application/x-java-archive")},
            headers=headers,
            timeout=request_timeout_seconds,
        )
    if resp.status_code >= 400:
        raise RuntimeError(f"Flink jar upload failed: HTTP {resp.status_code} {resp.text[:500]!r}")
    body = resp.json()
    if body.get("status") != "success":
        raise RuntimeError(f"Flink jar upload returned unexpected shape: {body!r}")
    # The real jar id to run is the server-assigned filename, not the local path.
    return body["filename"].rsplit("/", 1)[-1]


def _run_jar(
    jobmanager_url: str,
    jar_id: str,
    entry_class: Optional[str],
    program_args: Optional[List[str]],
    parallelism: Optional[int],
    savepoint_path: Optional[str],
    allow_non_restored_state: bool,
    flink_configuration: Optional[Dict[str, str]],
    headers: Dict[str, str],
    request_timeout_seconds: int,
) -> str:
    import requests

    body: Dict[str, Any] = {}
    if entry_class:
        body["entryClass"] = entry_class
    if program_args:
        body["programArgsList"] = list(program_args)
    if parallelism is not None:
        body["parallelism"] = parallelism
    if savepoint_path:
        body["savepointPath"] = savepoint_path
    if allow_non_restored_state:
        body["allowNonRestoredState"] = True
    if flink_configuration:
        body["flinkConfiguration"] = dict(flink_configuration)

    resp = requests.post(
        f"{jobmanager_url}/jars/{jar_id}/run", json=body, headers=headers, timeout=request_timeout_seconds,
    )
    if resp.status_code >= 400:
        raise RuntimeError(f"Flink job run failed: HTTP {resp.status_code} {resp.text[:500]!r}")
    return resp.json()["jobid"]


def _get_job_state(jobmanager_url: str, job_id: str, headers: Dict[str, str], request_timeout_seconds: int) -> str:
    import requests

    resp = requests.get(f"{jobmanager_url}/jobs/{job_id}", headers=headers, timeout=request_timeout_seconds)
    if resp.status_code >= 400:
        raise RuntimeError(f"Flink job status poll failed: HTTP {resp.status_code} {resp.text[:500]!r}")
    return resp.json()["state"]


def _poll_until(
    jobmanager_url: str, job_id: str, target_states: set, log, headers: Dict[str, str],
    poll_interval_seconds: float, timeout_seconds: int, request_timeout_seconds: int,
) -> str:
    import time
    deadline = time.monotonic() + timeout_seconds
    state = _get_job_state(jobmanager_url, job_id, headers, request_timeout_seconds)
    while state not in target_states:
        if state in _FAILURE_STATES:
            return state  # terminal failure state the caller didn't ask to wait for -- stop polling, let it decide
        if time.monotonic() >= deadline:
            raise TimeoutError(f"Flink job {job_id!r} did not reach {target_states} within {timeout_seconds}s (last state={state!r}).")
        log.info(f"flink_job_run_asset: job {job_id!r} state={state!r}, waiting for {target_states}")
        time.sleep(poll_interval_seconds)
        state = _get_job_state(jobmanager_url, job_id, headers, request_timeout_seconds)
    return state


class FlinkJobRunAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Submit (and optionally wait on) a job on a self-hosted Apache Flink cluster.

    Talks to the real Flink JobManager REST API directly -- works against any
    standalone/Kubernetes/YARN-session Flink deployment that exposes it, not
    just AWS's managed runtime (see `managed_flink_application_asset` for
    that -- a different control plane, `kinesisanalyticsv2`, with no
    JobManager REST API of its own).

    `wait_mode` controls what "materializing" means for this job:
      - `until_finished` (default): for a **batch** job -- poll until FINISHED
        / CANCELED / FAILED.
      - `until_running`: for a **streaming** job that's meant to keep running
        forever -- poll only until it reaches RUNNING, then return; the job
        keeps running in the cluster after the asset materializes.
      - `fire_and_forget`: submit and return immediately, no polling at all.
    """

    asset_name: str = Field(description="Dagster asset key.")
    jobmanager_url: str = Field(description="Base URL of the Flink JobManager REST API, e.g. 'http://flink-jobmanager:8081'.")
    jar_path: Optional[str] = Field(default=None, description="Local path to a jar to upload before running. Mutually exclusive with jar_id.")
    jar_id: Optional[str] = Field(default=None, description="Id of a jar already uploaded to the cluster (the filename POST /jars/upload returned). Mutually exclusive with jar_path.")
    entry_class: Optional[str] = Field(default=None, description="Fully qualified entry-point class. Omit to use the jar's manifest Main-Class.")
    program_args: Optional[List[str]] = Field(default=None, description="Arguments passed to the job's main method.")
    parallelism: Optional[int] = Field(default=None, description="Job parallelism. Omit to use the cluster default.")
    savepoint_path: Optional[str] = Field(default=None, description="Restore from this savepoint path on start.")
    allow_non_restored_state: bool = Field(default=False, description="Allow the job to start even if some savepoint state can't be restored.")
    flink_configuration: Optional[Dict[str, str]] = Field(default=None, description="Per-job Flink configuration overrides.")
    request_headers: Optional[Dict[str, str]] = Field(default=None, description="Extra HTTP headers for every request, e.g. {'Authorization': 'Bearer ...'} if the JobManager sits behind an authenticating proxy.")

    wait_mode: str = Field(default="until_finished", description="'until_finished' (batch job) | 'until_running' (streaming job) | 'fire_and_forget'.")
    poll_interval_seconds: float = Field(default=5.0, gt=0.0, description="Seconds between status polls.")
    timeout_seconds: int = Field(default=300, ge=1, description="Max time to wait for the target state (ignored for fire_and_forget).")
    request_timeout_seconds: int = Field(default=60, ge=1, description="HTTP request timeout.")
    fail_on_job_failure: bool = Field(default=True, description="Raise if the job ends CANCELED/FAILED (until_finished) or never reaches RUNNING (until_running). False materializes anyway with the state in metadata.")

    group_name: Optional[str] = Field(default=None, description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Extra asset tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Defaults to ['flink'].")
    deps: Optional[List[str]] = Field(default=None, description="Lineage-only upstream asset keys.")

    partition_type: Optional[str] = Field(default=None, description="'daily' | 'weekly' | 'monthly' | 'hourly' | 'static' | 'dynamic' | 'cron' | None.")
    partition_start: Optional[str] = Field(default=None)
    partition_values: Optional[Union[str, List[str]]] = Field(default=None)
    dynamic_partition_name: Optional[str] = Field(default=None)
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(default=None)
    partition_cron_schedule: Optional[str] = Field(
        default=None,
        description="Required when partition_type='cron'. A cron expression restricting the date axis to a custom cadence, e.g. '0 0 * * 1-5' for weekdays only.",
    )
    partition_cron_fmt: Optional[str] = Field(default="%Y-%m-%d", description="strftime format for partition keys produced by partition_type='cron'.")

    freshness_max_lag_minutes: Optional[int] = Field(default=None)
    freshness_cron: Optional[str] = Field(default=None)

    retry_policy_max_retries: Optional[int] = Field(default=None)
    retry_policy_delay_seconds: Optional[int] = Field(default=None)
    retry_policy_backoff: str = Field(default="exponential")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=("owners", "kinds", "deps", "program_args"), csv=("partition_values",), tags=("asset_tags",))

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        if self.wait_mode not in ("until_finished", "until_running", "fire_and_forget"):
            raise ValueError(f"wait_mode must be 'until_finished', 'until_running', or 'fire_and_forget', got {self.wait_mode!r}")
        if bool(self.jar_path) == bool(self.jar_id):
            raise ValueError("Set exactly one of jar_path or jar_id.")

        _self = self
        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values,
            self.dynamic_partition_name, self.partition_dimensions,
            cron_schedule=self.partition_cron_schedule,
            cron_fmt=self.partition_cron_fmt or "%Y-%m-%d",
        )

        _inferred_kinds = list(self.kinds or ["flink"])
        _all_tags = dict(self.asset_tags or {})
        for _kind in _inferred_kinds:
            _all_tags[f"dagster/kind/{_kind}"] = ""

        _freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            _freshness_policy = _freshness_policy_compat(int(self.freshness_max_lag_minutes), self.freshness_cron)

        _retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            _retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        @dg.asset(
            name=self.asset_name,
            group_name=self.group_name,
            description=self.description or f"Run a Flink job on {self.jobmanager_url!r}.",
            owners=self.owners or [],
            tags=_all_tags,
            kinds=set(_inferred_kinds),
            deps=[dg.AssetKey.from_user_string(d) for d in (self.deps or [])],
            partitions_def=partitions_def,
            freshness_policy=_freshness_policy,
            retry_policy=_retry_policy,
        )
        def _flink_job_run_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            self = _self
            headers = dict(self.request_headers or {})

            jar_id = self.jar_id
            if self.jar_path:
                jar_id = _upload_jar(self.jobmanager_url, self.jar_path, headers, self.request_timeout_seconds)
                context.log.info(f"flink_job_run_asset: uploaded {self.jar_path!r} -> jar_id={jar_id!r}")

            job_id = _run_jar(
                self.jobmanager_url, jar_id, self.entry_class, self.program_args, self.parallelism,
                self.savepoint_path, self.allow_non_restored_state, self.flink_configuration,
                headers, self.request_timeout_seconds,
            )
            context.log.info(f"flink_job_run_asset: submitted job_id={job_id!r}")

            state = "UNKNOWN"
            if self.wait_mode == "until_finished":
                state = _poll_until(
                    self.jobmanager_url, job_id, _TERMINAL_STATES, context.log, headers,
                    self.poll_interval_seconds, self.timeout_seconds, self.request_timeout_seconds,
                )
                if self.fail_on_job_failure and state in _FAILURE_STATES:
                    raise RuntimeError(f"Flink job {job_id!r} ended {state!r} instead of FINISHED.")
            elif self.wait_mode == "until_running":
                state = _poll_until(
                    self.jobmanager_url, job_id, {"RUNNING"}, context.log, headers,
                    self.poll_interval_seconds, self.timeout_seconds, self.request_timeout_seconds,
                )
                if self.fail_on_job_failure and state != "RUNNING":
                    raise RuntimeError(f"Flink job {job_id!r} ended {state!r} before reaching RUNNING.")

            metadata: Dict[str, Any] = {
                "job_id": dg.MetadataValue.text(job_id),
                "jar_id": dg.MetadataValue.text(jar_id),
                "wait_mode": dg.MetadataValue.text(self.wait_mode),
                "jobmanager_url": dg.MetadataValue.text(self.jobmanager_url),
            }
            if state != "UNKNOWN":
                metadata["state"] = dg.MetadataValue.text(state)

            return dg.MaterializeResult(metadata=metadata)

        return dg.Definitions(assets=[_flink_job_run_asset])
