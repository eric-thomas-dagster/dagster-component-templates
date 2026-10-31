"""External Flink Job Component.

The Flink analog of `external_kafka_asset`'s `topics:` list: declare N
running jobs on a self-hosted Flink cluster as external assets from one
component instance, with one shared observation sensor that polls every
tracked job's status over a SINGLE `GET /jobs/overview` call per tick (that
endpoint already returns every job the cluster knows about in one response,
real shape verified directly against the Flink REST API reference -- even
more efficient than Kafka's per-topic offset lookups, which each need their
own call).

Important Flink-specific gotcha this does NOT share with Kafka topics: a
Flink job's `jid` is NOT stable -- it's assigned fresh every time the job is
(re)submitted (see `flink_job_run_asset`/`managed_flink_application_asset`).
What IS stable across resubmissions is the job's configured `name` (set by
the job code itself, e.g. `StreamExecutionEnvironment.execute("my-job")` or
a Flink SQL job's default name) -- so this component tracks jobs by `name`,
matching whatever `/jobs/overview` reports, not by `jid`.

Set `create_observation_sensor: true` to also get the polling sensor
standalone -- same pattern as ExternalKafkaAsset.
"""
import re
from typing import Any, Dict, List, Optional, Union

import dagster as dg
from pydantic import Field, model_validator


def _safe_sensor_name_part(s: str) -> str:
    """Sanitize a job name (which may contain '-', '.', '/', spaces, etc.)
    into a valid Dagster sensor-name fragment (must match ^[A-Za-z0-9_]+$)."""
    return re.sub(r"[^A-Za-z0-9_]", "_", s)


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


def _fetch_jobs_overview(jobmanager_url: str, headers: Dict[str, str], request_timeout_seconds: int) -> List[Dict[str, Any]]:
    import requests
    resp = requests.get(f"{jobmanager_url}/jobs/overview", headers=headers, timeout=request_timeout_seconds)
    if resp.status_code >= 400:
        raise RuntimeError(f"Flink /jobs/overview failed: HTTP {resp.status_code} {resp.text[:500]!r}")
    return resp.json().get("jobs", [])


class ExternalFlinkJobComponent(dg.Component, dg.Model, dg.Resolvable):
    """Declare one or more Flink jobs running on a self-hosted cluster as external assets."""

    jobmanager_url: str = Field(description="Base URL of the Flink JobManager REST API, e.g. 'http://flink-jobmanager:8081'.")
    asset_key: Optional[str] = Field(
        default=None,
        description=(
            "Dagster asset key for a single job. Mutually exclusive with `jobs:` -- use this "
            "(with `job_name:`) to declare exactly one job, or `jobs:` to declare several from "
            "one component instance sharing jobmanager_url and one observation sensor."
        ),
    )
    job_name: Optional[str] = Field(
        default=None,
        description="The job's configured name, as reported by GET /jobs/overview. Mutually exclusive with `jobs:`.",
    )
    jobs: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description=(
            "Declare several Flink jobs as external assets from one component instance, instead "
            "of one ExternalFlinkJobComponent YAML block (and, with create_observation_sensor=true, "
            "one polling sensor and one /jobs/overview call) per job. Each entry: `key` (Dagster "
            "asset key, required), `job_name` (the job's configured name, required), plus optional "
            "`deps`, `owners`, `metadata`, `group_name`, `description`. All entries share this "
            "component's jobmanager_url, partitioning config, and (if enabled) ONE sensor that "
            "polls every job's status over a SINGLE /jobs/overview call per tick -- set "
            "`sensor_name` explicitly when using `jobs:` with create_observation_sensor=true. "
            "Mutually exclusive with the top-level `asset_key`/`job_name` fields."
        ),
    )
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Human-readable description.")

    create_observation_sensor: bool = Field(
        default=False,
        description="Also create the polling sensor that keeps these external assets' status current. When False (default), this component only declares the AssetSpec(s).",
    )
    sensor_name: Optional[str] = Field(
        default=None,
        description="Unique sensor name. Defaults to '{job_name}__observation_sensor' in single-job mode. Required when using `jobs:`. Only used when create_observation_sensor=True.",
    )
    check_interval_seconds: int = Field(default=60, description="Seconds between status polls. Only used when create_observation_sensor=True.")
    request_headers: Optional[Dict[str, str]] = Field(default=None, description="Extra HTTP headers for every poll, e.g. an Authorization header if the JobManager sits behind an authenticating proxy.")
    request_timeout_seconds: int = Field(default=30, description="HTTP request timeout for each poll.")
    emit_materialization: bool = Field(
        default=True,
        description="When True (default), the sensor emits AssetMaterialization. When False, emits AssetObservation instead (no Dagster+ credit charge, but eager()-style conditions won't fire).",
    )

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

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=(), csv=("partition_values",), tags=())

    def _resolve_job_entries(self) -> List[Dict[str, Any]]:
        has_single = bool(self.asset_key or self.job_name)
        has_multi = bool(self.jobs)
        if has_single and has_multi:
            raise ValueError("Set either asset_key/job_name (single job) or jobs: (several jobs), not both.")
        if not has_single and not has_multi:
            raise ValueError("Set either asset_key/job_name (single job) or jobs: (several jobs).")

        if has_single:
            if not self.asset_key or not self.job_name:
                raise ValueError("Both asset_key and job_name are required in single-job mode.")
            return [{
                "key": self.asset_key, "job_name": self.job_name, "deps": None, "owners": None,
                "metadata": None, "group_name": self.group_name, "description": self.description,
            }]

        entries = []
        seen_keys = set()
        for i, raw in enumerate(self.jobs):
            if not isinstance(raw, dict):
                raise ValueError(f"jobs[{i}] must be a mapping, got {type(raw).__name__}")
            key = raw.get("key")
            job_name = raw.get("job_name")
            if not key:
                raise ValueError(f"jobs[{i}] is missing required field 'key'")
            if not job_name:
                raise ValueError(f"jobs[{i}] ({key!r}) is missing required field 'job_name'")
            if key in seen_keys:
                raise ValueError(f"jobs: duplicate key {key!r}")
            seen_keys.add(key)
            entries.append({
                "key": key, "job_name": job_name, "deps": raw.get("deps"), "owners": raw.get("owners"),
                "metadata": raw.get("metadata"), "group_name": raw.get("group_name", self.group_name),
                "description": raw.get("description"),
            })
        return entries

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        entries = self._resolve_job_entries()
        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values,
            self.dynamic_partition_name, self.partition_dimensions,
            cron_schedule=self.partition_cron_schedule,
            cron_fmt=self.partition_cron_fmt or "%Y-%m-%d", end_offset=self.partition_end_offset, timezone=self.partition_timezone,
        )

        specs = []
        for e in entries:
            metadata = {
                "jobmanager_url": self.jobmanager_url,
                "job_name": e["job_name"],
                "dagster.observability_type": "external",
            }
            if e["metadata"]:
                metadata.update(e["metadata"])
            specs.append(dg.AssetSpec(
                key=dg.AssetKey.from_user_string(e["key"]),
                group_name=e["group_name"],
                description=e["description"] or f"Flink job {e['job_name']}",
                kinds={"flink", "streaming"},
                deps=[dg.AssetKey.from_user_string(d) for d in e["deps"]] if e["deps"] else None,
                owners=e["owners"] or None,
                metadata=metadata,
                partitions_def=partitions_def,
            ))

        if not self.create_observation_sensor:
            return dg.Definitions(assets=specs)

        is_multi = self.jobs is not None
        if is_multi and not self.sensor_name:
            raise ValueError(
                "sensor_name is required when using jobs: with create_observation_sensor=true "
                "(there's no single job name to derive a default from)."
            )
        sensor_name = self.sensor_name or f"{_safe_sensor_name_part(entries[0]['job_name'])}__observation_sensor"
        _entries = entries
        _self = self

        @dg.sensor(
            name=sensor_name,
            minimum_interval_seconds=self.check_interval_seconds,
            asset_selection=dg.AssetSelection.assets(*[dg.AssetKey.from_user_string(e["key"]) for e in _entries]),
        )
        def _flink_obs(context: dg.SensorEvaluationContext):
            import json
            from dagster._core.definitions.data_version import DATA_VERSION_TAG

            _event_cls = dg.AssetMaterialization if _self.emit_materialization else dg.AssetObservation
            headers = dict(_self.request_headers or {})

            try:
                jobs_overview = _fetch_jobs_overview(_self.jobmanager_url, headers, _self.request_timeout_seconds)
            except Exception as ex:
                return dg.SensorResult(skip_reason=f"Flink /jobs/overview failed: {ex}")

            by_name: Dict[str, Dict[str, Any]] = {}
            for j in jobs_overview:
                by_name.setdefault(j["name"], j)  # first (most recent) entry per name wins

            events = []
            for e in _entries:
                job = by_name.get(e["job_name"])
                if job is None:
                    context.log.warning(f"Flink job named {e['job_name']!r} not found in /jobs/overview this tick.")
                    continue
                data_version = json.dumps({"state": job.get("state"), "start-time": job.get("start-time")}, sort_keys=True)
                obs_metadata = {
                    "job_name": e["job_name"],
                    "jid": job.get("jid"),
                    "state": job.get("state"),
                    "start_time": job.get("start-time"),
                    "duration_ms": job.get("duration"),
                    "jobmanager_url": _self.jobmanager_url,
                }
                events.append(_event_cls(
                    asset_key=dg.AssetKey.from_user_string(e["key"]),
                    metadata=obs_metadata,
                    tags={DATA_VERSION_TAG: data_version},
                ))

            if not events:
                return dg.SensorResult(skip_reason="no tracked jobs found in /jobs/overview this tick")
            return dg.SensorResult(asset_events=events)

        return dg.Definitions(assets=specs, sensors=[_flink_obs])
