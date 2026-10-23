"""Druid SQL Query Asset Component.

Execute a SQL query against an Apache Druid broker's real SQL HTTP API and
materialize the result as a Dagster asset.

Request/response shape verified directly against the official Druid SQL
API reference (druid.apache.org/docs/latest/api-reference/sql-api), not
from memory: `POST /druid/v2/sql` with a JSON body `{"query": ...,
"resultFormat": "object", ...}` (Content-Type: application/json) returns a
JSON array of objects -- one per row, column name -> value -- when
`resultFormat: "object"` (the default here). No native Druid Python client
needed; this is a plain HTTP+JSON API.
"""
from typing import Any, Dict, List, Optional, Union

import pandas as pd
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
):
    """Canonical `_build_partitions_def` shared across the registry.

    Both flat-fields and multi-axis shapes are supported. Raises ValueError
    on misconfigured combinations rather than silently picking a default.
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


def _run_druid_sql(broker_url: str, query: str, auth, headers: Dict[str, str], request_timeout_seconds: int) -> List[Dict[str, Any]]:
    import requests

    resp = requests.post(
        f"{broker_url}/druid/v2/sql",
        json={"query": query, "resultFormat": "object"},
        auth=auth, headers=headers, timeout=request_timeout_seconds,
    )
    if resp.status_code >= 400:
        raise RuntimeError(f"Druid SQL query failed: HTTP {resp.status_code} {resp.text[:1000]!r}")
    return resp.json()


class DruidSqlQueryComponent(dg.Component, dg.Model, dg.Resolvable):
    """Execute Druid SQL (via POST /druid/v2/sql) and materialize the result as a Dagster asset."""

    asset_name: str = Field(description="Dagster asset key.")
    broker_url: str = Field(description="Base URL of the Druid broker, e.g. 'http://druid-broker:8082'.")
    query: str = Field(description="Druid SQL query to execute. {partition_key}/{partition_date}/{partition_start}/{partition_end} tokens are substituted on a partitioned run.")
    username: Optional[str] = Field(default=None, description="Basic auth username, if the broker requires it.")
    password_env_var: Optional[str] = Field(default=None, description="Env var holding the basic auth password.")
    request_headers: Optional[Dict[str, str]] = Field(default=None, description="Extra HTTP headers, e.g. a bearer token if Druid sits behind an authenticating proxy.")
    request_timeout_seconds: int = Field(default=120, description="HTTP request timeout.")

    group_name: Optional[str] = Field(default=None, description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Extra asset tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Defaults to ['druid'].")
    deps: Optional[List[str]] = Field(default=None, description="Lineage-only upstream asset keys.")

    column_lineage: Optional[Dict[str, List[str]]] = Field(default=None, description="Output column -> input column names, recorded against every key in deps.")
    include_preview_metadata: bool = Field(default=True, description="Include a markdown preview of the result in materialization metadata.")
    preview_rows: int = Field(default=20, description="Max rows in the preview.")

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
        return coerce_field_shapes(data, lists=('deps', 'owners', 'kinds'), csv=('partition_values',), tags=('asset_tags',))

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        kinds = self.kinds or ["druid"]
        tags = dict(self.asset_tags or {})
        for k in kinds:
            tags[f"dagster/kind/{k}"] = ""

        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values,
            self.dynamic_partition_name, self.partition_dimensions,
            cron_schedule=self.partition_cron_schedule,
            cron_fmt=self.partition_cron_fmt or "%Y-%m-%d",
        )

        freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            freshness_policy = _freshness_policy_compat(self.freshness_max_lag_minutes, self.freshness_cron)

        retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        _self = self

        @dg.asset(
            key=dg.AssetKey.from_user_string(self.asset_name),
            description=self.description or f"Druid SQL query: {self.query[:60].strip()}...",
            group_name=self.group_name,
            owners=self.owners or [],
            tags=tags,
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
            partitions_def=partitions_def,
            freshness_policy=freshness_policy,
            retry_policy=retry_policy,
        )
        def druid_sql_query_asset(context: dg.AssetExecutionContext) -> pd.DataFrame:
            self = _self
            import os

            auth = None
            if self.username:
                password = os.environ.get(self.password_env_var) if self.password_env_var else None
                auth = (self.username, password or "")

            rendered_query = _substitute_partition(self.query, _partition_vars(context))
            context.log.info(f"druid_sql_query: executing against {self.broker_url}")
            rows = _run_druid_sql(self.broker_url, rendered_query, auth, dict(self.request_headers or {}), self.request_timeout_seconds)
            df = pd.DataFrame(rows)
            context.log.info(f"druid_sql_query: returned {len(df)} rows, {len(df.columns)} columns")

            metadata: Dict[str, Any] = {"dagster/row_count": dg.MetadataValue.int(len(df))}
            if len(df.columns) > 0:
                schema = dg.TableSchema(columns=[dg.TableColumn(name=str(c), type=str(df.dtypes[c])) for c in df.columns])
                metadata["dagster/column_schema"] = dg.MetadataValue.table_schema(schema)
            if self.column_lineage:
                lineage_deps: Dict[str, List[dg.TableColumnDep]] = {}
                upstream_deps_keys = [dg.AssetKey.from_user_string(k) for k in (self.deps or [])]
                for out_col, in_cols in self.column_lineage.items():
                    for uak in upstream_deps_keys:
                        lineage_deps.setdefault(str(out_col), []).extend(
                            dg.TableColumnDep(asset_key=uak, column_name=str(ic)) for ic in in_cols
                        )
                if lineage_deps:
                    metadata["dagster/column_lineage"] = dg.MetadataValue.column_lineage(dg.TableColumnLineage(lineage_deps))
            if self.include_preview_metadata and len(df) > 0:
                try:
                    _prev = df.sample(min(self.preview_rows, len(df))) if len(df) > self.preview_rows * 10 else df.head(self.preview_rows)
                    metadata["preview"] = dg.MetadataValue.md(_prev.to_markdown(index=False))
                except Exception as e:  # noqa: BLE001
                    context.log.warning(f"preview emission failed: {e}")
            context.add_output_metadata(metadata)
            return df

        return dg.Definitions(assets=[druid_sql_query_asset])
