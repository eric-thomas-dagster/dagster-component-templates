"""SqlToFileAssetComponent.

Run a SQL query against a registered warehouse/database resource (or a raw connection URL env
var) and write the result set to a file -- local path or an fsspec URI (s3://, gs://,
abfss://.../abfs://.../az://...). The reverse direction of `adls_to_database_asset`/
`cloud_storage_copy_asset`'s `sink_table`: a Copy activity whose SINK is a file, not a table.

Three things combined from established sibling conventions in this repo, not invented fresh:
  - SQL source dispatch (resource_key -- .get_engine()/.get_connection()/.get_client() -- OR
    database_url_env_var) is the same shape as data_masking's own `_ingest_warehouse_query`.
  - File-sink writing (fsspec URI + Azure credential env vars) is the same shape as
    cloud_storage_copy_asset's own `_write_dataframe`/`_azure_storage_options`.
  - run_window / pool / dry_run / retry_policy / partitioning are the same shape as sql_asset's.
"""
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Union

import dagster as dg
from pydantic import Field, model_validator


# BEGIN field-shape normalization (inlined per component; keep copies identical — see tests/test_field_shapes.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def coerce_field_shapes(data, *, lists=(), csv=(), tags=(), strs=()):
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
    for k in strs:  # str (or Dict[str, str]) fields: a numeric-looking YAML scalar resolves to int/float/bool
        v = out.get(k)
        if isinstance(v, bool):
            out[k] = "true" if v else "false"
        elif isinstance(v, (int, float)):
            out[k] = str(v)
        elif isinstance(v, dict):
            out[k] = {str(a): (("true" if b is True else "false") if isinstance(b, bool) else (str(b) if isinstance(b, (int, float)) else b)) for a, b in v.items()}
    return out
# END field-shape normalization


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


# BEGIN azure storage options (inlined per component; keep copies identical -- see
# tests/test_azure_storage_options_drift.py. fsspec storage_options built only when explicit Azure
# credential env vars are configured for an abfs://.../abfss://.../az://... URI -- ambient fsspec
# credential discovery applies otherwise, same as every other scheme already relies on.
def _require_env(name: str) -> str:
    """Read an env var via dagster.EnvVar, with an actionable message when it is unset."""
    value = dg.EnvVar(name).get_value()
    if value is None:
        raise KeyError(
            f"Environment variable {name!r} is not set. Set it in the environment of the "
            "Dagster code location (or point the *_env_var field at one that is)."
        )
    return value


def _azure_storage_options(uri: str, *, account_name_env_var=None, account_key_env_var=None, connection_string_env_var=None) -> Optional[Dict[str, str]]:
    """fsspec storage_options for an abfs://.../abfss://.../az://... URI, built only when Azure credential env
    vars are configured for it. Returns None for a non-Azure URI, or an Azure one with no explicit credentials
    configured (fsspec's ambient credential discovery applies then, same as the read side already relies on)."""
    if not uri.lower().startswith(("abfs://", "abfss://", "az://")):
        return None
    opts: Dict[str, str] = {}
    if account_name_env_var:
        opts["account_name"] = _require_env(account_name_env_var)
    if connection_string_env_var:
        opts["connection_string"] = _require_env(connection_string_env_var)
    elif account_key_env_var:
        opts["account_key"] = _require_env(account_key_env_var)
    return opts or None
# END azure storage options


def _write_dataframe(df, uri: str, fmt: str, *, compression: Optional[str] = None, storage_options: Optional[Dict[str, str]] = None):
    kwargs: Dict[str, Any] = {"storage_options": storage_options} if storage_options else {}
    if fmt == "csv":
        df.to_csv(uri, index=False, compression=compression, **kwargs)
    elif fmt == "jsonl":
        df.to_json(uri, orient="records", lines=True, **kwargs)
    elif fmt == "json":
        df.to_json(uri, orient="records", **kwargs)
    elif fmt == "parquet":
        df.to_parquet(uri, compression=compression, **kwargs)
    else:
        raise ValueError(f"Unsupported sink_format: {fmt!r} (supported: csv, jsonl, json, parquet)")


def _run_query(query: str, *, resource_key: Optional[str], database_url_env_var: Optional[str], context):
    """Same dispatch shape as data_masking's own `_ingest_warehouse_query`: a registered resource
    exposing .get_engine() (SQLAlchemy), .get_connection() (DBAPI), or .get_client() (vendor
    client -- BigQuery's .query(sql).to_dataframe(), Redshift's .execute_query(sql,
    fetch_results=True, cursor_factory=RealDictCursor)), or a bare SQLAlchemy engine built from
    database_url_env_var when no Dagster resource is registered."""
    import pandas as pd

    if resource_key:
        resource = getattr(context.resources, resource_key)
        if hasattr(resource, "get_engine"):
            return pd.read_sql(query, resource.get_engine())
        if hasattr(resource, "get_connection"):
            with resource.get_connection() as conn:
                return pd.read_sql(query, conn)
        if hasattr(resource, "get_client"):
            client = resource.get_client()
            if hasattr(client, "query"):
                job = client.query(query)
                if hasattr(job, "to_dataframe"):
                    return job.to_dataframe()
            if hasattr(client, "execute_query"):
                try:
                    from psycopg2.extras import RealDictCursor
                    rows = client.execute_query(query, fetch_results=True, cursor_factory=RealDictCursor)
                except ImportError:
                    rows = client.execute_query(query, fetch_results=True)
                return pd.DataFrame([dict(r) for r in (rows or [])])
            raise ValueError(
                f"resource {resource_key!r}'s get_client() returned {type(client).__name__}, "
                "which this component doesn't know how to query (no .query()/.to_dataframe() "
                "or .execute_query() method found)."
            )
        raise ValueError(
            f"resource {resource_key!r} must expose .get_engine() (SQLAlchemy), "
            f".get_connection() (DBAPI), or .get_client() (vendor client); got {type(resource).__name__}"
        )
    if database_url_env_var:
        from sqlalchemy import create_engine
        url = os.environ.get(database_url_env_var, "")
        if not url:
            raise ValueError(f"database_url_env_var {database_url_env_var!r} is unset")
        return pd.read_sql(query, create_engine(url))
    raise ValueError("SqlToFileAssetComponent requires 'resource_key' OR 'database_url_env_var' (unless dry_run)")


def _build_partitions_def(
    partition_type, partition_start, partition_values,
    dynamic_partition_name, partition_dimensions,
    cron_schedule=None, cron_fmt="%Y-%m-%d", end_offset=0, timezone=None,
):
    """Standard partitions_def builder, same shape repo-wide -- see cloud_storage_copy_asset's
    own copy for the fuller multi-dimension (partition_dimensions) variant this mirrors."""
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, DynamicPartitionsDefinition,
    )
    if not partition_type:
        return None
    if isinstance(partition_values, (list, tuple)):
        _values = [str(v).strip() for v in partition_values if str(v).strip()]
    else:
        _values = [v.strip() for v in (str(partition_values) if partition_values else "").split(",") if v.strip()]
    if partition_type in ("daily", "weekly", "monthly", "hourly", "cron") and not partition_start:
        raise ValueError(f"partition_type={partition_type!r} requires partition_start.")
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
        if not _values:
            raise ValueError("static requires values")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError("dynamic requires dynamic_partition_name")
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    raise ValueError(f"unknown partition_type: {partition_type!r}")


class SqlToFileAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Run a SQL query and write the result set to a file -- local path or an fsspec URI (s3://,
    gs://, abfss://.../abfs://.../az://...). The reverse of a Copy activity whose sink is a file
    rather than a table."""

    asset_key: str = Field(description="Dagster asset key (e.g. 'exports/daily_orders').")
    resource_key: Optional[str] = Field(
        default=None,
        description=(
            "Key of a registered resource exposing .get_engine() (SQLAlchemy), .get_connection() "
            "(DBAPI), or .get_client() (vendor client, e.g. BigQuery/Redshift). Set this OR "
            "database_url_env_var (unless dry_run is True, which needs neither)."
        ),
    )
    database_url_env_var: Optional[str] = Field(
        default=None,
        description="Env var with a SQLAlchemy database URL, read at run time. Set this OR resource_key (unless dry_run).",
    )
    query: str = Field(description="SQL query. Result set becomes the output file's rows.")

    sink_path: str = Field(
        description=(
            "fsspec URI (s3://, gs://, abfss://..., or a local/plain path) to write the query "
            "result to. '{partition_key}' is templated when partitioned."
        ),
    )
    sink_format: str = Field(
        description="File format: 'csv' | 'jsonl' | 'json' | 'parquet'.",
    )
    sink_compression: Optional[str] = Field(default=None, description="e.g. 'gzip', 'snappy' (parquet).")

    azure_account_name_env_var: Optional[str] = Field(default=None, description="Only used when sink_path is an abfs://.../abfss://... URI.")
    azure_account_key_env_var: Optional[str] = Field(default=None, description="Only used when sink_path is an abfs://.../abfss://... URI (ignored if azure_connection_string_env_var is also set).")
    azure_connection_string_env_var: Optional[str] = Field(default=None, description="Only used when sink_path is an abfs://.../abfss://... URI. Takes precedence over azure_account_key_env_var.")

    dry_run: bool = Field(
        default=False,
        description="Log the query and resolved sink_path and skip actually running it -- no resource is looked up, no connection opened, nothing written.",
    )
    run_window: Optional[str] = Field(
        default=None,
        description=(
            "'HH:MM-HH:MM tz [day,day,...]' (e.g. '09:00-17:00 America/New_York "
            "mon,tue,wed,thu,fri'). Outside this window, the asset SKIPS cleanly instead of "
            "running or failing -- no query runs, no file is written, and real downstream "
            "dependents skip too. Checked once per materialization. A malformed spec fails open "
            "(never blocks). Unset (default): always runs."
        ),
    )
    pool: Optional[str] = Field(
        default=None,
        description="Dagster's native named concurrency pool -- a cap on concurrent exports sharing the same pool name. The pool's own slot count is configured at the deployment level, not here.",
    )

    group_name: str = Field(default="ingestion", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    deps: Optional[List[str]] = Field(default=None, description="Upstream Dagster asset keys for lineage.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners -- team names ('team:analytics') or email addresses.")
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Additional key-value tags applied to the asset in the Dagster catalog.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds for the catalog. Auto-includes 'sql' if empty.")

    retry_policy_max_retries: Optional[int] = Field(default=None, description="Max retries on asset failure. Defines a RetryPolicy.")
    retry_policy_delay_seconds: Optional[int] = Field(default=None, description="Seconds between retries (default 1).")
    retry_policy_backoff: str = Field(default="exponential", description="Backoff strategy: 'linear' or 'exponential'.")

    partition_type: Optional[str] = Field(default=None, description="Partition type: 'daily' / 'weekly' / 'monthly' / 'hourly' / 'cron' / 'static' / 'dynamic' / None for unpartitioned.")
    partition_cron_schedule: Optional[str] = Field(default=None, description="Required when partition_type='cron'. e.g. '0 0 * * 1-5' for weekdays only.")
    partition_cron_fmt: Optional[str] = Field(default="%Y-%m-%d", description="strftime format for partition keys produced by partition_type='cron'.")
    partition_end_offset: int = Field(default=0, description="0 (default): a time-based partition set only ever holds COMPLETED windows. 1: include the current, in-progress window.")
    partition_timezone: Optional[str] = Field(default=None, description="IANA timezone for the underlying partitions definition (defaults to UTC).")
    partition_start: Optional[str] = Field(default=None, description="Partition start date in ISO format. Required for time-based partition types.")
    partition_values: Optional[Union[str, List[str]]] = Field(default=None, description="Comma-separated values for static partitioning.")
    dynamic_partition_name: Optional[str] = Field(default=None, description="Name for DynamicPartitionsDefinition when partition_type='dynamic'.")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=("deps", "owners", "kinds"), csv=("partition_values",), tags=("asset_tags",))

    @model_validator(mode="after")
    def _validate_connection_source(self):
        if self.dry_run:
            if self.resource_key and self.database_url_env_var:
                raise ValueError("SqlToFileAssetComponent: set at most one of resource_key or database_url_env_var -- not both.")
            return self
        if bool(self.resource_key) == bool(self.database_url_env_var):
            raise ValueError("SqlToFileAssetComponent: set exactly one of resource_key or database_url_env_var -- not both, not neither.")
        return self

    @model_validator(mode="after")
    def _validate_sink_format(self):
        if self.sink_format not in ("csv", "jsonl", "json", "parquet"):
            raise ValueError(f"SqlToFileAssetComponent: sink_format must be one of csv/jsonl/json/parquet, got {self.sink_format!r}.")
        return self

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="SQL to File Asset", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        component = self
        kinds = set(component.kinds) if component.kinds else {"sql"}
        deps_keys = [dg.AssetKey.from_user_string(k) for k in (component.deps or [])]

        _retry_policy = None
        if component.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            _retry_policy = RetryPolicy(
                max_retries=component.retry_policy_max_retries,
                delay=component.retry_policy_delay_seconds or 1,
                backoff=Backoff[component.retry_policy_backoff.upper()],
            )

        partitions_def = _build_partitions_def(
            component.partition_type, component.partition_start, component.partition_values,
            component.dynamic_partition_name, None,
            cron_schedule=component.partition_cron_schedule,
            cron_fmt=component.partition_cron_fmt or "%Y-%m-%d",
            end_offset=component.partition_end_offset, timezone=component.partition_timezone,
        )

        required_resource_keys = {component.resource_key} if (component.resource_key and not component.dry_run) else None

        @dg.asset(
            key=dg.AssetKey(component.asset_key.split("/")),
            description=component.description or f"SQL query result -> {component.sink_path}",
            group_name=component.group_name,
            deps=deps_keys,
            owners=component.owners or None,
            tags=component.asset_tags or None,
            kinds=kinds,
            partitions_def=partitions_def,
            required_resource_keys=required_resource_keys,
            retry_policy=_retry_policy,
            pool=component.pool,
        )
        def _sql_to_file_asset(context: dg.AssetExecutionContext):
            if component.run_window and not _is_in_run_window(time.time(), component.run_window):
                context.log.info(f"run_window={component.run_window!r}: outside the configured window -- skipping (not failing).")
                return

            raw_key = context.partition_key if context.has_partition_key else None
            sink_path = component.sink_path.replace("{partition_key}", raw_key) if raw_key is not None else component.sink_path

            if component.dry_run:
                context.log.info(f"[dry_run] would run query and write to {sink_path} ({component.sink_format})")
                context.log.info(f"[dry_run] query: {component.query}")
                return dg.MaterializeResult(metadata={
                    "dry_run": dg.MetadataValue.bool(True),
                    "sink_path": dg.MetadataValue.text(sink_path),
                    "query": dg.MetadataValue.text(component.query),
                })

            df = _run_query(
                component.query, resource_key=component.resource_key,
                database_url_env_var=component.database_url_env_var, context=context,
            )
            sink_opts = _azure_storage_options(
                sink_path, account_name_env_var=component.azure_account_name_env_var,
                account_key_env_var=component.azure_account_key_env_var,
                connection_string_env_var=component.azure_connection_string_env_var,
            )
            _write_dataframe(df, sink_path, component.sink_format, compression=component.sink_compression, storage_options=sink_opts)
            context.log.info(f"Wrote {len(df)} rows to {sink_path} ({component.sink_format})")
            return dg.MaterializeResult(metadata={
                "sink_path": dg.MetadataValue.text(sink_path),
                "num_rows": dg.MetadataValue.int(len(df)),
                "num_columns": dg.MetadataValue.int(len(df.columns)),
                "dry_run": dg.MetadataValue.bool(False),
            })

        return dg.Definitions(assets=[_sql_to_file_asset])
