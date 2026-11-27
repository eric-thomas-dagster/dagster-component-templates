"""RedshiftDataApiAssetComponent.

Runs a SQL query against Amazon Redshift (provisioned cluster or Serverless)
via the AWS Redshift Data API as a Dagster asset: the Airflow
`RedshiftDataOperator` equivalent. Real boto3 `redshift-data` client API,
verified directly against the botocore reference:
`redshift-data.execute_statement(ClusterIdentifier=..., WorkgroupName=...,
Database=..., DbUser=..., SecretArn=..., Sql=...)` -> `Id`, then
`redshift-data.describe_statement(Id=...)` -> `Status` in
`SUBMITTED|PICKED|STARTED|FINISHED|ABORTED|FAILED`, `Error`, `ResultRows`,
`ResultSize`, `Duration` (nanoseconds), `RedshiftQueryId`.

Connection: EITHER a provisioned cluster (`cluster_identifier` + `database`,
with `db_user` for temporary IAM credentials OR `secret_arn` for a Secrets
Manager-stored user/password) OR Redshift Serverless (`workgroup_name` +
`database`, with `secret_arn` optional -- Serverless can authenticate via the
caller's own IAM identity without one).
"""
import time
from typing import Any, Dict, List, Literal, Optional, Union

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


def _make_redshift_data_client(region_name: str, aws_profile: Optional[str]):
    import boto3
    session = boto3.Session(profile_name=aws_profile, region_name=region_name)
    return session.client("redshift-data")


def _run_redshift_statement(
    client,
    sql: str,
    database: str,
    cluster_identifier: Optional[str],
    db_user: Optional[str],
    workgroup_name: Optional[str],
    secret_arn: Optional[str],
    poll_interval_seconds: float,
    timeout_seconds: Optional[float],
    log,
) -> Dict[str, Any]:
    """Execute the statement, poll to a terminal state, return a plain dict. Raises on FAILED/ABORTED or timeout."""
    kwargs: Dict[str, Any] = {"Database": database, "Sql": sql}
    if cluster_identifier:
        kwargs["ClusterIdentifier"] = cluster_identifier
        if db_user:
            kwargs["DbUser"] = db_user
    if workgroup_name:
        kwargs["WorkgroupName"] = workgroup_name
    if secret_arn:
        kwargs["SecretArn"] = secret_arn

    start = client.execute_statement(**kwargs)
    statement_id = start["Id"]
    log.info(f"redshift_data_api_asset: started statement Id={statement_id!r}")

    deadline = (time.monotonic() + timeout_seconds) if timeout_seconds else None
    while True:
        resp = client.describe_statement(Id=statement_id)
        status = resp["Status"]
        if status in ("FINISHED", "FAILED", "ABORTED"):
            return {
                "statement_id": statement_id,
                "status": status,
                "error": resp.get("Error"),
                "result_rows": resp.get("ResultRows"),
                "result_size": resp.get("ResultSize"),
                "duration_ns": resp.get("Duration"),
                "redshift_query_id": resp.get("RedshiftQueryId"),
                "has_result_set": resp.get("HasResultSet"),
            }
        if deadline is not None and time.monotonic() >= deadline:
            raise TimeoutError(
                f"Redshift Data API statement {statement_id!r} did not reach a terminal state within "
                f"{timeout_seconds}s (last status: {status!r})."
            )
        time.sleep(poll_interval_seconds)


class RedshiftDataApiAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Run a SQL query against Amazon Redshift via the Redshift Data API as a Dagster asset.

    The Airflow `RedshiftDataOperator` equivalent: calls `execute_statement`,
    polls `describe_statement` until the query reaches `FINISHED`/`FAILED`/
    `ABORTED`, and raises with the real `Error` message on anything but
    success. `sql` is rendered through Jinja2 first, so
    `{{ partition_key }}`/`{{ partition_start }}`/`{{ partition_end }}`/
    `{{ partition_date }}` are available for a partitioned asset.

    Connection: set EITHER `cluster_identifier` (a provisioned cluster --
    pair with `db_user` for temporary IAM credentials, or `secret_arn_env_var`
    for Secrets Manager auth) OR `workgroup_name` (Redshift Serverless --
    `secret_arn_env_var` optional there, since Serverless can authenticate
    via the caller's own IAM identity).

    This does NOT fetch result rows into asset metadata (avoids pulling
    potentially large/sensitive query results through Dagster) -- only
    `result_rows`/`result_size` counts from `describe_statement` are reported.
    """

    asset_name: str = Field(description="Dagster asset key.")
    sql: str = Field(description="SQL query text. Rendered through Jinja2, so {{ partition_key }} etc. are available for a partitioned asset.")
    database: str = Field(description="Redshift database name.")
    cluster_identifier: Optional[str] = Field(default=None, description="Provisioned cluster identifier. Set this OR workgroup_name, not both.")
    db_user: Optional[str] = Field(default=None, description="Database user for temporary IAM credentials against a provisioned cluster_identifier. Required if cluster_identifier is set and secret_arn_env_var is not.")
    workgroup_name: Optional[str] = Field(default=None, description="Redshift Serverless workgroup name. Set this OR cluster_identifier, not both.")
    secret_arn_env_var: Optional[str] = Field(default=None, description="Environment variable holding a Secrets Manager secret ARN with Redshift credentials. Required for cluster_identifier if db_user is not set; optional for workgroup_name.")
    region_name: str = Field(description="AWS region, e.g. 'us-east-1'.")
    aws_profile: Optional[str] = Field(default=None, description="Named AWS profile. Omit to use the default boto3 credential chain (env vars, instance role, etc.).")
    poll_interval_seconds: float = Field(default=2.0, description="Seconds between describe_statement polls.")
    timeout_seconds: Optional[float] = Field(default=900.0, description="Fail the run if the statement hasn't reached a terminal state after this many seconds. Set null to wait indefinitely.")

    group_name: Optional[str] = Field(default=None, description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Extra asset tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Defaults to ['aws', 'redshift'].")
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

    freshness_max_lag_minutes: Optional[int] = Field(default=None)
    freshness_cron: Optional[str] = Field(default=None)

    retry_policy_max_retries: Optional[int] = Field(default=None)
    retry_policy_delay_seconds: Optional[int] = Field(default=None)
    retry_policy_backoff: str = Field(default="exponential")

    backfill_policy: Optional[Literal["single_run", "multi_run"]] = Field(
        default=None,
        description=(
            "Backfill execution strategy for a partitioned asset. 'single_run' "
            "fires ONE Redshift statement covering the whole backfill range -- "
            "the partition_key/partition_start/partition_end Jinja variables "
            "in sql are comma-joined/range-spanning rather than a single key "
            "-- instead of one statement per partition. 'multi_run' launches "
            "one run per partition (optionally capped via "
            "backfill_policy_max_partitions_per_run). Leave unset for "
            "Dagster's default (one run per partition, uncapped). Ignored "
            "when partition_type is not set."
        ),
    )
    backfill_policy_max_partitions_per_run: Optional[int] = Field(
        default=None,
        description="For backfill_policy='multi_run' only: maximum partitions materialized per run. Leave unset for Dagster's default (1 partition per run).",
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=("owners", "kinds", "deps"), csv=("partition_values",), tags=("asset_tags",))

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='Redshift Data API Asset', editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        if bool(self.cluster_identifier) == bool(self.workgroup_name):
            raise ValueError(
                "Set exactly one of cluster_identifier (provisioned cluster) or "
                "workgroup_name (Redshift Serverless), not both and not neither."
            )
        if self.cluster_identifier and not (self.db_user or self.secret_arn_env_var):
            raise ValueError(
                "cluster_identifier requires either db_user (temporary IAM credentials) "
                "or secret_arn_env_var (Secrets Manager auth)."
            )

        _self = self
        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values,
            self.dynamic_partition_name, self.partition_dimensions,
            cron_schedule=self.partition_cron_schedule,
            cron_fmt=self.partition_cron_fmt or "%Y-%m-%d", end_offset=self.partition_end_offset, timezone=self.partition_timezone,
        )

        _inferred_kinds = list(self.kinds or ["aws", "redshift"])
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

        backfill_policy = None
        if partitions_def is not None and self.backfill_policy == "single_run":
            backfill_policy = dg.BackfillPolicy.single_run()
        elif partitions_def is not None and self.backfill_policy == "multi_run":
            backfill_policy = dg.BackfillPolicy.multi_run(
                max_partitions_per_run=self.backfill_policy_max_partitions_per_run or 1
            )

        @dg.asset(
            name=self.asset_name,
            group_name=self.group_name,
            description=self.description or f"Run a Redshift Data API statement (database={self.database!r}).",
            owners=self.owners or [],
            tags=_all_tags,
            kinds=set(_inferred_kinds),
            deps=[dg.AssetKey.from_user_string(d) for d in (self.deps or [])],
            partitions_def=partitions_def,
            freshness_policy=_freshness_policy,
            retry_policy=_retry_policy,
            backfill_policy=backfill_policy,
        )
        def _redshift_data_api_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            self = _self
            from jinja2 import Template

            auto_vars = dict(_partition_vars(context))
            rendered_sql = Template(self.sql).render(**auto_vars)

            secret_arn = dg.EnvVar(self.secret_arn_env_var).get_value() if self.secret_arn_env_var else None

            client = _make_redshift_data_client(self.region_name, self.aws_profile)
            result = _run_redshift_statement(
                client, rendered_sql, self.database, self.cluster_identifier,
                self.db_user, self.workgroup_name, secret_arn,
                self.poll_interval_seconds, self.timeout_seconds, context.log,
            )

            context.log.info(
                f"redshift_data_api_asset: statement Id={result['statement_id']!r} "
                f"status={result['status']!r}"
            )

            if result["status"] != "FINISHED":
                raise RuntimeError(
                    f"Redshift Data API statement {result['statement_id']!r} ended in status "
                    f"{result['status']!r}: {result['error']}"
                )

            metadata: Dict[str, Any] = {
                "statement_id": dg.MetadataValue.text(result["statement_id"]),
                "status": dg.MetadataValue.text(result["status"]),
            }
            if result["result_rows"] is not None:
                metadata["result_rows"] = dg.MetadataValue.int(result["result_rows"])
            if result["result_size"] is not None:
                metadata["result_size_bytes"] = dg.MetadataValue.int(result["result_size"])
            if result["duration_ns"] is not None:
                metadata["duration_ms"] = dg.MetadataValue.int(int(result["duration_ns"]) // 1_000_000)
            if result["redshift_query_id"] is not None:
                metadata["redshift_query_id"] = dg.MetadataValue.int(result["redshift_query_id"])

            return dg.MaterializeResult(metadata=metadata)

        return dg.Definitions(assets=[_redshift_data_api_asset])
