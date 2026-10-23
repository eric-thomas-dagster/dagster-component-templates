"""LambdaInvokeAssetComponent.

Invokes an AWS Lambda function as a Dagster asset: synchronous
(`RequestResponse`) by default, parses the JSON response payload, and fails
loudly on a function error (`FunctionError` in the response) unless told
not to. `InvocationType='Event'` fires-and-forgets (no payload to parse).

Real boto3 API, verified directly against the botocore reference:
`lambda.invoke(FunctionName=..., InvocationType=..., Payload=<bytes>,
Qualifier=...)` -> `{'StatusCode', 'Payload': StreamingBody, 'FunctionError':
'Unhandled'|'Handled' (absent on success), 'LogResult': base64, ...}`.
"""
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


def _make_lambda_client(region_name: str, aws_profile: Optional[str]):
    import boto3
    session = boto3.Session(profile_name=aws_profile, region_name=region_name)
    return session.client("lambda")


def _invoke_lambda(
    client,
    function_name: str,
    payload: Dict[str, Any],
    invocation_type: str,
    qualifier: Optional[str],
    include_logs: bool,
) -> Dict[str, Any]:
    """Invoke, parse the response, return a plain dict. Never raises on a function
    error -- that's the caller's call (fail_on_function_error)."""
    import base64
    import json

    kwargs: Dict[str, Any] = {
        "FunctionName": function_name,
        "InvocationType": invocation_type,
        "Payload": json.dumps(payload).encode("utf-8"),
    }
    if qualifier:
        kwargs["Qualifier"] = qualifier
    if include_logs:
        kwargs["LogType"] = "Tail"

    response = client.invoke(**kwargs)
    status_code = response["StatusCode"]
    function_error = response.get("FunctionError")  # 'Unhandled' | 'Handled' | absent

    raw = response["Payload"].read() if "Payload" in response else b""
    response_payload: Any = None
    if raw:
        try:
            response_payload = json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError):
            response_payload = raw.decode("utf-8", errors="replace")

    log_tail = None
    if include_logs and response.get("LogResult"):
        log_tail = base64.b64decode(response["LogResult"]).decode("utf-8", errors="replace")

    return {
        "status_code": status_code,
        "function_error": function_error,
        "payload": response_payload,
        "log_tail": log_tail,
    }


class LambdaInvokeAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Invoke an AWS Lambda function as a Dagster asset.

    `RequestResponse` (the default) invokes synchronously and returns the
    function's JSON response as `payload` in the asset's output. `Event`
    invokes asynchronously and fire-and-forgets -- AWS returns immediately
    with no payload to parse.

    By default a Lambda-side error (`FunctionError` in the real boto3
    response -- the function raised/returned an error, not a transport
    failure) fails the Dagster run with the function's `errorMessage`
    surfaced in the exception. Set `fail_on_function_error: false` to treat
    it as a materialized-but-errored result instead (useful for a Lambda
    that intentionally returns structured error payloads).
    """

    asset_name: str = Field(description="Dagster asset key.")
    function_name: str = Field(description="Lambda function name, partial ARN, or full ARN.")
    region_name: str = Field(description="AWS region, e.g. 'us-east-1'.")
    aws_profile: Optional[str] = Field(default=None, description="Named AWS profile. Omit to use the default boto3 credential chain (env vars, instance role, etc.).")

    payload: Optional[Dict[str, Any]] = Field(default=None, description="Static JSON payload sent as the Lambda event. Mutually exclusive with payload_template.")
    payload_template: Optional[str] = Field(
        default=None,
        description=(
            "Jinja2 template rendered to a JSON string and sent as the Lambda event -- use for a payload "
            "that needs `{{ partition_key }}`/`{{ env.* }}`/etc. Mutually exclusive with payload."
        ),
    )
    invocation_type: str = Field(default="RequestResponse", description="'RequestResponse' (synchronous, parses the response) or 'Event' (async fire-and-forget).")
    qualifier: Optional[str] = Field(default=None, description="Function version or alias to invoke, e.g. 'PROD' or '3'. Omit for $LATEST.")
    fail_on_function_error: bool = Field(default=True, description="Raise if the Lambda's own invocation errored (FunctionError in the response). False materializes anyway with the error payload in metadata.")
    include_logs: bool = Field(default=False, description="Request the last 4KB of CloudWatch logs (LogType=Tail) and surface them as metadata. Adds response overhead -- leave off unless debugging.")

    group_name: Optional[str] = Field(default=None, description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Extra asset tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Defaults to ['aws', 'lambda'].")
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

    backfill_policy: Optional[Literal["single_run", "multi_run"]] = Field(
        default=None,
        description=(
            "Backfill execution strategy for a partitioned asset. 'single_run' "
            "fires ONE Lambda invocation covering the whole backfill range -- "
            "the partition_key/partition_date/partition_start/partition_end "
            "Jinja variables available in payload_template are comma-joined/"
            "range-spanning rather than a single key -- instead of one "
            "invocation per partition. 'multi_run' launches one run per "
            "partition (optionally capped via "
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
        return coerce_field_shapes(data, lists=("owners", "kinds", "deps"), csv=("partition_values",), tags=("asset_tags",))

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        if self.invocation_type not in ("RequestResponse", "Event"):
            raise ValueError(f"invocation_type must be 'RequestResponse' or 'Event', got {self.invocation_type!r}")
        if self.payload is not None and self.payload_template is not None:
            raise ValueError("Set either payload or payload_template, not both.")

        _self = self
        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values,
            self.dynamic_partition_name, self.partition_dimensions,
            cron_schedule=self.partition_cron_schedule,
            cron_fmt=self.partition_cron_fmt or "%Y-%m-%d",
        )

        _inferred_kinds = list(self.kinds or ["aws", "lambda"])
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
            description=self.description or f"Invoke AWS Lambda function {self.function_name!r}.",
            owners=self.owners or [],
            tags=_all_tags,
            kinds=set(_inferred_kinds),
            deps=[dg.AssetKey.from_user_string(d) for d in (self.deps or [])],
            partitions_def=partitions_def,
            freshness_policy=_freshness_policy,
            retry_policy=_retry_policy,
            backfill_policy=backfill_policy,
        )
        def _lambda_invoke_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            self = _self
            payload: Dict[str, Any] = {}
            if self.payload is not None:
                payload = dict(self.payload)
            elif self.payload_template is not None:
                import json as _json
                from jinja2 import Template
                auto_vars: Dict[str, Any] = dict(_partition_vars(context))
                rendered = Template(self.payload_template).render(**auto_vars)
                payload = _json.loads(rendered)

            client = _make_lambda_client(self.region_name, self.aws_profile)
            result = _invoke_lambda(
                client, self.function_name, payload, self.invocation_type,
                self.qualifier, self.include_logs,
            )

            context.log.info(
                f"lambda_invoke_asset: invoked {self.function_name!r} "
                f"(status={result['status_code']}, function_error={result['function_error']!r})"
            )

            if result["function_error"] and self.fail_on_function_error:
                err = result["payload"] or {}
                message = err.get("errorMessage") if isinstance(err, dict) else str(err)
                raise RuntimeError(
                    f"Lambda function {self.function_name!r} invocation errored "
                    f"({result['function_error']}): {message}"
                )

            metadata: Dict[str, Any] = {
                "status_code": dg.MetadataValue.int(result["status_code"]),
                "function_name": dg.MetadataValue.text(self.function_name),
                "invocation_type": dg.MetadataValue.text(self.invocation_type),
            }
            if result["function_error"]:
                metadata["function_error"] = dg.MetadataValue.text(result["function_error"])
            if result["payload"] is not None:
                metadata["response_payload"] = dg.MetadataValue.json(result["payload"]) if isinstance(result["payload"], (dict, list)) else dg.MetadataValue.text(str(result["payload"]))
            if result["log_tail"]:
                metadata["log_tail"] = dg.MetadataValue.text(result["log_tail"])

            return dg.MaterializeResult(metadata=metadata)

        return dg.Definitions(assets=[_lambda_invoke_asset])
