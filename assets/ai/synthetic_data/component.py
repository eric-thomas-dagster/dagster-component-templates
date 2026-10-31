"""Synthetic Data.

Generate synthetic data rows using an LLM based on a schema definition.
"""
from dataclasses import field
from typing import Any, Dict, List, Optional, Union

import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    MetadataValue,
    Model,
    Resolvable,
    asset,
)
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


class SyntheticDataComponent(Component, Model, Resolvable):
    """Generate synthetic data rows using an LLM based on a schema definition."""

    asset_name: str = Field(description="Output Dagster asset name")
    schema: Dict[str, Any] = Field(description="Column definitions e.g. {name: 'full name', age: 'integer 18-80'}")
    n_rows: int = Field(default=100, description="Total number of synthetic rows to generate")
    model: str = Field(default="gpt-4o-mini", description="LLM model identifier (passed to litellm)")
    api_key_env_var: str = Field(default="OPENAI_API_KEY", description="Environment variable name holding the API key")
    context: Optional[str] = Field(default=None, description="Business context to guide generation e.g. 'e-commerce customers'")
    batch_size: int = Field(default=25, description="Number of rows per LLM call")
    seed_data_asset_key: Optional[str] = Field(default=None, description="Optional upstream asset key for few-shot seed rows")
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name")
    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', or None for unpartitioned",
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
        description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.",
    )
    partition_date_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter upstream DataFrame to the current date partition key.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition (when partition_type='dynamic'), e.g. 'tenants'.",
    )

    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Multi-axis partition spec: list of {name, type, start, values, dynamic_partition_name} dicts. Overrides flat fields when set.",
    )

    partition_values: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning, e.g. 'customer_a,customer_b,customer_c'.",
    )
    partition_static_dim: Optional[str] = Field(
        default=None,
        description="Dimension name for the static axis in multi-partitioning, e.g. 'customer' or 'region'.",
    )
    partition_static_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter upstream DataFrame to the current static partition dimension (e.g. 'customer_id').",
    )
    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners — list of team names or email addresses, e.g. ['team:analytics', 'user@company.com']",
    )
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(
        default=None,
        description="Additional key-value tags to apply to the asset, e.g. {'domain': 'finance', 'tier': 'gold'}",
    )
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds for the Dagster catalog, e.g. ['snowflake', 'python']. Auto-inferred from component name if not set.",
    )
    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale. Defines a FreshnessPolicy.",
    )
    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5' (weekdays at 9am).",
    )
    column_lineage: Optional[Dict[str, List[str]]] = Field(
        default=None,
        description="Column-level lineage, emitted as `dagster/column_lineage` metadata on every materialization: {output column: [upstream columns]}, e.g. {'revenue': ['price', 'quantity']}. The upstream asset is `upstream_asset_key`, or the only entry of `deps`; with several upstream assets, name each column '<asset_key>:<column>'. Ignored with a warning when no upstream asset can be resolved.",
    )


    description: Optional[str] = Field(
        default=None,
        description="Asset description shown in the Dagster catalog.",
    )

    deps: Optional[List[str]] = Field(
        default=None,
        description="Lineage-only upstream asset keys (no data passed at runtime).",
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('owners', 'kinds', 'deps'), csv=('partition_values',), tags=('asset_tags',))

    @classmethod
    def get_description(cls) -> str:
        return "Generate synthetic data rows using an LLM based on a schema definition."

    retry_policy_max_retries: Optional[int] = Field(

        default=None,

        description="Max retries on asset failure. Defines a RetryPolicy. Useful for transient network failures, rate limits, etc.",

    )

    retry_policy_delay_seconds: Optional[int] = Field(

        default=None,

        description="Seconds between retries (default 1).",

    )

    retry_policy_backoff: str = Field(

        default="exponential",

        description="Backoff strategy: 'linear' or 'exponential'.",

    )


    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        _column_lineage_md = _column_lineage_metadata(self)
        asset_name = self.asset_name
        schema = self.schema
        n_rows = self.n_rows
        llm_model = self.model
        api_key_env_var = self.api_key_env_var
        context_str = self.context
        batch_size = self.batch_size
        seed_data_asset_key = self.seed_data_asset_key
        group_name = self.group_name

        # Build retry policy (auto-generated; opt-in via retry_policy_max_retries).
        _retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            _retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        if seed_data_asset_key:
            ins = {"seed_data": AssetIn(key=AssetKey.from_user_string(seed_data_asset_key))}

        partitions_def = _build_partitions_def(
            self.partition_type,
            self.partition_start,
            self.partition_values,
            self.dynamic_partition_name,
            self.partition_dimensions,
        
    cron_schedule=self.partition_cron_schedule,
    cron_fmt=self.partition_cron_fmt or "%Y-%m-%d", end_offset=self.partition_end_offset, timezone=self.partition_timezone,
)
        partition_type = self.partition_type
        partition_date_column = self.partition_date_column
        partition_static_column = self.partition_static_column
        partition_static_dim = self.partition_static_dim

        # Infer kinds from component name if not explicitly set
        _comp_name = "synthetic_data"  # component directory name
        _kind_map = {
            "snowflake": "snowflake", "bigquery": "bigquery", "redshift": "redshift",
            "postgres": "postgres", "postgresql": "postgres", "mysql": "mysql",
            "s3": "s3", "adls": "azure", "azure": "azure", "gcs": "gcp",
            "google": "gcp", "databricks": "databricks", "dbt": "dbt",
            "kafka": "kafka", "mongodb": "mongodb", "redis": "redis",
            "neo4j": "neo4j", "elasticsearch": "elasticsearch", "pinecone": "pinecone",
            "chromadb": "chromadb", "pgvector": "postgres",
        }
        _inferred_kinds = self.kinds or []
        if not _inferred_kinds:
            _comp_lower = asset_name.lower()
            for keyword, kind in _kind_map.items():
                if keyword in _comp_lower:
                    _inferred_kinds.append(kind)
            if not _inferred_kinds:
                _inferred_kinds = ["python"]

        # Build combined tags: user tags + kind tags
        _all_tags = dict(self.asset_tags or {})
        for _kind in _inferred_kinds:
            _all_tags[f"dagster/kind/{_kind}"] = ""

        # Build freshness policy
        _freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy
            _freshness_policy = _freshness_policy_compat(self.freshness_max_lag_minutes, self.freshness_cron)

        owners = self.owners or []
        column_lineage = self.column_lineage if hasattr(self, 'column_lineage') else None


        if seed_data_asset_key:
            @asset(description=self.description, freshness_policy=_freshness_policy, retry_policy=_retry_policy, partitions_def=partitions_def, key=AssetKey.from_user_string(asset_name), ins=ins, group_name=group_name, deps=[AssetKey.from_user_string(k) for k in (self.deps or [])])
            def _asset(
                context: AssetExecutionContext,
                seed_data: Any,
            ) -> pd.DataFrame:
                # partition bridge dict-concat: when an unpartitioned
                # asset consumes a partitioned upstream, Dagster's IO
                # manager loads ALL partitions as a dict; concat to
                # a single DataFrame before any DataFrame ops.
                if _column_lineage_md:
                    context.add_output_metadata(_column_lineage_md)
                if isinstance(seed_data, dict):
                    _frames = [v for v in seed_data.values() if isinstance(v, pd.DataFrame)]
                    seed_data = pd.concat(_frames, ignore_index=True) if _frames else pd.DataFrame()
                # Filter to current partition if partitioned
                if context.has_partition_key or context.has_partition_key_range:
                    # One filter pass per partition key: a normal tick gives exactly one,
                    # a single_run backfill gives the whole range. Rows are selected by
                    # position so keys that match overlapping rows never duplicate them.
                    _pk_base = seed_data
                    _pk_pos = _pk_base.reset_index(drop=True) if hasattr(_pk_base, 'reset_index') else _pk_base
                    _pk_keep = set()
                    _pk_filtered = False
                    for _pk_iter in context.partition_keys:
                        seed_data = _pk_pos
                        _pk = _pk_iter
                        _is_multi = hasattr(_pk, "keys_by_dimension")
                        _date_key = _pk.keys_by_dimension.get("date", "") if _is_multi else str(_pk)
                        _static_key = next((_pk.keys_by_dimension[_d] for _d in (partition_static_dim, "static_dim", "segment") if _d and _d in _pk.keys_by_dimension), "") if _is_multi else None
                        if partition_date_column and partition_date_column in seed_data.columns and _date_key:
                            _col_dates = pd.to_datetime(seed_data[partition_date_column], errors="coerce").dt.strftime("%Y-%m-%d")
                            seed_data = seed_data[_col_dates == _date_key]
                        if partition_static_column and partition_static_column in seed_data.columns and _static_key:
                            seed_data = seed_data[seed_data[partition_static_column].astype(str) == _static_key]
                        elif partition_static_column and partition_static_column in seed_data.columns and not _is_multi:
                            seed_data = seed_data[seed_data[partition_static_column].astype(str) == str(_pk)]
                        if seed_data is not _pk_pos:
                            _pk_filtered = True
                            _pk_keep.update(seed_data.index)
                    seed_data = _pk_base.iloc[sorted(_pk_keep)] if _pk_filtered else _pk_base
                _generated = _generate_data(
                    context=context,
                    schema=schema,
                    n_rows=n_rows,
                    llm_model=llm_model,
                    api_key_env_var=api_key_env_var,
                    context_str=(_partition_note(context, partition_static_dim) + (context_str or "")) or context_str,
                    batch_size=batch_size,
                    seed_df=seed_data,
                )
                return _stamp_df_to_partition(
                    _generated, context, partition_date_column, partition_static_column, partition_static_dim
                )

        else:

            @asset(description=self.description, partitions_def=partitions_def, retry_policy=_retry_policy, freshness_policy=_freshness_policy, key=AssetKey.from_user_string(asset_name), deps=[], group_name=group_name)
            def _asset(context: AssetExecutionContext) -> pd.DataFrame:  # type: ignore[no-redef]
                if _column_lineage_md:
                    context.add_output_metadata(_column_lineage_md)
                _generated = _generate_data(
                    context=context,
                    schema=schema,
                    n_rows=n_rows,
                    llm_model=llm_model,
                    api_key_env_var=api_key_env_var,
                    context_str=(_partition_note(context, partition_static_dim) + (context_str or "")) or context_str,
                    batch_size=batch_size,
                    seed_df=None,
                )
                return _stamp_df_to_partition(
                    _generated, context, partition_date_column, partition_static_column, partition_static_dim
                )

        from dagster import build_column_schema_change_checks


        _schema_checks = build_column_schema_change_checks(assets=[_asset])


        return Definitions(assets=[_asset], asset_checks=list(_schema_checks))


def _partition_static_values(context, partition_static_dim):
    """The static-axis value of each partition key being materialized (several for a backfill range)."""
    values = []
    for key in context.partition_keys:
        if hasattr(key, "keys_by_dimension"):
            dims = key.keys_by_dimension
            value = next((dims[d] for d in (partition_static_dim, "static_dim", "segment") if d and d in dims), None)
        else:
            value = str(key)
        if value is not None:
            values.append(value)
    return values


def _partition_note(context, partition_static_dim):
    """A line telling the model which partition it is generating for, so partitions do not all get the same data."""
    if not (context.has_partition_key or context.has_partition_key_range):
        return ""
    parts = []
    try:
        window = context.partition_time_window
        parts.append(f"dates from {window.start.date()} up to (not including) {window.end.date()}")
    except Exception:
        pass
    static = _partition_static_values(context, partition_static_dim)
    if static:
        parts.append("segment " + ", ".join(sorted(set(static))))
    return ("Generate data for " + " and ".join(parts) + ".\n") if parts else ""


def _stamp_df_to_partition(df, context, partition_date_column, partition_static_column, partition_static_dim):
    """Generated rows have no input to filter, so a partition means: the rows belong to it. Rows are stamped with
    dates spread evenly across the partition's time window and with its static-axis value (round-robin across the keys
    of a backfill range), so downstream partitioned assets that filter on these columns route them correctly."""
    if not (context.has_partition_key or context.has_partition_key_range) or len(df) == 0:
        return df
    out = df.copy()
    n = len(out)
    if partition_date_column and partition_date_column in out.columns:
        try:
            window = context.partition_time_window
        except Exception:
            window = None  # static / dynamic partition: no time window
        if window is not None:
            start, end = pd.Timestamp(window.start), pd.Timestamp(window.end)
            stamped = [start + (end - start) * i / n for i in range(n)]
            out[partition_date_column] = [d.strftime("%Y-%m-%d %H:%M:%S") for d in stamped] if out[partition_date_column].dtype == object else stamped
    if partition_static_column and partition_static_column in out.columns:
        values = _partition_static_values(context, partition_static_dim)
        if values:
            out[partition_static_column] = [values[i % len(values)] for i in range(n)]
    return out


def _generate_data(
    context: AssetExecutionContext,
    schema: dict,
    n_rows: int,
    llm_model: str,
    api_key_env_var: str,
    context_str: Optional[str],
    batch_size: int,
    seed_df: Optional[pd.DataFrame],
) -> pd.DataFrame:
    import json
    import os

    try:
        from litellm import completion
    except ImportError as e:
        raise ImportError("litellm is required: pip install litellm") from e

    api_key = os.environ.get(api_key_env_var)
    all_rows = []

    seed_example = ""
    if seed_df is not None and len(seed_df) > 0:
        sample = seed_df.head(3).to_dict(orient="records")
        seed_example = f"\nHere are example rows for reference:\n{json.dumps(sample, default=str)}\n"

    for batch_start in range(0, n_rows, batch_size):
        batch_n = min(batch_size, n_rows - batch_start)
        ctx_prefix = f"Context: {context_str}\n" if context_str else ""
        prompt = (
            f"{ctx_prefix}"
            f"Generate {batch_n} rows of synthetic data as a JSON array.{seed_example}"
            f"Schema: {json.dumps(schema)}\n"
            f"Return only a JSON array of objects with no additional explanation."
        )
        resp = completion(
            model=llm_model,
            messages=[{"role": "user", "content": prompt}],
            api_key=api_key,
        )
        raw = resp.choices[0].message.content.strip()
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[1].rsplit("```", 1)[0]
        rows = json.loads(raw)
        all_rows.extend(rows)

    result = pd.DataFrame(all_rows)

    return result


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
