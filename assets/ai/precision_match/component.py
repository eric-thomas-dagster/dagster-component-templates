"""Precision Match.

LLM-assisted fuzzy matching to standardize varied string representations to canonical forms.
"""
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
    if partition_type in ("daily", "weekly", "monthly", "hourly") and not partition_start:
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



# BEGIN upstream-asset-key normalization (inlined per component; keep copies identical — see tests/test_upstream_keys.py)
from typing import List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def normalize_upstream_keys(single, plural=None) -> List[str]:
    """Merge ``upstream_asset_key`` / ``upstream_asset_keys`` into one clean list.

    Each argument may be a str, a list/tuple of str, or None. Entries are
    stripped, empties dropped, and duplicates removed (order preserved).
    """
    out: List[str] = []
    for value in (single, plural):
        if value is None:
            continue
        items = [value] if isinstance(value, str) else value
        if not isinstance(items, (list, tuple)):
            raise ValueError(
                f"upstream asset key(s) must be a string or a list of strings, got {value!r}"
            )
        for item in items:
            if not isinstance(item, str):
                raise ValueError(f"upstream asset keys must be strings, got {item!r}")
            item = item.strip()
            if item and item not in out:
                out.append(item)
    return out


def coerce_upstream_input(data, *, component: str, mode: str, optional: bool = True):
    """Rewrite raw component input so both spellings/shapes land on the declared field(s).

    mode "single": declared field is ``upstream_asset_key`` (str); >1 key is an error.
    mode "multi":  declared field is ``upstream_asset_keys`` (list).
    mode "both":   both fields are declared; everything is merged into the plural one.
    """
    if not isinstance(data, dict):
        return data
    if "upstream_asset_key" not in data and "upstream_asset_keys" not in data:
        return data
    keys = normalize_upstream_keys(data.get("upstream_asset_key"), data.get("upstream_asset_keys"))
    out = {k: v for k, v in data.items() if k not in ("upstream_asset_key", "upstream_asset_keys")}
    if mode == "single":
        if len(keys) > 1:
            raise ValueError(f"{component} takes exactly one upstream asset, got {keys}")
        out["upstream_asset_key"] = keys[0] if keys else (None if optional else "")
    else:
        out["upstream_asset_keys"] = keys or (None if optional else [])
    return out
# END upstream-asset-key normalization



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


class PrecisionMatchComponent(Component, Model, Resolvable):
    """LLM-assisted fuzzy matching to standardize varied string representations to canonical forms."""

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: Union[str, List[str]] = Field(description="Upstream asset key providing a DataFrame with values to standardize")
    column: Union[str, int] = Field(description="Column with varied strings to standardize")
    reference_asset_key: Optional[str] = Field(default=None, description="Optional upstream asset key for a reference DataFrame containing canonical values")
    reference_column: Optional[Union[str, int]] = Field(default=None, description="Column in the reference DataFrame containing canonical values")
    reference_values: Optional[List[str]] = Field(default=None, description="Explicit list of canonical values (alternative to reference_asset_key)")
    model: str = Field(default="gpt-4o-mini", description="LLM model identifier (passed to litellm)")
    api_key_env_var: str = Field(default="OPENAI_API_KEY", description="Environment variable name holding the API key")
    output_column: Union[str, int] = Field(default="matched_value", description="Column name for the matched canonical value")
    confidence_column: Optional[Union[str, int]] = Field(default="match_confidence", description="Column name for match confidence score (None to skip)")
    batch_size: int = Field(default=20, description="Number of unique values to match per LLM call")
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name")
    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', or None for unpartitioned",
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

    @model_validator(mode="before")
    @classmethod
    def _coerce_upstream_asset_keys(cls, data):
        return coerce_upstream_input(data, component=cls.__name__, mode="single", optional=False)

    @classmethod
    def get_description(cls) -> str:
        return "LLM-assisted fuzzy matching to standardize varied string representations to canonical forms."

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
        upstream_asset_key = self.upstream_asset_key
        column = self.column
        reference_asset_key = self.reference_asset_key
        reference_column = self.reference_column
        reference_values = self.reference_values
        llm_model = self.model
        api_key_env_var = self.api_key_env_var
        output_column = self.output_column
        confidence_column = self.confidence_column
        batch_size = self.batch_size
        group_name = self.group_name

        if reference_asset_key:
            ins = {
                "upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key)),
                "reference": AssetIn(key=AssetKey.from_user_string(reference_asset_key)),
            }

        partitions_def = _build_partitions_def(
            self.partition_type,
            self.partition_start,
            self.partition_values,
            self.dynamic_partition_name,
            self.partition_dimensions,
        )
        partition_type = self.partition_type
        partition_date_column = self.partition_date_column
        partition_static_column = self.partition_static_column
        partition_static_dim = self.partition_static_dim

        # Infer kinds from component name if not explicitly set
        _comp_name = "precision_match"  # component directory name
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


        # Build retry policy (auto-generated; opt-in via retry_policy_max_retries).
        _retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            _retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        if reference_asset_key:
            @asset(description=self.description, freshness_policy=_freshness_policy, retry_policy=_retry_policy, partitions_def=partitions_def, key=AssetKey.from_user_string(asset_name), ins=ins, group_name=group_name, deps=[AssetKey.from_user_string(k) for k in (self.deps or [])])
            def _asset(
                context: AssetExecutionContext,
                upstream: Any,
                reference: pd.DataFrame,
            ) -> pd.DataFrame:
                # partition bridge dict-concat: when an unpartitioned
                # asset consumes a partitioned upstream, Dagster's IO
                # manager loads ALL partitions as a dict; concat to
                # a single DataFrame before any DataFrame ops.
                if _column_lineage_md:
                    context.add_output_metadata(_column_lineage_md)
                if isinstance(upstream, dict):
                    _frames = [v for v in upstream.values() if isinstance(v, pd.DataFrame)]
                    upstream = pd.concat(_frames, ignore_index=True) if _frames else pd.DataFrame()
                # Filter to current partition if partitioned
                if context.has_partition_key or context.has_partition_key_range:
                    # One filter pass per partition key: a normal tick gives exactly one,
                    # a single_run backfill gives the whole range. Rows are selected by
                    # position so keys that match overlapping rows never duplicate them.
                    _pk_base = upstream
                    _pk_pos = _pk_base.reset_index(drop=True) if hasattr(_pk_base, 'reset_index') else _pk_base
                    _pk_keep = set()
                    _pk_filtered = False
                    for _pk_iter in context.partition_keys:
                        upstream = _pk_pos
                        _pk = _pk_iter
                        _is_multi = hasattr(_pk, "keys_by_dimension")
                        _date_key = _pk.keys_by_dimension.get("date", "") if _is_multi else str(_pk)
                        _static_key = next((_pk.keys_by_dimension[_d] for _d in (partition_static_dim, "static_dim", "segment") if _d and _d in _pk.keys_by_dimension), "") if _is_multi else None
                        if partition_date_column and partition_date_column in upstream.columns and _date_key:
                            _col_dates = pd.to_datetime(upstream[partition_date_column], errors="coerce").dt.strftime("%Y-%m-%d")
                            upstream = upstream[_col_dates == _date_key]
                        if partition_static_column and partition_static_column in upstream.columns and _static_key:
                            upstream = upstream[upstream[partition_static_column].astype(str) == _static_key]
                        elif partition_static_column and partition_static_column in upstream.columns and not _is_multi:
                            upstream = upstream[upstream[partition_static_column].astype(str) == str(_pk)]
                        if upstream is not _pk_pos:
                            _pk_filtered = True
                            _pk_keep.update(upstream.index)
                    upstream = _pk_base.iloc[sorted(_pk_keep)] if _pk_filtered else _pk_base
                return _run_matching(
                    context=context,
                    df=upstream,
                    column=column,
                    canonical=reference[reference_column].dropna().unique().tolist(),
                    llm_model=llm_model,
                    api_key_env_var=api_key_env_var,
                    output_column=output_column,
                    confidence_column=confidence_column,
                    batch_size=batch_size,
                )

        else:
            ins = {"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))}

            @asset(description=self.description, partitions_def=partitions_def, retry_policy=_retry_policy, freshness_policy=_freshness_policy, key=AssetKey.from_user_string(asset_name), ins=ins, group_name=group_name)
            def _asset(  # type: ignore[no-redef]
                context: AssetExecutionContext,
                upstream: pd.DataFrame,
            ) -> pd.DataFrame:
                # Filter to current partition if partitioned
                if _column_lineage_md:
                    context.add_output_metadata(_column_lineage_md)
                if context.has_partition_key or context.has_partition_key_range:
                    # One filter pass per partition key: a normal tick gives exactly one,
                    # a single_run backfill gives the whole range. Rows are selected by
                    # position so keys that match overlapping rows never duplicate them.
                    _pk_base = upstream
                    _pk_pos = _pk_base.reset_index(drop=True) if hasattr(_pk_base, 'reset_index') else _pk_base
                    _pk_keep = set()
                    _pk_filtered = False
                    for _pk_iter in context.partition_keys:
                        upstream = _pk_pos
                        _pk = _pk_iter
                        _is_multi = hasattr(_pk, "keys_by_dimension")
                        _date_key = _pk.keys_by_dimension.get("date", "") if _is_multi else str(_pk)
                        _static_key = next((_pk.keys_by_dimension[_d] for _d in (partition_static_dim, "static_dim", "segment") if _d and _d in _pk.keys_by_dimension), "") if _is_multi else None
                        if partition_date_column and partition_date_column in upstream.columns and _date_key:
                            _col_dates = pd.to_datetime(upstream[partition_date_column], errors="coerce").dt.strftime("%Y-%m-%d")
                            upstream = upstream[_col_dates == _date_key]
                        if partition_static_column and partition_static_column in upstream.columns and _static_key:
                            upstream = upstream[upstream[partition_static_column].astype(str) == _static_key]
                        elif partition_static_column and partition_static_column in upstream.columns and not _is_multi:
                            upstream = upstream[upstream[partition_static_column].astype(str) == str(_pk)]
                        if upstream is not _pk_pos:
                            _pk_filtered = True
                            _pk_keep.update(upstream.index)
                    upstream = _pk_base.iloc[sorted(_pk_keep)] if _pk_filtered else _pk_base
                canonical = reference_values or []
                return _run_matching(
                    context=context,
                    df=upstream,
                    column=column,
                    canonical=canonical,
                    llm_model=llm_model,
                    api_key_env_var=api_key_env_var,
                    output_column=output_column,
                    confidence_column=confidence_column,
                    batch_size=batch_size,
                )

        from dagster import build_column_schema_change_checks


        _schema_checks = build_column_schema_change_checks(assets=[_asset])


        return Definitions(assets=[_asset], asset_checks=list(_schema_checks))


def _run_matching(
    context: AssetExecutionContext,
    df: pd.DataFrame,
    column: str,
    canonical: list,
    llm_model: str,
    api_key_env_var: str,
    output_column: str,
    confidence_column: Optional[str],
    batch_size: int,
) -> pd.DataFrame:
    import json
    import os

    try:
        from litellm import completion
    except ImportError as e:
        raise ImportError("litellm is required: pip install litellm") from e

    df = df.copy()
    unique_vals = df[column].dropna().unique().tolist()
    api_key = os.environ.get(api_key_env_var)

    mapping: dict = {}
    confidence_map: dict = {}

    for i in range(0, len(unique_vals), batch_size):
        batch = unique_vals[i : i + batch_size]
        prompt = (
            f"Match each input string to the closest canonical value. "
            f"Return a JSON object mapping each input to its matched canonical value "
            f"and a confidence score between 0 and 1.\n"
            f"Format: {{\"input_value\": {{\"match\": \"canonical_value\", \"confidence\": 0.95}}, ...}}\n"
            f"Canonical values: {json.dumps(canonical)}\n"
            f"Inputs to match: {json.dumps(batch)}"
        )
        resp = completion(
            model=llm_model,
            messages=[{"role": "user", "content": prompt}],
            api_key=api_key,
        )
        raw = resp.choices[0].message.content
        # Strip markdown code fences if present
        raw = raw.strip()
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[1].rsplit("```", 1)[0]
        result = json.loads(raw)
        for val, info in result.items():
            if isinstance(info, dict):
                mapping[val] = info.get("match", val)
                confidence_map[val] = info.get("confidence", None)
            else:
                mapping[val] = info

    df[output_column] = df[column].map(mapping)
    if confidence_column:
        df[confidence_column] = df[column].map(confidence_map)

    matched = df[output_column].notna().sum()

    return df


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
