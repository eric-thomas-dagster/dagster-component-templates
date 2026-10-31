"""Text Classifier Asset Component.

Classify text using LLMs with predefined categories or custom classification tasks.
Accepts a DataFrame with a text column and returns the DataFrame enriched with classification results.
"""

import os
import json
from typing import Any, Dict, List, Optional, Union, Literal
import pandas as pd

from dagster import (
    AssetIn,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    AssetExecutionContext,
    asset,
    Resolvable,
    Model,
    MetadataValue,
)
from pydantic import ConfigDict, Field


# BEGIN reasoning-output normalization (inlined per component; keep copies identical — see tests/test_reasoning_output.py)
import json as _json_r  # noqa: E402
import re as _re_r  # noqa: E402
from typing import List, Optional, Tuple  # noqa: E402,F401

_THINK_BLOCK_R = _re_r.compile(r"<think>(.*?)</think>", _re_r.S | _re_r.I)
_TOOL_CALL_BLOCK_R = _re_r.compile(r"<tool_call>\s*(\{.*?\})\s*</tool_call>", _re_r.S)


def split_reasoning(content: Optional[str]) -> Tuple[str, Optional[str]]:
    """Split `<think>…</think>` reasoning out of model text -> (answer, reasoning).

    Reasoning models (Qwen3, DeepSeek-R1, ...) often inline their chain of thought. Left in, it breaks JSON parsing,
    classification and routing downstream. Handles a closed block, an UNCLOSED block (output truncated by max_tokens
    mid-thought: everything after `<think>` is reasoning and there is no answer yet), and a bare closing tag (some chat
    templates put the opening tag in the prompt). Returns (content, None) untouched when there is no reasoning."""
    content = content or ""
    if "think>" not in content.lower():
        return content, None
    parts = [m.strip() for m in _THINK_BLOCK_R.findall(content) if m.strip()]
    rest = _THINK_BLOCK_R.sub("", content)
    m = _re_r.search(r"<think>(.*)$", rest, _re_r.S | _re_r.I)
    if m:
        if m.group(1).strip():
            parts.append(m.group(1).strip())
        rest = rest[: m.start()]
    elif _re_r.search(r"</think>", rest, _re_r.I) and not parts:
        before, after = _re_r.split(r"</think>", rest, maxsplit=1, flags=_re_r.I)
        if before.strip():
            parts.append(before.strip())
        rest = after
    return rest.strip(), ("\n\n".join(parts) if parts else None)


def parse_text_tool_calls(content: Optional[str]) -> Tuple[str, List[dict]]:
    """Recover `<tool_call>{"name":…,"arguments":{…}}</tool_call>` that Qwen/Hermes-style models emit as plain text when the
    server did not parse it into structured tool_calls -> (remaining text, [{name, arguments(json str)}])."""
    content = content or ""
    calls: List[dict] = []
    for raw in _TOOL_CALL_BLOCK_R.findall(content):
        try:
            obj = _json_r.loads(raw)
        except _json_r.JSONDecodeError:
            continue
        name = obj.get("name")
        if not name:
            continue
        args = obj.get("arguments", obj.get("parameters", {}))
        calls.append({"name": name, "arguments": args if isinstance(args, str) else _json_r.dumps(args)})
    return (_TOOL_CALL_BLOCK_R.sub("", content).strip() if calls else content), calls


def ollama_chat_model(model: str, has_tools: bool) -> str:
    """LiteLLM's `ollama/` route is /api/generate (no tool support); `ollama_chat/` is /api/chat, which has it."""
    if has_tools and model.lower().startswith("ollama/"):
        return "ollama_chat/" + model.split("/", 1)[1]
    return model
# END reasoning-output normalization


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


class TextClassifierComponent(Component, Model, Resolvable):
    """Component for classifying text with LLMs.

    Accepts a DataFrame via ins= and applies LLM classification to each row's
    input_column, adding the classification result as new column(s).

    Example:
        ```yaml
        type: dagster_community_components.TextClassifierComponent
        attributes:
          asset_name: text_classification
          upstream_asset_key: documents
          input_column: document_text
          output_column: category
          provider: openai
          model: gpt-4
          categories:
            - positive
            - negative
            - neutral
          classification_task: "sentiment analysis"
        ```
    """

    model_config = ConfigDict(populate_by_name=True)
    asset_name: str = Field(description="Name of the asset")
    upstream_asset_key: Union[str, List[str]] = Field(description="Upstream asset key providing a DataFrame with text to classify")
    input_column: Union[str, int] = Field(default="text", description="Column name containing text to classify")
    output_column: Union[str, int] = Field(default="category", description="Column name for classification result")
    confidence_column: Optional[Union[str, int]] = Field(default="confidence", description="Column name for confidence score (None to skip)")
    provider: str = Field(description="LLM provider")
    model_id: str = Field(
        alias="model",
        description="Model name")
    categories: List[Union[str, int]] = Field(
        description=(
            "List of categories to classify into. Accepts int too: dagster-components runs every "
            "string attribute through Jinja2's NativeTemplate for {{ }} templating support, which "
            "coerces any purely-numeric-looking value (e.g. a year like 2024) back to int regardless "
            "of how it was quoted in YAML -- List[str] alone would reject that at load time."
        )
    )
    classification_task: str = Field(default="classification", description="Task description")
    include_confidence: bool = Field(default=True, description="Include confidence scores")
    include_reasoning: bool = Field(default=False, description="Include reasoning")
    api_key: Optional[str] = Field(default=None, description="API key with ${VAR_NAME} syntax")
    temperature: float = Field(default=0.1, description="Temperature (low for consistency)")
    description: Optional[str] = Field(default=None, description="Asset description")
    group_name: Optional[str] = Field(default=None, description="Asset group")
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
        description="Column-level lineage mapping: output column name → list of upstream column names it was derived from, e.g. {'revenue': ['price', 'quantity']}",
    )

    include_preview_metadata: bool = Field(
        default=False,
        description=(
            "Include a preview of the output data in metadata (first 25 "
            "rows or a sample) for builder UIs."
        ),
    )

    preview_rows: int = Field(
        default=25,
        ge=1,
        le=500,
        description=(
            "Rows to include in the preview metadata. For long DataFrames "
            "(>10x preview_rows), a random sample is used; otherwise head()."
        ),
    )

    backfill_policy: Optional[Literal["single_run", "multi_run"]] = Field(
        default=None,
        description=(
            "Backfill execution strategy for a partitioned asset. 'single_run' materializes an entire backfill "
            'range in ONE run: the component processes each partition key in the range in turn (filtering '
            'upstream rows by partition_date_column / partition_static_column per key) and concatenates the '
            "results into one DataFrame. 'multi_run' launches one run per partition (optionally capped via "
            "backfill_policy_max_partitions_per_run). Leave unset for Dagster's default (one run per partition, "
            'uncapped). Ignored when no partitioning is configured. COST: total paid-API/LLM calls are the same '
            'as running the partitions separately, but note that when no '
            'partition_date_column/partition_static_column matches, every partition key processes ALL upstream '
            "rows, so a range multiplies calls by the number of partitions. 'single_run' requires a "
            'partition-range-aware IO manager (e.g. a DB-table IO manager with partition_expr set); the default '
            'filesystem IO manager cannot persist one output spanning multiple partitions and will raise a clear '
            'error at materialization time. Per-partition output metadata is overwritten by the last partition in '
            'the range.'
        ),
    )
    backfill_policy_max_partitions_per_run: Optional[int] = Field(
        default=None,
        description="For backfill_policy='multi_run' only: maximum partitions materialized per run. Leave unset for Dagster's default (1 partition per run).",
    )

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

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows
        upstream_asset_key = self.upstream_asset_key
        input_column = self.input_column
        output_column = self.output_column
        confidence_column = self.confidence_column
        provider = self.provider
        model = self.model_id
        categories_list = self.categories
        classification_task = self.classification_task
        include_confidence = self.include_confidence
        include_reasoning = self.include_reasoning
        api_key = self.api_key
        temperature = self.temperature
        description = self.description or f"Classify text using {provider}"
        group_name = self.group_name

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
        _comp_name = "text_classifier"  # component directory name
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



        from dagster import BackfillPolicy as _BackfillPolicy
        _backfill_policy_obj = None
        if partitions_def is not None and self.backfill_policy == 'single_run':
            _backfill_policy_obj = _BackfillPolicy.single_run()
        elif partitions_def is not None and self.backfill_policy == 'multi_run':
            _backfill_policy_obj = _BackfillPolicy.multi_run(
                max_partitions_per_run=self.backfill_policy_max_partitions_per_run or 1
            )

        _asset_deco = asset(retry_policy=_retry_policy, 
            key=AssetKey.from_user_string(asset_name),
            description=description,
            partitions_def=partitions_def, backfill_policy=_backfill_policy_obj,
                        owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
group_name=group_name,
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def _text_classifier_asset_impl(context: AssetExecutionContext, upstream: pd.DataFrame, _pk=None) -> pd.DataFrame:
            # Filter to current partition if partitioned
            if _pk is not None:
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
            """Classify text column in upstream DataFrame."""

            df = upstream.copy()

            if input_column not in df.columns:
                raise ValueError(f"Input column '{input_column}' not found. Available: {list(df.columns)}")

            # Normalize to str regardless of what came through the Jinja
            # native-type coercion above -- keeps the prompt text and the
            # `', '.join(categories)` below correct either way.
            categories = [str(c) for c in categories_list]

            # Expand environment variables in API key
            expanded_api_key = None
            if api_key:
                expanded_api_key = os.path.expandvars(api_key)
                if expanded_api_key == api_key and '${' in api_key:
                    raise ValueError(f"Environment variable in api_key '{api_key}' is not set")

            context.log.info(f"Classifying {len(df)} rows into {len(categories)} categories using {provider}/{model}")

            def build_prompt(text: str) -> str:
                p = f"Perform {classification_task} on the following text.\n\n"
                p += f"Categories: {', '.join(categories)}\n\n"
                p += f"Text: {text}\n\n"
                p += "Respond with a JSON object containing:\n"
                p += "- category: the selected category\n"
                if include_confidence:
                    p += "- confidence: confidence score between 0 and 1\n"
                if include_reasoning:
                    p += "- reasoning: brief explanation of the classification\n"
                return p

            def call_llm(prompt: str) -> dict:
                if provider == "openai":
                    import openai
                    client = _make_openai_client(expanded_api_key)
                    response = client.chat.completions.create(
                        model=model,
                        messages=[{"role": "user", "content": prompt}],
                        temperature=temperature,
                        response_format={"type": "json_object"}
                    )
                    return json.loads(split_reasoning(response.choices[0].message.content)[0])  # drop <think>...</think>
                elif provider == "anthropic":
                    import anthropic
                    client = anthropic.Anthropic(api_key=expanded_api_key)
                    message = client.messages.create(
                        model=model,
                        max_tokens=1024,
                        temperature=temperature,
                        messages=[{"role": "user", "content": prompt}]
                    )
                    return json.loads(message.content[0].text)
                else:
                    raise ValueError(f"Unsupported provider: {provider}")

            categories_out = []
            confidences_out = []

            for idx, row in df.iterrows():
                text = str(row[input_column])
                prompt = build_prompt(text)
                result = call_llm(prompt)
                categories_out.append(result.get("category"))
                confidences_out.append(result.get("confidence"))
                if idx % 10 == 0:
                    context.log.info(f"Classified {idx + 1}/{len(df)}")

            df[output_column] = categories_out
            if include_confidence and confidence_column:
                df[confidence_column] = confidences_out

            context.log.info(f"Classification complete: {len(df)} rows processed")
            context.add_output_metadata({
                "rows_processed": len(df),
                "provider": provider,
                "model": model,
                "categories": categories,
            })

            # Build column schema metadata
            from dagster import TableSchema, TableColumn, TableColumnLineage, TableColumnDep
            _col_schema = TableSchema(columns=[
                TableColumn(name=str(col), type=str(df.dtypes[col]))
                for col in df.columns
            ])
            _metadata = {
                "dagster/row_count": MetadataValue.int(len(df)),
                "dagster/column_schema": MetadataValue.table_schema(_col_schema),
            }
            # Use explicit lineage, or auto-infer passthrough columns at runtime
            _effective_lineage = column_lineage
            if not _effective_lineage:
                try:
                    _upstream_cols = set(upstream.columns)
                    _effective_lineage = {
                        col.name: [col.name] for col in _col_schema.columns
                        if col.name in _upstream_cols
                    }
                except Exception:
                    pass
            if _effective_lineage:
                _upstream_key = AssetKey.from_user_string(upstream_asset_key) if upstream_asset_key else None
                if _upstream_key:
                    _lineage_deps = {}
                    for out_col, in_cols in _effective_lineage.items():
                        _lineage_deps[str(out_col)] = [
                            TableColumnDep(asset_key=_upstream_key, column_name=str(ic))
                            for ic in in_cols
                        ]
                    _metadata["dagster/column_lineage"] = MetadataValue.column_lineage(
                        TableColumnLineage(_lineage_deps)
                    )
            if include_preview and len(df) > 0:
                try:
                    _prev = df.sample(min(preview_rows, len(df))) if len(df) > preview_rows * 10 else df.head(preview_rows)
                    _metadata["preview"] = MetadataValue.md(_prev.to_markdown(index=False))
                except Exception as _e:
                    context.log.warning(f"preview emission failed: {_e}")
            context.add_output_metadata(_metadata)

            return df

        @_asset_deco
        def text_classifier_asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
            _keys = list(context.partition_keys) if (context.has_partition_key or context.has_partition_key_range) else [None]
            if len(_keys) == 1:
                return _text_classifier_asset_impl(context, upstream, _keys[0])
            context.log.info(f'Processing {len(_keys)} partition keys in one run (backfill range)')
            return pd.concat([_text_classifier_asset_impl(context, upstream, _k) for _k in _keys], ignore_index=True)

        from dagster import build_column_schema_change_checks


        _schema_checks = build_column_schema_change_checks(assets=[text_classifier_asset])


        return Definitions(assets=[text_classifier_asset], asset_checks=list(_schema_checks))



def _make_openai_client(api_key):
    """Build an OpenAI or AzureOpenAI client based on env vars.

    Set OPENAI_AZURE_ENDPOINT to route through Azure OpenAI Service. Optional:
    OPENAI_AZURE_API_VERSION (default 2024-10-21). For Entra OAuth, set
    OPENAI_AZURE_USE_ENTRA=1 and the standard AZURE_TENANT_ID/CLIENT_ID/
    CLIENT_SECRET env vars (or rely on managed identity in Azure compute).
    """
    import openai as _openai
    azure_endpoint = os.environ.get("OPENAI_AZURE_ENDPOINT")
    if not azure_endpoint:
        return _openai.OpenAI(api_key=api_key)
    api_version = os.environ.get("OPENAI_AZURE_API_VERSION", "2024-10-21")
    if os.environ.get("OPENAI_AZURE_USE_ENTRA") == "1":
        from azure.identity import DefaultAzureCredential, get_bearer_token_provider
        token_provider = get_bearer_token_provider(
            DefaultAzureCredential(),
            "https://cognitiveservices.azure.com/.default",
        )
        return _openai.AzureOpenAI(
            azure_ad_token_provider=token_provider,
            azure_endpoint=azure_endpoint,
            api_version=api_version,
        )
    return _openai.AzureOpenAI(
        api_key=api_key,
        azure_endpoint=azure_endpoint,
        api_version=api_version,
    )


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
