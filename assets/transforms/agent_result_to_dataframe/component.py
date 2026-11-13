"""AgentResultToDataFrameComponent -- turn an agent (or agentic_pipeline step)
dict result into a DataFrame so the existing dataframe_to_* sinks (table, csv,
parquet, s3, gcs, duckdb, snowflake, ...) can store it.

Agent assets (anthropic_agent, openai_agent, litellm_agent, gemini_agent,
vercel_ai_gateway_agent, snowflake_cortex_agent, ...) all materialize a dict
shaped roughly like {final_answer, iterations, tool_calls_made,
tool_call_details, ...}. An agentic_pipeline step asset materializes a dict
shaped {text, ...op-specific fields}. No sink in this repo accepts either
shape directly -- they all read a DataFrame. This component bridges the two.

Partition-bridge heuristic: when this asset is unpartitioned but its upstream
is partitioned (or a single_run backfill spans a partition range), Dagster's
IO manager hands back a dict keyed by partition key instead of one result. A
real agent-result dict is *also* just a dict, so the two are told apart by
whether the top-level dict carries any of the common result-shape keys
(final_answer/text/output/result/answer/json) -- if none do and every value
is itself a dict, it's treated as a partition bridge (one row group per
partition). Partitioning this asset the same way as its upstream (the common
case) avoids the heuristic firing at all.
"""

from datetime import datetime, timezone
from typing import Any, Dict, List, Literal, Optional, Union
import json
import re

import pandas as pd

from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    Backoff,
    Component,
    ComponentLoadContext,
    Definitions,
    MetadataValue,
    Model,
    Output,
    Resolvable,
    RetryPolicy,
    asset,
)
from pydantic import Field


_RESULT_SHAPE_KEYS = ("final_answer", "text", "output", "result", "answer", "json")


def _is_scalar(v: Any) -> bool:
    return v is None or isinstance(v, (str, int, float, bool))


def _coerce_value(v: Any) -> Any:
    """Pass scalars through; JSON-serialize anything else so it fits a DataFrame cell."""
    return v if _is_scalar(v) else json.dumps(v, default=str)


def _extract_json_block(text: str) -> Any:
    """Parse text as JSON, falling back to the content of a fenced ```json block."""
    text = (text or "").strip()
    try:
        return json.loads(text)
    except Exception:
        pass
    m = re.search(r"```(?:json)?\s*(.*?)```", text, re.DOTALL)
    if m:
        return json.loads(m.group(1).strip())
    raise ValueError(f"no valid JSON found in text: {text[:200]!r}")


def _coerce_agent_results(upstream: Any) -> List[tuple]:
    """Return [(partition_key_or_None, result_dict_or_value), ...]."""
    if hasattr(upstream, "value") and hasattr(upstream, "metadata"):
        upstream = upstream.value
    if (
        isinstance(upstream, dict)
        and upstream
        and all(isinstance(v, dict) for v in upstream.values())
        and not any(k in upstream for k in _RESULT_SHAPE_KEYS)
    ):
        return list(upstream.items())
    return [(None, upstream)]


def _row_from_dict(d: Dict[str, Any], include: Optional[List[str]], exclude: Optional[List[str]]) -> Dict[str, Any]:
    row: Dict[str, Any] = {}
    for k, v in d.items():
        if include:
            if k not in include:
                continue
        elif not _is_scalar(v):
            continue
        row[k] = _coerce_value(v)
    for k in exclude or ():
        row.pop(k, None)
    return row


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()



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


class AgentResultToDataFrameComponent(Component, Model, Resolvable):
    """Turn an agent/agentic_pipeline-step dict result into a DataFrame."""

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: Union[str, List[str]] = Field(
        description="Upstream asset key -- an agent asset, an agentic_pipeline step asset, or any dict-valued asset."
    )

    mode: Literal["one_row", "explode", "extract"] = Field(
        default="one_row",
        description=(
            "'one_row': one row per run, built from the result dict's own scalar fields "
            "(final_answer, iterations, tool_calls_made, ... whatever the upstream actually has). "
            "'explode': one row per entry of a list-valued field (see explode_field). "
            "'extract': parse `final_answer` as JSON (or a fenced ```json block) into columns/rows -- "
            "for agents prompted to return structured output."
        ),
    )
    explode_field: str = Field(
        default="tool_call_details",
        description="mode='explode' only: the list-valued field to explode, one output row per entry.",
    )
    on_parse_error: Literal["raise", "keep_raw"] = Field(
        default="raise",
        description=(
            "mode='extract' only. 'raise' (default): fail the run with a clear error on invalid JSON. "
            "'keep_raw': instead emit one row with the raw text under `final_answer_raw` and the error "
            "message under `parse_error`, so a single bad response doesn't break the asset."
        ),
    )

    include_columns: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Only keep these columns from each row (run/extra columns are always added). Default: every scalar field present.",
    )
    exclude_columns: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Drop these columns from the default/explicit column set.",
    )
    add_run_columns: bool = Field(
        default=True,
        description=(
            "Add run_id, partition_key, materialized_at, and asset_key columns to every row. "
            "Needed so history accumulates correctly in a table a dataframe_to_* sink appends to."
        ),
    )
    extra_columns: Optional[Dict[str, str]] = Field(
        default=None,
        description=(
            "Extra literal columns stamped onto every row, e.g. {pipeline: 'daily_digest', "
            "run_date: '{partition_key}'}. The literal substring '{partition_key}' is replaced "
            "with the current partition key (empty string when unpartitioned)."
        ),
    )

    group_name: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)

    retry_policy_max_retries: Optional[int] = Field(
        default=None,
        description="Max retries on asset failure. Useful for transient errors like network glitches or rate limits.",
    )
    retry_policy_delay_seconds: Optional[int] = Field(
        default=None,
        description="Seconds between retries (default 1).",
    )
    retry_policy_backoff: str = Field(
        default="exponential",
        description="Backoff strategy: 'linear' or 'exponential'.",
    )

    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'cron', 'static', 'multi', 'dynamic', or None for unpartitioned. Match the upstream agent asset's own partitioning.",
    )
    partition_cron_schedule: Optional[str] = Field(
        default=None,
        description="Required when partition_type='cron'. A cron expression restricting the date axis to a custom cadence, e.g. '0 0 * * 1-5' for weekdays only.",
    )
    partition_cron_fmt: Optional[str] = Field(
        default="%Y-%m-%d",
        description="strftime format for partition keys produced by partition_type='cron'. Only change this for a sub-daily cron_schedule.",
    )
    partition_end_offset: int = Field(
        default=0,
        description="Passed through to the underlying Daily/Weekly/Monthly/Hourly/cron partitions definition. 0 (default) excludes the current, still-in-progress window; 1 includes it.",
    )
    partition_timezone: Optional[str] = Field(
        default=None,
        description="IANA timezone for the underlying Daily/Weekly/Monthly/Hourly/cron partitions definition (defaults to UTC).",
    )
    backfill_policy: Optional[Literal["single_run", "multi_run"]] = Field(
        default=None,
        description=(
            "Backfill execution strategy for a partitioned asset. 'single_run' materializes an "
            "entire backfill range in one run (the partition-bridge handling above produces one "
            "row group per partition in the range). 'multi_run' launches one run per partition. "
            "Leave unset for Dagster's default. Ignored when partition_type is not set."
        ),
    )
    backfill_policy_max_partitions_per_run: Optional[int] = Field(
        default=None,
        description="For backfill_policy='multi_run' only: maximum partitions materialized per run. Leave unset for Dagster's default (1 partition per run).",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.",
    )
    partition_values: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning, e.g. 'customer_a,customer_b'.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition (when partition_type='dynamic'), e.g. 'tenants'.",
    )
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Multi-axis partition spec: list of {name, type, start, values, dynamic_partition_name} dicts. Overrides the flat partition fields.",
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(
            data,
            lists=("owners", "kinds", "deps", "include_columns", "exclude_columns"),
            csv=(),
            tags=("asset_tags",),
        )

    @model_validator(mode="before")
    @classmethod
    def _coerce_upstream_asset_keys(cls, data):
        return coerce_upstream_input(data, component=cls.__name__, mode="single", optional=False)

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Agent Result To DataFrame", editable=True)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values,
            self.dynamic_partition_name, self.partition_dimensions,
            cron_schedule=self.partition_cron_schedule,
            cron_fmt=self.partition_cron_fmt or "%Y-%m-%d",
            end_offset=self.partition_end_offset,
            timezone=self.partition_timezone,
        )

        backfill_policy = None
        if partitions_def is not None and self.backfill_policy == "single_run":
            from dagster import BackfillPolicy
            backfill_policy = BackfillPolicy.single_run()
        elif partitions_def is not None and self.backfill_policy == "multi_run":
            from dagster import BackfillPolicy
            backfill_policy = BackfillPolicy.multi_run(
                max_partitions_per_run=self.backfill_policy_max_partitions_per_run or 1
            )

        retry_policy = None
        if self.retry_policy_max_retries is not None:
            retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        mode = self.mode
        explode_field = self.explode_field
        on_parse_error = self.on_parse_error
        include_columns = list(self.include_columns) if self.include_columns else None
        exclude_columns = list(self.exclude_columns) if self.exclude_columns else None
        add_run_columns = self.add_run_columns
        extra_columns = dict(self.extra_columns or {})

        tags = dict(self.asset_tags or {})
        for k in (self.kinds or ["python", "pandas"]):
            tags[f"dagster/kind/{k}"] = ""

        @asset(
            key=AssetKey.from_user_string(asset_name),
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
            group_name=self.group_name,
            description=self.description or "Turn an agent/agentic_pipeline-step dict result into a DataFrame.",
            tags=tags,
            owners=self.owners or [],
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
            retry_policy=retry_policy,
            partitions_def=partitions_def,
            backfill_policy=backfill_policy,
        )
        def _asset(context: AssetExecutionContext, upstream: Any):
            coerced = _coerce_agent_results(upstream)

            built_rows: List[tuple] = []  # [(partition_key_or_None, row_dict), ...]

            if mode == "one_row":
                for pk, result in coerced:
                    result = result if isinstance(result, dict) else {"value": result}
                    built_rows.append((pk, _row_from_dict(result, include_columns, exclude_columns)))

            elif mode == "explode":
                saw_field = False
                for pk, result in coerced:
                    if not isinstance(result, dict):
                        continue
                    items = result.get(explode_field)
                    if items is None:
                        continue
                    if not isinstance(items, list):
                        raise ValueError(
                            f"agent_result_to_dataframe: explode_field={explode_field!r} is not a list "
                            f"(got {type(items).__name__}). Available fields: {sorted(result.keys())}"
                        )
                    saw_field = True
                    for item in items:
                        item_d = item if isinstance(item, dict) else {"value": item}
                        built_rows.append((pk, _row_from_dict(item_d, include_columns, exclude_columns)))
                if not saw_field and coerced:
                    sample_keys = sorted(coerced[0][1].keys()) if isinstance(coerced[0][1], dict) else []
                    raise ValueError(
                        f"agent_result_to_dataframe: no result had explode_field={explode_field!r}. "
                        f"Available fields on the first result: {sample_keys}"
                    )

            else:  # extract
                for pk, result in coerced:
                    raw = result.get("final_answer") if isinstance(result, dict) else result
                    raw_text = raw if isinstance(raw, str) else ("" if raw is None else str(raw))
                    try:
                        parsed = _extract_json_block(raw_text)
                    except Exception as e:
                        if on_parse_error == "keep_raw":
                            built_rows.append((pk, {"final_answer_raw": raw_text, "parse_error": str(e)}))
                            continue
                        raise ValueError(
                            f"agent_result_to_dataframe: mode='extract' could not parse final_answer as JSON: {e}"
                        ) from e
                    if isinstance(parsed, list):
                        for item in parsed:
                            item_d = item if isinstance(item, dict) else {"value": item}
                            built_rows.append((pk, _row_from_dict(item_d, include_columns, exclude_columns)))
                    else:
                        parsed_d = parsed if isinstance(parsed, dict) else {"value": parsed}
                        built_rows.append((pk, _row_from_dict(parsed_d, include_columns, exclude_columns)))

            final_rows: List[Dict[str, Any]] = []
            for pk, row in built_rows:
                resolved_pk = pk if pk is not None else (context.partition_key if context.has_partition_key else None)
                row = dict(row)
                for k, v in extra_columns.items():
                    row[k] = v.replace("{partition_key}", str(resolved_pk or "")) if isinstance(v, str) else v
                if add_run_columns:
                    row["run_id"] = context.run.run_id
                    row["partition_key"] = resolved_pk
                    row["materialized_at"] = _now_iso()
                    row["asset_key"] = asset_name
                final_rows.append(row)

            df = pd.DataFrame(final_rows)
            return Output(
                value=df,
                metadata={
                    "row_count": MetadataValue.int(len(df)),
                    "columns": MetadataValue.text(", ".join(df.columns.tolist())),
                    "mode": MetadataValue.text(mode),
                    "preview": MetadataValue.md(df.head(5).to_markdown(index=False) if len(df) else "_(no rows)_"),
                },
            )

        return Definitions(assets=[_asset])


# BEGIN scaffold partitions-def builder (inlined per component; keep copies identical, see tests/test_partition_port_scaffold.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from dagster import (  # noqa: E402,F401
    DailyPartitionsDefinition,
    DynamicPartitionsDefinition,
    HourlyPartitionsDefinition,
    MonthlyPartitionsDefinition,
    MultiPartitionsDefinition,
    StaticPartitionsDefinition,
    WeeklyPartitionsDefinition,
)


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
# END scaffold partitions-def builder
