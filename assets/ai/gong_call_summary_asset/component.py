"""Gong Call Summary Asset Component.

Reads a DataFrame of Gong calls (typically produced by
``GongCallsIngestionComponent``) and uses an OpenAI-compatible LLM to summarize
each call transcript into a short summary + bulleted action items. Emits an
enriched DataFrame with two new columns.

Row-wise pattern (one API call per row) mirrors ``langchain_chain_asset`` and
``anthropic_llm`` — favors simplicity over batching. For heavy volume switch to
a batched wrapper.
"""

import json
import os
from typing import Any, Dict, List, Optional

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
    Output,
    Resolvable,
    asset,
)
from pydantic import Field

SYSTEM_PROMPT = (
    "You are an assistant that reads sales call transcripts and produces a "
    "concise structured summary. Respond ONLY with a JSON object with exactly "
    "two keys: 'summary' (a 2-4 sentence paragraph) and 'action_items' (an "
    "array of short imperative strings, each a concrete follow-up). Do not "
    "include any prose outside the JSON."
)



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


class GongCallSummaryAssetComponent(Component, Model, Resolvable):
    """Summarize each call transcript into a short summary + action items.

    Example:

        ```yaml
        type: dagster_component_templates.GongCallSummaryAssetComponent
        attributes:
          asset_name: gong_call_summaries
          upstream_asset_key: gong_calls
          llm_provider: openai
          model: gpt-4o-mini
          api_key_env_var: OPENAI_API_KEY
          transcript_column: transcript
        ```
    """

    asset_name: str = Field(description="Name of the asset to create")

    upstream_asset_key: Union[str, List[str]] = Field(
        description="Upstream asset key providing a DataFrame with a transcript column",
    )

    llm_provider: str = Field(
        default="openai",
        description="LLM provider. Currently supports 'openai' and OpenAI-compatible endpoints via base_url.",
    )

    model: str = Field(
        default="gpt-4o-mini",
        description="Model identifier passed to the provider",
    )

    api_key_env_var: str = Field(
        default="OPENAI_API_KEY",
        description="Env var holding the LLM provider API key",
    )

    base_url: Optional[str] = Field(
        default=None,
        description="Override the OpenAI-compatible base URL (e.g. for Azure OpenAI, LiteLLM, local vLLM)",
    )

    transcript_column: str = Field(
        default="transcript",
        description="Name of the column containing the flattened transcript text",
    )

    summary_column: str = Field(
        default="call_summary",
        description="Name of the output column to write the summary text into",
    )

    action_items_column: str = Field(
        default="action_items",
        description="Name of the output column to write the action items list into",
    )

    max_tokens: int = Field(
        default=500,
        description="Maximum tokens per completion",
    )

    temperature: float = Field(
        default=0.2,
        description="Sampling temperature (lower = more deterministic)",
    )

    skip_empty: bool = Field(
        default=True,
        description="If true, rows where the transcript column is empty/NaN are skipped (empty outputs).",
    )

    description: Optional[str] = Field(default=None, description="Asset description")

    group_name: Optional[str] = Field(
        default="gong",
        description="Asset group for organization",
    )

    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', 'dynamic', or None for unpartitioned. A partitioned run sends only the rows in its partition.",
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
    partition_date_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter the rows upstream rows to the current date partition (time partitions).",
    )
    partition_static_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter upstream rows to the current static/dynamic partition key.",
    )
    partition_static_dim: Optional[str] = Field(
        default=None,
        description="Name of the static axis in a multi-partition, e.g. 'customer' or 'region'.",
    )

    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners — list of team names or email addresses",
    )

    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(
        default=None,
        description="Additional key-value tags to apply to the asset",
    )

    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds for the Dagster catalog. Defaults to ['gong', 'openai', 'python'].",
    )

    include_preview_metadata: bool = Field(
        default=True,
        description="Include sample data preview in metadata",
    )

    preview_rows: int = Field(
        default=10,
        ge=1,
        le=200,
        description="Rows to include in the preview metadata",
    )

    deps: Optional[List[str]] = Field(
        default=None,
        description="Lineage-only upstream asset keys (no data passed at runtime)",
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('owners', 'kinds', 'deps'), csv=(), tags=('asset_tags',))

    @model_validator(mode="before")
    @classmethod
    def _coerce_upstream_asset_keys(cls, data):
        return coerce_upstream_input(data, component=cls.__name__, mode="single", optional=False)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values, self.dynamic_partition_name, self.partition_dimensions
        )
        partition_date_column = self.partition_date_column
        partition_static_column = self.partition_static_column
        partition_static_dim = self.partition_static_dim

        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        llm_provider = self.llm_provider
        model = self.model
        api_key_env_var = self.api_key_env_var
        base_url = self.base_url
        transcript_column = self.transcript_column
        summary_column = self.summary_column
        action_items_column = self.action_items_column
        max_tokens = self.max_tokens
        temperature = self.temperature
        skip_empty = self.skip_empty
        description = self.description or f"LLM summaries + action items using {model}"
        group_name = self.group_name
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows

        _kinds = list(self.kinds or ["gong", llm_provider, "python"])
        _all_tags = dict(self.asset_tags or {})
        for _k in _kinds:
            _all_tags[f"dagster/kind/{_k}"] = ""

        owners = self.owners or []

        @asset(partitions_def=partitions_def, 
            key=AssetKey.from_user_string(asset_name),
            description=description,
            owners=owners,
            tags=_all_tags,
            group_name=group_name,
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def gong_call_summary_asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
            # Filter to current partition if partitioned
            if hasattr(upstream, "columns") and (context.has_partition_key or context.has_partition_key_range):
                import pandas as pd
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
            api_key = os.environ.get(api_key_env_var)
            if not api_key:
                raise ValueError(f"Env var {api_key_env_var!r} is not set")

            if llm_provider != "openai":
                raise ValueError(
                    f"llm_provider={llm_provider!r} not supported. "
                    "Use 'openai' (or an OpenAI-compatible endpoint via base_url)."
                )

            from openai import OpenAI
            client = OpenAI(api_key=api_key, base_url=base_url) if base_url else OpenAI(api_key=api_key)

            if transcript_column not in upstream.columns:
                raise ValueError(
                    f"Column {transcript_column!r} not found in upstream DataFrame. "
                    f"Available columns: {list(upstream.columns)}"
                )

            df = upstream.copy()
            summaries: List[str] = []
            action_items: List[List[str]] = []

            for idx, row in df.iterrows():
                transcript = row.get(transcript_column)
                if skip_empty and (not transcript or (isinstance(transcript, float) and pd.isna(transcript))):
                    summaries.append("")
                    action_items.append([])
                    continue

                resp = client.chat.completions.create(
                    model=model,
                    max_tokens=max_tokens,
                    temperature=temperature,
                    response_format={"type": "json_object"},
                    messages=[
                        {"role": "system", "content": SYSTEM_PROMPT},
                        {"role": "user", "content": str(transcript)},
                    ],
                )
                content = resp.choices[0].message.content or "{}"
                parsed = json.loads(content)
                summaries.append(str(parsed.get("summary", "")))
                items = parsed.get("action_items") or []
                action_items.append([str(i) for i in items] if isinstance(items, list) else [])

                if (len(summaries)) % 10 == 0:
                    context.log.info(f"Summarized {len(summaries)}/{len(df)} calls")

            df[summary_column] = summaries
            df[action_items_column] = action_items

            metadata: Dict[str, Any] = {
                "row_count": MetadataValue.int(len(df)),
                "model": MetadataValue.text(model),
                "provider": MetadataValue.text(llm_provider),
                "summarized_rows": MetadataValue.int(sum(1 for s in summaries if s)),
            }
            if include_preview and len(df) > 0:
                _cols = [c for c in [transcript_column, summary_column, action_items_column] if c in df.columns]
                _prev = df[_cols].head(preview_rows).copy()
                if transcript_column in _prev.columns:
                    _prev[transcript_column] = _prev[transcript_column].astype(str).str.slice(0, 200) + "…"
                metadata["preview"] = MetadataValue.md(_prev.to_markdown(index=False))
            return Output(value=df, metadata=metadata)

        return Definitions(assets=[gong_call_summary_asset])


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
# END scaffold partitions-def builder
