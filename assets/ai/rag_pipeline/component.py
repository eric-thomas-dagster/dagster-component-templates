"""RAG Pipeline Asset Component.

Complete Retrieval-Augmented Generation pipeline combining query, retrieval, and generation.
"""

import os
from typing import Any, Dict, List, Literal, Optional, Union

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
    asset_check,
    BackfillPolicy,
    AssetCheckResult,
    AssetCheckSeverity,
)
from pydantic import Field


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


def _require_env(name: str) -> str:
    """Read an env var via dagster.EnvVar, with an actionable message when it is unset."""
    from dagster import EnvVar

    value = EnvVar(name).get_value()
    if value is None:
        raise KeyError(
            f"Environment variable {name!r} is not set. Set it in the environment of the "
            "Dagster code location (or point the *_env_var field at one that is)."
        )
    return value



def _filter_df_to_partition(df: pd.DataFrame, context, partition_date_column, partition_static_column,
                            partition_static_dim=None) -> pd.DataFrame:
    """Keep only the upstream rows that belong to the partition(s) this run is for.

    `partition_date_column` / `partition_static_column` were declared (and documented as "Column used to filter upstream
    DataFrame to the current partition") but nothing used them, so every partition answered every upstream row. The date
    column is matched against `context.partition_time_window` (range-aware, so it is right for a one-partition run and a
    multi-partition run alike); the static column against the run's partition key(s), or just the `partition_static_dim`
    part of a multi-dimensional key. Both set => both apply. Neither set => unchanged (use a partitioned upstream)."""
    if not (context.has_partition_key or context.has_partition_key_range):
        return df
    out = df
    if partition_date_column is not None and partition_date_column in out.columns:
        try:
            window = context.partition_time_window
        except Exception:
            window = None  # no time dimension (static / dynamic partitions)
        if window is not None:
            dates = pd.to_datetime(out[partition_date_column], errors="coerce", utc=True)
            out = out[(dates >= pd.Timestamp(window.start)) & (dates < pd.Timestamp(window.end))]
    if partition_static_column is not None and partition_static_column in out.columns:
        keys = set()
        for k in context.partition_keys:
            by_dim = getattr(k, "keys_by_dimension", None)
            keys.add(str(by_dim[partition_static_dim]) if by_dim and partition_static_dim in by_dim else str(k))
        out = out[out[partition_static_column].astype(str).isin(keys)]
    return out


_PARTITION_PLACEHOLDERS = ("{partition_key}", "{partition.")


def _build_backfill_policy(partitions_def, policy: Optional[str], max_per_run: Optional[int], query_prompt_template: Optional[str]):
    """Validate `backfill_policy` / `backfill_policy_max_partitions_per_run` and return a dagster BackfillPolicy (or None).

    Rows are answered independently, so one run CAN cover several partitions (single_run, or multi_run with
    max_partitions_per_run > 1) as long as nothing in the run needs to know WHICH partition a row belongs to. The one thing
    that does is `query_prompt_template` with `{partition_key}` / `{partition.<name>}`: those resolve from the run's single
    partition key, so such a template is incompatible with multi-partition runs and is rejected here."""
    where = "RAGPipelineComponent"
    if max_per_run is not None and policy != "multi_run":
        raise ValueError(f"{where}: backfill_policy_max_partitions_per_run only applies to backfill_policy='multi_run'")
    if policy is None or partitions_def is None:
        return None  # unset, or ignored because the component is not partitioned
    if policy not in ("single_run", "multi_run"):
        raise ValueError(f"{where}: backfill_policy must be 'single_run' or 'multi_run', got {policy!r}")
    if max_per_run is not None and (isinstance(max_per_run, bool) or not isinstance(max_per_run, int) or max_per_run < 1):
        raise ValueError(f"{where}: backfill_policy_max_partitions_per_run must be an integer >= 1")
    multi_partition_run = policy == "single_run" or (max_per_run or 1) > 1
    if multi_partition_run and query_prompt_template and any(t in query_prompt_template for t in _PARTITION_PLACEHOLDERS):
        raise ValueError(
            f"{where}: query_prompt_template uses {{partition_key}} / {{partition.<name>}}, which resolve from one partition per "
            "run, so backfill_policy='single_run' (or multi_run with backfill_policy_max_partitions_per_run > 1) would answer "
            "every row with an empty partition key; use multi_run with one partition per run, or drop the placeholders.")
    if policy == "single_run":
        return BackfillPolicy.single_run()
    return BackfillPolicy.multi_run(max_partitions_per_run=max_per_run or 1)


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


class RAGPipelineComponent(Component, Model, Resolvable):
    """One YAML, whole RAG pipeline — retrieve, (optional) rerank, generate.

    Pipeline shape: `personas:` + `retrievers:` + `rerankers:` + `steps:`.
    Every row in the upstream DataFrame's `query_column` gets its own
    trip through the ops chain. Family sibling of AgenticPipeline /
    MLPipeline / PolarsPipeline.

    Ops menu:
      - retrieve         — vector search against a named retriever
      - hybrid_search    — BM25 + vector combined via RRF
      - rerank           — Voyage rerank-2.5 / Cohere rerank-3.5 over docs
      - expand_query     — HyDE — LLM writes a hypothetical answer to embed
      - generate         — LLM answer using retrieved docs as context;
                           full reasoning + prompt-caching support

    Example:
        ```yaml
        type: dagster_community_components.RAGPipelineComponent
        attributes:
          asset_name: docs_qa
          upstream_asset_key: user_questions

          retrievers:
            docs_vs:
              kind: chromadb
              collection_name: product_docs
              path: ./chroma_db
              embedding:
                provider: voyage
                model: voyage-3-large
                input_type: query
                api_key_env_var: VOYAGE_API_KEY

          rerankers:
            voyage_rr:
              provider: voyage
              model: rerank-2.5
              api_key_env_var: VOYAGE_API_KEY

          steps:
            - {id: retrieved, op: retrieve, retriever: docs_vs, top_k: 20}
            - {id: reranked, op: rerank, source: retrieved, reranker: voyage_rr, top_k: 5}
            - id: answered
              op: generate
              source: reranked
              provider: openai
              model: gpt-4o-mini
              api_key_env_var: OPENAI_API_KEY
        ```
    """

    asset_name: str = Field(description="Output asset name")
    include_sources: bool = Field(
        default=True,
        description="Emit the retrieved docs list as `sources_column` on each output row.",
    )
    include_reasoning: bool = Field(default=False, description="Reasoning models (Qwen3, DeepSeek-R1, ...) inline <think>...</think>. The thinking is always stripped from answers and from HyDE expanded queries; when true it is also written to an extra '<answer_column>_reasoning' column (the final step's thinking).")
    query_column: Union[str, int] = Field(default="query", description="Column name containing query text")
    answer_column: Union[str, int] = Field(default="answer", description="Column name for generated answers")
    sources_column: Union[str, int] = Field(default="sources", description="Column name for retrieved source documents")
    description: Optional[str] = Field(default=None, description="Asset description")
    group_name: Optional[str] = Field(default=None, description="Asset group")
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
    backfill_policy: Optional[Literal["single_run", "multi_run"]] = Field(
        default=None,
        description=(
            "Backfill execution strategy for a partitioned asset. 'multi_run' launches one run per partition (optionally "
            "capped via backfill_policy_max_partitions_per_run). 'single_run' answers a whole backfill range in one run (rows "
            "are independent; the upstream is read for the whole range and filtered by partition_date_column / "
            "partition_static_column if set) and needs an IO manager that can store a multi-partition output. Both are "
            "rejected at load time with a query_prompt_template that uses {partition_key} / {partition.<name>} when a run "
            "could cover several partitions. Leave unset for Dagster's default (one run per partition). Ignored when no "
            "partitioning is set."
        ),
    )
    backfill_policy_max_partitions_per_run: Optional[int] = Field(
        default=None,
        description="For backfill_policy='multi_run' only: maximum partitions materialized per run. Leave unset for Dagster's default (1 partition per run).",
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
    upstream_asset_key: Union[str, List[str]] = Field(description="Upstream asset key providing a DataFrame with query text")

    # ── OPS-BASED MODE (opt-in via `steps:`) ────────────────────────────
    #
    # When `steps:` is set, the pipeline runs as a chain of named ops
    # (retrieve → rerank → generate → …) instead of the monolithic single-
    # asset shape. Enables multi-hop retrieval, hybrid search, reranking,
    # query expansion, and reasoning-model + prompt-caching support on the
    # generation step. All backward-compat: when `steps:` is empty/unset,
    # the pipeline behaves exactly as before.

    personas: Optional[Dict[str, Dict[str, Any]]] = Field(
        default=None,
        description=(
            "Named reusable LLM sub-configs (ops mode only). Each persona "
            "bundles `{provider, model, api_key_env_var, api_base_env_var, "
            "system_prompt, temperature, max_tokens, reasoning_effort, "
            "thinking_budget, prompt_caching}`. Reference from a step via "
            "`persona: <name>`; the persona's fields are merged into the "
            "step (explicit inline fields win). Applies to `generate`, "
            "`expand_query`, and `refine` ops. Same shape as "
            "AgenticPipelineComponent."
        ),
    )
    retrievers: Optional[Dict[str, Dict[str, Any]]] = Field(
        default=None,
        description=(
            "Named reusable vector-store configs (ops mode only). Each "
            "entry declares `{kind: chromadb|pinecone|qdrant|pgvector|"
            "weaviate, collection_name / index_name, connection / path / "
            "url, embedding: {provider, model, api_key_env_var}}`. "
            "Reference from a step via `retriever: <name>`."
        ),
    )
    rerankers: Optional[Dict[str, Dict[str, Any]]] = Field(
        default=None,
        description=(
            "Named reusable reranker configs (ops mode only). Each entry: "
            "`{provider: voyage|cohere, model, api_key_env_var}`. "
            "Reference from a `rerank` step via `reranker: <name>`."
        ),
    )
    steps: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description=(
            "Ordered ops chain (opt-in — unlocks the ops-based mode). "
            "Each entry: `{id, op, ...op-specific}`. Supported ops: "
            "`retrieve` (vector search across a named retriever), "
            "`hybrid_search` (BM25 + vector combined via RRF), "
            "`rerank` (Voyage rerank-2.5 or Cohere rerank-3.5 over "
            "retrieved docs), `expand_query` (HyDE-style — LLM generates "
            "a hypothetical document to embed instead of the query), "
            "`generate` (final answer using retrieved docs as context). "
            "See README for per-op field reference."
        ),
    )
    query_prompt_template: Optional[str] = Field(
        default=None,
        description=(
            "Ops mode only. Template applied to `query_column` values before "
            "they're fed into the first step. Placeholders: `{query}` "
            "(raw query text), `{partition_key}`, `{partition.<name>}`. "
            "Leave unset (default) to pass queries through unchanged."
        ),
    )

    receipts: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Optional closed-loop context receipts (ops mode). `{resource_key (required: a registered "
            "ContextReceiptStoreComponent resource), context_name (default: asset_name), caller_id, purpose, "
            "policy_version, fail_on_error (default false), rerank: {prior_weight: 0.2, explore_fraction: 0.1, "
            "fetch_multiplier: 3}}`. Every row's final retrieval is recorded as a receipt (item ids and provenance "
            "only, never text) and its id is written to a `receipt_id` column. With `rerank`, retrieve / "
            "hybrid_search over-fetch top_k*fetch_multiplier candidates and reorder them by learned item priors, "
            "reserving an exploration slot. `shadow: {retriever (a candidate entry in `retrievers`), fraction (0,1], seed, "
            "label, replaces, code_version_override, live_code_version, candidate_code_version, source_asset_key, "
            "min_outcomes, margin, window_days}` serves that share of queries from the candidate retriever (falling back "
            "to the live one if it fails), records those receipts with purpose 'shadow:<label>', and with both code "
            "versions named adds the WARN check `shadow_vs_live`. Omit for unchanged behaviour."
        ),
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
        if not self.steps:
            raise ValueError(
                f"RAGPipelineComponent {self.asset_name!r}: `steps:` is required. "
                "Define a chain of ops — at minimum `retrieve` + `generate`. "
                "See the component README for examples."
            )
        _validate_receipts(self.receipts, self.asset_name, self.retrievers, self.steps)
        return _build_ops_mode(self)





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


# ═════════════════════════════════════════════════════════════════════
# Ops-based mode (opt-in via `steps:`)
# ═════════════════════════════════════════════════════════════════════
#
# The ops-based mode decomposes RAG into named steps
# (retrieve → rerank → generate → ...) each producing a state dict that
# subsequent steps consume by id. Backed by LiteLLM for the generation
# side and native provider SDKs for the retrieval side. Fully opt-in:
# absent `steps:`, the legacy monolithic mode above runs unchanged.


# ── Persona / retriever / reranker resolution ─────────────────────────

_PERSONA_FIELDS = (
    "provider", "model", "api_key_env_var", "api_base_env_var",
    "system_prompt", "temperature", "max_tokens",
    "reasoning_effort", "thinking_budget", "prompt_caching",
)


def _merge_persona(target: dict, persona: dict) -> dict:
    """Merge persona fields into `target`. Explicit inline fields on
    `target` win — persona is a defaults-provider."""
    merged = dict(target)
    for k in _PERSONA_FIELDS:
        if k in persona and merged.get(k) is None:
            merged[k] = persona[k]
    merged.pop("persona", None)
    return merged


def _resolve_persona(step: dict, personas: Optional[Dict[str, Dict[str, Any]]]) -> dict:
    """If step has `persona: <name>`, merge that persona's fields into step."""
    if not personas or "persona" not in step:
        return step
    name = step["persona"]
    if name not in personas:
        raise ValueError(
            f"step {step.get('id', '?')!r} references persona {name!r} not in "
            f"personas: block. Available: {sorted(personas.keys())}"
        )
    return _merge_persona(step, personas[name])


def _resolve_retriever(name: str, retrievers: Optional[Dict[str, Dict[str, Any]]]) -> dict:
    """Look up a retriever spec by name."""
    if not retrievers or name not in retrievers:
        raise ValueError(
            f"retriever {name!r} not declared in `retrievers:` block. "
            f"Available: {sorted((retrievers or {}).keys())}"
        )
    return retrievers[name]


def _resolve_reranker(name: str, rerankers: Optional[Dict[str, Dict[str, Any]]]) -> dict:
    """Look up a reranker spec by name."""
    if not rerankers or name not in rerankers:
        raise ValueError(
            f"reranker {name!r} not declared in `rerankers:` block. "
            f"Available: {sorted((rerankers or {}).keys())}"
        )
    return rerankers[name]


# ── LLM completion (LiteLLM + reasoning + caching) ────────────────────
#
# Mirrors AgenticPipelineComponent._completion. Kept self-contained per
# the "components are self-contained" convention — no shared helpers.

def _rag_completion(
    *,
    model: str,
    system_prompt: Optional[str],
    user_prompt: str,
    api_key_env_var: Optional[str],
    api_base_env_var: Optional[str],
    temperature: float,
    max_tokens: int,
    reasoning_effort: Optional[str] = None,
    thinking_budget: Optional[int] = None,
    prompt_caching: bool = False,
) -> Dict[str, Any]:
    """Thin LiteLLM wrapper; returns
    {"content": str, "usage": {...}, "cost_usd": float, "latency_ms": int,
     "tokens_total": int, "cache_read_tokens": int, "cache_creation_tokens": int}."""
    import time as _time
    try:
        import litellm
    except ImportError:
        raise ImportError("rag_pipeline ops mode requires litellm>=1.30.0")
    litellm.drop_params = True

    m_lower = model.lower()
    is_openai_ish = m_lower.startswith(("gpt-", "o1", "o3", "o4", "openai/", "azure/", "groq/"))
    is_gemini = m_lower.startswith(("gemini/", "google/", "vertex_ai/gemini"))
    is_anthropic = (
        "claude" in m_lower or m_lower.startswith(("anthropic/", "bedrock/anthropic."))
    )

    messages: List[Dict[str, Any]] = []
    if system_prompt:
        if prompt_caching and is_anthropic:
            messages.append({
                "role": "system",
                "content": [
                    {"type": "text", "text": system_prompt,
                     "cache_control": {"type": "ephemeral"}}
                ],
            })
        else:
            messages.append({"role": "system", "content": system_prompt})
    messages.append({"role": "user", "content": user_prompt})

    kwargs: Dict[str, Any] = {
        "model": model,
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max_tokens,
    }
    if api_key_env_var and os.environ.get(api_key_env_var):
        kwargs["api_key"] = _require_env(api_key_env_var)
    if api_base_env_var and os.environ.get(api_base_env_var):
        kwargs["api_base"] = _require_env(api_base_env_var)
    if reasoning_effort is not None and (is_openai_ish or is_gemini):
        kwargs["reasoning_effort"] = reasoning_effort
    if thinking_budget is not None:
        if is_gemini:
            kwargs["thinking_budget"] = int(thinking_budget)
        elif is_anthropic:
            kwargs["thinking"] = {"type": "enabled", "budget_tokens": int(thinking_budget)}

    t0 = _time.time()
    response = litellm.completion(**kwargs)
    latency_ms = int((_time.time() - t0) * 1000)

    msg = response.choices[0].message
    # Reasoning models inline <think>...</think>; it must not leak into answers or HyDE queries.
    content, reasoning = split_reasoning(msg.content)
    usage = None
    u = getattr(response, "usage", None)
    if u is not None:
        try:
            usage = u.model_dump()
        except AttributeError:
            try:
                usage = dict(u)
            except (TypeError, ValueError):
                usage = None

    cost_usd = 0.0
    try:
        cost_usd = float(litellm.completion_cost(completion_response=response))
    except Exception:
        cost_usd = 0.0

    tokens_total = 0
    cache_read_tokens = 0
    cache_creation_tokens = 0
    if usage is not None:
        tt = usage.get("total_tokens")
        if isinstance(tt, (int, float)):
            tokens_total = int(tt)
        for k in ("cache_read_input_tokens", "cache_read"):
            v = usage.get(k)
            if isinstance(v, (int, float)):
                cache_read_tokens = int(v)
                break
        for k in ("cache_creation_input_tokens", "cache_creation"):
            v = usage.get(k)
            if isinstance(v, (int, float)):
                cache_creation_tokens = int(v)
                break

    return {
        "content": content,
        "reasoning": reasoning,
        "usage": usage,
        "cost_usd": cost_usd,
        "latency_ms": latency_ms,
        "tokens_total": tokens_total,
        "cache_read_tokens": cache_read_tokens,
        "cache_creation_tokens": cache_creation_tokens,
    }


# ── Provider dispatchers ──────────────────────────────────────────────


def _embed_query(embedding_cfg: dict, query: str) -> List[float]:
    """Embed a single query using the retriever's declared embedding provider."""
    provider = (embedding_cfg or {}).get("provider", "openai")
    model = (embedding_cfg or {}).get("model", "text-embedding-3-small")
    api_key = os.environ.get((embedding_cfg or {}).get("api_key_env_var") or "OPENAI_API_KEY")

    if provider == "openai":
        client = _make_openai_client(api_key)
        return client.embeddings.create(model=model, input=[query]).data[0].embedding
    if provider == "voyage":
        try:
            import voyageai
        except ImportError:
            raise ImportError("voyage embedding requires: pip install 'voyageai>=0.3.0'")
        client = voyageai.Client(api_key=api_key)
        input_type = (embedding_cfg or {}).get("input_type", "query")
        result = client.embed(texts=[query], model=model, input_type=input_type)
        return result.embeddings[0]
    if provider == "cohere":
        try:
            import cohere
        except ImportError:
            raise ImportError("cohere embedding requires: pip install cohere")
        client = cohere.Client(api_key=api_key)
        r = client.embed(texts=[query], model=model, input_type="search_query")
        return list(r.embeddings[0])
    if provider == "sentence_transformers":
        from sentence_transformers import SentenceTransformer
        st = SentenceTransformer(model)
        return st.encode([query])[0].tolist()
    raise ValueError(f"unsupported embedding provider: {provider!r}")


def _vector_search(retriever: dict, embedding: List[float], top_k: int) -> List[Dict[str, Any]]:
    """Run a vector search against a named retriever. Returns list of
    `{text, metadata, score}` dicts."""
    kind = retriever.get("kind")
    if kind == "chromadb":
        import chromadb
        client = chromadb.PersistentClient(path=retriever.get("path") or "./chroma_db")
        collection = client.get_collection(name=retriever["collection_name"])
        results = collection.query(query_embeddings=[embedding], n_results=top_k)
        docs = []
        for i in range(len(results["ids"][0])):
            docs.append({
                "text": results["documents"][0][i] if "documents" in results else "",
                "metadata": results["metadatas"][0][i] if "metadatas" in results else {},
                "score": 1.0 - float(results["distances"][0][i]) if "distances" in results else 0.0,
            })
        return docs
    if kind == "pinecone":
        from pinecone import Pinecone
        api_key = os.environ.get(retriever.get("api_key_env_var") or "PINECONE_API_KEY")
        pc = Pinecone(api_key=api_key)
        index = pc.Index(retriever["index_name"])
        r = index.query(
            vector=embedding, top_k=top_k, include_metadata=True,
            namespace=retriever.get("namespace"),
        )
        return [
            {"text": m.get("metadata", {}).get("text", ""),
             "metadata": m.get("metadata", {}),
             "score": float(m.get("score", 0.0))}
            for m in r["matches"]
        ]
    if kind == "qdrant":
        from qdrant_client import QdrantClient
        url_env = retriever.get("url_env_var")
        url = os.environ.get(url_env) if url_env else retriever.get("url")
        api_key = os.environ.get(retriever.get("api_key_env_var") or "QDRANT_API_KEY")
        client = QdrantClient(url=url, api_key=api_key) if url else QdrantClient(path=retriever.get("path") or "./qdrant_db")
        hits = client.search(
            collection_name=retriever["collection_name"],
            query_vector=embedding, limit=top_k,
        )
        return [
            {"text": (h.payload or {}).get("text", ""),
             "metadata": h.payload or {},
             "score": float(h.score)}
            for h in hits
        ]
    if kind == "pgvector":
        # Uses psycopg + a simple ORDER BY <cosine> LIMIT k. Assumes
        # column names {content_column, embedding_column, metadata_column?}.
        import psycopg2
        conn_env = retriever.get("connection_env_var") or "PGVECTOR_URL"
        conn = psycopg2.connect(_require_env(conn_env))
        try:
            table = retriever["table"]
            emb_col = retriever.get("embedding_column", "embedding")
            content_col = retriever.get("content_column", "content")
            meta_col = retriever.get("metadata_column")
            select_cols = f"{content_col}, 1 - ({emb_col} <=> %s::vector) AS score"
            if meta_col:
                select_cols += f", {meta_col}"
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT {select_cols} FROM {table} ORDER BY {emb_col} <=> %s::vector LIMIT %s",
                    (embedding, embedding, top_k),
                )
                rows = cur.fetchall()
            docs = []
            for row in rows:
                d = {"text": row[0], "score": float(row[1]), "metadata": {}}
                if meta_col:
                    d["metadata"] = row[2] if isinstance(row[2], dict) else {}
                docs.append(d)
            return docs
        finally:
            conn.close()
    if kind == "weaviate":
        import weaviate
        url_env = retriever.get("url_env_var") or "WEAVIATE_URL"
        client = weaviate.Client(url=_require_env(url_env))
        class_name = retriever["collection_name"]
        result = (client.query
                  .get(class_name, [retriever.get("content_property", "text")])
                  .with_near_vector({"vector": embedding})
                  .with_limit(top_k)
                  .with_additional(["distance"])
                  .do())
        objs = result.get("data", {}).get("Get", {}).get(class_name, [])
        content_prop = retriever.get("content_property", "text")
        return [
            {"text": o.get(content_prop, ""), "metadata": o,
             "score": 1.0 - float(o.get("_additional", {}).get("distance", 0.0))}
            for o in objs
        ]
    raise ValueError(f"unsupported retriever kind: {kind!r}")


def _rerank_docs(reranker: dict, query: str, docs: List[Dict[str, Any]], top_k: int) -> List[Dict[str, Any]]:
    """Rerank candidate docs with Voyage or Cohere. Preserves original
    metadata; overwrites `score` with the reranker's relevance score."""
    provider = reranker.get("provider", "voyage")
    model = reranker.get("model", "rerank-2.5" if provider == "voyage" else "rerank-3.5")
    api_key = os.environ.get(reranker.get("api_key_env_var") or ("VOYAGE_API_KEY" if provider == "voyage" else "COHERE_API_KEY"))
    doc_texts = [d.get("text", "") for d in docs]

    if provider == "voyage":
        try:
            import voyageai
        except ImportError:
            raise ImportError("voyage rerank requires: pip install 'voyageai>=0.3.0'")
        client = voyageai.Client(api_key=api_key)
        r = client.rerank(query=query, documents=doc_texts, model=model, top_k=top_k)
        # r.results is a list of RerankingResult with `.index` + `.relevance_score`
        out = []
        for res in r.results:
            base = docs[res.index]
            out.append({**base, "score": float(res.relevance_score)})
        return out
    if provider == "cohere":
        try:
            import cohere
        except ImportError:
            raise ImportError("cohere rerank requires: pip install cohere")
        client = cohere.Client(api_key=api_key)
        r = client.rerank(query=query, documents=doc_texts, model=model, top_n=top_k)
        out = []
        for res in r.results:
            base = docs[res.index]
            out.append({**base, "score": float(res.relevance_score)})
        return out
    raise ValueError(f"unsupported rerank provider: {provider!r}")


def _bm25_search(docs: List[Dict[str, Any]], query: str, top_k: int) -> List[Dict[str, Any]]:
    """Rank a corpus of already-loaded docs with BM25. Returns top_k."""
    try:
        from rank_bm25 import BM25Okapi
    except ImportError:
        raise ImportError("hybrid_search requires: pip install rank_bm25")
    tokenized_corpus = [d.get("text", "").lower().split() for d in docs]
    bm25 = BM25Okapi(tokenized_corpus)
    scores = bm25.get_scores(query.lower().split())
    ranked = sorted(zip(scores, docs), key=lambda x: x[0], reverse=True)[:top_k]
    return [{**d, "score": float(s)} for s, d in ranked]


def _rrf_combine(ranked_lists: List[List[Dict[str, Any]]], k: int = 60, top_k: int = 10) -> List[Dict[str, Any]]:
    """Reciprocal Rank Fusion — combine N ranked lists into one.
    Dedupes by doc text; assigns each doc a fused score of Σ 1/(k + rank)."""
    fused: Dict[str, Dict[str, Any]] = {}
    for lst in ranked_lists:
        for rank, doc in enumerate(lst, start=1):
            key = doc.get("text", "")[:500]  # cheap dedupe key
            entry = fused.get(key) or {**doc, "score": 0.0}
            entry["score"] = float(entry["score"]) + 1.0 / (k + rank)
            fused[key] = entry
    ordered = sorted(fused.values(), key=lambda d: d["score"], reverse=True)
    return ordered[:top_k]


# ── Context receipts (optional `receipts:` block) ────────────────────

_RECEIPT_KEYS = {"resource_key", "context_name", "caller_id", "purpose", "policy_version", "fail_on_error", "rerank", "shadow"}
_SHADOW_KEYS = {"retriever", "replaces", "fraction", "seed", "label", "code_version_override",
                "live_code_version", "candidate_code_version", "source_asset_key",
                "min_outcomes", "margin", "window_days"}
_RECEIPT_RERANK_KEYS = {"prior_weight", "explore_fraction", "fetch_multiplier"}


def _validate_receipts(cfg: Optional[Dict[str, Any]], asset_name: str,
                       retrievers: Optional[Dict[str, Any]] = None, steps: Optional[List[Dict[str, Any]]] = None) -> None:
    """Fail at build_defs on a malformed `receipts:` block (None = feature off)."""
    if cfg is None:
        return
    where = f"RAGPipelineComponent {asset_name!r}: receipts"
    if not isinstance(cfg, dict):
        raise ValueError(f"{where}: must be a mapping, got {type(cfg).__name__}")
    unknown = set(cfg) - _RECEIPT_KEYS
    if unknown:
        raise ValueError(f"{where}: unknown key(s) {sorted(unknown)}; valid keys: {sorted(_RECEIPT_KEYS)}")
    rk = cfg.get("resource_key")
    if not isinstance(rk, str) or not rk.strip():
        raise ValueError(f"{where}.resource_key is required (the key of a registered ContextReceiptStoreComponent resource)")
    for k in ("context_name", "caller_id", "purpose", "policy_version"):
        if cfg.get(k) is not None and (not isinstance(cfg[k], str) or not cfg[k].strip()):
            raise ValueError(f"{where}.{k} must be a non-empty string")
    if not isinstance(cfg.get("fail_on_error", False), bool):
        raise ValueError(f"{where}.fail_on_error must be a boolean")
    _validate_shadow(cfg.get("shadow"), where, retrievers, steps)
    rr = cfg.get("rerank")
    if rr is None:
        return
    if not isinstance(rr, dict):
        raise ValueError(f"{where}.rerank must be a mapping")
    unknown = set(rr) - _RECEIPT_RERANK_KEYS
    if unknown:
        raise ValueError(f"{where}.rerank: unknown key(s) {sorted(unknown)}; valid keys: {sorted(_RECEIPT_RERANK_KEYS)}")
    for k in ("prior_weight", "explore_fraction"):
        v = rr.get(k)
        if v is not None and (isinstance(v, bool) or not isinstance(v, (int, float))):
            raise ValueError(f"{where}.rerank.{k} must be a number")
    if not 0 <= float(rr.get("explore_fraction", 0.1)) <= 1:
        raise ValueError(f"{where}.rerank.explore_fraction must be between 0 and 1")
    fm = rr.get("fetch_multiplier", 3)
    if isinstance(fm, bool) or not isinstance(fm, int) or fm < 1:
        raise ValueError(f"{where}.rerank.fetch_multiplier must be an integer >= 1")


def _shadow_replaces(shadow: Dict[str, Any], steps: Optional[List[Dict[str, Any]]]) -> Optional[str]:
    """The live retriever the candidate replaces: `shadow.replaces`, else the only retriever the steps use."""
    if shadow.get("replaces"):
        return shadow["replaces"]
    used = {st["retriever"] for st in (steps or []) if st.get("op") in ("retrieve", "hybrid_search") and st.get("retriever")}
    return next(iter(used)) if len(used) == 1 else None


def _validate_shadow(sh: Any, where: str, retrievers: Optional[Dict[str, Any]], steps: Optional[List[Dict[str, Any]]]) -> None:
    """Fail at build_defs on a malformed `receipts.shadow` block (None = no traffic split)."""
    if sh is None:
        return
    where = f"{where}.shadow"
    if not isinstance(sh, dict):
        raise ValueError(f"{where} must be a mapping")
    unknown = set(sh) - _SHADOW_KEYS
    if unknown:
        raise ValueError(f"{where}: unknown key(s) {sorted(unknown)}; valid keys: {sorted(_SHADOW_KEYS)}")
    cand = sh.get("retriever")
    if not isinstance(cand, str) or not cand.strip():
        raise ValueError(f"{where}.retriever is required (the name of a retriever in `retrievers` that points at the candidate index)")
    if retrievers is not None and cand not in retrievers:
        raise ValueError(f"{where}.retriever {cand!r} is not declared in `retrievers`; available: {sorted(retrievers)}")
    fr = sh.get("fraction")
    if isinstance(fr, bool) or not isinstance(fr, (int, float)) or not 0 < float(fr) <= 1:
        raise ValueError(f"{where}.fraction is required and must be a number in (0, 1]")
    seed = sh.get("seed")
    if seed is not None and (isinstance(seed, bool) or not isinstance(seed, (int, str))):
        raise ValueError(f"{where}.seed must be an int or a string")
    for k in ("label", "replaces", "code_version_override", "live_code_version", "candidate_code_version", "source_asset_key"):
        if sh.get(k) is not None and (not isinstance(sh[k], str) or not sh[k].strip()):
            raise ValueError(f"{where}.{k} must be a non-empty string")
    if steps is not None:
        live = _shadow_replaces(sh, steps)
        if live is None:
            raise ValueError(f"{where}.replaces is required: the retrieve/hybrid_search steps use several retrievers (or none), "
                             "so say which live retriever the candidate stands in for")
        if live == cand:
            raise ValueError(f"{where}.retriever must differ from the live retriever {live!r}")
        if not any(st.get("op") in ("retrieve", "hybrid_search") and st.get("retriever") == live for st in steps):
            raise ValueError(f"{where}.replaces {live!r} is not used by any retrieve/hybrid_search step")
    cv = [sh.get("live_code_version"), sh.get("candidate_code_version")]
    if any(cv) and not all(cv):
        raise ValueError(f"{where}: live_code_version and candidate_code_version must be set together (they enable the shadow_vs_live check)")
    if all(cv):
        if cv[0] == cv[1]:
            raise ValueError(f"{where}: live_code_version and candidate_code_version must differ")
        if not sh.get("source_asset_key"):
            raise ValueError(f"{where}.source_asset_key is required with the code versions: the key of the index asset whose chunks carry them")
    for k in ("min_outcomes", "window_days"):
        v = sh.get(k)
        if v is not None and (isinstance(v, bool) or not isinstance(v, (int, float)) or v <= 0):
            raise ValueError(f"{where}.{k} must be a positive number")
    m = sh.get("margin")
    if m is not None and (isinstance(m, bool) or not isinstance(m, (int, float)) or m < 0):
        raise ValueError(f"{where}.margin must be a number >= 0")


def _shadow_pick(shadow: Optional[Dict[str, Any]], raw_query: str) -> bool:
    """Whether this query is served by the candidate. With a seed the answer is a pure function of (seed, query)."""
    if not shadow:
        return False
    fraction = float(shadow["fraction"])
    if shadow.get("seed") is None:
        import random
        return random.random() < fraction
    import hashlib
    u = int(hashlib.sha256(f"{shadow['seed']}|{raw_query}".encode("utf-8")).hexdigest()[:15], 16) / 16 ** 15
    return u < fraction


def _shadow_label(shadow: Dict[str, Any]) -> str:
    return shadow.get("label") or shadow["retriever"]


def _search_with_shadow(step: dict, state: Dict[str, Any], comp: "RAGPipelineComponent", context,
                        query: str, k: int) -> Tuple[List[Dict[str, Any]], bool]:
    """Embed + vector-search for a retrieve/hybrid step. For a query picked for the shadow split, and a step that uses
    the live retriever being replaced, search the candidate retriever (with its own embedding config) instead; if the
    candidate fails, log and fall back to the live retriever. Returns (docs, served_by_candidate)."""
    shadow = (comp.receipts or {}).get("shadow")
    live = _resolve_retriever(step["retriever"], comp.retrievers)
    if shadow and state.get("__shadow__") and step["retriever"] == _shadow_replaces(shadow, comp.steps):
        try:
            cand = _resolve_retriever(shadow["retriever"], comp.retrievers)
            return _vector_search(cand, _embed_query(cand.get("embedding") or {}, query), k), True
        except Exception as exc:
            context.log.warning(f"shadow: candidate retriever {shadow['retriever']!r} failed ({type(exc).__name__}: {exc}); "
                                f"serving this query from the live retriever {step['retriever']!r}")
            state["__shadow_failed__"] = state.get("__shadow_failed__", 0) + 1
    return _vector_search(live, _embed_query(live.get("embedding") or {}, query), k), False


def compare_recipes(by_code_version: Dict[str, Dict[str, Any]], live: str, candidate: str,
                    min_outcomes: int = 10, margin: float = 0.05) -> Dict[str, Any]:
    """Judge a candidate recipe against the live one from `store.outcomes_by_code_version(...)`.

    Only receipts that actually received an outcome count (`outcomes`, not `receipts`). Fewer than `min_outcomes` on
    either side is `insufficient_evidence`; otherwise the candidate must beat (or lose to) the live mean by MORE than
    `margin` to be `candidate_better` / `live_better`, else `inconclusive`. This is a threshold on the mean outcome
    difference, not a significance test: pick min_outcomes and margin to suit your outcome noise."""
    def side(cv):
        e = (by_code_version or {}).get(cv) or {}
        return int(e.get("outcomes") or 0), e.get("mean_outcome")
    n_live, live_mean = side(live)
    n_cand, cand_mean = side(candidate)
    if n_live < min_outcomes or n_cand < min_outcomes or live_mean is None or cand_mean is None:
        verdict = "insufficient_evidence"
    else:
        diff = round(float(cand_mean) - float(live_mean), 9)
        verdict = "candidate_better" if diff > margin else "live_better" if diff < -margin else "inconclusive"
    return {"verdict": verdict, "live_mean": live_mean, "candidate_mean": cand_mean,
            "n_live": n_live, "n_candidate": n_cand}


def _build_shadow_check(comp: "RAGPipelineComponent", asset_key: AssetKey):
    """Optional WARN check `shadow_vs_live` (only when both code versions are named under receipts.shadow)."""
    sh = (comp.receipts or {}).get("shadow") or {}
    if not (sh.get("live_code_version") and sh.get("candidate_code_version")):
        return None
    rk = comp.receipts["resource_key"]

    @asset_check(asset=asset_key, name="shadow_vs_live", required_resource_keys={rk},
                 description="Candidate recipe vs live recipe by real outcomes (receipt outcomes per source code version).")
    def shadow_vs_live(context):
        from datetime import datetime, timedelta, timezone
        since = (datetime.now(timezone.utc) - timedelta(days=float(sh.get("window_days", 30)))).isoformat()
        by_cv = getattr(context.resources, rk).outcomes_by_code_version(sh["source_asset_key"], since)
        r = compare_recipes(by_cv, sh["live_code_version"], sh["candidate_code_version"],
                            min_outcomes=int(sh.get("min_outcomes", 10)), margin=float(sh.get("margin", 0.05)))
        md = {k: (MetadataValue.text(v) if isinstance(v, str) else MetadataValue.float(float(v)) if isinstance(v, float)
                  else MetadataValue.int(v) if isinstance(v, int) else MetadataValue.text("n/a")) for k, v in r.items()}
        md["live_code_version"] = MetadataValue.text(sh["live_code_version"])
        md["candidate_code_version"] = MetadataValue.text(sh["candidate_code_version"])
        return AssetCheckResult(passed=r["verdict"] != "live_better", severity=AssetCheckSeverity.WARN, metadata=md)

    return shadow_vs_live


# BEGIN receipt-rerank (inlined from resources/context_receipt_store; keep identical, see tests/test_receipts_parity.py)
def receipt_rerank(candidates, scores, *, top_k=None, prior_weight=0.2, explore_fraction=0.1, rng=None):
    """Reorder retrieval candidates by learned item priors, with reserved exploration (store `rerank` semantics)."""
    import random as _random

    rng = rng or _random
    if not 0 <= explore_fraction <= 1:
        raise ValueError("explore_fraction must be between 0 and 1")
    adjusted = []
    for candidate in candidates:
        prior = (scores.get(candidate["item_id"]) or {}).get("prior") or 0.0
        adjusted.append({**candidate, "adjusted_score": float(candidate.get("score") or 0.0) + prior_weight * prior, "selection": "ranked"})
    adjusted.sort(key=lambda c: -c["adjusted_score"])
    if top_k is None or top_k >= len(adjusted):
        return adjusted
    chosen, rest = adjusted[:top_k], adjusted[top_k:]
    if top_k > 0 and rest and rng.random() < explore_fraction:
        pick = dict(rng.choice(rest))
        pick["selection"] = "explore"
        chosen[-1] = pick
    return chosen
# END receipt-rerank


def _receipt_failed(comp: "RAGPipelineComponent", context, what: str, exc: Exception) -> None:
    if (comp.receipts or {}).get("fail_on_error"):
        raise exc
    context.log.warning(f"receipts: {what} failed ({type(exc).__name__}: {exc}); continuing without it")


def _receipt_store(comp: "RAGPipelineComponent", context):
    return getattr(context.resources, comp.receipts["resource_key"])


def _doc_chunk_id(doc: Dict[str, Any]) -> Optional[str]:
    cid = (doc.get("metadata") or {}).get("chunk_id")
    return str(cid) if cid not in (None, "") else None


def _fetch_k(comp: "RAGPipelineComponent", top_k: int) -> int:
    """Candidates to pull from the vector store: top_k, or top_k*fetch_multiplier when reranking by priors."""
    rr = (comp.receipts or {}).get("rerank")
    return top_k * int(rr.get("fetch_multiplier", 3)) if rr is not None else top_k


def _apply_receipt_rerank(docs: List[Dict[str, Any]], top_k: int, comp: "RAGPipelineComponent", context) -> List[Dict[str, Any]]:
    """Reorder over-fetched `docs` by the store's learned priors and keep top_k. On a store failure (and
    fail_on_error false) fall back to the plain top_k by score."""
    rr = (comp.receipts or {}).get("rerank")
    if rr is None:
        return docs
    candidates = [{**d, "item_id": _doc_chunk_id(d) or f"__noid_{i}", "score": d.get("score", 0.0)} for i, d in enumerate(docs)]
    try:
        priors = _receipt_store(comp, context).get_item_scores([c["item_id"] for c in candidates if not c["item_id"].startswith("__noid_")])
        ranked = receipt_rerank(
            candidates, priors, top_k=top_k,
            prior_weight=float(rr.get("prior_weight", 0.2)),
            explore_fraction=float(rr.get("explore_fraction", 0.1)),
        )
    except Exception as exc:
        _receipt_failed(comp, context, "reading item priors", exc)
        return docs[:top_k]
    return ranked


def _opt_str(v: Any) -> Optional[str]:
    return None if v is None or v == "" else str(v)


def _receipt_partition_key(comp: "RAGPipelineComponent", context, row=None) -> Optional[str]:
    """The partition of THIS asset's run a receipt belongs to (``dagster_partition_key``; multi-dimension keys are 'a|b').

    Single-partition run: its key. Several-partition (range) run: the one partition the row's own filter columns
    (`partition_date_column` / `partition_static_column`, the same ones `_filter_df_to_partition` applies) place it in, when exactly
    one of the run's partitions matches; otherwise the run's range as Dagster's "start...end" string (the row's partition is not
    knowable). Unpartitioned: None. Not the chunk's `source_partition` (that is the partition of the SOURCE asset)."""
    try:
        if context.has_partition_key:
            return str(context.partition_key)
        if not context.has_partition_key_range:
            return None
        keys = list(context.partition_keys)
        if len(keys) == 1:
            return str(keys[0])
        rng = context.partition_key_range
        fallback = f"{rng.start}...{rng.end}"
        if row is None:
            return fallback
        date_col, static_col = comp.partition_date_column, comp.partition_static_column
        use_date = date_col is not None and date_col in row.index
        use_static = static_col is not None and static_col in row.index
        if not (use_date or use_static):
            return fallback
        pdef = context.assets_def.partitions_def
        time_def, time_dim = pdef, None
        if hasattr(pdef, "time_window_dimension"):          # multi-dimensional: the daily/weekly axis
            time_dim = pdef.time_window_dimension
            time_def = time_dim.partitions_def
        row_date = pd.to_datetime(row[date_col], errors="coerce", utc=True) if use_date else None
        matches = []
        for k in keys:
            by_dim = getattr(k, "keys_by_dimension", None)
            if use_static:
                sd = comp.partition_static_dim
                want = str(by_dim[sd]) if by_dim and sd in by_dim else str(k)
                if str(row[static_col]) != want:
                    continue
            if use_date:
                if row_date is None or pd.isna(row_date):
                    continue
                tkey = by_dim[time_dim.name] if (by_dim and time_dim is not None) else str(k)
                window = time_def.time_window_for_partition_key(tkey)
                if not (pd.Timestamp(window.start) <= row_date < pd.Timestamp(window.end)):
                    continue
            matches.append(str(k))
        return matches[0] if len(matches) == 1 else fallback
    except Exception:
        return None  # a partition key is metadata: never lose the receipt over it


def _record_receipt(comp: "RAGPipelineComponent", context, query: str, docs: List[Dict[str, Any]],
                    eligible: Optional[int], shadow: bool = False, row=None) -> Optional[str]:
    """Record one retrieval receipt for the docs that reached the final step. Returns the receipt id or None."""
    import hashlib

    cfg = comp.receipts
    items, seen = [], set()
    for doc in docs:
        cid = _doc_chunk_id(doc)
        if cid is None or cid in seen:
            continue  # chunks without a chunk_id cannot be tracked as items
        seen.add(cid)
        meta = doc.get("metadata") or {}
        score = doc.get("score")
        items.append({
            "item_id": cid,
            "source_asset_key": _opt_str(meta.get("source_asset_key")),
            "source_version": _opt_str(meta.get("source_version")),
            "source_partition": _opt_str(meta.get("source_partition")),
            "source_code_version": (_opt_str(cfg["shadow"].get("code_version_override")) if shadow and cfg.get("shadow") else None)
            or _opt_str(meta.get("source_code_version")),
            "rank": len(items) + 1,
            "score": float(score) if isinstance(score, (int, float)) and not isinstance(score, bool) and score == score and abs(score) != float("inf") else None,
            "selection": "explore" if doc.get("selection") == "explore" else "ranked",
        })
    versions = {i["source_version"] for i in items}
    try:
        asset_key = context.asset_key.to_user_string()
    except Exception:
        asset_key = comp.asset_name
    try:
        return _receipt_store(comp, context).record_retrieval(
            context_name=cfg.get("context_name") or comp.asset_name,
            items=items,
            context_version=versions.pop() if len(versions) == 1 else None,
            policy_version=cfg.get("policy_version"),
            caller_id=cfg.get("caller_id"),
            purpose=f"shadow:{_shadow_label(cfg['shadow'])}" if shadow else cfg.get("purpose"),
            query_hash=hashlib.sha256(query.encode("utf-8")).hexdigest(),
            eligible_count=max(int(eligible), len(items)) if eligible is not None else None,
            excluded_count=0,
            dagster_asset_key=asset_key,
            dagster_run_id=context.run_id,
            dagster_partition_key=_receipt_partition_key(comp, context, row),
        )
    except Exception as exc:
        _receipt_failed(comp, context, "recording the retrieval", exc)
        return None


# ── Ops (5): retrieve / hybrid_search / rerank / expand_query / generate ─


def _current_query(state: Dict[str, Any], step: dict) -> str:
    """Resolve the query string for this step. Steps can override with
    `query_source: <step_id>` to consume a prior step's `query` field
    (used by expand_query → retrieve for HyDE)."""
    src = step.get("query_source")
    if src:
        if src not in state:
            raise ValueError(f"query_source={src!r} not in state; known: {sorted(state)}")
        s = state[src]
        return s.get("query") or s.get("text") or ""
    return state.get("__query__", "")


def _current_docs(state: Dict[str, Any], step: dict) -> List[Dict[str, Any]]:
    """Resolve the doc list to feed a downstream step (rerank / generate)."""
    src = step.get("source")
    if src:
        if src not in state:
            raise ValueError(f"source={src!r} not in state; known: {sorted(state)}")
        return state[src].get("docs") or []
    # Fall back to most recent step that produced docs.
    for k in reversed(list(state.keys())):
        if isinstance(state[k], dict) and state[k].get("docs") is not None:
            return state[k]["docs"]
    return []


def _do_retrieve(step: dict, state: Dict[str, Any], comp: "RAGPipelineComponent", context) -> Dict[str, Any]:
    """Vector search across a named retriever."""
    retriever = _resolve_retriever(step["retriever"], comp.retrievers)
    top_k = int(step.get("top_k", 10))
    query = _current_query(state, step)
    if not query:
        raise ValueError("retrieve: no query available (set __query__ or query_source)")
    docs, served_shadow = _search_with_shadow(step, state, comp, context, query, _fetch_k(comp, top_k))
    eligible = len(docs)
    if comp.receipts:
        docs = _apply_receipt_rerank(docs, top_k, comp, context)
    context.log.info(f"[retrieve:{step['id']}] retriever={step['retriever']!r} top_k={top_k} → {len(docs)} docs")
    out = {"op": "retrieve", "query": query, "docs": docs, "top_k": top_k,
           "retriever": step["retriever"], "shadow_served": served_shadow}
    if comp.receipts:
        out["eligible_count"] = eligible
    return out


def _do_hybrid_search(step: dict, state: Dict[str, Any], comp: "RAGPipelineComponent", context) -> Dict[str, Any]:
    """BM25 + vector search combined via Reciprocal Rank Fusion (RRF).
    Requires `retriever:` (vector) + `bm25_corpus_source:` (a prior step
    that produced docs) OR `bm25_corpus_docs:` inline."""
    retriever = _resolve_retriever(step["retriever"], comp.retrievers)
    top_k = int(step.get("top_k", 10))
    query = _current_query(state, step)

    # Vector arm — pull top_k*2 to give RRF headroom.
    vec_hits, served_shadow = _search_with_shadow(step, state, comp, context, query, _fetch_k(comp, top_k * 2))

    # BM25 arm — needs an in-memory corpus. Users pass a prior step id
    # whose docs form the BM25 corpus (typical pattern: retrieve → bm25
    # on same corpus with a wider recall to get more diversity), OR
    # supply an inline list.
    corpus_source = step.get("bm25_corpus_source")
    if corpus_source:
        if corpus_source not in state:
            raise ValueError(f"bm25_corpus_source={corpus_source!r} not in state")
        corpus = state[corpus_source].get("docs") or []
    else:
        corpus = step.get("bm25_corpus_docs") or vec_hits  # default: rerank same vec_hits
    bm25_hits = _bm25_search(corpus, query, _fetch_k(comp, top_k * 2))

    fused = _rrf_combine([vec_hits, bm25_hits], k=int(step.get("rrf_k", 60)), top_k=_fetch_k(comp, top_k))
    eligible = len(fused)
    if comp.receipts:
        fused = _apply_receipt_rerank(fused, top_k, comp, context)
    context.log.info(f"[hybrid_search:{step['id']}] vec={len(vec_hits)} bm25={len(bm25_hits)} → fused={len(fused)}")
    out = {"op": "hybrid_search", "query": query, "docs": fused, "top_k": top_k,
           "retriever": step["retriever"], "shadow_served": served_shadow}
    if comp.receipts:
        out["eligible_count"] = eligible
    return out


def _do_rerank(step: dict, state: Dict[str, Any], comp: "RAGPipelineComponent", context) -> Dict[str, Any]:
    """Rerank docs from a prior step using Voyage rerank-2.5 or Cohere rerank-3.5."""
    reranker = _resolve_reranker(step["reranker"], comp.rerankers)
    docs = _current_docs(state, step)
    query = _current_query(state, step)
    top_k = int(step.get("top_k", 5))
    if not docs:
        return {"op": "rerank", "query": query, "docs": [], "top_k": top_k, "reranker": step["reranker"]}
    reranked = _rerank_docs(reranker, query, docs, top_k)
    context.log.info(f"[rerank:{step['id']}] {step['reranker']!r} {len(docs)}→{len(reranked)}")
    return {"op": "rerank", "query": query, "docs": reranked, "top_k": top_k, "reranker": step["reranker"]}


def _do_expand_query(step: dict, state: Dict[str, Any], comp: "RAGPipelineComponent", context) -> Dict[str, Any]:
    """HyDE — LLM generates a hypothetical answer to embed instead of the
    raw query. Improves retrieval when queries are short/underspecified."""
    step = _resolve_persona(step, comp.personas)
    query = _current_query(state, step)
    system_prompt = step.get("system_prompt") or (
        "Given a user question, write a concise, factual paragraph that would "
        "answer it. This will be used for semantic search, so include the "
        "kind of terminology and details you'd expect in a source document."
    )
    prompt_template = step.get("prompt_template", "Question: {query}\n\nHypothetical answer:")
    prompt = prompt_template.replace("{query}", query)

    result = _rag_completion(
        model=step.get("model") or "gpt-4o-mini",
        system_prompt=system_prompt,
        user_prompt=prompt,
        api_key_env_var=step.get("api_key_env_var"),
        api_base_env_var=step.get("api_base_env_var"),
        temperature=float(step.get("temperature", 0.3)),
        max_tokens=int(step.get("max_tokens", 300)),
        reasoning_effort=step.get("reasoning_effort"),
        thinking_budget=step.get("thinking_budget"),
        prompt_caching=bool(step.get("prompt_caching", False)),
    )
    hypothetical = result["content"] or ""
    # New "query" downstream steps will see is the hypothetical answer;
    # keep the original query available under original_query.
    context.log.info(f"[expand_query:{step['id']}] {len(hypothetical)}c hypothetical")
    return {
        "op": "expand_query",
        "query": hypothetical,
        "original_query": query,
        "reasoning": result["reasoning"],
        "cost_usd": result["cost_usd"],
        "latency_ms": result["latency_ms"],
        "tokens_total": result["tokens_total"],
    }


def _do_generate(step: dict, state: Dict[str, Any], comp: "RAGPipelineComponent", context) -> Dict[str, Any]:
    """Generate the final answer using retrieved/reranked docs as context."""
    step = _resolve_persona(step, comp.personas)
    query = _current_query(state, step)
    docs = _current_docs(state, step)
    docs_context = "\n\n---\n\n".join(
        d.get("text", "") for d in docs
    )
    prompt_template = step.get("prompt_template") or (
        "Answer the question using ONLY the provided context. If the context "
        "doesn't contain the answer, say so explicitly.\n\n"
        "Context:\n{context}\n\nQuestion: {query}\n\nAnswer:"
    )
    prompt = (
        prompt_template
        .replace("{context}", docs_context)
        .replace("{query}", query)
    )
    result = _rag_completion(
        model=step.get("model") or "gpt-4o-mini",
        system_prompt=step.get("system_prompt"),
        user_prompt=prompt,
        api_key_env_var=step.get("api_key_env_var"),
        api_base_env_var=step.get("api_base_env_var"),
        temperature=float(step.get("temperature", 0.0)),
        max_tokens=int(step.get("max_tokens", 2048)),
        reasoning_effort=step.get("reasoning_effort"),
        thinking_budget=step.get("thinking_budget"),
        prompt_caching=bool(step.get("prompt_caching", False)),
    )
    context.log.info(f"[generate:{step['id']}] {result['tokens_total']} tokens, ${result['cost_usd']:.5f}")
    return {
        "op": "generate",
        "query": query,
        "answer": result["content"],
        "reasoning": result["reasoning"],
        "docs": docs,  # forward downstream
        "cost_usd": result["cost_usd"],
        "latency_ms": result["latency_ms"],
        "tokens_total": result["tokens_total"],
        "cache_read_tokens": result["cache_read_tokens"],
        "cache_creation_tokens": result["cache_creation_tokens"],
    }


_OPS = {
    "retrieve": _do_retrieve,
    "hybrid_search": _do_hybrid_search,
    "rerank": _do_rerank,
    "expand_query": _do_expand_query,
    "generate": _do_generate,
}


# ── Ops-mode asset builder ────────────────────────────────────────────


def _build_ops_mode(comp: "RAGPipelineComponent") -> Definitions:
    """Build the ops-mode asset: for each row in `upstream_asset_key`,
    run the `steps:` chain and materialize the final step's answer +
    aggregate cost/latency."""
    asset_name = comp.asset_name
    steps_cfg = list(comp.steps or [])
    upstream_asset_key = comp.upstream_asset_key
    query_column = comp.query_column
    answer_column = comp.answer_column
    sources_column = comp.sources_column
    include_sources = comp.include_sources
    include_reasoning = comp.include_reasoning
    query_prompt_template = comp.query_prompt_template

    if not steps_cfg:
        raise ValueError("ops mode requires at least one step in `steps:`")
    for s in steps_cfg:
        if "id" not in s or "op" not in s:
            raise ValueError(f"every step needs {{id, op}}; got: {s}")
        if s["op"] not in _OPS:
            raise ValueError(
                f"step {s['id']!r} op={s['op']!r} unsupported. Valid: {sorted(_OPS)}"
            )

    partitions_def = _build_partitions_def(
        comp.partition_type, comp.partition_start, comp.partition_values,
        comp.dynamic_partition_name, comp.partition_dimensions,
    )
    _backfill_policy = _build_backfill_policy(
        partitions_def, comp.backfill_policy, comp.backfill_policy_max_partitions_per_run, query_prompt_template)
    group_name = comp.group_name
    owners = comp.owners or []
    description = comp.description or "RAG pipeline (ops mode)"

    _inferred_kinds = list(comp.kinds or ["ai", "rag", "pipeline"])
    _all_tags = dict(comp.asset_tags or {})
    for _kind in _inferred_kinds:
        _all_tags[f"dagster/kind/{_kind}"] = ""

    _freshness_policy = None
    if comp.freshness_max_lag_minutes is not None:
        from dagster import FreshnessPolicy
        _freshness_policy = _freshness_policy_compat(comp.freshness_max_lag_minutes, comp.freshness_cron)

    _retry_policy = None
    if comp.retry_policy_max_retries is not None:
        from dagster import Backoff, RetryPolicy
        _retry_policy = RetryPolicy(
            max_retries=comp.retry_policy_max_retries,
            delay=comp.retry_policy_delay_seconds or 1,
            backoff=Backoff[comp.retry_policy_backoff.upper()],
        )

    _column_lineage_md = _column_lineage_metadata(comp)
    shadow_cfg = (comp.receipts or {}).get("shadow")

    @asset(
        key=AssetKey.from_user_string(asset_name),
        partitions_def=partitions_def,
        backfill_policy=_backfill_policy,
        group_name=group_name,
        description=description,
        owners=owners,
        tags=_all_tags,
        freshness_policy=_freshness_policy,
        retry_policy=_retry_policy,
        ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
        deps=[AssetKey.from_user_string(k) for k in (comp.deps or [])],
        **({"required_resource_keys": {comp.receipts["resource_key"]}} if comp.receipts else {}),
    )
    def _rag_ops_asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
        if _column_lineage_md:
            context.add_output_metadata(_column_lineage_md)
        # Defensive Output/MaterializeResult unwrap.
        if hasattr(upstream, "value") and hasattr(upstream, "metadata"):
            upstream = upstream.value
        if isinstance(upstream, dict):
            _frames = [v for v in upstream.values() if isinstance(v, pd.DataFrame)]
            upstream = pd.concat(_frames, ignore_index=True) if _frames else pd.DataFrame()

        df = upstream.copy()
        if query_column not in df.columns:
            raise ValueError(f"query_column {query_column!r} not in upstream columns: {list(df.columns)}")
        df = _filter_df_to_partition(df, context, comp.partition_date_column, comp.partition_static_column,
                                     comp.partition_static_dim).reset_index(drop=True)

        # Partition-key substitution values.
        subs = {"run_id": context.run_id}
        if partitions_def is not None and not context.has_partition_key and query_prompt_template \
                and any(t in query_prompt_template for t in _PARTITION_PLACEHOLDERS):
            raise RuntimeError("RAGPipelineComponent: this run covers several partitions but query_prompt_template resolves "
                               "{partition_key} per single partition; launch one partition per run.")
        if context.has_partition_key:
            pk = context.partition_key
            if hasattr(pk, "keys_by_dimension"):
                subs["partition_key"] = str(pk)
                subs["partition"] = dict(pk.keys_by_dimension)
            else:
                subs["partition_key"] = str(pk)
                subs["partition"] = {}

        answers: List[str] = []
        reasonings: List[Optional[str]] = []
        sources_per_row: List[List[Dict[str, Any]]] = []
        receipt_ids: List[Optional[str]] = []
        shadow_served = shadow_failed = 0
        totals = {"cost_usd": 0.0, "latency_ms": 0, "tokens_total": 0,
                  "cache_read_tokens": 0, "cache_creation_tokens": 0}

        for idx, row in df.iterrows():
            raw_query = str(row[query_column])
            query = raw_query
            if query_prompt_template:
                query = query_prompt_template.replace("{query}", raw_query).replace(
                    "{partition_key}", str(subs.get("partition_key", ""))
                )
                for k, v in (subs.get("partition") or {}).items():
                    query = query.replace(f"{{partition.{k}}}", str(v))
            state: Dict[str, Any] = {"__query__": query}
            if shadow_cfg:
                state["__shadow__"] = _shadow_pick(shadow_cfg, raw_query)

            for step in steps_cfg:
                op_fn = _OPS[step["op"]]
                context.log.info(f"[row {idx+1}/{len(df)}] step={step['id']} op={step['op']}")
                state[step["id"]] = op_fn(step, state, comp, context)

            # The last step's outputs become this row's answer.
            last = state[steps_cfg[-1]["id"]]
            answers.append(last.get("answer") or last.get("content") or "")
            reasonings.append(last.get("reasoning"))
            sources_per_row.append(last.get("docs") or [])
            row_shadow = any(isinstance(o, dict) and o.get("shadow_served") for o in state.values())
            shadow_served += int(row_shadow)
            shadow_failed += int(state.get("__shadow_failed__", 0) > 0)
            if comp.receipts:
                # One receipt per row: the docs the final step consumed; eligible = candidates the last retrieval saw.
                eligible = next((o["eligible_count"] for o in reversed(list(state.values()))
                                 if isinstance(o, dict) and "eligible_count" in o), None)
                receipt_ids.append(_record_receipt(comp, context, query, last.get("docs") or [], eligible, shadow=row_shadow, row=row)
                                   if eligible is not None else None)

            for k in totals:
                # Sum across all steps this row.
                for step_out in state.values():
                    if isinstance(step_out, dict) and k in step_out:
                        totals[k] += step_out[k] or 0

        df[answer_column] = answers
        if include_reasoning:
            df[f"{answer_column}_reasoning"] = reasonings
        if include_sources:
            df[sources_column] = sources_per_row

        if comp.receipts:
            df["receipt_id"] = receipt_ids

        from dagster import TableSchema, TableColumn
        _col_schema = TableSchema(columns=[
            TableColumn(name=str(c), type=str(df.dtypes[c])) for c in df.columns
        ])
        _receipt_meta = {}
        if comp.receipts:
            _receipt_meta = {
                "receipt_ids": MetadataValue.json([r for r in receipt_ids if r]),
                "receipts_recorded": MetadataValue.int(sum(1 for r in receipt_ids if r)),
                "receipts_failed": MetadataValue.int(sum(1 for r in receipt_ids if not r)),
            }
            if shadow_cfg:
                _receipt_meta["shadow_served"] = MetadataValue.int(shadow_served)
                _receipt_meta["shadow_fallbacks"] = MetadataValue.int(shadow_failed)
        context.add_output_metadata({
            **_receipt_meta,
            "dagster/row_count": MetadataValue.int(len(df)),
            "dagster/column_schema": MetadataValue.table_schema(_col_schema),
            "n_steps": MetadataValue.int(len(steps_cfg)),
            "step_ids": MetadataValue.text(" → ".join(s["id"] for s in steps_cfg)),
            "total_cost_usd": MetadataValue.float(totals["cost_usd"]),
            "total_latency_ms": MetadataValue.int(totals["latency_ms"]),
            "total_tokens": MetadataValue.int(totals["tokens_total"]),
            "cache_read_tokens": MetadataValue.int(totals["cache_read_tokens"]),
            "cache_creation_tokens": MetadataValue.int(totals["cache_creation_tokens"]),
        })
        _emit_preview(context, df, comp.include_preview_metadata, comp.preview_rows)
        return df

    from dagster import build_column_schema_change_checks
    _schema_checks = build_column_schema_change_checks(assets=[_rag_ops_asset])
    _shadow_check = _build_shadow_check(comp, AssetKey.from_user_string(asset_name))
    return Definitions(assets=[_rag_ops_asset], asset_checks=list(_schema_checks) + ([_shadow_check] if _shadow_check else []))


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

# BEGIN output-preview helper (inlined per component; keep copies identical, see tests/test_output_preview.py)
def _emit_preview(context, df, enabled, n):
    """Attach a markdown preview of up to `n` rows to the asset's output metadata when `enabled`."""
    if not enabled or df is None or not hasattr(df, "head") or len(df) == 0:
        return
    try:
        import dagster as _dg

        if hasattr(df, "to_pandas"):
            df = df.to_pandas()
        sample = df.sample(min(n, len(df)), random_state=0) if len(df) > n * 10 else df.head(n)
        try:
            text = sample.to_markdown(index=False)
        except ImportError:
            text = "```\n" + sample.to_string(index=False) + "\n```"
        context.add_output_metadata({"preview": _dg.MetadataValue.md(text)})
    except Exception as exc:
        context.log.warning(f"preview emission failed: {exc}")
# END output-preview helper

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
