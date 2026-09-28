"""RagGroundingCheckComponent — verify RAG-generated answers are actually
grounded in their retrieved source chunks, as a first-class checked asset.

This is `rag_eval`'s sibling: `rag_eval` scores *retrieval* quality (did we
fetch the right chunks for a golden set of queries); this component scores
*generation* faithfulness (does the model's answer actually say only what
the retrieved chunks support, for a batch of already-generated
query/answer/sources rows -- e.g. `rag_pipeline`'s own `generate` step
output, which already has `query_column`/`answer_column`/`sources_column`
in exactly the shape this component expects).

Two ways to check groundedness:

- mode='overlap' (default): deterministic, free, no API key -- n-gram
  overlap between the answer and its source chunks, plus a fabricated-
  number check (any number in the answer that doesn't appear anywhere in
  the sources -- a concrete, common hallucination pattern: an invented or
  transposed statistic). This matches dbt's own stated philosophy that
  "grounding tests" should be deterministic/free, not another paid LLM
  call per test.
- mode='llm_judge' (opt-in): any litellm-supported model judges whether
  the answer is fully supported by the sources -- costs money per row,
  but catches paraphrased claims that share no n-grams with their source
  yet are still ungrounded (or vice versa: claims that share n-grams but
  assert something the source didn't actually say).

The attached asset check fails when the mean grounding score drops below
`min_grounding_score_threshold`, OR regresses by more than
`regression_pct_threshold` percentage points against the immediately-prior
materialization, OR the fabrication rate exceeds `max_fabrication_rate` --
mirroring `rag_eval`'s cross-materialization regression-detection design.
"""
import re
from typing import Any, Dict, List, Optional, Union

import pandas as pd

from dagster import (
    AssetCheckExecutionContext,
    AssetCheckResult,
    AssetCheckSeverity,
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
    asset_check,
)
from pydantic import Field


def _ingest_warehouse_query(source_config: dict, context) -> "pd.DataFrame":
    """Execute SQL via a Dagster resource that exposes .get_engine() (SQLAlchemy)
    OR .get_connection() (DB-API) -- works out of the box with duckdb_resource,
    postgres_resource, snowflake_resource, bigquery_resource, and any custom
    resource implementing the same duck-typed interface. Falls back to a bare
    SQLAlchemy engine via `database_url_env_var` when no Dagster resource is
    registered -- same dual pattern already used by this repo's reverse_etl
    components (e.g. greenhouse_candidate_update)."""
    sql = source_config["sql"]
    resource_key = source_config.get("resource_key")
    if resource_key:
        resource = getattr(context.resources, resource_key)
        if hasattr(resource, "get_engine"):
            return pd.read_sql(sql, resource.get_engine())
        if hasattr(resource, "get_connection"):
            # get_connection() is a @contextmanager (confirmed live against
            # dagster_duckdb.DuckDBResource) -- calling it without `with` hands
            # back a _GeneratorContextManager, not a connection, and pd.read_sql
            # fails with AttributeError. Must be entered via `with`.
            with resource.get_connection() as conn:
                return pd.read_sql(sql, conn)
        raise ValueError(
            f"resource {resource_key!r} must expose .get_engine() (SQLAlchemy) "
            f"or .get_connection() (DB-API); got {type(resource).__name__}"
        )
    env_var = source_config.get("database_url_env_var")
    if env_var:
        import os
        from sqlalchemy import create_engine
        url = os.environ.get(env_var, "")
        if not url:
            raise ValueError(f"database_url_env_var {env_var!r} is unset")
        return pd.read_sql(sql, create_engine(url))
    raise ValueError("source requires 'resource_key' OR 'database_url_env_var'")


def _build_partitions_def(partition_type, partition_start, partition_values, dynamic_partition_name):
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, DynamicPartitionsDefinition,
    )
    if not partition_type:
        return None
    _values = [v.strip() for v in (partition_values or "").split(",") if v.strip()]
    if partition_type in ("daily", "weekly", "monthly", "hourly") and not partition_start:
        raise ValueError(f"partition_type={partition_type!r} requires partition_start (ISO date).")
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
            raise ValueError("partition_type='static' requires partition_values.")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    raise ValueError(f"unknown partition_type: {partition_type!r}")


def _extract_source_texts(value: Any) -> List[str]:
    """Normalize a `sources_column` cell into a flat list of source texts.
    Handles rag_pipeline's native shape (list of {"text": ..., ...} dicts),
    a plain list of strings, or a single pre-joined string."""
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, (list, tuple)):
        texts = []
        for item in value:
            if isinstance(item, dict):
                texts.append(str(item.get("text", "")))
            else:
                texts.append(str(item))
        return texts
    return [str(value)]


_WORD_RE = re.compile(r"[a-z0-9]+")
_NUMBER_RE = re.compile(r"(?<![A-Za-z])\d[\d,]*\.?\d*%?")


def _tokenize(text: str) -> List[str]:
    return _WORD_RE.findall(text.lower())


def _ngrams(tokens: List[str], n: int):
    if len(tokens) < n:
        return {tuple(tokens)} if tokens else set()
    return {tuple(tokens[i:i + n]) for i in range(len(tokens) - n + 1)}


def _overlap_score(answer_text: str, source_text: str, n: int) -> Optional[float]:
    answer_tokens = _tokenize(answer_text)
    if not answer_tokens:
        return None
    answer_ngrams = _ngrams(answer_tokens, n)
    if not answer_ngrams:
        return None
    source_ngrams = _ngrams(_tokenize(source_text), n)
    return len(answer_ngrams & source_ngrams) / len(answer_ngrams)


def _fabricated_numbers(answer_text: str, source_text: str) -> List[str]:
    """Numbers that appear in the answer but nowhere in the sources -- a
    concrete, common hallucination pattern (an invented or transposed
    statistic). Deterministic, no model required."""
    answer_nums = set(_NUMBER_RE.findall(answer_text))
    if not answer_nums:
        return []
    source_nums = set(_NUMBER_RE.findall(source_text))
    return sorted(n for n in answer_nums if n not in source_nums)


def _do_overlap_grounding(df: pd.DataFrame, answer_column: str, sources_column: str, ngram_size: int, context) -> pd.DataFrame:
    out = df.copy()
    scores: List[Optional[float]] = []
    fabricated: List[List[str]] = []
    skipped = 0
    for _, row in out.iterrows():
        answer_text = str(row[answer_column]) if pd.notna(row[answer_column]) else ""
        source_texts = _extract_source_texts(row[sources_column])
        combined_sources = "\n".join(source_texts)
        if not answer_text.strip():
            skipped += 1
            scores.append(None)
            fabricated.append([])
            continue
        scores.append(_overlap_score(answer_text, combined_sources, ngram_size))
        fabricated.append(_fabricated_numbers(answer_text, combined_sources))
    if skipped:
        context.log.warning(f"rag_grounding_check: skipped {skipped} row(s) with empty {answer_column!r}")
    out["grounding_score"] = scores
    out["fabricated_numbers"] = fabricated
    return out


def _do_llm_judge_grounding(df: pd.DataFrame, answer_column: str, sources_column: str, llm_model: str, llm_api_key_env_var: str, llm_max_retries: int, llm_prompt_prefix: Optional[str], context) -> pd.DataFrame:
    import json
    import os
    try:
        from litellm import completion
    except ImportError:
        raise ImportError("mode='llm_judge' requires litellm: pip install litellm")

    out = df.copy()
    scores: List[Optional[float]] = []
    fabricated: List[List[str]] = []
    reasoning: List[Optional[str]] = []
    for _, row in out.iterrows():
        answer_text = str(row[answer_column]) if pd.notna(row[answer_column]) else ""
        source_texts = _extract_source_texts(row[sources_column])
        combined_sources = "\n\n".join(source_texts)
        fabricated.append(_fabricated_numbers(answer_text, combined_sources))
        if not answer_text.strip():
            scores.append(None)
            reasoning.append(None)
            continue
        prompt_parts = []
        if llm_prompt_prefix:
            prompt_parts.append(llm_prompt_prefix)
        prompt_parts.append(
            "You are auditing a RAG (retrieval-augmented generation) answer for hallucination.\n"
            "Judge whether the ANSWER is fully supported by the SOURCES -- every factual claim in "
            "the answer must be traceable to something stated in the sources. Flag ANY unsupported "
            "claim, invented detail, or number not present in the sources as not grounded.\n\n"
            f"SOURCES:\n{combined_sources}\n\nANSWER:\n{answer_text}\n\n"
            'Return only a JSON object: {"grounded": true|false, "confidence": <0.0-1.0>, "reasoning": "<one sentence>"}.'
        )
        try:
            resp = completion(
                model=llm_model,
                messages=[{"role": "user", "content": "\n\n".join(prompt_parts)}],
                api_key=os.environ.get(llm_api_key_env_var),
                num_retries=llm_max_retries,
            )
            raw = resp.choices[0].message.content.strip()
            if raw.startswith("```"):
                raw = raw.split("```")[1]
                if raw.startswith("json"):
                    raw = raw[4:]
            parsed = json.loads(raw)
            grounded = bool(parsed.get("grounded"))
            confidence = float(parsed.get("confidence", 1.0))
            confidence = max(0.0, min(1.0, confidence))
            scores.append(confidence if grounded else (1.0 - confidence))
            reasoning.append(str(parsed.get("reasoning", "")))
        except Exception as e:
            context.log.warning(f"rag_grounding_check (llm_judge): failed for a row: {e}")
            scores.append(None)
            reasoning.append(f"judge error: {e}")
    out["grounding_score"] = scores
    out["fabricated_numbers"] = fabricated
    out["judge_reasoning"] = reasoning
    return out


class RagGroundingCheckComponent(Component, Model, Resolvable):
    """Verify RAG-generated answers are grounded in their retrieved source
    chunks -- `rag_eval`'s sibling for generation faithfulness rather than
    retrieval quality. Pairs naturally with `rag_pipeline`'s `generate` step
    output (same query_column/answer_column/sources_column shape)."""

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: Optional[str] = Field(
        default=None,
        description="Upstream asset key providing a DataFrame of query/answer/sources rows (e.g. rag_pipeline's output). Mutually exclusive with `source` -- set exactly one.",
    )
    source: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Pull rows directly via SQL instead of from an upstream asset: "
            "{kind: warehouse_query, resource_key: <registered resource>, sql: <query>}. "
            "resource_key must point at a resource exposing .get_engine() (SQLAlchemy) or "
            ".get_connection() (DB-API); alternatively set `database_url_env_var` to a bare "
            "SQLAlchemy connection string from an environment variable when no Dagster "
            "resource is registered. Mutually exclusive with `upstream_asset_key` -- set "
            "exactly one."
        ),
    )
    query_column: Union[str, int] = Field(default="query", description="Column containing the original query.")
    answer_column: Union[str, int] = Field(default="answer", description="Column containing the generated answer to check.")
    sources_column: Union[str, int] = Field(
        default="sources",
        description="Column containing the retrieved source chunks the answer should be grounded in -- a list of {text: ...} dicts (rag_pipeline's native shape), a list of strings, or a single pre-joined string.",
    )

    mode: str = Field(
        default="overlap",
        description=(
            "'overlap' (default): deterministic n-gram overlap + fabricated-number check -- "
            "free, no API key. 'llm_judge': any litellm-supported model judges groundedness -- "
            "costs money per row, catches paraphrased claims overlap scoring can't."
        ),
    )
    ngram_size: int = Field(default=3, ge=1, description="N-gram size for mode='overlap' scoring (default: trigrams).")
    per_row_grounded_threshold: float = Field(
        default=0.3,
        description="Per-row grounding_score at or above this is marked `grounded=True` in the output column.",
    )

    llm_model: str = Field(default="gpt-4o-mini", description="litellm model name for mode='llm_judge'.")
    llm_api_key_env_var: str = Field(default="OPENAI_API_KEY", description="Environment variable holding the API key for mode='llm_judge'.")
    llm_max_retries: int = Field(default=2, description="Retries on transient LLM failures for mode='llm_judge'.")
    llm_prompt_prefix: Optional[str] = Field(default=None, description="Optional text prepended to every mode='llm_judge' prompt (e.g. domain context).")

    min_grounding_score_threshold: float = Field(
        default=0.5,
        description="Absolute minimum mean grounding_score across the batch. Asset check fails below this.",
    )
    regression_pct_threshold: float = Field(
        default=10.0,
        description="Max allowed drop (percentage points) in mean grounding_score vs the immediately-prior materialization. Mirrors rag_eval's regression check.",
    )
    max_fabrication_rate: Optional[float] = Field(
        default=None,
        description="If set, asset check fails when the fraction of rows with at least one fabricated number exceeds this (0.0-1.0). None (default) skips this check.",
    )

    group_name: Optional[str] = Field(default=None, description="Dagster asset group name")
    description: Optional[str] = Field(default=None, description="Asset description shown in the Dagster catalog.")
    deps: Optional[List[str]] = Field(default=None, description="Lineage-only upstream asset keys (no data passed at runtime).")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners — list of team names or email addresses.")
    asset_tags: Optional[Dict[str, str]] = Field(default=None, description="Additional key-value tags to apply to the asset.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds for the Dagster catalog. Auto-inferred if unset.")

    include_preview_metadata: bool = Field(default=False, description="Include a preview of the output data in metadata.")
    preview_rows: int = Field(default=25, ge=1, le=500, description="Rows to include in the preview metadata when include_preview_metadata is True.")

    retry_policy_max_retries: Optional[int] = Field(default=None, description="Max retries on asset failure. Defines a RetryPolicy.")
    retry_policy_delay_seconds: Optional[int] = Field(default=None, description="Seconds between retries (default 1).")
    retry_policy_backoff: str = Field(default="exponential", description="Backoff strategy: 'linear' or 'exponential'.")

    freshness_max_lag_minutes: Optional[int] = Field(default=None, description="Maximum acceptable lag in minutes before the asset is considered stale.")
    freshness_cron: Optional[str] = Field(default=None, description="Cron schedule string for the freshness policy.")

    partition_type: Optional[str] = Field(default=None, description="Partition type: 'daily'/'weekly'/'monthly'/'hourly'/'static'/'dynamic'/None.")
    partition_start: Optional[str] = Field(default=None, description="Partition start date (ISO), required for time-based types.")
    partition_values: Optional[str] = Field(default=None, description="Comma-separated values for static partitioning.")
    dynamic_partition_name: Optional[str] = Field(default=None, description="Name for DynamicPartitionsDefinition.")

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        if self.mode not in ("overlap", "llm_judge"):
            raise ValueError(f"RagGroundingCheckComponent: mode must be 'overlap' or 'llm_judge', got {self.mode!r}.")
        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError("RagGroundingCheckComponent: set exactly one of `upstream_asset_key` or `source`.")

        asset_name = self.asset_name
        query_column = self.query_column
        answer_column = self.answer_column
        sources_column = self.sources_column
        mode = self.mode
        ngram_size = self.ngram_size
        per_row_grounded_threshold = self.per_row_grounded_threshold
        llm_model = self.llm_model
        llm_api_key_env_var = self.llm_api_key_env_var
        llm_max_retries = self.llm_max_retries
        llm_prompt_prefix = self.llm_prompt_prefix
        min_grounding_score_threshold = self.min_grounding_score_threshold
        regression_pct_threshold = self.regression_pct_threshold
        max_fabrication_rate = self.max_fabrication_rate
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows
        source_cfg = self.source

        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values, self.dynamic_partition_name,
        )

        freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy
            freshness_policy = FreshnessPolicy(maximum_lag_minutes=self.freshness_max_lag_minutes, cron_schedule=self.freshness_cron)

        retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        _inferred_kinds = self.kinds or ["rag", "eval"]
        _all_tags = dict(self.asset_tags or {})
        for _kind in _inferred_kinds:
            _all_tags[f"dagster/kind/{_kind}"] = ""

        asset_kwargs: Dict[str, Any] = dict(
            key=AssetKey.from_user_string(asset_name),
            description=self.description or f"RAG grounding check ({mode} mode) over {answer_column!r} vs {sources_column!r}",
            group_name=self.group_name,
            tags=_all_tags,
            owners=self.owners or None,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])] or None,
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
            partitions_def=partitions_def,
        )
        if self.upstream_asset_key:
            asset_kwargs["ins"] = {"upstream": AssetIn(key=AssetKey.from_user_string(self.upstream_asset_key))}
        if source_cfg and source_cfg.get("resource_key"):
            asset_kwargs["required_resource_keys"] = {source_cfg["resource_key"]}

        @asset(**asset_kwargs)
        def _grounding_asset(context: AssetExecutionContext, **kwargs) -> Output:
            upstream = kwargs.get("upstream")
            if upstream is None:
                upstream = _ingest_warehouse_query(source_cfg, context)
            if hasattr(upstream, "value") and hasattr(upstream, "metadata"):
                upstream = upstream.value
            if isinstance(upstream, dict):
                _frames = [v for v in upstream.values() if isinstance(v, pd.DataFrame)]
                upstream = pd.concat(_frames, ignore_index=True) if _frames else pd.DataFrame()

            if answer_column not in upstream.columns:
                raise ValueError(f"answer_column={answer_column!r} not in upstream: {list(upstream.columns)}")
            if sources_column not in upstream.columns:
                raise ValueError(f"sources_column={sources_column!r} not in upstream: {list(upstream.columns)}")

            if mode == "overlap":
                df = _do_overlap_grounding(upstream, answer_column, sources_column, ngram_size, context)
            else:
                df = _do_llm_judge_grounding(upstream, answer_column, sources_column, llm_model, llm_api_key_env_var, llm_max_retries, llm_prompt_prefix, context)

            df["grounded"] = df["grounding_score"].apply(lambda s: bool(s is not None and s >= per_row_grounded_threshold))
            scored = df["grounding_score"].dropna()
            mean_score = float(scored.mean()) if len(scored) else 0.0
            fabrication_rate = float((df["fabricated_numbers"].apply(len) > 0).mean()) if len(df) else 0.0

            context.log.info(
                f"rag_grounding_check ({mode}): {len(df)} rows, mean_grounding_score={mean_score:.3f}, "
                f"fabrication_rate={fabrication_rate:.3f}"
            )

            metadata: Dict[str, Any] = {
                "dagster/row_count": MetadataValue.int(len(df)),
                "mode": MetadataValue.text(mode),
                "mean_grounding_score": MetadataValue.float(mean_score),
                "fabrication_rate": MetadataValue.float(fabrication_rate),
                "n_scored": MetadataValue.int(len(scored)),
                "min_threshold": MetadataValue.float(min_grounding_score_threshold),
                "regression_pct_threshold": MetadataValue.float(regression_pct_threshold),
            }
            if include_preview and len(df) > 0:
                try:
                    _prev_cols = [c for c in (query_column, answer_column, "grounding_score", "grounded", "fabricated_numbers") if c in df.columns]
                    _prev = df[_prev_cols]
                    _prev = _prev.sample(min(preview_rows, len(_prev))) if len(_prev) > preview_rows * 10 else _prev.head(preview_rows)
                    metadata["preview"] = MetadataValue.md(_prev.to_markdown(index=False) or "")
                except Exception as e:
                    context.log.warning(f"preview emission failed: {e}")
            return Output(value=df, metadata=metadata)

        grounding_asset_key = AssetKey.from_user_string(asset_name)

        @asset_check(
            asset=_grounding_asset,
            name=f"{asset_name}_grounding_quality_check",
            description=(
                "Fails when mean grounding_score drops below the absolute minimum, regresses by "
                "more than the threshold vs the prior materialization, or the fabrication rate "
                "exceeds max_fabrication_rate. Cross-materialization comparison, mirrors rag_eval."
            ),
        )
        def _grounding_quality_check(context: AssetCheckExecutionContext) -> AssetCheckResult:
            from dagster import DagsterEventType, EventRecordsFilter

            records = context.instance.get_event_records(
                event_records_filter=EventRecordsFilter(
                    event_type=DagsterEventType.ASSET_MATERIALIZATION,
                    asset_key=grounding_asset_key,
                ),
                limit=2,
                ascending=False,
            )
            if not records:
                return AssetCheckResult(
                    passed=True,
                    metadata={"note": MetadataValue.text("No materializations found — treating as pass on first run.")},
                )

            def _extract(rec, key) -> Optional[float]:
                try:
                    mat = rec.asset_materialization
                    if not mat:
                        return None
                    md = mat.metadata or {}
                    if key in md:
                        v = getattr(md[key], "value", None)
                        return float(v) if v is not None else None
                except Exception:
                    return None
                return None

            current_score = _extract(records[0], "mean_grounding_score")
            current_fab_rate = _extract(records[0], "fabrication_rate")
            prior_score = _extract(records[1], "mean_grounding_score") if len(records) > 1 else None

            metadata: Dict[str, Any] = {
                "current_score": MetadataValue.float(current_score if current_score is not None else 0.0),
            }
            if current_fab_rate is not None:
                metadata["current_fabrication_rate"] = MetadataValue.float(current_fab_rate)
            if prior_score is not None:
                metadata["prior_score"] = MetadataValue.float(prior_score)
                metadata["delta"] = MetadataValue.float((current_score or 0.0) - prior_score)

            if current_score is None:
                return AssetCheckResult(
                    passed=False,
                    metadata={**metadata, "reason": MetadataValue.text("current materialization missing mean_grounding_score metadata")},
                )

            if current_score < min_grounding_score_threshold:
                return AssetCheckResult(
                    passed=False,
                    severity=AssetCheckSeverity.ERROR,
                    metadata={**metadata, "reason": MetadataValue.text(
                        f"mean grounding_score {current_score:.3f} < absolute floor {min_grounding_score_threshold:.3f}"
                    )},
                )

            if prior_score is not None:
                allowed_floor = prior_score - (regression_pct_threshold / 100.0)
                if current_score < allowed_floor:
                    return AssetCheckResult(
                        passed=False,
                        severity=AssetCheckSeverity.ERROR,
                        metadata={**metadata, "reason": MetadataValue.text(
                            f"regression: current {current_score:.3f} < prior {prior_score:.3f} - "
                            f"{regression_pct_threshold:.1f}pp allowance"
                        )},
                    )

            if max_fabrication_rate is not None and current_fab_rate is not None and current_fab_rate > max_fabrication_rate:
                return AssetCheckResult(
                    passed=False,
                    severity=AssetCheckSeverity.ERROR,
                    metadata={**metadata, "reason": MetadataValue.text(
                        f"fabrication_rate {current_fab_rate:.3f} > max_fabrication_rate {max_fabrication_rate:.3f}"
                    )},
                )

            return AssetCheckResult(passed=True, metadata=metadata)

        return Definitions(assets=[_grounding_asset], asset_checks=[_grounding_quality_check])
