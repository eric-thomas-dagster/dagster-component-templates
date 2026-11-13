"""KnowledgeBaseEvalComponent -- golden-set retrieval quality as a first-class, checked asset,
against a LIVE knowledge-search resource (not a vector-index snapshot).

Deliberately NOT built on `rag_eval`: that component is tied to `vector_index_snapshot`'s own
on-disk ChromaDB snapshot directories, keyed by a `DynamicPartitionsDefinition` partition_key --
a real, different design for a real, different use case (rollback-by-partition). This component
evaluates whatever is live right now, via a registered `KnowledgeSearchResource` (from
`knowledge_search_server`) or `HybridKnowledgeSearchResource` (from
`hybrid_knowledge_search_server`) resource -- so it works with any vector store those components
support, and with graph-assisted (hybrid) search, with no on-disk snapshot involved at all.

`search_resource_key` is a PLAIN resource-key string, resolved the normal Dagster way
(`context.resources.<key>`) -- there is no special `<kb>_hybrid_knowledge_search` naming
convention anywhere in this repo (checked directly: zero hits for that pattern). Whatever
resource you registered knowledge_search_server/hybrid_knowledge_search_server's own
`resource_key` under is what you point this at. Both resources' `.search(query, top_k=k)`
methods are dispatched by return shape: `{"chunks": [...]}` (KnowledgeSearchResource) or
`{"passages": [...]}` (HybridKnowledgeSearchResource) -- both list items share
chunk_id/text/score/source/doc_id.

Each golden_set question gets:
  - `score`: fraction of `expected_terms` found (case-insensitive substring match) anywhere in
    the top-k results' text. A question with no `expected_terms` scores 1.0 (nothing to miss).
  - `hit`/`rank`: whether any of `expected_sources` (matched against each result's source,
    doc_id, OR chunk_id) appears anywhere in the top-k, and the 1-indexed rank of the first
    such match. Both are `None` when `expected_sources` isn't given for that question (not
    applicable, not "missed").

`eval_summary` (asset metadata) is the run-level JSON: `{score, hit_rate, questions, passed, k}`
-- `score` is the mean per-question score, `hit_rate` the fraction of hit-applicable questions
that hit, `passed` the same score>=min_score verdict the asset check also evaluates.
`eval_results` is the per-question JSON list: `{question, score, hit, rank, matched_terms,
missing_terms, sources}` -- `sources` is each top-k result's source/doc_id/chunk_id ONLY, never
its text, so passage content never lands in metadata.

The `<asset_name>_quality_check` asset check fails when `eval_summary.score` is below
`min_score`, OR drops by more than `regression_pct` (percentage POINTS of the 0-1 score, e.g.
regression_pct=10 means "may not fall more than 0.10 below the prior real eval" -- same
percentage-point convention `rag_eval`'s own regression check uses) against the most recent
PRIOR materialization that itself has an `eval_summary` (an ad hoc `{question}` run's
materialization has no `eval_summary` at all, so it's transparently skipped when walking back
for "the previous real eval" -- it never becomes a false baseline and never gets evaluated
against min_score/regression_pct itself). min_score is enforced unconditionally, even on the
very first real eval (same precedent as `rag_eval`'s own check); only the REGRESSION comparison
is skipped when there's no prior real eval to compare against, or on any ad hoc run (nothing to
check against at all).

Run-config override `{question: "..."}` runs ONE ad hoc question through the exact same scoring
function and reports it under `ad_hoc_question`/`ad_hoc_result` metadata instead of
`eval_summary`/`eval_results` -- it never touches golden_set, never writes eval history, and
(per the paragraph above) can never corrupt the regression baseline.
"""
from typing import Any, Dict, List, Optional, Union

import dagster as dg
from pydantic import Field


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


def _search_resource_results(resource: Any, question: str, k: int) -> List[Dict[str, Any]]:
    """Dispatches across the two real search-resource shapes this repo has: HybridKnowledgeSearchResource
    (`{"passages": [...], "triples": [...]}`) and KnowledgeSearchResource (`{"chunks": [...]}`). Both
    list items share chunk_id/text/score/source/doc_id. Raises a clear error for anything else rather
    than silently returning an empty result set."""
    result = resource.search(question, top_k=k)
    if not isinstance(result, dict):
        raise ValueError(f"search_resource_key's .search() returned {type(result).__name__}, not a dict.")
    if "passages" in result:
        return result["passages"] or []
    if "chunks" in result:
        return result["chunks"] or []
    raise ValueError(
        f"search_resource_key's .search() returned neither 'passages' nor 'chunks' "
        f"(got keys: {sorted(result.keys())}) -- expected a KnowledgeSearchResource or "
        "HybridKnowledgeSearchResource (or anything else matching their return shape)."
    )


def _score_question(resource: Any, question: str, expected_terms: List[str], expected_sources: List[str], k: int) -> Dict[str, Any]:
    """Pure scoring logic, decoupled from Dagster so it's directly unit-testable: takes an
    already-resolved resource object (or a test double) and returns the per-question dict shape
    both the golden-set loop and the ad hoc {question} override use."""
    results = _search_resource_results(resource, question, k)
    combined_text = "\n".join(str(r.get("text") or "") for r in results).lower()
    matched_terms = [t for t in expected_terms if t.lower() in combined_text]
    missing_terms = [t for t in expected_terms if t not in matched_terms]
    score = (len(matched_terms) / len(expected_terms)) if expected_terms else 1.0

    hit: Optional[bool] = None
    rank: Optional[int] = None
    if expected_sources:
        hit = False
        expected_set = set(expected_sources)
        for i, r in enumerate(results):
            candidates = {str(r.get("source") or ""), str(r.get("doc_id") or ""), str(r.get("chunk_id") or "")}
            candidates.discard("")
            if candidates & expected_set:
                hit = True
                rank = i + 1
                break

    sources = [{"source": r.get("source"), "doc_id": r.get("doc_id"), "chunk_id": r.get("chunk_id")} for r in results]
    return {
        "question": question, "score": score, "hit": hit, "rank": rank,
        "matched_terms": matched_terms, "missing_terms": missing_terms, "sources": sources,
    }


class KnowledgeBaseEvalRunConfig(dg.Config):
    """Launchpad/API-settable override for a single run -- an ad hoc retrieval test without
    touching golden_set or the stored eval history. Defaults to a no-op (unset = run the full
    golden_set as usual)."""

    question: Optional[str] = Field(
        default=None,
        description=(
            "Run this ONE question through the same scoring logic and report it under "
            "ad_hoc_question/ad_hoc_result metadata, instead of running golden_set. Never "
            "writes eval_summary/eval_results, so it can't become a false regression baseline "
            "or a false 'first run' for the NEXT golden_set run's asset check."
        ),
    )


class KnowledgeBaseEvalComponent(dg.Component, dg.Model, dg.Resolvable):
    """Golden-set retrieval quality against a live knowledge-search resource (see module docstring)."""

    asset_name: str = Field(description="Dagster asset name")
    knowledge_base: Optional[str] = Field(
        default=None,
        description="Optional UI-linking label only -- the knowledge base this evaluates. Not read or validated.",
    )
    search_resource_key: str = Field(
        description=(
            "Dagster resource key of an already-registered KnowledgeSearchResource "
            "(knowledge_search_server) or HybridKnowledgeSearchResource "
            "(hybrid_knowledge_search_server) to query. A plain resource key, resolved the "
            "normal way -- not a special naming convention."
        ),
    )
    upstream_asset_key: Union[str, List[str]] = Field(
        description=(
            "The knowledge base's own base asset key, declared as a lineage dep (no data "
            "passed at runtime) so this eval re-evaluates whenever the base asset is "
            "rebuilt/updated."
        ),
    )
    golden_set: List[Dict[str, Any]] = Field(
        description=(
            "List of {question: str, expected_terms?: [str], expected_sources?: [str]} dicts. "
            "expected_terms are matched case-insensitively as substrings of the top-k results' "
            "text; expected_sources are matched against each result's source, doc_id, OR "
            "chunk_id (any one matching counts). Either (or both) may be omitted per question."
        ),
    )
    k: int = Field(default=5, description="Top-k results requested per question.")
    min_score: float = Field(default=0.6, description="Absolute minimum mean score across golden_set. Asset check fails below this.")
    regression_pct: float = Field(
        default=10.0,
        description=(
            "Max allowed drop, in percentage POINTS of the 0-1 score (e.g. 10 = may not fall "
            "more than 0.10 below the prior real eval), vs the most recent PRIOR "
            "materialization that itself has eval_summary metadata -- an ad hoc {question} "
            "run's materialization is transparently skipped when walking back for this."
        ),
    )

    group_name: Optional[str] = Field(default=None, description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Additional key-value tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds (defaults to ['rag', 'eval']).")

    retry_policy_max_retries: Optional[int] = Field(default=None, description="Max retries on asset failure (opt-in).")
    retry_policy_delay_seconds: Optional[int] = Field(default=None, description="Seconds between retries (default 1).")
    retry_policy_backoff: str = Field(default="exponential", description="Backoff strategy: 'linear' or 'exponential'.")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=("owners", "kinds"), csv=(), tags=("asset_tags",))

    @model_validator(mode="before")
    @classmethod
    def _coerce_upstream_asset_keys(cls, data):
        return coerce_upstream_input(data, component=cls.__name__, mode="single", optional=False)

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Knowledge Base Eval", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        asset_name = self.asset_name
        search_resource_key = self.search_resource_key
        upstream_asset_key = self.upstream_asset_key
        golden_set = list(self.golden_set)
        k = self.k
        min_score = self.min_score
        regression_pct = self.regression_pct
        knowledge_base = self.knowledge_base

        kinds = self.kinds or ["rag", "eval"]
        tags = dict(self.asset_tags or {})
        for kd in kinds:
            tags[f"dagster/kind/{kd}"] = ""
        if knowledge_base:
            tags.setdefault("dagster-community/knowledge-base-name", knowledge_base)

        retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        eval_asset_key = dg.AssetKey.from_user_string(asset_name)

        @dg.asset(
            key=eval_asset_key,
            description=self.description or "Golden-set retrieval quality eval against a live knowledge-search resource.",
            group_name=self.group_name,
            owners=self.owners or [],
            tags=tags,
            deps=[dg.AssetKey.from_user_string(upstream_asset_key)],
            retry_policy=retry_policy,
            required_resource_keys={search_resource_key},
        )
        def _eval_asset(context: dg.AssetExecutionContext, config: KnowledgeBaseEvalRunConfig) -> None:
            resource = getattr(context.resources, search_resource_key)

            if config.question:
                result = _score_question(resource, config.question, [], [], k)
                context.log.info(f"ad hoc question: {config.question!r} -> score={result['score']:.3f}")
                context.add_output_metadata({
                    "ad_hoc_question": dg.MetadataValue.text(config.question),
                    "ad_hoc_result": dg.MetadataValue.json(result),
                })
                return

            results = [
                _score_question(
                    resource,
                    str(item.get("question", "")).strip(),
                    [str(t) for t in (item.get("expected_terms") or [])],
                    [str(s) for s in (item.get("expected_sources") or [])],
                    k,
                )
                for item in golden_set
            ]
            scores = [r["score"] for r in results]
            overall_score = (sum(scores) / len(scores)) if scores else 0.0
            hit_applicable = [r["hit"] for r in results if r["hit"] is not None]
            hit_rate = (sum(1 for h in hit_applicable if h) / len(hit_applicable)) if hit_applicable else None
            passed = overall_score >= min_score

            context.log.info(
                f"knowledge_base_eval: {len(golden_set)} question(s), score={overall_score:.3f}, "
                f"hit_rate={hit_rate if hit_rate is None else f'{hit_rate:.3f}'}, passed={passed}"
            )
            eval_summary = {"score": overall_score, "hit_rate": hit_rate, "questions": len(golden_set), "passed": passed, "k": k}
            context.add_output_metadata({
                "eval_summary": dg.MetadataValue.json(eval_summary),
                "eval_results": dg.MetadataValue.json(results),
            })

        @dg.asset_check(
            asset=_eval_asset,
            name=f"{asset_name}_quality_check",
            description=(
                "Fails when eval_summary.score is below min_score (enforced even on the first "
                "real eval) OR regresses by more than regression_pct (percentage points) vs the "
                "most recent prior materialization that itself has eval_summary metadata (an ad "
                "hoc {question} run is skipped when walking back for this). The regression half "
                "has nothing to compare against on the first real eval, so only that half passes "
                "trivially then; an ad hoc run has no eval_summary of its own, so the whole check "
                "passes trivially for it."
            ),
        )
        def _quality_check(context: dg.AssetCheckExecutionContext) -> dg.AssetCheckResult:
            from dagster import DagsterEventType, EventRecordsFilter

            records = context.instance.get_event_records(
                event_records_filter=EventRecordsFilter(
                    event_type=DagsterEventType.ASSET_MATERIALIZATION,
                    asset_key=eval_asset_key,
                ),
                limit=50,
                ascending=False,
            )
            if not records:
                return dg.AssetCheckResult(
                    passed=True,
                    metadata={"note": dg.MetadataValue.text("No materializations found -- treating as pass on first run.")},
                )

            def _eval_summary_of(rec) -> Optional[Dict[str, Any]]:
                try:
                    mat = rec.asset_materialization
                    if not mat:
                        return None
                    md = mat.metadata or {}
                    if "eval_summary" not in md:
                        return None
                    v = getattr(md["eval_summary"], "value", None)
                    return v if isinstance(v, dict) else None
                except Exception:  # noqa: BLE001
                    return None

            current_summary = _eval_summary_of(records[0])
            if current_summary is None:
                return dg.AssetCheckResult(
                    passed=True,
                    metadata={"note": dg.MetadataValue.text("No eval_summary on this materialization (ad hoc {question} run?) -- nothing to check.")},
                )

            current_score = current_summary.get("score")
            if current_score is None:
                return dg.AssetCheckResult(
                    passed=False,
                    metadata={"reason": dg.MetadataValue.text("current materialization's eval_summary has no 'score'.")},
                )

            prior_summary = None
            for rec in records[1:]:
                s = _eval_summary_of(rec)
                if s is not None:
                    prior_summary = s
                    break

            metadata: Dict[str, Any] = {"current_score": dg.MetadataValue.float(float(current_score))}
            if prior_summary is not None:
                prior_score = prior_summary.get("score")
                if prior_score is not None:
                    metadata["prior_score"] = dg.MetadataValue.float(float(prior_score))
                    metadata["delta"] = dg.MetadataValue.float(float(current_score) - float(prior_score))

            if current_score < min_score:
                return dg.AssetCheckResult(
                    passed=False,
                    severity=dg.AssetCheckSeverity.ERROR,
                    metadata={**metadata, "reason": dg.MetadataValue.text(f"score {current_score:.3f} < min_score {min_score:.3f}")},
                )

            if prior_summary is not None and prior_summary.get("score") is not None:
                prior_score = float(prior_summary["score"])
                allowed_floor = prior_score - (regression_pct / 100.0)
                if current_score < allowed_floor:
                    return dg.AssetCheckResult(
                        passed=False,
                        severity=dg.AssetCheckSeverity.ERROR,
                        metadata={
                            **metadata,
                            "reason": dg.MetadataValue.text(
                                f"regression: {current_score:.3f} < prior {prior_score:.3f} - {regression_pct:.1f}pp allowance"
                            ),
                        },
                    )

            return dg.AssetCheckResult(passed=True, metadata=metadata)

        return dg.Definitions(assets=[_eval_asset], asset_checks=[_quality_check])
