"""HybridKnowledgeSearchQueryComponent -- "ask a question from a run": a small asset that
calls an already-registered `HybridKnowledgeSearchResource`'s `.search()` with a run-config
overridable query, so a hybrid knowledge base (vector + graph, joined by chunk_id -- see
`hybrid_knowledge_search_server`) can be queried and the results inspected from Dagster+ run
metadata, without any vector/graph client code of its own.

Requires a `HybridKnowledgeSearchServerComponent` already registered under `hybrid_resource_key`
in the same code location (its `resource_key`, default `hybrid_knowledge_search`) -- this
component only references that resource via Dagster's own resource-injection, the same
`required_resource_keys` + `getattr(context.resources, resource_key)` pattern used throughout
this repo (e.g. `anthropic_llm_batch`, `document_chunker`, `snowflake_cortex_asset`) to reuse an
already-built client without duplicating its connection code.
"""
from typing import Any, Dict, List, Literal, Optional

from dagster import (
    AssetExecutionContext,
    AssetKey,
    Component,
    ComponentLoadContext,
    Config,
    Definitions,
    MetadataValue,
    Model,
    Resolvable,
    asset,
)
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


class HybridKnowledgeSearchQueryRunConfig(Config):
    """Launchpad/API-settable overrides for a single run -- an ad-hoc retrieval test without
    editing the component's own (YAML/definition-time) config. All fields default to a no-op
    (None = use the component's own static value)."""

    query_text: Optional[str] = Field(default=None, description="Run this query instead of the component's own static `query_text`. Required (here or statically) for every run.")
    top_k: Optional[int] = Field(default=None, description="Override the component's own top_k for this run only.")
    hops: Optional[int] = Field(default=None, description="Override the component's own hops for this run only.")
    mode: Optional[str] = Field(default=None, description="Override the component's own mode ('hybrid'/'vector'/'graph') for this run only.")


class HybridKnowledgeSearchQueryComponent(Component, Model, Resolvable):
    """One asset per materialization: calls `hybrid_resource_key`'s `.search(query, top_k, hops,
    mode)` and reports the results as run metadata -- `hits` (passages + supporting triples),
    `entities_matched`, and counts. See HybridKnowledgeSearchQueryRunConfig for the Launchpad
    run-config overrides (query_text/top_k/hops/mode)."""

    asset_name: str = Field(description="Output Dagster asset name.")
    hybrid_resource_key: str = Field(description="Resource key of an already-registered HybridKnowledgeSearchResource (from a HybridKnowledgeSearchServerComponent in the same code location).")
    query_text: Optional[str] = Field(default=None, description="Static default query. Can be left unset if every run supplies HybridKnowledgeSearchQueryRunConfig.query_text instead.")
    top_k: int = Field(default=5, description="Max passages to return.")
    hops: int = Field(default=1, description="Graph-expansion hop count for mode='hybrid'/'graph'.")
    mode: Literal["hybrid", "vector", "graph"] = Field(default="hybrid", description="Search mode: 'hybrid' (vector + graph expansion), 'vector' (vector store only), or 'graph' (graph expansion only, seeded by entity-name matches in the query).")

    group_name: Optional[str] = Field(default=None, description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description shown in the Dagster catalog.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    asset_tags: Optional[Dict[str, Any]] = Field(default=None, description="Additional key-value tags to apply to the asset.")
    deps: Optional[List[str]] = Field(default=None, description="Lineage-only upstream asset keys (no data passed at runtime).")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=("owners", "deps"), csv=(), tags=("asset_tags",))

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Hybrid Knowledge Search Query", editable=True)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        resource_key = self.hybrid_resource_key
        query_text, top_k, hops, mode = self.query_text, self.top_k, self.hops, self.mode

        tags = dict(self.asset_tags or {})
        tags.setdefault("dagster-community/hybrid-knowledge-search-query", resource_key)

        @asset(
            key=AssetKey.from_user_string(self.asset_name),
            description=self.description or "Ask-a-question run against a hybrid (vector + graph) knowledge base.",
            group_name=self.group_name,
            tags=tags,
            owners=self.owners or None,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])] or None,
            required_resource_keys={resource_key},
        )
        def _asset(context: AssetExecutionContext, config: HybridKnowledgeSearchQueryRunConfig):
            resource = getattr(context.resources, resource_key)
            q = config.query_text or query_text
            if not q:
                raise ValueError(
                    "hybrid_knowledge_search_query: no query_text -- set it on the component or "
                    "via HybridKnowledgeSearchQueryRunConfig.query_text for this run."
                )
            _top_k = config.top_k if config.top_k is not None else top_k
            _hops = config.hops if config.hops is not None else hops
            _mode = config.mode if config.mode is not None else mode
            if _mode not in ("hybrid", "vector", "graph"):
                raise ValueError(f"hybrid_knowledge_search_query: mode must be 'hybrid', 'vector', or 'graph', got {_mode!r}")

            result = resource.search(q, top_k=_top_k, hops=_hops, mode=_mode)
            passages = result.get("passages") or []
            triples = result.get("triples") or []
            entities_matched = sorted(
                {t["subject"] for t in triples if t.get("subject")} | {t["object"] for t in triples if t.get("object")}
            )

            context.add_output_metadata({
                "hits": MetadataValue.json({"passages": passages, "triples": triples}),
                "entities_matched": MetadataValue.json(entities_matched),
                "passage_count": MetadataValue.int(len(passages)),
                "triple_count": MetadataValue.int(len(triples)),
                "query_text": MetadataValue.text(q),
                "mode": MetadataValue.text(_mode),
                "top_k": MetadataValue.int(_top_k),
                "hops": MetadataValue.int(_hops),
            })

        return Definitions(assets=[_asset])
