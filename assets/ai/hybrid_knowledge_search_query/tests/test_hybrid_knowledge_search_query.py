"""Committed tests for HybridKnowledgeSearchQueryComponent. A fake resource (not the real
HybridKnowledgeSearchResource -- that's covered by hybrid_knowledge_search_server's own tests)
stands in under `hybrid_resource_key`, since this component's only job is to call
`.search()` and shape the result into metadata."""
import dagster as dg
import pytest

from .conftest import load_component_module

CLS = "HybridKnowledgeSearchQueryComponent"


@pytest.fixture()
def mod():
    return load_component_module()


class _FakeHybridResource:
    def __init__(self):
        self.calls = []

    def search(self, query, top_k=5, hops=1, mode="hybrid"):
        self.calls.append({"query": query, "top_k": top_k, "hops": hops, "mode": mode})
        return {
            "passages": [{"chunk_id": "c1", "text": "Alice works at Acme.", "score": 0.9, "source": "doc1", "paths": ["vector"]}],
            "triples": [{"subject": "Alice", "relation": "works_at", "object": "Acme", "chunk_id": "c1", "evidence": "Alice works at Acme.", "confidence": 0.9, "path": "vector"}],
        }


def test_static_query_text_used_when_no_run_config_override(mod):
    cls = getattr(mod, CLS)
    comp = cls(asset_name="ask", hybrid_resource_key="hybrid_search", query_text="who works at Acme?")
    defs = comp.build_defs(None)
    resource = _FakeHybridResource()
    result = dg.materialize(list(defs.assets), resources={"hybrid_search": resource})
    assert result.success
    assert resource.calls == [{"query": "who works at Acme?", "top_k": 5, "hops": 1, "mode": "hybrid"}]
    md = result.asset_materializations_for_node("ask")[0].metadata
    assert md["passage_count"].value == 1
    assert md["triple_count"].value == 1
    assert md["entities_matched"].value == ["Acme", "Alice"]
    assert md["hits"].value["passages"][0]["chunk_id"] == "c1"
    assert md["mode"].value == "hybrid"


def test_run_config_overrides_static_fields(mod):
    cls = getattr(mod, CLS)
    comp = cls(asset_name="ask", hybrid_resource_key="hybrid_search", query_text="default query", top_k=5, hops=1, mode="hybrid")
    defs = comp.build_defs(None)
    resource = _FakeHybridResource()
    run_config = dg.RunConfig(ops={"ask": mod.HybridKnowledgeSearchQueryRunConfig(query_text="override query", top_k=3, hops=2, mode="graph")})
    result = dg.materialize(list(defs.assets), resources={"hybrid_search": resource}, run_config=run_config)
    assert result.success
    assert resource.calls == [{"query": "override query", "top_k": 3, "hops": 2, "mode": "graph"}]


def test_raises_clear_error_when_no_query_text_anywhere(mod):
    cls = getattr(mod, CLS)
    comp = cls(asset_name="ask", hybrid_resource_key="hybrid_search")
    defs = comp.build_defs(None)
    result = dg.materialize(list(defs.assets), resources={"hybrid_search": _FakeHybridResource()}, raise_on_error=False)
    assert not result.success


def test_no_matches_yields_empty_entities_matched(mod):
    class _Empty(_FakeHybridResource):
        def search(self, query, top_k=5, hops=1, mode="hybrid"):
            return {"passages": [], "triples": []}

    cls = getattr(mod, CLS)
    comp = cls(asset_name="ask", hybrid_resource_key="hybrid_search", query_text="nothing matches this")
    defs = comp.build_defs(None)
    result = dg.materialize(list(defs.assets), resources={"hybrid_search": _Empty()})
    assert result.success
    md = result.asset_materializations_for_node("ask")[0].metadata
    assert md["entities_matched"].value == []
    assert md["passage_count"].value == 0
