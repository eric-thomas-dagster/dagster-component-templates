"""Committed tests for KnowledgeGraphSearchQueryComponent. A fake resource (not the real
KnowledgeGraphSearchResource -- that's covered by knowledge_graph_search_server's own tests)
stands in under `graph_resource_key`, since this component's only job is to call
`search_entities`/`get_neighbors` and shape the results into metadata."""
import dagster as dg
import pytest

from .conftest import load_component_module

CLS = "KnowledgeGraphSearchQueryComponent"


@pytest.fixture()
def mod():
    return load_component_module()


class _FakeGraphResource:
    def __init__(self):
        self.neighbor_calls = []

    def search_entities(self, query, type=None):
        if query.lower() == "alice":
            return {"entities": [{"name": "Alice", "type": "PERSON", "mention_count": 3, "aliases": ["Alice"]}]}
        return {"entities": []}

    def get_neighbors(self, name, hops=1, relation=None):
        self.neighbor_calls.append({"name": name, "hops": hops, "relation": relation})
        if name == "Alice":
            return {"neighbors": [{"name": "Acme", "type": "ORG", "distance": 1}]}
        return {"error": f"entity {name!r} not found"}


def test_exact_match_resolves_and_expands_neighbors(mod):
    cls = getattr(mod, CLS)
    comp = cls(asset_name="ask", graph_resource_key="graph_search", entity="Alice", hops=1)
    defs = comp.build_defs(None)
    resource = _FakeGraphResource()
    result = dg.materialize(list(defs.assets), resources={"graph_search": resource})
    assert result.success
    assert resource.neighbor_calls == [{"name": "Alice", "hops": 1, "relation": None}]
    md = result.asset_materializations_for_node("ask")[0].metadata
    assert md["resolved_entity"].value == "Alice"
    assert md["neighbors"].value == [{"name": "Acme", "type": "ORG", "distance": 1}]
    assert md["neighbor_error"].value == ""


def test_run_config_overrides_static_entity_and_hops(mod):
    cls = getattr(mod, CLS)
    comp = cls(asset_name="ask", graph_resource_key="graph_search", entity="default", hops=1)
    defs = comp.build_defs(None)
    resource = _FakeGraphResource()
    run_config = dg.RunConfig(ops={"ask": mod.KnowledgeGraphSearchQueryRunConfig(entity="Alice", hops=2)})
    result = dg.materialize(list(defs.assets), resources={"graph_search": resource}, run_config=run_config)
    assert result.success
    assert resource.neighbor_calls == [{"name": "Alice", "hops": 2, "relation": None}]


def test_no_entity_match_falls_back_to_raw_query_term(mod):
    cls = getattr(mod, CLS)
    comp = cls(asset_name="ask", graph_resource_key="graph_search", entity="Nobody")
    defs = comp.build_defs(None)
    resource = _FakeGraphResource()
    result = dg.materialize(list(defs.assets), resources={"graph_search": resource})
    assert result.success
    md = result.asset_materializations_for_node("ask")[0].metadata
    assert md["entities_matched"].value == []
    assert md["resolved_entity"].value == "Nobody"
    assert "not found" in md["neighbor_error"].value


def test_raises_clear_error_when_no_entity_anywhere(mod):
    cls = getattr(mod, CLS)
    comp = cls(asset_name="ask", graph_resource_key="graph_search")
    defs = comp.build_defs(None)
    result = dg.materialize(list(defs.assets), resources={"graph_search": _FakeGraphResource()}, raise_on_error=False)
    assert not result.success
