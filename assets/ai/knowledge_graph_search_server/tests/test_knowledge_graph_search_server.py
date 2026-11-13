"""Committed tests for KnowledgeGraphSearchServerComponent. Builds a real small networkx
graph file (the exact node-link JSON shape KnowledgeGraphBuilderComponent's write_graph
step writes) and queries it through the real resource -- nothing mocked for the
provider='networkx' path, which is this component's free/local default."""
import json

import dagster as dg
import networkx as nx
import pytest

from .conftest import load_component_module

CLS = "KnowledgeGraphSearchServerComponent"
RESOURCE_CLS = "KnowledgeGraphSearchResource"


@pytest.fixture()
def mod():
    return load_component_module()


def _write_graph_file(path: str) -> None:
    g = nx.MultiDiGraph()
    g.add_node("Alice", type="PERSON", aliases=["Alice", "Alice Smith"], mention_count=3)
    g.add_node("Acme Corp", type="ORG", aliases=["Acme Corp", "ACME"], mention_count=2)
    g.add_node("Bob", type="PERSON", aliases=["Bob"], mention_count=1)
    g.add_edge("Alice", "Acme Corp", relation="works_at", confidence=0.9, mention_count=2,
               chunk_ids=["c1", "c2"], source_ids=["d1", "d2"], content_hashes=["h1", "h2"],
               evidences=["Alice works at Acme Corp.", "Alice is employed by Acme Corp."])
    g.add_edge("Bob", "Acme Corp", relation="works_at", confidence=0.7, mention_count=1,
               chunk_ids=["c3"], source_ids=["d3"], content_hashes=["h3"], evidences=["Bob works at Acme Corp too."])
    data = nx.node_link_data(g, edges="edges")
    with open(path, "w") as f:
        json.dump(data, f)


def _resource(mod, path):
    cls = getattr(mod, RESOURCE_CLS)
    return cls(provider="networkx", path=path, graph_name="test_kg")


def test_search_entities_matches_name_and_alias(mod, tmp_path):
    path = str(tmp_path / "kg.json")
    _write_graph_file(path)
    r = _resource(mod, path)
    res = r.search_entities("acme")
    assert {e["name"] for e in res["entities"]} == {"Acme Corp"}
    res2 = r.search_entities("smith")  # matches the alias, not the canonical name
    assert {e["name"] for e in res2["entities"]} == {"Alice"}


def test_search_entities_type_filter(mod, tmp_path):
    path = str(tmp_path / "kg.json")
    _write_graph_file(path)
    r = _resource(mod, path)
    res = r.search_entities("a", type="ORG")
    assert {e["name"] for e in res["entities"]} == {"Acme Corp"}


def test_get_entity_returns_relations_with_evidence(mod, tmp_path):
    path = str(tmp_path / "kg.json")
    _write_graph_file(path)
    r = _resource(mod, path)
    res = r.get_entity("Alice")
    assert res["type"] == "PERSON"
    assert res["mention_count"] == 3
    rels = res["relations"]
    assert len(rels) == 1
    assert rels[0]["relation"] == "works_at"
    assert rels[0]["other"] == "Acme Corp"
    assert rels[0]["evidence"] == "Alice works at Acme Corp."


def test_get_entity_not_found(mod, tmp_path):
    path = str(tmp_path / "kg.json")
    _write_graph_file(path)
    r = _resource(mod, path)
    assert "error" in r.get_entity("Nobody")


def test_get_neighbors_one_hop(mod, tmp_path):
    path = str(tmp_path / "kg.json")
    _write_graph_file(path)
    r = _resource(mod, path)
    res = r.get_neighbors("Alice", hops=1)
    assert {n["name"] for n in res["neighbors"]} == {"Acme Corp"}


def test_get_neighbors_two_hops_reaches_bob_via_acme(mod, tmp_path):
    path = str(tmp_path / "kg.json")
    _write_graph_file(path)
    r = _resource(mod, path)
    res = r.get_neighbors("Alice", hops=2)
    assert {n["name"] for n in res["neighbors"]} == {"Acme Corp", "Bob"}


def test_find_path_between_alice_and_bob(mod, tmp_path):
    path = str(tmp_path / "kg.json")
    _write_graph_file(path)
    r = _resource(mod, path)
    res = r.find_path("Alice", "Bob")
    assert res["path"] == ["Alice", "Acme Corp", "Bob"]


def test_find_path_no_path_when_entities_not_found(mod, tmp_path):
    path = str(tmp_path / "kg.json")
    _write_graph_file(path)
    r = _resource(mod, path)
    assert "error" in r.find_path("Alice", "Nobody")


def test_top_entities_ranked_by_mention_count(mod, tmp_path):
    path = str(tmp_path / "kg.json")
    _write_graph_file(path)
    r = _resource(mod, path)
    res = r.top_entities()
    names = [e["name"] for e in res["entities"]]
    assert names[0] == "Alice"  # mention_count 3, the highest


def test_graph_file_cache_is_loaded_once_per_resource_instance(mod, tmp_path):
    path = str(tmp_path / "kg.json")
    _write_graph_file(path)
    r = _resource(mod, path)
    g1 = r._graph()
    g2 = r._graph()
    assert g1 is g2  # same in-memory object -- confirms the cache, not a re-read per call


# ─── component build_defs: resource + inspection asset + optional mcp server ─────────────

def test_build_defs_registers_resource_and_inspection_asset(mod, tmp_path):
    path = str(tmp_path / "kg.json")
    _write_graph_file(path)
    cls = getattr(mod, CLS)
    comp = cls(resource_key="kg_search", provider="networkx", path=path, graph_name="test_kg")
    defs = comp.build_defs(None)
    assert "kg_search" in defs.resources
    result = dg.materialize(list(defs.assets), resources=defs.resources)
    assert result.success
    md = result.asset_materializations_for_node("knowledge_graph_search_tools")[0].metadata
    assert md["provider"].value == "networkx"
    assert md["tool_names"].value == ["search_entities", "get_entity", "get_neighbors", "find_path", "top_entities"]


def test_build_defs_registers_mcp_server_resource_with_env_passthrough(mod, tmp_path):
    path = str(tmp_path / "kg.json")
    cls = getattr(mod, CLS)
    comp = cls(provider="neo4j", uri="bolt://localhost:7687", username_env_var="NEO4J_USER",
               password_env_var="NEO4J_PASS", mcp_server_resource_key="kg_mcp")
    defs = comp.build_defs(None)
    mcp_resource = defs.resources["kg_mcp"]
    assert mcp_resource.type == "stdio"
    assert mcp_resource.live_env_passthrough == ["NEO4J_USER", "NEO4J_PASS"]
    assert mcp_resource.env["KNOWLEDGE_GRAPH_SEARCH_URI"] == "bolt://localhost:7687"


def test_build_defs_neo4j_requires_uri(mod):
    cls = getattr(mod, CLS)
    comp = cls(provider="neo4j")
    with pytest.raises(ValueError, match="requires `uri`"):
        comp.build_defs(None)


# ─── MCP tool wrapper: errors surface as data, never crash the agent loop ────────────────

def test_safe_wrapper_returns_error_dict_instead_of_raising(mod, tmp_path):
    path = str(tmp_path / "kg.json")
    r = _resource(mod, path)  # file doesn't exist -> empty graph, not an exception
    res = mod._safe(r.get_entity, "Anyone")
    assert res == {"error": "entity 'Anyone' not found"}
