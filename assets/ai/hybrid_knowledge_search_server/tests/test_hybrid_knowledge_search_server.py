"""Committed tests for HybridKnowledgeSearchServerComponent. Builds a REAL chromadb
collection (real sentence-transformers embeddings, no mocking) and a REAL networkx graph
file sharing the same chunk ids, then exercises vector/graph/hybrid search against both for
real -- nothing mocked for the chromadb+networkx combination, which is this component's
free/local default."""
import json

import dagster as dg
import networkx as nx
import pytest

from .conftest import load_component_module

CLS = "HybridKnowledgeSearchServerComponent"
RESOURCE_CLS = "HybridKnowledgeSearchResource"

CHUNK_C1 = "Alice works at Acme Corp."
CHUNK_C2 = "Bob founded Beta Inc."


@pytest.fixture()
def mod():
    return load_component_module()


def _build_chroma(path: str, collection_name: str, ids=("c1", "c2"), texts=(CHUNK_C1, CHUNK_C2), source_ids=("d1", "d2")):
    import chromadb
    from sentence_transformers import SentenceTransformer

    model = SentenceTransformer("all-MiniLM-L6-v2")
    embeddings = model.encode(list(texts), normalize_embeddings=True, show_progress_bar=False).tolist()
    client = chromadb.PersistentClient(path=path)
    collection = client.get_or_create_collection(name=collection_name)
    collection.upsert(
        ids=list(ids), embeddings=embeddings, documents=list(texts),
        metadatas=[{"chunk_id": cid, "source_id": sid, "source_asset_key": "support_kb"} for cid, sid in zip(ids, source_ids)],
    )


def _build_graph(path: str) -> None:
    g = nx.MultiDiGraph()
    g.add_node("Alice", type="PERSON", aliases=["Alice"], mention_count=1)
    g.add_node("Acme Corp", type="ORG", aliases=["Acme Corp"], mention_count=1)
    g.add_node("Bob", type="PERSON", aliases=["Bob"], mention_count=1)
    g.add_node("Beta Inc", type="ORG", aliases=["Beta Inc"], mention_count=1)
    g.add_edge("Alice", "Acme Corp", relation="works_at", confidence=0.9, mention_count=1,
               chunk_ids=["c1"], source_ids=["d1"], content_hashes=["h1"], evidences=[CHUNK_C1])
    g.add_edge("Bob", "Beta Inc", relation="founded", confidence=0.9, mention_count=1,
               chunk_ids=["c2"], source_ids=["d2"], content_hashes=["h2"], evidences=[CHUNK_C2])
    data = nx.node_link_data(g, edges="edges")
    with open(path, "w") as f:
        json.dump(data, f)


@pytest.fixture()
def resource(mod, tmp_path):
    chroma_path = str(tmp_path / "chroma")
    graph_path = str(tmp_path / "kg.json")
    _build_chroma(chroma_path, "support_kb")
    _build_graph(graph_path)
    cls = getattr(mod, RESOURCE_CLS)
    return cls(
        vector_store={"provider": "chromadb", "connection_string": chroma_path, "collection_name": "support_kb"},
        graph={"provider": "networkx", "path": graph_path, "graph_name": "support_kg"},
    )


# ─── vector / graph / hybrid search ────────────────────────────────────────

def test_vector_mode_finds_the_right_chunk_with_real_embeddings(resource):
    res = resource.search("Who works at Acme?", top_k=2, mode="vector")
    assert res["passages"][0]["chunk_id"] == "c1"
    assert res["passages"][0]["text"] == CHUNK_C1
    assert "vector" in res["passages"][0]["paths"]
    assert res["triples"] == []  # pure vector mode adds no triples


def test_graph_mode_finds_entity_and_its_chunk(resource):
    res = resource.search("Alice", mode="graph", hops=1)
    chunk_ids = {p["chunk_id"] for p in res["passages"]}
    assert "c1" in chunk_ids
    triple_subjects = {t["subject"] for t in res["triples"]}
    assert "Alice" in triple_subjects


def test_hybrid_mode_merges_vector_and_graph_paths(resource):
    res = resource.search("Alice works at Acme", top_k=2, hops=1, mode="hybrid")
    c1 = next(p for p in res["passages"] if p["chunk_id"] == "c1")
    assert "vector" in c1["paths"]
    assert any(p.startswith("graph:") for p in c1["paths"])
    assert any(t["subject"] == "Alice" and t["object"] == "Acme Corp" for t in res["triples"])


def test_hybrid_mode_graph_only_passage_gets_text_backfilled_from_chromadb(resource):
    """Bob/Beta Inc is not matched by a vector hit for an Acme-focused query, but IS reached
    via graph traversal seeded by a direct entity-name match -- text must be backfilled."""
    res = resource.search("Bob", top_k=1, hops=1, mode="graph")
    c2 = next(p for p in res["passages"] if p["chunk_id"] == "c2")
    assert c2["text"] == CHUNK_C2  # backfilled via chromadb get-by-id, not a vector hit


def test_search_mode_must_be_valid(resource):
    with pytest.raises(ValueError, match="mode must be"):
        resource.search("x", mode="bogus")


# ─── chunk <-> entity round trip (explicit test ask) ───────────────────────

def test_chunk_found_by_vector_search_resolves_to_its_entities(resource):
    vector_hits = resource._vector_search("Who works at Acme?", top_k=1)
    chunk_id = vector_hits[0]["chunk_id"]
    ctx = resource.get_graph_context(chunk_id)
    assert set(ctx["entities"]) == {"Alice", "Acme Corp"}


def test_entity_resolves_back_to_its_chunks(resource):
    res = resource.get_chunks_for_entity("Alice")
    assert res["chunk_ids"] == ["c1"]


def test_get_chunks_for_entity_unknown_entity(resource):
    assert "error" in resource.get_chunks_for_entity("Nobody")


# ─── graph primitives (same as knowledge_graph_search_server) ──────────────

def test_search_entities_fuzzy_match(resource):
    res = resource.search_entities("acme")
    assert {e["name"] for e in res["entities"]} == {"Acme Corp"}


def test_get_entity_relations(resource):
    res = resource.get_entity("Alice")
    assert res["relations"][0]["other"] == "Acme Corp"


def test_find_path(resource):
    res = resource.find_path("Alice", "Bob")
    assert res["path"] is None  # disconnected components in this fixture


# ─── corpus-overlap check ───────────────────────────────────────────────

def test_check_overlap_full_overlap_chromadb_networkx(resource):
    res = resource.check_overlap()
    assert res["overlap_fraction"] == 1.0
    assert res["vector_chunk_count"] == 2
    assert res["graph_chunk_count"] == 2


def test_check_overlap_partial(mod, tmp_path):
    chroma_path, graph_path = str(tmp_path / "chroma"), str(tmp_path / "kg.json")
    _build_chroma(chroma_path, "kb_partial", ids=("c1", "c2", "c3"), texts=(CHUNK_C1, CHUNK_C2, "Carol runs Gamma LLC."), source_ids=("d1", "d2", "d3"))
    _build_graph(graph_path)  # only c1, c2
    cls = getattr(mod, RESOURCE_CLS)
    r = cls(vector_store={"provider": "chromadb", "connection_string": chroma_path, "collection_name": "kb_partial"},
            graph={"provider": "networkx", "path": graph_path})
    res = r.check_overlap()
    assert res["overlap_fraction"] == pytest.approx(2 / 3)


def test_check_overlap_unsupported_combo_returns_none(mod):
    cls = getattr(mod, RESOURCE_CLS)
    r = cls(vector_store={"provider": "pinecone", "collection_name": "x"}, graph={"provider": "networkx"})
    res = r.check_overlap()
    assert res["overlap_fraction"] is None
    assert "not the supported" in res["overlap_note"]


# ─── component build_defs ──────────────────────────────────────────────

def test_build_defs_registers_resource_and_inspection_asset(mod, tmp_path):
    chroma_path, graph_path = str(tmp_path / "chroma"), str(tmp_path / "kg.json")
    _build_chroma(chroma_path, "support_kb")
    _build_graph(graph_path)
    cls = getattr(mod, CLS)
    comp = cls(
        resource_key="hybrid_search",
        vector_store={"provider": "chromadb", "connection_string": chroma_path, "collection_name": "support_kb"},
        graph={"provider": "networkx", "path": graph_path},
    )
    defs = comp.build_defs(None)
    assert "hybrid_search" in defs.resources
    result = dg.materialize(list(defs.assets), resources=defs.resources)
    assert result.success
    md = result.asset_materializations_for_node("hybrid_knowledge_search_tools")[0].metadata
    assert md["overlap_with_knowledge_base"].value == 1.0
    assert md["chunk_count_vector"].value == 2
    assert md["chunk_count_graph"].value == 2
    assert "search" in md["tool_names"].value


def test_build_defs_requires_vector_collection_name(mod):
    cls = getattr(mod, CLS)
    comp = cls(vector_store={}, graph={"provider": "networkx"})
    with pytest.raises(ValueError, match="collection_name is required"):
        comp.build_defs(None)


def test_build_defs_neo4j_graph_requires_uri(mod):
    cls = getattr(mod, CLS)
    comp = cls(vector_store={"collection_name": "x"}, graph={"provider": "neo4j"})
    with pytest.raises(ValueError, match="requires graph.uri"):
        comp.build_defs(None)


def test_build_defs_registers_mcp_server_resource(mod, tmp_path):
    cls = getattr(mod, CLS)
    comp = cls(
        vector_store={"collection_name": "x", "api_key_env_var": "PINECONE_API_KEY"},
        graph={"provider": "networkx", "path": str(tmp_path / "kg.json")},
        mcp_server_resource_key="hybrid_mcp",
    )
    defs = comp.build_defs(None)
    mcp_resource = defs.resources["hybrid_mcp"]
    assert mcp_resource.type == "stdio"
    assert "PINECONE_API_KEY" in mcp_resource.live_env_passthrough
    assert mcp_resource.env["HYBRID_KG_SEARCH_VECTOR_COLLECTION_NAME"] == "x"


# ─── MCP tool wrapper: errors surface as data, never crash the agent loop ──

def test_safe_wrapper_returns_error_dict(mod, resource):
    res = mod._safe(resource.get_entity, "Nobody")
    assert res == {"error": "entity 'Nobody' not found"}
