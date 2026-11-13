"""Committed tests for KnowledgeGraphBuilderComponent. Real pandas/networkx throughout;
litellm.completion is monkeypatched (the only paid call) rather than hitting a real model.
sentence_transformers is exercised for real (skipped if not installed, same convention as
context_engineering_pipeline's own tests) since it's free/local."""
import json
import os

import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module

CLS = "KnowledgeGraphBuilderComponent"


@pytest.fixture()
def mod():
    return load_component_module()


def _sentence_transformers_available() -> bool:
    try:
        import sentence_transformers  # noqa: F401
        return True
    except ImportError:
        return False


requires_sentence_transformers = pytest.mark.skipif(not _sentence_transformers_available(), reason="sentence-transformers not installed")


class _FakeChoice:
    def __init__(self, content):
        self.message = type("M", (), {"content": content})()


class _FakeResponse:
    def __init__(self, content):
        self.choices = [_FakeChoice(content)]


# ─── chunk ──────────────────────────────────────────────────────────────────

def test_chunk_produces_expected_columns_and_stable_hash(mod):
    df = pd.DataFrame({"doc_id": ["d1"], "body": ["Sentence one. Sentence two. Sentence three."]})
    out = mod._do_chunk(df, {"chunk_size": 20, "chunk_overlap": 5}, "doc_id", "body", [], _Logger())
    assert len(out) >= 1
    assert set(["doc_id", "chunk_id", "chunk_index", "total_chunks", "chunk_text", "content_hash"]) <= set(out.columns)
    assert out.iloc[0]["content_hash"] == mod._sha256(out.iloc[0]["chunk_text"])


def test_chunk_overlap_must_be_smaller_than_chunk_size(mod):
    df = pd.DataFrame({"doc_id": ["d1"], "body": ["text"]})
    with pytest.raises(ValueError, match="chunk_overlap"):
        mod._do_chunk(df, {"chunk_size": 10, "chunk_overlap": 10}, "doc_id", "body", [], _Logger())


# ─── extract (llm) ──────────────────────────────────────────────────────────

class _Logger:
    class log:
        @staticmethod
        def warning(msg):
            pass

        @staticmethod
        def info(msg):
            pass


def _chunk_df():
    return pd.DataFrame([
        {"doc_id": "d1", "chunk_id": "d1_0", "content_hash": "hashA", "chunk_text": "Alice works at Acme Corp."},
        {"doc_id": "d1", "chunk_id": "d1_1", "content_hash": "hashB", "chunk_text": "Bob founded Beta Inc."},
    ])


def test_extract_llm_parses_json_array_and_respects_min_confidence(mod, monkeypatch):
    def fake_completion(model, messages, api_key, temperature):
        if "Alice" in messages[0]["content"]:
            payload = [
                {"subject": "Alice", "subject_type": "PERSON", "relation": "works_at", "object": "Acme Corp",
                 "object_type": "ORG", "confidence": 0.9, "evidence": "Alice works at Acme Corp."},
                {"subject": "Alice", "subject_type": "PERSON", "relation": "maybe_knows", "object": "Someone",
                 "object_type": "PERSON", "confidence": 0.1, "evidence": "n/a"},
            ]
        else:
            payload = [{"subject": "Bob", "subject_type": "PERSON", "relation": "founded", "object": "Beta Inc",
                        "object_type": "ORG", "confidence": 0.95, "evidence": "Bob founded Beta Inc."}]
        return _FakeResponse(json.dumps(payload))

    import litellm
    monkeypatch.setattr(litellm, "completion", fake_completion)
    out = mod._do_extract(_chunk_df(), {"method": "llm", "model": "gpt-4o-mini", "min_confidence": 0.5}, "doc_id", _Logger())
    assert len(out) == 2  # the 0.1-confidence triple is filtered out
    assert set(out["subject"]) == {"Alice", "Bob"}
    assert out.iloc[0]["chunk_id"] == "d1_0"


def test_extract_llm_requires_model(mod):
    with pytest.raises(ValueError, match="requires `model`"):
        mod._do_extract(_chunk_df(), {"method": "llm"}, "doc_id", _Logger())


def test_extract_llm_skips_failing_chunk_without_crashing_run(mod, monkeypatch):
    def fake_completion(model, messages, api_key, temperature):
        if "Alice" in messages[0]["content"]:
            raise RuntimeError("rate limited")
        return _FakeResponse(json.dumps([{"subject": "Bob", "relation": "founded", "object": "Beta Inc"}]))

    import litellm
    monkeypatch.setattr(litellm, "completion", fake_completion)
    out = mod._do_extract(_chunk_df(), {"method": "llm", "model": "gpt-4o-mini"}, "doc_id", _Logger())
    assert len(out) == 1
    assert out.iloc[0]["subject"] == "Bob"


def test_extract_method_must_be_llm_or_spacy(mod):
    with pytest.raises(ValueError, match="method must be"):
        mod._do_extract(_chunk_df(), {"method": "bogus"}, "doc_id", _Logger())


def test_extract_spacy_raises_clear_import_error_when_spacy_missing(mod):
    with pytest.raises(ImportError, match="pip install spacy"):
        mod._do_extract(_chunk_df(), {"method": "spacy"}, "doc_id", _Logger())


# ─── resolve ────────────────────────────────────────────────────────────────

def _triples_df():
    return pd.DataFrame([
        {"subject": "Acme Corp", "subject_type": "ORG", "relation": "employs", "object": "Alice", "object_type": "PERSON", "confidence": 0.9, "evidence": "e1", "chunk_id": "c1", "content_hash": "h1", "doc_id": "d1"},
        {"subject": "ACME CORP.", "subject_type": "ORG", "relation": "employs", "object": "Alice", "object_type": "PERSON", "confidence": 0.8, "evidence": "e2", "chunk_id": "c2", "content_hash": "h2", "doc_id": "d1"},
        {"subject": "Beta Inc", "subject_type": "ORG", "relation": "employs", "object": "Carol", "object_type": "PERSON", "confidence": 0.7, "evidence": "e3", "chunk_id": "c3", "content_hash": "h3", "doc_id": "d2"},
    ])


def test_resolve_merges_case_and_punctuation_aliases(mod):
    out = mod._do_resolve(_triples_df(), {}, _Logger())
    assert set(out["subject"]) == {"Acme Corp", "Beta Inc"}
    assert out.attrs["entities"]["Acme Corp"]["mention_count"] == 2
    assert "ACME CORP." in out.attrs["entities"]["Acme Corp"]["aliases"]


def test_resolve_min_mentions_drops_low_frequency_entities_and_their_triples(mod):
    out = mod._do_resolve(_triples_df(), {"min_mentions": 2}, _Logger())
    # "Beta Inc" (1 mention) and "Carol" (1 mention) must be dropped along with their triple;
    # the 2 remaining rows both reference the merged "Acme Corp" (2 mentions) / "Alice" (2 mentions).
    assert "Beta Inc" not in out.attrs["entities"]
    assert len(out) == 2
    assert set(out["subject"]) == {"Acme Corp"}


@requires_sentence_transformers
def test_resolve_embedding_similarity_merge(mod):
    df = pd.DataFrame([
        {"subject": "International Business Machines", "subject_type": "ORG", "relation": "employs", "object": "Alice", "object_type": "PERSON", "confidence": 0.9, "evidence": "e1", "chunk_id": "c1", "content_hash": "h1", "doc_id": "d1"},
        {"subject": "IBM", "subject_type": "ORG", "relation": "employs", "object": "Bob", "object_type": "PERSON", "confidence": 0.9, "evidence": "e2", "chunk_id": "c2", "content_hash": "h2", "doc_id": "d1"},
    ])
    out = mod._do_resolve(df, {"embedding_similarity_threshold": 0.5}, _Logger())
    assert len(out.attrs["entities"]) < 4  # IBM and International Business Machines merged into one


def test_resolve_llm_adjudication_requires_model(mod):
    df = _triples_df()
    with pytest.raises(ValueError, match="adjudication_model"):
        mod._do_resolve(df, {"embedding_similarity_threshold": 0.0, "llm_adjudication": True}, _Logger())


# ─── write_graph (networkx) ────────────────────────────────────────────────

def test_write_graph_networkx_upsert_creates_file_with_expected_shape(mod, tmp_path):
    path = str(tmp_path / "kg.json")
    df = pd.DataFrame([
        {"doc_id": "d1", "chunk_id": "c1", "content_hash": "h1", "subject": "Alice", "subject_type": "PERSON",
         "relation": "works_at", "object": "Acme", "object_type": "ORG", "confidence": 0.9, "evidence": "ev1"},
    ])
    out = mod._do_write_graph(df, {"provider": "networkx", "path": path}, "doc_id", _Logger(), partitioned=False)
    assert os.path.exists(path)
    stats = out.attrs["stats"]
    assert stats["node_count"] == 2
    assert stats["edge_count"] == 1
    import networkx as nx
    g = mod._build_networkx_graph(path)
    assert g.has_edge("Alice", "Acme")
    edge_data = list(g.get_edge_data("Alice", "Acme").values())[0]
    assert edge_data["chunk_ids"] == ["c1"]
    assert edge_data["source_ids"] == ["d1"]


def test_write_graph_networkx_replace_sources_removes_stale_triples(mod, tmp_path):
    path = str(tmp_path / "kg.json")
    first = pd.DataFrame([
        {"doc_id": "d1", "chunk_id": "c1", "content_hash": "h1", "subject": "Alice", "subject_type": "PERSON",
         "relation": "works_at", "object": "Acme", "object_type": "ORG", "confidence": 0.9, "evidence": "ev1"},
    ])
    mod._do_write_graph(first, {"provider": "networkx", "path": path, "sync_mode": "replace_sources"}, "doc_id", _Logger(), partitioned=False)
    # Second run: same source doc_id d1, but the triple changed (Alice no longer at Acme) -> old edge must be removed.
    second = pd.DataFrame([
        {"doc_id": "d1", "chunk_id": "c2", "content_hash": "h2", "subject": "Alice", "subject_type": "PERSON",
         "relation": "works_at", "object": "Beta", "object_type": "ORG", "confidence": 0.9, "evidence": "ev2"},
    ])
    mod._do_write_graph(second, {"provider": "networkx", "path": path, "sync_mode": "replace_sources"}, "doc_id", _Logger(), partitioned=False)
    g = mod._build_networkx_graph(path)
    assert not g.has_edge("Alice", "Acme")
    assert g.has_edge("Alice", "Beta")


def test_write_graph_networkx_upsert_does_not_remove_stale_triples(mod, tmp_path):
    path = str(tmp_path / "kg.json")
    first = pd.DataFrame([
        {"doc_id": "d1", "chunk_id": "c1", "content_hash": "h1", "subject": "Alice", "subject_type": "PERSON",
         "relation": "works_at", "object": "Acme", "object_type": "ORG", "confidence": 0.9, "evidence": "ev1"},
    ])
    mod._do_write_graph(first, {"provider": "networkx", "path": path, "sync_mode": "upsert"}, "doc_id", _Logger(), partitioned=False)
    second = pd.DataFrame([
        {"doc_id": "d1", "chunk_id": "c2", "content_hash": "h2", "subject": "Alice", "subject_type": "PERSON",
         "relation": "works_at", "object": "Beta", "object_type": "ORG", "confidence": 0.9, "evidence": "ev2"},
    ])
    mod._do_write_graph(second, {"provider": "networkx", "path": path, "sync_mode": "upsert"}, "doc_id", _Logger(), partitioned=False)
    g = mod._build_networkx_graph(path)
    assert g.has_edge("Alice", "Acme")  # upsert never deletes
    assert g.has_edge("Alice", "Beta")


def test_write_graph_replace_all_refused_when_partitioned(mod, tmp_path):
    df = pd.DataFrame([{"doc_id": "d1", "subject": "A", "relation": "r", "object": "B"}])
    with pytest.raises(ValueError, match="replace_all"):
        mod._do_write_graph(df, {"provider": "networkx", "path": str(tmp_path / "kg.json"), "sync_mode": "replace_all"}, "doc_id", _Logger(), partitioned=True)


def test_write_graph_unsupported_provider_raises(mod):
    df = pd.DataFrame([{"doc_id": "d1", "subject": "A", "relation": "r", "object": "B"}])
    with pytest.raises(ValueError, match="unsupported provider"):
        mod._do_write_graph(df, {"provider": "bogus"}, "doc_id", _Logger(), partitioned=False)


def test_write_graph_sync_mode_must_be_valid(mod, tmp_path):
    df = pd.DataFrame([{"doc_id": "d1", "subject": "A", "relation": "r", "object": "B"}])
    with pytest.raises(ValueError, match="sync_mode"):
        mod._do_write_graph(df, {"provider": "networkx", "path": str(tmp_path / "kg.json"), "sync_mode": "bogus"}, "doc_id", _Logger(), partitioned=False)


# ─── graph_preview (run-metadata preview) ──────────────────────────────────

def test_graph_preview_defaults_to_top_60_nodes_untruncated_for_small_graph(mod, tmp_path):
    path = str(tmp_path / "kg.json")
    df = pd.DataFrame([
        {"doc_id": "d1", "chunk_id": "c1", "subject": "Alice", "subject_type": "PERSON",
         "relation": "works_at", "object": "Acme", "object_type": "ORG"},
    ])
    out = mod._do_write_graph(df, {"provider": "networkx", "path": path}, "doc_id", _Logger(), partitioned=False)
    preview = out.attrs["stats"]["graph_preview"]
    assert {n["name"] for n in preview["nodes"]} == {"Alice", "Acme"}
    assert preview["edges"] == [{"source": "Alice", "target": "Acme", "relation": "works_at", "weight": 1}]
    assert out.attrs["stats"]["preview_truncated"] is False


def test_graph_preview_truncates_and_only_includes_edges_among_shown_nodes(mod, tmp_path):
    path = str(tmp_path / "kg.json")
    # A has degree 2 (A-B, A-C); D-E is a separate, lower-degree edge. With preview_max_nodes=2,
    # only {A, B} make the cut -- the A-C and D-E edges must be dropped since C/D/E aren't shown.
    df = pd.DataFrame([
        {"doc_id": "d1", "subject": "A", "subject_type": "X", "relation": "r1", "object": "B", "object_type": "X"},
        {"doc_id": "d1", "subject": "A", "subject_type": "X", "relation": "r2", "object": "C", "object_type": "X"},
        {"doc_id": "d1", "subject": "D", "subject_type": "X", "relation": "r3", "object": "E", "object_type": "X"},
    ])
    out = mod._do_write_graph(df, {"provider": "networkx", "path": path, "preview_max_nodes": 2}, "doc_id", _Logger(), partitioned=False)
    stats = out.attrs["stats"]
    assert stats["node_count"] == 5
    preview = stats["graph_preview"]
    shown = {n["name"] for n in preview["nodes"]}
    assert shown == {"A", "B"}
    assert preview["edges"] == [{"source": "A", "target": "B", "relation": "r1", "weight": 1}]
    assert stats["preview_truncated"] is True


def test_graph_preview_respects_preview_max_nodes_override(mod, tmp_path):
    path = str(tmp_path / "kg.json")
    df = pd.DataFrame([{"doc_id": "d1", "subject": "A", "relation": "r", "object": "B"}])
    out = mod._do_write_graph(df, {"provider": "networkx", "path": path, "preview_max_nodes": 1}, "doc_id", _Logger(), partitioned=False)
    stats = out.attrs["stats"]
    assert len(stats["graph_preview"]["nodes"]) == 1
    assert stats["preview_truncated"] is True


# ─── extract reuse_from (incremental) ──────────────────────────────────────

def test_extract_reuse_from_networkx_skips_unchanged_chunks(mod, tmp_path, monkeypatch):
    path = str(tmp_path / "kg.json")
    written = pd.DataFrame([
        {"doc_id": "d1", "chunk_id": "c1", "content_hash": "hashA", "subject": "Alice", "subject_type": "PERSON",
         "relation": "works_at", "object": "Acme Corp", "object_type": "ORG", "confidence": 0.9, "evidence": "Alice works at Acme Corp."},
    ])
    mod._do_write_graph(written, {"provider": "networkx", "path": path}, "doc_id", _Logger(), partitioned=False)

    calls = []

    def fake_completion(model, messages, api_key, temperature):
        calls.append(messages[0]["content"])
        return _FakeResponse(json.dumps([{"subject": "Bob", "relation": "founded", "object": "Beta Inc"}]))

    import litellm
    monkeypatch.setattr(litellm, "completion", fake_completion)

    chunks = _chunk_df()  # chunk_id d1_0 content_hash "hashA" (unchanged vs above is a DIFFERENT chunk_id -- adjust to match)
    chunks = pd.DataFrame([
        {"doc_id": "d1", "chunk_id": "c1", "content_hash": "hashA", "chunk_text": "Alice works at Acme Corp."},
        {"doc_id": "d1", "chunk_id": "c2", "content_hash": "hashNEW", "chunk_text": "Bob founded Beta Inc."},
    ])
    out = mod._do_extract(chunks, {"method": "llm", "model": "gpt-4o-mini", "reuse_from": {"provider": "networkx", "path": path}}, "doc_id", _Logger())
    assert len(calls) == 1  # only the changed/new chunk (c2) was actually sent to the LLM
    assert set(out["subject"]) == {"Alice", "Bob"}
    assert "Acme Corp" in set(out["object"])  # reused triple for c1 came back from the store, not re-extracted


# ─── full pipeline, real dg.materialize ────────────────────────────────────

def test_full_pipeline_end_to_end(mod, tmp_path, monkeypatch):
    path = str(tmp_path / "kg.json")

    def fake_completion(model, messages, api_key, temperature):
        return _FakeResponse(json.dumps([
            {"subject": "Alice", "subject_type": "PERSON", "relation": "works_at", "object": "Acme Corp",
             "object_type": "ORG", "confidence": 0.9, "evidence": "ev"},
        ]))

    import litellm
    monkeypatch.setattr(litellm, "completion", fake_completion)

    @dg.asset(name="raw_docs")
    def raw_docs():
        return pd.DataFrame({"doc_id": ["d1", "d2"], "body": ["Alice works at Acme Corp.", "Alice works at Acme Corp too."]})

    cls = getattr(mod, CLS)
    comp = cls(
        asset_name="kg_out", upstream_asset_key="raw_docs", id_column="doc_id", text_column="body",
        steps=[
            {"id": "chunks", "op": "chunk", "chunk_size": 50, "chunk_overlap": 5},
            {"id": "triples", "op": "extract", "method": "llm", "model": "gpt-4o-mini"},
            {"id": "resolved", "op": "resolve", "min_mentions": 1},
            {"id": "graph", "op": "write_graph", "provider": "networkx", "path": path},
        ],
    )
    comp_assets = comp.build_defs(None).assets
    result = dg.materialize([raw_docs] + list(comp_assets), resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success
    mats = result.asset_materializations_for_node("kg_out")
    md = mats[0].metadata
    assert md["node_count"].value == 2
    assert md["edge_count"].value == 1
    assert md["chunk_count"].value == 2
    assert md["entities_linked_to_chunks"].value == 2
    assert os.path.exists(path)
    preview = md["graph_preview"].value
    assert {n["name"] for n in preview["nodes"]} == {"Alice", "Acme Corp"}
    assert preview["edges"] == [{"source": "Alice", "target": "Acme Corp", "relation": "works_at", "weight": 2}]
    assert md["preview_truncated"].value is False


# ─── shared chunk-id contract: starting from a pre-chunked upstream ───────

def test_first_step_can_be_extract_when_upstream_is_already_chunked(mod, tmp_path, monkeypatch):
    """Simulates pointing upstream_asset_key at context_engineering_pipeline's own chunk-step
    output: already has chunk_id/content_hash/chunk_text, no raw `body` column at all."""
    path = str(tmp_path / "kg.json")

    def fake_completion(model, messages, api_key, temperature):
        return _FakeResponse(json.dumps([{"subject": "Alice", "relation": "works_at", "object": "Acme"}]))

    import litellm
    monkeypatch.setattr(litellm, "completion", fake_completion)

    @dg.asset(name="pre_chunked")
    def pre_chunked():
        return pd.DataFrame([
            {"doc_id": "d1", "chunk_id": "d1_0", "chunk_index": 0, "total_chunks": 1,
             "chunk_text": "Alice works at Acme.", "content_hash": "h1"},
        ])

    cls = getattr(mod, CLS)
    comp = cls(
        asset_name="kg_out2", upstream_asset_key="pre_chunked", id_column="doc_id", text_column="body",
        steps=[
            {"id": "triples", "op": "extract", "method": "llm", "model": "gpt-4o-mini"},
            {"id": "resolved", "op": "resolve"},
            {"id": "graph", "op": "write_graph", "provider": "networkx", "path": path},
        ],
    )
    comp_assets = comp.build_defs(None).assets
    result = dg.materialize([pre_chunked] + list(comp_assets), resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success


def test_pre_chunked_upstream_without_chunk_id_raises_clear_error(mod, tmp_path):
    @dg.asset(name="not_chunked")
    def not_chunked():
        return pd.DataFrame({"doc_id": ["d1"], "body": ["text"]})

    cls = getattr(mod, CLS)
    comp = cls(
        asset_name="kg_out3", upstream_asset_key="not_chunked", id_column="doc_id", text_column="body",
        steps=[
            {"id": "triples", "op": "extract", "method": "llm", "model": "gpt-4o-mini"},
            {"id": "graph", "op": "write_graph", "provider": "networkx", "path": str(tmp_path / "kg.json")},
        ],
    )
    comp_assets = comp.build_defs(None).assets
    with pytest.raises(Exception, match="no 'chunk_id' column"):
        dg.materialize([not_chunked] + list(comp_assets), resources={"io_manager": dg.InMemoryIOManager()})


def test_first_step_must_be_chunk_or_extract(mod):
    cls = getattr(mod, CLS)
    comp = cls(asset_name="x", upstream_asset_key="y", id_column="id", text_column="t",
               steps=[{"id": "r", "op": "resolve"}])
    with pytest.raises(ValueError, match="the first step must be"):
        comp.build_defs(None)
