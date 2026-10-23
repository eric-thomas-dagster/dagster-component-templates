"""Parent-child chunking, LLM enrichment, expired-chunk removal and the retrieval-quality gate.
Real chromadb and real Dagster materializations; the embedding and completion calls are faked."""
import hashlib
from unittest.mock import MagicMock, patch

import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module, requires_chromadb


@pytest.fixture()
def mod():
    return load_component_module()


class Ctx:
    class log:
        @staticmethod
        def info(*a, **k): ...
        warning = info


def _fake_embedding(model, input, api_key=None):
    resp = MagicMock()
    resp.data = [{"embedding": [float(len(t)), float(int(hashlib.md5(t.encode()).hexdigest(), 16) % 97), 1.0]} for t in input]
    return resp


def _chunk(mod, text, **step):
    df = pd.DataFrame({"id": ["d1"], "body": [text]})
    return mod._do_chunk(df, {"op": "chunk", "strategy": "recursive", "chunk_overlap": 0, **step}, "id", "body", [], Ctx)


PARAS = ["First paragraph about refund policies and timelines.",
         "Second paragraph about shipping delays and carriers.",
         "Third paragraph about app crashes and logs."]
DOC = "\n\n".join(PARAS)


# ── parent-child chunking ───────────────────────────────────────────────────

def test_children_carry_their_parent_passage(mod):
    out = _chunk(mod, DOC, chunk_size=30, parent_chunk_size=60)
    assert {"parent_id", "parent_text"} <= set(out.columns)
    assert out["chunk_text"].str.len().max() <= 30
    for row in out.itertuples():
        assert row.chunk_text in row.parent_text  # the child is part of its parent
    parents = out.groupby("parent_id")["parent_text"].first()
    assert len(parents) >= 2 and all(len(p) <= 60 for p in parents)
    assert out["chunk_id"].is_unique


def test_child_identity_includes_the_parent_so_context_changes_flip_the_hash(mod):
    base = _chunk(mod, DOC, chunk_size=30, parent_chunk_size=60)
    other = _chunk(mod, DOC.replace("shipping delays", "shipping costs"), chunk_size=30, parent_chunk_size=60)
    shared = set(base["chunk_text"]) & set(other["chunk_text"])
    assert shared  # at least one child text is identical in both
    for text in shared:
        a = base[base["chunk_text"] == text].iloc[0]
        b = other[other["chunk_text"] == text].iloc[0]
        if a["parent_text"] != b["parent_text"]:
            assert a["content_hash"] != b["content_hash"]


def test_parent_size_is_validated(mod):
    with pytest.raises(ValueError, match="larger"):
        _chunk(mod, DOC, chunk_size=50, parent_chunk_size=50)
    with pytest.raises(ValueError, match="overlap"):
        _chunk(mod, DOC, chunk_size=30, parent_chunk_size=60, parent_chunk_overlap=60)
    assert "parent_id" not in _chunk(mod, DOC, chunk_size=30).columns  # opt-in


# ── enrichment ──────────────────────────────────────────────────────────────

def _completion(reply):
    def _call(**kwargs):
        resp = MagicMock()
        resp.choices = [MagicMock(message=MagicMock(content=reply(kwargs)))]
        return resp
    return _call


def _frame(texts, **cols):
    return pd.DataFrame({"chunk_id": [f"c{i}" for i in range(len(texts))], "chunk_text": texts,
                         "content_hash": [hashlib.sha256(t.encode()).hexdigest() for t in texts], **cols})


def test_enrich_adds_a_column_and_prepend_rehashes(mod):
    df = _frame(["alpha text", "beta text"])
    with patch("litellm.completion", side_effect=_completion(lambda kw: f"ABOUT {kw['messages'][-1]['content'].split(chr(10))[-1]}")):
        col = mod._do_enrich(df, {"op": "enrich", "model": "m", "mode": "summary"}, Ctx)
        assert list(col["enrichment_summary"]) == ["ABOUT alpha text", "ABOUT beta text"]
        assert list(col["chunk_text"]) == ["alpha text", "beta text"]  # not prepended: text and hash untouched
        assert list(col["content_hash"]) == list(df["content_hash"])
        pre = mod._do_enrich(df, {"op": "enrich", "model": "m", "mode": "summary", "prepend": True}, Ctx)
    assert pre["chunk_text"].iloc[0] == "ABOUT alpha text\n\nalpha text"
    assert pre["content_hash"].iloc[0] == hashlib.sha256(pre["chunk_text"].iloc[0].encode()).hexdigest()


def test_enrich_context_mode_shows_the_parent_passage_and_errors_can_skip(mod):
    df = _frame(["child text"], parent_text=["the whole parent passage"])
    seen = {}
    def reply(kw):
        seen["user"] = kw["messages"][-1]["content"]
        return "situating sentence"
    with patch("litellm.completion", side_effect=_completion(reply)):
        out = mod._do_enrich(df, {"op": "enrich", "model": "m", "mode": "context"}, Ctx)
    assert "the whole parent passage" in seen["user"] and "child text" in seen["user"]
    assert out["enrichment_context"].iloc[0] == "situating sentence"
    with patch("litellm.completion", side_effect=RuntimeError("rate limited")):
        with pytest.raises(RuntimeError):
            mod._do_enrich(df, {"op": "enrich", "model": "m"}, Ctx)
        out = mod._do_enrich(df, {"op": "enrich", "model": "m", "on_error": "skip"}, Ctx)
    assert out["enrichment_context"].iloc[0] == "" and out.attrs["stats"]["failed"] == 1


@pytest.mark.parametrize("step,match", [({"mode": "nope", "model": "m"}, "mode"), ({"mode": "summary"}, "model"),
                                        ({"model": "m", "on_error": "x"}, "on_error")])
def test_enrich_validates(mod, step, match):
    with pytest.raises(ValueError, match=match):
        mod._do_enrich(_frame(["t"]), step, Ctx)


def test_write_vector_store_is_python_mode_only(mod):
    """enrich now has a real execution_mode='sql' path (ML.GENERATE_TEXT / CORTEX.COMPLETE / ai_query,
    see test_sql_enrich.py); write_vector_store still has no warehouse-native equivalent."""
    comp = mod.ContextEngineeringPipelineComponent(
        asset_name="kb", id_column="id", text_column="body", execution_mode="sql", sql_dialect="snowflake_cortex",
        output_table="db.s.t", source={"kind": "warehouse_query", "resource_key": "r", "sql": "select 1"},
        steps=[{"id": "c", "op": "chunk"}, {"id": "w", "op": "write_vector_store", "provider": "chromadb"}])
    with pytest.raises(ValueError, match="python-mode-only"):
        comp.build_defs(None)


# ── expired-chunk removal and the retrieval gate (real chroma) ──────────────

def _component(mod, tmp_path, extra_steps=(), write_extra=None, **kw):
    write = {"id": "w", "op": "write_vector_store", "provider": "chromadb",
             "connection_string": str(tmp_path / "chroma"), "collection_name": "kb_test", **(write_extra or {})}
    return mod.ContextEngineeringPipelineComponent(
        asset_name="kb", upstream_asset_key="raw", id_column="id", text_column="body",
        steps=[{"id": "c", "op": "chunk", "strategy": "recursive", "chunk_size": 60, "chunk_overlap": 0}, *extra_steps,
               {"id": "e", "op": "embed", "provider": "litellm", "model": "fake"}, write], **kw)


def _run(component, rows, instance=None, raise_on_error=True):
    asset_def = list(component.build_defs(None).assets)[0]

    @dg.asset(name="raw")
    def raw():
        return pd.DataFrame(rows, columns=["id", "body"])
    return dg.materialize([asset_def, raw], instance=instance, raise_on_error=raise_on_error)


def _collection(tmp_path):
    import chromadb
    return chromadb.PersistentClient(path=str(tmp_path / "chroma")).get_collection("kb_test")


@requires_chromadb
def test_parent_text_reaches_the_index_metadata(mod, tmp_path):
    comp = mod.ContextEngineeringPipelineComponent(
        asset_name="kb", upstream_asset_key="raw", id_column="id", text_column="body",
        steps=[{"id": "c", "op": "chunk", "strategy": "recursive", "chunk_size": 30, "chunk_overlap": 0, "parent_chunk_size": 60},
               {"id": "e", "op": "embed", "provider": "litellm", "model": "fake"},
               {"id": "w", "op": "write_vector_store", "provider": "chromadb",
                "connection_string": str(tmp_path / "chroma"), "collection_name": "kb_test"}])
    with patch("litellm.embedding", side_effect=_fake_embedding):
        _run(comp, [("d1", DOC)])
    got = _collection(tmp_path).get(include=["metadatas", "documents"])
    assert all(m["parent_id"].startswith("d1_p") and d in m["parent_text"] for m, d in zip(got["metadatas"], got["documents"]))


@requires_chromadb
def test_purge_expired_removes_only_this_assets_expired_chunks(mod, tmp_path):
    stamp = lambda days: {"id": "s", "op": "stamp", "ttl_days": days}
    with patch("litellm.embedding", side_effect=_fake_embedding):
        _run(_component(mod, tmp_path, [stamp(30)], {"purge_expired": True}), [("keep", "Kept document text here.")])
        result = _run(_component(mod, tmp_path, [stamp(-2)], {"purge_expired": True}), [("gone", "Expired document text here.")])
    ids = _collection(tmp_path).get()["ids"]
    assert ids == ["keep_0"]  # the expired chunk was written then removed; the live one is untouched
    stats = result.asset_materializations_for_node("kb")[0].metadata["step_metadata"].value["w"]
    assert stats["expired_deleted"] == 1


@requires_chromadb
def test_purge_expired_is_off_by_default(mod, tmp_path):
    with patch("litellm.embedding", side_effect=_fake_embedding):
        _run(_component(mod, tmp_path, [{"id": "s", "op": "stamp", "ttl_days": -2}]), [("d", "Expired but kept text.")])
    assert _collection(tmp_path).get()["ids"] == ["d_0"]


def _golden(queries, **kw):
    return {"queries": queries, "k": 3, **kw}


@requires_chromadb
def test_retrieval_quality_check_passes_and_fails_on_a_golden_set(mod, tmp_path):
    rows = [("billing", PARAS[0]), ("shipping", PARAS[1]), ("crashes", PARAS[2])]
    good = _golden([{"query": PARAS[0], "expected": ["billing"]}, {"query": PARAS[1], "expected": ["shipping_0"]},
                    {"query": PARAS[2], "expected": ["crashes"]}], min_recall=1.0, min_mrr=1.0)
    with patch("litellm.embedding", side_effect=_fake_embedding):
        result = _run(_component(mod, tmp_path, retrieval_eval=good), rows)
        ev = {e.check_name: e for e in result.get_asset_check_evaluations()}["retrieval_quality"]
        assert ev.passed and ev.metadata["recall_at_k"].value == 1.0 and ev.metadata["mrr"].value == 1.0
        bad = _golden([{"query": PARAS[0], "expected": ["billing"]}, {"query": PARAS[1], "expected": ["not_in_the_index"]}],
                      min_recall=0.8, severity="WARN")
        result = _run(_component(mod, tmp_path, retrieval_eval=bad), rows)
    ev = {e.check_name: e for e in result.get_asset_check_evaluations()}["retrieval_quality"]
    assert not ev.passed and ev.severity == dg.AssetCheckSeverity.WARN
    assert ev.metadata["recall_at_k"].value == 0.5 and ev.metadata["misses"].value == [PARAS[1]]


@requires_chromadb
def test_blocking_gate_and_eval_validation(mod, tmp_path):
    comp = _component(mod, tmp_path, retrieval_eval=_golden([{"query": "q", "expected": ["x"]}], blocking=True))
    spec = list(list(comp.build_defs(None).assets)[0].check_specs)[0]
    assert spec.name == "retrieval_quality" and spec.blocking
    assert not list(list(_component(mod, tmp_path).build_defs(None).assets)[0].check_specs)  # opt-in
    for cfg, match in [({"queries": []}, "non-empty"), ({"queries": [{"query": "q"}]}, "expected"),
                       ({"queries": [{"query": "q", "expected": ["x"]}], "k": 0}, "k"),
                       ({"queries": [{"query": "q", "expected": ["x"]}], "min_recall": 2}, "min_recall"),
                       ({"queries": [{"query": "q", "expected": ["x"]}], "bogus": 1}, "unknown")]:
        with pytest.raises(ValueError, match=match):
            _component(mod, tmp_path, retrieval_eval=cfg).build_defs(None)
    no_embed = mod.ContextEngineeringPipelineComponent(
        asset_name="kb", upstream_asset_key="raw", id_column="id", text_column="body",
        steps=[{"id": "c", "op": "chunk"}], retrieval_eval=_golden([{"query": "q", "expected": ["x"]}]))
    with pytest.raises(ValueError, match="embed step"):
        no_embed.build_defs(None)
