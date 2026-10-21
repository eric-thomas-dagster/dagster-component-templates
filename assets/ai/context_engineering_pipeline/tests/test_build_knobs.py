"""Tests for the context-building knobs: chunk strategies/units/ids/headers, filter, dedupe,
sensitivity, stamp, embedding reuse. Pure-frame tests need no models; embedding is faked at
`litellm.embedding` (no free local litellm backend), chromadb is real."""
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


def fake_embedding(calls):
    def _embed(model, input, api_key=None):
        calls.extend(input)
        resp = MagicMock()
        resp.data = [{"embedding": [float(len(t)), float(int(hashlib.md5(t.encode()).hexdigest(), 16) % 97), 1.0]} for t in input]
        return resp
    return _embed


def _chunk(mod, text, **step):
    df = pd.DataFrame({"id": ["d1"], "body": [text], "title": ["Guide"]})
    return mod._do_chunk(df, {"op": "chunk", **step}, "id", "body", ["title"], Ctx)


# ── chunk strategies and units ──────────────────────────────────────────────

def test_recursive_respects_size_and_prefers_paragraph_boundaries(mod):
    text = "\n\n".join(f"Paragraph {i} " + "word " * 20 for i in range(6))
    out = _chunk(mod, text, strategy="recursive", chunk_size=250, chunk_overlap=0)
    assert all(len(t) <= 250 for t in out["chunk_text"])
    assert all(t.startswith("Paragraph") for t in out["chunk_text"])  # never cut mid-paragraph here
    assert len(out) > 1


def test_markdown_strategy_records_section_breadcrumb(mod):
    text = "# Billing\nIntro text.\n## Refunds\nRefund policy details here.\n## Invoices\nInvoice details here.\n# Shipping\nTracking info."
    out = _chunk(mod, text, strategy="markdown", chunk_size=200, chunk_overlap=0)
    paths = dict(zip(out["chunk_text"], out["section_path"]))
    assert paths["Refund policy details here."] == "Billing > Refunds"
    assert paths["Invoice details here."] == "Billing > Invoices"
    assert paths["Tracking info."] == "Shipping"


def test_token_unit_uses_chars_per_token_estimate(mod):
    text = "x" * 4000
    by_chars = _chunk(mod, text, chunk_size=400, chunk_overlap=0, preserve_sentences=False)
    by_tokens = _chunk(mod, text, chunk_size=100, chunk_overlap=0, length_unit="tokens", preserve_sentences=False)
    assert list(by_chars["chunk_text"].str.len()) == list(by_tokens["chunk_text"].str.len())
    tight = _chunk(mod, text, chunk_size=100, chunk_overlap=0, length_unit="tokens", chars_per_token=2, preserve_sentences=False)
    assert tight["chunk_text"].str.len().max() == 200


def test_min_chunk_and_max_chunks_per_source(mod):
    text = "\n\n".join(["tiny", "A longer paragraph that clearly exceeds the minimum size. " * 3] * 3)
    out = _chunk(mod, text, strategy="recursive", chunk_size=120, chunk_overlap=0, min_chunk_chars=30)
    assert (out["chunk_text"].str.len() >= 30).all()
    capped = _chunk(mod, text, strategy="recursive", chunk_size=120, chunk_overlap=0, max_chunks_per_source=2)
    assert len(capped) == 2


def test_header_template_prepended_and_hashed(mod):
    plain = _chunk(mod, "Body text here.", chunk_size=100, chunk_overlap=0)
    headed = _chunk(mod, "Body text here.", chunk_size=100, chunk_overlap=0, header_template="[{title}] ")
    assert headed["chunk_text"].iloc[0] == "[Guide] Body text here."
    assert headed["content_hash"].iloc[0] != plain["content_hash"].iloc[0]
    assert headed["content_hash"].iloc[0] == hashlib.sha256(b"[Guide] Body text here.").hexdigest()


def test_content_ids_are_hash_derived_positional_ids_are_not(mod):
    a = _chunk(mod, "Same text.", chunk_id_mode="content")
    b = _chunk(mod, "Same text.", chunk_id_mode="content")
    c = _chunk(mod, "Different text.", chunk_id_mode="content")
    assert a["chunk_id"].iloc[0] == b["chunk_id"].iloc[0] != c["chunk_id"].iloc[0]
    assert _chunk(mod, "Same text.")["chunk_id"].iloc[0] == "d1_0"
    dup = _chunk(mod, "Same sentence. " * 1, chunk_id_mode="content", strategy="recursive", chunk_size=20, chunk_overlap=0)
    assert dup["chunk_id"].is_unique


@pytest.mark.parametrize("step,match", [
    ({"strategy": "bogus"}, "strategy"),
    ({"length_unit": "words"}, "length_unit"),
    ({"chunk_size": 50, "chunk_overlap": 50}, "overlap"),
    ({"chunk_id_mode": "x"}, "chunk_id_mode"),
])
def test_chunk_validates_knobs(mod, step, match):
    with pytest.raises(ValueError, match=match):
        _chunk(mod, "text", **step)


# ── filter / dedupe / sensitivity / stamp ───────────────────────────────────

def _frame(texts, **cols):
    return pd.DataFrame({"chunk_id": [f"c{i}" for i in range(len(texts))], "chunk_text": texts,
                         "content_hash": [hashlib.sha256(t.encode()).hexdigest() for t in texts], **cols})


def test_filter_drops_by_length_symbols_and_patterns_and_reports_reasons(mod):
    df = _frame(["ok text here", "x", "!!!???###", "Unsubscribe from this list", "a good chunk of useful text"])
    out = mod._do_filter(df, {"op": "filter", "min_chars": 5, "min_alnum_ratio": 0.5, "exclude_patterns": ["unsubscribe"]}, Ctx)
    assert list(out["chunk_text"]) == ["ok text here", "a good chunk of useful text"]
    assert out.attrs["stats"]["reasons"] == {"min_chars": 1, "min_alnum_ratio": 1, "exclude:unsubscribe": 1}


def test_dedupe_exact_vs_normalized_and_within_scope(mod):
    df = _frame(["Hello,  World!", "hello world", "Hello,  World!", "other"], tenant=["a", "a", "b", "a"])
    exact = mod._do_dedupe(df, {"op": "dedupe", "mode": "exact"}, Ctx)
    assert len(exact) == 3  # only the byte-identical pair collapses
    norm = mod._do_dedupe(df, {"op": "dedupe", "mode": "normalized"}, Ctx)
    assert list(norm["chunk_id"]) == ["c3"] or len(norm) == 2
    scoped = mod._do_dedupe(df, {"op": "dedupe", "mode": "normalized", "within": ["tenant"]}, Ctx)
    assert list(scoped["chunk_id"]) == ["c0", "c2", "c3"]  # tenant b's copy is not suppressed by tenant a's
    with pytest.raises(ValueError, match="mode"):
        mod._do_dedupe(df, {"mode": "fuzzy"}, Ctx)


def test_sensitivity_tag_redact_drop_and_luhn(mod):
    df = _frame([
        "Contact jane@example.com or 415-555-0199.",
        "Card 4111 1111 1111 1111 on file.",
        "Order number 1234 5678 9012 3456 shipped.",  # 16 digits but fails Luhn: not a card
        "Nothing sensitive here.",
        "SSN 123-45-6789 and IP 10.0.0.1",
    ])
    tagged = mod._do_sensitivity(df, {"op": "sensitivity"}, Ctx)
    assert list(tagged["sensitivity"]) == ["pii", "pii", "none", "none", "pii"]
    assert tagged["pii_types"].iloc[0] == "email,phone"
    assert tagged["pii_types"].iloc[1] == "credit_card" and tagged["pii_types"].iloc[4] == "ipv4,ssn"
    assert tagged["chunk_text"].iloc[0] == df["chunk_text"].iloc[0]  # tag leaves text alone
    red = mod._do_sensitivity(df, {"op": "sensitivity", "action": "redact"}, Ctx)
    assert red["chunk_text"].iloc[0] == "Contact [EMAIL] or [PHONE]."
    assert red["content_hash"].iloc[0] == hashlib.sha256(red["chunk_text"].iloc[0].encode()).hexdigest()
    dropped = mod._do_sensitivity(df, {"op": "sensitivity", "action": "drop", "detectors": ["email"]}, Ctx)
    assert len(dropped) == 4 and "jane@example.com" not in " ".join(dropped["chunk_text"])
    with pytest.raises(ValueError, match="detector"):
        mod._do_sensitivity(df, {"detectors": ["dna"]}, Ctx)


def test_stamp_templates_and_ttl(mod):
    df = _frame(["a", "b"], customer=["acme", "globex"])
    out = mod._do_stamp(df, {"op": "stamp", "columns": {"scope": "{customer}", "access": "support"}, "ttl_days": 30}, Ctx)
    assert list(out["scope"]) == ["acme", "globex"] and set(out["access"]) == {"support"}
    assert len(out["valid_until"].iloc[0]) == 10 and out["valid_until"].nunique() == 1


# ── embedding reuse (real chroma) ───────────────────────────────────────────

def _component(mod, tmp_path, texts_asset="raw", **kw):
    steps = kw.pop("steps", None) or [
        {"id": "chunks", "op": "chunk", "chunk_size": 60, "chunk_overlap": 0, "strategy": "recursive", "chunk_id_mode": "content"},
        {"id": "embedded", "op": "embed", "provider": "litellm", "model": "fake-1",
         "reuse_from": {"connection_string": str(tmp_path / "chroma"), "collection_name": "kb_test"}},
        {"id": "indexed", "op": "write_vector_store", "provider": "chromadb",
         "connection_string": str(tmp_path / "chroma"), "collection_name": "kb_test"},
    ]
    return mod.ContextEngineeringPipelineComponent(
        asset_name="kb", upstream_asset_key=texts_asset, id_column="id", text_column="body", steps=steps, **kw)


def _run(component, df, instance=None):
    asset_def = list(component.build_defs(None).assets)[0]

    @dg.asset(name="raw")
    def raw():
        return df
    return dg.materialize([asset_def, raw], instance=instance, raise_on_error=True)


@requires_chromadb
def test_embedding_reuse_only_embeds_new_or_changed_chunks(mod, tmp_path):
    base = "First paragraph about billing refunds.\n\nSecond paragraph about shipping delays.\n\nThird paragraph about app crashes."
    df = pd.DataFrame({"id": ["d1"], "body": [base]})
    calls: list = []
    with patch("litellm.embedding", side_effect=fake_embedding(calls)):
        r1 = _run(_component(mod, tmp_path), df)
        assert len(calls) == 3 and r1.success
        calls.clear()
        _run(_component(mod, tmp_path), df)  # identical rebuild
        assert calls == []  # nothing re-embedded
        calls.clear()
        changed = pd.DataFrame({"id": ["d1"], "body": [base.replace("shipping delays", "shipping costs")]})
        r3 = _run(_component(mod, tmp_path), changed)
        assert calls == ["Second paragraph about shipping costs."]  # only the edited chunk
        stats = r3.asset_materializations_for_node("kb")[0].metadata["step_metadata"].value["embedded"]
        assert stats["embeddings_reused"] == 2 and stats["embeddings_computed"] == 1
        calls.clear()
        other = _component(mod, tmp_path, steps=[
            {"id": "chunks", "op": "chunk", "chunk_size": 60, "chunk_overlap": 0, "strategy": "recursive", "chunk_id_mode": "content"},
            {"id": "embedded", "op": "embed", "provider": "litellm", "model": "fake-2",
             "reuse_from": {"connection_string": str(tmp_path / "chroma"), "collection_name": "kb_test"}},
            {"id": "indexed", "op": "write_vector_store", "provider": "chromadb",
             "connection_string": str(tmp_path / "chroma"), "collection_name": "kb_test"},
        ])
        _run(other, changed)
        assert len(calls) == 3  # new embedding model: nothing reusable


def test_enrich_rejected_in_sql_mode(mod):
    """enrich has no warehouse-native equivalent (an arbitrary per-chunk LLM prompt) and stays
    python-mode-only; filter/dedupe/sensitivity/stamp are real execution_mode='sql' steps now
    (see test_sql_filter_dedupe_sensitivity_stamp.py for their real, executed coverage)."""
    comp = mod.ContextEngineeringPipelineComponent(
        asset_name="kb", id_column="id", text_column="body", execution_mode="sql", sql_dialect="snowflake_cortex",
        output_table="db.s.t", source={"kind": "warehouse_query", "resource_key": "r", "sql": "select 1"},
        steps=[{"id": "c", "op": "chunk"}, {"id": "e", "op": "enrich", "mode": "summary", "model": "gpt-4o-mini"}])
    with pytest.raises(ValueError, match="python-mode-only"):
        comp.build_defs(None)


def test_new_sql_ops_rejected_with_per_step_ops(mod):
    comp = mod.ContextEngineeringPipelineComponent(
        asset_name="kb", id_column="id", text_column="body", execution_mode="sql", sql_dialect="snowflake_cortex",
        output_table="db.s.t", source={"kind": "warehouse_query", "resource_key": "r", "sql": "select 1"},
        per_step_ops=True,
        steps=[{"id": "c", "op": "chunk"}, {"id": "d", "op": "dedupe"}])
    with pytest.raises(ValueError, match="aren't available as their own per-step SQL tables"):
        comp.build_defs(None)
