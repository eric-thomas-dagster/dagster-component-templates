"""Committed tests for KnowledgeGraphBuilderComponent. Real pandas/networkx throughout;
litellm.completion is monkeypatched (the only paid call) rather than hitting a real model.
sentence_transformers is exercised for real (skipped if not installed, same convention as
context_engineering_pipeline's own tests) since it's free/local."""
import json
import os

import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module, requires_duckdb, requires_sqlglot

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


# ─── extract: entity_filter (noise reduction) ──────────────────────────────
# Real production evidence (docs.dagster.io, method='spacy', min_mentions: 2): the top
# "entities" by mention count were "you" (1766), "that" (1208), "we" (689), a zero-width
# space (631), "it" (492), "which" (468) -- pronouns/determiners/invisible characters, not
# real entities. entity_filter='basic' (default) drops these; 'none' restores old behavior.

def test_entity_text_is_noisy_catches_pronouns_zero_width_space_and_short_text(mod):
    assert mod._entity_text_is_noisy("you", 3) is True  # pronoun
    assert mod._entity_text_is_noisy("​", 3) is True  # zero-width space: no alphabetic char
    assert mod._entity_text_is_noisy("ab", 3) is True  # shorter than min_entity_chars
    assert mod._entity_text_is_noisy("", 3) is True
    assert mod._entity_text_is_noisy("   ", 3) is True
    assert mod._entity_text_is_noisy("Acme", 3) is False


class _FakeLexeme:
    def __init__(self, is_oov):
        self.is_oov = is_oov


class _FakeVocab:
    """Fakes just enough of spaCy's Vocab for _is_real_word: `len(vocab.vectors)` and
    `vocab[lemma].is_oov`."""
    def __init__(self, n_vectors, oov_lemmas=()):
        self.vectors = [None] * n_vectors
        self._oov_lemmas = set(oov_lemmas)

    def __getitem__(self, lemma):
        return _FakeLexeme(is_oov=lemma in self._oov_lemmas)


def test_is_real_word_requires_alphabetic_lemma(mod):
    assert mod._is_real_word("", _FakeVocab(0)) is False
    assert mod._is_real_word("123", _FakeVocab(0)) is False


def test_is_real_word_with_no_vectors_is_a_no_op(mod):
    # en_core_web_sm (this component's default spacy_model) ships with no word vectors --
    # is_oov is unreliable for it, so the check must not flag every lemma as fake.
    vocab = _FakeVocab(n_vectors=0)
    assert mod._is_real_word("dependencie", vocab) is True


def test_is_real_word_with_vectors_flags_oov_lemma(mod):
    vocab = _FakeVocab(n_vectors=1, oov_lemmas={"dependencie"})
    assert mod._is_real_word("dependencie", vocab) is False
    assert mod._is_real_word("dependency", vocab) is True


def test_extract_llm_entity_filter_basic_drops_pronoun_entities(mod, monkeypatch):
    def fake_completion(model, messages, api_key, temperature):
        payload = [
            {"subject": "you", "relation": "use", "object": "Acme Corp", "confidence": 0.9, "evidence": "e"},
            {"subject": "Alice", "relation": "works_at", "object": "Acme Corp", "confidence": 0.9, "evidence": "e"},
        ]
        return _FakeResponse(json.dumps(payload))

    import litellm
    monkeypatch.setattr(litellm, "completion", fake_completion)
    out = mod._do_extract(_chunk_df(), {"method": "llm", "model": "gpt-4o-mini"}, "doc_id", _Logger())
    assert set(out["subject"]) == {"Alice"}  # "you" dropped from both chunks
    assert out.attrs["filter_counts"] == {"entities_filtered": 2, "triples_filtered": 2}


def test_extract_llm_entity_filter_none_keeps_pronoun_entities(mod, monkeypatch):
    def fake_completion(model, messages, api_key, temperature):
        return _FakeResponse(json.dumps([{"subject": "you", "relation": "use", "object": "Acme Corp", "confidence": 0.9, "evidence": "e"}]))

    import litellm
    monkeypatch.setattr(litellm, "completion", fake_completion)
    out = mod._do_extract(_chunk_df(), {"method": "llm", "model": "gpt-4o-mini", "entity_filter": "none"}, "doc_id", _Logger())
    assert "you" in set(out["subject"])
    assert out.attrs["filter_counts"] == {"entities_filtered": 0, "triples_filtered": 0}


def test_extract_llm_entity_filter_must_be_basic_or_none(mod):
    with pytest.raises(ValueError, match="entity_filter must be"):
        mod._do_extract(_chunk_df(), {"method": "llm", "model": "x", "entity_filter": "bogus"}, "doc_id", _Logger())


class _FakeToken:
    """Fakes just enough of a spaCy Token for _extract_spacy_sentence: .text, .pos_, .dep_,
    .i (index in the sentence, used by _entity_span_at), .lemma_, .is_stop, and .children
    (iterated directly, not a real spaCy Children generator)."""
    def __init__(self, text, pos_, dep_, i, lemma_=None, is_stop=False, children=()):
        self.text = text
        self.pos_ = pos_
        self.dep_ = dep_
        self.i = i
        self.lemma_ = lemma_ if lemma_ is not None else text.lower()
        self.is_stop = is_stop
        self.children = list(children)


class _FakeSpan:
    """Fakes just enough of a spaCy entity Span for _extract_spacy_sentence: .text, .label_,
    .root (the span's syntactic head token), .start/.end (token index range)."""
    def __init__(self, text, label_, root, start, end):
        self.text = text
        self.label_ = label_
        self.root = root
        self.start = start
        self.end = end


class _FakeSent:
    def __init__(self, tokens, text):
        self._tokens = tokens
        self.text = text

    def __iter__(self):
        return iter(self._tokens)


def _pronoun_sentence():
    """'You use that.' -- a pronoun subject and a pronoun object, no named entities
    recognized (matches the real evidence: pronouns are extracted as raw dependency-parse
    children, never as spaCy NER spans, so they only ever reach this filter as raw tokens)."""
    subj = _FakeToken("You", pos_="PRON", dep_="nsubj", i=0, is_stop=True)
    verb = _FakeToken("use", pos_="VERB", dep_="ROOT", i=1, lemma_="use")
    obj = _FakeToken("that", pos_="PRON", dep_="dobj", i=2, is_stop=True)
    verb.children = [subj, obj]
    return _FakeSent([subj, verb, obj], "You use that.")


def test_extract_spacy_sentence_basic_filter_drops_pronoun_subject_and_object(mod):
    triples, entities_filtered, triples_filtered = mod._extract_spacy_sentence(_pronoun_sentence(), ents=[], entity_filter="basic")
    assert triples == []
    assert entities_filtered == 2
    assert triples_filtered == 1


def test_extract_spacy_sentence_none_filter_keeps_pronoun_nodes(mod):
    triples, entities_filtered, triples_filtered = mod._extract_spacy_sentence(_pronoun_sentence(), ents=[], entity_filter="none")
    assert len(triples) == 1
    assert triples[0]["subject"] == "You"
    assert triples[0]["object"] == "that"
    assert entities_filtered == 0
    assert triples_filtered == 0


def _real_entity_sentence():
    """'Dagster orchestrates pipelines.' -- a PROPN subject and a NOUN object, both real
    entities entity_filter='basic' must keep."""
    subj = _FakeToken("Dagster", pos_="PROPN", dep_="nsubj", i=0)
    verb = _FakeToken("orchestrates", pos_="VERB", dep_="ROOT", i=1, lemma_="orchestrate")
    obj = _FakeToken("pipelines", pos_="NOUN", dep_="dobj", i=2)
    verb.children = [subj, obj]
    return _FakeSent([subj, verb, obj], "Dagster orchestrates pipelines.")


def test_extract_spacy_sentence_basic_filter_keeps_propn_and_noun_entities(mod):
    triples, entities_filtered, triples_filtered = mod._extract_spacy_sentence(_real_entity_sentence(), ents=[], entity_filter="basic")
    assert len(triples) == 1
    assert triples[0]["subject"] == "Dagster"
    assert triples[0]["object"] == "pipelines"
    assert entities_filtered == 0
    assert triples_filtered == 0


def test_extract_spacy_sentence_basic_filter_checks_entity_span_root_pos(mod):
    """When spaCy's NER recognizes a multi-token span ("Acme Corp"), the POS check applies
    to the span's root token (its syntactic head), not the raw dependency-parse child token
    the span happened to be found at."""
    root_tok = _FakeToken("Corp", pos_="PROPN", dep_="ROOT", i=2)
    raw_obj_tok = _FakeToken("Acme", pos_="PROPN", dep_="dobj", i=1)
    span = _FakeSpan("Acme Corp", "ORG", root=root_tok, start=1, end=3)
    subj = _FakeToken("Alice", pos_="PROPN", dep_="nsubj", i=0)
    verb = _FakeToken("founded", pos_="VERB", dep_="ROOT", i=3, lemma_="found")
    verb.children = [subj, raw_obj_tok]
    sent = _FakeSent([subj, verb, raw_obj_tok], "Alice founded Acme Corp.")
    triples, entities_filtered, triples_filtered = mod._extract_spacy_sentence(sent, ents=[span], entity_filter="basic")
    assert len(triples) == 1
    assert triples[0]["object"] == "Acme Corp"
    assert triples[0]["object_type"] == "ORG"
    assert entities_filtered == 0


def test_extract_spacy_sentence_basic_filter_drops_relation_with_unreal_lemma(mod):
    vocab = _FakeVocab(n_vectors=1, oov_lemmas={"orchestrix"})
    sent = _real_entity_sentence()
    sent._tokens[1].lemma_ = "orchestrix"  # a lemmatizer artifact, not a real word
    triples, entities_filtered, triples_filtered = mod._extract_spacy_sentence(sent, ents=[], entity_filter="basic", vocab=vocab)
    assert triples == []
    assert triples_filtered == 1


def test_extract_spacy_sentence_respects_custom_entity_pos_and_min_entity_chars(mod):
    subj = _FakeToken("AI", pos_="PROPN", dep_="nsubj", i=0)  # 2 chars, under the default min_entity_chars=3
    verb = _FakeToken("helps", pos_="VERB", dep_="ROOT", i=1, lemma_="help")
    obj = _FakeToken("teams", pos_="NOUN", dep_="dobj", i=2)
    verb.children = [subj, obj]
    sent = _FakeSent([subj, verb, obj], "AI helps teams.")
    triples, _, triples_filtered = mod._extract_spacy_sentence(sent, ents=[], entity_filter="basic")
    assert triples == []  # "AI" is shorter than the default min_entity_chars=3
    assert triples_filtered == 1
    triples, _, triples_filtered = mod._extract_spacy_sentence(sent, ents=[], entity_filter="basic", min_entity_chars=2)
    assert len(triples) == 1  # with a lower min_entity_chars, "AI" now qualifies


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


# ─── execution_mode='sql': in-warehouse extraction + write_graph(provider='sql_tables') ────────
# Snowflake/Databricks extract SQL is checked for real syntactic validity with
# sqlglot.parse(sql, read=dialect); BigQuery's ML.GENERATE_TEXT table-function call has no sqlglot
# support as of sqlglot 28.0.0 (confirmed directly, same as context_engineering_pipeline's own
# ai_stubs.py note) so that one fragment is checked structurally instead, and the rest of the
# BigQuery query (JSON flattening, filtering, ROW_NUMBER) IS sqlglot-parsed. write_graph's
# MERGE/aggregation logic is executed for real against DuckDB (not one of the 3 supported
# warehouses, but DuckDB's own MERGE INTO + ARRAY_AGG are close enough to exercise the real control
# flow -- same stand-in technique assets/analytics/anomaly_detection's own tests use).

SOURCE_SQL = "SELECT doc_id, chunk_id, content_hash, chunk_text FROM support_kb_chunks"


@requires_sqlglot
@pytest.mark.parametrize("dialect,read,model", [
    ("snowflake_cortex", "snowflake", "llama3.1-70b"),
    ("databricks", "databricks", "databricks-meta-llama-3-1-70b-instruct"),
])
def test_sql_extract_query_is_valid_sql_for_dialect(mod, dialect, read, model):
    import sqlglot
    sql = mod._build_sql_extract_query(dialect, SOURCE_SQL, "doc_id", "chunk_text", model,
                                       ["PERSON", "ORG"], ["works_at"], 0.5, 5)
    parsed = sqlglot.parse(sql, read=read)
    assert len(parsed) == 1


@requires_sqlglot
def test_sql_extract_query_bigquery_structure_and_json_flattening_parses(mod):
    import sqlglot
    sql = mod._build_sql_extract_query("bigquery", SOURCE_SQL, "doc_id", "chunk_text",
                                       "my-project.my_dataset.my_model", None, None, 0.0, None)
    # ML.GENERATE_TEXT's MODEL/STRUCT table-function syntax has no sqlglot support as of 28.0.0 --
    # checked structurally against the real documented shape instead.
    assert "ML.GENERATE_TEXT(MODEL `my-project.my_dataset.my_model`," in sql
    assert "STRUCT(TRUE AS flatten_json_output)" in sql
    assert "ml_generate_text_llm_result AS raw_json" in sql
    # The rest (JSON_EXTRACT_SCALAR/SAFE.JSON_EXTRACT_ARRAY flattening, filtering) IS real BigQuery SQL.
    flatten_only = sql.split("joined AS (", 1)[1]
    flatten_only = "WITH joined AS (" + flatten_only
    sqlglot.parse(flatten_only, read="bigquery")


def test_sql_extract_query_unsupported_dialect_raises(mod):
    with pytest.raises(ValueError, match="unsupported sql_dialect"):
        mod._build_sql_extract_query("postgres", SOURCE_SQL, "doc_id", "chunk_text", "m", None, None)


@requires_sqlglot
@pytest.mark.parametrize("dialect,read", [
    ("snowflake_cortex", "snowflake"), ("databricks", "databricks"), ("bigquery", "bigquery"),
])
def test_sql_write_graph_ddl_and_merge_statements_are_valid_sql(mod, dialect, read):
    import sqlglot
    nodes_tbl, edges_tbl = mod._sql_nodes_edges_table_names("support_kg", dialect)
    for sql in (
        mod._build_sql_create_nodes_table(dialect, nodes_tbl),
        mod._build_sql_create_edges_table(dialect, edges_tbl),
        mod._build_sql_nodes_merge(nodes_tbl, mod._build_sql_nodes_agg_query("staging")),
        mod._build_sql_edges_merge(edges_tbl, mod._build_sql_edges_agg_query(dialect, "staging")),
    ):
        sqlglot.parse(sql, read=read)


class _Ctx:
    log = _Logger.log


@requires_duckdb
def test_do_write_graph_sql_upsert_merges_and_computes_stats(mod, monkeypatch):
    import duckdb
    # DuckDB stand-in: its parser doesn't accept Snowflake's untyped ARRAY/NUMBER column types, so
    # swap in DuckDB-native spellings for this one real-execution test only (production code still
    # emits the real Snowflake types -- checked separately by test_sql_write_graph_ddl_and_merge_statements_are_valid_sql).
    monkeypatch.setitem(mod._SQL_TYPES["snowflake_cortex"], "array_string", "STRING[]")
    monkeypatch.setitem(mod._SQL_TYPES["snowflake_cortex"], "int", "BIGINT")

    con = duckdb.connect()
    con.execute(
        "CREATE TABLE staging (doc_id STRING, chunk_id STRING, content_hash STRING, subject STRING, "
        "subject_type STRING, relation STRING, object STRING, object_type STRING, confidence DOUBLE, evidence STRING)"
    )
    con.execute("""INSERT INTO staging VALUES
        ('d1','c1','h1','Alice','PERSON','works_at','Acme','ORG',0.9,'ev1'),
        ('d2','c2','h2','Bob','PERSON','works_at','Acme','ORG',0.7,'ev2')
    """)
    session = mod._SqlSession(lambda s: con.execute(s), lambda s: con.execute(s).fetchall())

    stats = mod._do_write_graph_sql(session, "snowflake_cortex", "staging", "kg", "upsert", False, 60, _Ctx())
    assert stats["node_count"] == 3
    assert stats["edge_count"] == 2
    assert stats["chunk_count"] == 2
    assert stats["entities_linked_to_chunks"] == 3
    assert stats["preview_truncated"] is False
    assert {n["name"] for n in stats["graph_preview"]["nodes"]} == {"Alice", "Bob", "Acme"}
    nodes = dict((r[0], r[3]) for r in con.execute("SELECT * FROM kg_nodes").fetchall())
    assert nodes == {"Alice": 1, "Bob": 1, "Acme": 2}

    # Second run: only Alice-Acme triple present. Upsert REPLACES that edge's own provenance
    # (not accumulated with history, same as python mode's own upsert) but leaves Bob-Acme alone.
    con.execute("DELETE FROM staging")
    con.execute("INSERT INTO staging VALUES ('d1','c1','h1','Alice','PERSON','works_at','Acme','ORG',0.95,'ev1-updated')")
    mod._do_write_graph_sql(session, "snowflake_cortex", "staging", "kg", "upsert", False, 60, _Ctx())
    edges = {(r[0], r[1]): (r[3], r[4]) for r in con.execute("SELECT * FROM kg_edges").fetchall()}
    assert edges[("Alice", "Acme")] == (["c1"], "ev1-updated")
    assert ("Bob", "Acme") in edges  # untouched by upsert


@requires_duckdb
def test_do_write_graph_sql_replace_all_clears_other_rows(mod, monkeypatch):
    import duckdb
    monkeypatch.setitem(mod._SQL_TYPES["snowflake_cortex"], "array_string", "STRING[]")
    monkeypatch.setitem(mod._SQL_TYPES["snowflake_cortex"], "int", "BIGINT")
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE staging (doc_id STRING, chunk_id STRING, content_hash STRING, subject STRING, "
        "subject_type STRING, relation STRING, object STRING, object_type STRING, confidence DOUBLE, evidence STRING)"
    )
    con.execute("INSERT INTO staging VALUES ('d1','c1','h1','Alice','PERSON','works_at','Acme','ORG',0.9,'ev1')")
    session = mod._SqlSession(lambda s: con.execute(s), lambda s: con.execute(s).fetchall())
    mod._do_write_graph_sql(session, "snowflake_cortex", "staging", "kg2", "upsert", False, 60, _Ctx())

    con.execute("DELETE FROM staging")
    con.execute("INSERT INTO staging VALUES ('d2','c2','h2','Zed','PERSON','works_at','Beta','ORG',0.9,'ev2')")
    mod._do_write_graph_sql(session, "snowflake_cortex", "staging", "kg2", "replace_all", False, 60, _Ctx())
    names = {r[0] for r in con.execute("SELECT * FROM kg2_nodes").fetchall()}
    assert names == {"Zed", "Beta"}  # Alice/Acme gone


@requires_duckdb
def test_do_write_graph_sql_graph_preview_truncates(mod, monkeypatch):
    import duckdb
    monkeypatch.setitem(mod._SQL_TYPES["snowflake_cortex"], "array_string", "STRING[]")
    monkeypatch.setitem(mod._SQL_TYPES["snowflake_cortex"], "int", "BIGINT")
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE staging (doc_id STRING, chunk_id STRING, content_hash STRING, subject STRING, "
        "subject_type STRING, relation STRING, object STRING, object_type STRING, confidence DOUBLE, evidence STRING)"
    )
    con.execute("""INSERT INTO staging VALUES
        ('d1','c1','h1','A','X','r1','B','X',0.9,'e1'),
        ('d1','c1','h1','A','X','r2','C','X',0.9,'e2'),
        ('d2','c2','h2','D','X','r3','E','X',0.9,'e3')
    """)
    session = mod._SqlSession(lambda s: con.execute(s), lambda s: con.execute(s).fetchall())
    stats = mod._do_write_graph_sql(session, "snowflake_cortex", "staging", "kg3", "upsert", False, 2, _Ctx())
    assert stats["node_count"] == 5
    assert len(stats["graph_preview"]["nodes"]) == 2
    assert {n["name"] for n in stats["graph_preview"]["nodes"]} == {"A", "B"}
    assert stats["graph_preview"]["edges"] == [{"source": "A", "target": "B", "relation": "r1", "weight": 1}]
    assert stats["preview_truncated"] is True


@requires_duckdb
def test_do_write_graph_sql_replace_sources_not_supported(mod):
    session = mod._SqlSession(lambda s: None, lambda s: [])
    with pytest.raises(ValueError, match="replace_sources"):
        mod._do_write_graph_sql(session, "snowflake_cortex", "staging", "kg", "replace_sources", False, 60, _Ctx())


@requires_duckdb
def test_do_write_graph_sql_replace_all_refused_when_partitioned(mod):
    session = mod._SqlSession(lambda s: None, lambda s: [])
    with pytest.raises(ValueError, match="replace_all"):
        mod._do_write_graph_sql(session, "snowflake_cortex", "staging", "kg", "replace_all", True, 60, _Ctx())


def test_do_write_graph_sql_bad_sync_mode_raises(mod):
    session = mod._SqlSession(lambda s: None, lambda s: [])
    with pytest.raises(ValueError, match="sync_mode"):
        mod._do_write_graph_sql(session, "snowflake_cortex", "staging", "kg", "bogus", False, 60, _Ctx())


# ─── execution_mode='sql': build_defs-level validation ─────────────────────────────────────────

def test_sql_mode_requires_valid_sql_dialect(mod):
    cls = getattr(mod, CLS)
    comp = cls(asset_name="kg", id_column="doc_id", text_column="chunk_text", execution_mode="sql",
               source={"kind": "warehouse_query", "resource_key": "wh", "sql": SOURCE_SQL},
               steps=[{"id": "t", "op": "extract", "model": "m"}, {"id": "g", "op": "write_graph", "output_table": "kg"}])
    with pytest.raises(ValueError, match="sql_dialect"):
        comp.build_defs(None)


def test_sql_mode_requires_exactly_extract_then_write_graph_steps(mod):
    cls = getattr(mod, CLS)
    comp = cls(asset_name="kg", id_column="doc_id", text_column="chunk_text", execution_mode="sql", sql_dialect="snowflake_cortex",
               source={"kind": "warehouse_query", "resource_key": "wh", "sql": SOURCE_SQL},
               steps=[{"id": "c", "op": "chunk"}, {"id": "t", "op": "extract", "model": "m"}, {"id": "g", "op": "write_graph", "output_table": "kg"}])
    with pytest.raises(ValueError, match="exactly two steps"):
        comp.build_defs(None)


def test_sql_mode_rejects_spacy_method(mod):
    cls = getattr(mod, CLS)
    comp = cls(asset_name="kg", id_column="doc_id", text_column="chunk_text", execution_mode="sql", sql_dialect="snowflake_cortex",
               source={"kind": "warehouse_query", "resource_key": "wh", "sql": SOURCE_SQL},
               steps=[{"id": "t", "op": "extract", "method": "spacy"}, {"id": "g", "op": "write_graph", "output_table": "kg"}])
    with pytest.raises(ValueError, match="method='llm'"):
        comp.build_defs(None)


def test_sql_mode_rejects_non_sql_tables_provider(mod):
    cls = getattr(mod, CLS)
    comp = cls(asset_name="kg", id_column="doc_id", text_column="chunk_text", execution_mode="sql", sql_dialect="snowflake_cortex",
               source={"kind": "warehouse_query", "resource_key": "wh", "sql": SOURCE_SQL},
               steps=[{"id": "t", "op": "extract", "model": "m"}, {"id": "g", "op": "write_graph", "provider": "networkx", "output_table": "kg"}])
    with pytest.raises(ValueError, match="sql_tables"):
        comp.build_defs(None)


def test_sql_mode_rejects_upstream_asset_key(mod):
    cls = getattr(mod, CLS)
    comp = cls(asset_name="kg", id_column="doc_id", text_column="chunk_text", execution_mode="sql", sql_dialect="snowflake_cortex",
               upstream_asset_key="chunks",
               steps=[{"id": "t", "op": "extract", "model": "m"}, {"id": "g", "op": "write_graph", "output_table": "kg"}])
    with pytest.raises(ValueError, match="upstream_asset_key"):
        comp.build_defs(None)


def test_sql_mode_requires_warehouse_query_source(mod):
    cls = getattr(mod, CLS)
    comp = cls(asset_name="kg", id_column="doc_id", text_column="chunk_text", execution_mode="sql", sql_dialect="snowflake_cortex",
               steps=[{"id": "t", "op": "extract", "model": "m"}, {"id": "g", "op": "write_graph", "output_table": "kg"}])
    with pytest.raises(ValueError, match="source"):
        comp.build_defs(None)


def test_bad_execution_mode_raises(mod):
    cls = getattr(mod, CLS)
    comp = cls(asset_name="kg", id_column="doc_id", text_column="chunk_text", execution_mode="rust",  # type: ignore[arg-type]
               upstream_asset_key="x", steps=[{"id": "c", "op": "chunk"}])
    with pytest.raises(ValueError, match="execution_mode"):
        comp.build_defs(None)


# ─── execution_mode='sql': full orchestration (_run_knowledge_graph_sql_mode) ──────────────────

@requires_duckdb
def test_run_knowledge_graph_sql_mode_end_to_end(mod, monkeypatch):
    """Full orchestration against a real DuckDB-backed resource: partition-placeholder-free path,
    staging table created then dropped, final Output/metadata shape. The extract step's own SQL
    generation (_build_sql_extract_query) is swapped for a trivial literal-values SELECT so this
    doesn't need a real warehouse LLM function -- that generation is covered separately by the
    sqlglot-parse tests above; this test is about the surrounding control flow."""
    import duckdb

    monkeypatch.setitem(mod._SQL_TYPES["snowflake_cortex"], "array_string", "STRING[]")
    monkeypatch.setitem(mod._SQL_TYPES["snowflake_cortex"], "int", "BIGINT")

    def _fake_extract_query(dialect, source_sql, id_column, text_column, model, entity_types, relation_types, min_confidence=0.0, max_triples_per_chunk=None):
        return (
            "SELECT * FROM (VALUES ('d1','c1','h1','Alice','PERSON','works_at','Acme','ORG',0.9,'ev1')) "
            "AS t(doc_id, chunk_id, content_hash, subject, subject_type, relation, object, object_type, confidence, evidence)"
        )
    monkeypatch.setattr(mod, "_build_sql_extract_query", _fake_extract_query)

    con = duckdb.connect()

    class _FakeResource:
        def get_connection(self):
            import contextlib as _cl
            @_cl.contextmanager
            def _cm():
                yield con
            return _cm()

    class _FakeResources:
        wh = _FakeResource()

    class _FakeCtx:
        log = _Logger.log
        resources = _FakeResources()
        has_partition_key = False
        has_partition_key_range = False

    out = mod._run_knowledge_graph_sql_mode(
        _FakeCtx(), [{"id": "t", "op": "extract", "model": "gpt-oss"}, {"id": "g", "op": "write_graph", "output_table": "e2e_kg"}],
        {"kind": "warehouse_query", "resource_key": "wh", "sql": SOURCE_SQL}, "snowflake_cortex", "doc_id", "chunk_text", False,
    )
    assert out.value is None
    assert out.metadata["node_count"].value == 2
    assert out.metadata["edge_count"].value == 1
    assert out.metadata["sql_dialect"].value == "snowflake_cortex"
    assert out.metadata["execution_mode"].value == "sql"
    # staging table was cleaned up
    staging_tables = [r[0] for r in con.execute("SHOW TABLES").fetchall() if "staging" in r[0]]
    assert staging_tables == []


@requires_duckdb
def test_run_knowledge_graph_sql_mode_requires_extract_model(mod):
    class _FakeCtx:
        log = _Logger.log
        has_partition_key = False
        has_partition_key_range = False

    with pytest.raises(ValueError, match="requires `model`"):
        mod._run_knowledge_graph_sql_mode(
            _FakeCtx(), [{"id": "t", "op": "extract"}, {"id": "g", "op": "write_graph", "output_table": "kg"}],
            {"kind": "warehouse_query", "resource_key": "wh", "sql": SOURCE_SQL}, "snowflake_cortex", "doc_id", "chunk_text", False,
        )
