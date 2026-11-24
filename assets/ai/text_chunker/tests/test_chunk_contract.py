"""stamp_chunk_contract: the shared vector-metadata contract (document_id, source_uri,
page_start/page_end, section, chunk_index, chunk_id, content_hash, extractor_version) stamped onto
every chunk -- same field names and content_hash approach as context_engineering_pipeline's
stamp_provenance (see that component's README), ported here since text_chunker has no
pipeline-step/recipe concept of its own. embedding_model is NOT stamped here (this component runs
before embedding) -- see vector_store_writer's own embedding_model field.

Also exercises a real, pre-existing bug fixed alongside this work: a nested `import pandas as pd`
inside the partition-filtering block shadowed the module-level import for the WHOLE function (not
just that branch -- that's how Python scoping works), so every UNPARTITIONED materialization of
this asset raised UnboundLocalError at `pd.DataFrame(all_chunk_records)`. Confirmed live against the
unmodified component before the fix.
"""
import numbers

import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize(component, df, instance=None, raise_on_error=True):
    defs = component.build_defs(context=None)

    @dg.asset(name="raw_docs")
    def raw_docs():
        return df

    return dg.materialize(
        [raw_docs, *defs.assets, *defs.asset_checks], instance=instance, raise_on_error=raise_on_error
    )


def _component(**kw):
    attrs = dict(
        asset_name="chunks_out", upstream_asset_key="raw_docs", chunking_strategy="fixed_chars",
        chunk_size=1000, chunk_overlap=0, source_column="text", preserve_sentences=False,
    )
    attrs.update(kw)
    return attrs


def test_unpartitioned_materialize_succeeds_at_all(mod):
    """Regression test for the UnboundLocalError bug described above -- this used to crash every
    single unpartitioned materialization of this asset, with no existing test ever catching it."""
    df = pd.DataFrame({"text": ["Hello world, a short plain document for this test."]})
    component = mod.TextChunkerComponent(**_component())
    result = _materialize(component, df)
    assert result.success


def test_idempotent_chunk_ids_and_hashes_on_rerun(mod):
    df = pd.DataFrame({
        "text": ["Alpha paragraph one. Alpha paragraph two is here as well, a bit longer."],
        "doc_id": ["doc-a"],
    })
    component = mod.TextChunkerComponent(**_component(document_id_column="doc_id"))
    out1 = _materialize(component, df).output_for_node("chunks_out")
    out2 = _materialize(component, df).output_for_node("chunks_out")
    assert list(out1["chunk_id"]) == list(out2["chunk_id"])
    assert list(out1["content_hash"]) == list(out2["content_hash"])


def test_changing_one_documents_text_only_changes_that_documents_chunks(mod):
    df_before = pd.DataFrame({
        "text": [
            "Document A paragraph one. Document A paragraph two follows right along nicely.",
            "Document B original text that will change between the two runs of this test.",
        ],
        "doc_id": ["doc-a", "doc-b"],
    })
    df_after = df_before.copy()
    df_after.loc[1, "text"] = "Document B text has now been edited completely to something else."

    component = mod.TextChunkerComponent(**_component(document_id_column="doc_id", chunk_size=500))
    out_before = _materialize(component, df_before).output_for_node("chunks_out")
    out_after = _materialize(component, df_after).output_for_node("chunks_out")

    before_a = out_before[out_before["document_id"] == "doc-a"]
    after_a = out_after[out_after["document_id"] == "doc-a"]
    assert list(before_a["chunk_id"]) == list(after_a["chunk_id"])
    assert list(before_a["content_hash"]) == list(after_a["content_hash"])

    before_b = out_before[out_before["document_id"] == "doc-b"]
    after_b = out_after[out_after["document_id"] == "doc-b"]
    assert list(before_b["content_hash"]) != list(after_b["content_hash"])


def test_every_contract_field_present_and_correctly_typed(mod):
    df = pd.DataFrame({
        "text": ["# Intro\nHello there world, this is a reasonably short test document for the suite."],
        "doc_id": ["doc-a"],
        "uri": ["s3://bucket/doc-a.md"],
    })
    component = mod.TextChunkerComponent(
        **_component(document_id_column="doc_id", source_uri_column="uri", chunk_size=1000)
    )
    out = _materialize(component, df).output_for_node("chunks_out")
    for col in (
        "document_id", "source_uri", "page_start", "page_end", "section",
        "chunk_index", "chunk_id", "content_hash", "extractor_version",
    ):
        assert col in out.columns, f"missing contract column: {col}"

    row = out.iloc[0]
    assert isinstance(row["document_id"], str) and row["document_id"] == "doc-a"
    assert row["source_uri"] == "s3://bucket/doc-a.md"
    assert row["page_start"] is None and row["page_end"] is None  # no page_column configured -- never fabricated
    assert row["section"] == "Intro"
    assert isinstance(row["chunk_index"], numbers.Integral)
    assert isinstance(row["chunk_id"], str) and row["chunk_id"].startswith("doc-a_")
    assert isinstance(row["content_hash"], str) and len(row["content_hash"]) == 64
    assert isinstance(row["extractor_version"], str) and row["extractor_version"].startswith("cfg-")


def test_page_column_stamps_page_start_and_page_end_when_present(mod):
    df = pd.DataFrame({
        "text": ["Some document text long enough to produce at least one chunk for this test."],
        "page": [7],
    })
    component = mod.TextChunkerComponent(**_component(page_column="page"))
    out = _materialize(component, df).output_for_node("chunks_out")
    assert (out["page_start"] == 7).all()
    assert (out["page_end"] == 7).all()


def test_contract_present_whether_or_not_add_metadata_is_set(mod):
    """add_metadata is this component's own, pre-existing, unrelated metadata toggle
    (char_start/char_end/word_count/total_chunks) -- the contract fields must be present either
    way, not conditional on it."""
    df = pd.DataFrame({"text": ["Plain document text long enough to chunk for this test case."]})
    component = mod.TextChunkerComponent(**_component(add_metadata=False))
    out = _materialize(component, df).output_for_node("chunks_out")
    assert "content_hash" in out.columns
    assert "chunk_id" in out.columns
    assert "total_chunks" not in out.columns  # add_metadata's own field, unaffected


def test_disabling_contract_omits_every_new_column(mod):
    df = pd.DataFrame({"text": ["Plain document text for the opt-out test case here."]})
    component = mod.TextChunkerComponent(**_component(stamp_chunk_contract=False))
    out = _materialize(component, df).output_for_node("chunks_out")
    for col in (
        "document_id", "source_uri", "page_start", "page_end", "section",
        "chunk_id", "content_hash", "extractor_version",
    ):
        assert col not in out.columns


def test_schema_change_check_catches_a_dropped_contract_field(mod):
    df = pd.DataFrame({"text": ["Document text long enough to produce a chunk for this check."]})

    def _build(stamp):
        return mod.TextChunkerComponent(**_component(stamp_chunk_contract=stamp)).build_defs(context=None)

    instance = dg.DagsterInstance.ephemeral()

    @dg.asset(name="raw_docs")
    def raw_docs():
        return df

    defs1 = _build(True)
    result1 = dg.materialize([raw_docs, *defs1.assets, *defs1.asset_checks], instance=instance, raise_on_error=True)
    evals1 = result1.get_asset_check_evaluations()
    assert len(evals1) == 1 and evals1[0].passed

    defs2 = _build(False)
    result2 = dg.materialize([raw_docs, *defs2.assets, *defs2.asset_checks], instance=instance, raise_on_error=False)
    evals2 = result2.get_asset_check_evaluations()
    assert len(evals2) == 1
    assert evals2[0].passed is False
    assert "document_id" in evals2[0].description
