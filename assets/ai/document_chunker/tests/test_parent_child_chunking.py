"""Parent-child (small-to-big) chunking: parent_chunk_size, when set, splits each document into
larger parent passages first, then each parent into the regular, smaller chunk_size child chunks
-- every child chunk carries parent_id/parent_text so retrieval can match precisely on the small
chunk but return the surrounding context. Mirrors context_engineering_pipeline's own
parent_chunk_size/parent_chunk_overlap design (that component already had this feature;
document_chunker did not).
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize(component, df):
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]

    @dg.asset(name="raw_docs")
    def raw_docs():
        return df

    return dg.materialize([asset_def, raw_docs])


def test_every_child_chunk_carries_parent_id_and_parent_text(mod):
    text = "Sentence one is here. Sentence two follows it. Sentence three comes next. Sentence four ends it."
    df = pd.DataFrame({"text": [text]})
    component = mod.DocumentChunkerComponent(
        asset_name="chunks_out",
        upstream_asset_key="raw_docs",
        strategy="fixed",
        chunk_size=20,
        chunk_overlap=0,
        min_chunk_size=0,  # default 100 + merge_small_chunks=True would merge these tiny test chunks back together
        parent_chunk_size=50,
        parent_chunk_overlap=0,
        source_column="text",
        preserve_sentences=False,
    )
    result = _materialize(component, df)
    assert result.success
    out = result.output_for_node("chunks_out")
    assert len(out) > 1
    assert "parent_id" in out.columns
    assert "parent_text" in out.columns
    assert out["parent_id"].notna().all()
    # Every child's parent_text must actually contain that child's own text (it was split FROM it).
    for _, row in out.iterrows():
        assert row["chunk"] in row["parent_text"]


def test_children_sharing_a_parent_share_the_same_parent_id_and_text(mod):
    text = "A" * 30 + " " + "B" * 30  # one parent passage split into two child chunks
    df = pd.DataFrame({"text": [text]})
    component = mod.DocumentChunkerComponent(
        asset_name="chunks_out",
        upstream_asset_key="raw_docs",
        strategy="fixed",
        chunk_size=10,
        chunk_overlap=0,
        min_chunk_size=0,
        parent_chunk_size=70,
        parent_chunk_overlap=0,
        source_column="text",
        preserve_sentences=False,
    )
    result = _materialize(component, df)
    out = result.output_for_node("chunks_out")
    assert out["parent_id"].nunique() == 1
    assert out["parent_text"].nunique() == 1


def test_unset_parent_chunk_size_behaves_exactly_as_before(mod):
    """No parent_id/parent_text column at all when the feature isn't used -- a real behavior
    change (a new, always-present column) would break every existing consumer's assumed schema."""
    df = pd.DataFrame({"text": ["Hello world. This is a test sentence to chunk."]})
    component = mod.DocumentChunkerComponent(
        asset_name="chunks_out",
        upstream_asset_key="raw_docs",
        strategy="fixed",
        chunk_size=1000,
        source_column="text",
    )
    result = _materialize(component, df)
    out = result.output_for_node("chunks_out")
    assert "parent_id" not in out.columns
    assert "parent_text" not in out.columns


def test_parent_chunk_size_must_be_larger_than_chunk_size(mod):
    with pytest.raises(ValueError, match="parent_chunk_size must be larger"):
        mod.DocumentChunkerComponent(
            asset_name="x", upstream_asset_key="raw_docs", strategy="fixed",
            chunk_size=500, parent_chunk_size=500, source_column="text",
        ).build_defs(context=None)


def test_parent_chunk_overlap_must_be_smaller_than_parent_chunk_size(mod):
    with pytest.raises(ValueError, match="parent_chunk_overlap must be smaller"):
        mod.DocumentChunkerComponent(
            asset_name="x", upstream_asset_key="raw_docs", strategy="fixed",
            chunk_size=100, chunk_overlap=0, parent_chunk_size=200, parent_chunk_overlap=200, source_column="text",
        ).build_defs(context=None)


def test_chunk_overlap_must_be_smaller_than_chunk_size(mod):
    """Pre-existing gap: this was never validated, risking an infinite loop in chunk_fixed's own
    `start = end - overlap` advance if overlap >= size. Fixed alongside the parent-child work
    since it's the exact same class of bug, now newly possible at two levels instead of one."""
    with pytest.raises(ValueError, match="chunk_overlap must be smaller"):
        mod.DocumentChunkerComponent(
            asset_name="x", upstream_asset_key="raw_docs", strategy="fixed",
            chunk_size=100, chunk_overlap=100, source_column="text",
        ).build_defs(context=None)


def test_parent_child_works_with_recursive_strategy(mod):
    text = "Para one here.\n\nPara two here.\n\nPara three here.\n\nPara four here."
    df = pd.DataFrame({"text": [text]})
    component = mod.DocumentChunkerComponent(
        asset_name="chunks_out",
        upstream_asset_key="raw_docs",
        strategy="recursive",
        chunk_size=20,
        chunk_overlap=0,
        min_chunk_size=0,
        parent_chunk_size=60,
        source_column="text",
    )
    result = _materialize(component, df)
    assert result.success
    out = result.output_for_node("chunks_out")
    assert out["parent_id"].notna().all()
