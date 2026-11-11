"""provider='lancedb': real end-to-end tests against a real (embedded, local) lancedb table --
no AWS/cloud account needed, same testability tier as chromadb/faiss. `dg.materialize()` is real
throughout. The other 5 providers (chromadb, pinecone, qdrant, faiss, s3vectors) are untouched by
this change and are not re-tested here.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module

mod = load_component_module()
VectorStoreQueryComponent = mod.VectorStoreQueryComponent

lancedb = pytest.importorskip("lancedb")


def _write_table(path, table_name, rows):
    """rows: list of (chunk_id, vector, text) -- mirrors the shape context_engineering_pipeline's
    write_vector_store op provider='lancedb' writes (a fixed 'vector' column + a 'chunk_id' column)."""
    db = lancedb.connect(str(path))
    df = pd.DataFrame({
        "chunk_id": [r[0] for r in rows],
        "vector": [r[1] for r in rows],
        "chunk_text": [r[2] for r in rows],
    })
    db.create_table(table_name, df, mode="overwrite")


def _component(**kw):
    attrs = dict(asset_name="results", provider="lancedb", collection_name="kb", upstream_asset_key="queries")
    attrs.update(kw)
    return VectorStoreQueryComponent(**attrs)


def _materialize(comp, df, **kwargs):
    @dg.asset(name="queries")
    def queries():
        return df

    defs = comp.build_defs(None)
    return dg.materialize([queries, *defs.assets], **kwargs)


def test_ranks_by_vector_similarity(tmp_path):
    _write_table(tmp_path / "lance", "kb", [
        ("c0", [1.0, 0.0], "cat"),
        ("c1", [0.0, 1.0], "dog"),
        ("c2", [0.9, 0.1], "kitten"),
    ])
    comp = _component(connection_string=str(tmp_path / "lance"), top_k=2)
    result = _materialize(comp, pd.DataFrame({"embedding": [[1.0, 0.0]]}), raise_on_error=True)
    df = result.asset_value("results")
    assert list(df["id"]) == ["c0", "c2"]
    assert df.iloc[0]["metadata"]["chunk_text"] == "cat"


def test_filter_metadata_uses_the_same_json_contract_as_s3vectors(tmp_path):
    _write_table(tmp_path / "lance", "kb", [
        ("c0", [1.0, 0.0], "allowed"),
        ("c1", [1.0, 0.0], "blocked"),
    ])
    import json
    comp = _component(
        connection_string=str(tmp_path / "lance"), top_k=5,
        filter_metadata=json.dumps({"chunk_text": "allowed"}),
    )
    result = _materialize(comp, pd.DataFrame({"embedding": [[1.0, 0.0]]}), raise_on_error=True)
    df = result.asset_value("results")
    assert list(df["id"]) == ["c0"]


def test_include_distances(tmp_path):
    _write_table(tmp_path / "lance", "kb", [("c0", [1.0, 0.0], "cat")])
    comp = _component(connection_string=str(tmp_path / "lance"), top_k=1, include_distances=True)
    result = _materialize(comp, pd.DataFrame({"embedding": [[1.0, 0.0]]}), raise_on_error=True)
    df = result.asset_value("results")
    assert "distance" in df.columns
    assert df.iloc[0]["distance"] == pytest.approx(0.0, abs=1e-6)


def test_track_retrieval_count_increments_via_real_lancedb_update(tmp_path):
    _write_table(tmp_path / "lance", "kb", [("c0", [1.0, 0.0], "cat")])
    comp = _component(connection_string=str(tmp_path / "lance"), top_k=1, track_retrieval_count=True)
    _materialize(comp, pd.DataFrame({"embedding": [[1.0, 0.0]]}), raise_on_error=True)
    _materialize(comp, pd.DataFrame({"embedding": [[1.0, 0.0]]}), raise_on_error=True)

    db = lancedb.connect(str(tmp_path / "lance"))
    tbl = db.open_table("kb")
    rows = tbl.to_pandas()
    assert rows.iloc[0]["retrieval_count"] == 2
