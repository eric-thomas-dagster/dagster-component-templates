"""write_vector_store op providers 's3vectors' and 'lancedb'.

lancedb is embedded/local (like chromadb) -- exercised for real, including merge_insert
upsert and sync_mode='replace_sources' orphan deletion via a real SQL predicate. s3vectors
needs a real AWS account, so `boto3.client` is mocked (matching how this repo's standalone
vector_store_writer/vector_store_query test s3vectors' real-mode path) -- only the local
`_DemoS3VectorsClient`-equivalent behavior here is a bare MagicMock, not a fake local file,
since CEP's write_vector_store op has no demo_mode concept for any provider (chromadb is the
only one exercised live; pinecone/qdrant are documented as untested against live services).
"""
import hashlib
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from .conftest import load_component_module, requires_lancedb


@pytest.fixture()
def mod():
    return load_component_module()


class Ctx:
    class log:
        @staticmethod
        def info(*a, **k): ...
        warning = info


def _df(ids, texts, embeddings, source_ids=None):
    return pd.DataFrame({
        "chunk_id": ids,
        "chunk_text": texts,
        "embedding": embeddings,
        "source_id": source_ids or ids,
        "content_hash": [hashlib.sha256(t.encode()).hexdigest() for t in texts],
    })


# ── lancedb: real, end-to-end ────────────────────────────────────────────────

@requires_lancedb
def test_lancedb_writes_a_searchable_table(mod, tmp_path):
    import lancedb

    df = _df(["c1", "c2"], ["alpha text", "beta text"], [[1.0, 0.0], [0.0, 1.0]])
    step = {"op": "write_vector_store", "provider": "lancedb",
            "connection_string": str(tmp_path / "lance"), "collection_name": "kb"}
    out = mod._do_write_vector_store(df, step, "source_id", Ctx)
    assert out.attrs["stats"]["upserted"] == 2

    db = lancedb.connect(str(tmp_path / "lance"))
    tbl = db.open_table("kb")
    hits = tbl.search([1.0, 0.0]).limit(1).to_pandas()
    assert hits.iloc[0]["chunk_id"] == "c1"
    assert hits.iloc[0]["chunk_text"] == "alpha text"


@requires_lancedb
def test_lancedb_upsert_updates_existing_rows_not_duplicates(mod, tmp_path):
    import lancedb

    step = {"op": "write_vector_store", "provider": "lancedb",
            "connection_string": str(tmp_path / "lance"), "collection_name": "kb", "sync_mode": "upsert"}
    mod._do_write_vector_store(_df(["c1"], ["original"], [[1.0, 0.0]]), step, "source_id", Ctx)
    mod._do_write_vector_store(_df(["c1"], ["updated"], [[1.0, 0.0]]), step, "source_id", Ctx)

    db = lancedb.connect(str(tmp_path / "lance"))
    tbl = db.open_table("kb")
    rows = tbl.to_pandas()
    assert len(rows) == 1
    assert rows.iloc[0]["chunk_text"] == "updated"


@requires_lancedb
def test_lancedb_replace_sources_deletes_orphaned_chunks(mod, tmp_path):
    import lancedb

    step = {"op": "write_vector_store", "provider": "lancedb",
            "connection_string": str(tmp_path / "lance"), "collection_name": "kb"}
    # First run: source doc "d1" produces 2 chunks.
    mod._do_write_vector_store(
        _df(["d1_0", "d1_1"], ["a", "b"], [[1.0, 0.0], [0.0, 1.0]], source_ids=["d1", "d1"]),
        step, "source_id", Ctx,
    )
    # Second run: "d1" shrank to 1 chunk -- d1_1 is now an orphan and must be deleted.
    out = mod._do_write_vector_store(
        _df(["d1_0"], ["a"], [[1.0, 0.0]], source_ids=["d1"]),
        step, "source_id", Ctx,
    )
    assert out.attrs["stats"]["orphans_deleted"] == 1

    db = lancedb.connect(str(tmp_path / "lance"))
    tbl = db.open_table("kb")
    assert set(tbl.to_pandas()["chunk_id"]) == {"d1_0"}


# ── s3vectors: boto3 mocked (needs a real AWS account otherwise) ────────────

def _fake_s3vectors_client():
    client = MagicMock()

    class _Conflict(Exception):
        pass

    client.exceptions.ConflictException = _Conflict
    return client


def test_s3vectors_requires_bucket_and_region(mod):
    df = _df(["c1"], ["a"], [[1.0, 0.0]])
    step = {"op": "write_vector_store", "provider": "s3vectors", "collection_name": "idx"}
    with pytest.raises(ValueError, match="vector_bucket_name.*aws_region"):
        mod._do_write_vector_store(df, step, "source_id", Ctx)


def test_s3vectors_creates_bucket_index_and_puts_vectors(mod):
    df = _df(["c1", "c2"], ["alpha", "beta"], [[1.0, 0.0], [0.0, 1.0]])
    step = {"op": "write_vector_store", "provider": "s3vectors", "collection_name": "idx",
            "vector_bucket_name": "buck", "aws_region": "us-east-2"}
    client = _fake_s3vectors_client()
    with patch("boto3.client", return_value=client) as mock_boto:
        mod._do_write_vector_store(df, step, "source_id", Ctx)

    mock_boto.assert_called_once_with("s3vectors", region_name="us-east-2")
    client.create_vector_bucket.assert_called_once_with(vectorBucketName="buck")
    client.create_index.assert_called_once()
    assert client.create_index.call_args.kwargs["dimension"] == 2
    assert client.create_index.call_args.kwargs["distanceMetric"] == "cosine"
    client.put_vectors.assert_called_once()
    put_kwargs = client.put_vectors.call_args.kwargs
    assert put_kwargs["vectorBucketName"] == "buck"
    assert put_kwargs["indexName"] == "idx"
    assert {v["key"] for v in put_kwargs["vectors"]} == {"c1", "c2"}


def test_s3vectors_tolerates_already_existing_bucket_and_index(mod):
    df = _df(["c1"], ["alpha"], [[1.0, 0.0]])
    step = {"op": "write_vector_store", "provider": "s3vectors", "collection_name": "idx",
            "vector_bucket_name": "buck", "aws_region": "us-east-2"}
    client = _fake_s3vectors_client()
    client.create_vector_bucket.side_effect = client.exceptions.ConflictException("exists")
    client.create_index.side_effect = client.exceptions.ConflictException("exists")
    with patch("boto3.client", return_value=client):
        out = mod._do_write_vector_store(df, step, "source_id", Ctx)
    assert out.attrs["stats"]["upserted"] == 1


def test_s3vectors_put_vectors_batches_at_500(mod):
    n = 1200
    df = _df([f"c{i}" for i in range(n)], [str(i) for i in range(n)], [[float(i), 0.0] for i in range(n)])
    step = {"op": "write_vector_store", "provider": "s3vectors", "collection_name": "idx",
            "vector_bucket_name": "buck", "aws_region": "us-east-2", "batch_size": 1000}
    client = _fake_s3vectors_client()
    with patch("boto3.client", return_value=client):
        mod._do_write_vector_store(df, step, "source_id", Ctx)
    assert client.put_vectors.call_count == 3  # 500 + 500 + 200, regardless of batch_size
    for call in client.put_vectors.call_args_list:
        assert len(call.kwargs["vectors"]) <= 500


def test_s3vectors_non_upsert_sync_mode_warns_and_behaves_as_upsert(mod):
    df = _df(["c1"], ["alpha"], [[1.0, 0.0]])
    step = {"op": "write_vector_store", "provider": "s3vectors", "collection_name": "idx",
            "vector_bucket_name": "buck", "aws_region": "us-east-2", "sync_mode": "replace_sources"}
    warnings = []
    ctx = Ctx
    ctx.log.warning = staticmethod(lambda msg: warnings.append(msg))
    client = _fake_s3vectors_client()
    with patch("boto3.client", return_value=client):
        mod._do_write_vector_store(df, step, "source_id", ctx)
    assert any("s3vectors" in w and "upsert" in w for w in warnings)
