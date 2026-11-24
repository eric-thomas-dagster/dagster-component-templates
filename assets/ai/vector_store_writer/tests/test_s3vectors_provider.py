"""provider='s3vectors': real end-to-end tests of the demo-mode seam,
real-mode boto3 client construction, and the PutVectors 500-vector batch cap.

demo_mode (the default) exercises the real asset-building/execution code with
the component's own internal `_DemoS3VectorsClient` -- a local JSON file, no
AWS calls. Real mode is exercised by monkeypatching the module-level
`_build_s3vectors_client` factory (this component's documented seam).
`dg.materialize()` is real throughout. The other 4 providers (chromadb,
pinecone, qdrant, faiss) are untouched by this change and are not re-tested
here.
"""
import json

import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module

mod = load_component_module()
VectorStoreWriterComponent = mod.VectorStoreWriterComponent


def _component(**kw):
    attrs = dict(
        asset_name="vecs", provider="s3vectors", collection_name="idx",
        vector_bucket_name="buck", aws_region="us-east-2", upstream_asset_key="embeddings",
    )
    attrs.update(kw)
    return VectorStoreWriterComponent(**attrs)


def _materialize(comp, df, **kwargs):
    @dg.asset(name="embeddings")
    def embeddings():
        return df

    defs = comp.build_defs(None)
    return dg.materialize([embeddings, *defs.assets], **kwargs)


def test_demo_mode_writes_local_json_file(tmp_path):
    demo_path = tmp_path / "demo.json"
    comp = _component(connection_string=str(demo_path))
    df = pd.DataFrame({"embedding": [[0.1, 0.2, 0.3], [0.4, 0.5, 0.6]], "text": ["a", "b"]})
    result = _materialize(comp, df, raise_on_error=True)
    assert result.success
    data = json.loads(demo_path.read_text())
    assert data["indexes"]["idx"]["dimension"] == 3
    assert data["indexes"]["idx"]["distanceMetric"] == "cosine"
    assert set(data["indexes"]["idx"]["vectors"].keys()) == {"doc_0", "doc_1"}
    assert data["indexes"]["idx"]["vectors"]["doc_0"]["data"]["float32"] == [0.1, 0.2, 0.3]
    assert data["indexes"]["idx"]["vectors"]["doc_0"]["metadata"] == {"text": "a"}


def test_metadata_columns_included(tmp_path):
    path = str(tmp_path / "demo.json")
    comp = _component(connection_string=path, metadata_columns=["species"])
    df = pd.DataFrame({"embedding": [[1.0, 0.0]], "text": ["dog"], "species": ["canine"]})
    _materialize(comp, df, raise_on_error=True)
    data = json.loads(open(path).read())
    assert data["indexes"]["idx"]["vectors"]["doc_0"]["metadata"] == {"text": "dog", "species": "canine"}


def test_put_vectors_batches_at_500(tmp_path):
    demo_path = tmp_path / "demo.json"
    # batch_size is configured above the real 500/call AWS limit -- the component
    # must still cap each put_vectors call at 500, regardless of batch_size.
    comp = _component(connection_string=str(demo_path), batch_size=1000)
    df = pd.DataFrame({"embedding": [[float(i), 0.0] for i in range(1200)], "text": [str(i) for i in range(1200)]})

    calls = {"put": 0}
    real_put = mod._DemoS3VectorsClient.put_vectors

    def counting_put(self, **kw):
        calls["put"] += 1
        assert len(kw["vectors"]) <= 500
        return real_put(self, **kw)

    mod._DemoS3VectorsClient.put_vectors = counting_put
    try:
        _materialize(comp, df, raise_on_error=True)
    finally:
        mod._DemoS3VectorsClient.put_vectors = real_put
    assert calls["put"] == 3  # 1200 / 500 -> 3 batches (500, 500, 200)


def test_real_mode_seam_uses_configured_region_and_builds_index():
    calls = {}

    class FakeClient:
        def create_vector_bucket(self, **kw):
            calls["bucket"] = kw

        def create_index(self, **kw):
            calls["index"] = kw

        def put_vectors(self, **kw):
            calls.setdefault("put", []).append(kw)

    mod._build_s3vectors_client = lambda region: (calls.setdefault("region", region), FakeClient())[1]
    comp = _component(demo_mode=False, aws_region="eu-west-1")
    df = pd.DataFrame({"embedding": [[0.5, 0.5]], "text": ["x"]})
    _materialize(comp, df, raise_on_error=True)
    assert calls["region"] == "eu-west-1"
    assert calls["bucket"] == {"vectorBucketName": "buck"}
    # text_column is non-filterable by default (chunk text commonly exceeds the
    # 2 KB filterable-metadata limit -- item 18).
    assert calls["index"] == {
        "vectorBucketName": "buck", "indexName": "idx", "dataType": "float32",
        "dimension": 2, "distanceMetric": "cosine",
        "metadataConfiguration": {"nonFilterableMetadataKeys": ["text"]},
    }
    assert calls["put"][0]["vectors"][0]["key"] == "doc_0"


def test_create_calls_are_idempotent_against_a_conflict(tmp_path):
    # A real account returns ConflictException on the second write, or when the
    # bucket/index was created by hand -- this must not fail the run (item 17).
    class ConflictException(Exception):
        pass

    class FakeClientRaisingConflict:
        exceptions = type("exceptions", (), {"ConflictException": ConflictException})()

        def create_vector_bucket(self, **kw):
            raise ConflictException("bucket already exists")

        def create_index(self, **kw):
            raise ConflictException("index already exists")

        def put_vectors(self, **kw):
            pass

    mod._build_s3vectors_client = lambda region: FakeClientRaisingConflict()
    comp = _component(demo_mode=False)
    df = pd.DataFrame({"embedding": [[0.5, 0.5]], "text": ["x"]})
    result = _materialize(comp, df, raise_on_error=True)
    assert result.success


def test_create_calls_do_not_swallow_other_errors():
    class FakeClientRaisingAccessDenied:
        def create_vector_bucket(self, **kw):
            raise Exception("AccessDeniedException: not authorized")

    mod._build_s3vectors_client = lambda region: FakeClientRaisingAccessDenied()
    comp = _component(demo_mode=False)
    df = pd.DataFrame({"embedding": [[0.5, 0.5]], "text": ["x"]})
    result = _materialize(comp, df, raise_on_error=False)
    assert not result.success


def test_manage_bucket_and_index_false_skips_both_create_calls():
    calls = {"create_bucket": 0, "create_index": 0}

    class FakeClient:
        def create_vector_bucket(self, **kw):
            calls["create_bucket"] += 1

        def create_index(self, **kw):
            calls["create_index"] += 1

        def put_vectors(self, **kw):
            calls.setdefault("put", []).append(kw)

    mod._build_s3vectors_client = lambda region: FakeClient()
    comp = _component(demo_mode=False, manage_bucket_and_index=False)
    df = pd.DataFrame({"embedding": [[0.5, 0.5]], "text": ["x"]})
    _materialize(comp, df, raise_on_error=True)
    assert calls["create_bucket"] == 0
    assert calls["create_index"] == 0
    assert len(calls["put"]) == 1


def test_nonfilterable_metadata_keys_can_be_overridden():
    calls = {}

    class FakeClient:
        def create_vector_bucket(self, **kw):
            pass

        def create_index(self, **kw):
            calls["index"] = kw

        def put_vectors(self, **kw):
            pass

    mod._build_s3vectors_client = lambda region: FakeClient()
    comp = _component(demo_mode=False, nonfilterable_metadata_keys=["species"], metadata_columns=["species"])
    df = pd.DataFrame({"embedding": [[0.5, 0.5]], "text": ["x"], "species": ["canine"]})
    _materialize(comp, df, raise_on_error=True)
    assert calls["index"]["metadataConfiguration"] == {"nonFilterableMetadataKeys": ["species"]}


def test_nonfilterable_metadata_keys_empty_list_opts_out():
    calls = {}

    class FakeClient:
        def create_vector_bucket(self, **kw):
            pass

        def create_index(self, **kw):
            calls["index"] = kw

        def put_vectors(self, **kw):
            pass

    mod._build_s3vectors_client = lambda region: FakeClient()
    comp = _component(demo_mode=False, nonfilterable_metadata_keys=[])
    df = pd.DataFrame({"embedding": [[0.5, 0.5]], "text": ["x"]})
    _materialize(comp, df, raise_on_error=True)
    assert "metadataConfiguration" not in calls["index"]


def test_missing_vector_bucket_name_raises():
    with pytest.raises(Exception, match="requires both"):
        VectorStoreWriterComponent(
            asset_name="vecs", provider="s3vectors", collection_name="idx",
            aws_region="us-east-2", upstream_asset_key="embeddings",
        )


def test_missing_aws_region_raises():
    with pytest.raises(Exception, match="requires both"):
        VectorStoreWriterComponent(
            asset_name="vecs", provider="s3vectors", collection_name="idx",
            vector_bucket_name="buck", upstream_asset_key="embeddings",
        )


def test_other_providers_unaffected_by_new_fields():
    # faiss has no vector_bucket_name/aws_region/demo_mode requirement -- the
    # validator only fires for provider == "s3vectors".
    VectorStoreWriterComponent(
        asset_name="vecs", provider="faiss", collection_name="idx", upstream_asset_key="embeddings",
    )
