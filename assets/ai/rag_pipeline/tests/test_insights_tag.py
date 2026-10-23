"""Opt-in for insights_custom_metrics_sync `presets: auto`: the asset carries insights/metrics."""
from .conftest import load_component_module

TAG, PRESET = "insights/metrics", "rag_pipeline"


def _build(**kw):
    mod = load_component_module()
    comp = mod.RAGPipelineComponent(
        asset_name="answers", upstream_asset_key="questions", retrievers={"kb": {"kind": "chromadb", "path": "/nonexistent", "collection_name": "c"}},
        steps=[{"id": "found", "op": "retrieve", "retriever": "kb", "top_k": 2}], **kw)
    return [dict(a.tags_by_key[k]) for a in comp.build_defs(None).assets for k in a.keys]


def test_asset_carries_the_preset_tag():
    tags = _build()
    assert tags and all(t[TAG] == PRESET for t in tags) and all("dagster/kind/rag" in t for t in tags)


def test_user_tag_of_same_key_is_not_overwritten():
    assert _build(asset_tags={TAG: "other"})[0][TAG] == "other"
