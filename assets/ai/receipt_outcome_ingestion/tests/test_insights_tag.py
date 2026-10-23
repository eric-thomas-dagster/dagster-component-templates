"""Opt-in for insights_custom_metrics_sync `presets: auto`: the asset carries insights/metrics."""
from .conftest import load_component_module

TAG, PRESET = "insights/metrics", "receipt_outcome_ingestion"


def _tags(**kw):
    mod = load_component_module()
    c = mod.ReceiptOutcomeIngestionComponent(source={"kind": "file", "path": "/nonexistent/*.csv"},
                                             mapping={"receipt_id": "rid", "value": "score"}, **kw)
    return [dict(a.tags_by_key[k]) for a in c.build_defs(None).assets for k in a.keys]


def test_asset_carries_the_preset_tag():
    tags = _tags()
    assert tags and all(t[TAG] == PRESET for t in tags) and all("dagster/kind/feedback" in t for t in tags)


def test_user_tag_of_same_key_is_not_overwritten():
    assert _tags(asset_tags={TAG: "other"})[0][TAG] == "other"
