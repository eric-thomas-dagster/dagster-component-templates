"""Opt-in for insights_custom_metrics_sync `presets: auto`: the asset carries insights/metrics."""
TAG, PRESET = "insights/metrics", "receipts_sync"


def _tags(comp, **kw):
    c = comp.ContextReceiptBigQuerySyncComponent(store_resource_key="receipts", bigquery_resource_key="bigquery", dataset="proj.ctx", **kw)
    return [dict(a.tags_by_key[k]) for a in c.build_defs(None).assets for k in a.keys]


def test_asset_carries_the_preset_tag(comp):
    tags = _tags(comp)
    assert tags and all(t[TAG] == PRESET for t in tags) and all("dagster/kind/bigquery" in t for t in tags)


def test_user_tag_of_same_key_is_not_overwritten(comp):
    assert _tags(comp, asset_tags={TAG: "other"})[0][TAG] == "other"
