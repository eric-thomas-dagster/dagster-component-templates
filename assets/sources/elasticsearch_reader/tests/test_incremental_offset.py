"""Committed tests for ElasticsearchReaderComponent.

No tests/ directory existed for this component before this retrofit, so this file
covers both the baseline single-query path (query/search_text + n_results, unchanged)
and the new offset_field incremental-read retrofit. Real dg.materialize() end to end;
only the external `elasticsearch` client is faked (see conftest.py).
"""
import dagster as dg
import pytest

from .conftest import install_fake_elasticsearch, load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def fake_env(monkeypatch):
    monkeypatch.setenv("ELASTICSEARCH_URL", "http://localhost:9200")


def _component(mod, **kw):
    return mod.ElasticsearchReaderComponent(asset_name="from_es", index_name="products", **kw)


def test_baseline_match_all_unchanged(mod, fake_env, monkeypatch):
    install_fake_elasticsearch(
        monkeypatch,
        hits=[
            {"_id": "1", "_source": {"title": "wireless headphones"}},
            {"_id": "2", "_source": {"title": "wired headphones"}},
        ],
    )
    source = _component(mod).build_defs(None).assets[0]
    result = dg.materialize([source])
    assert result.success
    mats = result.asset_materializations_for_node("from_es")
    assert mats[0].metadata["dagster/row_count"].value == 2
    assert "dagster_community_components/last_offset" not in mats[0].metadata


def test_baseline_search_text_unchanged(mod, fake_env, monkeypatch):
    install_fake_elasticsearch(
        monkeypatch,
        hits=[
            {"_id": "1", "_source": {"title": "wireless headphones"}},
            {"_id": "2", "_source": {"title": "wired mouse"}},
        ],
    )
    source = _component(mod, search_text="headphones", search_fields=["title"]).build_defs(None).assets[0]
    result = dg.materialize([source])
    assert result.success
    mats = result.asset_materializations_for_node("from_es")
    assert mats[0].metadata["dagster/row_count"].value == 1


def test_offset_field_paginates_and_stores_high_water_mark(mod, fake_env, monkeypatch):
    fake = install_fake_elasticsearch(
        monkeypatch,
        hits=[{"_id": str(i), "_source": {"updated_at": i}} for i in range(1, 6)],
    )
    source = _component(
        mod, offset_field="updated_at", initial_offset="0", page_size=2
    ).build_defs(None).assets[0]

    instance = dg.DagsterInstance.ephemeral()
    result = dg.materialize([source], instance=instance)
    assert result.success
    mats = result.asset_materializations_for_node("from_es")
    assert mats[0].metadata["dagster/row_count"].value == 5
    assert mats[0].metadata["dagster_community_components/last_offset"].value == "5"
    # page_size=2 over 5 docs: 2 full pages + 1 short page to detect exhaustion
    assert len(fake.instances[0].search_calls) == 3
    assert all(c["size"] == 2 for c in fake.instances[0].search_calls)


def test_offset_field_resumes_from_stored_offset_on_next_run(mod, fake_env, monkeypatch):
    fake = install_fake_elasticsearch(
        monkeypatch,
        hits=[
            {"_id": "1", "_source": {"updated_at": 1}},
            {"_id": "2", "_source": {"updated_at": 2}},
        ],
    )
    source = _component(
        mod, offset_field="updated_at", initial_offset="0", page_size=10
    ).build_defs(None).assets[0]
    instance = dg.DagsterInstance.ephemeral()

    result1 = dg.materialize([source], instance=instance)
    assert result1.success
    mats1 = result1.asset_materializations_for_node("from_es")
    assert mats1[0].metadata["dagster/row_count"].value == 2
    assert mats1[0].metadata["dagster_community_components/last_offset"].value == "2"

    # Simulate a new document having been indexed since the first run: the component
    # reconnects on every run, so update what the NEXT client will be preseeded with.
    fake.preseed = [
        {"_id": "1", "_source": {"updated_at": 1}},
        {"_id": "2", "_source": {"updated_at": 2}},
        {"_id": "3", "_source": {"updated_at": 3}},
    ]

    result2 = dg.materialize([source], instance=instance)
    assert result2.success
    mats2 = result2.asset_materializations_for_node("from_es")
    assert mats2[0].metadata["dagster/row_count"].value == 1
    assert mats2[0].metadata["dagster_community_components/last_offset"].value == "3"


def test_offset_field_with_no_new_rows_reemits_same_offset(mod, fake_env, monkeypatch):
    install_fake_elasticsearch(monkeypatch, hits=[{"_id": "1", "_source": {"updated_at": 1}}])
    source = _component(
        mod, offset_field="updated_at", initial_offset="0", page_size=10
    ).build_defs(None).assets[0]
    instance = dg.DagsterInstance.ephemeral()
    result1 = dg.materialize([source], instance=instance)
    assert result1.success

    result2 = dg.materialize([source], instance=instance)
    assert result2.success
    mats2 = result2.asset_materializations_for_node("from_es")
    assert mats2[0].metadata["dagster/row_count"].value == 0
    assert mats2[0].metadata["dagster_community_components/last_offset"].value == "1"


def test_offset_field_and_partition_type_mutually_exclusive(mod, fake_env, monkeypatch):
    install_fake_elasticsearch(monkeypatch, hits=[])
    comp = _component(
        mod,
        offset_field="updated_at",
        partition_type="daily",
        partition_start="2024-01-01",
    )
    with pytest.raises(ValueError, match="mutually exclusive"):
        comp.build_defs(None)
