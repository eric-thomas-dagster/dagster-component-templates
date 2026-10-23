"""Opt-in for insights_custom_metrics_sync `presets: auto`: every asset this component defines carries insights/metrics."""
import pytest

from .conftest import load_component_module

TAG, PRESET = "insights/metrics", "context_engineering_pipeline"
CHUNKS = [{"id": "chunks", "op": "chunk", "chunk_size": 200}, {"id": "again", "op": "chunk", "chunk_size": 100}]
SQL = dict(source={"kind": "warehouse_query", "resource_key": "bq", "sql": "SELECT id, body FROM ds.src"}, id_column="id", text_column="body",
           execution_mode="sql", sql_dialect="bigquery", output_table="ds.kb", code_version="v1", steps=[CHUNKS[0]])


@pytest.fixture()
def mod():
    return load_component_module()


def _tags(defs):
    return [dict(a.tags_by_key[k]) for a in defs.assets for k in a.keys]


def _comp(mod, **kw):
    base = dict(asset_name="kb", id_column="id", text_column="body", steps=CHUNKS)
    if "source" not in kw:
        base["upstream_asset_key"] = "raw"
    base.update(kw)
    return mod.ContextEngineeringPipelineComponent(**base)


@pytest.mark.parametrize("kw", [{}, {"per_step_ops": True}, SQL, {**SQL, "per_step_ops": True}], ids=["python", "python_per_step", "sql", "sql_per_step"])
def test_every_asset_carries_the_preset_tag(mod, kw):
    tags = _tags(_comp(mod, **kw).build_defs(None))
    assert tags and all(t.get(TAG) == PRESET for t in tags), tags
    assert all("dagster/kind/ai" in t for t in tags)  # kind tags unchanged


def test_per_step_mode_really_has_several_assets(mod):
    assert len(_tags(_comp(mod, per_step_ops=True).build_defs(None))) == 2


def test_user_tag_of_same_key_is_not_overwritten(mod):
    tags = _tags(_comp(mod, asset_tags={TAG: "agentic_pipeline", "team": "x"}).build_defs(None))
    assert tags[0][TAG] == "agentic_pipeline" and tags[0]["team"] == "x"
