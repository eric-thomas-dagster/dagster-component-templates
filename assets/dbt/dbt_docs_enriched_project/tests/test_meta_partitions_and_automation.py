"""Tests for `meta.dagster.partitions_def` + `meta.dagster.automation_condition`
reading in DbtDocsEnrichedProjectComponent.

Tests exercise the two pure helper functions (`_partitions_def_from_meta`,
`_automation_condition_from_meta`) directly — no dbt project on disk needed.
"""
import importlib.util
import pathlib

import pytest
import dagster as dg


_HERE = pathlib.Path(__file__).resolve().parent.parent
_COMPONENT_PY = _HERE / "component.py"
_spec = importlib.util.spec_from_file_location("dbt_docs_enriched", _COMPONENT_PY)
assert _spec is not None and _spec.loader is not None
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

_partitions_def_from_meta = _mod._partitions_def_from_meta
_automation_condition_from_meta = _mod._automation_condition_from_meta


# ── partitions_def ───────────────────────────────────────────────────────────


@pytest.mark.parametrize("ptype,expected_cls,start_date", [
    ("daily",   dg.DailyPartitionsDefinition,   "2025-01-01"),
    ("hourly",  dg.HourlyPartitionsDefinition,  "2025-01-01-00:00"),
    ("weekly",  dg.WeeklyPartitionsDefinition,  "2025-01-01"),
    ("monthly", dg.MonthlyPartitionsDefinition, "2025-01-01"),
])
def test_time_partitions_from_meta(ptype, expected_cls, start_date):
    """Model with meta.dagster.partitions_def gets the right PartitionsDefinition."""
    meta = {"type": ptype, "start_date": start_date}
    pdef = _partitions_def_from_meta(meta)
    assert isinstance(pdef, expected_cls), (
        f"expected {expected_cls.__name__} for type={ptype!r}, got {type(pdef).__name__}"
    )


def test_hourly_uses_hour_precision_start():
    pdef = _partitions_def_from_meta({"type": "hourly", "start_date": "2025-01-01-00:00"})
    assert isinstance(pdef, dg.HourlyPartitionsDefinition)


def test_static_partitions_from_meta():
    pdef = _partitions_def_from_meta({"type": "static", "values": ["US", "CA", "MX"]})
    assert isinstance(pdef, dg.StaticPartitionsDefinition)
    assert set(pdef.get_partition_keys()) == {"US", "CA", "MX"}


def test_dynamic_partitions_from_meta():
    pdef = _partitions_def_from_meta({"type": "dynamic", "name": "filenames"})
    assert isinstance(pdef, dg.DynamicPartitionsDefinition)


def test_missing_type_returns_none():
    """No `type` key → no partition, no crash."""
    assert _partitions_def_from_meta({"start_date": "2025-01-01"}) is None


def test_missing_start_date_returns_none():
    """Invalid daily config (no start_date) returns None without raising."""
    assert _partitions_def_from_meta({"type": "daily"}) is None


def test_missing_static_values_returns_none():
    assert _partitions_def_from_meta({"type": "static"}) is None


def test_missing_dynamic_name_returns_none():
    assert _partitions_def_from_meta({"type": "dynamic"}) is None


def test_unknown_type_returns_none():
    assert _partitions_def_from_meta({"type": "quarterly", "start_date": "2025-01-01"}) is None


def test_empty_dict_returns_none():
    assert _partitions_def_from_meta({}) is None


# ── automation_condition ─────────────────────────────────────────────────────


def test_automation_preset_eager():
    cond = _automation_condition_from_meta({"preset": "eager"})
    assert isinstance(cond, dg.AutomationCondition)


def test_automation_preset_on_missing():
    cond = _automation_condition_from_meta({"preset": "on_missing"})
    assert isinstance(cond, dg.AutomationCondition)


def test_automation_preset_on_deploy_if_code_changed():
    """Synthetic composite — matches the applicator's preset vocabulary."""
    cond = _automation_condition_from_meta({"preset": "on_deploy_if_code_changed"})
    assert isinstance(cond, dg.AutomationCondition)
    # Verify it composes code_version_changed (structural check via repr).
    # The class name is CodeVersionChangedCondition — case-insensitive match.
    assert "codeversionchanged" in repr(cond).lower()


def test_automation_cron():
    cond = _automation_condition_from_meta({"cron": "0 9 * * *"})
    assert isinstance(cond, dg.AutomationCondition)


def test_automation_unknown_preset_returns_none():
    """Bogus preset → returns None, doesn't crash."""
    assert _automation_condition_from_meta({"preset": "does_not_exist"}) is None


def test_automation_empty_dict_returns_none():
    assert _automation_condition_from_meta({}) is None


# ── End-to-end: spec enrichment applies partitions + automation ─────────────


def _build_fake_manifest(models: list) -> dict:
    """Minimal manifest.json shape sufficient for _enrich_spec."""
    nodes = {}
    for m in models:
        uid = f"model.demo.{m['name']}"
        nodes[uid] = {
            "resource_type": "model",
            "name": m["name"],
            "meta": m.get("meta", {}),
            "config": {},
            "columns": {},
        }
    return {
        "nodes": nodes,
        "sources": {},
        "snapshots": {},
        "exposures": {},
        "metrics": {},
        "semantic_models": {},
        "docs": {},
        "child_map": {uid: [] for uid in nodes},
    }


def _make_component():
    """Instantiate DbtDocsEnrichedProjectComponent without triggering pydantic
    validation (skip normal __init__ — we only exercise _enrich_spec)."""
    return _mod.DbtDocsEnrichedProjectComponent.__new__(_mod.DbtDocsEnrichedProjectComponent)


def _spec_with_unique_id(uid: str, name: str) -> dg.AssetSpec:
    return dg.AssetSpec(
        key=dg.AssetKey([name]),
        metadata={"dagster_dbt/unique_id": uid},
    )


def test_enrich_spec_applies_daily_partitions():
    """A dbt model with meta.dagster.partitions_def gets partitions_def set."""
    manifest = _build_fake_manifest([
        {"name": "fct_daily", "meta": {"dagster": {"partitions_def": {
            "type": "daily", "start_date": "2025-01-01",
        }}}},
    ])
    c = _make_component()
    # Set the include_* flags to False to keep the enrichment minimal.
    c.dbt_docs_url = None
    c.include_exposures = False
    c.include_metrics = False
    c.include_semantic_models = False
    c.include_contracts = False
    c.include_meta = False
    c.include_source_freshness = False
    c.include_doc_blocks = False

    spec = _spec_with_unique_id("model.demo.fct_daily", "fct_daily")
    enriched = c._enrich_spec(spec, manifest)
    assert isinstance(enriched.partitions_def, dg.DailyPartitionsDefinition)


def test_enrich_spec_applies_automation_condition():
    """A dbt model with meta.dagster.automation_condition gets it set."""
    manifest = _build_fake_manifest([
        {"name": "fct_auto", "meta": {"dagster": {"automation_condition": {
            "preset": "eager",
        }}}},
    ])
    c = _make_component()
    c.dbt_docs_url = None
    c.include_exposures = False
    c.include_metrics = False
    c.include_semantic_models = False
    c.include_contracts = False
    c.include_meta = False
    c.include_source_freshness = False
    c.include_doc_blocks = False

    spec = _spec_with_unique_id("model.demo.fct_auto", "fct_auto")
    enriched = c._enrich_spec(spec, manifest)
    assert enriched.automation_condition is not None
    assert isinstance(enriched.automation_condition, dg.AutomationCondition)


def test_enrich_spec_model_without_meta_stays_unpartitioned():
    """A dbt model without meta.dagster.partitions_def keeps partitions_def=None."""
    manifest = _build_fake_manifest([{"name": "dim_no_partitions", "meta": {}}])
    c = _make_component()
    c.dbt_docs_url = None
    c.include_exposures = False
    c.include_metrics = False
    c.include_semantic_models = False
    c.include_contracts = False
    c.include_meta = False
    c.include_source_freshness = False
    c.include_doc_blocks = False

    spec = _spec_with_unique_id("model.demo.dim_no_partitions", "dim_no_partitions")
    enriched = c._enrich_spec(spec, manifest)
    assert enriched.partitions_def is None
    assert enriched.automation_condition is None
