"""Real tests for ExternalAssetCatalogComponent -- no mocking needed, since
this component makes no external calls and holds no credentials; it only
ever builds plain AssetSpec objects from the YAML config given to it.
"""
import importlib.util
import pathlib

import dagster as dg

_HERE = pathlib.Path(__file__).resolve().parent.parent
_COMPONENT_PY = _HERE / "component.py"
_spec = importlib.util.spec_from_file_location("external_asset_catalog_component", _COMPONENT_PY)
assert _spec is not None and _spec.loader is not None
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

ExternalAssetCatalogComponent = _mod.ExternalAssetCatalogComponent


def _component(**overrides):
    attrs = dict(assets=[{"key": "finance/gl_balances"}])
    attrs.update(overrides)
    return ExternalAssetCatalogComponent(**attrs)


def test_single_entry_with_only_required_fields_gets_sensible_defaults():
    comp = _component(assets=[{"key": "finance/gl_balances"}])
    defs = comp.build_defs(None)
    specs = list(defs.assets)
    assert len(specs) == 1
    spec = specs[0]
    assert spec.key == dg.AssetKey.from_user_string("finance/gl_balances")
    assert list(spec.deps) == []
    assert spec.owners == []
    assert spec.kinds == set()
    assert spec.group_name is None
    assert spec.description is None


def test_string_key_with_slashes_becomes_a_multi_segment_asset_key():
    comp = _component(assets=[{"key": "finance/gl_balances"}])
    spec = next(iter(comp.build_defs(None).assets))
    assert spec.key == dg.AssetKey(["finance", "gl_balances"])


def test_list_key_becomes_a_multi_segment_asset_key():
    comp = _component(assets=[{"key": ["ops", "shipments"]}])
    spec = next(iter(comp.build_defs(None).assets))
    assert spec.key == dg.AssetKey(["ops", "shipments"])


def test_deps_accepts_a_list():
    comp = _component(assets=[{"key": "a/b", "deps": ["x/y", "x/z"]}])
    spec = next(iter(comp.build_defs(None).assets))
    dep_keys = {d.asset_key for d in spec.deps}
    assert dg.AssetKey.from_user_string("x/y") in dep_keys
    assert dg.AssetKey.from_user_string("x/z") in dep_keys


def test_deps_accepts_a_comma_separated_string():
    comp = _component(assets=[{"key": "a/b", "deps": "x/y, x/z"}])
    spec = next(iter(comp.build_defs(None).assets))
    dep_keys = {d.asset_key for d in spec.deps}
    assert dg.AssetKey.from_user_string("x/y") in dep_keys
    assert dg.AssetKey.from_user_string("x/z") in dep_keys


def test_owners_and_kinds_accept_comma_separated_strings():
    comp = _component(assets=[{
        "key": "finance/gl_balances",
        "owners": "team:finance-eng, jane@company.com",
        "kinds": "snowflake, sql",
    }])
    spec = next(iter(comp.build_defs(None).assets))
    assert spec.owners == ["team:finance-eng", "jane@company.com"]
    assert spec.kinds == {"snowflake", "sql"}


def test_tags_accepts_a_dict():
    comp = _component(assets=[{"key": "a/b", "tags": {"pii": "false"}}])
    spec = next(iter(comp.build_defs(None).assets))
    assert spec.tags.get("pii") == "false"


def test_tags_accepts_a_list_of_key_equals_value_strings():
    comp = _component(assets=[{"key": "a/b", "tags": ["pii=false", "tier=gold"]}])
    spec = next(iter(comp.build_defs(None).assets))
    assert spec.tags.get("pii") == "false"
    assert spec.tags.get("tier") == "gold"


def test_description_group_name_and_metadata_reach_the_spec():
    comp = _component(assets=[{
        "key": "finance/gl_balances",
        "description": "General ledger balances.",
        "group_name": "finance",
        "metadata": {"source_url": "https://internal.example.com/gl"},
    }])
    spec = next(iter(comp.build_defs(None).assets))
    assert spec.description == "General ledger balances."
    assert spec.group_name == "finance"
    assert spec.metadata.get("source_url") == "https://internal.example.com/gl"


def test_multiple_entries_with_different_owners_and_kinds_all_appear():
    comp = _component(assets=[
        {"key": "finance/gl_balances", "owners": ["team:finance-eng"], "kinds": ["snowflake"]},
        {"key": "marketing/campaign_performance", "deps": ["finance/gl_balances"], "owners": ["team:marketing-data"], "kinds": ["dbt", "snowflake"], "group_name": "marketing"},
        {"key": "ops/shipments", "owners": ["team:ops"], "kinds": ["python"]},
    ])
    specs = {s.key: s for s in comp.build_defs(None).assets}
    assert len(specs) == 3
    assert specs[dg.AssetKey.from_user_string("finance/gl_balances")].owners == ["team:finance-eng"]
    assert specs[dg.AssetKey.from_user_string("marketing/campaign_performance")].group_name == "marketing"
    marketing_deps = {d.asset_key for d in specs[dg.AssetKey.from_user_string("marketing/campaign_performance")].deps}
    assert dg.AssetKey.from_user_string("finance/gl_balances") in marketing_deps
    assert specs[dg.AssetKey.from_user_string("ops/shipments")].kinds == {"python"}


def test_declared_assets_are_non_executable_specs_not_materializable_assets():
    """The whole point: these are declare-only AssetSpecs, not AssetsDefinitions
    with a compute function -- there's nothing here Dagster could try to run."""
    comp = _component(assets=[{"key": "finance/gl_balances"}])
    defs = comp.build_defs(None)
    for spec in defs.assets:
        assert isinstance(spec, dg.AssetSpec)
        assert not isinstance(spec, dg.AssetsDefinition)
