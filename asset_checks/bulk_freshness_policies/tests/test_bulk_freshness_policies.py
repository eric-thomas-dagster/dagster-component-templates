import importlib.util
import pathlib

import dagster as dg
import pytest


def _load_component_module():
    here = pathlib.Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("bulk_freshness_policies_component", here / "component.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


mod = _load_component_module()
BulkFreshnessPoliciesComponent = mod.BulkFreshnessPoliciesComponent


def test_explicit_list_selection_produces_one_check_per_asset():
    comp = BulkFreshnessPoliciesComponent(
        groups=[
            {"name": "hourly", "policy_type": "time_window", "fail_window_hours": 2,
             "selection": ["sap/bseg", "sap/konv"]},
            {"name": "weekly", "policy_type": "cron", "deadline_cron": "0 6 * * 1",
             "lower_bound_delta_hours": 24, "selection": ["sap/company_codes"]},
        ]
    )
    defs = comp.build_defs(context=None)
    check_asset_keys = {k.asset_key for checks_def in defs.asset_checks for k in checks_def.check_keys}
    assert check_asset_keys == {
        dg.AssetKey(["sap", "bseg"]),
        dg.AssetKey(["sap", "konv"]),
        dg.AssetKey(["sap", "company_codes"]),
    }


def test_selection_dsl_resolves_against_real_sibling_assets():
    """Exercises the real AssetSelection.from_string() path (tag: / group:),
    not just the explicit-list fast path -- against a real dg.Definitions
    object, the same resolution `_resolve_selection` uses in build_defs()."""

    @dg.asset(group_name="sap_landing", tags={"cadence": "hourly"})
    def bseg():
        ...

    @dg.asset(group_name="sap_landing", tags={"cadence": "hourly"})
    def konv():
        ...

    @dg.asset(group_name="sap_rare")
    def company_codes():
        ...

    sibling_defs = dg.Definitions(assets=[bseg, konv, company_codes])
    discovered_keys = [
        key.to_user_string()
        for assets_def in sibling_defs.assets
        for key in assets_def.keys
    ]

    hourly_match = mod._resolve_selection("tag:cadence=hourly", discovered_keys, sibling_defs)
    assert set(hourly_match) == {"bseg", "konv"}

    group_match = mod._resolve_selection("group:sap_rare", discovered_keys, sibling_defs)
    assert set(group_match) == {"company_codes"}

    everything = mod._resolve_selection("*", discovered_keys, sibling_defs)
    assert set(everything) == {"bseg", "konv", "company_codes"}


def test_selection_with_no_sibling_assets_discovered_returns_empty():
    assert mod._resolve_selection("tag:cadence=hourly", [], None) == []


def test_duplicate_asset_key_across_groups_rejected():
    with pytest.raises(ValueError, match="matched by both group"):
        BulkFreshnessPoliciesComponent(
            groups=[
                {"name": "a", "policy_type": "time_window", "fail_window_hours": 2, "selection": ["t/x"]},
                {"name": "b", "policy_type": "time_window", "fail_window_hours": 4, "selection": ["t/x"]},
            ]
        ).build_defs(context=None)


def test_time_window_requires_fail_window_hours():
    with pytest.raises(ValueError, match="requires fail_window_hours"):
        BulkFreshnessPoliciesComponent(
            groups=[{"name": "a", "policy_type": "time_window", "selection": ["t/x"]}]
        )


def test_cron_requires_deadline_and_lower_bound():
    with pytest.raises(ValueError, match="requires deadline_cron"):
        BulkFreshnessPoliciesComponent(
            groups=[{"name": "a", "policy_type": "cron", "selection": ["t/x"]}]
        )


def test_empty_groups_rejected():
    with pytest.raises(ValueError, match="non-empty"):
        BulkFreshnessPoliciesComponent(groups=[])


def test_missing_selection_rejected():
    with pytest.raises(ValueError, match="'selection' is required"):
        BulkFreshnessPoliciesComponent(
            groups=[{"name": "a", "policy_type": "time_window", "fail_window_hours": 2}]
        )


def test_selection_matching_nothing_raises_clear_error():
    comp = BulkFreshnessPoliciesComponent(
        groups=[{"name": "a", "policy_type": "time_window", "fail_window_hours": 2, "selection": "tag:nope=nope"}]
    )
    with pytest.raises(ValueError, match="matched no assets"):
        comp.build_defs(context=None)
