import importlib.util
import pathlib

import dagster as dg
import pytest


def _load_component_module():
    here = pathlib.Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("table_freshness_workspace_component", here / "component.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


mod = _load_component_module()
TableFreshnessWorkspaceComponent = mod.TableFreshnessWorkspaceComponent


def test_groups_produce_one_check_per_asset_across_all_groups():
    comp = TableFreshnessWorkspaceComponent(
        groups=[
            {"name": "hourly", "policy_type": "time_window", "fail_window_hours": 2,
             "asset_keys": ["sap/bseg", "sap/konv"]},
            {"name": "weekly", "policy_type": "cron", "deadline_cron": "0 6 * * 1",
             "lower_bound_delta_hours": 24, "asset_keys": ["sap/company_codes"]},
        ]
    )
    defs = comp.build_defs(context=None)
    check_asset_keys = {k.asset_key for checks_def in defs.asset_checks for k in checks_def.check_keys}
    assert check_asset_keys == {
        dg.AssetKey(["sap", "bseg"]),
        dg.AssetKey(["sap", "konv"]),
        dg.AssetKey(["sap", "company_codes"]),
    }


def test_duplicate_asset_key_across_groups_rejected():
    with pytest.raises(ValueError, match="appears in both group"):
        TableFreshnessWorkspaceComponent(
            groups=[
                {"name": "a", "policy_type": "time_window", "fail_window_hours": 2, "asset_keys": ["t/x"]},
                {"name": "b", "policy_type": "time_window", "fail_window_hours": 4, "asset_keys": ["t/x"]},
            ]
        )


def test_time_window_requires_fail_window_hours():
    with pytest.raises(ValueError, match="requires fail_window_hours"):
        TableFreshnessWorkspaceComponent(
            groups=[{"name": "a", "policy_type": "time_window", "asset_keys": ["t/x"]}]
        )


def test_cron_requires_deadline_and_lower_bound():
    with pytest.raises(ValueError, match="requires deadline_cron"):
        TableFreshnessWorkspaceComponent(
            groups=[{"name": "a", "policy_type": "cron", "asset_keys": ["t/x"]}]
        )


def test_empty_groups_rejected():
    with pytest.raises(ValueError, match="non-empty"):
        TableFreshnessWorkspaceComponent(groups=[])


def test_empty_asset_keys_in_group_rejected():
    with pytest.raises(ValueError, match="asset_keys must be non-empty"):
        TableFreshnessWorkspaceComponent(
            groups=[{"name": "a", "policy_type": "time_window", "fail_window_hours": 2, "asset_keys": []}]
        )
