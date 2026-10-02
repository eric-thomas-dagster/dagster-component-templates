"""Committed regression tests for OktaUserUpsertComponent.

`FakeOktaResource` (conftest.py) stands in for the real Okta Users API and
has no delete capability whatsoever -- everything this component owns
(dual source resolution, validation, login matching, sync vs. deactivate
branching, the explicit-flag requirement for deactivation, already-
deactivated handling, and the structural absence of any delete path) is
exercised for real.
"""
import pandas as pd
import pytest
import dagster as dg

from .conftest import FakeOktaResource, load_component_module, make_upstream_asset


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize(component, upstream_df, resource):
    upstream_asset = make_upstream_asset("upstream_employees", upstream_df)
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    return dg.materialize([asset_def, upstream_asset], resources={"okta_resource": resource})


def _metadata_for(result, asset_name: str) -> dict:
    raw = result.asset_materializations_for_node(asset_name)[0].metadata
    out = {}
    for k, v in raw.items():
        out[k] = v.value if hasattr(v, "value") else v
    return out


# --- validation (build_defs time) -------------------------------------------

def test_neither_upstream_nor_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.OktaUserUpsertComponent(
            asset_name="x",
            fields_map={"login_col": "login"},
        ).build_defs(context=None)


def test_both_upstream_and_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.OktaUserUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            source={"kind": "inline", "rows": []},
            fields_map={"login_col": "login"},
        ).build_defs(context=None)


def test_invalid_operation_raises(mod):
    with pytest.raises(ValueError, match="operation must be one of"):
        mod.OktaUserUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            fields_map={"login_col": "login"},
            operation="delete",
        ).build_defs(context=None)


def test_delete_is_explicitly_not_an_accepted_operation(mod):
    with pytest.raises(ValueError, match="no 'delete' operation"):
        mod.OktaUserUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            fields_map={"login_col": "login"},
            operation="delete",
        ).build_defs(context=None)


def test_fields_map_without_login_raises(mod):
    with pytest.raises(ValueError, match="exactly one upstream column"):
        mod.OktaUserUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            fields_map={"first_name_col": "firstName"},
        ).build_defs(context=None)


def test_fields_map_with_two_logins_raises(mod):
    with pytest.raises(ValueError, match="exactly one upstream column"):
        mod.OktaUserUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            fields_map={"login_col": "login", "login_col2": "login"},
        ).build_defs(context=None)


def test_fields_map_targeting_status_raises(mod):
    with pytest.raises(ValueError, match="may not target"):
        mod.OktaUserUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            fields_map={"login_col": "login", "is_active": "status"},
        ).build_defs(context=None)


# --- sync: create-if-absent / merge-update-if-present -----------------------

def test_sync_creates_user_when_no_match(mod):
    df = pd.DataFrame({"login_col": ["new@example.com"], "first_col": ["New"]})
    resource = FakeOktaResource(existing_users=[])
    component = mod.OktaUserUpsertComponent(
        asset_name="okta_out",
        upstream_asset_key="upstream_employees",
        fields_map={"login_col": "login", "first_col": "firstName"},
    )
    result = _materialize(component, df, resource)
    assert result.success

    assert len(resource.create_calls) == 1
    assert resource.create_calls[0]["profile"] == {"login": "new@example.com", "firstName": "New"}
    assert resource.create_calls[0]["activate"] is True  # Okta's own documented default
    assert resource.update_calls == []
    assert resource.deactivate_calls == []

    out = _metadata_for(result, "okta_out")
    assert out["rows_created"] == 1
    assert out["operation"] == "sync"


def test_sync_updates_user_when_matched_merge_semantics(mod):
    df = pd.DataFrame({"login_col": ["existing@example.com"], "first_col": ["Updated"]})
    resource = FakeOktaResource(
        existing_users=[
            {"id": "00u1", "status": "ACTIVE", "profile": {"login": "existing@example.com", "firstName": "Old", "lastName": "Keep"}}
        ]
    )
    component = mod.OktaUserUpsertComponent(
        asset_name="okta_out",
        upstream_asset_key="upstream_employees",
        fields_map={"login_col": "login", "first_col": "firstName"},
    )
    result = _materialize(component, df, resource)
    assert result.success

    assert resource.create_calls == []
    assert resource.update_calls == [("00u1", {"login": "existing@example.com", "firstName": "Updated"})]
    assert resource.deactivate_calls == []
    # Merge semantics: lastName, never mentioned by fields_map, survives untouched.
    assert resource.users[0]["profile"]["lastName"] == "Keep"

    out = _metadata_for(result, "okta_out")
    assert out["rows_updated"] == 1


def test_sync_never_deactivates_even_an_already_deprovisioned_user(mod):
    df = pd.DataFrame({"login_col": ["existing@example.com"]})
    resource = FakeOktaResource(
        existing_users=[{"id": "00u1", "status": "DEPROVISIONED", "profile": {"login": "existing@example.com"}}]
    )
    component = mod.OktaUserUpsertComponent(
        asset_name="okta_out",
        upstream_asset_key="upstream_employees",
        fields_map={"login_col": "login"},
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert resource.deactivate_calls == []
    assert resource.users[0]["status"] == "DEPROVISIONED"  # untouched either direction


def test_sync_skips_rows_with_blank_login(mod):
    df = pd.DataFrame({"login_col": [None, ""], "first_col": ["A", "B"]})
    resource = FakeOktaResource()
    component = mod.OktaUserUpsertComponent(
        asset_name="okta_out",
        upstream_asset_key="upstream_employees",
        fields_map={"login_col": "login", "first_col": "firstName"},
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert resource.create_calls == []
    out = _metadata_for(result, "okta_out")
    assert out["rows_skipped_no_login"] == 2


# --- deactivate: EXPLICIT flag required, never creates ----------------------

def test_deactivate_requires_explicit_operation_flag(mod):
    """The same upstream data that would deactivate a user under
    operation=deactivate must NOT deactivate anyone when operation is left
    at its default -- this is the central safety guarantee."""
    df = pd.DataFrame({"login_col": ["existing@example.com"]})
    resource = FakeOktaResource(
        existing_users=[{"id": "00u1", "status": "ACTIVE", "profile": {"login": "existing@example.com"}}]
    )
    component = mod.OktaUserUpsertComponent(
        asset_name="okta_out",
        upstream_asset_key="upstream_employees",
        fields_map={"login_col": "login"},
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert resource.deactivate_calls == []
    assert resource.users[0]["status"] == "ACTIVE"


def test_deactivate_deactivates_matched_active_user(mod):
    df = pd.DataFrame({"login_col": ["existing@example.com"]})
    resource = FakeOktaResource(
        existing_users=[{"id": "00u1", "status": "ACTIVE", "profile": {"login": "existing@example.com"}}]
    )
    component = mod.OktaUserUpsertComponent(
        asset_name="okta_out",
        upstream_asset_key="upstream_employees",
        fields_map={"login_col": "login"},
        operation="deactivate",
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert resource.deactivate_calls == [("00u1", False)]
    assert resource.create_calls == []
    assert resource.update_calls == []
    assert resource.users[0]["status"] == "DEPROVISIONED"

    out = _metadata_for(result, "okta_out")
    assert out["rows_deactivated"] == 1
    assert out["operation"] == "deactivate"


def test_deactivate_never_creates_a_user_for_unmatched_row(mod):
    df = pd.DataFrame({"login_col": ["ghost@example.com"]})
    resource = FakeOktaResource(existing_users=[])
    component = mod.OktaUserUpsertComponent(
        asset_name="okta_out",
        upstream_asset_key="upstream_employees",
        fields_map={"login_col": "login"},
        operation="deactivate",
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert resource.create_calls == []
    assert resource.deactivate_calls == []
    out = _metadata_for(result, "okta_out")
    assert out["rows_skipped_not_found"] == 1
    assert out["rows_deactivated"] == 0


def test_deactivate_skips_already_deprovisioned_as_noop(mod):
    df = pd.DataFrame({"login_col": ["existing@example.com"]})
    resource = FakeOktaResource(
        existing_users=[{"id": "00u1", "status": "DEPROVISIONED", "profile": {"login": "existing@example.com"}}]
    )
    component = mod.OktaUserUpsertComponent(
        asset_name="okta_out",
        upstream_asset_key="upstream_employees",
        fields_map={"login_col": "login"},
        operation="deactivate",
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert resource.deactivate_calls == []  # never re-called on an already-deprovisioned user
    out = _metadata_for(result, "okta_out")
    assert out["rows_skipped_already_deactivated"] == 1
    assert out["rows_deactivated"] == 0


def test_send_deactivate_email_param_is_threaded_through(mod):
    df = pd.DataFrame({"login_col": ["existing@example.com"]})
    resource = FakeOktaResource(
        existing_users=[{"id": "00u1", "status": "ACTIVE", "profile": {"login": "existing@example.com"}}]
    )
    component = mod.OktaUserUpsertComponent(
        asset_name="okta_out",
        upstream_asset_key="upstream_employees",
        fields_map={"login_col": "login"},
        operation="deactivate",
        send_deactivate_email=True,
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert resource.deactivate_calls == [("00u1", True)]


# --- structural: no delete code path -----------------------------------------

def test_no_delete_capability_exists_in_component_source():
    import pathlib

    source = pathlib.Path(__file__).resolve().parent.parent.joinpath("component.py").read_text()
    assert "delete_user" not in source
    assert ".delete(" not in source


def test_fake_resource_has_no_delete_method():
    resource = FakeOktaResource()
    assert not hasattr(resource, "delete_user")
    assert not hasattr(resource, "delete")


# --- source=inline mode -------------------------------------------------------

def test_source_inline_mode(mod):
    resource = FakeOktaResource()
    component = mod.OktaUserUpsertComponent(
        asset_name="okta_out",
        source={"kind": "inline", "rows": [{"login_col": "a@b.com"}, {"login_col": "c@d.com"}]},
        fields_map={"login_col": "login"},
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], resources={"okta_resource": resource})
    assert result.success
    out = _metadata_for(result, "okta_out")
    assert out["rows_created"] == 2


def test_batch_size_caps_rows(mod):
    df = pd.DataFrame({"login_col": [f"user{i}@example.com" for i in range(10)]})
    resource = FakeOktaResource()
    component = mod.OktaUserUpsertComponent(
        asset_name="okta_out",
        upstream_asset_key="upstream_employees",
        fields_map={"login_col": "login"},
        batch_size=3,
    )
    result = _materialize(component, df, resource)
    assert result.success
    out = _metadata_for(result, "okta_out")
    assert out["rows_total"] == 3
    assert out["rows_created"] == 3
