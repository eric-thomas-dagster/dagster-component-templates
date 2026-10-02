"""Committed regression tests for Auth0UserUpsertComponent.

`FakeAuth0Resource` (conftest.py) stands in for the real Auth0 Management
API and has no delete capability whatsoever -- everything this component
owns (dual source resolution, validation, email matching, sync vs.
deactivate branching, the explicit-flag requirement for deactivation, and
the structural absence of any delete path) is exercised for real.
"""
import pandas as pd
import pytest
import dagster as dg

from .conftest import FakeAuth0Resource, load_component_module, make_upstream_asset


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize(component, upstream_df, resource):
    upstream_asset = make_upstream_asset("upstream_employees", upstream_df)
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    return dg.materialize([asset_def, upstream_asset], resources={"auth0_resource": resource})


def _metadata_for(result, asset_name: str) -> dict:
    raw = result.asset_materializations_for_node(asset_name)[0].metadata
    out = {}
    for k, v in raw.items():
        out[k] = v.value if hasattr(v, "value") else v
    return out


# --- validation (build_defs time) -------------------------------------------

def test_neither_upstream_nor_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.Auth0UserUpsertComponent(
            asset_name="x",
            connection="Username-Password-Authentication",
            fields_map={"email": "email"},
        ).build_defs(context=None)


def test_both_upstream_and_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.Auth0UserUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            source={"kind": "inline", "rows": []},
            connection="Username-Password-Authentication",
            fields_map={"email": "email"},
        ).build_defs(context=None)


def test_invalid_operation_raises(mod):
    with pytest.raises(ValueError, match="operation must be one of"):
        mod.Auth0UserUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            connection="Username-Password-Authentication",
            fields_map={"email": "email"},
            operation="delete",
        ).build_defs(context=None)


def test_delete_is_explicitly_not_an_accepted_operation(mod):
    """There is no delete operation -- asserting this isn't just absent by
    omission, it's REJECTED by validation."""
    with pytest.raises(ValueError, match="no 'delete' operation"):
        mod.Auth0UserUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            connection="Username-Password-Authentication",
            fields_map={"email": "email"},
            operation="delete",
        ).build_defs(context=None)


def test_fields_map_without_email_raises(mod):
    with pytest.raises(ValueError, match="exactly one upstream column"):
        mod.Auth0UserUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            connection="Username-Password-Authentication",
            fields_map={"name_col": "name"},
        ).build_defs(context=None)


def test_fields_map_with_two_emails_raises(mod):
    with pytest.raises(ValueError, match="exactly one upstream column"):
        mod.Auth0UserUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            connection="Username-Password-Authentication",
            fields_map={"email_col": "email", "email_col2": "email"},
        ).build_defs(context=None)


def test_fields_map_targeting_blocked_raises(mod):
    """A row must never be able to deactivate a user just by having a
    particular column value -- so mapping anything to Auth0's `blocked`
    field is rejected outright, regardless of `operation`."""
    with pytest.raises(ValueError, match="may not target"):
        mod.Auth0UserUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            connection="Username-Password-Authentication",
            fields_map={"email_col": "email", "is_blocked": "blocked"},
        ).build_defs(context=None)


# --- sync: create-if-absent / update-if-present -----------------------------

def test_sync_creates_user_when_no_match(mod):
    df = pd.DataFrame({"email_col": ["new@example.com"], "name_col": ["New Person"]})
    resource = FakeAuth0Resource(existing_users=[])
    component = mod.Auth0UserUpsertComponent(
        asset_name="auth0_out",
        upstream_asset_key="upstream_employees",
        connection="Username-Password-Authentication",
        fields_map={"email_col": "email", "name_col": "name"},
    )
    result = _materialize(component, df, resource)
    assert result.success

    assert len(resource.create_calls) == 1
    assert resource.create_calls[0]["email"] == "new@example.com"
    assert resource.create_calls[0]["name"] == "New Person"
    assert resource.create_calls[0]["connection"] == "Username-Password-Authentication"
    assert resource.update_calls == []
    assert resource.set_blocked_calls == []

    out = _metadata_for(result, "auth0_out")
    assert out["rows_created"] == 1
    assert out["rows_updated"] == 0
    assert out["rows_deactivated"] == 0
    assert out["operation"] == "sync"


def test_sync_updates_user_when_matched(mod):
    df = pd.DataFrame({"email_col": ["existing@example.com"], "name_col": ["Updated Name"]})
    resource = FakeAuth0Resource(
        existing_users=[{"user_id": "auth0|abc", "email": "existing@example.com", "name": "Old Name"}]
    )
    component = mod.Auth0UserUpsertComponent(
        asset_name="auth0_out",
        upstream_asset_key="upstream_employees",
        connection="Username-Password-Authentication",
        fields_map={"email_col": "email", "name_col": "name"},
    )
    result = _materialize(component, df, resource)
    assert result.success

    assert resource.create_calls == []
    assert len(resource.update_calls) == 1
    assert resource.update_calls[0] == ("auth0|abc", {"email": "existing@example.com", "name": "Updated Name"})
    assert resource.set_blocked_calls == []  # sync mode NEVER touches blocked

    out = _metadata_for(result, "auth0_out")
    assert out["rows_created"] == 0
    assert out["rows_updated"] == 1


def test_sync_never_touches_blocked_even_for_a_previously_blocked_user(mod):
    """Core safety guarantee: ordinary sync traffic must never reactivate
    OR deactivate anyone -- it doesn't even read/write `blocked`."""
    df = pd.DataFrame({"email_col": ["existing@example.com"], "name_col": ["Same Name"]})
    resource = FakeAuth0Resource(
        existing_users=[{"user_id": "auth0|abc", "email": "existing@example.com", "name": "Same Name", "blocked": True}]
    )
    component = mod.Auth0UserUpsertComponent(
        asset_name="auth0_out",
        upstream_asset_key="upstream_employees",
        connection="Username-Password-Authentication",
        fields_map={"email_col": "email", "name_col": "name"},
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert resource.set_blocked_calls == []
    # The user's blocked flag is untouched by this run.
    assert resource.users[0]["blocked"] is True


def test_sync_skips_ambiguous_email_matches(mod):
    df = pd.DataFrame({"email_col": ["shared@example.com"], "name_col": ["Someone"]})
    resource = FakeAuth0Resource(
        existing_users=[
            {"user_id": "auth0|a", "email": "shared@example.com"},
            {"user_id": "auth0|b", "email": "shared@example.com"},
        ]
    )
    component = mod.Auth0UserUpsertComponent(
        asset_name="auth0_out",
        upstream_asset_key="upstream_employees",
        connection="Username-Password-Authentication",
        fields_map={"email_col": "email", "name_col": "name"},
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert resource.create_calls == []
    assert resource.update_calls == []
    out = _metadata_for(result, "auth0_out")
    assert out["rows_skipped_ambiguous_email"] == 1


def test_sync_skips_rows_with_blank_email(mod):
    df = pd.DataFrame({"email_col": [None, ""], "name_col": ["A", "B"]})
    resource = FakeAuth0Resource()
    component = mod.Auth0UserUpsertComponent(
        asset_name="auth0_out",
        upstream_asset_key="upstream_employees",
        connection="Username-Password-Authentication",
        fields_map={"email_col": "email", "name_col": "name"},
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert resource.create_calls == []
    out = _metadata_for(result, "auth0_out")
    assert out["rows_skipped_no_email"] == 2


# --- deactivate: EXPLICIT flag required, never creates ----------------------

def test_deactivate_requires_explicit_operation_flag(mod):
    """The same upstream data that would deactivate a user under
    operation=deactivate must NOT deactivate anyone when operation is left
    at its default -- this is the central safety guarantee."""
    df = pd.DataFrame({"email_col": ["existing@example.com"]})
    resource = FakeAuth0Resource(
        existing_users=[{"user_id": "auth0|abc", "email": "existing@example.com", "blocked": False}]
    )
    # Default operation (sync) -- no `operation:` configured at all.
    component = mod.Auth0UserUpsertComponent(
        asset_name="auth0_out",
        upstream_asset_key="upstream_employees",
        connection="Username-Password-Authentication",
        fields_map={"email_col": "email"},
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert resource.set_blocked_calls == []
    assert resource.users[0]["blocked"] is False


def test_deactivate_blocks_matched_user_via_set_blocked_only(mod):
    df = pd.DataFrame({"email_col": ["existing@example.com"]})
    resource = FakeAuth0Resource(
        existing_users=[{"user_id": "auth0|abc", "email": "existing@example.com", "blocked": False}]
    )
    component = mod.Auth0UserUpsertComponent(
        asset_name="auth0_out",
        upstream_asset_key="upstream_employees",
        connection="Username-Password-Authentication",
        fields_map={"email_col": "email"},
        operation="deactivate",
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert resource.set_blocked_calls == [("auth0|abc", True)]
    assert resource.create_calls == []
    assert resource.update_calls == []
    assert resource.users[0]["blocked"] is True

    out = _metadata_for(result, "auth0_out")
    assert out["rows_deactivated"] == 1
    assert out["operation"] == "deactivate"


def test_deactivate_never_creates_a_user_for_unmatched_row(mod):
    df = pd.DataFrame({"email_col": ["ghost@example.com"]})
    resource = FakeAuth0Resource(existing_users=[])
    component = mod.Auth0UserUpsertComponent(
        asset_name="auth0_out",
        upstream_asset_key="upstream_employees",
        connection="Username-Password-Authentication",
        fields_map={"email_col": "email"},
        operation="deactivate",
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert resource.create_calls == []
    assert resource.set_blocked_calls == []
    out = _metadata_for(result, "auth0_out")
    assert out["rows_skipped_not_found"] == 1
    assert out["rows_deactivated"] == 0


def test_deactivate_skips_ambiguous_email_matches(mod):
    df = pd.DataFrame({"email_col": ["shared@example.com"]})
    resource = FakeAuth0Resource(
        existing_users=[
            {"user_id": "auth0|a", "email": "shared@example.com"},
            {"user_id": "auth0|b", "email": "shared@example.com"},
        ]
    )
    component = mod.Auth0UserUpsertComponent(
        asset_name="auth0_out",
        upstream_asset_key="upstream_employees",
        connection="Username-Password-Authentication",
        fields_map={"email_col": "email"},
        operation="deactivate",
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert resource.set_blocked_calls == []
    out = _metadata_for(result, "auth0_out")
    assert out["rows_skipped_ambiguous_email"] == 1


# --- structural: no delete code path -----------------------------------------

def test_no_delete_capability_exists_in_component_source():
    """There is no code path in this component that can call a delete
    operation -- not a `delete_user` call, not a raw DELETE request."""
    import pathlib

    source = pathlib.Path(__file__).resolve().parent.parent.joinpath("component.py").read_text()
    assert "delete_user" not in source
    assert ".delete(" not in source


def test_fake_resource_has_no_delete_method():
    """The test double mirrors the real resource's API surface exactly --
    including the absence of any delete method."""
    resource = FakeAuth0Resource()
    assert not hasattr(resource, "delete_user")
    assert not hasattr(resource, "delete")


# --- source=inline mode -------------------------------------------------------

def test_source_inline_mode(mod):
    resource = FakeAuth0Resource()
    component = mod.Auth0UserUpsertComponent(
        asset_name="auth0_out",
        source={"kind": "inline", "rows": [{"email_col": "a@b.com"}, {"email_col": "c@d.com"}]},
        connection="Username-Password-Authentication",
        fields_map={"email_col": "email"},
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], resources={"auth0_resource": resource})
    assert result.success
    out = _metadata_for(result, "auth0_out")
    assert out["rows_created"] == 2


def test_batch_size_caps_rows(mod):
    df = pd.DataFrame({"email_col": [f"user{i}@example.com" for i in range(10)]})
    resource = FakeAuth0Resource()
    component = mod.Auth0UserUpsertComponent(
        asset_name="auth0_out",
        upstream_asset_key="upstream_employees",
        connection="Username-Password-Authentication",
        fields_map={"email_col": "email"},
        batch_size=3,
    )
    result = _materialize(component, df, resource)
    assert result.success
    out = _metadata_for(result, "auth0_out")
    assert out["rows_total"] == 3
    assert out["rows_created"] == 3
