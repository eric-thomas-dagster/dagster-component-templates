"""Committed regression tests for WorkableCandidateUpsertComponent.

The real Workable API is never called here -- FakeWorkableResource
(conftest.py) stands in for the one external-call boundary, while
everything this component actually owns -- dual source resolution,
search-then-write branching (create via job pipeline vs talent pool,
update + stage move on an existing match), validation, and metadata --
is exercised for real.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import FakeWorkableResource, load_component_module, make_upstream_asset


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize(component, upstream_df, resource):
    upstream_asset = make_upstream_asset("upstream_applicants", upstream_df)
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    return dg.materialize([asset_def, upstream_asset], resources={"workable_resource": resource})


def _metadata_for(result, asset_name: str) -> dict:
    """MaterializeResult(metadata=...) with no `value=` carries its data on
    the materialization event, not the step output -- read the event's
    metadata rather than output_for_node()."""
    raw = result.asset_materializations_for_node(asset_name)[0].metadata
    out = {}
    for k, v in raw.items():
        out[k] = v.value if hasattr(v, "value") else v
    return out


# --- validation ------------------------------------------------------------

def test_neither_upstream_nor_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.WorkableCandidateUpsertComponent(
            asset_name="x",
            email_column="email",
            name_column="name",
        ).build_defs(context=None)


def test_both_upstream_and_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.WorkableCandidateUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            source={"kind": "inline", "rows": []},
            email_column="email",
            name_column="name",
        ).build_defs(context=None)


def test_stage_column_without_member_id_raises(mod):
    with pytest.raises(ValueError, match="member_id"):
        mod.WorkableCandidateUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            email_column="email",
            name_column="name",
            stage_column="stage",
        ).build_defs(context=None)


def test_stage_column_with_member_id_is_valid(mod):
    # Should not raise.
    mod.WorkableCandidateUpsertComponent(
        asset_name="x",
        upstream_asset_key="foo",
        email_column="email",
        name_column="name",
        stage_column="stage",
        member_id="111",
    ).build_defs(context=None)


# --- missing-email rows -----------------------------------------------------

def test_missing_email_rows_skipped_and_counted(mod):
    df = pd.DataFrame({
        "email": ["jane@example.com", None, ""],
        "name": ["Jane Doe", "No Email", "Blank Email"],
    })
    resource = FakeWorkableResource()
    component = mod.WorkableCandidateUpsertComponent(
        asset_name="workable_out",
        upstream_asset_key="upstream_applicants",
        email_column="email",
        name_column="name",
    )
    result = _materialize(component, df, resource)
    assert result.success
    out = _metadata_for(result, "workable_out")
    assert out["rows_skipped_no_key"] == 2
    assert out["rows_created"] == 1
    assert resource.create_talent_pool_calls  # the one valid row was created


# --- create: talent pool vs job pipeline ------------------------------------

def test_new_email_creates_via_talent_pool_when_job_shortcode_unset(mod):
    df = pd.DataFrame({"email": ["jane@example.com"], "name": ["Jane Doe"]})
    resource = FakeWorkableResource()
    component = mod.WorkableCandidateUpsertComponent(
        asset_name="workable_out",
        upstream_asset_key="upstream_applicants",
        email_column="email",
        name_column="name",
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert len(resource.create_talent_pool_calls) == 1
    assert resource.create_job_calls == []
    body = resource.create_talent_pool_calls[0]["body"]
    assert body["email"] == "jane@example.com"
    assert body["name"] == "Jane Doe"
    out = _metadata_for(result, "workable_out")
    assert out["rows_created"] == 1
    assert out["rows_updated"] == 0


def test_new_email_creates_via_job_pipeline_when_job_shortcode_set(mod):
    df = pd.DataFrame({"email": ["jane@example.com"], "name": ["Jane Doe"]})
    resource = FakeWorkableResource()
    component = mod.WorkableCandidateUpsertComponent(
        asset_name="workable_out",
        upstream_asset_key="upstream_applicants",
        email_column="email",
        name_column="name",
        job_shortcode="ABCD1234",
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert len(resource.create_job_calls) == 1
    assert resource.create_talent_pool_calls == []
    call = resource.create_job_calls[0]
    assert call["shortcode"] == "ABCD1234"
    assert call["body"]["email"] == "jane@example.com"
    assert call["body"]["name"] == "Jane Doe"


# --- update + stage move on existing match ----------------------------------

def test_existing_email_updates_fields_and_moves_stage_not_create(mod):
    existing = {"jane@example.com": {"id": 42, "email": "jane@example.com", "stage": "sourced"}}
    resource = FakeWorkableResource(existing=existing)
    df = pd.DataFrame({
        "email": ["jane@example.com"],
        "name": ["Jane Doe"],
        "phone": ["+14155552671"],
        "stage": ["phone_screen"],
    })
    component = mod.WorkableCandidateUpsertComponent(
        asset_name="workable_out",
        upstream_asset_key="upstream_applicants",
        email_column="email",
        name_column="name",
        fields_map={"phone": "phone"},
        stage_column="stage",
        member_id="111",
    )
    result = _materialize(component, df, resource)
    assert result.success

    assert resource.create_job_calls == []
    assert resource.create_talent_pool_calls == []

    assert len(resource.update_calls) == 1
    update_call = resource.update_calls[0]
    assert update_call["candidate_id"] == 42
    assert update_call["body"] == {"phone": "+14155552671"}

    assert len(resource.move_calls) == 1
    move_call = resource.move_calls[0]
    assert move_call["candidate_id"] == 42
    assert move_call["member_id"] == "111"
    assert move_call["target_stage"] == "phone_screen"

    out = _metadata_for(result, "workable_out")
    assert out["rows_updated"] == 1
    assert out["rows_created"] == 0


def test_existing_email_without_stage_value_does_not_move(mod):
    existing = {"jane@example.com": {"id": 42, "email": "jane@example.com"}}
    resource = FakeWorkableResource(existing=existing)
    df = pd.DataFrame({
        "email": ["jane@example.com"],
        "name": ["Jane Doe"],
        "stage": [None],
    })
    component = mod.WorkableCandidateUpsertComponent(
        asset_name="workable_out",
        upstream_asset_key="upstream_applicants",
        email_column="email",
        name_column="name",
        stage_column="stage",
        member_id="111",
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert resource.move_calls == []


# --- max_rows cap ------------------------------------------------------------

def test_max_rows_caps_processed_rows(mod):
    df = pd.DataFrame({
        "email": [f"user{i}@example.com" for i in range(10)],
        "name": [f"User {i}" for i in range(10)],
    })
    resource = FakeWorkableResource()
    component = mod.WorkableCandidateUpsertComponent(
        asset_name="workable_out",
        upstream_asset_key="upstream_applicants",
        email_column="email",
        name_column="name",
        max_rows=3,
    )
    result = _materialize(component, df, resource)
    assert result.success
    out = _metadata_for(result, "workable_out")
    assert out["rows_created"] == 3


# --- inline source -----------------------------------------------------------

def test_source_inline_mode_end_to_end(mod):
    resource = FakeWorkableResource()
    component = mod.WorkableCandidateUpsertComponent(
        asset_name="workable_out",
        source={
            "kind": "inline",
            "rows": [
                {"email": "a@b.com", "name": "A B"},
                {"email": "c@d.com", "name": "C D"},
            ],
        },
        email_column="email",
        name_column="name",
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], resources={"workable_resource": resource})
    assert result.success
    out = _metadata_for(result, "workable_out")
    assert out["rows_created"] == 2
    assert len(resource.create_talent_pool_calls) == 2


# --- resource-call exception is caught, counted, doesn't fail the run ------

def test_resource_exception_is_caught_and_counted(mod):
    class RaisingResource(FakeWorkableResource):
        def find_candidate_by_email(self, email: str):
            if email == "boom@example.com":
                raise RuntimeError("simulated API failure")
            return super().find_candidate_by_email(email)

    resource = RaisingResource()
    df = pd.DataFrame({
        "email": ["ok@example.com", "boom@example.com"],
        "name": ["OK Person", "Boom Person"],
    })
    component = mod.WorkableCandidateUpsertComponent(
        asset_name="workable_out",
        upstream_asset_key="upstream_applicants",
        email_column="email",
        name_column="name",
    )
    result = _materialize(component, df, resource)
    assert result.success
    out = _metadata_for(result, "workable_out")
    assert out["rows_errored"] == 1
    assert out["rows_created"] == 1
    assert "first_errors" in out
