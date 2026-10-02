"""Committed regression tests for LeverCandidateUpsertComponent.

FakeLeverResource (conftest.py) stands in for the real `lever_resource` --
the one external, network-calling boundary -- while everything this
component actually owns (dual source resolution, email-match search, the
tag/stage/archive branching, create-body construction, row skipping, error
handling, metadata) is exercised for real via `dg.materialize`.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import FakeLeverResource, load_component_module, make_upstream_asset


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize(component, upstream_df, resource, asset_name="upstream_candidates"):
    upstream_asset = make_upstream_asset(asset_name, upstream_df)
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    return dg.materialize([asset_def, upstream_asset], resources={"lever_resource": resource})


def _metadata_for(result, asset_name: str) -> dict:
    """MaterializeResult(metadata=...) with no `value=` carries its data on
    the materialization event, not the step output -- read the event's
    metadata instead of output_for_node()."""
    raw = result.asset_materializations_for_node(asset_name)[0].metadata
    out = {}
    for k, v in raw.items():
        out[k] = v.value if hasattr(v, "value") else v
    return out


# --- dual-source validation -----------------------------------------------

def test_neither_upstream_nor_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.LeverCandidateUpsertComponent(
            asset_name="x",
            email_column="email",
        ).build_defs(context=None)


def test_both_upstream_and_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.LeverCandidateUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            source={"kind": "inline", "rows": []},
            email_column="email",
        ).build_defs(context=None)


# --- missing email rows ---------------------------------------------------

def test_missing_email_rows_are_skipped_and_counted(mod):
    df = pd.DataFrame({"email": ["jane@example.com", None, "", "bob@example.com"]})
    resource = FakeLeverResource()
    component = mod.LeverCandidateUpsertComponent(
        asset_name="lever_candidates_out",
        upstream_asset_key="upstream_candidates",
        email_column="email",
    )
    result = _materialize(component, df, resource)
    assert result.success
    out = _metadata_for(result, "lever_candidates_out")
    assert out["rows_skipped_no_key"] == 2
    assert out["rows_created"] == 2


# --- create path (no existing match) --------------------------------------

def test_no_match_creates_opportunity_with_full_body(mod):
    df = pd.DataFrame({
        "email": ["jane@example.com"],
        "full_name": ["Jane Doe"],
        "title": ["Eng Manager @ Acme"],
        "tags": ["warm-lead,referral"],
        "stage": ["lead-stage-id"],
    })
    resource = FakeLeverResource()
    component = mod.LeverCandidateUpsertComponent(
        asset_name="lever_candidates_out",
        upstream_asset_key="upstream_candidates",
        email_column="email",
        name_column="full_name",
        headline_column="title",
        tags_column="tags",
        stage_id_column="stage",
        posting_id="posting-abc",
    )
    result = _materialize(component, df, resource)
    assert result.success

    assert len(resource.create_calls) == 1
    call = resource.create_calls[0]
    assert call["posting_id"] == "posting-abc"
    body = call["body"]
    assert body["emails"] == ["jane@example.com"]
    assert body["name"] == "Jane Doe"
    assert body["headline"] == "Eng Manager @ Acme"
    assert body["tags"] == ["warm-lead", "referral"]
    assert body["stage"] == "lead-stage-id"

    assert resource.add_tags_calls == []  # tags land directly in the create body
    out = _metadata_for(result, "lever_candidates_out")
    assert out["rows_created"] == 1
    assert out["rows_updated"] == 0


# --- update path (existing match) -----------------------------------------

def test_match_calls_tag_stage_archive_not_create(mod):
    resource = FakeLeverResource()
    resource.seed_opportunity("bob@example.com")

    df = pd.DataFrame({
        "email": ["bob@example.com"],
        "tags": ["re-engaged"],
        "stage": ["interview-stage-id"],
        "archive_reason": ["not-a-fit"],
    })
    component = mod.LeverCandidateUpsertComponent(
        asset_name="lever_candidates_out",
        upstream_asset_key="upstream_candidates",
        email_column="email",
        tags_column="tags",
        stage_id_column="stage",
        archive_reason_column="archive_reason",
    )
    result = _materialize(component, df, resource)
    assert result.success

    assert resource.create_calls == []
    assert len(resource.add_tags_calls) == 1
    assert resource.add_tags_calls[0]["tags"] == ["re-engaged"]
    assert len(resource.update_stage_calls) == 1
    assert resource.update_stage_calls[0]["stage_id"] == "interview-stage-id"
    assert len(resource.archive_calls) == 1
    assert resource.archive_calls[0]["reason"] == "not-a-fit"

    out = _metadata_for(result, "lever_candidates_out")
    assert out["rows_created"] == 0
    assert out["rows_updated"] == 1


def test_match_with_no_configured_action_still_counts_as_updated(mod):
    resource = FakeLeverResource()
    resource.seed_opportunity("bob@example.com")

    df = pd.DataFrame({"email": ["bob@example.com"]})
    component = mod.LeverCandidateUpsertComponent(
        asset_name="lever_candidates_out",
        upstream_asset_key="upstream_candidates",
        email_column="email",
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert resource.add_tags_calls == []
    assert resource.update_stage_calls == []
    assert resource.archive_calls == []
    out = _metadata_for(result, "lever_candidates_out")
    assert out["rows_updated"] == 1


# --- max_rows cap ----------------------------------------------------------

def test_max_rows_caps_processed_rows(mod):
    df = pd.DataFrame({"email": [f"user{i}@example.com" for i in range(10)]})
    resource = FakeLeverResource()
    component = mod.LeverCandidateUpsertComponent(
        asset_name="lever_candidates_out",
        upstream_asset_key="upstream_candidates",
        email_column="email",
        max_rows=3,
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert len(resource.create_calls) == 3
    out = _metadata_for(result, "lever_candidates_out")
    assert out["rows_created"] == 3


# --- inline source shape ----------------------------------------------------

def test_source_inline_mode(mod):
    resource = FakeLeverResource()
    component = mod.LeverCandidateUpsertComponent(
        asset_name="lever_candidates_out",
        source={"kind": "inline", "rows": [
            {"email": "a@b.com"},
            {"email": "c@d.com"},
        ]},
        email_column="email",
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], resources={"lever_resource": resource})
    assert result.success
    out = _metadata_for(result, "lever_candidates_out")
    assert out["rows_created"] == 2


# --- error handling ----------------------------------------------------------

def test_create_error_is_caught_counted_and_does_not_blow_up_run(mod, monkeypatch):
    resource = FakeLeverResource()

    def _boom(body, posting_id=None):
        raise RuntimeError("Lever API 500")

    monkeypatch.setattr(resource, "create_opportunity", _boom)

    df = pd.DataFrame({"email": ["jane@example.com", "bob@example.com"]})
    component = mod.LeverCandidateUpsertComponent(
        asset_name="lever_candidates_out",
        upstream_asset_key="upstream_candidates",
        email_column="email",
    )
    result = _materialize(component, df, resource)
    assert result.success  # the asset itself still succeeds -- errors are collected, not raised
    out = _metadata_for(result, "lever_candidates_out")
    assert out["rows_errored"] == 2
    assert out["rows_created"] == 0
    assert "first_errors" in out
