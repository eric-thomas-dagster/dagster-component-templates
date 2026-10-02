"""Committed regression tests for JiraServiceManagementRequestUpsertComponent.

The real Jira Service Management HTTP API is never called here --
FakeJiraServiceManagementResource (conftest.py) stands in for the one
external boundary, while everything this component actually owns (dual
source resolution, JQL-clause building, validation, counting, comment
posting) is exercised for real.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import FakeJiraServiceManagementResource, load_component_module, make_upstream_asset


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize(component, upstream_df, resource):
    upstream_asset = make_upstream_asset("upstream_tickets", upstream_df)
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    return dg.materialize(
        [asset_def, upstream_asset],
        resources={"jira_service_management_resource": resource},
    )


def _metadata_for(result, asset_name: str) -> dict:
    """MaterializeResult(metadata=...) with no `value=` carries its data on
    the materialization event, not the step output -- read the event's
    metadata instead of output_for_node()."""
    raw = result.asset_materializations_for_node(asset_name)[0].metadata
    out = {}
    for k, v in raw.items():
        out[k] = v.value if hasattr(v, "value") else v
    return out


# --- _jql_match_clause (pure, no resource involved) ----------------------

def test_jql_match_clause_customfield(mod):
    assert mod._jql_match_clause("customfield_10050", "ABC-1") == 'cf[10050] = "ABC-1"'


def test_jql_match_clause_plain_field(mod):
    assert mod._jql_match_clause("labels", "value") == 'labels = "value"'


# --- validation ------------------------------------------------------------

def test_neither_upstream_nor_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.JiraServiceManagementRequestUpsertComponent(
            asset_name="x",
            project_key="ITSM",
            service_desk_id="1",
            request_type_id="10",
            key_field="customfield_10050",
            fields_map={"ticket_id": "customfield_10050"},
        ).build_defs(context=None)


def test_both_upstream_and_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.JiraServiceManagementRequestUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            source={"kind": "inline", "rows": []},
            project_key="ITSM",
            service_desk_id="1",
            request_type_id="10",
            key_field="customfield_10050",
            fields_map={"ticket_id": "customfield_10050"},
        ).build_defs(context=None)


def test_key_field_not_in_fields_map_raises(mod):
    with pytest.raises(ValueError, match="not present in fields_map"):
        mod.JiraServiceManagementRequestUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            project_key="ITSM",
            service_desk_id="1",
            request_type_id="10",
            key_field="customfield_10050",
            fields_map={"title": "summary"},
        ).build_defs(context=None)


# --- full asset body, against the fake resource ----------------------------

def test_missing_key_rows_skipped_and_counted(mod):
    df = pd.DataFrame({
        "ticket_id": ["T-1", None, ""],
        "title": ["first", "second", "third"],
    })
    resource = FakeJiraServiceManagementResource()
    component = mod.JiraServiceManagementRequestUpsertComponent(
        asset_name="jsm_upsert_out",
        upstream_asset_key="upstream_tickets",
        project_key="ITSM",
        service_desk_id="1",
        request_type_id="10",
        key_field="customfield_10050",
        fields_map={"ticket_id": "customfield_10050", "title": "summary"},
    )
    result = _materialize(component, df, resource)
    assert result.success
    out = _metadata_for(result, "jsm_upsert_out")
    assert out["rows_skipped_no_key"] == 2
    assert out["rows_created"] == 1


def test_new_key_value_creates_request_not_update(mod):
    df = pd.DataFrame({"ticket_id": ["T-1"], "title": ["first ticket"]})
    resource = FakeJiraServiceManagementResource()
    component = mod.JiraServiceManagementRequestUpsertComponent(
        asset_name="jsm_upsert_out",
        upstream_asset_key="upstream_tickets",
        project_key="ITSM",
        service_desk_id="1",
        request_type_id="10",
        key_field="customfield_10050",
        fields_map={"ticket_id": "customfield_10050", "title": "summary"},
    )
    result = _materialize(component, df, resource)
    assert result.success

    assert len(resource.create_calls) == 1
    assert resource.create_calls[0]["service_desk_id"] == "1"
    assert resource.create_calls[0]["request_type_id"] == "10"
    assert resource.create_calls[0]["request_field_values"] == {
        "customfield_10050": "T-1",
        "summary": "first ticket",
    }
    assert resource.update_calls == []

    out = _metadata_for(result, "jsm_upsert_out")
    assert out["rows_created"] == 1
    assert out["rows_updated"] == 0


def test_existing_key_value_updates_not_create(mod):
    df = pd.DataFrame({"ticket_id": ["T-1"], "title": ["updated title"]})
    resource = FakeJiraServiceManagementResource()
    expected_jql = f'project = "ITSM" AND {mod._jql_match_clause("customfield_10050", "T-1")}'
    resource.seed_issue(expected_jql, issue_key="ITSM-42")

    component = mod.JiraServiceManagementRequestUpsertComponent(
        asset_name="jsm_upsert_out",
        upstream_asset_key="upstream_tickets",
        project_key="ITSM",
        service_desk_id="1",
        request_type_id="10",
        key_field="customfield_10050",
        fields_map={"ticket_id": "customfield_10050", "title": "summary"},
    )
    result = _materialize(component, df, resource)
    assert result.success

    assert resource.create_calls == []
    assert len(resource.update_calls) == 1
    assert resource.update_calls[0]["issue_id_or_key"] == "ITSM-42"
    assert resource.update_calls[0]["fields"] == {
        "customfield_10050": "T-1",
        "summary": "updated title",
    }

    out = _metadata_for(result, "jsm_upsert_out")
    assert out["rows_created"] == 0
    assert out["rows_updated"] == 1


def test_comment_column_posts_comment_on_create_and_update(mod):
    df = pd.DataFrame({
        "ticket_id": ["T-1", "T-2"],
        "title": ["first", "second"],
        "note": ["internal note one", "internal note two"],
    })
    resource = FakeJiraServiceManagementResource()
    expected_jql_t2 = f'project = "ITSM" AND {mod._jql_match_clause("customfield_10050", "T-2")}'
    resource.seed_issue(expected_jql_t2, issue_key="ITSM-99")

    component = mod.JiraServiceManagementRequestUpsertComponent(
        asset_name="jsm_upsert_out",
        upstream_asset_key="upstream_tickets",
        project_key="ITSM",
        service_desk_id="1",
        request_type_id="10",
        key_field="customfield_10050",
        fields_map={"ticket_id": "customfield_10050", "title": "summary"},
        comment_column="note",
    )
    result = _materialize(component, df, resource)
    assert result.success

    assert len(resource.comment_calls) == 2
    # One on the newly-created request...
    created_key = resource.create_calls[0]["request_field_values"]["customfield_10050"]
    created_issue_key = next(
        c["issue_id_or_key"] for c in resource.comment_calls if c["body_text"] == "internal note one"
    )
    assert created_issue_key  # posted against the real created key, not the input value
    # ...and one on the updated (pre-existing) request.
    updated_comment = next(c for c in resource.comment_calls if c["body_text"] == "internal note two")
    assert updated_comment["issue_id_or_key"] == "ITSM-99"
    assert updated_comment["public"] is False


def test_batch_size_caps_rows(mod):
    df = pd.DataFrame({
        "ticket_id": [f"T-{i}" for i in range(10)],
        "title": [f"ticket {i}" for i in range(10)],
    })
    resource = FakeJiraServiceManagementResource()
    component = mod.JiraServiceManagementRequestUpsertComponent(
        asset_name="jsm_upsert_out",
        upstream_asset_key="upstream_tickets",
        project_key="ITSM",
        service_desk_id="1",
        request_type_id="10",
        key_field="customfield_10050",
        fields_map={"ticket_id": "customfield_10050", "title": "summary"},
        batch_size=3,
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert len(resource.create_calls) == 3
    out = _metadata_for(result, "jsm_upsert_out")
    assert out["rows_created"] == 3


def test_source_inline_mode(mod):
    resource = FakeJiraServiceManagementResource()
    component = mod.JiraServiceManagementRequestUpsertComponent(
        asset_name="jsm_upsert_out",
        source={"kind": "inline", "rows": [
            {"ticket_id": "T-1", "title": "a"},
            {"ticket_id": "T-2", "title": "b"},
        ]},
        project_key="ITSM",
        service_desk_id="1",
        request_type_id="10",
        key_field="customfield_10050",
        fields_map={"ticket_id": "customfield_10050", "title": "summary"},
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], resources={"jira_service_management_resource": resource})
    assert result.success
    out = _metadata_for(result, "jsm_upsert_out")
    assert out["rows_created"] == 2


def test_resource_exception_caught_and_counted_as_error(mod):
    df = pd.DataFrame({"ticket_id": ["T-1"], "title": ["first"]})
    resource = FakeJiraServiceManagementResource()
    resource.raise_on_create = True
    component = mod.JiraServiceManagementRequestUpsertComponent(
        asset_name="jsm_upsert_out",
        upstream_asset_key="upstream_tickets",
        project_key="ITSM",
        service_desk_id="1",
        request_type_id="10",
        key_field="customfield_10050",
        fields_map={"ticket_id": "customfield_10050", "title": "summary"},
    )
    result = _materialize(component, df, resource)
    # The run as a whole still succeeds -- per-row errors are caught and
    # surfaced in metadata, not raised.
    assert result.success
    out = _metadata_for(result, "jsm_upsert_out")
    assert out["rows_errored"] == 1
    assert out["rows_created"] == 0
    assert "simulated create_request failure" in out["first_errors"][0]
