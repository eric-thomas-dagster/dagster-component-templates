"""Committed regression tests for LinearIssueUpsertComponent.

The real Linear API is never called here -- `_call_linear_api` (the one
external, paid-API boundary) is monkeypatched wholesale, while dual source
resolution, deterministic uuid5 id derivation, the bulk existence check,
label/state/user lookup caching, create-vs-update routing, and metadata
are all exercised for real.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import FakeLinearResource, load_component_module, make_upstream_asset, metadata_for

TEAM_ID = "team-abc-123"


def _fake_backend(existing_ids=None, labels=None, states=None, users=None):
    """Builds a `_call_linear_api` replacement that dispatches on query
    text, mimicking the shape of Linear's real responses closely enough to
    exercise this component's parsing logic for real."""
    existing_ids = set(existing_ids or [])
    labels = labels or []     # [{"id": ..., "name": ...}, ...]
    states = states or []
    users = users or []
    calls = {"create": [], "update": [], "exists": [], "labels": 0, "states": 0, "users": 0}

    def _call(resource, query, variables=None):
        variables = variables or {}
        if "issues(filter" in query:
            calls["exists"].append(variables)
            matched = [{"id": i} for i in variables["ids"] if i in existing_ids]
            return {"issues": {"nodes": matched}}
        if "issueCreate" in query:
            calls["create"].append(variables["input"])
            return {"issueCreate": {"success": True, "issue": {"id": variables["input"]["id"], "identifier": "ENG-1"}}}
        if "issueUpdate" in query:
            calls["update"].append(variables)
            return {"issueUpdate": {"success": True, "issue": {"id": variables["id"], "identifier": "ENG-1"}}}
        if "labels(first" in query:
            calls["labels"] += 1
            return {"team": {"labels": {"nodes": labels}}}
        if "states(first" in query:
            calls["states"] += 1
            return {"team": {"states": {"nodes": states}}}
        if "users(first" in query:
            calls["users"] += 1
            return {"users": {"nodes": users}}
        raise AssertionError(f"unexpected query:\n{query}")

    return _call, calls


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize(component, upstream_df, fake_call, monkeypatch, mod_):
    monkeypatch.setattr(mod_, "_call_linear_api", fake_call)
    upstream_asset = make_upstream_asset("upstream_issues", upstream_df)
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    return dg.materialize([asset_def, upstream_asset], resources={"linear_resource": FakeLinearResource()})


# --- validation --------------------------------------------------------

def test_neither_upstream_nor_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.LinearIssueUpsertComponent(
            asset_name="x", team_id=TEAM_ID, key_column="id", title_column="title",
        ).build_defs(context=None)


def test_both_upstream_and_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.LinearIssueUpsertComponent(
            asset_name="x", team_id=TEAM_ID, key_column="id", title_column="title",
            upstream_asset_key="foo", source={"kind": "inline", "rows": []},
        ).build_defs(context=None)


def test_missing_upstream_columns_raises_failure(mod, monkeypatch):
    fake_call, _ = _fake_backend()
    df = pd.DataFrame({"id": ["1"]})  # missing title_column
    component = mod.LinearIssueUpsertComponent(
        asset_name="linear_out", upstream_asset_key="upstream_issues",
        team_id=TEAM_ID, key_column="id", title_column="title",
    )
    with pytest.raises(Exception):
        _materialize(component, df, fake_call, monkeypatch, mod)


# --- deterministic id derivation ----------------------------------------

def test_derive_issue_id_is_deterministic(mod):
    id1 = mod._derive_issue_id(TEAM_ID, "INC-1")
    id2 = mod._derive_issue_id(TEAM_ID, "INC-1")
    id3 = mod._derive_issue_id(TEAM_ID, "INC-2")
    assert id1 == id2
    assert id1 != id3


def test_derive_issue_id_differs_across_teams(mod):
    id1 = mod._derive_issue_id("team-a", "INC-1")
    id2 = mod._derive_issue_id("team-b", "INC-1")
    assert id1 != id2


# --- create / update routing --------------------------------------------

def test_fresh_rows_are_created(mod, monkeypatch):
    fake_call, calls = _fake_backend(existing_ids=set())
    df = pd.DataFrame({"id": ["INC-1", "INC-2"], "title": ["A", "B"]})
    component = mod.LinearIssueUpsertComponent(
        asset_name="linear_out", upstream_asset_key="upstream_issues",
        team_id=TEAM_ID, key_column="id", title_column="title",
    )
    result = _materialize(component, df, fake_call, monkeypatch, mod)
    assert result.success
    assert len(calls["create"]) == 2
    assert len(calls["update"]) == 0
    for c in calls["create"]:
        assert c["teamId"] == TEAM_ID
        assert "id" in c
    out = metadata_for(result, "linear_out")
    assert out["rows_created"] == 2
    assert out["rows_updated"] == 0
    assert out["rows_upserted"] == 2


def test_existing_rows_are_updated(mod, monkeypatch):
    # Pre-compute the deterministic id for INC-1 so the fake "exists" check matches it.
    existing_id = mod._derive_issue_id(TEAM_ID, "INC-1")
    fake_call, calls = _fake_backend(existing_ids={existing_id})
    df = pd.DataFrame({"id": ["INC-1", "INC-2"], "title": ["A", "B"]})
    component = mod.LinearIssueUpsertComponent(
        asset_name="linear_out", upstream_asset_key="upstream_issues",
        team_id=TEAM_ID, key_column="id", title_column="title",
    )
    result = _materialize(component, df, fake_call, monkeypatch, mod)
    assert result.success
    assert len(calls["update"]) == 1
    assert calls["update"][0]["id"] == existing_id
    assert len(calls["create"]) == 1
    out = metadata_for(result, "linear_out")
    assert out["rows_created"] == 1
    assert out["rows_updated"] == 1
    assert out["rows_upserted"] == 2


def test_rerun_with_same_keys_all_updates_not_duplicates(mod, monkeypatch):
    """Simulates a second run: every id from the first run now 'exists'."""
    df = pd.DataFrame({"id": ["INC-1", "INC-2", "INC-3"], "title": ["A", "B", "C"]})
    all_ids = {mod._derive_issue_id(TEAM_ID, k) for k in df["id"]}
    fake_call, calls = _fake_backend(existing_ids=all_ids)
    component = mod.LinearIssueUpsertComponent(
        asset_name="linear_out", upstream_asset_key="upstream_issues",
        team_id=TEAM_ID, key_column="id", title_column="title",
    )
    result = _materialize(component, df, fake_call, monkeypatch, mod)
    assert result.success
    assert len(calls["create"]) == 0
    assert len(calls["update"]) == 3


# --- priority mapping -----------------------------------------------------

def test_priority_name_mapped_to_int(mod, monkeypatch):
    fake_call, calls = _fake_backend()
    df = pd.DataFrame({"id": ["INC-1"], "title": ["A"], "severity": ["Urgent"]})
    component = mod.LinearIssueUpsertComponent(
        asset_name="linear_out", upstream_asset_key="upstream_issues",
        team_id=TEAM_ID, key_column="id", title_column="title", priority_column="severity",
    )
    result = _materialize(component, df, fake_call, monkeypatch, mod)
    assert result.success
    assert calls["create"][0]["priority"] == 1


def test_priority_int_passthrough(mod, monkeypatch):
    fake_call, calls = _fake_backend()
    df = pd.DataFrame({"id": ["INC-1"], "title": ["A"], "severity": [2]})
    component = mod.LinearIssueUpsertComponent(
        asset_name="linear_out", upstream_asset_key="upstream_issues",
        team_id=TEAM_ID, key_column="id", title_column="title", priority_column="severity",
    )
    result = _materialize(component, df, fake_call, monkeypatch, mod)
    assert result.success
    assert calls["create"][0]["priority"] == 2


# --- labels / states / assignee lookups ------------------------------------

def test_label_names_resolved_to_ids_and_cached(mod, monkeypatch):
    fake_call, calls = _fake_backend(labels=[{"id": "lbl-1", "name": "bug"}, {"id": "lbl-2", "name": "urgent"}])
    df = pd.DataFrame({"id": ["INC-1", "INC-2"], "title": ["A", "B"], "labels": ["bug", "urgent,bug"]})
    component = mod.LinearIssueUpsertComponent(
        asset_name="linear_out", upstream_asset_key="upstream_issues",
        team_id=TEAM_ID, key_column="id", title_column="title", label_names_column="labels",
    )
    result = _materialize(component, df, fake_call, monkeypatch, mod)
    assert result.success
    assert calls["create"][0]["labelIds"] == ["lbl-1"]
    assert set(calls["create"][1]["labelIds"]) == {"lbl-1", "lbl-2"}
    # Cached: only one labels() lookup despite 2 rows needing it.
    assert calls["labels"] == 1


def test_unknown_label_name_skipped_without_crashing(mod, monkeypatch):
    fake_call, calls = _fake_backend(labels=[{"id": "lbl-1", "name": "bug"}])
    df = pd.DataFrame({"id": ["INC-1"], "title": ["A"], "labels": ["nonexistent"]})
    component = mod.LinearIssueUpsertComponent(
        asset_name="linear_out", upstream_asset_key="upstream_issues",
        team_id=TEAM_ID, key_column="id", title_column="title", label_names_column="labels",
    )
    result = _materialize(component, df, fake_call, monkeypatch, mod)
    assert result.success
    assert "labelIds" not in calls["create"][0]


def test_default_labels_always_applied(mod, monkeypatch):
    fake_call, calls = _fake_backend(labels=[{"id": "lbl-1", "name": "auto-synced"}])
    df = pd.DataFrame({"id": ["INC-1"], "title": ["A"]})
    component = mod.LinearIssueUpsertComponent(
        asset_name="linear_out", upstream_asset_key="upstream_issues",
        team_id=TEAM_ID, key_column="id", title_column="title", default_labels=["auto-synced"],
    )
    result = _materialize(component, df, fake_call, monkeypatch, mod)
    assert result.success
    assert calls["create"][0]["labelIds"] == ["lbl-1"]


def test_state_name_resolved_to_id(mod, monkeypatch):
    fake_call, calls = _fake_backend(states=[{"id": "state-1", "name": "Done"}])
    df = pd.DataFrame({"id": ["INC-1"], "title": ["A"], "status": ["Done"]})
    component = mod.LinearIssueUpsertComponent(
        asset_name="linear_out", upstream_asset_key="upstream_issues",
        team_id=TEAM_ID, key_column="id", title_column="title", state_name_column="status",
    )
    result = _materialize(component, df, fake_call, monkeypatch, mod)
    assert result.success
    assert calls["create"][0]["stateId"] == "state-1"


def test_assignee_email_resolved_to_user_id(mod, monkeypatch):
    fake_call, calls = _fake_backend(users=[{"id": "user-1", "email": "a@b.com"}])
    df = pd.DataFrame({"id": ["INC-1"], "title": ["A"], "owner_email": ["a@b.com"]})
    component = mod.LinearIssueUpsertComponent(
        asset_name="linear_out", upstream_asset_key="upstream_issues",
        team_id=TEAM_ID, key_column="id", title_column="title", assignee_email_column="owner_email",
    )
    result = _materialize(component, df, fake_call, monkeypatch, mod)
    assert result.success
    assert calls["create"][0]["assigneeId"] == "user-1"


# --- edge cases -------------------------------------------------------------

def test_empty_upstream_short_circuits(mod, monkeypatch):
    fake_call, calls = _fake_backend()
    df = pd.DataFrame({"id": [], "title": []})
    component = mod.LinearIssueUpsertComponent(
        asset_name="linear_out", upstream_asset_key="upstream_issues",
        team_id=TEAM_ID, key_column="id", title_column="title",
    )
    result = _materialize(component, df, fake_call, monkeypatch, mod)
    assert result.success
    assert calls["create"] == [] and calls["update"] == [] and calls["exists"] == []
    out = metadata_for(result, "linear_out")
    assert out["rows_upserted"] == 0


def test_rows_missing_key_skipped_and_counted(mod, monkeypatch):
    fake_call, calls = _fake_backend()
    df = pd.DataFrame({"id": ["INC-1", None], "title": ["A", "B"]})
    component = mod.LinearIssueUpsertComponent(
        asset_name="linear_out", upstream_asset_key="upstream_issues",
        team_id=TEAM_ID, key_column="id", title_column="title",
    )
    result = _materialize(component, df, fake_call, monkeypatch, mod)
    assert result.success
    out = metadata_for(result, "linear_out")
    assert out["rows_skipped_no_key"] == 1
    assert len(calls["create"]) == 1


def test_batch_size_caps_rows(mod, monkeypatch):
    fake_call, calls = _fake_backend()
    df = pd.DataFrame({"id": [f"INC-{i}" for i in range(10)], "title": [f"T{i}" for i in range(10)]})
    component = mod.LinearIssueUpsertComponent(
        asset_name="linear_out", upstream_asset_key="upstream_issues",
        team_id=TEAM_ID, key_column="id", title_column="title", batch_size=3,
    )
    result = _materialize(component, df, fake_call, monkeypatch, mod)
    assert result.success
    out = metadata_for(result, "linear_out")
    assert out["rows_upserted"] == 3


def test_source_inline_mode(mod, monkeypatch):
    fake_call, calls = _fake_backend()
    component = mod.LinearIssueUpsertComponent(
        asset_name="linear_out",
        source={"kind": "inline", "rows": [{"id": "INC-1", "title": "A"}, {"id": "INC-2", "title": "B"}]},
        team_id=TEAM_ID, key_column="id", title_column="title",
    )
    monkeypatch.setattr(mod, "_call_linear_api", fake_call)
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], resources={"linear_resource": FakeLinearResource()})
    assert result.success
    assert len(calls["create"]) == 2


def test_issue_create_success_false_recorded_as_error(mod, monkeypatch):
    def _fake_call(resource, query, variables=None):
        variables = variables or {}
        if "issues(filter" in query:
            return {"issues": {"nodes": []}}
        if "issueCreate" in query:
            return {"issueCreate": {"success": False, "issue": None}}
        raise AssertionError(f"unexpected query:\n{query}")

    monkeypatch.setattr(mod, "_call_linear_api", _fake_call)
    df = pd.DataFrame({"id": ["INC-1"], "title": ["A"]})
    component = mod.LinearIssueUpsertComponent(
        asset_name="linear_out", upstream_asset_key="upstream_issues",
        team_id=TEAM_ID, key_column="id", title_column="title",
    )
    upstream_asset = make_upstream_asset("upstream_issues", df)
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def, upstream_asset], resources={"linear_resource": FakeLinearResource()})
    assert result.success  # component swallows per-row errors into metadata
    out = metadata_for(result, "linear_out")
    assert out["rows_errored"] == 1
    assert out["rows_created"] == 0


def test_api_exception_recorded_as_error_not_raised(mod, monkeypatch):
    def _fake_call(resource, query, variables=None):
        if "issues(filter" in query:
            return {"issues": {"nodes": []}}
        if "issueCreate" in query:
            raise RuntimeError("Linear 500")
        raise AssertionError(f"unexpected query:\n{query}")

    monkeypatch.setattr(mod, "_call_linear_api", _fake_call)
    df = pd.DataFrame({"id": ["INC-1"], "title": ["A"]})
    component = mod.LinearIssueUpsertComponent(
        asset_name="linear_out", upstream_asset_key="upstream_issues",
        team_id=TEAM_ID, key_column="id", title_column="title",
    )
    upstream_asset = make_upstream_asset("upstream_issues", df)
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def, upstream_asset], resources={"linear_resource": FakeLinearResource()})
    assert result.success
    out = metadata_for(result, "linear_out")
    assert out["rows_errored"] == 1
    assert "Linear 500" in out["first_errors"][0]
