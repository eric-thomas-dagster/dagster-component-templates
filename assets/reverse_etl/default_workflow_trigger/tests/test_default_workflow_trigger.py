"""Committed regression tests for DefaultWorkflowTriggerComponent.

No real network/`requests` calls are made here -- `_call_fire_trigger`
(the one external, paid-API boundary) is monkeypatched wholesale, while
dual source resolution, the email-vs-responses row mapping (the part that
differs from a naive flat-dict assumption), validation, row skipping, and
metadata are all exercised for real.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import FakeDefaultResource, load_component_module, make_upstream_asset, metadata_for


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def recorded_calls(mod, monkeypatch):
    calls = []

    def _fake_call(resource, trigger_id, email, responses):
        calls.append({"trigger_id": trigger_id, "email": email, "responses": responses})
        return {"executionId": "exec-fake", "outcome": {"type": "none"}}

    monkeypatch.setattr(mod, "_call_fire_trigger", _fake_call)
    return calls


def _materialize(component, upstream_df, resource=None):
    upstream_asset = make_upstream_asset("upstream_leads", upstream_df)
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    return dg.materialize(
        [asset_def, upstream_asset],
        resources={"default_resource": resource or FakeDefaultResource()},
    )


# --- validation --------------------------------------------------------------

def test_neither_upstream_nor_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.DefaultWorkflowTriggerComponent(
            asset_name="x",
            trigger_id="trig-1",
            email_column="email",
        ).build_defs(context=None)


def test_both_upstream_and_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.DefaultWorkflowTriggerComponent(
            asset_name="x",
            upstream_asset_key="foo",
            source={"kind": "inline", "rows": []},
            trigger_id="trig-1",
            email_column="email",
        ).build_defs(context=None)


def test_missing_required_columns_raises_failure(mod):
    component = mod.DefaultWorkflowTriggerComponent(
        asset_name="default_trigger_out",
        upstream_asset_key="upstream_leads",
        trigger_id="trig-1",
        email_column="email",
        field_mapping={"company": "Company"},
    )
    df = pd.DataFrame({"email": ["jane@example.com"]})  # missing 'company'
    with pytest.raises(dg.Failure, match="Columns not in upstream"):
        _materialize(component, df)


# --- the real, confirmed request shape: email is NOT folded into responses --

def test_fires_one_trigger_per_row_with_email_separate_from_responses(mod, recorded_calls):
    df = pd.DataFrame({
        "email": ["jane@example.com", "bob@example.com"],
        "company": ["Acme", "Globex"],
        "deal_size": [5000, 12000],
    })
    component = mod.DefaultWorkflowTriggerComponent(
        asset_name="default_trigger_out",
        upstream_asset_key="upstream_leads",
        trigger_id="trig-1",
        email_column="email",
        field_mapping={"company": "Company", "deal_size": "Deal Size"},
    )
    result = _materialize(component, df)
    assert result.success

    assert len(recorded_calls) == 2
    first = recorded_calls[0]
    assert first["trigger_id"] == "trig-1"
    assert first["email"] == "jane@example.com"
    # email must never leak into the responses dict -- it's a separate
    # top-level field in Default's real request shape.
    assert "email" not in first["responses"]
    assert first["responses"] == {"Company": "Acme", "Deal Size": 5000}

    out = metadata_for(result, "default_trigger_out")
    assert out["rows_fired"] == 2
    assert out["rows_errored"] == 0
    assert out["rows_skipped_no_email"] == 0
    assert out["trigger_id"] == "trig-1"


def test_rows_with_no_email_are_skipped_not_fired(mod, recorded_calls):
    df = pd.DataFrame({
        "email": ["jane@example.com", None, ""],
        "company": ["Acme", "Globex", "Initech"],
    })
    component = mod.DefaultWorkflowTriggerComponent(
        asset_name="default_trigger_out",
        upstream_asset_key="upstream_leads",
        trigger_id="trig-1",
        email_column="email",
        field_mapping={"company": "Company"},
    )
    result = _materialize(component, df)
    assert result.success
    assert len(recorded_calls) == 1
    out = metadata_for(result, "default_trigger_out")
    assert out["rows_fired"] == 1
    assert out["rows_skipped_no_email"] == 2


def test_field_mapping_optional_empty_still_fires(mod, recorded_calls):
    df = pd.DataFrame({"email": ["jane@example.com"]})
    component = mod.DefaultWorkflowTriggerComponent(
        asset_name="default_trigger_out",
        upstream_asset_key="upstream_leads",
        trigger_id="trig-1",
        email_column="email",
    )
    result = _materialize(component, df)
    assert result.success
    assert recorded_calls[0]["responses"] == {}


def test_null_mapped_values_are_dropped_from_responses(mod, recorded_calls):
    df = pd.DataFrame({
        "email": ["jane@example.com"],
        "company": [None],
        "deal_size": [5000],
    })
    component = mod.DefaultWorkflowTriggerComponent(
        asset_name="default_trigger_out",
        upstream_asset_key="upstream_leads",
        trigger_id="trig-1",
        email_column="email",
        field_mapping={"company": "Company", "deal_size": "Deal Size"},
    )
    result = _materialize(component, df)
    assert result.success
    assert recorded_calls[0]["responses"] == {"Deal Size": 5000}


# --- errors are counted, not fatal to the run -------------------------------

def test_api_errors_are_counted_not_raised(mod, monkeypatch):
    def _failing_call(resource, trigger_id, email, responses):
        if email == "bob@example.com":
            raise RuntimeError("Default API error: HTTP 429 (RATE_LIMITED)")
        return {"executionId": "exec-ok", "outcome": {"type": "none"}}

    monkeypatch.setattr(mod, "_call_fire_trigger", _failing_call)

    df = pd.DataFrame({"email": ["jane@example.com", "bob@example.com"]})
    component = mod.DefaultWorkflowTriggerComponent(
        asset_name="default_trigger_out",
        upstream_asset_key="upstream_leads",
        trigger_id="trig-1",
        email_column="email",
    )
    result = _materialize(component, df)
    assert result.success  # errors are tallied, the asset still succeeds
    out = metadata_for(result, "default_trigger_out")
    assert out["rows_fired"] == 1
    assert out["rows_errored"] == 1
    assert "bob@example.com" in out["first_errors"][0]


# --- safety cap / empty upstream --------------------------------------------

def test_max_rows_caps_upstream(mod, recorded_calls):
    df = pd.DataFrame({"email": [f"user{i}@example.com" for i in range(5)]})
    component = mod.DefaultWorkflowTriggerComponent(
        asset_name="default_trigger_out",
        upstream_asset_key="upstream_leads",
        trigger_id="trig-1",
        email_column="email",
        max_rows=2,
    )
    result = _materialize(component, df)
    assert result.success
    assert len(recorded_calls) == 2


def test_empty_upstream_makes_no_calls(mod, recorded_calls):
    df = pd.DataFrame({"email": []})
    component = mod.DefaultWorkflowTriggerComponent(
        asset_name="default_trigger_out",
        upstream_asset_key="upstream_leads",
        trigger_id="trig-1",
        email_column="email",
    )
    result = _materialize(component, df)
    assert result.success
    assert recorded_calls == []
    out = metadata_for(result, "default_trigger_out")
    assert out["rows_fired"] == 0


# --- source: inline mode -----------------------------------------------------

def test_source_inline_mode(mod, recorded_calls):
    component = mod.DefaultWorkflowTriggerComponent(
        asset_name="default_trigger_out",
        source={"kind": "inline", "rows": [{"email": "a@b.com"}, {"email": "c@d.com"}]},
        trigger_id="trig-1",
        email_column="email",
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], resources={"default_resource": FakeDefaultResource()})
    assert result.success
    out = metadata_for(result, "default_trigger_out")
    assert out["rows_fired"] == 2
