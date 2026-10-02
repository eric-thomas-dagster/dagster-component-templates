"""Committed regression tests for RecurlyAccountUpsertComponent.

The real Recurly API is never called here -- `_recurly_lookup_account`
and `_recurly_write_account` (the two external, paid-API boundaries) are
monkeypatched wholesale, while dual source resolution, fields_map
application, lookup_field validation, the existence-check-then-create-
or-update branching, and metadata are all exercised for real.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import FakeRecurlyResource, load_component_module, make_upstream_asset, metadata_for


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize(component, upstream_df, resource=None):
    resource = resource or FakeRecurlyResource()
    upstream_asset = make_upstream_asset("upstream_customers", upstream_df)
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    return dg.materialize([asset_def, upstream_asset], resources={"recurly": resource})


# --- validation ------------------------------------------------------------

def test_neither_upstream_nor_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.RecurlyAccountUpsertComponent(
            asset_name="x",
            fields_map={"customer_id": "code"},
        ).build_defs(context=None)


def test_both_upstream_and_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.RecurlyAccountUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            source={"kind": "inline", "rows": []},
            fields_map={"customer_id": "code"},
        ).build_defs(context=None)


def test_lookup_field_not_in_fields_map_raises(mod):
    with pytest.raises(ValueError, match="not in fields_map values"):
        mod.RecurlyAccountUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            lookup_field="code",
            fields_map={"email": "email"},
        ).build_defs(context=None)


def test_default_lookup_field_is_code(mod):
    component = mod.RecurlyAccountUpsertComponent(
        asset_name="x",
        upstream_asset_key="foo",
        fields_map={"customer_id": "code"},
    )
    assert component.lookup_field == "code"
    component.build_defs(context=None)


def test_missing_upstream_columns_raises(mod):
    df = pd.DataFrame({"customer_id": ["acme"]})
    component = mod.RecurlyAccountUpsertComponent(
        asset_name="recurly_account_upsert_out",
        upstream_asset_key="upstream_customers",
        fields_map={"customer_id": "code", "email": "email"},
    )
    with pytest.raises(Exception):
        _materialize(component, df)


# --- existence-check-then-write branching (the core create-vs-update mechanic) --

def test_no_existing_account_creates(mod, monkeypatch):
    lookup_calls = []
    write_calls = []

    def _fake_lookup(resource, lookup_value):
        lookup_calls.append(lookup_value)
        return False  # 404 -- no existing account

    def _fake_write(resource, lookup_value, exists, body):
        write_calls.append((lookup_value, exists, body))
        return {"id": "new-id-1", "action": "created"}

    monkeypatch.setattr(mod, "_recurly_lookup_account", _fake_lookup)
    monkeypatch.setattr(mod, "_recurly_write_account", _fake_write)

    df = pd.DataFrame({"customer_id": ["acme"]})
    component = mod.RecurlyAccountUpsertComponent(
        asset_name="recurly_account_upsert_out",
        upstream_asset_key="upstream_customers",
        fields_map={"customer_id": "code"},
    )
    result = _materialize(component, df)
    assert result.success

    assert lookup_calls == ["acme"]
    assert write_calls[0] == ("acme", False, {"code": "acme"})

    out = metadata_for(result, "recurly_account_upsert_out")
    assert out["rows_created"] == 1
    assert out["rows_updated"] == 0
    assert out["api_requests"] == 2  # 1 lookup + 1 write


def test_existing_account_updates(mod, monkeypatch):
    write_calls = []

    def _fake_lookup(resource, lookup_value):
        return True  # account exists

    def _fake_write(resource, lookup_value, exists, body):
        write_calls.append((lookup_value, exists, body))
        return {"id": "existing-id", "action": "updated"}

    monkeypatch.setattr(mod, "_recurly_lookup_account", _fake_lookup)
    monkeypatch.setattr(mod, "_recurly_write_account", _fake_write)

    df = pd.DataFrame({"customer_id": ["acme"]})
    component = mod.RecurlyAccountUpsertComponent(
        asset_name="recurly_account_upsert_out",
        upstream_asset_key="upstream_customers",
        fields_map={"customer_id": "code"},
    )
    result = _materialize(component, df)
    assert result.success

    assert write_calls[0][1] is True

    out = metadata_for(result, "recurly_account_upsert_out")
    assert out["rows_created"] == 0
    assert out["rows_updated"] == 1


def test_mixed_create_and_update_counted_separately(mod, monkeypatch):
    def _fake_lookup(resource, lookup_value):
        return lookup_value == "existing-co"

    def _fake_write(resource, lookup_value, exists, body):
        return {"id": lookup_value, "action": "updated" if exists else "created"}

    monkeypatch.setattr(mod, "_recurly_lookup_account", _fake_lookup)
    monkeypatch.setattr(mod, "_recurly_write_account", _fake_write)

    df = pd.DataFrame({"customer_id": ["new-co", "existing-co"]})
    component = mod.RecurlyAccountUpsertComponent(
        asset_name="recurly_account_upsert_out",
        upstream_asset_key="upstream_customers",
        fields_map={"customer_id": "code"},
    )
    result = _materialize(component, df)
    assert result.success
    out = metadata_for(result, "recurly_account_upsert_out")
    assert out["rows_created"] == 1
    assert out["rows_updated"] == 1
    assert out["rows_upserted"] == 2


# --- multi-field fields_map --------------------------------------------------

def test_fields_map_builds_flat_body(mod, monkeypatch):
    write_calls = []
    monkeypatch.setattr(mod, "_recurly_lookup_account", lambda *a, **k: False)

    def _fake_write(resource, lookup_value, exists, body):
        write_calls.append(body)
        return {"id": "1", "action": "created"}

    monkeypatch.setattr(mod, "_recurly_write_account", _fake_write)

    df = pd.DataFrame({"customer_id": ["acme"], "email": ["a@b.com"], "company_name": ["Acme Inc"]})
    component = mod.RecurlyAccountUpsertComponent(
        asset_name="recurly_account_upsert_out",
        upstream_asset_key="upstream_customers",
        fields_map={
            "customer_id": "code",
            "email": "email",
            "company_name": "company",
        },
    )
    result = _materialize(component, df)
    assert result.success
    assert write_calls[0] == {
        "code": "acme",
        "email": "a@b.com",
        "company": "Acme Inc",
    }


# --- missing lookup value / empty upstream ----------------------------------

def test_rows_missing_lookup_value_skipped_and_counted(mod, monkeypatch):
    calls = []

    def _fake_lookup(resource, lookup_value):
        calls.append(lookup_value)
        return False

    monkeypatch.setattr(mod, "_recurly_lookup_account", _fake_lookup)
    monkeypatch.setattr(mod, "_recurly_write_account", lambda *a, **k: {"id": "1", "action": "created"})

    df = pd.DataFrame({"customer_id": ["acme", None]})
    component = mod.RecurlyAccountUpsertComponent(
        asset_name="recurly_account_upsert_out",
        upstream_asset_key="upstream_customers",
        fields_map={"customer_id": "code"},
    )
    result = _materialize(component, df)
    assert result.success
    assert calls == ["acme"]
    out = metadata_for(result, "recurly_account_upsert_out")
    assert out["rows_skipped_no_key"] == 1


def test_empty_upstream_short_circuits_without_api_calls(mod, monkeypatch):
    called = []
    monkeypatch.setattr(mod, "_recurly_lookup_account", lambda *a, **k: called.append(1))
    monkeypatch.setattr(mod, "_recurly_write_account", lambda *a, **k: called.append(1))

    df = pd.DataFrame({"customer_id": []})
    component = mod.RecurlyAccountUpsertComponent(
        asset_name="recurly_account_upsert_out",
        upstream_asset_key="upstream_customers",
        fields_map={"customer_id": "code"},
    )
    result = _materialize(component, df)
    assert result.success
    assert called == []
    out = metadata_for(result, "recurly_account_upsert_out")
    assert out["rows_upserted"] == 0


def test_batch_size_caps_rows(mod, monkeypatch):
    monkeypatch.setattr(mod, "_recurly_lookup_account", lambda *a, **k: False)
    monkeypatch.setattr(mod, "_recurly_write_account", lambda *a, **k: {"id": "x", "action": "created"})

    df = pd.DataFrame({"customer_id": [f"co-{i}" for i in range(10)]})
    component = mod.RecurlyAccountUpsertComponent(
        asset_name="recurly_account_upsert_out",
        upstream_asset_key="upstream_customers",
        fields_map={"customer_id": "code"},
        batch_size=3,
    )
    result = _materialize(component, df)
    assert result.success
    out = metadata_for(result, "recurly_account_upsert_out")
    assert out["rows_upserted"] == 3


def test_source_inline_mode(mod, monkeypatch):
    monkeypatch.setattr(mod, "_recurly_lookup_account", lambda *a, **k: False)
    monkeypatch.setattr(mod, "_recurly_write_account", lambda *a, **k: {"id": "x", "action": "created"})

    component = mod.RecurlyAccountUpsertComponent(
        asset_name="recurly_account_upsert_out",
        source={"kind": "inline", "rows": [{"customer_id": "a"}, {"customer_id": "b"}]},
        fields_map={"customer_id": "code"},
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], resources={"recurly": FakeRecurlyResource()})
    assert result.success
    out = metadata_for(result, "recurly_account_upsert_out")
    assert out["rows_upserted"] == 2


def test_lookup_exception_recorded_as_error_not_raised(mod, monkeypatch):
    def _fake_lookup(resource, lookup_value):
        raise RuntimeError("Recurly 500")

    monkeypatch.setattr(mod, "_recurly_lookup_account", _fake_lookup)
    monkeypatch.setattr(mod, "_recurly_write_account", lambda *a, **k: {"id": "x", "action": "created"})

    df = pd.DataFrame({"customer_id": ["acme"]})
    component = mod.RecurlyAccountUpsertComponent(
        asset_name="recurly_account_upsert_out",
        upstream_asset_key="upstream_customers",
        fields_map={"customer_id": "code"},
    )
    result = _materialize(component, df)
    assert result.success
    out = metadata_for(result, "recurly_account_upsert_out")
    assert out["rows_errored"] == 1
    assert "Recurly 500" in out["first_errors"][0]


def test_write_exception_recorded_as_error(mod, monkeypatch):
    monkeypatch.setattr(mod, "_recurly_lookup_account", lambda *a, **k: False)

    def _fake_write(resource, lookup_value, exists, body):
        raise RuntimeError("Invalid account code format (422)")

    monkeypatch.setattr(mod, "_recurly_write_account", _fake_write)

    df = pd.DataFrame({"customer_id": ["bad/code"]})
    component = mod.RecurlyAccountUpsertComponent(
        asset_name="recurly_account_upsert_out",
        upstream_asset_key="upstream_customers",
        fields_map={"customer_id": "code"},
    )
    result = _materialize(component, df)
    assert result.success
    out = metadata_for(result, "recurly_account_upsert_out")
    assert out["rows_errored"] == 1
    assert "Invalid account code" in out["first_errors"][0]
