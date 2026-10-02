"""Committed regression tests for XeroContactUpsertComponent.

The real Xero API is never called here -- `_xero_lookup_contact` and
`_xero_write_contact` (the two external, paid-API boundaries) are
monkeypatched wholesale, while dual source resolution, fields_map
application, lookup_field validation, the lookup-then-create-or-update
branching, and metadata are all exercised for real.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import FakeXeroResource, load_component_module, make_upstream_asset, metadata_for


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize(component, upstream_df, resource=None):
    resource = resource or FakeXeroResource()
    upstream_asset = make_upstream_asset("upstream_customers", upstream_df)
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    return dg.materialize([asset_def, upstream_asset], resources={"xero": resource})


# --- validation ------------------------------------------------------------

def test_neither_upstream_nor_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.XeroContactUpsertComponent(
            asset_name="x",
            fields_map={"customer_name": "Name"},
        ).build_defs(context=None)


def test_both_upstream_and_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.XeroContactUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            source={"kind": "inline", "rows": []},
            fields_map={"customer_name": "Name"},
        ).build_defs(context=None)


def test_lookup_field_not_in_fields_map_raises(mod):
    with pytest.raises(ValueError, match="not in fields_map values"):
        mod.XeroContactUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            lookup_field="Name",
            fields_map={"email": "EmailAddress"},
        ).build_defs(context=None)


def test_default_lookup_field_is_name(mod):
    component = mod.XeroContactUpsertComponent(
        asset_name="x",
        upstream_asset_key="foo",
        fields_map={"customer_name": "Name"},
    )
    assert component.lookup_field == "Name"
    component.build_defs(context=None)


def test_missing_upstream_columns_raises(mod):
    df = pd.DataFrame({"customer_name": ["Acme"]})
    component = mod.XeroContactUpsertComponent(
        asset_name="xero_contact_upsert_out",
        upstream_asset_key="upstream_customers",
        fields_map={"customer_name": "Name", "email": "EmailAddress"},
    )
    with pytest.raises(Exception):
        _materialize(component, df)


# --- lookup-then-write branching (the core create-vs-update mechanic) ------

def test_lookup_miss_creates_without_contact_id(mod, monkeypatch):
    lookup_calls = []
    write_calls = []

    def _fake_lookup(resource, lookup_field, lookup_value):
        lookup_calls.append((lookup_field, lookup_value))
        return None  # no existing match

    def _fake_write(resource, contact_id, body):
        write_calls.append((contact_id, body))
        return {"ContactID": "new-id-1"}

    monkeypatch.setattr(mod, "_xero_lookup_contact", _fake_lookup)
    monkeypatch.setattr(mod, "_xero_write_contact", _fake_write)

    df = pd.DataFrame({"customer_name": ["Acme"]})
    component = mod.XeroContactUpsertComponent(
        asset_name="xero_contact_upsert_out",
        upstream_asset_key="upstream_customers",
        fields_map={"customer_name": "Name"},
    )
    result = _materialize(component, df)
    assert result.success

    assert lookup_calls == [("Name", "Acme")]
    assert write_calls[0][0] is None
    assert write_calls[0][1] == {"Name": "Acme"}

    out = metadata_for(result, "xero_contact_upsert_out")
    assert out["rows_created"] == 1
    assert out["rows_updated"] == 0
    assert out["api_requests"] == 2  # 1 lookup + 1 write


def test_lookup_hit_updates_with_contact_id(mod, monkeypatch):
    write_calls = []

    def _fake_lookup(resource, lookup_field, lookup_value):
        return "existing-id-42"

    def _fake_write(resource, contact_id, body):
        write_calls.append((contact_id, body))
        return {"ContactID": contact_id}

    monkeypatch.setattr(mod, "_xero_lookup_contact", _fake_lookup)
    monkeypatch.setattr(mod, "_xero_write_contact", _fake_write)

    df = pd.DataFrame({"customer_name": ["Acme Updated"]})
    component = mod.XeroContactUpsertComponent(
        asset_name="xero_contact_upsert_out",
        upstream_asset_key="upstream_customers",
        fields_map={"customer_name": "Name"},
    )
    result = _materialize(component, df)
    assert result.success

    assert write_calls[0][0] == "existing-id-42"
    assert write_calls[0][1] == {"Name": "Acme Updated"}

    out = metadata_for(result, "xero_contact_upsert_out")
    assert out["rows_created"] == 0
    assert out["rows_updated"] == 1


def test_mixed_create_and_update_counted_separately(mod, monkeypatch):
    def _fake_lookup(resource, lookup_field, lookup_value):
        return "id-1" if lookup_value == "Existing Co" else None

    def _fake_write(resource, contact_id, body):
        return {"ContactID": contact_id or "new"}

    monkeypatch.setattr(mod, "_xero_lookup_contact", _fake_lookup)
    monkeypatch.setattr(mod, "_xero_write_contact", _fake_write)

    df = pd.DataFrame({"customer_name": ["New Co", "Existing Co"]})
    component = mod.XeroContactUpsertComponent(
        asset_name="xero_contact_upsert_out",
        upstream_asset_key="upstream_customers",
        fields_map={"customer_name": "Name"},
    )
    result = _materialize(component, df)
    assert result.success
    out = metadata_for(result, "xero_contact_upsert_out")
    assert out["rows_created"] == 1
    assert out["rows_updated"] == 1
    assert out["rows_upserted"] == 2


# --- multi-field fields_map --------------------------------------------------

def test_fields_map_builds_flat_body(mod, monkeypatch):
    write_calls = []
    monkeypatch.setattr(mod, "_xero_lookup_contact", lambda *a, **k: None)

    def _fake_write(resource, contact_id, body):
        write_calls.append(body)
        return {"ContactID": "1"}

    monkeypatch.setattr(mod, "_xero_write_contact", _fake_write)

    df = pd.DataFrame({"customer_name": ["Acme"], "email": ["a@b.com"], "contact_number": ["CUST-1"]})
    component = mod.XeroContactUpsertComponent(
        asset_name="xero_contact_upsert_out",
        upstream_asset_key="upstream_customers",
        fields_map={
            "customer_name": "Name",
            "email": "EmailAddress",
            "contact_number": "ContactNumber",
        },
    )
    result = _materialize(component, df)
    assert result.success
    assert write_calls[0] == {
        "Name": "Acme",
        "EmailAddress": "a@b.com",
        "ContactNumber": "CUST-1",
    }


# --- missing lookup value / empty upstream ----------------------------------

def test_rows_missing_lookup_value_skipped_and_counted(mod, monkeypatch):
    calls = []

    def _fake_lookup(resource, lookup_field, lookup_value):
        calls.append(lookup_value)
        return None

    monkeypatch.setattr(mod, "_xero_lookup_contact", _fake_lookup)
    monkeypatch.setattr(mod, "_xero_write_contact", lambda *a, **k: {"ContactID": "1"})

    df = pd.DataFrame({"customer_name": ["Acme", None]})
    component = mod.XeroContactUpsertComponent(
        asset_name="xero_contact_upsert_out",
        upstream_asset_key="upstream_customers",
        fields_map={"customer_name": "Name"},
    )
    result = _materialize(component, df)
    assert result.success
    assert calls == ["Acme"]
    out = metadata_for(result, "xero_contact_upsert_out")
    assert out["rows_skipped_no_key"] == 1


def test_empty_upstream_short_circuits_without_api_calls(mod, monkeypatch):
    called = []
    monkeypatch.setattr(mod, "_xero_lookup_contact", lambda *a, **k: called.append(1))
    monkeypatch.setattr(mod, "_xero_write_contact", lambda *a, **k: called.append(1))

    df = pd.DataFrame({"customer_name": []})
    component = mod.XeroContactUpsertComponent(
        asset_name="xero_contact_upsert_out",
        upstream_asset_key="upstream_customers",
        fields_map={"customer_name": "Name"},
    )
    result = _materialize(component, df)
    assert result.success
    assert called == []
    out = metadata_for(result, "xero_contact_upsert_out")
    assert out["rows_upserted"] == 0


def test_batch_size_caps_rows(mod, monkeypatch):
    monkeypatch.setattr(mod, "_xero_lookup_contact", lambda *a, **k: None)
    monkeypatch.setattr(mod, "_xero_write_contact", lambda *a, **k: {"ContactID": "x"})

    df = pd.DataFrame({"customer_name": [f"Co {i}" for i in range(10)]})
    component = mod.XeroContactUpsertComponent(
        asset_name="xero_contact_upsert_out",
        upstream_asset_key="upstream_customers",
        fields_map={"customer_name": "Name"},
        batch_size=3,
    )
    result = _materialize(component, df)
    assert result.success
    out = metadata_for(result, "xero_contact_upsert_out")
    assert out["rows_upserted"] == 3


def test_source_inline_mode(mod, monkeypatch):
    monkeypatch.setattr(mod, "_xero_lookup_contact", lambda *a, **k: None)
    monkeypatch.setattr(mod, "_xero_write_contact", lambda *a, **k: {"ContactID": "x"})

    component = mod.XeroContactUpsertComponent(
        asset_name="xero_contact_upsert_out",
        source={"kind": "inline", "rows": [{"customer_name": "A"}, {"customer_name": "B"}]},
        fields_map={"customer_name": "Name"},
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], resources={"xero": FakeXeroResource()})
    assert result.success
    out = metadata_for(result, "xero_contact_upsert_out")
    assert out["rows_upserted"] == 2


def test_lookup_exception_recorded_as_error_not_raised(mod, monkeypatch):
    def _fake_lookup(resource, lookup_field, lookup_value):
        raise RuntimeError("Xero 500")

    monkeypatch.setattr(mod, "_xero_lookup_contact", _fake_lookup)
    monkeypatch.setattr(mod, "_xero_write_contact", lambda *a, **k: {"ContactID": "x"})

    df = pd.DataFrame({"customer_name": ["Acme"]})
    component = mod.XeroContactUpsertComponent(
        asset_name="xero_contact_upsert_out",
        upstream_asset_key="upstream_customers",
        fields_map={"customer_name": "Name"},
    )
    result = _materialize(component, df)
    assert result.success
    out = metadata_for(result, "xero_contact_upsert_out")
    assert out["rows_errored"] == 1
    assert "Xero 500" in out["first_errors"][0]


def test_write_exception_e_g_duplicate_name_recorded_as_error(mod, monkeypatch):
    monkeypatch.setattr(mod, "_xero_lookup_contact", lambda *a, **k: None)

    def _fake_write(resource, contact_id, body):
        raise RuntimeError("The contact name is already assigned to another contact. (400)")

    monkeypatch.setattr(mod, "_xero_write_contact", _fake_write)

    df = pd.DataFrame({"customer_name": ["Acme"]})
    component = mod.XeroContactUpsertComponent(
        asset_name="xero_contact_upsert_out",
        upstream_asset_key="upstream_customers",
        fields_map={"customer_name": "Name"},
    )
    result = _materialize(component, df)
    assert result.success
    out = metadata_for(result, "xero_contact_upsert_out")
    assert out["rows_errored"] == 1
    assert "already assigned" in out["first_errors"][0]
