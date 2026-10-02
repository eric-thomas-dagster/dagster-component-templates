"""Committed regression tests for ChargebeeCustomerUpsertComponent.

The real Chargebee API is never called here -- `_chargebee_lookup_customer`
and `_chargebee_write_customer` (the two external, paid-API boundaries)
are monkeypatched wholesale, while dual source resolution, fields_map
application, lookup_field validation, the lookup-then-create-or-update
branching, and metadata are all exercised for real.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import FakeChargebeeResource, load_component_module, make_upstream_asset, metadata_for


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize(component, upstream_df, resource=None):
    resource = resource or FakeChargebeeResource()
    upstream_asset = make_upstream_asset("upstream_customers", upstream_df)
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    return dg.materialize([asset_def, upstream_asset], resources={"chargebee": resource})


# --- validation ------------------------------------------------------------

def test_neither_upstream_nor_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.ChargebeeCustomerUpsertComponent(
            asset_name="x",
            fields_map={"email": "email"},
        ).build_defs(context=None)


def test_both_upstream_and_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.ChargebeeCustomerUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            source={"kind": "inline", "rows": []},
            fields_map={"email": "email"},
        ).build_defs(context=None)


def test_lookup_field_not_in_fields_map_raises(mod):
    with pytest.raises(ValueError, match="not in fields_map values"):
        mod.ChargebeeCustomerUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            lookup_field="email",
            fields_map={"first_name": "first_name"},
        ).build_defs(context=None)


def test_default_lookup_field_is_email(mod):
    component = mod.ChargebeeCustomerUpsertComponent(
        asset_name="x",
        upstream_asset_key="foo",
        fields_map={"email": "email"},
    )
    assert component.lookup_field == "email"
    component.build_defs(context=None)


def test_missing_upstream_columns_raises(mod):
    df = pd.DataFrame({"email": ["a@b.com"]})
    component = mod.ChargebeeCustomerUpsertComponent(
        asset_name="chargebee_customer_upsert_out",
        upstream_asset_key="upstream_customers",
        fields_map={"email": "email", "first_name": "first_name"},
    )
    with pytest.raises(Exception):
        _materialize(component, df)


# --- lookup-then-write branching (the core create-vs-update mechanic) ------

def test_lookup_miss_creates(mod, monkeypatch):
    lookup_calls = []
    write_calls = []

    def _fake_lookup(resource, lookup_field, lookup_value):
        lookup_calls.append((lookup_field, lookup_value))
        return None  # no existing match

    def _fake_write(resource, customer_id, body):
        write_calls.append((customer_id, body))
        return {"id": "new-id-1", "action": "created"}

    monkeypatch.setattr(mod, "_chargebee_lookup_customer", _fake_lookup)
    monkeypatch.setattr(mod, "_chargebee_write_customer", _fake_write)

    df = pd.DataFrame({"email": ["a@b.com"]})
    component = mod.ChargebeeCustomerUpsertComponent(
        asset_name="chargebee_customer_upsert_out",
        upstream_asset_key="upstream_customers",
        fields_map={"email": "email"},
    )
    result = _materialize(component, df)
    assert result.success

    assert lookup_calls == [("email", "a@b.com")]
    assert write_calls[0][0] is None
    assert write_calls[0][1] == {"email": "a@b.com"}

    out = metadata_for(result, "chargebee_customer_upsert_out")
    assert out["rows_created"] == 1
    assert out["rows_updated"] == 0
    assert out["api_requests"] == 2  # 1 lookup + 1 write


def test_lookup_hit_updates(mod, monkeypatch):
    write_calls = []

    def _fake_lookup(resource, lookup_field, lookup_value):
        return "existing-id-42"

    def _fake_write(resource, customer_id, body):
        write_calls.append((customer_id, body))
        return {"id": customer_id, "action": "updated"}

    monkeypatch.setattr(mod, "_chargebee_lookup_customer", _fake_lookup)
    monkeypatch.setattr(mod, "_chargebee_write_customer", _fake_write)

    df = pd.DataFrame({"email": ["a@b.com"]})
    component = mod.ChargebeeCustomerUpsertComponent(
        asset_name="chargebee_customer_upsert_out",
        upstream_asset_key="upstream_customers",
        fields_map={"email": "email"},
    )
    result = _materialize(component, df)
    assert result.success

    assert write_calls[0][0] == "existing-id-42"

    out = metadata_for(result, "chargebee_customer_upsert_out")
    assert out["rows_created"] == 0
    assert out["rows_updated"] == 1


def test_mixed_create_and_update_counted_separately(mod, monkeypatch):
    def _fake_lookup(resource, lookup_field, lookup_value):
        return "id-1" if lookup_value == "existing@b.com" else None

    def _fake_write(resource, customer_id, body):
        return {"id": customer_id or "new", "action": "updated" if customer_id else "created"}

    monkeypatch.setattr(mod, "_chargebee_lookup_customer", _fake_lookup)
    monkeypatch.setattr(mod, "_chargebee_write_customer", _fake_write)

    df = pd.DataFrame({"email": ["new@b.com", "existing@b.com"]})
    component = mod.ChargebeeCustomerUpsertComponent(
        asset_name="chargebee_customer_upsert_out",
        upstream_asset_key="upstream_customers",
        fields_map={"email": "email"},
    )
    result = _materialize(component, df)
    assert result.success
    out = metadata_for(result, "chargebee_customer_upsert_out")
    assert out["rows_created"] == 1
    assert out["rows_updated"] == 1
    assert out["rows_upserted"] == 2


# --- lookup_field: id mode ----------------------------------------------------

def test_lookup_field_id_mode(mod, monkeypatch):
    lookup_calls = []

    def _fake_lookup(resource, lookup_field, lookup_value):
        lookup_calls.append((lookup_field, lookup_value))
        return None

    monkeypatch.setattr(mod, "_chargebee_lookup_customer", _fake_lookup)
    monkeypatch.setattr(mod, "_chargebee_write_customer", lambda *a, **k: {"id": "x", "action": "created"})

    df = pd.DataFrame({"customer_id": ["cust_123"]})
    component = mod.ChargebeeCustomerUpsertComponent(
        asset_name="chargebee_customer_upsert_out",
        upstream_asset_key="upstream_customers",
        lookup_field="id",
        fields_map={"customer_id": "id"},
    )
    result = _materialize(component, df)
    assert result.success
    assert lookup_calls == [("id", "cust_123")]


# --- multi-field fields_map --------------------------------------------------

def test_fields_map_builds_flat_body(mod, monkeypatch):
    write_calls = []
    monkeypatch.setattr(mod, "_chargebee_lookup_customer", lambda *a, **k: None)

    def _fake_write(resource, customer_id, body):
        write_calls.append(body)
        return {"id": "1", "action": "created"}

    monkeypatch.setattr(mod, "_chargebee_write_customer", _fake_write)

    df = pd.DataFrame({"email": ["a@b.com"], "first_name": ["Ada"], "company_name": ["Acme"]})
    component = mod.ChargebeeCustomerUpsertComponent(
        asset_name="chargebee_customer_upsert_out",
        upstream_asset_key="upstream_customers",
        fields_map={
            "email": "email",
            "first_name": "first_name",
            "company_name": "company",
        },
    )
    result = _materialize(component, df)
    assert result.success
    assert write_calls[0] == {
        "email": "a@b.com",
        "first_name": "Ada",
        "company": "Acme",
    }


# --- missing lookup value / empty upstream ----------------------------------

def test_rows_missing_lookup_value_skipped_and_counted(mod, monkeypatch):
    calls = []

    def _fake_lookup(resource, lookup_field, lookup_value):
        calls.append(lookup_value)
        return None

    monkeypatch.setattr(mod, "_chargebee_lookup_customer", _fake_lookup)
    monkeypatch.setattr(mod, "_chargebee_write_customer", lambda *a, **k: {"id": "1", "action": "created"})

    df = pd.DataFrame({"email": ["a@b.com", None]})
    component = mod.ChargebeeCustomerUpsertComponent(
        asset_name="chargebee_customer_upsert_out",
        upstream_asset_key="upstream_customers",
        fields_map={"email": "email"},
    )
    result = _materialize(component, df)
    assert result.success
    assert calls == ["a@b.com"]
    out = metadata_for(result, "chargebee_customer_upsert_out")
    assert out["rows_skipped_no_key"] == 1


def test_empty_upstream_short_circuits_without_api_calls(mod, monkeypatch):
    called = []
    monkeypatch.setattr(mod, "_chargebee_lookup_customer", lambda *a, **k: called.append(1))
    monkeypatch.setattr(mod, "_chargebee_write_customer", lambda *a, **k: called.append(1))

    df = pd.DataFrame({"email": []})
    component = mod.ChargebeeCustomerUpsertComponent(
        asset_name="chargebee_customer_upsert_out",
        upstream_asset_key="upstream_customers",
        fields_map={"email": "email"},
    )
    result = _materialize(component, df)
    assert result.success
    assert called == []
    out = metadata_for(result, "chargebee_customer_upsert_out")
    assert out["rows_upserted"] == 0


def test_batch_size_caps_rows(mod, monkeypatch):
    monkeypatch.setattr(mod, "_chargebee_lookup_customer", lambda *a, **k: None)
    monkeypatch.setattr(mod, "_chargebee_write_customer", lambda *a, **k: {"id": "x", "action": "created"})

    df = pd.DataFrame({"email": [f"co{i}@b.com" for i in range(10)]})
    component = mod.ChargebeeCustomerUpsertComponent(
        asset_name="chargebee_customer_upsert_out",
        upstream_asset_key="upstream_customers",
        fields_map={"email": "email"},
        batch_size=3,
    )
    result = _materialize(component, df)
    assert result.success
    out = metadata_for(result, "chargebee_customer_upsert_out")
    assert out["rows_upserted"] == 3


def test_source_inline_mode(mod, monkeypatch):
    monkeypatch.setattr(mod, "_chargebee_lookup_customer", lambda *a, **k: None)
    monkeypatch.setattr(mod, "_chargebee_write_customer", lambda *a, **k: {"id": "x", "action": "created"})

    component = mod.ChargebeeCustomerUpsertComponent(
        asset_name="chargebee_customer_upsert_out",
        source={"kind": "inline", "rows": [{"email": "a@b.com"}, {"email": "c@d.com"}]},
        fields_map={"email": "email"},
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], resources={"chargebee": FakeChargebeeResource()})
    assert result.success
    out = metadata_for(result, "chargebee_customer_upsert_out")
    assert out["rows_upserted"] == 2


def test_lookup_exception_recorded_as_error_not_raised(mod, monkeypatch):
    def _fake_lookup(resource, lookup_field, lookup_value):
        raise RuntimeError("Chargebee 500")

    monkeypatch.setattr(mod, "_chargebee_lookup_customer", _fake_lookup)
    monkeypatch.setattr(mod, "_chargebee_write_customer", lambda *a, **k: {"id": "x", "action": "created"})

    df = pd.DataFrame({"email": ["a@b.com"]})
    component = mod.ChargebeeCustomerUpsertComponent(
        asset_name="chargebee_customer_upsert_out",
        upstream_asset_key="upstream_customers",
        fields_map={"email": "email"},
    )
    result = _materialize(component, df)
    assert result.success
    out = metadata_for(result, "chargebee_customer_upsert_out")
    assert out["rows_errored"] == 1
    assert "Chargebee 500" in out["first_errors"][0]


def test_write_exception_e_g_duplicate_id_recorded_as_error(mod, monkeypatch):
    monkeypatch.setattr(mod, "_chargebee_lookup_customer", lambda *a, **k: None)

    def _fake_write(resource, customer_id, body):
        raise RuntimeError("id already exists for another customer (400)")

    monkeypatch.setattr(mod, "_chargebee_write_customer", _fake_write)

    df = pd.DataFrame({"email": ["a@b.com"]})
    component = mod.ChargebeeCustomerUpsertComponent(
        asset_name="chargebee_customer_upsert_out",
        upstream_asset_key="upstream_customers",
        fields_map={"email": "email"},
    )
    result = _materialize(component, df)
    assert result.success
    out = metadata_for(result, "chargebee_customer_upsert_out")
    assert out["rows_errored"] == 1
    assert "already exists" in out["first_errors"][0]
