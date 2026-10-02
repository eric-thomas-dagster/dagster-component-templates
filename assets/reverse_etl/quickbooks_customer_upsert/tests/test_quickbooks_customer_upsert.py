"""Committed regression tests for QuickBooksCustomerUpsertComponent.

The real QuickBooks API is never called here -- `_quickbooks_query_customer`
and `_quickbooks_write_customer` (the two external, paid-API boundaries)
are monkeypatched wholesale, while dual source resolution, fields_map
application (incl. dotted-key nesting), lookup_field validation, the
query-then-create-or-update-with-SyncToken branching, and metadata are
all exercised for real.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import FakeQuickBooksResource, load_component_module, make_upstream_asset, metadata_for


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize(component, upstream_df, resource=None):
    resource = resource or FakeQuickBooksResource()
    upstream_asset = make_upstream_asset("upstream_customers", upstream_df)
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    return dg.materialize([asset_def, upstream_asset], resources={"quickbooks": resource})


# --- validation ------------------------------------------------------------

def test_neither_upstream_nor_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.QuickBooksCustomerUpsertComponent(
            asset_name="x",
            fields_map={"customer_name": "DisplayName"},
        ).build_defs(context=None)


def test_both_upstream_and_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.QuickBooksCustomerUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            source={"kind": "inline", "rows": []},
            fields_map={"customer_name": "DisplayName"},
        ).build_defs(context=None)


def test_lookup_field_not_in_fields_map_raises(mod):
    with pytest.raises(ValueError, match="not in fields_map values"):
        mod.QuickBooksCustomerUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            lookup_field="DisplayName",
            fields_map={"company_name": "CompanyName"},
        ).build_defs(context=None)


def test_dotted_lookup_field_raises(mod):
    with pytest.raises(ValueError, match="top-level field"):
        mod.QuickBooksCustomerUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            lookup_field="PrimaryEmailAddr.Address",
            fields_map={"email": "PrimaryEmailAddr.Address"},
        ).build_defs(context=None)


def test_default_lookup_field_is_display_name(mod):
    component = mod.QuickBooksCustomerUpsertComponent(
        asset_name="x",
        upstream_asset_key="foo",
        fields_map={"customer_name": "DisplayName"},
    )
    assert component.lookup_field == "DisplayName"
    component.build_defs(context=None)


def test_missing_upstream_columns_raises(mod):
    df = pd.DataFrame({"customer_name": ["Acme"]})
    component = mod.QuickBooksCustomerUpsertComponent(
        asset_name="quickbooks_customer_upsert_out",
        upstream_asset_key="upstream_customers",
        fields_map={"customer_name": "DisplayName", "company_name": "CompanyName"},
    )
    with pytest.raises(Exception):
        _materialize(component, df)


# --- query-then-write branching (the core SyncToken mechanic) --------------

def test_query_miss_creates_without_id_or_synctoken(mod, monkeypatch):
    query_calls = []
    write_calls = []

    def _fake_query(resource, lookup_field, lookup_value):
        query_calls.append((lookup_field, lookup_value))
        return None  # no existing match

    def _fake_write(resource, body):
        write_calls.append(body)
        return {"Id": "999", "SyncToken": "0"}

    monkeypatch.setattr(mod, "_quickbooks_query_customer", _fake_query)
    monkeypatch.setattr(mod, "_quickbooks_write_customer", _fake_write)

    df = pd.DataFrame({"customer_name": ["Acme"]})
    component = mod.QuickBooksCustomerUpsertComponent(
        asset_name="quickbooks_customer_upsert_out",
        upstream_asset_key="upstream_customers",
        fields_map={"customer_name": "DisplayName"},
    )
    result = _materialize(component, df)
    assert result.success

    assert query_calls == [("DisplayName", "Acme")]
    assert "Id" not in write_calls[0]
    assert "SyncToken" not in write_calls[0]
    assert write_calls[0] == {"DisplayName": "Acme"}

    out = metadata_for(result, "quickbooks_customer_upsert_out")
    assert out["rows_created"] == 1
    assert out["rows_updated"] == 0
    assert out["api_requests"] == 2  # 1 query + 1 write


def test_query_hit_updates_with_id_synctoken_and_sparse(mod, monkeypatch):
    write_calls = []

    def _fake_query(resource, lookup_field, lookup_value):
        return {"Id": "42", "SyncToken": "7"}

    def _fake_write(resource, body):
        write_calls.append(body)
        return {"Id": "42", "SyncToken": "8"}

    monkeypatch.setattr(mod, "_quickbooks_query_customer", _fake_query)
    monkeypatch.setattr(mod, "_quickbooks_write_customer", _fake_write)

    df = pd.DataFrame({"customer_name": ["Acme Updated"]})
    component = mod.QuickBooksCustomerUpsertComponent(
        asset_name="quickbooks_customer_upsert_out",
        upstream_asset_key="upstream_customers",
        fields_map={"customer_name": "DisplayName"},
    )
    result = _materialize(component, df)
    assert result.success

    assert write_calls[0]["Id"] == "42"
    assert write_calls[0]["SyncToken"] == "7"  # the SyncToken AT QUERY TIME, not post-write
    assert write_calls[0]["sparse"] is True

    out = metadata_for(result, "quickbooks_customer_upsert_out")
    assert out["rows_created"] == 0
    assert out["rows_updated"] == 1


def test_mixed_create_and_update_counted_separately(mod, monkeypatch):
    def _fake_query(resource, lookup_field, lookup_value):
        return {"Id": "1", "SyncToken": "3"} if lookup_value == "Existing Co" else None

    def _fake_write(resource, body):
        return {"Id": body.get("Id", "new"), "SyncToken": "0"}

    monkeypatch.setattr(mod, "_quickbooks_query_customer", _fake_query)
    monkeypatch.setattr(mod, "_quickbooks_write_customer", _fake_write)

    df = pd.DataFrame({"customer_name": ["New Co", "Existing Co"]})
    component = mod.QuickBooksCustomerUpsertComponent(
        asset_name="quickbooks_customer_upsert_out",
        upstream_asset_key="upstream_customers",
        fields_map={"customer_name": "DisplayName"},
    )
    result = _materialize(component, df)
    assert result.success
    out = metadata_for(result, "quickbooks_customer_upsert_out")
    assert out["rows_created"] == 1
    assert out["rows_updated"] == 1
    assert out["rows_upserted"] == 2


# --- dotted fields_map keys (nested QBO objects) ----------------------------

def test_dotted_fields_map_builds_nested_object(mod, monkeypatch):
    write_calls = []
    monkeypatch.setattr(mod, "_quickbooks_query_customer", lambda *a, **k: None)

    def _fake_write(resource, body):
        write_calls.append(body)
        return {"Id": "1", "SyncToken": "0"}

    monkeypatch.setattr(mod, "_quickbooks_write_customer", _fake_write)

    df = pd.DataFrame({"customer_name": ["Acme"], "email": ["a@b.com"], "phone": ["555-1234"]})
    component = mod.QuickBooksCustomerUpsertComponent(
        asset_name="quickbooks_customer_upsert_out",
        upstream_asset_key="upstream_customers",
        fields_map={
            "customer_name": "DisplayName",
            "email": "PrimaryEmailAddr.Address",
            "phone": "PrimaryPhone.FreeFormNumber",
        },
    )
    result = _materialize(component, df)
    assert result.success
    assert write_calls[0] == {
        "DisplayName": "Acme",
        "PrimaryEmailAddr": {"Address": "a@b.com"},
        "PrimaryPhone": {"FreeFormNumber": "555-1234"},
    }


# --- missing lookup value / empty upstream ----------------------------------

def test_rows_missing_lookup_value_skipped_and_counted(mod, monkeypatch):
    calls = []

    def _fake_query(resource, lookup_field, lookup_value):
        calls.append(lookup_value)
        return None

    monkeypatch.setattr(mod, "_quickbooks_query_customer", _fake_query)
    monkeypatch.setattr(mod, "_quickbooks_write_customer", lambda *a, **k: {"Id": "1", "SyncToken": "0"})

    df = pd.DataFrame({"customer_name": ["Acme", None]})
    component = mod.QuickBooksCustomerUpsertComponent(
        asset_name="quickbooks_customer_upsert_out",
        upstream_asset_key="upstream_customers",
        fields_map={"customer_name": "DisplayName"},
    )
    result = _materialize(component, df)
    assert result.success
    assert calls == ["Acme"]
    out = metadata_for(result, "quickbooks_customer_upsert_out")
    assert out["rows_skipped_no_key"] == 1


def test_empty_upstream_short_circuits_without_api_calls(mod, monkeypatch):
    called = []
    monkeypatch.setattr(mod, "_quickbooks_query_customer", lambda *a, **k: called.append(1))
    monkeypatch.setattr(mod, "_quickbooks_write_customer", lambda *a, **k: called.append(1))

    df = pd.DataFrame({"customer_name": []})
    component = mod.QuickBooksCustomerUpsertComponent(
        asset_name="quickbooks_customer_upsert_out",
        upstream_asset_key="upstream_customers",
        fields_map={"customer_name": "DisplayName"},
    )
    result = _materialize(component, df)
    assert result.success
    assert called == []
    out = metadata_for(result, "quickbooks_customer_upsert_out")
    assert out["rows_upserted"] == 0


def test_batch_size_caps_rows(mod, monkeypatch):
    monkeypatch.setattr(mod, "_quickbooks_query_customer", lambda *a, **k: None)
    monkeypatch.setattr(mod, "_quickbooks_write_customer", lambda *a, **k: {"Id": "x", "SyncToken": "0"})

    df = pd.DataFrame({"customer_name": [f"Co {i}" for i in range(10)]})
    component = mod.QuickBooksCustomerUpsertComponent(
        asset_name="quickbooks_customer_upsert_out",
        upstream_asset_key="upstream_customers",
        fields_map={"customer_name": "DisplayName"},
        batch_size=3,
    )
    result = _materialize(component, df)
    assert result.success
    out = metadata_for(result, "quickbooks_customer_upsert_out")
    assert out["rows_upserted"] == 3


def test_source_inline_mode(mod, monkeypatch):
    monkeypatch.setattr(mod, "_quickbooks_query_customer", lambda *a, **k: None)
    monkeypatch.setattr(mod, "_quickbooks_write_customer", lambda *a, **k: {"Id": "x", "SyncToken": "0"})

    component = mod.QuickBooksCustomerUpsertComponent(
        asset_name="quickbooks_customer_upsert_out",
        source={"kind": "inline", "rows": [{"customer_name": "A"}, {"customer_name": "B"}]},
        fields_map={"customer_name": "DisplayName"},
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], resources={"quickbooks": FakeQuickBooksResource()})
    assert result.success
    out = metadata_for(result, "quickbooks_customer_upsert_out")
    assert out["rows_upserted"] == 2


def test_query_exception_recorded_as_error_not_raised(mod, monkeypatch):
    def _fake_query(resource, lookup_field, lookup_value):
        raise RuntimeError("QBO 500")

    monkeypatch.setattr(mod, "_quickbooks_query_customer", _fake_query)
    monkeypatch.setattr(mod, "_quickbooks_write_customer", lambda *a, **k: {"Id": "x", "SyncToken": "0"})

    df = pd.DataFrame({"customer_name": ["Acme"]})
    component = mod.QuickBooksCustomerUpsertComponent(
        asset_name="quickbooks_customer_upsert_out",
        upstream_asset_key="upstream_customers",
        fields_map={"customer_name": "DisplayName"},
    )
    result = _materialize(component, df)
    assert result.success
    out = metadata_for(result, "quickbooks_customer_upsert_out")
    assert out["rows_errored"] == 1
    assert "QBO 500" in out["first_errors"][0]


def test_write_exception_e_g_stale_synctoken_recorded_as_error(mod, monkeypatch):
    monkeypatch.setattr(mod, "_quickbooks_query_customer", lambda *a, **k: {"Id": "1", "SyncToken": "3"})

    def _fake_write(resource, body):
        raise RuntimeError("Stale object error: SyncToken mismatch (400)")

    monkeypatch.setattr(mod, "_quickbooks_write_customer", _fake_write)

    df = pd.DataFrame({"customer_name": ["Acme"]})
    component = mod.QuickBooksCustomerUpsertComponent(
        asset_name="quickbooks_customer_upsert_out",
        upstream_asset_key="upstream_customers",
        fields_map={"customer_name": "DisplayName"},
    )
    result = _materialize(component, df)
    assert result.success
    out = metadata_for(result, "quickbooks_customer_upsert_out")
    assert out["rows_errored"] == 1
    assert "SyncToken mismatch" in out["first_errors"][0]
