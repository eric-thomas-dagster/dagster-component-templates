"""Committed regression tests for ZuoraAccountUpsertComponent.

The real Zuora API is never called here -- `_zuora_lookup_account` and
`_zuora_write_account` (the two external, paid-API boundaries) are
monkeypatched wholesale, while dual source resolution, fields_map
application, lookup_field validation, the query-then-create-or-update
branching, and metadata are all exercised for real.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import FakeZuoraResource, load_component_module, make_upstream_asset, metadata_for


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize(component, upstream_df, resource=None):
    resource = resource or FakeZuoraResource()
    upstream_asset = make_upstream_asset("upstream_customers", upstream_df)
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    return dg.materialize([asset_def, upstream_asset], resources={"zuora": resource})


# --- validation ------------------------------------------------------------

def test_neither_upstream_nor_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.ZuoraAccountUpsertComponent(
            asset_name="x",
            fields_map={"customer_id": "AccountNumber"},
        ).build_defs(context=None)


def test_both_upstream_and_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.ZuoraAccountUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            source={"kind": "inline", "rows": []},
            fields_map={"customer_id": "AccountNumber"},
        ).build_defs(context=None)


def test_lookup_field_not_in_fields_map_raises(mod):
    with pytest.raises(ValueError, match="not in fields_map values"):
        mod.ZuoraAccountUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            lookup_field="AccountNumber",
            fields_map={"account_name": "Name"},
        ).build_defs(context=None)


def test_default_lookup_field_is_account_number(mod):
    component = mod.ZuoraAccountUpsertComponent(
        asset_name="x",
        upstream_asset_key="foo",
        fields_map={"customer_id": "AccountNumber"},
    )
    assert component.lookup_field == "AccountNumber"
    component.build_defs(context=None)


def test_missing_upstream_columns_raises(mod):
    df = pd.DataFrame({"customer_id": ["A001"]})
    component = mod.ZuoraAccountUpsertComponent(
        asset_name="zuora_account_upsert_out",
        upstream_asset_key="upstream_customers",
        fields_map={"customer_id": "AccountNumber", "account_name": "Name"},
    )
    with pytest.raises(Exception):
        _materialize(component, df)


# --- query-then-write branching (the core create-vs-update mechanic) -------

def test_query_miss_creates_without_account_id(mod, monkeypatch):
    lookup_calls = []
    write_calls = []

    def _fake_lookup(resource, lookup_field, lookup_value):
        lookup_calls.append((lookup_field, lookup_value))
        return None  # no existing match

    def _fake_write(resource, account_id, body):
        write_calls.append((account_id, body))
        return {"id": "new-id-1", "action": "created"}

    monkeypatch.setattr(mod, "_zuora_lookup_account", _fake_lookup)
    monkeypatch.setattr(mod, "_zuora_write_account", _fake_write)

    df = pd.DataFrame({"customer_id": ["A001"]})
    component = mod.ZuoraAccountUpsertComponent(
        asset_name="zuora_account_upsert_out",
        upstream_asset_key="upstream_customers",
        fields_map={"customer_id": "AccountNumber"},
    )
    result = _materialize(component, df)
    assert result.success

    assert lookup_calls == [("AccountNumber", "A001")]
    assert write_calls[0][0] is None
    assert write_calls[0][1] == {"AccountNumber": "A001"}

    out = metadata_for(result, "zuora_account_upsert_out")
    assert out["rows_created"] == 1
    assert out["rows_updated"] == 0
    assert out["api_requests"] == 2  # 1 query + 1 write


def test_query_hit_updates_with_account_id(mod, monkeypatch):
    write_calls = []

    def _fake_lookup(resource, lookup_field, lookup_value):
        return "8ad09378-existing-id"

    def _fake_write(resource, account_id, body):
        write_calls.append((account_id, body))
        return {"id": account_id, "action": "updated"}

    monkeypatch.setattr(mod, "_zuora_lookup_account", _fake_lookup)
    monkeypatch.setattr(mod, "_zuora_write_account", _fake_write)

    df = pd.DataFrame({"customer_id": ["A001"]})
    component = mod.ZuoraAccountUpsertComponent(
        asset_name="zuora_account_upsert_out",
        upstream_asset_key="upstream_customers",
        fields_map={"customer_id": "AccountNumber"},
    )
    result = _materialize(component, df)
    assert result.success

    assert write_calls[0][0] == "8ad09378-existing-id"

    out = metadata_for(result, "zuora_account_upsert_out")
    assert out["rows_created"] == 0
    assert out["rows_updated"] == 1


def test_mixed_create_and_update_counted_separately(mod, monkeypatch):
    def _fake_lookup(resource, lookup_field, lookup_value):
        return "id-1" if lookup_value == "A_EXIST" else None

    def _fake_write(resource, account_id, body):
        return {"id": account_id or "new", "action": "updated" if account_id else "created"}

    monkeypatch.setattr(mod, "_zuora_lookup_account", _fake_lookup)
    monkeypatch.setattr(mod, "_zuora_write_account", _fake_write)

    df = pd.DataFrame({"customer_id": ["A_NEW", "A_EXIST"]})
    component = mod.ZuoraAccountUpsertComponent(
        asset_name="zuora_account_upsert_out",
        upstream_asset_key="upstream_customers",
        fields_map={"customer_id": "AccountNumber"},
    )
    result = _materialize(component, df)
    assert result.success
    out = metadata_for(result, "zuora_account_upsert_out")
    assert out["rows_created"] == 1
    assert out["rows_updated"] == 1
    assert out["rows_upserted"] == 2


# --- multi-field fields_map (incl. Zuora's required create fields) --------

def test_fields_map_builds_flat_body_with_create_required_fields(mod, monkeypatch):
    write_calls = []
    monkeypatch.setattr(mod, "_zuora_lookup_account", lambda *a, **k: None)

    def _fake_write(resource, account_id, body):
        write_calls.append(body)
        return {"id": "1", "action": "created"}

    monkeypatch.setattr(mod, "_zuora_write_account", _fake_write)

    df = pd.DataFrame({
        "customer_id": ["A001"],
        "account_name": ["Acme Inc"],
        "currency": ["USD"],
        "bill_cycle_day": [1],
        "status": ["Active"],
    })
    component = mod.ZuoraAccountUpsertComponent(
        asset_name="zuora_account_upsert_out",
        upstream_asset_key="upstream_customers",
        fields_map={
            "customer_id": "AccountNumber",
            "account_name": "Name",
            "currency": "Currency",
            "bill_cycle_day": "BillCycleDay",
            "status": "Status",
        },
    )
    result = _materialize(component, df)
    assert result.success
    assert write_calls[0] == {
        "AccountNumber": "A001",
        "Name": "Acme Inc",
        "Currency": "USD",
        "BillCycleDay": 1,
        "Status": "Active",
    }


# --- missing lookup value / empty upstream ----------------------------------

def test_rows_missing_lookup_value_skipped_and_counted(mod, monkeypatch):
    calls = []

    def _fake_lookup(resource, lookup_field, lookup_value):
        calls.append(lookup_value)
        return None

    monkeypatch.setattr(mod, "_zuora_lookup_account", _fake_lookup)
    monkeypatch.setattr(mod, "_zuora_write_account", lambda *a, **k: {"id": "1", "action": "created"})

    df = pd.DataFrame({"customer_id": ["A001", None]})
    component = mod.ZuoraAccountUpsertComponent(
        asset_name="zuora_account_upsert_out",
        upstream_asset_key="upstream_customers",
        fields_map={"customer_id": "AccountNumber"},
    )
    result = _materialize(component, df)
    assert result.success
    assert calls == ["A001"]
    out = metadata_for(result, "zuora_account_upsert_out")
    assert out["rows_skipped_no_key"] == 1


def test_empty_upstream_short_circuits_without_api_calls(mod, monkeypatch):
    called = []
    monkeypatch.setattr(mod, "_zuora_lookup_account", lambda *a, **k: called.append(1))
    monkeypatch.setattr(mod, "_zuora_write_account", lambda *a, **k: called.append(1))

    df = pd.DataFrame({"customer_id": []})
    component = mod.ZuoraAccountUpsertComponent(
        asset_name="zuora_account_upsert_out",
        upstream_asset_key="upstream_customers",
        fields_map={"customer_id": "AccountNumber"},
    )
    result = _materialize(component, df)
    assert result.success
    assert called == []
    out = metadata_for(result, "zuora_account_upsert_out")
    assert out["rows_upserted"] == 0


def test_batch_size_caps_rows(mod, monkeypatch):
    monkeypatch.setattr(mod, "_zuora_lookup_account", lambda *a, **k: None)
    monkeypatch.setattr(mod, "_zuora_write_account", lambda *a, **k: {"id": "x", "action": "created"})

    df = pd.DataFrame({"customer_id": [f"A{i:03d}" for i in range(10)]})
    component = mod.ZuoraAccountUpsertComponent(
        asset_name="zuora_account_upsert_out",
        upstream_asset_key="upstream_customers",
        fields_map={"customer_id": "AccountNumber"},
        batch_size=3,
    )
    result = _materialize(component, df)
    assert result.success
    out = metadata_for(result, "zuora_account_upsert_out")
    assert out["rows_upserted"] == 3


def test_source_inline_mode(mod, monkeypatch):
    monkeypatch.setattr(mod, "_zuora_lookup_account", lambda *a, **k: None)
    monkeypatch.setattr(mod, "_zuora_write_account", lambda *a, **k: {"id": "x", "action": "created"})

    component = mod.ZuoraAccountUpsertComponent(
        asset_name="zuora_account_upsert_out",
        source={"kind": "inline", "rows": [{"customer_id": "A1"}, {"customer_id": "A2"}]},
        fields_map={"customer_id": "AccountNumber"},
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], resources={"zuora": FakeZuoraResource()})
    assert result.success
    out = metadata_for(result, "zuora_account_upsert_out")
    assert out["rows_upserted"] == 2


def test_query_exception_recorded_as_error_not_raised(mod, monkeypatch):
    def _fake_lookup(resource, lookup_field, lookup_value):
        raise RuntimeError("Zuora 500")

    monkeypatch.setattr(mod, "_zuora_lookup_account", _fake_lookup)
    monkeypatch.setattr(mod, "_zuora_write_account", lambda *a, **k: {"id": "x", "action": "created"})

    df = pd.DataFrame({"customer_id": ["A001"]})
    component = mod.ZuoraAccountUpsertComponent(
        asset_name="zuora_account_upsert_out",
        upstream_asset_key="upstream_customers",
        fields_map={"customer_id": "AccountNumber"},
    )
    result = _materialize(component, df)
    assert result.success
    out = metadata_for(result, "zuora_account_upsert_out")
    assert out["rows_errored"] == 1
    assert "Zuora 500" in out["first_errors"][0]


def test_write_exception_e_g_missing_required_create_field_recorded_as_error(mod, monkeypatch):
    monkeypatch.setattr(mod, "_zuora_lookup_account", lambda *a, **k: None)

    def _fake_write(resource, account_id, body):
        raise RuntimeError("MISSING_REQUIRED_VALUE: Currency (400)")

    monkeypatch.setattr(mod, "_zuora_write_account", _fake_write)

    df = pd.DataFrame({"customer_id": ["A001"]})
    component = mod.ZuoraAccountUpsertComponent(
        asset_name="zuora_account_upsert_out",
        upstream_asset_key="upstream_customers",
        fields_map={"customer_id": "AccountNumber"},
    )
    result = _materialize(component, df)
    assert result.success
    out = metadata_for(result, "zuora_account_upsert_out")
    assert out["rows_errored"] == 1
    assert "MISSING_REQUIRED_VALUE" in out["first_errors"][0]
