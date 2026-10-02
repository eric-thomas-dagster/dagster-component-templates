"""Committed regression tests for MetronomeInvoicesIngestionComponent.

The real `requests` network calls are never made here --
`_list_customers_page` / `_list_invoices_page` (the two external, paid-API
boundaries) are monkeypatched wholesale, while customer resolution,
pagination following, and DataFrame assembly are all exercised for real.
"""
import dagster as dg
import pytest

from .conftest import FakeMetronomeResource, load_component_module, metadata_for, output_value


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize(component, resource):
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    return dg.materialize([asset_def], resources={component.resource_name: resource})


def test_explicit_customer_ids_skips_customer_listing(mod, monkeypatch):
    customers_calls = []
    invoices_calls = []

    def _fake_customers(client, base_url, limit, next_page):
        customers_calls.append(next_page)
        return {"data": [], "next_page": None}

    def _fake_invoices(client, base_url, customer_id, limit, next_page, status, starting_on, ending_before):
        invoices_calls.append((customer_id, next_page))
        return {"data": [{"id": "inv_1", "status": "FINALIZED", "total": 100, "customer_id": customer_id}], "next_page": None}

    monkeypatch.setattr(mod, "_list_customers_page", _fake_customers)
    monkeypatch.setattr(mod, "_list_invoices_page", _fake_invoices)

    component = mod.MetronomeInvoicesIngestionComponent(
        asset_name="metronome_invoices",
        customer_ids=["cust_1", "cust_2"],
    )
    result = _materialize(component, FakeMetronomeResource())
    assert result.success
    assert customers_calls == []  # never listed -- customer_ids was explicit
    assert len(invoices_calls) == 2

    out = metadata_for(result, "metronome_invoices")
    assert out["row_count"] == 2
    assert out["customer_count"] == 2


def test_customer_listing_paginates_until_next_page_none(mod, monkeypatch):
    customer_pages = [
        {"data": [{"id": "cust_1"}], "next_page": "cursor-2"},
        {"data": [{"id": "cust_2"}], "next_page": None},
    ]

    def _fake_customers(client, base_url, limit, next_page):
        return customer_pages.pop(0)

    def _fake_invoices(client, base_url, customer_id, limit, next_page, status, starting_on, ending_before):
        return {"data": [{"id": f"inv_{customer_id}", "customer_id": customer_id}], "next_page": None}

    monkeypatch.setattr(mod, "_list_customers_page", _fake_customers)
    monkeypatch.setattr(mod, "_list_invoices_page", _fake_invoices)

    component = mod.MetronomeInvoicesIngestionComponent(asset_name="metronome_invoices")
    result = _materialize(component, FakeMetronomeResource())
    assert result.success

    out = metadata_for(result, "metronome_invoices")
    assert out["customer_count"] == 2
    assert out["row_count"] == 2


def test_invoice_listing_paginates_per_customer(mod, monkeypatch):
    def _fake_customers(client, base_url, limit, next_page):
        return {"data": [{"id": "cust_1"}], "next_page": None}

    invoice_pages = [
        {"data": [{"id": "inv_1", "customer_id": "cust_1"}], "next_page": "cursor-b"},
        {"data": [{"id": "inv_2", "customer_id": "cust_1"}], "next_page": None},
    ]

    def _fake_invoices(client, base_url, customer_id, limit, next_page, status, starting_on, ending_before):
        return invoice_pages.pop(0)

    monkeypatch.setattr(mod, "_list_customers_page", _fake_customers)
    monkeypatch.setattr(mod, "_list_invoices_page", _fake_invoices)

    component = mod.MetronomeInvoicesIngestionComponent(asset_name="metronome_invoices")
    result = _materialize(component, FakeMetronomeResource())
    assert result.success

    df = output_value(result, "metronome_invoices")
    assert len(df) == 2
    assert set(df["id"]) == {"inv_1", "inv_2"}


def test_max_customers_caps_fan_out(mod, monkeypatch):
    customer_pages = [
        {"data": [{"id": "cust_1"}, {"id": "cust_2"}], "next_page": "cursor-2"},
        {"data": [{"id": "cust_3"}], "next_page": None},
    ]

    def _fake_customers(client, base_url, limit, next_page):
        return customer_pages.pop(0)

    invoice_calls = []

    def _fake_invoices(client, base_url, customer_id, limit, next_page, status, starting_on, ending_before):
        invoice_calls.append(customer_id)
        return {"data": [{"id": f"inv_{customer_id}", "customer_id": customer_id}], "next_page": None}

    monkeypatch.setattr(mod, "_list_customers_page", _fake_customers)
    monkeypatch.setattr(mod, "_list_invoices_page", _fake_invoices)

    component = mod.MetronomeInvoicesIngestionComponent(
        asset_name="metronome_invoices",
        max_customers=1,
    )
    result = _materialize(component, FakeMetronomeResource())
    assert result.success
    assert len(invoice_calls) == 1


def test_invoice_filters_passed_through(mod, monkeypatch):
    def _fake_customers(client, base_url, limit, next_page):
        return {"data": [{"id": "cust_1"}], "next_page": None}

    captured = {}

    def _fake_invoices(client, base_url, customer_id, limit, next_page, status, starting_on, ending_before):
        captured["status"] = status
        captured["starting_on"] = starting_on
        captured["ending_before"] = ending_before
        return {"data": [], "next_page": None}

    monkeypatch.setattr(mod, "_list_customers_page", _fake_customers)
    monkeypatch.setattr(mod, "_list_invoices_page", _fake_invoices)

    component = mod.MetronomeInvoicesIngestionComponent(
        asset_name="metronome_invoices",
        invoice_status="FINALIZED",
        starting_on="2026-01-01T00:00:00.000Z",
        ending_before="2026-07-01T00:00:00.000Z",
    )
    result = _materialize(component, FakeMetronomeResource())
    assert result.success
    assert captured == {
        "status": "FINALIZED",
        "starting_on": "2026-01-01T00:00:00.000Z",
        "ending_before": "2026-07-01T00:00:00.000Z",
    }


def test_no_invoices_returns_empty_dataframe(mod, monkeypatch):
    def _fake_customers(client, base_url, limit, next_page):
        return {"data": [{"id": "cust_1"}], "next_page": None}

    def _fake_invoices(client, base_url, customer_id, limit, next_page, status, starting_on, ending_before):
        return {"data": [], "next_page": None}

    monkeypatch.setattr(mod, "_list_customers_page", _fake_customers)
    monkeypatch.setattr(mod, "_list_invoices_page", _fake_invoices)

    component = mod.MetronomeInvoicesIngestionComponent(asset_name="metronome_invoices")
    result = _materialize(component, FakeMetronomeResource())
    assert result.success

    out = metadata_for(result, "metronome_invoices")
    assert out["row_count"] == 0
    df = output_value(result, "metronome_invoices")
    assert len(df) == 0
