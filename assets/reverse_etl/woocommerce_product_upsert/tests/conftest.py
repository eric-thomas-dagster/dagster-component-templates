"""Shared test helpers for WooCommerceProductUpsertComponent.

Loads component.py directly via importlib so tests don't require the
parent package to be pip-installed. The real `WooCommerceResource` (and
the HTTP/requests layer it wraps) is never imported -- a minimal fake
resource stands in for the one external, paid-API boundary, implementing
only the public methods this sink's component.py calls
(`upsert_product_by_sku`, `update_product`), tracking calls in lists so
tests can assert on them. Every other line of this component's own logic
(source resolution, validation, row-building, metadata) is exercised for
real, matching this repo's committed-test convention.
"""
import importlib.util
import pathlib
from types import ModuleType

import dagster as dg


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location(
        "woocommerce_product_upsert_component", component_py
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def make_upstream_asset(name: str, df):
    """A fresh @asset closure per call -- needed because materializing the
    same function object twice under different DataFrames in one test file
    would otherwise share the same closed-over `df`."""
    @dg.asset(name=name)
    def _upstream():
        return df
    return _upstream


class FakeWooCommerceResource:
    """Stands in for the real WooCommerceResource -- implements only the
    public methods WooCommerceProductUpsertComponent's `_run_upsert` calls:
    `upsert_product_by_sku` and `update_product`. Tracks every call so
    tests can assert on create-vs-update branching and error injection
    without ever touching HTTP or the real WooCommerce REST API."""

    def __init__(self):
        self._by_sku: dict[str, dict] = {}
        self._next_id = 1000
        self.upsert_calls: list[tuple[str, dict]] = []
        self.update_calls: list[tuple[int, dict]] = []
        self.raise_for_sku: dict[str, Exception] = {}

    def _new_id(self) -> int:
        self._next_id += 1
        return self._next_id

    def upsert_product_by_sku(self, sku: str, product_body: dict) -> dict:
        self.upsert_calls.append((sku, dict(product_body)))
        if sku in self.raise_for_sku:
            raise self.raise_for_sku[sku]
        existing = self._by_sku.get(sku)
        if existing is not None:
            existing.update(product_body)
            return {"action": "updated", "product": existing}
        new_id = self._new_id()
        product = dict(product_body)
        product["id"] = new_id
        product.setdefault("sku", sku)
        self._by_sku[sku] = product
        return {"action": "created", "product": product}

    def update_product(self, product_id: int, product_body: dict) -> dict:
        self.update_calls.append((product_id, dict(product_body)))
        for product in self._by_sku.values():
            if product.get("id") == product_id:
                product.update(product_body)
                return product
        product = dict(product_body)
        product["id"] = product_id
        return product
