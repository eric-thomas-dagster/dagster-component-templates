"""Shared test helpers for BigCommerceProductUpsertComponent.

Loads component.py directly via importlib so tests don't require the
parent package to be pip-installed. The real BigCommerce Catalog API
(HTTP boundary) is never hit -- a minimal FakeBigCommerceResource stands
in for `BigCommerceResource`, implementing only the public methods the
sink calls (`upsert_product_by_sku`, `update_product`) and tracking
calls so tests can assert on them. Everything else this component owns
-- source resolution, validation, row-building, metadata -- is
exercised for real, per this repo's committed-test convention.
"""
import importlib.util
import pathlib
from types import ModuleType

import dagster as dg


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location(
        "bigcommerce_product_upsert_component", component_py
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


class FakeBigCommerceResource:
    """Stands in for BigCommerceResource -- no HTTP, no real Catalog API.

    Mimics the real resource's search-then-write semantics: `existing`
    seeds products already "in BigCommerce" by sku; `upsert_product_by_sku`
    updates them in place (if found) or creates a new one (assigning an
    incrementing id), exactly like the real resource's contract of
    returning `{'action': 'created'|'updated', 'product': {...}}`.
    """

    def __init__(self, existing: dict | None = None):
        # sku -> product dict (as if already present in BigCommerce).
        self.products: dict[str, dict] = {k: dict(v) for k, v in (existing or {}).items()}
        self._next_id = 1000 + len(self.products)
        for sku, product in self.products.items():
            product.setdefault("sku", sku)
            product.setdefault("id", self._next_id)
            self._next_id += 1

        self.upsert_calls: list[tuple[str, dict]] = []
        self.update_calls: list[tuple[int, dict]] = []
        self.raise_for_sku: dict[str, Exception] = {}

    def upsert_product_by_sku(self, sku: str, product_body: dict) -> dict:
        self.upsert_calls.append((sku, dict(product_body)))
        if sku in self.raise_for_sku:
            raise self.raise_for_sku[sku]

        body_out = dict(product_body)
        body_out.setdefault("sku", sku)

        existing = self.products.get(sku)
        if existing:
            existing.update(body_out)
            return {"action": "updated", "product": dict(existing)}

        self._next_id += 1
        new_id = self._next_id
        product = dict(body_out)
        product["id"] = new_id
        self.products[sku] = product
        return {"action": "created", "product": dict(product)}

    def update_product(self, product_id: int, product_body: dict) -> dict:
        self.update_calls.append((product_id, dict(product_body)))
        for product in self.products.values():
            if product.get("id") == product_id:
                product.update(product_body)
                return dict(product)
        raise AssertionError(f"update_product called for unknown id {product_id}")
