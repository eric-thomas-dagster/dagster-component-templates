"""Shared test helpers for MagentoProductUpsertComponent.

Loads component.py directly via importlib so tests don't require the
parent package to be pip-installed. The real `requests`-backed
MagentoResource is never imported here -- a minimal FakeMagentoResource
stands in for the one external HTTP boundary, matching the convention of
mocking only the external call while exercising all of this component's
own logic (dual source resolution, validation, row-building, metadata)
for real.

FakeMagentoResource deliberately mirrors the REAL resource's update-only
semantics: `update_product` can only succeed for a sku already present in
`existing_products` (seeded at construction time), and `upsert_product_by_sku`
does the same GET-then-PUT-or-POST dance as the real MagentoResource. This
lets tests assert that PUT (update_calls) is only ever exercised against
skus that already "existed" -- proving the sink never blindly PUTs against
Magento's update-only endpoint.
"""
import importlib.util
import pathlib
from types import ModuleType
from typing import Any, Dict, Optional


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location(
        "magento_product_upsert_component", component_py
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def make_upstream_asset(name: str, df):
    """A fresh @asset closure per call -- needed because materializing the
    same function object twice under different DataFrames in one test file
    would otherwise share the same closed-over `df`."""
    import dagster as dg

    @dg.asset(name=name)
    def _upstream():
        return df
    return _upstream


class FakeMagentoResource:
    """Stands in for the real MagentoResource -- no HTTP, no `requests`.

    Mirrors the real resource's update-only PUT semantics: `existing_products`
    is the fake "Magento catalog" at test start (sku -> product dict). A sku
    not already in it can never be reached by `update_product` through the
    normal `upsert_product_by_sku` path -- only `create_product` (POST) can
    add a new one, same as the real Magento REST API.
    """

    def __init__(
        self,
        existing_products: Optional[Dict[str, Dict[str, Any]]] = None,
        raise_on_sku: Optional[set] = None,
    ):
        # sku -> product body, simulating what's already in Magento.
        self.existing_products: Dict[str, Dict[str, Any]] = dict(existing_products or {})
        # skus that should raise when upserted (to exercise error aggregation).
        self.raise_on_sku = raise_on_sku or set()

        self.get_calls: list = []
        self.create_calls: list = []
        self.update_calls: list = []
        self.upsert_calls: list = []

    def get_product_by_sku(self, sku: str) -> Optional[Dict[str, Any]]:
        self.get_calls.append(sku)
        return self.existing_products.get(str(sku))

    def create_product(self, product_body: Dict[str, Any]) -> Dict[str, Any]:
        body_out = dict(product_body)
        body_out.setdefault("attribute_set_id", 4)
        self.create_calls.append(body_out)
        sku = body_out.get("sku")
        if sku is not None:
            self.existing_products[str(sku)] = body_out
        return body_out

    def update_product(self, sku: str, product_body: Dict[str, Any]) -> Dict[str, Any]:
        if str(sku) not in self.existing_products:
            # Real Magento: PUT against a non-existent sku does NOT create it.
            raise RuntimeError(
                f"update_product called for sku={sku!r} which does not exist "
                f"-- Magento's PUT /V1/products/:sku is update-only."
            )
        body_out = dict(product_body)
        body_out["sku"] = sku
        self.update_calls.append(body_out)
        self.existing_products[str(sku)].update(body_out)
        return self.existing_products[str(sku)]

    def upsert_product_by_sku(self, sku: str, product_body: Dict[str, Any]) -> Dict[str, Any]:
        self.upsert_calls.append((sku, dict(product_body)))
        if sku in self.raise_on_sku:
            raise RuntimeError(f"simulated Magento API error for sku={sku!r}")
        existing = self.get_product_by_sku(sku)
        if existing:
            product = self.update_product(sku, product_body)
            return {"action": "updated", "product": product}
        body_out = dict(product_body)
        body_out.setdefault("sku", sku)
        product = self.create_product(body_out)
        return {"action": "created", "product": product}
