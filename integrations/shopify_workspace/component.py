"""Shopify Workspace Component.

Auto-emits one Dagster asset per top-level Shopify resource
(products / orders / customers / inventory_items / discount_codes /
draft_orders) for a single store. Materializing runs a paginated REST
list call and returns a DataFrame.

Multi-store deployments should instantiate one workspace per store.
"""
import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, List, Optional

import dagster as dg

try:
    from dagster.components.component.state_backed_component import StateBackedComponent
    from dagster.components.utils.defs_state import (
        DefsStateConfig,
        DefsStateConfigArgs,
        ResolvedDefsStateConfig,
    )
    _HAS_STATE_BACKED = True
except ImportError:
    StateBackedComponent = None  # type: ignore
    DefsStateConfig = None  # type: ignore
    DefsStateConfigArgs = None  # type: ignore
    ResolvedDefsStateConfig = Any  # type: ignore
    _HAS_STATE_BACKED = False


# Canonical resources with their JSON envelope key.
_RESOURCES = {
    "products": "products",
    "orders": "orders",
    "customers": "customers",
    "inventory_items": "inventory_items",
    "discount_codes": "discount_codes",
    "draft_orders": "draft_orders",
    "collections": "collections",
    "checkouts": "checkouts",
    "abandoned_checkouts": "checkouts",
    "fulfillments": "fulfillments",
    "gift_cards": "gift_cards",
    "locations": "locations",
    "price_rules": "price_rules",
    "shipping_zones": "shipping_zones",
}


@dataclass
class ShopifyResourceSelector(dg.Resolvable):
    by_name: Optional[List[str]] = None
    by_pattern: Optional[List[str]] = None
    exclude_by_name: Optional[List[str]] = None
    exclude_by_pattern: Optional[List[str]] = None

    def matches(self, name: str) -> bool:
        import fnmatch
        if self.exclude_by_name and name in self.exclude_by_name:
            return False
        if self.exclude_by_pattern and any(fnmatch.fnmatch(name, p) for p in self.exclude_by_pattern):
            return False
        if not self.by_name and not self.by_pattern:
            return True
        if self.by_name and name in self.by_name:
            return True
        if self.by_pattern and any(fnmatch.fnmatch(name, p) for p in self.by_pattern):
            return True
        return False


if _HAS_STATE_BACKED:

    @dataclass
    class ShopifyWorkspaceComponent(StateBackedComponent, dg.Resolvable):
        shop_env_var: str  # e.g. SHOPIFY_SHOP: mystore.myshopify.com
        access_token_env_var: str
        api_version: str = "2024-04"
        verify_ssl: bool = True

        resource_selector: Optional[ShopifyResourceSelector] = None
        page_limit: int = 250  # Shopify REST caps at 250

        group_name: Optional[str] = None
        asset_key_prefix: List[str] = field(default_factory=lambda: ["shopify"])
        compute_kind: str = "shopify"

        defs_state: Optional[ResolvedDefsStateConfig] = None

        @property
        def defs_state_config(self) -> "DefsStateConfig":
            _key = f"ShopifyWorkspace[{hashlib.sha256(self.shop_env_var.encode()).hexdigest()[:12]}]"
            return DefsStateConfig.from_args(self.defs_state, default_key=_key)

        def write_state_to_path(self, state_path: Path) -> None:
            # Shopify's resource set is a well-known static list — we don't
            # actually probe the API here. We just filter through selector.
            all_resources = list(_RESOURCES.keys())
            if self.resource_selector is not None:
                all_resources = [r for r in all_resources if self.resource_selector.matches(r)]
            state_path.write_text(json.dumps({"resources": all_resources}, indent=2))

        def build_defs_from_state(
            self, context: dg.ComponentLoadContext, state_path: Optional[Path],
        ) -> dg.Definitions:
            if state_path is None or not state_path.exists():
                return dg.Definitions()
            state = json.loads(state_path.read_text())
            assets = []
            for r in state.get("resources", []):
                assets.append(self._build_asset(r))
            return dg.Definitions(assets=assets)

        def _build_asset(self, resource: str):
            _self = self
            key = dg.AssetKey([*self.asset_key_prefix, resource])
            envelope_key = _RESOURCES.get(resource, resource)

            @dg.asset(
                key=key, group_name=self.group_name, compute_kind=self.compute_kind,
                metadata={"shopify_resource": dg.MetadataValue.text(resource)},
            )
            def _asset(context: dg.AssetExecutionContext):
                import os
                try:
                    import pandas as pd
                    import requests
                except ImportError as e:
                    raise Exception("pandas or requests library not installed") from e

                shop = os.environ.get(_self.shop_env_var, "")
                token = os.environ.get(_self.access_token_env_var, "")
                session = requests.Session()
                session.verify = _self.verify_ssl
                headers = {"X-Shopify-Access-Token": token, "Accept": "application/json"}

                path = resource
                # Handle special-case endpoints.
                if resource == "abandoned_checkouts":
                    path = "checkouts"
                url = f"https://{shop}/admin/api/{_self.api_version}/{path}.json"

                r = session.get(url, headers=headers, params={"limit": _self.page_limit}, timeout=60)
                r.raise_for_status()
                body = r.json() or {}
                rows = body.get(envelope_key, [])
                df = pd.DataFrame(rows)
                context.add_output_metadata({
                    "row_count": len(df),
                    "shopify_resource": resource,
                })
                return df

            return _asset

else:
    class ShopifyWorkspaceComponent:  # type: ignore
        def __init__(self, *args, **kwargs):
            raise ImportError("ShopifyWorkspaceComponent requires Dagster with StateBackedComponent support.")
