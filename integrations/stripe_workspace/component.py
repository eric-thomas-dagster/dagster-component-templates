"""Stripe Workspace Component.

Auto-emit one Dagster asset per top-level Stripe resource (customers /
charges / subscriptions / invoices / payment_intents / balance_transactions
/ etc.) for a single Stripe account. Materializing runs a paginated
List call against the Stripe REST API and emits a DataFrame.
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


_RESOURCES = [
    "customers", "charges", "subscriptions", "invoices", "payment_intents",
    "balance_transactions", "refunds", "products", "prices", "coupons",
    "disputes", "payouts", "payment_methods", "setup_intents",
    "subscription_items", "credit_notes", "checkout/sessions",
    "billing_portal/sessions", "transfers",
]


@dataclass
class StripeResourceSelector(dg.Resolvable):
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
    class StripeWorkspaceComponent(StateBackedComponent, dg.Resolvable):
        api_key_env_var: str  # e.g. STRIPE_API_KEY
        verify_ssl: bool = True

        resource_selector: Optional[StripeResourceSelector] = None
        page_limit: int = 100  # Stripe caps at 100

        group_name: Optional[str] = None
        asset_key_prefix: List[str] = field(default_factory=lambda: ["stripe"])
        compute_kind: str = "stripe"

        defs_state: Optional[ResolvedDefsStateConfig] = None

        @property
        def defs_state_config(self) -> "DefsStateConfig":
            _key = f"StripeWorkspace[{hashlib.sha256(self.api_key_env_var.encode()).hexdigest()[:12]}]"
            return DefsStateConfig.from_args(self.defs_state, default_key=_key)

        def write_state_to_path(self, state_path: Path) -> None:
            resources = list(_RESOURCES)
            if self.resource_selector is not None:
                resources = [r for r in resources if self.resource_selector.matches(r)]
            state_path.write_text(json.dumps({"resources": resources}, indent=2))

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
            # Nested resources like "checkout/sessions" → asset key ["stripe", "checkout", "sessions"]
            segs = [seg for seg in resource.split("/") if seg]
            key = dg.AssetKey([*self.asset_key_prefix, *segs])

            @dg.asset(
                key=key, group_name=self.group_name, compute_kind=self.compute_kind,
                metadata={"stripe_resource": dg.MetadataValue.text(resource)},
            )
            def _asset(context: dg.AssetExecutionContext):
                import os
                try:
                    import pandas as pd
                    import requests
                except ImportError as e:
                    raise Exception("pandas or requests library not installed") from e

                api_key = os.environ.get(_self.api_key_env_var, "")
                session = requests.Session()
                session.verify = _self.verify_ssl
                # Stripe uses Basic auth with the API key as username, empty password.
                session.auth = (api_key, "")

                url = f"https://api.stripe.com/v1/{resource}"
                r = session.get(url, params={"limit": _self.page_limit}, timeout=60)
                r.raise_for_status()
                body = r.json() or {}
                rows = body.get("data", [])
                df = pd.DataFrame(rows)
                context.add_output_metadata({
                    "row_count": len(df),
                    "stripe_resource": resource,
                    "has_more": body.get("has_more", False),
                })
                return df

            return _asset

else:
    class StripeWorkspaceComponent:  # type: ignore
        def __init__(self, *args, **kwargs):
            raise ImportError("StripeWorkspaceComponent requires Dagster with StateBackedComponent support.")
