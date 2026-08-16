"""Stripe Resource component.

Secret-key wrapper over the Stripe REST API with ergonomic read + write
convenience methods so Dagster assets can call
`context.resources.stripe.list_customers(...)` or
`.create_customer(email=..., metadata=...)` without touching HTTP.

Stripe uses form-encoded requests (application/x-www-form-urlencoded)
and cursor-based pagination via `starting_after`. Both are handled by
the resource — you pass plain Python dicts.

Every write method accepts an `idempotency_key` — Stripe's native
client-side dedup mechanism. Same key sent twice returns the original
result, so retrying a create is safe.

Drop to `.get_client()` for anything not covered — returns an
authenticated `requests.Session` pointed at `api.stripe.com/v1`.
"""
from typing import Iterator, List, Optional

import dagster as dg
from dagster import ConfigurableResource
from pydantic import Field


class StripeResource(ConfigurableResource):
    """Dagster resource wrapping the Stripe REST API with convenience methods.

    Covers customers, charges, payment intents, invoices, subscriptions,
    products, prices, refunds, disputes, events, and a generic search. For
    anything not covered, use `.get_client()` to get an authenticated
    `requests.Session`.

    Auth: pass a secret key (`sk_test_...` for test mode or `sk_live_...`
    for production).
    """

    api_key: str = Field(description="Stripe secret key (sk_test_... or sk_live_...).")
    api_base_url: str = Field(
        default="https://api.stripe.com/v1",
        description="API base URL (override for Stripe's testmode edge locations or a mock server).",
    )
    verify_ssl: bool = Field(default=True, description="TLS cert verification.")

    def get_client(self):
        """Return an authenticated `requests.Session`. Escape hatch."""
        import requests
        session = requests.Session()
        session.verify = self.verify_ssl
        session.headers.update({
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/json",
        })
        return session

    def _url(self, path: str) -> str:
        return f"{self.api_base_url}{path}"

    def _flatten_form(self, data: dict, prefix: str = "") -> dict:
        """Flatten nested dicts into Stripe's bracketed form-encoding.

        `{"metadata": {"key": "val"}}` → `{"metadata[key]": "val"}`.
        Lists become `param[0]=a&param[1]=b`.
        """
        out: dict = {}
        for k, v in data.items():
            key = f"{prefix}[{k}]" if prefix else k
            if isinstance(v, dict):
                out.update(self._flatten_form(v, key))
            elif isinstance(v, list):
                for i, item in enumerate(v):
                    if isinstance(item, dict):
                        out.update(self._flatten_form(item, f"{key}[{i}]"))
                    else:
                        out[f"{key}[{i}]"] = item
            elif v is not None:
                out[key] = v
        return out

    def _get(self, path: str, **params) -> dict:
        r = self.get_client().get(self._url(path), params=params, timeout=60)
        r.raise_for_status()
        return r.json()

    def _post(
        self, path: str, data: Optional[dict] = None, idempotency_key: Optional[str] = None
    ) -> dict:
        session = self.get_client()
        headers = {}
        if idempotency_key:
            headers["Idempotency-Key"] = idempotency_key
        form = self._flatten_form(data or {})
        r = session.post(self._url(path), data=form, headers=headers, timeout=60)
        r.raise_for_status()
        return r.json()

    def _delete(self, path: str) -> dict:
        r = self.get_client().delete(self._url(path), timeout=60)
        r.raise_for_status()
        return r.json()

    def _iter_paginated(self, path: str, page_size: int = 100, **params) -> Iterator[dict]:
        """Auto-paginate a Stripe list endpoint via `starting_after` cursor."""
        session = self.get_client()
        starting_after: Optional[str] = None
        while True:
            resp_params = {**params, "limit": page_size}
            if starting_after:
                resp_params["starting_after"] = starting_after
            r = session.get(self._url(path), params=resp_params, timeout=60)
            r.raise_for_status()
            body = r.json()
            items = body.get("data", [])
            for it in items:
                yield it
            if not body.get("has_more") or not items:
                return
            starting_after = items[-1].get("id")
            if not starting_after:
                return

    # ------------------------------------------------------------------ reads

    def whoami(self) -> dict:
        """Return the current account (Stripe's `/v1/account`)."""
        return self._get("/account")

    def get_customer(self, customer_id: str) -> dict:
        """Retrieve a customer by ID."""
        return self._get(f"/customers/{customer_id}")

    def list_customers(
        self, email: Optional[str] = None, page_size: int = 100
    ) -> List[dict]:
        """List customers. Optionally filter by email exact match. First page only."""
        params: dict = {"limit": page_size}
        if email:
            params["email"] = email
        return self._get("/customers", **params).get("data", [])

    def iter_customers(
        self, email: Optional[str] = None, page_size: int = 100
    ) -> Iterator[dict]:
        """Auto-paginated variant of `list_customers`."""
        params: dict = {}
        if email:
            params["email"] = email
        yield from self._iter_paginated("/customers", page_size=page_size, **params)

    def search_customers(self, query: str, page_size: int = 100) -> List[dict]:
        """Search customers via Stripe's Search API.

        Example queries:
          - `email:'x@y.com'`
          - `metadata['dagster_key']:'INC-1001'`
          - `email:'x@y.com' AND -metadata['archived']:'true'`
        """
        return self._get("/customers/search", query=query, limit=page_size).get("data", [])

    def list_charges(self, page_size: int = 100) -> List[dict]:
        return self._get("/charges", limit=page_size).get("data", [])

    def iter_charges(self, page_size: int = 100) -> Iterator[dict]:
        yield from self._iter_paginated("/charges", page_size=page_size)

    def list_payment_intents(self, page_size: int = 100) -> List[dict]:
        return self._get("/payment_intents", limit=page_size).get("data", [])

    def iter_payment_intents(self, page_size: int = 100) -> Iterator[dict]:
        yield from self._iter_paginated("/payment_intents", page_size=page_size)

    def list_invoices(
        self, customer: Optional[str] = None, status: Optional[str] = None, page_size: int = 100
    ) -> List[dict]:
        """List invoices. `status` = draft | open | paid | uncollectible | void."""
        params: dict = {"limit": page_size}
        if customer:
            params["customer"] = customer
        if status:
            params["status"] = status
        return self._get("/invoices", **params).get("data", [])

    def list_subscriptions(
        self, customer: Optional[str] = None, status: Optional[str] = None, page_size: int = 100
    ) -> List[dict]:
        params: dict = {"limit": page_size}
        if customer:
            params["customer"] = customer
        if status:
            params["status"] = status
        return self._get("/subscriptions", **params).get("data", [])

    def list_products(self, active: Optional[bool] = None, page_size: int = 100) -> List[dict]:
        params: dict = {"limit": page_size}
        if active is not None:
            params["active"] = "true" if active else "false"
        return self._get("/products", **params).get("data", [])

    def list_prices(
        self, product: Optional[str] = None, active: Optional[bool] = None, page_size: int = 100
    ) -> List[dict]:
        params: dict = {"limit": page_size}
        if product:
            params["product"] = product
        if active is not None:
            params["active"] = "true" if active else "false"
        return self._get("/prices", **params).get("data", [])

    def list_events(self, event_type: Optional[str] = None, page_size: int = 100) -> List[dict]:
        params: dict = {"limit": page_size}
        if event_type:
            params["type"] = event_type
        return self._get("/events", **params).get("data", [])

    # ----------------------------------------------------------------- writes

    def create_customer(
        self,
        email: Optional[str] = None,
        name: Optional[str] = None,
        description: Optional[str] = None,
        metadata: Optional[dict] = None,
        idempotency_key: Optional[str] = None,
        **extra_fields,
    ) -> dict:
        """Create a customer. Pass `idempotency_key` for safe retries."""
        data: dict = {**extra_fields}
        if email is not None:
            data["email"] = email
        if name is not None:
            data["name"] = name
        if description is not None:
            data["description"] = description
        if metadata:
            data["metadata"] = metadata
        return self._post("/customers", data=data, idempotency_key=idempotency_key)

    def update_customer(
        self,
        customer_id: str,
        email: Optional[str] = None,
        name: Optional[str] = None,
        description: Optional[str] = None,
        metadata: Optional[dict] = None,
        **extra_fields,
    ) -> dict:
        """Patch a customer's fields."""
        data: dict = {**extra_fields}
        if email is not None:
            data["email"] = email
        if name is not None:
            data["name"] = name
        if description is not None:
            data["description"] = description
        if metadata:
            data["metadata"] = metadata
        return self._post(f"/customers/{customer_id}", data=data)

    def delete_customer(self, customer_id: str) -> dict:
        """Delete a customer (Stripe soft-deletes — object becomes inaccessible but retained)."""
        return self._delete(f"/customers/{customer_id}")

    def create_payment_intent(
        self,
        amount: int,
        currency: str = "usd",
        customer: Optional[str] = None,
        description: Optional[str] = None,
        metadata: Optional[dict] = None,
        idempotency_key: Optional[str] = None,
        **extra_fields,
    ) -> dict:
        """Create a payment intent. `amount` is in the currency's smallest unit (cents)."""
        data: dict = {"amount": amount, "currency": currency, **extra_fields}
        if customer:
            data["customer"] = customer
        if description:
            data["description"] = description
        if metadata:
            data["metadata"] = metadata
        return self._post("/payment_intents", data=data, idempotency_key=idempotency_key)

    def create_refund(
        self,
        payment_intent: Optional[str] = None,
        charge: Optional[str] = None,
        amount: Optional[int] = None,
        reason: Optional[str] = None,
        metadata: Optional[dict] = None,
        idempotency_key: Optional[str] = None,
    ) -> dict:
        """Refund a payment intent or charge (partial or full)."""
        data: dict = {}
        if payment_intent:
            data["payment_intent"] = payment_intent
        if charge:
            data["charge"] = charge
        if amount is not None:
            data["amount"] = amount
        if reason:
            data["reason"] = reason
        if metadata:
            data["metadata"] = metadata
        return self._post("/refunds", data=data, idempotency_key=idempotency_key)

    def create_invoice_item(
        self,
        customer: str,
        amount: int,
        currency: str = "usd",
        description: Optional[str] = None,
        idempotency_key: Optional[str] = None,
    ) -> dict:
        """Add a line item to an upcoming invoice for a customer."""
        data: dict = {"customer": customer, "amount": amount, "currency": currency}
        if description:
            data["description"] = description
        return self._post("/invoiceitems", data=data, idempotency_key=idempotency_key)

    def create_invoice(
        self,
        customer: str,
        auto_advance: bool = True,
        collection_method: str = "charge_automatically",
        metadata: Optional[dict] = None,
        idempotency_key: Optional[str] = None,
    ) -> dict:
        """Create an invoice from any pending invoice items for the customer."""
        data: dict = {
            "customer": customer,
            "auto_advance": "true" if auto_advance else "false",
            "collection_method": collection_method,
        }
        if metadata:
            data["metadata"] = metadata
        return self._post("/invoices", data=data, idempotency_key=idempotency_key)

    def create_product(
        self,
        name: str,
        description: Optional[str] = None,
        metadata: Optional[dict] = None,
        idempotency_key: Optional[str] = None,
    ) -> dict:
        """Create a product."""
        data: dict = {"name": name}
        if description:
            data["description"] = description
        if metadata:
            data["metadata"] = metadata
        return self._post("/products", data=data, idempotency_key=idempotency_key)

    def create_price(
        self,
        product: str,
        unit_amount: int,
        currency: str = "usd",
        recurring_interval: Optional[str] = None,
        idempotency_key: Optional[str] = None,
    ) -> dict:
        """Create a price. `recurring_interval` = 'day' | 'week' | 'month' | 'year' for subscriptions."""
        data: dict = {"product": product, "unit_amount": unit_amount, "currency": currency}
        if recurring_interval:
            data["recurring"] = {"interval": recurring_interval}
        return self._post("/prices", data=data, idempotency_key=idempotency_key)


class StripeResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a StripeResource for use by other components.

    Example:
        ```yaml
        type: dagster_community_components.StripeResourceComponent
        attributes:
          resource_key: stripe
          api_key_env_var: STRIPE_API_KEY
        ```
    """

    resource_key: str = Field(
        default="stripe",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    api_key_env_var: str = Field(
        default="STRIPE_API_KEY",
        description="Env var holding a Stripe secret key (sk_test_... or sk_live_...).",
    )
    api_base_url: str = Field(
        default="https://api.stripe.com/v1",
        description="API base URL (override for mocks / edge locations).",
    )
    verify_ssl: bool = Field(default=True, description="TLS cert verification.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = StripeResource(
            api_key=dg.EnvVar(self.api_key_env_var),
            api_base_url=self.api_base_url,
            verify_ssl=self.verify_ssl,
        )
        return dg.Definitions(resources={self.resource_key: resource})
