"""Shopify Resource component.

Self-contained Shopify Admin API workhorse using raw HTTP + Admin API access
token auth. Provides read + write convenience methods for downstream sinks
(`shopify_product_upsert`) and custom Dagster asset code.

Auth: Admin API access token. Create a Custom App from Settings → Apps and
sales channels → Develop apps → Create an app, then request `read_products`
+ `write_products` (or the scopes you need) and copy the generated Admin API
access token. Static bearer, no OAuth flow:

    X-Shopify-Access-Token: <access_token>

Convenience methods target the standard REST Admin API v{api_version}:

    list_products(...)                     # paginated (Link header cursor)
    iter_products(...)                     # cursor-safe iterator
    get_product(product_id)                # by numeric Id
    find_product_by_handle(handle)         # by URL slug (unique per shop)
    create_product(body)                   # POST
    update_product(product_id, body)       # PUT
    upsert_product_by_handle(handle, body) # search-then-write
    delete_product(product_id)             # DELETE
    list_customers / iter_customers        # paginated read
    find_customer_by_email(email)          # search-by-email
    create_customer / update_customer      # POST / PUT

Note: Shopify has no native upsert endpoint on Products or Customers — the
resource's upsert methods use a search-then-write pattern (GET by unique
field → PUT or POST).
"""
import time
from typing import Any, Dict, Iterator, List, Optional

import dagster as dg
from pydantic import Field


class ShopifyResource(dg.ConfigurableResource):
    """Shopify Admin API workhorse: Admin API bearer auth + read/write methods."""

    shop_url: str      # e.g. 'mystore.myshopify.com' (no https://)
    access_token: str
    api_version: str = "2024-01"
    request_timeout_seconds: int = 60
    max_retries: int = 3

    @property
    def base_url(self) -> str:
        # Normalize — accept 'mystore', 'mystore.myshopify.com', or full URL
        shop = self.shop_url.strip()
        for prefix in ("https://", "http://"):
            if shop.startswith(prefix):
                shop = shop[len(prefix):]
        shop = shop.rstrip("/")
        if not shop.endswith(".myshopify.com"):
            shop = f"{shop}.myshopify.com"
        return f"https://{shop}/admin/api/{self.api_version}"

    def _headers(self) -> Dict[str, str]:
        return {
            "X-Shopify-Access-Token": self.access_token,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def _url(self, path: str) -> str:
        if not path.startswith("/"):
            path = "/" + path
        # Allow callers to pass an absolute-path Link cursor URL as-is.
        if path.startswith("/admin/api/"):
            for prefix in ("https://", "http://"):
                if path.startswith(prefix):
                    return path
            return f"https://{self.base_url.split('//')[1].split('/')[0]}{path}"
        return f"{self.base_url}{path}"

    # ── HTTP wrappers ─────────────────────────────────────────────
    def _request(
        self,
        method: str,
        url: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Any] = None,
    ) -> Any:
        """Execute a request with retry on 429 (honors Retry-After) / 5xx.

        Returns (payload, response) so callers can inspect Link headers for
        pagination cursors.
        """
        import requests
        last_exc = None
        for attempt in range(1, self.max_retries + 1):
            try:
                r = requests.request(
                    method, url,
                    headers=self._headers(),
                    params=params or {},
                    json=json_body,
                    timeout=self.request_timeout_seconds,
                )
            except requests.RequestException as e:
                last_exc = e
                if attempt >= self.max_retries:
                    raise
                time.sleep(min(2 ** attempt, 10))
                continue
            if r.status_code == 429:
                retry_after = float(r.headers.get("Retry-After", "2"))
                if attempt >= self.max_retries:
                    r.raise_for_status()
                time.sleep(min(max(retry_after, 1.0), 30.0))
                continue
            if r.status_code in (500, 502, 503, 504):
                if attempt >= self.max_retries:
                    r.raise_for_status()
                time.sleep(min(2 ** attempt, 10))
                continue
            r.raise_for_status()
            if r.status_code == 204 or not r.content:
                return {"_response": r}
            try:
                payload = r.json()
            except ValueError:
                payload = {"raw": r.text}
            payload["_response"] = r
            return payload
        if last_exc:
            raise last_exc
        return {}

    def _get_page(self, path_or_url: str, **params) -> Dict[str, Any]:
        return self._request("GET", self._url(path_or_url), params=params) or {}

    def _post(self, path: str, body: Any) -> Dict[str, Any]:
        payload = self._request("POST", self._url(path), json_body=body) or {}
        payload.pop("_response", None)
        return payload

    def _put(self, path: str, body: Any) -> Dict[str, Any]:
        payload = self._request("PUT", self._url(path), json_body=body) or {}
        payload.pop("_response", None)
        return payload

    def _delete(self, path: str) -> None:
        self._request("DELETE", self._url(path))

    @staticmethod
    def _next_cursor_from_link(link_header: str) -> Optional[str]:
        """Parse Shopify's Link header for the next page URL."""
        if not link_header:
            return None
        # Format: <https://.../admin/api/2024-01/products.json?page_info=...>; rel="next"
        for part in link_header.split(","):
            if 'rel="next"' in part:
                start = part.find("<")
                end = part.find(">", start)
                if start >= 0 and end > start:
                    return part[start + 1:end]
        return None

    # ── Product methods ───────────────────────────────────────────
    def list_products(
        self,
        *,
        handle: Optional[str] = None,
        fields: Optional[List[str]] = None,
        limit: int = 50,
        since_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Single page of `/products.json`. Returns full payload including `products` list."""
        params: Dict[str, Any] = {"limit": limit}
        if handle:
            params["handle"] = handle
        if fields:
            params["fields"] = ",".join(fields)
        if since_id is not None:
            params["since_id"] = since_id
        return self._get_page("/products.json", **params)

    def iter_products(
        self,
        *,
        fields: Optional[List[str]] = None,
        page_size: int = 50,
        max_records: Optional[int] = None,
    ) -> Iterator[Dict[str, Any]]:
        """Paginate every product via Shopify's Link-header cursor pattern."""
        emitted = 0
        params: Dict[str, Any] = {"limit": page_size}
        if fields:
            params["fields"] = ",".join(fields)
        payload = self._get_page("/products.json", **params)
        while payload:
            for prod in payload.get("products", []) or []:
                yield prod
                emitted += 1
                if max_records is not None and emitted >= max_records:
                    return
            response = payload.get("_response")
            link_header = response.headers.get("Link", "") if response is not None else ""
            next_url = self._next_cursor_from_link(link_header)
            if not next_url:
                return
            payload = self._get_page(next_url)

    def get_product(self, product_id: int) -> Optional[Dict[str, Any]]:
        """GET /products/{id}.json — returns None on 404."""
        try:
            payload = self._get_page(f"/products/{product_id}.json")
            return payload.get("product")
        except Exception as e:
            import requests
            if isinstance(e, requests.HTTPError) and getattr(e, "response", None) is not None:
                if e.response.status_code == 404:
                    return None
            raise

    def find_product_by_handle(self, handle: str) -> Optional[Dict[str, Any]]:
        """Look up a product by its URL handle. Returns None if no match."""
        payload = self.list_products(handle=handle, limit=1)
        products = payload.get("products") or []
        return products[0] if products else None

    def create_product(self, product_body: Dict[str, Any]) -> Dict[str, Any]:
        """POST /products.json with `{'product': {...}}` body. Returns the created product."""
        result = self._post("/products.json", {"product": product_body})
        return result.get("product") or {}

    def update_product(
        self, product_id: int, product_body: Dict[str, Any]
    ) -> Dict[str, Any]:
        """PUT /products/{id}.json — partial update. Returns the updated product."""
        result = self._put(f"/products/{product_id}.json", {"product": product_body})
        return result.get("product") or {}

    def upsert_product_by_handle(
        self, handle: str, product_body: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Search-then-write. Returns `{'action': 'created'|'updated', 'product': {...}}`.

        Shopify has no native upsert. This method looks up the product by handle
        and either PUTs (if found) or POSTs (if not).
        """
        body_out = dict(product_body)
        body_out.setdefault("handle", handle)

        existing = self.find_product_by_handle(handle)
        if existing:
            product = self.update_product(existing["id"], body_out)
            return {"action": "updated", "product": product}
        product = self.create_product(body_out)
        return {"action": "created", "product": product}

    def delete_product(self, product_id: int) -> None:
        """DELETE /products/{id}.json — returns None on success."""
        self._delete(f"/products/{product_id}.json")

    # ── Customer methods ──────────────────────────────────────────
    def list_customers(
        self,
        *,
        fields: Optional[List[str]] = None,
        limit: int = 50,
        since_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {"limit": limit}
        if fields:
            params["fields"] = ",".join(fields)
        if since_id is not None:
            params["since_id"] = since_id
        return self._get_page("/customers.json", **params)

    def iter_customers(
        self,
        *,
        fields: Optional[List[str]] = None,
        page_size: int = 50,
        max_records: Optional[int] = None,
    ) -> Iterator[Dict[str, Any]]:
        emitted = 0
        params: Dict[str, Any] = {"limit": page_size}
        if fields:
            params["fields"] = ",".join(fields)
        payload = self._get_page("/customers.json", **params)
        while payload:
            for cust in payload.get("customers", []) or []:
                yield cust
                emitted += 1
                if max_records is not None and emitted >= max_records:
                    return
            response = payload.get("_response")
            link_header = response.headers.get("Link", "") if response is not None else ""
            next_url = self._next_cursor_from_link(link_header)
            if not next_url:
                return
            payload = self._get_page(next_url)

    def find_customer_by_email(self, email: str) -> Optional[Dict[str, Any]]:
        """Look up a customer by email. Uses /customers/search.json under the hood."""
        payload = self._get_page(
            "/customers/search.json", query=f"email:{email}", limit=1
        )
        customers = payload.get("customers") or []
        return customers[0] if customers else None

    def create_customer(self, customer_body: Dict[str, Any]) -> Dict[str, Any]:
        result = self._post("/customers.json", {"customer": customer_body})
        return result.get("customer") or {}

    def update_customer(
        self, customer_id: int, customer_body: Dict[str, Any]
    ) -> Dict[str, Any]:
        result = self._put(
            f"/customers/{customer_id}.json", {"customer": customer_body}
        )
        return result.get("customer") or {}

    def upsert_customer_by_email(
        self, email: str, customer_body: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Search-then-write. Returns `{'action', 'customer'}`."""
        body_out = dict(customer_body)
        body_out.setdefault("email", email)
        existing = self.find_customer_by_email(email)
        if existing:
            customer = self.update_customer(existing["id"], body_out)
            return {"action": "updated", "customer": customer}
        customer = self.create_customer(body_out)
        return {"action": "created", "customer": customer}


class ShopifyResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a Shopify Admin API resource for use by other components.

    Uses a Custom App access token (long-lived, no OAuth):
      Settings → Apps and sales channels → Develop apps → Create an app.
      Request API scopes (typically `read_products`, `write_products`,
      `read_customers`, `write_customers`).
      Copy the generated Admin API access token and store it in an env var.

    Pairs with:
      - `shopify_ingestion` — bulk pull from Shopify (dlt-backed).
      - `shopify_product_upsert` — reverse-ETL sink (search-by-handle upsert).

    Example:

        ```yaml
        type: dagster_component_templates.ShopifyResourceComponent
        attributes:
          resource_key: shopify
          shop_url: mystore.myshopify.com
          access_token_env_var: SHOPIFY_ADMIN_TOKEN
          api_version: "2024-01"
        ```
    """

    resource_key: str = Field(
        default="shopify",
        description="Resource key. Other components reference it via this name.",
    )
    shop_url: str = Field(
        description=(
            "Shopify store URL — accepts 'mystore', 'mystore.myshopify.com', or "
            "the full URL. The resource normalizes it to the canonical form."
        ),
    )
    access_token_env_var: str = Field(
        description=(
            "Env var holding the Shopify Admin API access token (from the "
            "Custom App you created under Settings → Apps and sales channels → "
            "Develop apps)."
        ),
    )
    api_version: str = Field(
        default="2024-01",
        description="Shopify Admin API version (YYYY-MM). Bump quarterly to stay current.",
    )
    request_timeout_seconds: int = Field(
        default=60,
        description="Per-request timeout in seconds.",
    )
    max_retries: int = Field(
        default=3,
        description="Retry attempts on 429 (honors Retry-After) / 5xx.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        import os
        access_token = os.environ.get(self.access_token_env_var, "")
        resource = ShopifyResource(
            shop_url=self.shop_url,
            access_token=access_token,
            api_version=self.api_version,
            request_timeout_seconds=self.request_timeout_seconds,
            max_retries=self.max_retries,
        )
        return dg.Definitions(resources={self.resource_key: resource})
