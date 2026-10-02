"""BigCommerce Resource component.

Self-contained BigCommerce Catalog API (v3) workhorse using raw HTTP + a
static store-level API access token. Provides read + write convenience
methods for downstream sinks (`bigcommerce_product_upsert`) and custom
Dagster asset code.

Auth: store-level API account access token. Create one via Settings ->
API -> Store-level API accounts -> Create API Account (grant the scopes
you need, e.g. Products: read/write), then copy the generated Access
Token. Static header, no OAuth refresh cycle at request time:

    X-Auth-Token: <access_token>

Base URL: https://api.bigcommerce.com/stores/{store_hash}/v3

Convenience methods target the Catalog API v3:

    list_products(...)                # single page, {data, meta.pagination}
    iter_products(...)                 # page/limit iterator
    get_product(product_id)            # by numeric id, None on 404
    find_product_by_sku(sku)           # by main SKU (unique, case-insensitive)
    create_product(body)               # POST
    update_product(product_id, body)   # PUT (partial update)
    upsert_product_by_sku(sku, body)   # search-then-write
    delete_product(product_id)         # DELETE

Note: BigCommerce has no native upsert endpoint on Products. SKU is
unique (case-insensitive) across the WHOLE catalog (not per-variant --
see BigCommerce's own "Product SKU" docs). The resource's upsert method
uses a search-then-write pattern: GET /catalog/products?sku=<sku> -> PUT
by id if found, else POST.

Gotcha -- required fields on create: BigCommerce's Create Product
endpoint (POST /v3/catalog/products) requires at minimum `name`, `type`
('physical' or 'digital'), `weight`, and `price` in the request body
(`categories` is also required when the store has the V2 product
experience enabled in the control panel). PUT (update) is a partial
update and does NOT require these fields again.

Gotcha -- rate-limit backoff header: unlike many REST APIs (and unlike
Shopify, which uses the standard `Retry-After` header), BigCommerce does
NOT send `Retry-After` on a 429. It sends `X-Rate-Limit-Time-Reset-Ms`
(milliseconds until your quota resets) plus `X-Rate-Limit-Requests-Left`
/ `X-Rate-Limit-Requests-Quota` on every response. This resource's retry
wrapper reads `X-Rate-Limit-Time-Reset-Ms` (converted to seconds) to
decide how long to back off.

Response envelope: every v3 Catalog API response wraps its payload as
`{"data": ... , "meta": {...}}` -- a single object for get/create/update,
a list plus `meta.pagination` for list endpoints. This resource unwraps
`data` for callers.
"""
import time
from typing import Any, Dict, Iterator, List, Optional

import dagster as dg
from pydantic import Field


class BigCommerceResource(dg.ConfigurableResource):
    """BigCommerce Catalog API v3 workhorse: X-Auth-Token auth + read/write methods."""

    store_hash: str
    access_token: str
    api_version: str = "v3"
    request_timeout_seconds: int = 60
    max_retries: int = 3

    @property
    def base_url(self) -> str:
        # Normalize -- accept 'abc123def' or a full stores URL.
        store = self.store_hash.strip()
        for prefix in ("https://", "http://"):
            if store.startswith(prefix):
                store = store[len(prefix):]
        store = store.rstrip("/")
        if store.startswith("api.bigcommerce.com/stores/"):
            store = store[len("api.bigcommerce.com/stores/"):].split("/")[0]
        return f"https://api.bigcommerce.com/stores/{store}/{self.api_version}"

    def _headers(self) -> Dict[str, str]:
        return {
            "X-Auth-Token": self.access_token,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def _url(self, path: str) -> str:
        if not path.startswith("/"):
            path = "/" + path
        return f"{self.base_url}{path}"

    # ── HTTP wrapper ──────────────────────────────────────────────
    def _request(
        self,
        method: str,
        url: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Any] = None,
    ) -> Optional[Dict[str, Any]]:
        """Execute a request with retry on 429 / 5xx.

        BigCommerce does not send a standard `Retry-After` header on 429 --
        it sends `X-Rate-Limit-Time-Reset-Ms` (milliseconds). We honor that.
        Returns the parsed JSON envelope (`{'data': ..., 'meta': ...}`), or
        None on 404, or {} on a 204/empty body.
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
                reset_ms = r.headers.get("X-Rate-Limit-Time-Reset-Ms")
                try:
                    wait_seconds = float(reset_ms) / 1000.0 if reset_ms else 2.0
                except (TypeError, ValueError):
                    wait_seconds = 2.0
                if attempt >= self.max_retries:
                    r.raise_for_status()
                time.sleep(min(max(wait_seconds, 1.0), 30.0))
                continue
            if r.status_code in (500, 502, 503, 504):
                if attempt >= self.max_retries:
                    r.raise_for_status()
                time.sleep(min(2 ** attempt, 10))
                continue
            if r.status_code == 404:
                return None
            r.raise_for_status()
            if r.status_code == 204 or not r.content:
                return {}
            try:
                return r.json()
            except ValueError:
                return {"raw": r.text}
        if last_exc:
            raise last_exc
        return {}

    def _get(self, path: str, **params) -> Optional[Dict[str, Any]]:
        return self._request("GET", self._url(path), params=params)

    def _post(self, path: str, body: Any) -> Optional[Dict[str, Any]]:
        return self._request("POST", self._url(path), json_body=body)

    def _put(self, path: str, body: Any) -> Optional[Dict[str, Any]]:
        return self._request("PUT", self._url(path), json_body=body)

    def _delete(self, path: str) -> None:
        self._request("DELETE", self._url(path))

    # ── Product methods ───────────────────────────────────────────
    def list_products(
        self,
        *,
        sku: Optional[str] = None,
        limit: int = 50,
        page: int = 1,
        include: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Single page of GET /catalog/products.

        Returns the full envelope `{'data': [...], 'meta': {'pagination': {...}}}`.
        `sku` filters by the product's main SKU (exact match, case-insensitive).
        """
        params: Dict[str, Any] = {"limit": limit, "page": page}
        if sku:
            params["sku"] = sku
        if include:
            params["include"] = ",".join(include)
        payload = self._get("/catalog/products", **params)
        return payload or {"data": [], "meta": {}}

    def iter_products(
        self,
        *,
        include: Optional[List[str]] = None,
        page_size: int = 50,
        max_records: Optional[int] = None,
    ) -> Iterator[Dict[str, Any]]:
        """Paginate every product via BigCommerce's page/limit + meta.pagination.

        BigCommerce has no cursor/Link-header pagination (unlike Shopify) --
        it reports `meta.pagination.total_pages` / `current_page`, so this
        walks pages 1..total_pages.
        """
        emitted = 0
        page = 1
        while True:
            payload = self.list_products(include=include, limit=page_size, page=page)
            rows = payload.get("data") or []
            if not rows:
                return
            for row in rows:
                yield row
                emitted += 1
                if max_records is not None and emitted >= max_records:
                    return
            pagination = (payload.get("meta") or {}).get("pagination") or {}
            total_pages = pagination.get("total_pages")
            current_page = pagination.get("current_page", page)
            if not total_pages or current_page >= total_pages:
                return
            page += 1

    def get_product(self, product_id: int) -> Optional[Dict[str, Any]]:
        """GET /catalog/products/{id} -- returns None on 404."""
        payload = self._get(f"/catalog/products/{product_id}")
        if payload is None:
            return None
        return payload.get("data")

    def find_product_by_sku(self, sku: str) -> Optional[Dict[str, Any]]:
        """Look up a product by its main SKU (unique, case-insensitive across
        the whole catalog). Returns None if no match."""
        payload = self.list_products(sku=sku, limit=1)
        rows = payload.get("data") or []
        return rows[0] if rows else None

    def create_product(self, product_body: Dict[str, Any]) -> Dict[str, Any]:
        """POST /catalog/products.

        Requires at minimum `name`, `type` ('physical'|'digital'), `weight`,
        and `price` in `product_body` (`categories` too if the store has the
        V2 product experience enabled). Returns the created product.
        """
        payload = self._post("/catalog/products", product_body) or {}
        return payload.get("data") or {}

    def update_product(
        self, product_id: int, product_body: Dict[str, Any]
    ) -> Dict[str, Any]:
        """PUT /catalog/products/{id} -- partial update. Returns the updated product."""
        payload = self._put(f"/catalog/products/{product_id}", product_body) or {}
        return payload.get("data") or {}

    def upsert_product_by_sku(
        self, sku: str, product_body: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Search-then-write. Returns `{'action': 'created'|'updated', 'product': {...}}`.

        BigCommerce has no native upsert. This method looks up the product
        by its main SKU and either PUTs (if found) or POSTs (if not).
        """
        body_out = dict(product_body)
        body_out.setdefault("sku", sku)

        existing = self.find_product_by_sku(sku)
        if existing:
            product = self.update_product(existing["id"], body_out)
            return {"action": "updated", "product": product}
        product = self.create_product(body_out)
        return {"action": "created", "product": product}

    def delete_product(self, product_id: int) -> None:
        """DELETE /catalog/products/{id} -- returns None on success."""
        self._delete(f"/catalog/products/{product_id}")


class BigCommerceResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a BigCommerce Catalog API resource for use by other components.

    Uses a store-level API account access token (static, no OAuth refresh
    needed at request time):
      Settings -> API -> Store-level API accounts -> Create API Account.
      Grant the scopes you need (typically Products: read/write for the
      upsert sink).
      Copy the generated Access Token and store it in an env var.

    Pairs with:
      - `bigcommerce_ingestion` -- bulk pull from BigCommerce (dlt-backed).
      - `bigcommerce_product_upsert` -- reverse-ETL sink (search-by-sku upsert).

    Example:

        ```yaml
        type: dagster_component_templates.BigCommerceResourceComponent
        attributes:
          resource_key: bigcommerce
          store_hash: abc123def
          access_token_env_var: BIGCOMMERCE_ACCESS_TOKEN
        ```
    """

    resource_key: str = Field(
        default="bigcommerce",
        description="Resource key. Other components reference it via this name.",
    )
    store_hash: str = Field(
        description=(
            "BigCommerce store hash -- the identifier in your store's API "
            "path, e.g. the 'abc123def' in "
            "https://api.bigcommerce.com/stores/abc123def/v3."
        ),
    )
    access_token_env_var: str = Field(
        description=(
            "Env var holding the BigCommerce API access token (from the "
            "store-level API account you created under Settings -> API)."
        ),
    )
    api_version: str = Field(
        default="v3",
        description="BigCommerce Catalog API version path segment.",
    )
    request_timeout_seconds: int = Field(
        default=60,
        description="Per-request timeout in seconds.",
    )
    max_retries: int = Field(
        default=3,
        description=(
            "Retry attempts on 429 (honors BigCommerce's "
            "X-Rate-Limit-Time-Reset-Ms header) / 5xx."
        ),
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        import os
        access_token = os.environ.get(self.access_token_env_var, "")
        resource = BigCommerceResource(
            store_hash=self.store_hash,
            access_token=access_token,
            api_version=self.api_version,
            request_timeout_seconds=self.request_timeout_seconds,
            max_retries=self.max_retries,
        )
        return dg.Definitions(resources={self.resource_key: resource})
