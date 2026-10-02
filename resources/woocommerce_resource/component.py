"""WooCommerce Resource component.

Self-contained WooCommerce REST API v3 workhorse using raw HTTP + HTTP
Basic auth. Provides read + write convenience methods for downstream sinks
(`woocommerce_product_upsert`) and custom Dagster asset code.

Auth: WooCommerce REST API v3 authenticates with the **consumer key as the
HTTP Basic-auth username and the consumer secret as the HTTP Basic-auth
password**, sent over HTTPS. This is NOT an OAuth2 client-credentials /
refresh-token flow (unlike e.g. this repo's `outreach_resource`) — it is a
static, long-lived credential pair, generated once from WooCommerce ->
Settings -> Advanced -> REST API -> Add key, and used directly as HTTP
Basic auth on every request:

    Authorization: Basic base64(consumer_key:consumer_secret)

(There is also a query-string fallback, `?consumer_key=X&consumer_secret=Y`,
documented for servers that mis-parse the Authorization header over SSL,
and a separate OAuth1-over-query-string scheme for plain-HTTP (non-HTTPS)
stores. Both are out of scope here — this resource implements HTTP Basic
auth as the primary/only mode, matching this repo's existing
`woocommerce_ingestion` component.)

Convenience methods target the standard REST API v3:

    find_product_by_sku(sku)           # search by SKU (unique merge key)
    get_product(product_id)            # by numeric Id
    create_product(body)               # POST
    update_product(product_id, body)   # PUT
    upsert_product_by_sku(sku, body)   # search-then-write
    delete_product(product_id)         # DELETE
    batch_products(create, update, delete)  # POST /products/batch (bulk fast-path)

Note: WooCommerce has no native single-call upsert on Products — the
resource's `upsert_product_by_sku` method uses a search-then-write pattern
(GET by SKU -> PUT or POST). The `/products/batch` endpoint is available as
an optional bulk fast-path for callers that want to batch create/update in
one request.
"""
import time
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


class WooCommerceResource(dg.ConfigurableResource):
    """WooCommerce REST API v3 workhorse: HTTP Basic auth + read/write methods."""

    store_url: str  # e.g. 'https://mystore.com' (the /wp-json/wc/v3 path is appended)
    consumer_key: str
    consumer_secret: str
    request_timeout_seconds: int = 60
    max_retries: int = 3

    @property
    def base_url(self) -> str:
        return self.store_url.rstrip("/") + "/wp-json/wc/v3"

    def _auth(self):
        from requests.auth import HTTPBasicAuth
        return HTTPBasicAuth(self.consumer_key, self.consumer_secret)

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
    ) -> Any:
        """Execute a request with retry on 429 (honors Retry-After) / 5xx."""
        import requests
        last_exc = None
        for attempt in range(1, self.max_retries + 1):
            try:
                r = requests.request(
                    method, url,
                    auth=self._auth(),
                    headers={"Content-Type": "application/json", "Accept": "application/json"},
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
                return {}
            try:
                return r.json()
            except ValueError:
                return {"raw": r.text}
        if last_exc:
            raise last_exc
        return {}

    def _get(self, path: str, **params) -> Any:
        return self._request("GET", self._url(path), params=params)

    def _post(self, path: str, body: Any) -> Any:
        return self._request("POST", self._url(path), json_body=body)

    def _put(self, path: str, body: Any) -> Any:
        return self._request("PUT", self._url(path), json_body=body)

    def _delete(self, path: str, **params) -> Any:
        return self._request("DELETE", self._url(path), params=params)

    # ── Product methods ───────────────────────────────────────────
    def find_product_by_sku(self, sku: str) -> Optional[Dict[str, Any]]:
        """GET /products?sku=<sku> — returns None if no match."""
        payload = self._get("/products", sku=sku, per_page=1)
        products = payload if isinstance(payload, list) else []
        return products[0] if products else None

    def get_product(self, product_id: int) -> Optional[Dict[str, Any]]:
        """GET /products/{id} — returns None on 404."""
        try:
            return self._get(f"/products/{product_id}")
        except Exception as e:
            import requests
            if isinstance(e, requests.HTTPError) and getattr(e, "response", None) is not None:
                if e.response.status_code == 404:
                    return None
            raise

    def create_product(self, product_body: Dict[str, Any]) -> Dict[str, Any]:
        """POST /products. Returns the created product."""
        return self._post("/products", product_body) or {}

    def update_product(self, product_id: int, product_body: Dict[str, Any]) -> Dict[str, Any]:
        """PUT /products/{id} — partial update. Returns the updated product."""
        return self._put(f"/products/{product_id}", product_body) or {}

    def upsert_product_by_sku(self, sku: str, product_body: Dict[str, Any]) -> Dict[str, Any]:
        """Search-then-write. Returns `{'action': 'created'|'updated', 'product': {...}}`.

        WooCommerce has no native single-call upsert. This method looks up
        the product by SKU and either PUTs (if found) or POSTs (if not).
        """
        body_out = dict(product_body)
        body_out.setdefault("sku", sku)

        existing = self.find_product_by_sku(sku)
        if existing:
            product = self.update_product(existing["id"], body_out)
            return {"action": "updated", "product": product}
        product = self.create_product(body_out)
        return {"action": "created", "product": product}

    def delete_product(self, product_id: int, force: bool = True) -> Any:
        """DELETE /products/{id} — WooCommerce soft-deletes to trash unless force=True."""
        return self._delete(f"/products/{product_id}", force=force)

    def batch_products(
        self,
        *,
        create: Optional[List[Dict[str, Any]]] = None,
        update: Optional[List[Dict[str, Any]]] = None,
        delete: Optional[List[int]] = None,
    ) -> Dict[str, Any]:
        """POST /products/batch — optional bulk fast-path.

        Accepts up to 100 items per array (WooCommerce's documented batch
        limit). Returns the raw batch response `{'create': [...], 'update':
        [...], 'delete': [...]}`. Prefer the sink's default per-row
        search-then-write path unless you specifically need the throughput.
        """
        body: Dict[str, Any] = {}
        if create:
            body["create"] = create
        if update:
            body["update"] = update
        if delete:
            body["delete"] = delete
        return self._post("/products/batch", body) or {}


class WooCommerceResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a WooCommerce REST API v3 resource for use by other components.

    Uses a long-lived consumer key/secret pair (HTTP Basic auth, not OAuth2):
      WooCommerce Admin -> Settings -> Advanced -> REST API -> Add key.
      Grant Read/Write permissions and copy the generated consumer key +
      consumer secret into env vars.

    Pairs with:
      - `woocommerce_ingestion` — bulk pull from WooCommerce (dlt-backed).
      - `woocommerce_product_upsert` — reverse-ETL sink (search-by-SKU upsert).

    Example:

        ```yaml
        type: dagster_component_templates.WooCommerceResourceComponent
        attributes:
          resource_key: woocommerce
          store_url: https://mystore.com
          consumer_key_env_var: WOOCOMMERCE_CONSUMER_KEY
          consumer_secret_env_var: WOOCOMMERCE_CONSUMER_SECRET
        ```
    """

    resource_key: str = Field(
        default="woocommerce",
        description="Resource key. Other components reference it via this name.",
    )
    store_url: str = Field(
        description=(
            "Base URL of your WooCommerce store, e.g. 'https://mystore.com'. "
            "The '/wp-json/wc/v3' REST API path is appended internally."
        ),
    )
    consumer_key_env_var: str = Field(
        description=(
            "Env var holding the WooCommerce REST API consumer key (from "
            "Settings -> Advanced -> REST API -> Add key). Used as the HTTP "
            "Basic-auth username."
        ),
    )
    consumer_secret_env_var: str = Field(
        description=(
            "Env var holding the WooCommerce REST API consumer secret. Used "
            "as the HTTP Basic-auth password."
        ),
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
        consumer_key = os.environ.get(self.consumer_key_env_var, "")
        consumer_secret = os.environ.get(self.consumer_secret_env_var, "")
        resource = WooCommerceResource(
            store_url=self.store_url,
            consumer_key=consumer_key,
            consumer_secret=consumer_secret,
            request_timeout_seconds=self.request_timeout_seconds,
            max_retries=self.max_retries,
        )
        return dg.Definitions(resources={self.resource_key: resource})
