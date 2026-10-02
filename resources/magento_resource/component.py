"""Magento / Adobe Commerce Resource component.

Self-contained Magento/Adobe Commerce REST API workhorse using raw HTTP +
a pre-generated admin/integration access token (static bearer, no OAuth
flow driven by this resource). Provides read + write convenience methods
for downstream sinks (`magento_product_upsert`) and custom Dagster asset
code.

Auth: Admin API access token. Generate one under Admin > System >
Integrations (create an integration, activate it, copy the generated
access token), or via `POST /V1/integration/admin/token` with admin
credentials. Static bearer, no refresh flow:

    Authorization: Bearer <admin_token>

Convenience methods target the standard REST API at
`{instance_url}/rest/V1`:

    get_product_by_sku(sku)             # GET  /products/:sku -- None on 404
    create_product(body)                # POST /products
    update_product(sku, body)           # PUT  /products/:sku
    upsert_product_by_sku(sku, body)    # search-then-write

IMPORTANT GOTCHA -- Magento's catalog product REST endpoint has NO native
upsert. `PUT /V1/products/:sku` is **update-ONLY**: if the SKU in the path
doesn't already exist, Magento does not create it -- it 404s / errors
instead. To create a new product you must `POST /V1/products` with a body
shaped `{"product": {"sku": ..., ...}}`. This resource's
`upsert_product_by_sku` does the required search-then-write dance (GET by
SKU -> PUT if found, else POST) the same way `ShopifyResource` does for
Shopify's equally upsert-less Products endpoint.

SECOND GOTCHA -- when updating, the `sku` inside the request body MUST
match the `sku` in the URL path. Passing a different SKU in the body than
in the path is the single most common Magento REST API footgun (Magento
will either reject the request or, worse, silently rename/alias the
product depending on version). `update_product` below always forces
`body["sku"] = sku` to make this impossible to get wrong by accident.
"""
import time
import urllib.parse
from typing import Any, Dict, Optional

import dagster as dg
from pydantic import Field


class MagentoResource(dg.ConfigurableResource):
    """Magento/Adobe Commerce REST API workhorse: bearer auth + read/write methods."""

    instance_url: str  # e.g. 'https://mystore.example.com' (self-hosted, no fixed SaaS domain)
    admin_token: str
    request_timeout_seconds: int = 60
    max_retries: int = 3
    attribute_set_id: int = 4  # Magento's default install ships a "Default" attribute set at id 4

    @property
    def base_url(self) -> str:
        url = self.instance_url.strip().rstrip("/")
        return f"{url}/rest/V1"

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.admin_token}",
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
    ) -> Any:
        """Execute a request with retry on 429 (honors Retry-After) / 5xx."""
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
            if isinstance(payload, dict):
                payload["_response"] = r
            return payload
        if last_exc:
            raise last_exc
        return {}

    def _get(self, path: str, **params) -> Any:
        return self._request("GET", self._url(path), params=params)

    def _post(self, path: str, body: Any) -> Dict[str, Any]:
        payload = self._request("POST", self._url(path), json_body=body) or {}
        if isinstance(payload, dict):
            payload.pop("_response", None)
        return payload

    def _put(self, path: str, body: Any) -> Dict[str, Any]:
        payload = self._request("PUT", self._url(path), json_body=body) or {}
        if isinstance(payload, dict):
            payload.pop("_response", None)
        return payload

    # ── Product methods ───────────────────────────────────────────
    def get_product_by_sku(self, sku: str) -> Optional[Dict[str, Any]]:
        """GET /products/:sku -- returns None on 404.

        SKUs can contain slashes/spaces, so the path segment is URL-encoded
        (Magento's router otherwise splits on an un-encoded '/').
        """
        encoded_sku = urllib.parse.quote(str(sku), safe="")
        try:
            return self._get(f"/products/{encoded_sku}")
        except Exception as e:
            import requests
            if isinstance(e, requests.HTTPError) and getattr(e, "response", None) is not None:
                if e.response.status_code == 404:
                    return None
            raise

    def create_product(self, product_body: Dict[str, Any]) -> Dict[str, Any]:
        """POST /products with `{'product': {...}}` body. Returns the created product.

        Magento requires at minimum `sku`, `name`, `price`, and
        `attribute_set_id` on create (plus, practically, `status`,
        `visibility`, and `type_id` -- otherwise the product is created
        disabled/hidden/typeless). Callers are expected to supply these;
        this method only fills `attribute_set_id` from the resource's
        configured default when the caller didn't supply one.
        """
        body_out = dict(product_body)
        body_out.setdefault("attribute_set_id", self.attribute_set_id)
        return self._post("/products", {"product": body_out})

    def update_product(self, sku: str, product_body: Dict[str, Any]) -> Dict[str, Any]:
        """PUT /products/:sku -- partial update. Returns the updated product.

        UPDATE-ONLY: Magento will NOT create a new product if `sku` doesn't
        already exist at this path -- use `create_product` / `upsert_product_by_sku`
        for that. The body's `sku` is always forced to match the path `sku`
        (the #1 Magento REST footgun is sending a mismatched body sku).
        """
        encoded_sku = urllib.parse.quote(str(sku), safe="")
        body_out = dict(product_body)
        body_out["sku"] = sku
        return self._put(f"/products/{encoded_sku}", {"product": body_out})

    def upsert_product_by_sku(self, sku: str, product_body: Dict[str, Any]) -> Dict[str, Any]:
        """Search-then-write. Returns `{'action': 'created'|'updated', 'product': {...}}`.

        Magento's catalog product endpoint has no native upsert -- `PUT
        /products/:sku` is update-only and will not create a missing SKU.
        This method looks the SKU up first (GET) and either PUTs (found) or
        POSTs (not found), mirroring `ShopifyResource.upsert_product_by_handle`.
        """
        existing = self.get_product_by_sku(sku)
        if existing:
            product = self.update_product(sku, product_body)
            return {"action": "updated", "product": product}
        body_out = dict(product_body)
        body_out.setdefault("sku", sku)
        product = self.create_product(body_out)
        return {"action": "created", "product": product}


class MagentoResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a Magento/Adobe Commerce REST API resource for use by other components.

    Uses a pre-generated admin/integration access token (long-lived, no
    OAuth flow): Admin > System > Integrations -> create + activate an
    integration -> copy the generated access token. Alternatively, generate
    one programmatically via `POST /V1/integration/admin/token`.

    Pairs with:
      - `magento_ingestion` -- bulk pull from Magento/Adobe Commerce (dlt-backed).
      - `magento_product_upsert` -- reverse-ETL sink (search-by-sku upsert).

    Example:

        ```yaml
        type: dagster_component_templates.MagentoResourceComponent
        attributes:
          resource_key: magento
          instance_url: "https://mystore.example.com"
          admin_token_env_var: MAGENTO_ADMIN_TOKEN
        ```
    """

    resource_key: str = Field(
        default="magento",
        description="Resource key. Other components reference it via this name.",
    )
    instance_url: str = Field(
        description=(
            "Your Magento/Adobe Commerce instance base URL, e.g. "
            "'https://mystore.example.com' (self-hosted -- no fixed SaaS domain)."
        ),
    )
    admin_token_env_var: str = Field(
        description=(
            "Env var holding the pre-generated Magento admin/integration access "
            "token (Admin > System > Integrations, or POST /V1/integration/admin/token)."
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
    attribute_set_id: int = Field(
        default=4,
        description=(
            "Default Magento attribute set Id used on product create when the "
            "sink's fields_map doesn't supply one. Default Magento installs ship "
            "a 'Default' attribute set at id 4."
        ),
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        import os
        admin_token = os.environ.get(self.admin_token_env_var, "")
        resource = MagentoResource(
            instance_url=self.instance_url,
            admin_token=admin_token,
            request_timeout_seconds=self.request_timeout_seconds,
            max_retries=self.max_retries,
            attribute_set_id=self.attribute_set_id,
        )
        return dg.Definitions(resources={self.resource_key: resource})
