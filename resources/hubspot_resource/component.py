"""HubSpot Resource component.

Self-contained HubSpot CRM API workhorse using raw HTTP + Private App bearer
token auth. Provides read + write convenience methods for downstream sinks
(`hubspot_object_upsert`) and custom Dagster asset code.

Auth: Private App access token (created under Settings → Integrations →
Private Apps in the HubSpot account). Static bearer, no OAuth flow:

    Authorization: Bearer <access_token>

No refresh needed — Private App tokens are long-lived until revoked.

Convenience methods (all use CRM v3 API):

    list_objects(object_type, properties, limit)     # single page
    iter_objects(object_type, properties, page_size) # paginated iterator
    get_object(object_type, id, properties)          # single by internal Id
    get_object_by_property(object_type, property, value, properties)
                                                     # single by alternate Id
    create_object(object_type, properties)           # POST
    update_object(object_type, id, properties)       # PATCH
    upsert_objects(object_type, key_property, records, batch_size)
                                                     # native batch upsert
    delete_object(object_type, id)                   # DELETE

Object types include the standard set (`contacts`, `companies`, `deals`,
`tickets`, `line_items`, `products`, `quotes`) plus any custom objects
you've defined (use the fully-qualified name like `p123456_custom_thing`).
"""
import time
from typing import Any, Dict, Iterator, List, Optional

import dagster as dg
from pydantic import Field


class HubSpotResource(dg.ConfigurableResource):
    """HubSpot CRM API workhorse: Private App bearer auth + read/write methods."""

    access_token: str
    base_url: str = "https://api.hubapi.com"
    request_timeout_seconds: int = 60
    max_retries: int = 3

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def _url(self, path: str) -> str:
        if not path.startswith("/"):
            path = "/" + path
        return f"{self.base_url}{path}"

    # ── HTTP wrappers ─────────────────────────────────────────────
    def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Any] = None,
    ) -> Any:
        """Execute a request with retry on 429 / 5xx."""
        import requests
        last_exc = None
        for attempt in range(1, self.max_retries + 1):
            try:
                r = requests.request(
                    method, self._url(path),
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
                # HubSpot returns Retry-After on rate limit — honor it.
                retry_after = int(r.headers.get("Retry-After", "1"))
                if attempt >= self.max_retries:
                    r.raise_for_status()
                time.sleep(min(max(retry_after, 1), 30))
                continue
            if r.status_code in (500, 502, 503, 504):
                if attempt >= self.max_retries:
                    r.raise_for_status()
                time.sleep(min(2 ** attempt, 10))
                continue
            if r.status_code == 204:
                return None
            r.raise_for_status()
            if not r.content:
                return None
            try:
                return r.json()
            except ValueError:
                return {"raw": r.text}
        if last_exc:
            raise last_exc
        return None

    def _get(self, path: str, **params) -> Any:
        return self._request("GET", path, params=params)

    def _post(self, path: str, body: Any) -> Any:
        return self._request("POST", path, json_body=body)

    def _patch(self, path: str, body: Any) -> Any:
        return self._request("PATCH", path, json_body=body)

    def _delete(self, path: str) -> Any:
        return self._request("DELETE", path)

    # ── Read methods ──────────────────────────────────────────────
    def list_objects(
        self,
        object_type: str,
        *,
        properties: Optional[List[str]] = None,
        after: Optional[str] = None,
        limit: int = 100,
    ) -> Dict[str, Any]:
        """Single page of `/crm/v3/objects/{objectType}`.

        Response shape: `{'results': [...], 'paging': {'next': {'after': ...}}}`.
        Use `iter_objects` for cursor-safe pagination.
        """
        params: Dict[str, Any] = {"limit": limit}
        if properties:
            params["properties"] = ",".join(properties)
        if after:
            params["after"] = after
        return self._get(f"/crm/v3/objects/{object_type}", **params) or {}

    def iter_objects(
        self,
        object_type: str,
        *,
        properties: Optional[List[str]] = None,
        page_size: int = 100,
        max_records: Optional[int] = None,
    ) -> Iterator[Dict[str, Any]]:
        """Paginate through every object of the given type."""
        emitted = 0
        after: Optional[str] = None
        while True:
            page = self.list_objects(
                object_type,
                properties=properties,
                after=after,
                limit=page_size,
            )
            for rec in page.get("results", []) or []:
                yield rec
                emitted += 1
                if max_records is not None and emitted >= max_records:
                    return
            next_cursor = (page.get("paging") or {}).get("next", {}).get("after")
            if not next_cursor:
                return
            after = next_cursor

    def get_object(
        self,
        object_type: str,
        object_id: str,
        *,
        properties: Optional[List[str]] = None,
        id_property: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """GET a single object by internal Id — or by an alternate unique property.

        Set `id_property` to look up by (say) email:
            get_object('contacts', 'me@example.com', id_property='email')
        """
        params: Dict[str, Any] = {}
        if properties:
            params["properties"] = ",".join(properties)
        if id_property:
            params["idProperty"] = id_property
        try:
            return self._get(f"/crm/v3/objects/{object_type}/{object_id}", **params)
        except Exception as e:
            import requests
            if isinstance(e, requests.HTTPError) and getattr(e, "response", None) is not None:
                if e.response.status_code == 404:
                    return None
            raise

    def get_object_by_property(
        self,
        object_type: str,
        property_name: str,
        property_value: Any,
        *,
        properties: Optional[List[str]] = None,
    ) -> Optional[Dict[str, Any]]:
        """Convenience wrapper for `get_object(id_property=property_name)`."""
        return self.get_object(
            object_type,
            str(property_value),
            properties=properties,
            id_property=property_name,
        )

    # ── Write methods ─────────────────────────────────────────────
    def create_object(
        self, object_type: str, properties: Dict[str, Any]
    ) -> Dict[str, Any]:
        """POST — create a new object with the given properties dict."""
        body = {"properties": properties}
        return self._post(f"/crm/v3/objects/{object_type}", body) or {}

    def update_object(
        self, object_type: str, object_id: str, properties: Dict[str, Any]
    ) -> Dict[str, Any]:
        """PATCH — update object by Id with a properties dict."""
        body = {"properties": properties}
        return self._patch(f"/crm/v3/objects/{object_type}/{object_id}", body) or {}

    def upsert_objects(
        self,
        object_type: str,
        key_property: str,
        records: List[Dict[str, Any]],
        *,
        batch_size: int = 100,
    ) -> List[Dict[str, Any]]:
        """Native HubSpot batch upsert.

        `POST /crm/v3/objects/{objectType}/batch/upsert` — for each record,
        HubSpot atomically creates or updates based on matching `key_property`.
        Handles up to 100 records per call (HubSpot's limit); this method
        chunks larger inputs automatically.

        Records must be a list of plain properties dicts. Each must include
        the `key_property` field as a scalar.

        Returns a list of the API responses, one per chunk. Each response
        contains `results` (list of upserted objects) + `numErrors` +
        `errors` if any records failed.
        """
        all_results: List[Dict[str, Any]] = []
        chunk = max(1, min(batch_size, 100))
        for start in range(0, len(records), chunk):
            slice_ = records[start:start + chunk]
            payload_records = []
            for rec in slice_:
                if key_property not in rec:
                    raise ValueError(
                        f"upsert_objects: record missing key_property "
                        f"{key_property!r}: {rec}"
                    )
                payload_records.append({
                    "idProperty": key_property,
                    "id": str(rec[key_property]),
                    "properties": rec,
                })
            body = {"inputs": payload_records}
            resp = self._post(
                f"/crm/v3/objects/{object_type}/batch/upsert", body
            ) or {}
            all_results.append(resp)
        return all_results

    def delete_object(self, object_type: str, object_id: str) -> None:
        """DELETE by internal Id. Returns None on success."""
        self._delete(f"/crm/v3/objects/{object_type}/{object_id}")


class HubSpotResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a HubSpot CRM API resource for use by other components.

    Uses Private App bearer token auth (long-lived, no OAuth flow):
      Settings → Integrations → Private Apps → Create a Private App.
      Set the scopes needed (usually `crm.objects.contacts.read`,
      `crm.objects.contacts.write`, `crm.objects.companies.read`,
      `crm.objects.companies.write`, `crm.objects.deals.read`,
      `crm.objects.deals.write`, `crm.schemas.custom.read`), then copy
      the generated access token and store it in an env var.

    Pairs with:
      - `hubspot_ingestion` — bulk pull from HubSpot (dlt-backed).
      - `hubspot_object_upsert` — reverse-ETL sink (native batch upsert).

    Example:

        ```yaml
        type: dagster_component_templates.HubSpotResourceComponent
        attributes:
          resource_key: hubspot
          access_token_env_var: HUBSPOT_ACCESS_TOKEN
        ```
    """

    resource_key: str = Field(
        default="hubspot",
        description="Resource key. Other components reference it via this name.",
    )
    access_token_env_var: str = Field(
        description=(
            "Env var holding the HubSpot Private App access token (from "
            "Settings → Integrations → Private Apps)."
        ),
    )
    base_url: str = Field(
        default="https://api.hubapi.com",
        description="HubSpot API base URL. Default matches all standard portals.",
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
        resource = HubSpotResource(
            access_token=access_token,
            base_url=self.base_url,
            request_timeout_seconds=self.request_timeout_seconds,
            max_retries=self.max_retries,
        )
        return dg.Definitions(resources={self.resource_key: resource})
