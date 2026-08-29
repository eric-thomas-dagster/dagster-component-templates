"""ServiceNow Resource component.

Provides shared connection config (instance + auth) for the ServiceNow Table
API plus a workhorse HTTP client with read + write convenience methods. Other
components (`servicenow_ingestion`, `servicenow_record_upsert`,
`servicenow_sensor`) reference this resource so credentials and the instance
subdomain are centralized.

Two auth modes:

  1. **Basic auth** — username + password env vars. Right for ServiceNow
     Developer Instances (PDIs) and dev sandboxes.
  2. **Bearer token** — OAuth bearer token via env var. Pair with the
     community `oauth_token_resource` component to acquire + rotate the
     token from a ServiceNow OAuth app (client_credentials grant works
     headlessly; refresh_token grant for delegated flows).

Convenience methods (all use the Table API):

    describe_table(table)                      # sys_dictionary metadata
    list_records(table, query, fields, limit)  # single page
    iter_records(table, query, page_size)      # paginated iterator
    get_record(table, sys_id)                  # single record by sys_id
    find_record(table, key_field, key_value)   # lookup by any field
    create_record(table, body)                 # POST
    update_record(table, sys_id, body)         # PATCH
    upsert_record(table, key_field, key_value, body)  # search-then-write
    delete_record(table, sys_id)               # DELETE
"""
import time
import urllib.parse
from typing import Any, Dict, Iterator, List, Optional

import dagster as dg
from pydantic import Field


class ServiceNowResource(dg.ConfigurableResource):
    """ServiceNow Table API workhorse: connection + read/write convenience methods."""

    instance: str  # e.g. 'mycompany' (no scheme, no .service-now.com)
    username: Optional[str] = None
    password: Optional[str] = None
    bearer_token: Optional[str] = None
    verify_ssl: bool = True
    request_timeout_seconds: int = 60
    max_retries: int = 3

    # ── URL / auth helpers ────────────────────────────────────────
    @property
    def base_url(self) -> str:
        return f"https://{self.instance}.service-now.com"

    def table_url(self, table: str) -> str:
        return f"{self.base_url}/api/now/table/{urllib.parse.quote(table)}"

    def get_auth_headers(self) -> dict:
        """Headers for the request. Bearer token when set."""
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        if self.bearer_token:
            headers["Authorization"] = f"Bearer {self.bearer_token}"
        return headers

    def get_auth(self):
        """Returns a (user, pass) tuple for requests' basic-auth, or None."""
        if self.bearer_token:
            return None
        if self.username and self.password:
            return (self.username, self.password)
        return None

    # ── HTTP wrappers ─────────────────────────────────────────────
    def _request(
        self,
        method: str,
        url: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Execute a request with basic retry on 5xx / 429."""
        import requests
        headers = self.get_auth_headers()
        auth = self.get_auth()
        last_exc = None
        for attempt in range(1, self.max_retries + 1):
            try:
                r = requests.request(
                    method, url,
                    headers=headers,
                    auth=auth,
                    params=params or {},
                    json=json_body,
                    timeout=self.request_timeout_seconds,
                    verify=self.verify_ssl,
                )
            except requests.RequestException as e:
                last_exc = e
                if attempt >= self.max_retries:
                    raise
                time.sleep(min(2 ** attempt, 10))
                continue
            if r.status_code in (429, 500, 502, 503, 504):
                if attempt >= self.max_retries:
                    r.raise_for_status()
                time.sleep(min(2 ** attempt, 10))
                continue
            r.raise_for_status()
            if not r.content:
                return {}
            try:
                return r.json()
            except ValueError:
                return {"raw": r.text}
        if last_exc:
            raise last_exc
        return {}

    def _get(self, url: str, **params) -> Dict[str, Any]:
        return self._request("GET", url, params=params)

    def _post(self, url: str, body: Dict[str, Any], **params) -> Dict[str, Any]:
        return self._request("POST", url, params=params, json_body=body)

    def _patch(self, url: str, body: Dict[str, Any], **params) -> Dict[str, Any]:
        return self._request("PATCH", url, params=params, json_body=body)

    def _delete(self, url: str, **params) -> Dict[str, Any]:
        return self._request("DELETE", url, params=params)

    # ── Read methods ──────────────────────────────────────────────
    def describe_table(self, table: str) -> List[Dict[str, Any]]:
        """Return sys_dictionary columns for `table` — name / type / max_length / mandatory."""
        rows = self.list_records(
            "sys_dictionary",
            query=f"name={table}^internal_type!=collection",
            fields=["element", "internal_type", "max_length", "mandatory"],
            limit=1000,
        )
        return rows

    def list_records(
        self,
        table: str,
        *,
        query: Optional[str] = None,
        fields: Optional[List[str]] = None,
        limit: int = 100,
        offset: int = 0,
        display_value: bool = False,
    ) -> List[Dict[str, Any]]:
        """Single-page list. Use `iter_records` for cursor-safe pagination."""
        params: Dict[str, Any] = {
            "sysparm_limit": limit,
            "sysparm_offset": offset,
        }
        if query:
            params["sysparm_query"] = query
        if fields:
            params["sysparm_fields"] = ",".join(fields)
        if display_value:
            params["sysparm_display_value"] = "true"
        data = self._get(self.table_url(table), **params)
        return data.get("result", [])

    def iter_records(
        self,
        table: str,
        *,
        query: Optional[str] = None,
        fields: Optional[List[str]] = None,
        page_size: int = 100,
        display_value: bool = False,
        max_records: Optional[int] = None,
    ) -> Iterator[Dict[str, Any]]:
        """Paginate through every matching record. Stops at max_records if set."""
        offset = 0
        emitted = 0
        while True:
            batch = self.list_records(
                table,
                query=query,
                fields=fields,
                limit=page_size,
                offset=offset,
                display_value=display_value,
            )
            if not batch:
                return
            for r in batch:
                yield r
                emitted += 1
                if max_records is not None and emitted >= max_records:
                    return
            if len(batch) < page_size:
                return
            offset += page_size

    def get_record(self, table: str, sys_id: str) -> Optional[Dict[str, Any]]:
        """Fetch a single record by sys_id."""
        url = f"{self.table_url(table)}/{urllib.parse.quote(sys_id)}"
        data = self._get(url)
        return data.get("result")

    def find_record(
        self, table: str, key_field: str, key_value: Any
    ) -> Optional[Dict[str, Any]]:
        """Return the first record where `key_field == key_value`, or None."""
        matches = self.list_records(
            table,
            query=f"{key_field}={key_value}",
            fields=None,
            limit=1,
        )
        return matches[0] if matches else None

    # ── Write methods ─────────────────────────────────────────────
    def create_record(self, table: str, body: Dict[str, Any]) -> Dict[str, Any]:
        """POST — create a new record. Returns the created row (with sys_id)."""
        data = self._post(self.table_url(table), body)
        return data.get("result", {})

    def update_record(
        self, table: str, sys_id: str, body: Dict[str, Any]
    ) -> Dict[str, Any]:
        """PATCH — partial update by sys_id. Returns the updated row."""
        url = f"{self.table_url(table)}/{urllib.parse.quote(sys_id)}"
        data = self._patch(url, body)
        return data.get("result", {})

    def upsert_record(
        self,
        table: str,
        key_field: str,
        key_value: Any,
        body: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Search-then-write. Returns {"action": "created"|"updated", "record": {...}}.

        ServiceNow has no native upsert endpoint — this does a `find_record`
        lookup and either PATCHes the match or POSTs a new record. Not atomic
        under concurrent writes; consumers relying on strict uniqueness should
        add a unique index on `key_field` in ServiceNow.
        """
        # Body must NOT overwrite the key field with a different value.
        body_out = dict(body)
        body_out.setdefault(key_field, key_value)

        existing = self.find_record(table, key_field, key_value)
        if existing:
            record = self.update_record(table, existing["sys_id"], body_out)
            return {"action": "updated", "record": record}
        record = self.create_record(table, body_out)
        return {"action": "created", "record": record}

    def delete_record(self, table: str, sys_id: str) -> None:
        """DELETE by sys_id. Returns None."""
        url = f"{self.table_url(table)}/{urllib.parse.quote(sys_id)}"
        self._delete(url)


class ServiceNowResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a ServiceNow Table API resource for use by other components.

    Pairs with:
      - `servicenow_ingestion` — read any ServiceNow table into a DataFrame.
      - `servicenow_record_upsert` — mirror a DataFrame INTO a ServiceNow table
        (reverse ETL, search-then-write per row).
      - `servicenow_sensor` — trigger on ServiceNow state changes.

    Example (Developer Instance — basic auth):

        ```yaml
        type: dagster_component_templates.ServiceNowResourceComponent
        attributes:
          resource_key: servicenow_resource
          instance_env_var: SNOW_INSTANCE
          username_env_var: SNOW_USERNAME
          password_env_var: SNOW_PASSWORD
        ```

    Example (Production — OAuth bearer token):

        ```yaml
        type: dagster_component_templates.ServiceNowResourceComponent
        attributes:
          resource_key: servicenow_resource
          instance_env_var: SNOW_INSTANCE
          bearer_token_env_var: SNOW_ACCESS_TOKEN
        ```

    Pair with `oauth_token_resource` to acquire the bearer_token via OAuth
    client_credentials grant (headless) or refresh_token rotation (delegated).
    """

    resource_key: str = Field(
        default="servicenow_resource",
        description="Resource key. Other components reference it via this name.",
    )
    instance_env_var: str = Field(
        description=(
            "Env var with the ServiceNow instance subdomain (e.g. 'mycompany' or "
            "'dev123456'). Do NOT include 'https://' or '.service-now.com' — just "
            "the subdomain."
        ),
    )
    username_env_var: Optional[str] = Field(
        default=None,
        description="Env var with ServiceNow username (for basic auth). Required unless bearer_token_env_var is set.",
    )
    password_env_var: Optional[str] = Field(
        default=None,
        description="Env var with ServiceNow password (for basic auth). Required unless bearer_token_env_var is set.",
    )
    bearer_token_env_var: Optional[str] = Field(
        default=None,
        description=(
            "Env var holding a ServiceNow OAuth bearer token. When set, basic-auth "
            "fields are ignored. Pair with `oauth_token_resource` to acquire + "
            "rotate the token."
        ),
    )
    verify_ssl: bool = Field(
        default=True,
        description="Enable TLS certificate verification (only set false for self-signed dev instances).",
    )
    request_timeout_seconds: int = Field(
        default=60,
        description="Per-request timeout in seconds.",
    )
    max_retries: int = Field(
        default=3,
        description="Retry attempts on 429 / 5xx / transient network errors (exponential backoff, capped at 10s).",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        import os

        has_bearer = bool(self.bearer_token_env_var)
        has_basic = bool(self.username_env_var and self.password_env_var)
        if not (has_bearer or has_basic):
            raise ValueError(
                "ServiceNowResourceComponent: provide either bearer_token_env_var "
                "OR (username_env_var + password_env_var)."
            )

        resource = ServiceNowResource(
            instance=os.environ.get(self.instance_env_var, ""),
            username=os.environ.get(self.username_env_var, "") if self.username_env_var else None,
            password=os.environ.get(self.password_env_var, "") if self.password_env_var else None,
            bearer_token=os.environ.get(self.bearer_token_env_var, "") if self.bearer_token_env_var else None,
            verify_ssl=self.verify_ssl,
            request_timeout_seconds=self.request_timeout_seconds,
            max_retries=self.max_retries,
        )
        return dg.Definitions(resources={self.resource_key: resource})
