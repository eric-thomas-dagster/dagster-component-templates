"""Salesforce Resource component.

Self-contained Salesforce REST API workhorse — OAuth password flow for auth,
raw HTTP for the API surface. Provides read + write convenience methods for
downstream sink / read components (`salesforce_record_upsert`, custom SOQL
readers, etc.).

Auth: OAuth 2.0 password grant against a Connected App:

    POST /services/oauth2/token
        grant_type=password
        client_id=<consumer_key>
        client_secret=<consumer_secret>
        username=<username>
        password=<password><security_token>   # concatenated

Returns access_token + instance_url. Subsequent calls hit
`{instance_url}/services/data/v{api_version}/sobjects/...` with
`Authorization: Bearer <access_token>`.

Convenience methods:

    describe_sobject(sobject)                        # metadata
    soql(query, batch_size)                          # SOQL cursor (paginated)
    get_record(sobject, id)                          # single record by Id
    find_record(sobject, ext_id_field, ext_id_value) # single record by external Id
    create_record(sobject, body)                     # POST
    update_record(sobject, id, body)                 # PATCH by Id
    upsert_record(sobject, ext_id_field, ext_id_value, body)  # native upsert
    delete_record(sobject, id)                       # DELETE
    composite_upsert(sobject, ext_id_field, records) # batch upsert (up to 200)

Note: Salesforce's native upsert is one of the cleanest write APIs of any SaaS —
`PATCH /services/data/vXX.0/sobjects/{Object}/{ExtIdField}/{ExtIdValue}` atomically
creates or updates in a single call, using any custom field marked `External ID` as
the merge key.
"""
import time
from typing import Any, Dict, Iterator, List, Optional

import dagster as dg
from pydantic import Field


class SalesforceResource(dg.ConfigurableResource):
    """Salesforce REST API workhorse: OAuth password auth + read/write methods."""

    username: str
    password: str
    security_token: str = ""
    consumer_key: str
    consumer_secret: str
    domain: str = "login"                # 'login' (prod), 'test' (sandbox), or custom
    api_version: str = "58.0"            # bump when you rely on newer surface
    request_timeout_seconds: int = 60
    max_retries: int = 3

    # Internal state populated lazily (kept out of the pydantic surface).
    def _access(self) -> Dict[str, str]:
        """Return {'access_token', 'instance_url'} — cached per instance."""
        cache = getattr(self, "__auth_cache__", None)
        if cache is not None:
            return cache
        cache = self._login()
        object.__setattr__(self, "__auth_cache__", cache)
        return cache

    def _login(self) -> Dict[str, str]:
        import requests
        url = f"https://{self.domain}.salesforce.com/services/oauth2/token"
        payload = {
            "grant_type": "password",
            "client_id": self.consumer_key,
            "client_secret": self.consumer_secret,
            "username": self.username,
            "password": self.password + (self.security_token or ""),
        }
        r = requests.post(
            url, data=payload, timeout=self.request_timeout_seconds
        )
        if r.status_code != 200:
            raise RuntimeError(
                f"Salesforce OAuth login failed ({r.status_code}): {r.text}"
            )
        payload_out = r.json()
        return {
            "access_token": payload_out["access_token"],
            "instance_url": payload_out["instance_url"],
        }

    def _refresh(self) -> None:
        """Force a re-login on the next request (call after a 401)."""
        try:
            object.__delattr__(self, "__auth_cache__")
        except AttributeError:
            pass

    # ── HTTP wrappers ─────────────────────────────────────────────
    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self._access()['access_token']}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    def _url(self, path: str) -> str:
        base = self._access()["instance_url"]
        # `path` may or may not include leading slash — normalize
        if not path.startswith("/"):
            path = "/" + path
        if not path.startswith("/services/data/"):
            path = f"/services/data/v{self.api_version}{path}"
        return base + path

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Any] = None,
    ) -> Any:
        """Execute a request with retry on 401 (token refresh) / 429 / 5xx."""
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
            if r.status_code == 401:
                # Expired access token — invalidate + retry once.
                self._refresh()
                if attempt >= self.max_retries:
                    r.raise_for_status()
                continue
            if r.status_code in (429, 500, 502, 503, 504):
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
    def describe_sobject(self, sobject: str) -> Dict[str, Any]:
        """Return the field metadata for an SObject (name / type / creatable / updateable / …)."""
        return self._get(f"/sobjects/{sobject}/describe") or {}

    def soql(
        self, query: str, *, batch_size: int = 2000, max_records: Optional[int] = None
    ) -> Iterator[Dict[str, Any]]:
        """Iterate every record from a SOQL query, transparently following nextRecordsUrl."""
        params = {"q": query}
        # Salesforce doesn't have a client-side batch_size for /query — it
        # returns up to 2000 records per response and includes nextRecordsUrl
        # when there are more. batch_size is informational for downstream.
        _ = batch_size
        emitted = 0
        payload = self._get("/query", **params)
        while payload:
            for rec in payload.get("records", []) or []:
                yield rec
                emitted += 1
                if max_records is not None and emitted >= max_records:
                    return
            next_url = payload.get("nextRecordsUrl")
            if not next_url:
                return
            # nextRecordsUrl is absolute-path shaped — normalized by _url
            payload = self._get(next_url)

    def get_record(self, sobject: str, record_id: str) -> Optional[Dict[str, Any]]:
        """GET a single record by Id. Returns None on 404."""
        try:
            return self._get(f"/sobjects/{sobject}/{record_id}")
        except Exception as e:
            # requests wraps 404 as HTTPError — treat as absent
            import requests
            if isinstance(e, requests.HTTPError) and getattr(e, "response", None) is not None:
                if e.response.status_code == 404:
                    return None
            raise

    def find_record(
        self, sobject: str, ext_id_field: str, ext_id_value: Any
    ) -> Optional[Dict[str, Any]]:
        """GET a single record by external Id field. Returns None on 404."""
        try:
            return self._get(f"/sobjects/{sobject}/{ext_id_field}/{ext_id_value}")
        except Exception as e:
            import requests
            if isinstance(e, requests.HTTPError) and getattr(e, "response", None) is not None:
                if e.response.status_code == 404:
                    return None
            raise

    # ── Write methods ─────────────────────────────────────────────
    def create_record(self, sobject: str, body: Dict[str, Any]) -> Dict[str, Any]:
        """POST — create a new record. Returns {'id', 'success', 'errors'}."""
        return self._post(f"/sobjects/{sobject}", body) or {}

    def update_record(
        self, sobject: str, record_id: str, body: Dict[str, Any]
    ) -> None:
        """PATCH by Id. 204 No Content on success — returns None."""
        self._patch(f"/sobjects/{sobject}/{record_id}", body)

    def upsert_record(
        self,
        sobject: str,
        ext_id_field: str,
        ext_id_value: Any,
        body: Dict[str, Any],
    ) -> Dict[str, Any]:
        """PATCH — Salesforce native External-ID upsert.

        Returns {'action': 'created'|'updated', 'id': <sf_id>}.

        Salesforce returns 201 Created when the record is new, 204 No Content
        when it was updated. The response body only exists on 201.
        """
        import requests
        path = f"/sobjects/{sobject}/{ext_id_field}/{ext_id_value}"
        # Do the request manually so we can inspect status_code (204 has no body).
        # Retry loop mirrors _request but returns response object for status inspection.
        last_exc = None
        for attempt in range(1, self.max_retries + 1):
            try:
                r = requests.patch(
                    self._url(path),
                    headers=self._headers(),
                    json=body,
                    timeout=self.request_timeout_seconds,
                )
            except requests.RequestException as e:
                last_exc = e
                if attempt >= self.max_retries:
                    raise
                time.sleep(min(2 ** attempt, 10))
                continue
            if r.status_code == 401:
                self._refresh()
                if attempt >= self.max_retries:
                    r.raise_for_status()
                continue
            if r.status_code in (429, 500, 502, 503, 504):
                if attempt >= self.max_retries:
                    r.raise_for_status()
                time.sleep(min(2 ** attempt, 10))
                continue
            r.raise_for_status()
            if r.status_code == 201:
                data = r.json() or {}
                return {"action": "created", "id": data.get("id")}
            # 204 (updated) — we don't get the Id back; look it up.
            existing = self.find_record(sobject, ext_id_field, ext_id_value)
            return {
                "action": "updated",
                "id": (existing or {}).get("Id") or (existing or {}).get("id"),
            }
        if last_exc:
            raise last_exc
        return {"action": "unknown", "id": None}

    def composite_upsert(
        self,
        sobject: str,
        ext_id_field: str,
        records: List[Dict[str, Any]],
        *,
        all_or_none: bool = False,
    ) -> List[Dict[str, Any]]:
        """Bulk upsert up to 200 records via /composite/sobjects.

        Records MUST include the `ext_id_field` key with a scalar value.
        Salesforce's Composite Sobjects Collections upsert applies External-ID
        matching for each record. Returns a list of {'id', 'success', 'errors'}
        matching the input order.
        """
        if len(records) > 200:
            raise ValueError("composite_upsert accepts at most 200 records per call")
        # Each record needs an `attributes.type` marker.
        body_records = []
        for rec in records:
            entry = {"attributes": {"type": sobject}}
            entry.update(rec)
            body_records.append(entry)
        payload = {"allOrNone": all_or_none, "records": body_records}
        result = self._patch(
            f"/composite/sobjects/{sobject}/{ext_id_field}", payload
        )
        # Salesforce returns a list of results.
        if isinstance(result, list):
            return result
        return []

    def delete_record(self, sobject: str, record_id: str) -> None:
        """DELETE by Id. 204 No Content on success — returns None."""
        self._delete(f"/sobjects/{sobject}/{record_id}")


class SalesforceResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a Salesforce REST API resource for use by other components.

    OAuth 2.0 password grant against a Connected App. Consumer key + secret
    come from the Connected App you configured under Setup → App Manager.
    Password + security token concatenated at auth time — for orgs with IP
    restrictions, the security_token is required; for IP-relaxed orgs it can
    be blank.

    Pairs with:
      - `salesforce_ingestion` — bulk pull from Salesforce (dlt-backed).
      - `salesforce_record_upsert` — reverse-ETL sink (native External-ID upsert).

    Example (production org):

        ```yaml
        type: dagster_component_templates.SalesforceResourceComponent
        attributes:
          resource_key: salesforce
          username: svc-account@mycompany.com
          password_env_var: SF_PASSWORD
          security_token_env_var: SF_SECURITY_TOKEN
          consumer_key_env_var: SF_CONSUMER_KEY
          consumer_secret_env_var: SF_CONSUMER_SECRET
          domain: login                  # 'login' (prod), 'test' (sandbox), or 'mycompany.my' (custom)
          api_version: "58.0"
        ```
    """

    resource_key: str = Field(
        default="salesforce",
        description="Resource key. Other components reference it via this name.",
    )
    username: str = Field(
        description="Salesforce username (usually an email)."
    )
    password_env_var: str = Field(
        description="Env var holding the Salesforce password."
    )
    security_token_env_var: Optional[str] = Field(
        default=None,
        description=(
            "Env var holding the Salesforce security token. Appended to the "
            "password at auth time. Required for IP-restricted orgs; leave "
            "unset for orgs with IP-relaxed access."
        ),
    )
    consumer_key_env_var: str = Field(
        description=(
            "Env var holding the Connected App Consumer Key (from Setup → App Manager)."
        ),
    )
    consumer_secret_env_var: str = Field(
        description=(
            "Env var holding the Connected App Consumer Secret."
        ),
    )
    domain: str = Field(
        default="login",
        description=(
            "Salesforce login domain — `login` for production, `test` for "
            "sandbox, or `mycompany.my` for custom domains. Used to construct "
            "`https://<domain>.salesforce.com/services/oauth2/token`."
        ),
    )
    api_version: str = Field(
        default="58.0",
        description="Salesforce REST API version. Bump when you rely on newer surface.",
    )
    request_timeout_seconds: int = Field(
        default=60,
        description="Per-request timeout in seconds.",
    )
    max_retries: int = Field(
        default=3,
        description="Retry attempts on 429 / 5xx (exponential backoff, capped at 10s) + one on 401 (token refresh).",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        import os
        password = os.environ.get(self.password_env_var, "")
        security_token = (
            os.environ.get(self.security_token_env_var, "")
            if self.security_token_env_var
            else ""
        )
        consumer_key = os.environ.get(self.consumer_key_env_var, "")
        consumer_secret = os.environ.get(self.consumer_secret_env_var, "")
        resource = SalesforceResource(
            username=self.username,
            password=password,
            security_token=security_token,
            consumer_key=consumer_key,
            consumer_secret=consumer_secret,
            domain=self.domain,
            api_version=self.api_version,
            request_timeout_seconds=self.request_timeout_seconds,
            max_retries=self.max_retries,
        )
        return dg.Definitions(resources={self.resource_key: resource})
