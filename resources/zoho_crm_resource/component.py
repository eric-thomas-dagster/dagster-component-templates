"""Zoho CRM Resource.

Self-contained Zoho CRM REST API workhorse -- real OAuth2 **refresh-token**
grant (not the static access-token-only approach `zoho_crm_ingestion` uses
for its short-lived dlt pull), plus `.get()` / `.post()` / `.patch()`
convenience methods and a native `.upsert()` wrapping Zoho's "Upsert
Records" API.

Auth -- OAuth2 refresh-token grant:

    POST {accounts_host}/oauth/v2/token
        grant_type=refresh_token
        client_id=<client_id>
        client_secret=<client_secret>
        refresh_token=<refresh_token>

    -> {"access_token": ..., "expires_in": 3600, "api_domain": "https://www.zohoapis.com", "token_type": "Bearer"}

Verified against Zoho's own developer docs (zoho.com/crm/developer/docs/api/v8):

  - Zoho's accounts host is region-specific: `accounts.zoho.com` (US),
    `.eu`, `.in`, `.com.au`, `.jp`, `.com.cn` -- EXCEPT Canada, which is
    `accounts.zohocloud.ca` (not `accounts.zoho.ca`). `accounts_domain`
    selects the region; set it to `"ca"` to get the correct zohocloud.ca
    host automatically.
  - The token response ITSELF returns `api_domain` -- Zoho's canonical way
    to tell a client which API host to use for its data center, rather
    than the client guessing from the accounts region. This resource uses
    the `api_domain` returned by the token response unless `api_domain` is
    explicitly overridden in config.
  - Refresh tokens do NOT rotate on refresh (unlike Outreach) and do not
    expire on their own -- they're valid indefinitely until revoked. BUT
    Zoho rate-limits token generation: at most 10 access-token exchanges
    per refresh token per 10-minute window, and at most 15 concurrently
    "active" access tokens are retained per refresh token (the 16th
    invalidates the oldest). This resource caches the access token
    in-memory per-instance and refreshes ~60s before expiry (access tokens
    last ~3600s) specifically to stay well under that 10-per-10-min limit
    under normal operation.
  - API version: defaults to `v8` (Zoho's current API version per its own
    docs as of this writing). `zoho_crm_ingestion`'s dlt-based read uses
    `v3` for a simple bulk pull, but v8 is what Zoho's current API
    reference describes, including the duplicate_check_fields upsert
    semantics this resource relies on -- so v8 is the better default for a
    write-oriented resource. Still fully configurable via `api_version`.

Native Upsert Records API:

    POST {api_domain}/crm/{version}/{module_api_name}/upsert
        {"data": [...], "duplicate_check_fields": [...]}

  - Max 100 records per request (Zoho-documented hard limit).
  - `duplicate_check_fields` is a list of Zoho field API names used to
    detect duplicates (e.g. `["Email"]` for Leads/Contacts -- Email is
    Zoho's system-defined duplicate-check field for those modules). If
    omitted, Zoho falls back to system-defined duplicate-check fields,
    then user-defined unique fields, in that order. Zoho's docs do NOT
    publish a hard maximum count for this array (its own examples show
    1-2 fields, e.g. `["Email", "Mobile"]`); this resource does not
    enforce an artificial cap, but keeping it small (1-3 fields marked
    unique/mandatory in the module) is the documented, sane usage.
  - Response: `{"data": [{"code": "SUCCESS", "duplicate_field": "Email",
    "action": "insert"|"update", "status": "success"|"error",
    "message": "...", "details": {"id": ..., ...}}, ...]}` -- one entry
    per input record, in order.

Auth header convention (matches `zoho_crm_ingestion`'s established
pattern in this repo): `Authorization: Zoho-oauthtoken {access_token}`.
"""
import time
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field

# Zoho's documented hard limit on records per /upsert call.
_UPSERT_RECORDS_PER_REQUEST = 100


def _accounts_host(accounts_domain: str) -> str:
    """Map a region code to Zoho's accounts host.

    Verified regions (per Zoho's own docs): com (US), eu, in, com.au, jp,
    com.cn all follow `accounts.zoho.{domain}`. Canada is the one
    exception -- `accounts.zohocloud.ca`, not `accounts.zoho.ca`.
    """
    domain = (accounts_domain or "com").strip().lower()
    if domain in ("ca", "canada", "zohocloud.ca"):
        return "accounts.zohocloud.ca"
    return f"accounts.zoho.{domain}"


class ZohoCrmResource(dg.ConfigurableResource):
    """Zoho CRM REST API client wrapper (OAuth2 refresh-token grant)."""

    client_id_env_var: str = Field(description="Env var holding the Zoho OAuth Client ID.")
    client_secret_env_var: str = Field(description="Env var holding the Zoho OAuth Client Secret.")
    refresh_token_env_var: str = Field(
        description=(
            "Env var holding the long-lived OAuth refresh token (obtained once "
            "via the authorization_code grant through Zoho's API Console). "
            "Zoho refresh tokens do not rotate and do not expire on their own."
        )
    )
    accounts_domain: str = Field(
        default="com",
        description=(
            "Zoho accounts data-center region: 'com' (US, default), 'eu', 'in', "
            "'com.au', 'jp', 'com.cn', or 'ca' (mapped to the special "
            "accounts.zohocloud.ca host). Used only for the OAuth token "
            "endpoint -- the API host itself is taken from the token "
            "response's `api_domain` unless `api_domain` is overridden below."
        ),
    )
    api_domain: Optional[str] = Field(
        default=None,
        description=(
            "Override the Zoho API host (e.g. 'https://www.zohoapis.eu'). "
            "Leave unset (default) to use the `api_domain` Zoho's own OAuth "
            "token response returns for your data center -- this is Zoho's "
            "recommended way to resolve the correct API host, rather than "
            "deriving it from accounts_domain."
        ),
    )
    api_version: str = Field(
        default="v8",
        description=(
            "Zoho CRM REST API version. v8 is Zoho's current version per its "
            "own developer docs (also what documents duplicate_check_fields "
            "upsert semantics this resource relies on). v3 (used by this "
            "repo's zoho_crm_ingestion dlt-based read) also still works."
        ),
    )
    request_timeout_seconds: int = Field(default=60, description="Per-request timeout in seconds.")
    max_retries: int = Field(
        default=3,
        description="Retry attempts on 429 / 5xx (exponential backoff, capped at 10s) + one on 401 (token refresh).",
    )

    _token_cache: dict = {}

    # ── Auth ────────────────────────────────────────────────────────
    def _get_access(self) -> Dict[str, str]:
        """Return {'access_token', 'api_domain'} -- cached per-instance.

        NOTE: must read/write via `self._token_cache` (the per-instance
        pydantic PrivateAttr value), not the class attribute -- mirrors the
        fix already applied to `marketo_resource` in this repo.
        """
        import os
        import requests

        cache_key = self.client_id_env_var
        cache = self._token_cache.get(cache_key) or {}
        if cache.get("expires", 0) > time.time() + 60:
            return {"access_token": cache["access_token"], "api_domain": cache["api_domain"]}

        client_id = os.environ.get(self.client_id_env_var)
        client_secret = os.environ.get(self.client_secret_env_var)
        refresh_token = os.environ.get(self.refresh_token_env_var)
        if not all([client_id, client_secret, refresh_token]):
            raise RuntimeError(
                "Missing Zoho OAuth env vars (client_id/client_secret/refresh_token)."
            )

        token_url = f"https://{_accounts_host(self.accounts_domain)}/oauth/v2/token"
        resp = requests.post(
            token_url,
            data={
                "grant_type": "refresh_token",
                "client_id": client_id,
                "client_secret": client_secret,
                "refresh_token": refresh_token,
            },
            timeout=self.request_timeout_seconds,
        )
        resp.raise_for_status()
        data = resp.json()
        if "access_token" not in data:
            raise RuntimeError(f"Zoho OAuth refresh failed: {data!r}")

        api_domain = self.api_domain or data.get("api_domain") or "https://www.zohoapis.com"
        self._token_cache[cache_key] = {
            "access_token": data["access_token"],
            # Refresh a little early (60s) rather than racing expiry.
            "expires": time.time() + data.get("expires_in", 3600) - 60,
            "api_domain": api_domain,
        }
        return {"access_token": data["access_token"], "api_domain": api_domain}

    def _force_refresh(self) -> None:
        """Invalidate the cached access token (call after a 401)."""
        self._token_cache.pop(self.client_id_env_var, None)

    # ── HTTP core ─────────────────────────────────────────────────
    def _url(self, api_domain: str, path: str) -> str:
        p = path if path.startswith("/") else f"/{path}"
        if p.startswith(f"/crm/{self.api_version}/"):
            return api_domain.rstrip("/") + p
        return f"{api_domain.rstrip('/')}/crm/{self.api_version}{p}"

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
            access = self._get_access()
            headers = {
                "Authorization": f"Zoho-oauthtoken {access['access_token']}",
                "Content-Type": "application/json",
            }
            try:
                r = requests.request(
                    method,
                    self._url(access["api_domain"], path),
                    headers=headers,
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
                self._force_refresh()
                if attempt >= self.max_retries:
                    r.raise_for_status()
                continue
            if r.status_code in (429, 500, 502, 503, 504):
                if attempt >= self.max_retries:
                    r.raise_for_status()
                time.sleep(min(2 ** attempt, 10))
                continue
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

    # ── Convenience methods ───────────────────────────────────────
    def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        return self._request("GET", path, params=params)

    def post(self, path: str, json_body: Optional[Any] = None) -> Any:
        return self._request("POST", path, json_body=json_body)

    def patch(self, path: str, json_body: Optional[Any] = None) -> Any:
        return self._request("PATCH", path, json_body=json_body)

    def upsert(
        self,
        module_api_name: str,
        records: List[Dict[str, Any]],
        duplicate_check_fields: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """Native Zoho "Upsert Records" call: `POST /{module}/upsert`.

        `records` must be at most 100 (Zoho's documented per-request cap --
        chunk upstream of this call for larger loads). Returns the `data`
        list from Zoho's response: one
        `{code, duplicate_field, action, status, message, details}` entry
        per input record, in order.
        """
        if len(records) > _UPSERT_RECORDS_PER_REQUEST:
            raise ValueError(
                f"ZohoCrmResource.upsert: {len(records)} records exceeds Zoho's "
                f"{_UPSERT_RECORDS_PER_REQUEST}-record-per-request limit -- chunk "
                f"upstream of this call."
            )
        body: Dict[str, Any] = {"data": records}
        if duplicate_check_fields:
            body["duplicate_check_fields"] = duplicate_check_fields
        result = self.post(f"/{module_api_name}/upsert", json_body=body)
        if isinstance(result, dict) and isinstance(result.get("data"), list):
            return result["data"]
        return []


class ZohoCrmResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a ZohoCrmResource for use by other components.

    Pairs with:
      - `zoho_crm_record_upsert` -- reverse-ETL sink using the native
        Upsert Records API (required).
      - `zoho_crm_ingestion` -- the READ-side counterpart (dlt-based bulk
        pull); it uses a static access token, not this resource.

    Example:

        ```yaml
        type: dagster_component_templates.ZohoCrmResourceComponent
        attributes:
          resource_key: zoho_crm
          client_id_env_var: ZOHO_CLIENT_ID
          client_secret_env_var: ZOHO_CLIENT_SECRET
          refresh_token_env_var: ZOHO_REFRESH_TOKEN
          accounts_domain: com
          api_version: v8
        ```
    """

    resource_key: str = Field(
        default="zoho_crm",
        description="Resource key. Other components reference it via this name.",
    )
    client_id_env_var: str = Field(
        default="ZOHO_CLIENT_ID",
        description="Env var holding the Zoho OAuth Client ID.",
    )
    client_secret_env_var: str = Field(
        default="ZOHO_CLIENT_SECRET",
        description="Env var holding the Zoho OAuth Client Secret.",
    )
    refresh_token_env_var: str = Field(
        default="ZOHO_REFRESH_TOKEN",
        description="Env var holding the long-lived OAuth refresh token.",
    )
    accounts_domain: str = Field(
        default="com",
        description=(
            "Zoho accounts data-center region: 'com' (US), 'eu', 'in', "
            "'com.au', 'jp', 'com.cn', or 'ca' (zohocloud.ca)."
        ),
    )
    api_domain: Optional[str] = Field(
        default=None,
        description=(
            "Override the Zoho API host. Leave unset to use the api_domain "
            "Zoho's OAuth token response returns for your data center."
        ),
    )
    api_version: str = Field(
        default="v8",
        description="Zoho CRM REST API version.",
    )
    request_timeout_seconds: int = Field(default=60, description="Per-request timeout in seconds.")
    max_retries: int = Field(
        default=3,
        description="Retry attempts on 429 / 5xx (exponential backoff) + one on 401 (token refresh).",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = ZohoCrmResource(
            client_id_env_var=self.client_id_env_var,
            client_secret_env_var=self.client_secret_env_var,
            refresh_token_env_var=self.refresh_token_env_var,
            accounts_domain=self.accounts_domain,
            api_domain=self.api_domain,
            api_version=self.api_version,
            request_timeout_seconds=self.request_timeout_seconds,
            max_retries=self.max_retries,
        )
        return dg.Definitions(resources={self.resource_key: resource})
