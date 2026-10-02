"""Dynamics CRM (Microsoft Dataverse) Resource component.

Self-contained Microsoft Dynamics 365 / Dataverse Web API workhorse --
Azure AD OAuth2 client_credentials auth + raw HTTP for the OData v4 API
surface, with a native alternate-key upsert helper for downstream sink
components (`dynamics_crm_record_upsert`).

Auth: Azure AD (Entra ID) OAuth 2.0 client_credentials grant:

    POST https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token
        grant_type=client_credentials
        client_id=<client_id>
        client_secret=<client_secret>
        scope={org_url}/.default

This requires:
  - An Azure AD app registration (client_id / client_secret).
  - A matching Dataverse **Application User** created in the target
    environment (Power Platform Admin Center -> Environment -> Settings ->
    Users + permissions -> Application users), assigned a security role.
    Access is governed by this Application User + its security role, NOT
    by an Entra "API permissions" block on the app registration -- Entra
    issues the access token for the Dataverse resource regardless of
    whether any API permission is configured on the registration;
    Dataverse itself authorizes (or rejects) each call based on the
    Application User record and the privileges its security role grants.

All calls hit `{org_url}/api/data/{api_version}/...` with
`Authorization: Bearer <token>`, `OData-MaxVersion: 4.0`,
`OData-Version: 4.0`.

Upsert-by-alternate-key (`.upsert_by_key`) is Dataverse's native
create-or-update mechanism:

    PATCH {org_url}/api/data/{api_version}/{entity_set}({key}='{value}')

`{key}` must be a pre-defined **Alternate Key** on the table (Power Apps
maker portal -> table -> Keys). Composite (multi-column) alternate keys
are supported: `(key1=val1,key2=val2)`.

Without `Prefer: return=representation`, Dataverse ALWAYS returns `204 No
Content` for both create and update -- the two are indistinguishable from
the status code alone, and the `OData-EntityId` response header echoes
back the *same alternate-key reference you sent* when you addressed the
row via an alternate key (not the row's real GUID primary key). The only
reliable way to learn whether a row was created vs. updated, and to get
its real primary-key GUID back in the same round trip, is
`Prefer: return=representation`: Dataverse then returns `201 Created`
(new row) or `200 OK` (existing row updated) with the full row --
including its GUID primary-key attribute -- in the response body. This
resource defaults `prefer_representation=True` for exactly that reason.

Dataverse has no single-call bulk/composite upsert the way Salesforce's
`/composite/sobjects` does. An OData `$batch` endpoint exists
(`POST {org_url}/api/data/{api_version}/$batch`, multipart/mixed request
bodies, optionally grouped into atomic "changesets"), but it requires
hand-building and parsing raw MIME multipart bodies for a win that's
about HTTP round-trips, not server-side parallelism the way Salesforce's
Bulk 2.0 is. This resource/component issues one PATCH per row; `$batch`
is left as a documented, not-yet-implemented optimization (see README).

Convenience methods:

    get(path, params)                                     # GET
    post(path, json_body)                                 # POST
    patch(path, json_body, prefer_representation=True)     # raw PATCH
    upsert_by_key(entity_set, key_name, key_value, body)   # alternate-key upsert
"""
import time
from typing import Any, Dict, List, Optional, Union

import dagster as dg
from pydantic import Field


class DynamicsCrmResource(dg.ConfigurableResource):
    """Microsoft Dynamics 365 (Dataverse) Web API client wrapper."""

    org_url: str = Field(
        description=(
            "Dynamics 365 organization URL, e.g. "
            "'https://myorg.crm.dynamics.com' (trailing slash optional)."
        ),
    )
    tenant_id: str = Field(description="Azure AD (Entra ID) tenant ID.")
    client_id_env_var: str = Field(
        description="Env var holding the Azure AD app registration's client ID."
    )
    client_secret_env_var: str = Field(
        description="Env var holding the Azure AD app registration's client secret."
    )
    api_version: str = Field(
        default="v9.2",
        description="Dataverse Web API version segment (e.g. 'v9.2').",
    )
    request_timeout_seconds: int = Field(
        default=60, description="Per-request timeout in seconds."
    )
    max_retries: int = Field(
        default=3,
        description=(
            "Retry attempts on 429 / 5xx (exponential backoff, capped at 10s) "
            "+ one retry on 401 (token refresh)."
        ),
    )

    # Instance-level token cache -- NOT a bare class attribute. Accessing a
    # leading-underscore attribute via `self._token_cache` on a pydantic
    # model returns the actual private dict; accessing it via the class
    # (`DynamicsCrmResource._token_cache`) returns the ModelPrivateAttr
    # descriptor object instead, and `.get()` on that raises AttributeError.
    _token_cache: dict = {}

    def _get_access_token(self) -> str:
        import os
        import requests

        cache = self._token_cache.get(self.org_url) or {}
        if cache.get("expires", 0) > time.time() + 60:
            return cache["access_token"]

        client_id = os.environ.get(self.client_id_env_var)
        client_secret = os.environ.get(self.client_secret_env_var)
        if not all([client_id, client_secret]):
            raise RuntimeError(
                "Missing Dynamics CRM OAuth env vars "
                f"({self.client_id_env_var} / {self.client_secret_env_var})"
            )

        resp = requests.post(
            f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token",
            data={
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret,
                "scope": f"{self.org_url.rstrip('/')}/.default",
            },
            timeout=self.request_timeout_seconds,
        )
        resp.raise_for_status()
        data = resp.json()
        self._token_cache[self.org_url] = {
            "access_token": data["access_token"],
            # Refresh a little early (60s) rather than racing expiry.
            "expires": time.time() + data.get("expires_in", 3600) - 60,
        }
        return data["access_token"]

    def _invalidate_token(self) -> None:
        self._token_cache.pop(self.org_url, None)

    def _headers(self, extra: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        h = {
            "Authorization": f"Bearer {self._get_access_token()}",
            "Accept": "application/json",
            "Content-Type": "application/json",
            "OData-MaxVersion": "4.0",
            "OData-Version": "4.0",
        }
        if extra:
            h.update(extra)
        return h

    def _url(self, path: str) -> str:
        path = path.lstrip("/")
        if path.startswith("api/data/"):
            return f"{self.org_url.rstrip('/')}/{path}"
        return f"{self.org_url.rstrip('/')}/api/data/{self.api_version}/{path}"

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Any] = None,
        extra_headers: Optional[Dict[str, str]] = None,
    ):
        """Execute a request with retry on 401 (token refresh) / 429 / 5xx.

        Returns the raw `requests.Response` -- callers are responsible for
        `.raise_for_status()` / status-code inspection, since Dataverse's
        upsert semantics depend on distinguishing 200 / 201 / 204.
        """
        import requests

        last_exc = None
        for attempt in range(1, self.max_retries + 1):
            try:
                r = requests.request(
                    method,
                    self._url(path),
                    headers=self._headers(extra_headers),
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
                self._invalidate_token()
                if attempt >= self.max_retries:
                    r.raise_for_status()
                continue
            if r.status_code in (429, 500, 502, 503, 504):
                if attempt >= self.max_retries:
                    r.raise_for_status()
                time.sleep(min(2 ** attempt, 10))
                continue
            return r
        if last_exc:
            raise last_exc
        raise RuntimeError(
            "Dynamics CRM request exhausted retries with no response and no exception."
        )

    # ── Public HTTP methods ────────────────────────────────────────
    def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """GET a path (relative to `{org_url}/api/data/{api_version}/`)."""
        r = self._request("GET", path, params=params)
        r.raise_for_status()
        if not r.content:
            return None
        return r.json()

    def post(self, path: str, json_body: Optional[Any] = None) -> Any:
        """POST a path. Returns parsed JSON body (or None for 204)."""
        r = self._request("POST", path, json_body=json_body)
        r.raise_for_status()
        if not r.content:
            return None
        return r.json()

    def patch(
        self,
        path: str,
        json_body: Optional[Any] = None,
        *,
        prefer_representation: bool = True,
    ) -> Dict[str, Any]:
        """PATCH a path. Returns {'status_code', 'body', 'odata_entity_id'}.

        `prefer_representation=True` (default) sends
        `Prefer: return=representation`, which makes Dataverse return
        `201 Created` (new row) with the row body, or `200 OK` (existing
        row) with the updated row body -- the only way to reliably
        distinguish create from update and recover the row's GUID primary
        key in one round trip. With `prefer_representation=False`,
        Dataverse always returns `204 No Content` for both outcomes.
        """
        extra_headers = (
            {"Prefer": "return=representation"} if prefer_representation else None
        )
        r = self._request(
            "PATCH", path, json_body=json_body, extra_headers=extra_headers
        )
        r.raise_for_status()
        body = None
        if r.content:
            try:
                body = r.json()
            except ValueError:
                body = None
        return {
            "status_code": r.status_code,
            "body": body,
            "odata_entity_id": r.headers.get("OData-EntityId"),
        }

    def delete(self, path: str) -> None:
        """DELETE a path. 204 No Content on success -- returns None."""
        r = self._request("DELETE", path)
        r.raise_for_status()

    # ── Alternate-key upsert ───────────────────────────────────────
    @staticmethod
    def _format_key_value(value: Any) -> str:
        """Render a single alternate-key value as an OData URL literal."""
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, (int, float)):
            return str(value)
        # String (and anything else): quote + escape embedded single quotes
        # by doubling them, per OData literal syntax.
        return "'" + str(value).replace("'", "''") + "'"

    def _build_key_expr(
        self,
        key_name_or_names: Union[str, List[str]],
        key_value_or_values: Union[Any, List[Any]],
    ) -> str:
        if isinstance(key_name_or_names, (list, tuple)):
            names = list(key_name_or_names)
            values = list(key_value_or_values)
            if len(names) != len(values):
                raise ValueError(
                    "upsert_by_key: key names and values must be the same length "
                    f"(got {len(names)} names, {len(values)} values)."
                )
            return ",".join(
                f"{n}={self._format_key_value(v)}" for n, v in zip(names, values)
            )
        return f"{key_name_or_names}={self._format_key_value(key_value_or_values)}"

    def upsert_by_key(
        self,
        entity_set: str,
        key_name_or_names: Union[str, List[str]],
        key_value_or_values: Union[Any, List[Any]],
        body: Dict[str, Any],
        *,
        prefer_representation: bool = True,
    ) -> Dict[str, Any]:
        """Upsert a row via Dataverse's native alternate-key PATCH.

            PATCH {entity_set}({key}='{value}')

        `key_name_or_names` / `key_value_or_values` are each either a
        single scalar (simple alternate key) or parallel lists (composite
        alternate key) -- e.g. `key_name_or_names=["k1", "k2"]`,
        `key_value_or_values=[1, 2]` renders `(k1=1,k2=2)`.

        `key_name_or_names` MUST be a pre-defined Alternate Key on the
        Dataverse table (maker portal -> table -> Keys) -- this is not
        just any column.

        Returns `{"action": "created" | "updated" | "unknown", "id": <guid-or-None>}`.
        `"unknown"` only occurs when `prefer_representation=False`, since
        a bare `204` can't distinguish create from update.
        """
        key_expr = self._build_key_expr(key_name_or_names, key_value_or_values)
        path = f"{entity_set}({key_expr})"
        result = self.patch(
            path, json_body=body, prefer_representation=prefer_representation
        )

        status = result["status_code"]
        resp_body = result.get("body") or {}

        if prefer_representation:
            if status == 201:
                action = "created"
            elif status == 200:
                action = "updated"
            else:
                action = "unknown"
            record_id = None
            for k, v in resp_body.items():
                if (
                    k.endswith("id")
                    and not k.startswith("@")
                    and not k.startswith("_")
                    and isinstance(v, str)
                    and len(v) == 36
                    and v.count("-") == 4
                ):
                    record_id = v
                    break
            return {"action": action, "id": record_id}

        # Without Prefer: return=representation, Dataverse always returns
        # 204 for both create and update -- genuinely indistinguishable.
        # OData-EntityId echoes the alternate-key reference we sent, not
        # the row's real GUID, so we can't recover an id here either.
        return {"action": "unknown", "id": result.get("odata_entity_id")}


class DynamicsCrmResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a Microsoft Dynamics 365 (Dataverse) resource for use by
    other components.

    Auth: Azure AD OAuth2 client_credentials grant. Requires an Azure AD
    app registration plus a matching Dataverse **Application User**
    (created in the target environment, assigned a security role) -- see
    the component README for the full setup checklist.

    Pairs with:
      - `dynamics_crm_ingestion` -- read side (dlt-backed bulk pull; has
        its own inline OAuth, doesn't use this resource).
      - `dynamics_crm_record_upsert` -- reverse-ETL sink (alternate-key
        upsert via this resource).

    Example:
        ```yaml
        type: dagster_component_templates.DynamicsCrmResourceComponent
        attributes:
          resource_key: dynamics_crm
          org_url: "https://myorg.crm.dynamics.com"
          tenant_id: "${DYNAMICS_TENANT_ID}"
          client_id_env_var: DYNAMICS_CLIENT_ID
          client_secret_env_var: DYNAMICS_CLIENT_SECRET
        ```
    """

    resource_key: str = Field(
        default="dynamics_crm",
        description="Resource key. Other components reference it via this name.",
    )
    org_url: str = Field(
        description=(
            "Dynamics 365 organization URL, e.g. "
            "'https://myorg.crm.dynamics.com'."
        ),
    )
    tenant_id: str = Field(description="Azure AD (Entra ID) tenant ID.")
    client_id_env_var: str = Field(
        default="DYNAMICS_CLIENT_ID",
        description="Env var holding the Azure AD app registration's client ID.",
    )
    client_secret_env_var: str = Field(
        default="DYNAMICS_CLIENT_SECRET",
        description="Env var holding the Azure AD app registration's client secret.",
    )
    api_version: str = Field(
        default="v9.2",
        description="Dataverse Web API version segment (e.g. 'v9.2').",
    )
    request_timeout_seconds: int = Field(
        default=60, description="Per-request timeout in seconds."
    )
    max_retries: int = Field(
        default=3,
        description="Retry attempts on 429 / 5xx + one on 401 (token refresh).",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = DynamicsCrmResource(
            org_url=self.org_url,
            tenant_id=self.tenant_id,
            client_id_env_var=self.client_id_env_var,
            client_secret_env_var=self.client_secret_env_var,
            api_version=self.api_version,
            request_timeout_seconds=self.request_timeout_seconds,
            max_retries=self.max_retries,
        )
        return dg.Definitions(resources={self.resource_key: resource})
