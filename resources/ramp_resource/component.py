"""Ramp Resource component.

Wraps Ramp's Developer API using OAuth 2.0 **client_credentials** against a
Ramp Developer App (not the static pre-minted bearer token that
``ramp_ingestion`` currently takes as a raw ``access_token`` field --
that component expects the caller to obtain and rotate the token
themselves outside Dagster; this resource performs the real token
exchange, with caching/refresh, so this write-side path is self-contained).

Ramp's own docs are explicit that the Developer API requires Client
Credentials, confirmed directly against https://docs.ramp.com (not assumed):

    Ramp uses OAuth 2.0 for secure API access... Ramp authenticates
    requests to /developer/v1/token using your client ID and client
    secret, typically with HTTP Basic Auth.

Token endpoint:  POST {base_url}/developer/v1/token
  headers: Authorization: Basic base64(client_id:client_secret)
  body:    grant_type=client_credentials&scope=<space-separated scopes>
  Client Credentials access tokens normally last 10 days (864000s) --
  use the real `expires_in` from the response rather than hard-coding that.

Base URLs (standard Developer API):
  Production: https://api.ramp.com
  Sandbox:    https://demo-api.ramp.com

Scopes used by this resource's convenience methods:
  - reimbursements:write -- create_mileage_reimbursement, upload_reimbursement_receipt
  - cards:write           -- update_physical_card
  - cards:read_vault, limits:write, funds:write (+ optionally users:read)
                          -- create_virtual_card (Vault API -- see below)

**Vault API constraint (read before using `create_virtual_card`):**
Creating a virtual card with a retrievable PAN/CVV goes through Ramp's
Vault API (`POST /developer/v1/cards/vault`), which is NOT a normal write
scope -- Ramp's docs state plainly:

    Ramp reviews your use case, security controls, and PCI handling
    before the Vault API can return full PANs and CVVs in production.
    All customers can use the Vault API in Sandbox. Submit a Developer
    API support ticket to begin the review.

So `create_virtual_card` will work against Sandbox (https://demo-api.ramp.com)
immediately once `cards:read_vault`/`limits:write`/`funds:write` are enabled
on your app, but calling it against Production before Ramp has approved
your app for Vault API access will fail -- this is a genuine Ramp-side
access-tier gate, not a bug in this resource. Ramp's guides additionally
describe a dedicated Vault API host (`https://vault-api.ramp.com` prod /
`https://demo-vault-api.ramp.com` sandbox) as an alternative way to reach
this same endpoint with a shorter path (`/cards/vault`, no `/developer/v1`
prefix) -- set `vault_base_url` if your app's Vault grant is issued against
that host instead of the standard one.

There is deliberately NO method on this resource to update the
spend_limit/restrictions on an already-issued card -- Ramp's Developer API
does not expose one. The only documented update to an existing card is
`PATCH /developer/v1/cards/physical/{card_id}`, which can change
`display_name`, `fund_id`, and `automatic_routing_enabled` -- never the
spend limit. `update_physical_card` reflects exactly that, and nothing
more.
"""
import base64
import time
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field

_VALID_DISTANCE_UNITS = {"KILOMETERS", "MILES"}


class RampResource(dg.ConfigurableResource):
    """Dagster resource wrapping Ramp's Developer API (client_credentials OAuth2)."""

    client_id_env_var: str = Field(
        default="RAMP_CLIENT_ID",
        description="Env var holding your Ramp Developer App's Client ID.",
    )
    client_secret_env_var: str = Field(
        default="RAMP_CLIENT_SECRET",
        description="Env var holding your Ramp Developer App's Client Secret.",
    )
    base_url: str = Field(
        default="https://api.ramp.com",
        description=(
            "Ramp Developer API base URL. Production: 'https://api.ramp.com'. "
            "Sandbox: 'https://demo-api.ramp.com' -- Ramp requires a separate "
            "app registration (separate client_id/client_secret) per environment."
        ),
    )
    vault_base_url: Optional[str] = Field(
        default=None,
        description=(
            "Optional dedicated Vault API host for create_virtual_card, per "
            "Ramp's guide (as opposed to the standard "
            "'{base_url}/developer/v1/cards/vault' path used by default): "
            "'https://vault-api.ramp.com' (production) or "
            "'https://demo-vault-api.ramp.com' (sandbox). Leave unset unless "
            "Ramp's support/onboarding explicitly tells you your app's Vault "
            "grant is issued against the dedicated host."
        ),
    )
    scope: str = Field(
        default="reimbursements:write cards:write",
        description=(
            "Space-separated OAuth scope(s) requested at token time. Add "
            "'cards:read_vault limits:write funds:write' (and optionally "
            "'users:read') if you will use create_virtual_card -- that scope "
            "combination additionally requires Ramp's production access "
            "review outside Sandbox (see module docstring)."
        ),
    )
    token_path: str = Field(
        default="/developer/v1/token",
        description="Token endpoint path, relative to base_url.",
    )

    _token_cache: dict = {}

    def _get_access_token(self) -> str:
        # NOTE: must read/write via `self._token_cache` (the per-instance
        # pydantic PrivateAttr value), NOT `RampResource._token_cache` (the
        # class attribute) -- the latter is a `ModelPrivateAttr` descriptor
        # object, not a dict, and `.get()`/`[]=` on it raises AttributeError.
        # (Same class-vs-instance footgun found and fixed in marketo_resource,
        # already avoided here by following okta_resource's convention.)
        import os

        import requests

        cache_key = f"{self.base_url}:{self.client_id_env_var}:{self.scope}"
        cache = self._token_cache.get(cache_key) or {}
        if cache.get("expires", 0) > time.time() + 60:
            return cache["access_token"]

        client_id = os.environ.get(self.client_id_env_var)
        client_secret = os.environ.get(self.client_secret_env_var)
        if not client_id or not client_secret:
            raise RuntimeError(
                f"Missing Ramp Developer App credentials: env vars "
                f"{self.client_id_env_var!r} and {self.client_secret_env_var!r} "
                f"must both be set."
            )

        basic = base64.b64encode(f"{client_id}:{client_secret}".encode("utf-8")).decode("ascii")
        resp = requests.post(
            f"{self.base_url.rstrip('/')}{self.token_path}",
            headers={
                "Authorization": f"Basic {basic}",
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/json",
            },
            data={"grant_type": "client_credentials", "scope": self.scope},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        self._token_cache[cache_key] = {
            "access_token": data["access_token"],
            # Ramp's Client Credentials tokens normally last 10 days
            # (864000s) -- always prefer the real expires_in over assuming that.
            "expires": time.time() + data.get("expires_in", 864000) - 60,
        }
        return data["access_token"]

    def get_client(self):
        """Return an authenticated `requests.Session` for ordinary JSON
        calls. Escape hatch for anything not covered by the convenience
        methods below. NOT used for the multipart receipt-upload call --
        that builds its own headers so requests can set its own
        multipart Content-Type/boundary."""
        import requests

        session = requests.Session()
        session.headers.update(
            {
                "Authorization": f"Bearer {self._get_access_token()}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        )
        return session

    def _auth_headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self._get_access_token()}",
            "Accept": "application/json",
        }

    # --- Reimbursements (reimbursements:write) ------------------------------

    def create_mileage_reimbursement(
        self,
        reimbursee_id: str,
        trip_date: str,
        distance: Any,
        distance_units: str = "MILES",
        start_location: Optional[str] = None,
        end_location: Optional[str] = None,
        memo: Optional[str] = None,
        spend_allocation_id: Optional[str] = None,
        waypoints: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """POST /developer/v1/reimbursements/mileage. Exact field names per
        Ramp's Developer API reference (docs.ramp.com/llms-api.txt)."""
        if distance_units not in _VALID_DISTANCE_UNITS:
            raise ValueError(f"distance_units must be one of {sorted(_VALID_DISTANCE_UNITS)}, got {distance_units!r}")
        body: Dict[str, Any] = {
            "reimbursee_id": reimbursee_id,
            "trip_date": trip_date,
            "distance": distance,
            "distance_units": distance_units,
        }
        if start_location is not None:
            body["start_location"] = start_location
        if end_location is not None:
            body["end_location"] = end_location
        if memo is not None:
            body["memo"] = memo
        if spend_allocation_id is not None:
            body["spend_allocation_id"] = spend_allocation_id
        if waypoints:
            body["waypoints"] = waypoints

        session = self.get_client()
        resp = session.post(
            f"{self.base_url.rstrip('/')}/developer/v1/reimbursements/mileage",
            json=body,
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()

    def upload_reimbursement_receipt(
        self,
        reimbursee_id: str,
        receipt_file_path: str,
        idempotency_key: str,
        reimbursement_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """POST /developer/v1/reimbursements/submit-receipt (multipart/form-data).

        If `reimbursement_id` is omitted, Ramp attempts to auto-create a
        draft reimbursement via OCR on the receipt image -- per Ramp's docs.
        Uses raw `requests.post(..., files=...)` rather than `get_client()`'s
        session so requests can set its own multipart Content-Type/boundary
        instead of the JSON one get_client() sets by default.
        """
        import pathlib

        import requests

        path = pathlib.Path(receipt_file_path)
        if not path.exists():
            raise FileNotFoundError(f"receipt_file_path does not exist: {receipt_file_path}")

        data = {
            "idempotency_key": idempotency_key,
            "reimbursee_id": reimbursee_id,
        }
        if reimbursement_id:
            data["reimbursement_id"] = reimbursement_id

        with path.open("rb") as fh:
            resp = requests.post(
                f"{self.base_url.rstrip('/')}/developer/v1/reimbursements/submit-receipt",
                headers=self._auth_headers(),
                data=data,
                files={"receipt": (path.name, fh)},
                timeout=60,
            )
        resp.raise_for_status()
        return resp.json()

    # --- Cards (cards:write / cards:read_vault+limits:write+funds:write) ---

    def create_virtual_card(
        self,
        user_id: str,
        limit_amount: Any,
        interval: str,
        currency_code: str = "USD",
        display_name: Optional[str] = None,
        spend_program_id: Optional[str] = None,
        lock_date: Optional[str] = None,
        transaction_amount_limit: Optional[Dict[str, Any]] = None,
        accounting_rules: Optional[List[Dict[str, Any]]] = None,
        allowed_overage_percent_override: Optional[Any] = None,
        spending_restrictions_extra: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """POST {vault host}/.../cards/vault -- "Create a spend limit and
        retrieve sensitive card details" per Ramp's reference. Requires
        Vault API access (cards:read_vault + limits:write + funds:write) --
        works in Sandbox for every customer; Production requires Ramp's
        approval review (see module docstring). The response includes a
        full PAN/CVV -- callers MUST NOT log or persist those fields;
        this method returns the raw response so the caller decides what
        (not) to keep, but every caller in this repo's
        ramp_reimbursement_card_write component redacts them before they
        ever reach Dagster metadata or logs.
        """
        restrictions: Dict[str, Any] = {
            "interval": interval,
            "limit": {"amount": limit_amount, "currency_code": currency_code},
        }
        if lock_date is not None:
            restrictions["lock_date"] = lock_date
        if transaction_amount_limit is not None:
            restrictions["transaction_amount_limit"] = transaction_amount_limit
        if spending_restrictions_extra:
            restrictions.update(spending_restrictions_extra)

        body: Dict[str, Any] = {
            "user_id": user_id,
            "spending_restrictions": restrictions,
        }
        if display_name is not None:
            body["display_name"] = display_name
        if spend_program_id is not None:
            body["spend_program_id"] = spend_program_id
        if accounting_rules:
            body["accounting_rules"] = accounting_rules
        if allowed_overage_percent_override is not None:
            body["allowed_overage_percent_override"] = allowed_overage_percent_override

        session = self.get_client()
        if self.vault_base_url:
            url = f"{self.vault_base_url.rstrip('/')}/cards/vault"
        else:
            url = f"{self.base_url.rstrip('/')}/developer/v1/cards/vault"
        resp = session.post(url, json=body, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def update_physical_card(
        self,
        card_id: str,
        display_name: Optional[str] = None,
        fund_id: Optional[str] = None,
        automatic_routing_enabled: Optional[bool] = None,
    ) -> Dict[str, Any]:
        """PATCH /developer/v1/cards/physical/{card_id}. These three fields
        are the ONLY ones Ramp's Developer API lets you change on an
        existing card -- there is no field here (or anywhere in Ramp's
        public API) to change a card's spend_limit/restrictions after
        creation. Physical cards only; virtual cards issued via the Vault
        API have no documented update endpoint at all."""
        body: Dict[str, Any] = {}
        if display_name is not None:
            body["display_name"] = display_name
        if fund_id is not None:
            body["fund_id"] = fund_id
        if automatic_routing_enabled is not None:
            body["automatic_routing_enabled"] = automatic_routing_enabled
        if not body:
            raise ValueError("update_physical_card: at least one of display_name/fund_id/automatic_routing_enabled must be set")

        session = self.get_client()
        resp = session.patch(
            f"{self.base_url.rstrip('/')}/developer/v1/cards/physical/{card_id}",
            json=body,
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()


class RampResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a RampResource for use by other components.

    Example:
        ```yaml
        type: dagster_component_templates.RampResourceComponent
        attributes:
          resource_key: ramp_resource
          client_id_env_var: RAMP_CLIENT_ID
          client_secret_env_var: RAMP_CLIENT_SECRET
          base_url: "https://api.ramp.com"
          scope: "reimbursements:write cards:write"
        ```
    """

    resource_key: str = Field(
        default="ramp_resource",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    client_id_env_var: str = Field(
        default="RAMP_CLIENT_ID",
        description="Env var holding your Ramp Developer App's Client ID.",
    )
    client_secret_env_var: str = Field(
        default="RAMP_CLIENT_SECRET",
        description="Env var holding your Ramp Developer App's Client Secret.",
    )
    base_url: str = Field(
        default="https://api.ramp.com",
        description="Ramp Developer API base URL ('https://demo-api.ramp.com' for Sandbox).",
    )
    vault_base_url: Optional[str] = Field(
        default=None,
        description="Optional dedicated Vault API host (see RampResource docstring). Leave unset unless Ramp tells you otherwise.",
    )
    scope: str = Field(
        default="reimbursements:write cards:write",
        description="Space-separated OAuth scope(s) requested at token time.",
    )
    token_path: str = Field(
        default="/developer/v1/token",
        description="Token endpoint path, relative to base_url.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = RampResource(
            client_id_env_var=self.client_id_env_var,
            client_secret_env_var=self.client_secret_env_var,
            base_url=self.base_url,
            vault_base_url=self.vault_base_url,
            scope=self.scope,
            token_path=self.token_path,
        )
        return dg.Definitions(resources={self.resource_key: resource})
