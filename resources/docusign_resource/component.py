"""DocuSign Resource.

Wraps DocuSign's eSignature REST API (v2.1) using the JWT Grant OAuth2
flow -- the server-to-server auth DocuSign recommends for unattended
integrations (no end-user browser redirect), and the same flow this
repo's `docusign_ingestion` component already uses for read access. This
resource is the write-side counterpart: it exposes envelope creation so
reverse-ETL components can trigger an envelope send per warehouse row.

Auth: JWT Grant (RFC 7523) -- requires:
  - `integration_key`: the Integration Key (client ID) with an RSA keypair
    registered for JWT grant in the DocuSign app settings.
  - `user_id`: GUID of the DocuSign user being impersonated. That user
    must have granted one-time JWT consent (via the OAuth consent URL)
    before the first JWT exchange succeeds.
  - `private_key`: PEM-format RSA private key matching the public key
    registered on the Integration Key.

Flow: sign a JWT assertion (iss=integration_key, sub=user_id, RS256),
exchange it at `POST https://{account|account-d}.docusign.com/oauth/token`
for an access_token, then call `GET .../oauth/userinfo` to resolve the
account's `base_uri` (DocuSign accounts live on different regional API
hosts -- `base_uri` is NOT a fixed URL, it comes back in the userinfo
response per-account). The access token + resolved base_uri/account_id
are cached in-memory (class-level, keyed by integration_key+user_id+
account_id+env) until shortly before the token's expiry.

Envelope creation: `POST {base_uri}/restapi/v2.1/accounts/{accountId}/envelopes`
with `templateId` + `templateRoles` (recipient roleName/name/email,
optionally prefilling template text tabs via `tabs`). `status="sent"`
sends immediately; `status="created"` leaves the envelope as a draft in
the sender's DocuSign account for manual review before sending.
"""
import time
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


class DocuSignResource(dg.ConfigurableResource):
    """DocuSign eSignature REST API client wrapper (JWT Grant OAuth2)."""

    integration_key: str = Field(
        description="DocuSign Integration Key (client ID) with an RSA keypair registered for JWT grant."
    )
    user_id: str = Field(
        description="GUID of the DocuSign user being impersonated (must have granted one-time JWT consent)."
    )
    private_key: str = Field(
        description="RSA private key (PEM format) matching the public key registered on the Integration Key."
    )
    account_id: Optional[str] = Field(
        default=None,
        description="Specific DocuSign account ID to use. If unset, the default account from the JWT userinfo response is used.",
    )
    use_demo_env: bool = Field(
        default=False,
        description="Use DocuSign's demo/sandbox environment (account-d.docusign.com) instead of production.",
    )

    _auth_cache: dict = {}

    def _cache_key(self) -> str:
        return f"{self.integration_key}:{self.user_id}:{self.account_id}:{self.use_demo_env}"

    def _get_auth(self):
        """Returns (access_token, base_uri, account_id) -- cached
        in-memory until ~60s before the access token's expiry."""
        import jwt
        import requests

        key = self._cache_key()
        cached = DocuSignResource._auth_cache.get(key) or {}
        if cached.get("expires", 0) > time.time() + 60:
            return cached["access_token"], cached["base_uri"], cached["account_id"]

        auth_host = "account-d.docusign.com" if self.use_demo_env else "account.docusign.com"
        now = int(time.time())
        assertion = jwt.encode(
            {
                "iss": self.integration_key,
                "sub": self.user_id,
                "aud": auth_host,
                "iat": now,
                "exp": now + 3600,
                "scope": "signature impersonation",
            },
            self.private_key,
            algorithm="RS256",
        )
        token_resp = requests.post(
            f"https://{auth_host}/oauth/token",
            data={
                "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
                "assertion": assertion,
            },
            timeout=30,
        )
        token_resp.raise_for_status()
        token_data = token_resp.json()
        access_token = token_data["access_token"]

        userinfo_resp = requests.get(
            f"https://{auth_host}/oauth/userinfo",
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=30,
        )
        userinfo_resp.raise_for_status()
        accounts = userinfo_resp.json().get("accounts", [])
        if self.account_id:
            account = next((a for a in accounts if a["account_id"] == self.account_id), None)
        else:
            account = next((a for a in accounts if a.get("is_default")), accounts[0] if accounts else None)
        if not account:
            raise RuntimeError(
                "Could not resolve a DocuSign account_id from the JWT userinfo response."
            )

        base_uri = account["base_uri"]
        resolved_account_id = account["account_id"]
        DocuSignResource._auth_cache[key] = {
            "access_token": access_token,
            "base_uri": base_uri,
            "account_id": resolved_account_id,
            # DocuSign access tokens are typically valid 8h (28800s); refresh
            # a little early rather than racing expiry.
            "expires": time.time() + token_data.get("expires_in", 28800) - 60,
        }
        return access_token, base_uri, resolved_account_id

    def create_envelope(
        self,
        template_id: str,
        template_roles: List[Dict[str, Any]],
        email_subject: str = "Please sign this document",
        status: str = "sent",
    ) -> dict:
        """`POST {base_uri}/restapi/v2.1/accounts/{accountId}/envelopes` --
        creates (and, when status='sent', immediately sends) an envelope
        built from a template. Returns the envelope creation response
        (`{'envelopeId', 'status', 'statusDateTime', 'uri'}`)."""
        import requests

        access_token, base_uri, account_id = self._get_auth()
        url = f"{base_uri}/restapi/v2.1/accounts/{account_id}/envelopes"
        body: Dict[str, Any] = {
            "templateId": template_id,
            "templateRoles": template_roles,
            "emailSubject": email_subject,
            "status": status,
        }
        resp = requests.post(
            url,
            json=body,
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
            },
            timeout=60,
        )
        resp.raise_for_status()
        return resp.json()


class DocuSignResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a DocuSignResource for use by other components.

    Example:
        ```yaml
        type: dagster_component_templates.DocuSignResourceComponent
        attributes:
          resource_key: docusign_resource
          integration_key: "{{ env('DOCUSIGN_INTEGRATION_KEY') }}"
          user_id: "{{ env('DOCUSIGN_USER_ID') }}"
          private_key: "{{ env('DOCUSIGN_PRIVATE_KEY') }}"
        ```
    """

    resource_key: str = Field(
        default="docusign_resource",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    integration_key: str = Field(
        description="DocuSign Integration Key (client ID) with an RSA keypair registered for JWT grant."
    )
    user_id: str = Field(
        description="GUID of the DocuSign user being impersonated (must have granted one-time JWT consent)."
    )
    private_key: str = Field(
        description="RSA private key (PEM format) matching the public key registered on the Integration Key."
    )
    account_id: Optional[str] = Field(
        default=None,
        description="Specific DocuSign account ID. If unset, the default account from the JWT userinfo response is used.",
    )
    use_demo_env: bool = Field(
        default=False,
        description="Use DocuSign's demo/sandbox environment (account-d.docusign.com) instead of production.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = DocuSignResource(
            integration_key=self.integration_key,
            user_id=self.user_id,
            private_key=self.private_key,
            account_id=self.account_id,
            use_demo_env=self.use_demo_env,
        )
        return dg.Definitions(resources={self.resource_key: resource})
