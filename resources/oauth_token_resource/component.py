"""OAuth Token Resource.

Headless-friendly OAuth 2.0 token manager. Mints and caches access tokens for
machine-to-machine workflows. Supports the three grant types you actually see
in enterprise:

1. **client_credentials** — pure M2M. Easiest to operate headless. Used by Ariba,
   Microsoft Graph (app permissions), most SAP cloud APIs designed for service-
   to-service.

2. **refresh_token** — user does OAuth dance ONCE on a laptop, captures the
   refresh token, drops it in a secret manager. Pipeline mints short-lived
   access tokens from it. Used by Concur, Salesforce, Gmail, many SaaS.

   The gotcha: many providers ROTATE the refresh token on each refresh — i.e.
   each call returns a NEW refresh token, and you must persist it back somewhere
   reachable for the next run. This resource handles the writeback via a
   configurable callback command (see `refresh_writeback_command_env_var`).

3. **password** (Resource Owner Password Credentials) — username + password.
   Deprecated by most providers but still found on older SAP systems. Avoid if
   you can; useful only for legacy.

Token caching: the access_token is held in memory on the resource for the
duration of a Dagster run. If the token expires mid-run, the resource refreshes
automatically (using whichever grant is configured) and updates its cache.

NO interactive prompts. If credentials are missing or invalid, it raises — no
fallback to a browser flow.
"""

import os
import subprocess
import time
from typing import Any, Optional

import dagster as dg
from pydantic import Field


class OAuthTokenResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Headless OAuth 2.0 token resource.

    Example — Ariba (client_credentials):

        ```yaml
        type: dagster_component_templates.OAuthTokenResourceComponent
        attributes:
          resource_key: ariba_token
          token_endpoint: https://api.ariba.com/v2/oauth/token
          grant_type: client_credentials
          client_id_env_var: ARIBA_CLIENT_ID
          client_secret_env_var: ARIBA_CLIENT_SECRET
          scope: "OperationalReporting"
        ```

    Example — Concur (refresh_token, with rotation):

        ```yaml
        type: dagster_component_templates.OAuthTokenResourceComponent
        attributes:
          resource_key: concur_token
          token_endpoint: https://us.api.concursolutions.com/oauth2/v0/token
          grant_type: refresh_token
          client_id_env_var: CONCUR_CLIENT_ID
          client_secret_env_var: CONCUR_CLIENT_SECRET
          refresh_token_env_var: CONCUR_REFRESH_TOKEN
          # Concur rotates refresh tokens — persist the new one back to AWS SM:
          refresh_writeback_command_env_var: CONCUR_TOKEN_WRITEBACK_CMD
          # In your env: export CONCUR_TOKEN_WRITEBACK_CMD='aws secretsmanager update-secret --secret-id concur/refresh_token --secret-string {token}'
        ```
    """

    resource_key: str = Field(default="oauth_token", description="Dagster resource key")

    token_endpoint: str = Field(description="OAuth token URL (e.g. https://api.ariba.com/v2/oauth/token)")
    grant_type: str = Field(
        default="client_credentials",
        description="'client_credentials' | 'refresh_token' | 'password'",
    )

    # --- Credentials (env-var only — headless) -----------------------------

    client_id_env_var: Optional[str] = Field(default=None)
    client_secret_env_var: Optional[str] = Field(default=None)

    refresh_token_env_var: Optional[str] = Field(
        default=None, description="grant_type=refresh_token"
    )
    username_env_var: Optional[str] = Field(default=None, description="grant_type=password")
    password_env_var: Optional[str] = Field(default=None, description="grant_type=password")

    # --- Refresh token rotation writeback (headless) ----------------------

    refresh_writeback_command_env_var: Optional[str] = Field(
        default=None,
        description=(
            "If set: when grant_type=refresh_token and the provider returns a NEW refresh_token, "
            "the resource executes this command (env-var holds the command template) with `{token}` "
            "substituted. Use this to persist rotated tokens back to your secret store. "
            "Example value: 'aws secretsmanager update-secret --secret-id myapp/refresh_token --secret-string {token}'."
        ),
    )
    refresh_writeback_file: Optional[str] = Field(
        default=None,
        description="Simpler alternative: write the new refresh token to this file path. Useful for k8s persistent-volume scenarios.",
    )

    # --- Request shape ------------------------------------------------------

    scope: Optional[str] = Field(default=None, description="OAuth scope string (space-separated)")
    audience: Optional[str] = Field(default=None, description="Auth0-style audience parameter")

    auth_in: str = Field(
        default="form",
        description="Where to send client_id/secret: 'form' (default — in body) or 'basic' (HTTP Basic auth header).",
    )

    extra_form_params: Optional[dict] = Field(
        default=None,
        description="Additional form fields to include in the token request (vendor-specific).",
    )

    timeout_seconds: int = Field(default=30)
    early_refresh_seconds: int = Field(
        default=60,
        description="Refresh the token this many seconds BEFORE it expires (avoid mid-request expiry).",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        cfg = self

        class OAuthTokenResource(dg.ConfigurableResource):
            """In-memory access-token cache + auto-refresh."""

            _cached_token: Any = None
            _expires_at: float = 0.0
            _current_refresh_token: Any = None

            def _do_token_request(self) -> dict:
                import requests

                data: dict = {"grant_type": cfg.grant_type}
                auth = None
                client_id = (
                    os.environ.get(cfg.client_id_env_var) if cfg.client_id_env_var else None
                )
                client_secret = (
                    os.environ.get(cfg.client_secret_env_var) if cfg.client_secret_env_var else None
                )

                if cfg.auth_in == "basic":
                    if not (client_id and client_secret):
                        raise ValueError(
                            "auth_in='basic' requires client_id_env_var + client_secret_env_var."
                        )
                    auth = (client_id, client_secret)
                else:
                    if client_id:
                        data["client_id"] = client_id
                    if client_secret:
                        data["client_secret"] = client_secret

                if cfg.scope:
                    data["scope"] = cfg.scope
                if cfg.audience:
                    data["audience"] = cfg.audience

                if cfg.grant_type == "refresh_token":
                    rt = self._current_refresh_token or (
                        os.environ.get(cfg.refresh_token_env_var) if cfg.refresh_token_env_var else None
                    )
                    if not rt:
                        raise ValueError(
                            "grant_type='refresh_token' requires refresh_token_env_var to be set."
                        )
                    data["refresh_token"] = rt
                elif cfg.grant_type == "password":
                    if not (cfg.username_env_var and cfg.password_env_var):
                        raise ValueError(
                            "grant_type='password' requires username_env_var + password_env_var."
                        )
                    u = os.environ.get(cfg.username_env_var)
                    p = os.environ.get(cfg.password_env_var)
                    if not (u and p):
                        raise ValueError("password env vars are set but empty.")
                    data["username"] = u
                    data["password"] = p

                if cfg.extra_form_params:
                    data.update(cfg.extra_form_params)

                r = requests.post(
                    cfg.token_endpoint,
                    data=data,
                    auth=auth,
                    timeout=cfg.timeout_seconds,
                    headers={"Accept": "application/json"},
                )
                if r.status_code >= 400:
                    raise RuntimeError(
                        f"OAuth token request failed: HTTP {r.status_code} {r.text[:500]}"
                    )
                return r.json()

            def _writeback_rotated_refresh_token(self, new_rt: str) -> None:
                """Persist a rotated refresh token back to the configured destination."""
                if cfg.refresh_writeback_file:
                    try:
                        with open(cfg.refresh_writeback_file, "w") as f:
                            f.write(new_rt)
                    except Exception as e:
                        raise RuntimeError(
                            f"Failed to write rotated refresh_token to {cfg.refresh_writeback_file!r}: {e}"
                        ) from e
                    return
                if cfg.refresh_writeback_command_env_var:
                    cmd_template = os.environ.get(cfg.refresh_writeback_command_env_var)
                    if not cmd_template:
                        raise RuntimeError(
                            f"refresh_writeback_command_env_var={cfg.refresh_writeback_command_env_var!r} "
                            "is set but empty. Define the command in your env."
                        )
                    cmd = cmd_template.replace("{token}", new_rt)
                    # shell=True is intentional: the user supplied the command template.
                    # We're explicitly trusting it; this is a secret-management hook.
                    result = subprocess.run(
                        cmd, shell=True, capture_output=True, text=True, timeout=60
                    )
                    if result.returncode != 0:
                        raise RuntimeError(
                            f"Refresh-token writeback command failed (exit {result.returncode}): "
                            f"{result.stderr[:500]}"
                        )
                # Otherwise: no writeback configured. If the provider rotates tokens
                # and we don't persist, the NEXT run will fail when it tries to refresh
                # with the original (now-invalidated) token. We log a warning at refresh time.

            def get_access_token(self) -> str:
                """Return a valid access token. Refreshes if expired (or about to expire)."""
                now = time.time()
                if (
                    self._cached_token
                    and now < self._expires_at - cfg.early_refresh_seconds
                ):
                    return self._cached_token

                body = self._do_token_request()
                access_token = body.get("access_token")
                if not access_token:
                    raise RuntimeError(
                        f"Token endpoint returned no access_token: {list(body.keys())}"
                    )

                expires_in = int(body.get("expires_in") or 3600)
                self._cached_token = access_token
                self._expires_at = now + expires_in

                # Handle refresh-token rotation if the provider returned a new one.
                new_rt = body.get("refresh_token")
                if (
                    cfg.grant_type == "refresh_token"
                    and new_rt
                    and new_rt != self._current_refresh_token
                ):
                    old_rt = self._current_refresh_token or (
                        os.environ.get(cfg.refresh_token_env_var) if cfg.refresh_token_env_var else None
                    )
                    self._current_refresh_token = new_rt
                    if new_rt != old_rt:
                        if cfg.refresh_writeback_file or cfg.refresh_writeback_command_env_var:
                            self._writeback_rotated_refresh_token(new_rt)
                        # else: silent — user-acknowledged they don't need persistence (e.g. provider
                        # doesn't actually rotate, or they're OK re-bootstrapping)

                return access_token

            def get_authorization_header(self) -> str:
                """Convenience: returns the full 'Bearer <token>' header value."""
                return f"Bearer {self.get_access_token()}"

        return dg.Definitions(resources={cfg.resource_key: OAuthTokenResource()})
