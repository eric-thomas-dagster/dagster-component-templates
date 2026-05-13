"""OData Resource.

Register an OData service connection once and let multiple components reference it.
Mirrors `sap_hana_resource` — the resource holds the base URL + auth config, and
exposes helpers (`session()`, `get_url()`) that downstream components / custom
assets use.

Headless-friendly: all credentials come from env vars, no interactive auth.

Typical wiring:

    ```yaml
    # resources/sap_s4_odata.yaml
    type: dagster_component_templates.ODataResourceComponent
    attributes:
      resource_key: sap_s4_odata
      service_url: https://my300000.s4hana.cloud.sap/sap/opu/odata/sap/API_BUSINESS_PARTNER
      auth_type: basic
      auth_username_env_var: S4HANA_USER
      auth_password_env_var: S4HANA_PASSWORD
      sap_client: "100"
    ```

    ```python
    # in a custom asset
    @asset(required_resource_keys={"sap_s4_odata"})
    def my_asset(context):
        s = context.resources.sap_s4_odata.session()
        r = s.get(context.resources.sap_s4_odata.url("A_BusinessPartner") + "?$top=5")
        ...
    ```

For OAuth flows (Concur / Ariba / Datasphere), use `oauth_token_resource`
alongside this one — that resource handles the token lifecycle (refresh,
rotation writeback) and exposes the `Authorization` header.
"""

import os
from typing import Optional

import dagster as dg
from pydantic import Field


class ODataResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a reusable OData connection (base URL + auth) as a Dagster resource.

    Exposes:
    - `session()` — pre-configured `requests.Session` with auth + default headers
    - `url(entity_set)` — joins service_url + entity_set
    - `service_url`, `odata_version`, `verify_ssl` — passthrough config
    """

    resource_key: str = Field(default="odata", description="Dagster resource key")

    service_url: str = Field(
        description="Base service URL, e.g. 'https://my300000.s4hana.cloud.sap/sap/opu/odata/sap/API_BUSINESS_PARTNER'"
    )
    odata_version: str = Field(
        default="v2", description="'v2' (default) or 'v4'."
    )

    # --- Auth -----------------------------------------------------------------

    auth_type: str = Field(
        default="basic",
        description="'basic' | 'bearer' | 'oauth_resource' | 'none'.",
    )
    auth_username_env_var: Optional[str] = Field(
        default=None, description="auth_type=basic"
    )
    auth_password_env_var: Optional[str] = Field(
        default=None, description="auth_type=basic"
    )
    auth_token_env_var: Optional[str] = Field(
        default=None, description="auth_type=bearer — env var holding the bearer token"
    )
    oauth_resource_key: Optional[str] = Field(
        default=None,
        description=(
            "auth_type=oauth_resource — name of an `oauth_token_resource` registered in the "
            "project that provides fresh access tokens. The session() method will read from "
            "that resource on each call."
        ),
    )

    # --- SAP-specific quality-of-life ----------------------------------------

    sap_client: Optional[str] = Field(
        default=None,
        description="Sets the `sap-client` header (e.g. '100'). Skip on HANA Cloud / non-SAP backends.",
    )
    csrf_fetch_path: Optional[str] = Field(
        default=None,
        description=(
            "For write APIs that require a CSRF token: relative path to fetch with "
            "`x-csrf-token: fetch`. The token is cached on the session. Optional."
        ),
    )

    # --- TLS ----------------------------------------------------------------

    verify_ssl: bool = Field(default=True)
    timeout_seconds: int = Field(default=120)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        cfg = self

        class ODataResource(dg.ConfigurableResource):
            """OData connection resource — exposes a pre-configured requests session."""

            @property
            def service_url(self) -> str:
                return cfg.service_url

            @property
            def odata_version(self) -> str:
                return cfg.odata_version

            @property
            def verify_ssl(self) -> bool:
                return cfg.verify_ssl

            @property
            def timeout_seconds(self) -> int:
                return cfg.timeout_seconds

            def url(self, entity_set: str) -> str:
                return cfg.service_url.rstrip("/") + "/" + entity_set.lstrip("/")

            def session(self):
                import requests

                s = requests.Session()
                s.verify = cfg.verify_ssl
                s.headers.update({"Accept": "application/json"})
                if cfg.sap_client:
                    s.headers["sap-client"] = cfg.sap_client

                if cfg.auth_type == "basic":
                    if not (cfg.auth_username_env_var and cfg.auth_password_env_var):
                        raise ValueError(
                            "auth_type='basic' requires auth_username_env_var + auth_password_env_var."
                        )
                    s.auth = (
                        os.environ[cfg.auth_username_env_var],
                        os.environ[cfg.auth_password_env_var],
                    )
                elif cfg.auth_type == "bearer":
                    if not cfg.auth_token_env_var:
                        raise ValueError("auth_type='bearer' requires auth_token_env_var.")
                    s.headers["Authorization"] = f"Bearer {os.environ[cfg.auth_token_env_var]}"
                elif cfg.auth_type == "oauth_resource":
                    # Note: actually resolving this requires the consumer to look up the
                    # oauth_token_resource and call its get_access_token() at request time.
                    # We can't reach into Dagster's resource graph from here.
                    raise NotImplementedError(
                        "auth_type='oauth_resource' is a marker — your asset must read the "
                        "named oauth_token_resource via context.resources and set the header itself. "
                        "See oauth_token_resource README for the pattern."
                    )
                elif cfg.auth_type == "none":
                    pass
                else:
                    raise ValueError(f"unknown auth_type: {cfg.auth_type!r}")

                if cfg.csrf_fetch_path:
                    r = s.get(
                        cfg.service_url.rstrip("/") + "/" + cfg.csrf_fetch_path.lstrip("/"),
                        headers={"x-csrf-token": "fetch"},
                        timeout=cfg.timeout_seconds,
                    )
                    token = r.headers.get("x-csrf-token")
                    if token:
                        s.headers["x-csrf-token"] = token

                return s

        return dg.Definitions(resources={cfg.resource_key: ODataResource()})
