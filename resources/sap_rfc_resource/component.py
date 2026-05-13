"""SAP RFC Resource.

Registers a connection to an SAP system over the classic RFC (Remote Function
Call) protocol — the integration path for on-prem R/3 / ECC / S/4HANA that
predates OData. RFC handles BAPIs, custom Z*-named function modules, and IDocs.

Why this matters: a huge share of enterprise SAP installs are on-prem R/3 or
ECC. They don't expose OData; integration is via RFC. This resource lets you
use those systems from Dagster.

Driver: `pyrfc` (the SAP-supplied Python binding to the NetWeaver RFC SDK).
The SDK itself is a closed-source C library — customers download it from SAP
Support Portal (requires SAP customer license, free for licensed customers):
https://launchpad.support.sap.com → Software Downloads → SAP NW RFC SDK 7.50.

Install sequence:

    1. Download "SAP NW RFC SDK 7.50" for your platform
    2. Extract to /opt/sap/nwrfcsdk (or wherever)
    3. Set env vars before `pip install pyrfc`:
         export SAPNWRFC_HOME=/opt/sap/nwrfcsdk
         export LD_LIBRARY_PATH=$SAPNWRFC_HOME/lib:$LD_LIBRARY_PATH      # Linux
         export DYLD_LIBRARY_PATH=$SAPNWRFC_HOME/lib:$DYLD_LIBRARY_PATH  # macOS
    4. pip install pyrfc

In containerized deployments, bake the SDK into the image and set the env vars
in the Dockerfile.

This resource registers the connection params; `sap_rfc_ingestion` uses it
to execute RFC calls.
"""

import os
from typing import Optional

import dagster as dg
from pydantic import Field


class SapRFCResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register an SAP RFC connection as a Dagster resource.

    Two connection modes (use ONE):
    1. **Direct application server** — `ashost` + `sysnr` + `client` + `user` + `passwd_env_var`
    2. **Message server (load-balanced)** — `mshost` + `sysid` + `group` + `client` + `user` + `passwd_env_var`

    Example — direct connect to ECC on-prem:

        ```yaml
        type: dagster_component_templates.SapRFCResourceComponent
        attributes:
          resource_key: sap_ecc
          ashost: sapecc01.acme.com
          sysnr: "00"
          client: "100"
          user: DAGSTER_RFC
          passwd_env_var: SAP_RFC_PASSWORD
          lang: EN
        ```
    """

    resource_key: str = Field(default="sap_rfc", description="Dagster resource key")

    # --- Direct application-server connect -----------------------------------

    ashost: Optional[str] = Field(
        default=None, description="Application server host (direct connect)"
    )
    sysnr: Optional[str] = Field(
        default=None, description="System number, two-digit string (e.g. '00')"
    )

    # --- Message-server (load-balanced) connect -----------------------------

    mshost: Optional[str] = Field(
        default=None, description="Message server host (load-balanced connect)"
    )
    msserv: Optional[str] = Field(
        default=None,
        description="Message server port/service name (default sapms<SYSID>).",
    )
    sysid: Optional[str] = Field(
        default=None, description="System ID, three-character (e.g. 'PRD')"
    )
    group: Optional[str] = Field(
        default=None, description="Logon group, e.g. 'PUBLIC'"
    )

    # --- Auth + identity ----------------------------------------------------

    client: str = Field(description="SAP client number, three-digit string (e.g. '100')")
    user: str = Field(description="RFC user name")
    passwd_env_var: str = Field(description="Env var holding the user's password")
    lang: str = Field(default="EN", description="Logon language code (e.g. 'EN', 'DE')")

    # --- TLS / SNC (optional) -----------------------------------------------

    snc_qop: Optional[str] = Field(
        default=None,
        description="SNC quality of protection: '1' (auth only) / '2' (integrity) / '3' (privacy) / '9' (max).",
    )
    snc_myname: Optional[str] = Field(
        default=None, description="SNC partner name (client side)"
    )
    snc_partnername: Optional[str] = Field(
        default=None, description="SNC partner name (server side)"
    )
    snc_lib: Optional[str] = Field(
        default=None, description="Path to SNC library (e.g. CommonCryptoLib)"
    )

    # --- Misc ---------------------------------------------------------------

    trace: int = Field(default=0, description="RFC trace level 0-3")
    connect_timeout_seconds: int = Field(default=30)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        cfg = self

        class SapRFCResource(dg.ConfigurableResource):
            """SAP RFC connection — provides connection-param dict and helpers."""

            def conn_params(self) -> dict:
                """Build the kwargs dict for pyrfc.Connection(**kwargs)."""
                if not cfg.ashost and not cfg.mshost:
                    raise ValueError(
                        "SAP RFC resource needs either ashost (direct) or mshost (message server)."
                    )
                params: dict = {
                    "client": cfg.client,
                    "user": cfg.user,
                    "passwd": os.environ[cfg.passwd_env_var],
                    "lang": cfg.lang,
                    "trace": str(cfg.trace),
                }
                if cfg.ashost:
                    if not cfg.sysnr:
                        raise ValueError("ashost requires sysnr.")
                    params["ashost"] = cfg.ashost
                    params["sysnr"] = cfg.sysnr
                else:
                    if not (cfg.sysid and cfg.group):
                        raise ValueError("mshost requires sysid + group.")
                    params["mshost"] = cfg.mshost
                    params["sysid"] = cfg.sysid
                    params["group"] = cfg.group
                    if cfg.msserv:
                        params["msserv"] = cfg.msserv
                if cfg.snc_qop:
                    params["snc_qop"] = cfg.snc_qop
                    if cfg.snc_myname:
                        params["snc_myname"] = cfg.snc_myname
                    if cfg.snc_partnername:
                        params["snc_partnername"] = cfg.snc_partnername
                    if cfg.snc_lib:
                        params["snc_lib"] = cfg.snc_lib
                return params

            def connection(self):
                """Return a live pyrfc.Connection. Caller is responsible for context-managing it.

                Usage:
                    with resource.connection() as conn:
                        result = conn.call('RFC_READ_TABLE', QUERY_TABLE='MARA', ROWCOUNT=10)
                """
                from pyrfc import Connection

                return Connection(**self.conn_params())

            def ping(self) -> bool:
                """Smoke test — calls RFC_PING. Raises on failure."""
                with self.connection() as conn:
                    conn.call("RFC_PING")
                return True

        return dg.Definitions(resources={cfg.resource_key: SapRFCResource()})
