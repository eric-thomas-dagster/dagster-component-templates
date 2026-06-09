"""CloudFunctionsInvokeAssetComponent — invoke a deployed Cloud Function.

Sends a synchronous HTTP request to a Cloud Function (gen 1 or gen 2)
or a Cloud Run-deployed function via OIDC-authenticated requests, with
optional per-row payload (when paired with an upstream DataFrame) or
a single static call.

Two source modes:
  - Static call: just `payload:` in YAML, returns the function's response.
  - Per-row call: pair with `upstream_asset_key` + `payload_column` (or
    `payload_template`), returns one row per call with the response.
"""

import json
import os
import time
from typing import Any, Dict, List, Optional

import pandas as pd

from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    MetadataValue,
    Model,
    Output,
    Resolvable,
    asset,
)
from pydantic import Field


class CloudFunctionsInvokeAssetComponent(Component, Model, Resolvable):
    """Invoke a deployed Cloud Function (gen 1 / gen 2) with OIDC auth."""

    asset_name: str = Field(description="Output asset name.")

    credentials: Optional[Dict[str, Any]] = Field(default=None)
    credentials_path: Optional[str] = Field(default=None, description="Falls back to GOOGLE_APPLICATION_CREDENTIALS.")

    function_url: str = Field(
        description=(
            "HTTPS URL of the function. For gen 2 / Cloud Run-deployed: "
            "`https://<region>-<project>.cloudfunctions.net/<name>` or the Cloud "
            "Run service URL. For private functions, OIDC-auth happens automatically."
        ),
    )
    method: str = Field(default="POST", description="HTTP method (POST / GET / PUT / PATCH / DELETE).")

    # Static call mode
    payload: Optional[Any] = Field(
        default=None,
        description="Static request body (dict → JSON, str → raw). Mutually exclusive with upstream_asset_key.",
    )

    # Per-row call mode
    upstream_asset_key: Optional[str] = Field(
        default=None,
        description="Upstream DataFrame asset. One function call per row.",
    )
    payload_column: Optional[str] = Field(
        default=None,
        description="When set, publish only this column's value as the body. dict → JSON, others → str.",
    )
    payload_template: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Static dict template. Each {column_name} string in any value is "
            "replaced with the row's value before serialization."
        ),
    )

    # Common HTTP options
    headers: Optional[Dict[str, str]] = Field(default=None)
    timeout_seconds: float = Field(default=60.0)
    max_retries: int = Field(default=3)
    rate_limit_delay: float = Field(default=0.0)

    description: Optional[str] = Field(default=None)
    group_name: Optional[str] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)
    tags: Optional[Dict[str, str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        creds_dict = self.credentials
        if creds_dict is None:
            cred_path = self.credentials_path or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
            if cred_path:
                with open(cred_path, "r") as fh:
                    creds_dict = json.load(fh)
        if creds_dict is None:
            raise ValueError("Provide credentials, credentials_path, or set GOOGLE_APPLICATION_CREDENTIALS.")

        if self.upstream_asset_key and self.payload is not None:
            raise ValueError("Set static `payload` OR `upstream_asset_key` (per-row), not both.")
        if not self.upstream_asset_key and self.payload is None and self.method.upper() in ("POST", "PUT", "PATCH"):
            # GET / DELETE OK without a body; mutating verbs need one.
            raise ValueError(f"method={self.method} requires either `payload` or `upstream_asset_key`.")

        asset_name = self.asset_name
        function_url = self.function_url
        method = self.method.upper()
        static_payload = self.payload
        upstream_asset_key = self.upstream_asset_key
        payload_column = self.payload_column
        payload_template = self.payload_template
        extra_headers = dict(self.headers or {})
        timeout_seconds = self.timeout_seconds
        max_retries = self.max_retries
        rate_limit_delay = self.rate_limit_delay

        ins_kwargs: Dict[str, Any] = {}
        if upstream_asset_key:
            ins_kwargs["ins"] = {"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))}

        @asset(
            key=AssetKey.from_user_string(asset_name),
            description=self.description or f"Invoke Cloud Function {function_url}.",
            group_name=self.group_name,
            kinds={"google", "cloud-functions"},
            tags=self.tags or None,
            owners=self.owners or None,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])] or None,
            **ins_kwargs,
        )
        def _asset(context: AssetExecutionContext, **kwargs) -> Output:
            try:
                from google.oauth2 import service_account
                from google.auth.transport.requests import Request, AuthorizedSession
                from google.oauth2.id_token import fetch_id_token
                import google.auth.transport.requests
                from google.oauth2 import id_token as gid_token
            except ImportError:
                raise ImportError("pip install google-auth google-auth-oauthlib requests")
            try:
                import requests
            except ImportError:
                raise ImportError("pip install requests")

            sa_creds = service_account.Credentials.from_service_account_info(creds_dict)

            # Mint an OIDC ID token with the function URL as the audience.
            from google.oauth2 import service_account as _sa
            id_creds = _sa.IDTokenCredentials.from_service_account_info(creds_dict, target_audience=function_url)
            id_creds.refresh(Request())
            id_token = id_creds.token

            base_headers = {"Authorization": f"Bearer {id_token}", "Content-Type": "application/json"}
            base_headers.update(extra_headers)

            def _build_payload_for_row(row: pd.Series) -> Any:
                if payload_column:
                    return row[payload_column]
                if payload_template:
                    rendered: Dict[str, Any] = {}
                    for k, v in payload_template.items():
                        if isinstance(v, str) and "{" in v:
                            try:
                                rendered[k] = v.format(**row.to_dict())
                            except KeyError:
                                rendered[k] = v
                        else:
                            rendered[k] = v
                    return rendered
                # Default: JSON-serialize the whole row
                return row.to_dict()

            def _do_one_call(body: Any) -> Dict[str, Any]:
                last_err: Optional[Exception] = None
                for attempt in range(max_retries + 1):
                    try:
                        if isinstance(body, (dict, list)) or body is None:
                            resp = requests.request(method, function_url, json=body, headers=base_headers, timeout=timeout_seconds)
                        elif isinstance(body, bytes):
                            resp = requests.request(method, function_url, data=body, headers=base_headers, timeout=timeout_seconds)
                        else:
                            resp = requests.request(method, function_url, data=str(body), headers=base_headers, timeout=timeout_seconds)
                        if 200 <= resp.status_code < 300:
                            try:
                                return {"status": resp.status_code, "body": resp.json()}
                            except Exception:
                                return {"status": resp.status_code, "body": resp.text}
                        if resp.status_code in (429, 500, 502, 503, 504) and attempt < max_retries:
                            time.sleep((2 ** attempt) * 0.5)
                            continue
                        return {"status": resp.status_code, "body": resp.text, "_error": f"HTTP {resp.status_code}"}
                    except Exception as e:
                        last_err = e
                        if attempt < max_retries:
                            time.sleep((2 ** attempt) * 0.5)
                            continue
                return {"_error": str(last_err)}

            if upstream_asset_key:
                df = kwargs["upstream"].copy().reset_index(drop=True)
                rows = []
                for _i, row in df.iterrows():
                    body = _build_payload_for_row(row)
                    res = _do_one_call(body)
                    rows.append({
                        "request": body if isinstance(body, (dict, list, str, int, float, bool, type(None))) else str(body),
                        **res,
                    })
                    if rate_limit_delay > 0:
                        time.sleep(rate_limit_delay)
                out_df = pd.concat([df.reset_index(drop=True), pd.DataFrame(rows)], axis=1)
                ok = sum(1 for r in rows if r.get("status", 0) and 200 <= r["status"] < 300)
                return Output(
                    value=out_df,
                    metadata={
                        "calls":     MetadataValue.int(len(rows)),
                        "ok":        MetadataValue.int(ok),
                        "function":  MetadataValue.url(function_url),
                        "preview":   MetadataValue.md(out_df.head(5).to_markdown(index=False) or ""),
                    },
                )

            # Static-call mode
            res = _do_one_call(static_payload)
            df = pd.DataFrame([res])
            return Output(
                value=df,
                metadata={
                    "function":  MetadataValue.url(function_url),
                    "status":    MetadataValue.int(int(res.get("status", 0) or 0)),
                    "preview":   MetadataValue.md(df.to_markdown(index=False) or ""),
                },
            )

        return Definitions(assets=[_asset])
