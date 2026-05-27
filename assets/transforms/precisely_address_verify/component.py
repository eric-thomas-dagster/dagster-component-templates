"""Precisely Address Verify / Geo Addressing — column transform.

Calls the Precisely Address Verify (a.k.a. Geo Addressing) REST API
to standardize + CASS-certify an address column in an upstream pandas
DataFrame. Emits new columns alongside the source — never mutates the
existing address column.

Distinct from the generic ``geocoder`` component: that one returns lat/lng
coordinates from Nominatim/Google/HERE; this one returns Precisely's
CASS-certified standardized address plus its match-quality scoring.
Customers using Precisely for address quality (mailing-list deliverability,
USPS Move Update, address parsing) want this component, not the geocoder.

Auth follows DIS's OAuth2 client-credentials flow against the global
token endpoint. The Address Verify API has a public trial-key tier
that lets this component reach ``live`` validation from CI; bigger
volumes (sustained > 1k calls/day) need a paid tenant.

Endpoint: POST https://api.precisely.com/addresses/v1/verify
Docs:     https://docs.precisely.com/docs/sftw/precisely-apis/

Output columns (added to the right of the source DataFrame):
  - {output_prefix}status                AV/AC/IC/EC — Address Verified/Corrected/Invalid/Error
  - {output_prefix}standardized          single-line standardized address
  - {output_prefix}match_score           0-100 confidence
  - {output_prefix}country               ISO 3166-1 alpha-3 (USA, CAN, ...)
  - {output_prefix}postal_code           normalized postal/ZIP code
  - {output_prefix}latitude              optional lat (if returned by tier)
  - {output_prefix}longitude             optional lng (if returned by tier)
"""
import os
from typing import Any, Dict, List, Optional

import dagster as dg
import pandas as pd
from pydantic import Field

PRECISELY_DEFAULT_TOKEN_URL = "https://api.precisely.com/oauth/token"
PRECISELY_DEFAULT_VERIFY_URL = "https://api.precisely.com/addresses/v1/verify"


class PreciselyAddressVerifyComponent(dg.Component, dg.Model, dg.Resolvable):
    """Verify + standardize an address column via Precisely Address Verify.

    Example:
        ```yaml
        type: dagster_community_components.PreciselyAddressVerifyComponent
        attributes:
          asset_name: verified_customer_addresses
          upstream_asset_key: customer_raw_addresses
          address_column: full_address
          country_column: country_code        # optional; defaults to USA
          output_prefix: av_
          client_id_env_var: PRECISELY_CLIENT_ID
          client_secret_env_var: PRECISELY_CLIENT_SECRET
        ```
    """

    asset_name: str = Field(description="Output Dagster asset name.")
    upstream_asset_key: str = Field(
        description="Upstream asset key providing a DataFrame with the address column.",
    )
    address_column: str = Field(
        description=(
            "Source column with the single-line address string "
            "('123 Main St, Springfield, MA 01103'). The API can also "
            "accept parsed components — see address_line_columns if you "
            "already have street/city/state split out."
        ),
    )
    address_line_columns: Optional[Dict[str, str]] = Field(
        default=None,
        description=(
            "Optional: pass parsed address parts instead of a single line. "
            "Map of {payload_field: source_column}, e.g. "
            "{addressLines: street, city: city, postalCode: zip}. When set, "
            "address_column is ignored."
        ),
    )
    country_column: Optional[str] = Field(
        default=None,
        description=(
            "Optional column with ISO 3166-1 alpha-3 country code per row. "
            "Falls back to default_country when null/missing."
        ),
    )
    default_country: str = Field(
        default="USA",
        description="Country code used when country_column is missing.",
    )
    output_prefix: str = Field(
        default="av_",
        description="Prefix for added columns (default 'av_': av_status, av_standardized, ...).",
    )

    client_id_env_var: str = Field(
        default="PRECISELY_CLIENT_ID",
        description="Env var with Precisely OAuth2 client_id.",
    )
    client_secret_env_var: str = Field(
        default="PRECISELY_CLIENT_SECRET",
        description="Env var with Precisely OAuth2 client_secret.",
    )
    token_url: str = Field(
        default=PRECISELY_DEFAULT_TOKEN_URL,
        description="OAuth2 token endpoint.",
    )
    verify_url: str = Field(
        default=PRECISELY_DEFAULT_VERIFY_URL,
        description="Address Verify endpoint.",
    )
    request_timeout_seconds: int = Field(
        default=30,
        ge=1,
        description="Per-request HTTP timeout.",
    )
    batch_size: int = Field(
        default=100,
        ge=1,
        le=1000,
        description="Addresses per request batch (Address Verify accepts batches).",
    )

    group_name: Optional[str] = Field(default="precisely", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    asset_tags: Optional[Dict[str, str]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds (auto-includes 'precisely').")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("precisely")

        @dg.asset(
            name=_self.asset_name,
            ins={"upstream": dg.AssetIn(key=dg.AssetKey.from_user_string(_self.upstream_asset_key))},
            group_name=_self.group_name,
            kinds=kinds,
            owners=_self.owners,
            tags=_self.asset_tags,
            description=_self.description or (
                "Addresses verified + standardized via the Precisely Address "
                "Verify (Geo Addressing) REST API. Adds CASS-certified columns "
                "alongside the source."
            ),
        )
        def _asset(context: dg.AssetExecutionContext, upstream: pd.DataFrame) -> dg.MaterializeResult:
            try:
                import requests
            except ImportError:
                raise ImportError("precisely_address_verify requires `requests`: pip install requests")

            client_id = os.environ.get(_self.client_id_env_var, "")
            client_secret = os.environ.get(_self.client_secret_env_var, "")
            if not client_id or not client_secret:
                raise RuntimeError(
                    f"Precisely creds missing: set {_self.client_id_env_var} "
                    f"and {_self.client_secret_env_var}."
                )

            # OAuth2 client-credentials. Single token reused across batches.
            token_resp = requests.post(
                _self.token_url,
                data={"grant_type": "client_credentials"},
                auth=(client_id, client_secret),
                timeout=_self.request_timeout_seconds,
            )
            token_resp.raise_for_status()
            token = token_resp.json()["access_token"]
            headers = {
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
                "Content-Type": "application/json",
            }

            def _row_to_payload(row) -> dict:
                country = _self.default_country
                if _self.country_column and _self.country_column in upstream.columns:
                    v = row.get(_self.country_column)
                    if isinstance(v, str) and v.strip():
                        country = v.strip().upper()
                if _self.address_line_columns:
                    parts: Dict[str, Any] = {}
                    for payload_field, src_col in _self.address_line_columns.items():
                        v = row.get(src_col)
                        if v is not None and not (isinstance(v, float) and v != v):  # not NaN
                            parts[payload_field] = str(v)
                    parts["country"] = country
                    return parts
                return {
                    "addressLines": [str(row.get(_self.address_column, "") or "")],
                    "country": country,
                }

            df = upstream.reset_index(drop=True).copy()
            results: List[Dict[str, Any]] = [{} for _ in range(len(df))]

            # Batch the calls.
            for start in range(0, len(df), _self.batch_size):
                end = min(start + _self.batch_size, len(df))
                batch_rows = [df.iloc[i].to_dict() for i in range(start, end)]
                payload = {"addresses": [_row_to_payload(r) for r in batch_rows]}
                resp = requests.post(
                    _self.verify_url,
                    headers=headers,
                    json=payload,
                    timeout=_self.request_timeout_seconds,
                )
                resp.raise_for_status()
                body = resp.json()
                items = body.get("addresses") or body.get("results") or []
                for idx_in_batch, item in enumerate(items):
                    if start + idx_in_batch >= len(df):
                        break
                    # Address Verify response shape: status code (AV/AC/IC/EC),
                    # standardized single-line, match score, postal, lat/lng.
                    addr = item.get("address") or {}
                    results[start + idx_in_batch] = {
                        f"{_self.output_prefix}status": item.get("status") or item.get("statusCode"),
                        f"{_self.output_prefix}standardized": (
                            addr.get("formattedAddress")
                            or addr.get("formatted_address")
                            or " ".join(addr.get("addressLines") or [])
                        ),
                        f"{_self.output_prefix}match_score": item.get("matchScore") or item.get("score"),
                        f"{_self.output_prefix}country": addr.get("country"),
                        f"{_self.output_prefix}postal_code": addr.get("postalCode") or addr.get("postal_code"),
                        f"{_self.output_prefix}latitude": (item.get("location") or {}).get("latitude"),
                        f"{_self.output_prefix}longitude": (item.get("location") or {}).get("longitude"),
                    }

            verify_df = pd.DataFrame(results)
            out = pd.concat([df, verify_df], axis=1)

            verified_count = int(
                (verify_df[f"{_self.output_prefix}status"].astype(str).str.upper().isin(["AV", "AC"])).sum()
            ) if f"{_self.output_prefix}status" in verify_df.columns else 0

            return dg.MaterializeResult(
                value=out,
                metadata={
                    "row_count": dg.MetadataValue.int(len(out)),
                    "verified_count": dg.MetadataValue.int(verified_count),
                    "verification_rate": dg.MetadataValue.float(
                        verified_count / len(out) if len(out) else 0.0
                    ),
                    "precisely_endpoint": dg.MetadataValue.url(_self.verify_url),
                },
            )

        return dg.Definitions(assets=[_asset])
