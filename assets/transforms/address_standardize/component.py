"""AddressStandardize.

Parse a column of messy address strings into structured components
(street_number, street, city, state, postcode, country, ...) — a free /
open-source drop-in for Alteryx's **CASS** (Coding Accuracy Support
System) address-cleansing tool.

Three providers:

  - `libpostal` — local, offline, ~MIT license. Best parser quality.
                   Requires the libpostal binary (homebrew / apt) +
                   `pypostal` Python bindings.
  - `geoapify`  — commercial API (free tier ~3k requests/day).
                   Set GEOAPIFY_API_KEY.
  - `nominatim` — OSM/Nominatim free geocode + parse. Slower (1 req/sec
                   rate limit). Set NOMINATIM_USER_AGENT to identify your app.
  - `regex`     — naive split (US street-address-style). Pure-Python
                   fallback when no provider is available; ZIP+4-aware.

USPS CASS-certification is a paid licensed product — none of the free
providers above offer it. For DPV/CASS use the commercial Geoapify
batch tier or a licensed vendor.
"""
import json
import os
import re
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
    Resolvable,
    asset,
)
from pydantic import Field


_VALID_PROVIDERS = ("libpostal", "geoapify", "nominatim", "regex")

# Standard component fields surfaced from parsers.
_FIELDS = (
    "house_number",
    "road",
    "unit",
    "city",
    "state",
    "postcode",
    "postcode_plus4",
    "country",
)


# Naive US-style regex fallback. Catches a useful 70-80% of US street addresses
# without requiring any external dependency.
_REGEX_PATTERN = re.compile(
    r"^\s*(?P<house_number>\d+[A-Z]?)\s+"          # house number (optional letter)
    r"(?P<road>[^,]+?)"                               # street name (lazy)
    r"(?:,\s*(?P<unit>(?:apt|unit|suite|ste|#)\s*\S+))?"  # unit / apt / suite
    r"\s*,?\s*(?P<city>[A-Za-z\.\s]+?)\s*,\s*"      # city
    r"(?P<state>[A-Z]{2})\s*"                         # 2-letter state
    r"(?P<postcode>\d{5})"                            # ZIP5
    r"(?:-(?P<postcode_plus4>\d{4}))?\s*$",          # optional +4
    re.IGNORECASE,
)


class AddressStandardizeComponent(Component, Model, Resolvable):
    """Parse messy address strings into structured components (street, city, state, postcode, +4).

    Free/open-source drop-in for Alteryx's CASS tool. Supports libpostal,
    Geoapify, Nominatim, or a built-in US regex fallback.
    """

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key (DataFrame)")
    address_column: str = Field(
        description="Column containing the raw address string (one address per row).",
    )
    provider: str = Field(
        default="regex",
        description=f"Parser provider. One of: {list(_VALID_PROVIDERS)}.",
    )
    api_key_env_var: str = Field(
        default="GEOAPIFY_API_KEY",
        description="Env var for the provider's API key (geoapify). Ignored for libpostal/regex.",
    )
    user_agent: str = Field(
        default="dagster-address-standardize",
        description="User-Agent header sent to Nominatim. Must be unique per app per ToS.",
    )
    output_prefix: str = Field(
        default="addr_",
        description="Prefix for emitted component columns (addr_house_number, addr_road, ...).",
    )
    keep_original: bool = Field(
        default=True,
        description="Keep the original address column in the output.",
    )
    rate_limit_seconds: float = Field(
        default=1.0,
        description="Per-row delay for API-rate-limited providers (Nominatim defaults to 1.0).",
    )
    timeout_seconds: int = Field(
        default=10,
        description="HTTP timeout per request for API providers.",
    )

    group_name: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    asset_tags: Optional[Dict[str, str]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        _self = self
        if self.provider not in _VALID_PROVIDERS:
            raise ValueError(
                f"address_standardize: provider must be one of {list(_VALID_PROVIDERS)}; "
                f"got {self.provider!r}."
            )

        tags = dict(self.asset_tags or {})
        for k in (self.kinds or ["python", "geo"]):
            tags[f"dagster/kind/{k}"] = ""

        @asset(
            name=self.asset_name,
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(self.upstream_asset_key))},
            group_name=self.group_name,
            description=self.description or f"Standardize addresses in {self.address_column!r} via {self.provider!r}.",
            tags=tags,
            owners=self.owners or [],
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def _asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
            df = upstream.copy()
            col = _self.address_column
            prefix = _self.output_prefix
            if col not in df.columns:
                raise KeyError(
                    f"address_standardize: address_column {col!r} not in upstream. "
                    f"Available: {list(df.columns)}"
                )

            # Initialize all output columns to None so the schema is stable
            # even when individual rows fail.
            for fld in _FIELDS:
                df[f"{prefix}{fld}"] = None

            parser = _self._build_parser(context)

            n_ok = 0
            n_fail = 0
            for i, raw in enumerate(df[col]):
                if raw is None or (isinstance(raw, float) and pd.isna(raw)) or str(raw).strip() == "":
                    n_fail += 1
                    continue
                try:
                    parsed = parser(str(raw))
                except Exception as e:
                    context.log.warning(f"address_standardize row {i}: {type(e).__name__}: {e}")
                    n_fail += 1
                    continue
                if not parsed:
                    n_fail += 1
                    continue
                for fld in _FIELDS:
                    if fld in parsed and parsed[fld] is not None:
                        df.at[i, f"{prefix}{fld}"] = parsed[fld]
                n_ok += 1

            if not _self.keep_original:
                df = df.drop(columns=[col])

            context.log.info(
                f"address_standardize ({_self.provider}): parsed {n_ok}/{len(df)} rows ({n_fail} failed)."
            )
            context.add_output_metadata({
                "dagster/row_count": MetadataValue.int(len(df)),
                "provider": MetadataValue.text(_self.provider),
                "address_column": MetadataValue.text(col),
                "parsed_ok": MetadataValue.int(n_ok),
                "parsed_failed": MetadataValue.int(n_fail),
            })
            return df

        return Definitions(assets=[_asset])

    # ---------------------------------------------------------------- parsers

    def _build_parser(self, context: AssetExecutionContext):
        """Return a `parser(raw_address) -> dict` callable for the configured provider."""
        provider = self.provider

        if provider == "libpostal":
            try:
                from postal.parser import parse_address as _libpostal_parse
            except ImportError as e:
                raise ImportError(
                    "libpostal provider requires the libpostal C library + "
                    "Python bindings: `brew install libpostal` (or apt-get "
                    "install libpostal-dev) + `pip install pypostal`."
                ) from e

            # libpostal returns [("123", "house_number"), ("main st", "road"), ...]
            # — map its label names to ours (mostly 1:1, plus postcode_plus4).
            _LP_MAP = {
                "house_number": "house_number",
                "road": "road",
                "unit": "unit",
                "po_box": "unit",
                "level": "unit",
                "city": "city",
                "city_district": "city",
                "state": "state",
                "state_district": "state",
                "postcode": "postcode",
                "country": "country",
                "country_region": "country",
            }

            def _parse(raw: str) -> Dict[str, Any]:
                out: Dict[str, Any] = {}
                for value, label in _libpostal_parse(raw):
                    key = _LP_MAP.get(label)
                    if key and key not in out:
                        out[key] = value
                # Split ZIP+4 if libpostal returned ZIP-XXXX
                pc = out.get("postcode")
                if pc and "-" in pc:
                    z, p4 = pc.split("-", 1)
                    out["postcode"] = z.strip()
                    out["postcode_plus4"] = p4.strip()
                return out

            return _parse

        if provider == "geoapify":
            api_key = os.environ.get(self.api_key_env_var)
            if not api_key:
                raise OSError(
                    f"geoapify provider needs env var {self.api_key_env_var!r}. "
                    "Free tier ~3k requests/day at https://www.geoapify.com/."
                )
            import time
            import requests

            def _parse(raw: str) -> Dict[str, Any]:
                resp = requests.get(
                    "https://api.geoapify.com/v1/geocode/search",
                    params={"text": raw, "format": "json", "apiKey": api_key},
                    timeout=self.timeout_seconds,
                )
                if resp.status_code == 429:
                    time.sleep(2.0)
                    resp = requests.get(
                        "https://api.geoapify.com/v1/geocode/search",
                        params={"text": raw, "format": "json", "apiKey": api_key},
                        timeout=self.timeout_seconds,
                    )
                resp.raise_for_status()
                results = (resp.json() or {}).get("results") or []
                if not results:
                    return {}
                r = results[0]
                pc = r.get("postcode") or ""
                p4 = ""
                if pc and "-" in pc:
                    pc, p4 = pc.split("-", 1)
                return {
                    "house_number": r.get("housenumber"),
                    "road": r.get("street"),
                    "unit": r.get("unit"),
                    "city": r.get("city") or r.get("town") or r.get("village"),
                    "state": r.get("state_code") or r.get("state"),
                    "postcode": pc,
                    "postcode_plus4": p4 or None,
                    "country": r.get("country_code") or r.get("country"),
                }

            return _parse

        if provider == "nominatim":
            ua = os.environ.get("NOMINATIM_USER_AGENT") or self.user_agent
            import time
            import requests

            def _parse(raw: str) -> Dict[str, Any]:
                resp = requests.get(
                    "https://nominatim.openstreetmap.org/search",
                    params={"q": raw, "format": "json", "addressdetails": 1, "limit": 1},
                    headers={"User-Agent": ua},
                    timeout=self.timeout_seconds,
                )
                resp.raise_for_status()
                results = resp.json() or []
                # Respect Nominatim's 1 req/sec rate limit.
                time.sleep(max(self.rate_limit_seconds, 0.0))
                if not results:
                    return {}
                addr = results[0].get("address") or {}
                pc = addr.get("postcode") or ""
                p4 = ""
                if pc and "-" in pc:
                    pc, p4 = pc.split("-", 1)
                return {
                    "house_number": addr.get("house_number"),
                    "road": addr.get("road"),
                    "city": addr.get("city") or addr.get("town") or addr.get("village") or addr.get("hamlet"),
                    "state": addr.get("state"),
                    "postcode": pc,
                    "postcode_plus4": p4 or None,
                    "country": addr.get("country_code") or addr.get("country"),
                }

            return _parse

        # `regex` fallback — pure Python, no external dep, no network.
        def _parse(raw: str) -> Dict[str, Any]:
            m = _REGEX_PATTERN.match(raw)
            if not m:
                return {}
            d = {k: (v.strip() if isinstance(v, str) else v) for k, v in m.groupdict().items()}
            return d

        return _parse

    @classmethod
    def get_description(cls) -> str:
        return cls.__doc__ or ""
