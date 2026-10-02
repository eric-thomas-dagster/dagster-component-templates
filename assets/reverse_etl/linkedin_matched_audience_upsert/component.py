"""DataFrame -> LinkedIn Matched Audiences (DMP Segment) upsert.

Mirrors an upstream DataFrame into an existing LinkedIn Matched Audiences
DMP Segment via the native `POST /rest/dmpSegments/{id}/users` endpoint,
batched via `X-RestLi-Method: BATCH_CREATE` (up to 5,000 elements per
request — confirmed against LinkedIn's own Matched Audiences API docs).

Unlike Google Ads / Meta / TikTok, LinkedIn's DMP Segment Users API only
documents `SHA256_EMAIL` / `SHA512_EMAIL` / `GOOGLE_AID` as identifier
types — there is no phone identifier type. This component therefore only
supports `email` in `fields_map`.

Identifiers MUST be hashed before leaving your infrastructure — LinkedIn
never sees plaintext PII. Normalization follows LinkedIn's documented
email-hashing guideline exactly:
  1. Convert to lowercase.
  2. Remove ALL whitespace (not just leading/trailing) from the address.
  3. SHA-256, hex-encoded.

LinkedIn's REST API requires a `Linkedin-Version` header (format YYYYMM)
that LinkedIn periodically sunsets (old versions stop working ~12 months
after release) — exposed here as `linkedin_api_version` rather than
hardcoded, so a version bump doesn't require a code change.

Pairs with:
  - ``linkedin_ads_resource`` — OAuth2 bearer token (required)
"""
import hashlib
import re
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field

_SUPPORTED_IDENTIFIER_TYPES = {"email"}
_ELEMENTS_PER_REQUEST = 5000


def _normalize_email(email: str) -> str:
    """LinkedIn's documented guideline: lowercase, then remove ALL
    whitespace (not just leading/trailing) -- distinct from Google Ads'/
    Meta's/TikTok's simple `.strip()` convention."""
    return re.sub(r"\s+", "", email.lower())


def _sha256_hex(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _hash_email(email: str) -> str:
    return _sha256_hex(_normalize_email(email))


def _row_to_element(row: Dict[str, Any], email_col: str, action: str) -> Optional[dict]:
    """Pure, SDK-independent row -> DMP Segment Users batch element, or
    None if the row has no valid email."""
    raw = row.get(email_col)
    if raw is None:
        return None
    try:
        import math
        if isinstance(raw, float) and math.isnan(raw):
            return None
    except Exception:  # noqa: BLE001
        pass
    raw_str = str(raw).strip()
    if not raw_str:
        return None
    return {
        "action": action,
        "userIds": [{"idType": "SHA256_EMAIL", "idValue": _hash_email(raw_str)}],
    }


def _call_linkedin_api(resource, segment_id: str, api_version: str, elements: List[dict]) -> dict:
    """Isolates the one external, paid-API boundary so it can be
    monkeypatched wholesale in tests without `requests`/network access."""
    import requests

    url = f"https://api.linkedin.com/rest/dmpSegments/{segment_id}/users"
    headers = {
        **resource.get_headers(),
        "Linkedin-Version": api_version,
        "X-Restli-Protocol-Version": "2.0.0",
        "X-RestLi-Method": "BATCH_CREATE",
    }
    response = requests.post(url, headers=headers, json={"elements": elements}, timeout=60)
    response.raise_for_status()
    return response.json() if response.content else {}


class LinkedInMatchedAudienceUpsertComponent(dg.Component, dg.Model, dg.Resolvable):
    """Upsert (add or remove) rows from an upstream DataFrame into a
    LinkedIn Matched Audiences DMP Segment.

    Example:
        ```yaml
        type: dagster_component_templates.LinkedInMatchedAudienceUpsertComponent
        attributes:
          asset_name: linkedin_high_ltv_customers_segment
          upstream_asset_key: dbt_marts_high_ltv_customers
          resource_key: linkedin_ads_resource
          segment_id: "10804"
          fields_map:
            email: email
          operation: add
        ```

    `fields_map` maps an upstream column to the identifier type `email` —
    the only type this component supports today (LinkedIn's DMP Segment
    Users API documents no phone identifier type, unlike Google Ads/Meta/
    TikTok).
    """

    asset_name: str = Field(description="Output Dagster asset name.")

    upstream_asset_key: Optional[str] = Field(
        default=None,
        description=(
            "Upstream Dagster asset providing the DataFrame. Mutually exclusive "
            "with `source:`."
        ),
    )
    source: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Inline source config. Mutually exclusive with `upstream_asset_key`. "
            "Shapes: {kind: sql, resource_key/database_url_env_var, query}, "
            "{kind: csv, path, read_csv_kwargs}, {kind: inline, rows}."
        ),
    )

    resource_key: str = Field(
        default="linkedin_ads_resource",
        description="Resource key registered by LinkedInAdsResourceComponent.",
    )

    segment_id: str = Field(
        description=(
            "Target DMP Segment ID. Must already exist — create it once via "
            "LinkedIn Campaign Manager (Account Assets -> Matched Audiences "
            "-> Create Audience -> Website/Contact list) or the Marketing API; "
            "this component only uploads members, it does not create segments. "
            "Wait a few seconds after creating a segment before uploading to it."
        ),
    )
    fields_map: Dict[str, str] = Field(
        description=(
            "Upstream column -> identifier type. Only 'email' is supported — "
            "LinkedIn's DMP Segment Users API documents no phone identifier "
            "type. Values are hashed (SHA-256) before upload — never sent to "
            "LinkedIn in plaintext."
        ),
    )
    operation: str = Field(
        default="add",
        description=(
            "'add' (default) adds matching users to the segment (LinkedIn "
            "action=ADD) — the normal activation case. 'remove' removes them "
            "(action=REMOVE) — useful for suppression."
        ),
    )
    linkedin_api_version: str = Field(
        default="202501",
        description=(
            "LinkedIn REST API version header (format YYYYMM). LinkedIn "
            "sunsets old versions roughly 12 months after release — bump "
            "this periodically rather than relying on a hardcoded default."
        ),
    )
    batch_size: int = Field(
        default=50000,
        description="Max upstream rows per run (safety cap).",
    )

    group_name: Optional[str] = Field(default="linkedin_ads", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(
        default=None, description="Asset kinds (auto-includes 'linkedin_ads')."
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("linkedin_ads")

        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError(
                "LinkedInMatchedAudienceUpsertComponent: supply exactly one of "
                "`upstream_asset_key` OR `source:` (got both or neither)."
            )

        if self.operation not in ("add", "remove"):
            raise ValueError(
                f"LinkedInMatchedAudienceUpsertComponent: operation must be "
                f"'add' or 'remove', got {self.operation!r}."
            )

        bad_types = set(self.fields_map.values()) - _SUPPORTED_IDENTIFIER_TYPES
        if bad_types:
            raise ValueError(
                f"LinkedInMatchedAudienceUpsertComponent: fields_map has "
                f"unsupported identifier type(s) {sorted(bad_types)}. "
                f"Supported: {sorted(_SUPPORTED_IDENTIFIER_TYPES)} (LinkedIn's "
                f"DMP Segment Users API documents no phone identifier type)."
            )

        email_col = next((c for c, t in self.fields_map.items() if t == "email"), None)
        if email_col is None:
            raise ValueError(
                "LinkedInMatchedAudienceUpsertComponent: fields_map must map "
                "exactly one column to 'email'."
            )

        use_source = self.source is not None
        extra_rks: set = set()
        if use_source and (self.source.get("kind") or "").lower() == "sql":
            _rk = self.source.get("resource_key")
            if _rk:
                extra_rks.add(_rk)

        def _resolve_source_df(exec_ctx):
            import pandas as pd
            src = _self.source or {}
            kind = (src.get("kind") or "").lower()
            if kind == "sql":
                query = src.get("query")
                if not query:
                    raise ValueError("source kind=sql requires 'query'")
                rk = src.get("resource_key")
                if rk:
                    resource = getattr(exec_ctx.resources, rk)
                    if hasattr(resource, "get_engine"):
                        return pd.read_sql(query, resource.get_engine())
                    if hasattr(resource, "get_connection"):
                        with resource.get_connection() as conn:
                            if hasattr(conn, "execute") and hasattr(conn, "df"):
                                return conn.execute(query).df()
                            return pd.read_sql(query, conn)
                    raise ValueError(f"source kind=sql: resource {rk!r} must expose .get_engine() or .get_connection()")
                env = src.get("database_url_env_var")
                if env:
                    import os
                    from sqlalchemy import create_engine
                    url = os.environ.get(env, "")
                    if not url:
                        raise ValueError(f"database_url_env_var {env!r} is unset")
                    return pd.read_sql(query, create_engine(url))
                raise ValueError("source kind=sql requires 'resource_key' OR 'database_url_env_var'")
            if kind == "csv":
                path = src.get("path")
                if not path:
                    raise ValueError("source kind=csv requires 'path'")
                return pd.read_csv(path, **(src.get("read_csv_kwargs") or {}))
            if kind == "inline":
                return pd.DataFrame(src.get("rows") or [])
            raise ValueError(f"LinkedInMatchedAudienceUpsertComponent source kind={kind!r} not supported (sql / csv / inline)")

        def _run_upsert(context, upstream):
            import pandas as pd
            if not isinstance(upstream, pd.DataFrame):
                df = pd.DataFrame([upstream]) if isinstance(upstream, dict) else pd.DataFrame(upstream)
            else:
                df = upstream

            if len(df) == 0:
                context.log.warning("Upstream DataFrame is empty — nothing to upload.")
                return dg.MaterializeResult(metadata={"rows_submitted": dg.MetadataValue.int(0)})

            if len(df) > _self.batch_size:
                context.log.warning(
                    f"Upstream has {len(df)} rows; capped at batch_size={_self.batch_size}."
                )
                df = df.head(_self.batch_size)

            if email_col not in df.columns:
                raise dg.Failure(
                    f"Column {email_col!r} not in upstream. Available: {list(df.columns)}"
                )

            action = "ADD" if _self.operation == "add" else "REMOVE"
            elements: List[dict] = []
            skipped_no_identifier = 0
            for _, row in df.iterrows():
                element = _row_to_element(row.to_dict(), email_col, action)
                if element is None:
                    skipped_no_identifier += 1
                    continue
                elements.append(element)

            if not elements:
                context.log.warning(
                    "No rows had a valid email — nothing to upload."
                )
                return dg.MaterializeResult(
                    metadata={
                        "rows_submitted": dg.MetadataValue.int(0),
                        "rows_skipped_no_identifier": dg.MetadataValue.int(skipped_no_identifier),
                    }
                )

            resource = getattr(context.resources, _self.resource_key)
            requests_made = 0
            for chunk_start in range(0, len(elements), _ELEMENTS_PER_REQUEST):
                chunk = elements[chunk_start:chunk_start + _ELEMENTS_PER_REQUEST]
                _call_linkedin_api(resource, _self.segment_id, _self.linkedin_api_version, chunk)
                requests_made += 1

            context.log.info(
                f"LinkedIn Matched Audience {_self.operation}: segment={_self.segment_id} "
                f"rows_submitted={len(elements)} rows_skipped_no_identifier={skipped_no_identifier} "
                f"requests={requests_made}."
            )

            return dg.MaterializeResult(
                metadata={
                    "segment_id": dg.MetadataValue.text(_self.segment_id),
                    "operation": dg.MetadataValue.text(_self.operation),
                    "rows_submitted": dg.MetadataValue.int(len(elements)),
                    "rows_skipped_no_identifier": dg.MetadataValue.int(skipped_no_identifier),
                    "api_requests": dg.MetadataValue.int(requests_made),
                }
            )

        common_kwargs = dict(
            key=dg.AssetKey.from_user_string(_self.asset_name),
            group_name=_self.group_name,
            kinds=kinds,
            owners=_self.owners,
            tags=_self.tags,
            description=_self.description or (
                f"Upsert DataFrame rows into LinkedIn Matched Audiences segment "
                f"{_self.segment_id} (operation={_self.operation})."
            ),
        )

        if use_source:
            required_rks = {_self.resource_key} | extra_rks

            @dg.asset(required_resource_keys=required_rks, **common_kwargs)
            def _asset(context: dg.AssetExecutionContext):
                df = _resolve_source_df(context)
                return _run_upsert(context, df)
        else:
            @dg.asset(
                ins={"upstream": dg.AssetIn(key=dg.AssetKey.from_user_string(_self.upstream_asset_key))},
                required_resource_keys={_self.resource_key},
                **common_kwargs,
            )
            def _asset(context: dg.AssetExecutionContext, upstream):
                return _run_upsert(context, upstream)

        return dg.Definitions(assets=[_asset])
