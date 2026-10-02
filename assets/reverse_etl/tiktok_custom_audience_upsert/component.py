"""DataFrame -> TikTok Custom Audience upsert.

Mirrors an upstream DataFrame into an existing TikTok Custom Audience via
TikTok's two-stage file-based flow:

  1. `POST /dmp/custom_audience/file/upload/` — upload one file PER
     identifier type present in `fields_map` (TikTok's file format is one
     ID type per file, with a header row naming it, e.g. "Email_SHA256" /
     "Phone_SHA256" — unlike Google Ads/Meta's single multi-identifier
     payload). Returns a `file_path` reference per file.
  2. `POST /dmp/custom_audience/update/` — append or remove members on the
     existing `custom_audience_id`, referencing all uploaded file_paths in
     one call, via `action: APPEND | REMOVE`.

This component only uploads members — it does not create audiences.

Identifiers MUST be hashed before leaving your infrastructure — TikTok
never sees plaintext PII. Supported identifier types: `email`, `phone`.
Normalization follows TikTok's documented rules (confirmed against
ads.tiktok.com/help — note phone uses E.164 WITH a leading '+', matching
Google Ads' convention, NOT Meta's digits-only one):
  - email: trim, lowercase, then SHA-256
  - phone: E.164 (leading '+' and country code), then SHA-256

TikTok requires at least 1,000 entries per uploaded file for it to be
usable for matching — this component logs a warning (not an error, since
enforcement specifics may vary) when an identifier-type file falls short.

Pairs with:
  - ``tiktok_ads_resource`` — access token + advertiser ID (required)
"""
import hashlib
import re
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field

_SUPPORTED_IDENTIFIER_TYPES = {"email", "phone"}
_CALCULATE_TYPE_BY_IDENTIFIER = {"email": "EMAIL_SHA256", "phone": "PHONE_SHA256"}
_FILE_HEADER_BY_IDENTIFIER = {"email": "Email_SHA256", "phone": "Phone_SHA256"}
_MIN_RECOMMENDED_FILE_ENTRIES = 1000


def _normalize_email(email: str) -> str:
    return email.strip().lower()


def _normalize_phone(phone: str) -> str:
    """TikTok requires E.164 (leading '+' + country code) before hashing --
    confirmed against TikTok's own customer-file help article. This is the
    same convention as Google Ads, NOT Meta's digits-only one."""
    stripped = re.sub(r"[^\d+]", "", phone.strip())
    digits_only = stripped.lstrip("+")
    return "+" + digits_only


def _sha256_hex(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _hash_email(email: str) -> str:
    return _sha256_hex(_normalize_email(email))


def _hash_phone(phone: str) -> str:
    return _sha256_hex(_normalize_phone(phone))


def _collect_hashed_values_by_type(
    df_records: List[Dict[str, Any]], fields_map: Dict[str, str]
) -> Dict[str, List[str]]:
    """Pure, SDK-independent extraction: rows -> {identifier_type: [hashed_value, ...]}.

    Unlike Google Ads/Meta (one UserData/row can carry multiple identifier
    types), TikTok's file format is one identifier type per file, so values
    are grouped by type across all rows rather than kept row-aligned."""
    values_by_type: Dict[str, List[str]] = {t: [] for t in set(fields_map.values())}
    for row in df_records:
        for col, id_type in fields_map.items():
            raw = row.get(col)
            if raw is None:
                continue
            try:
                import math
                if isinstance(raw, float) and math.isnan(raw):
                    continue
            except Exception:  # noqa: BLE001
                pass
            raw_str = str(raw).strip()
            if not raw_str:
                continue
            hashed = _hash_email(raw_str) if id_type == "email" else _hash_phone(raw_str)
            values_by_type[id_type].append(hashed)
    return values_by_type


def _build_identifier_file_content(id_type: str, hashed_values: List[str]) -> str:
    header = _FILE_HEADER_BY_IDENTIFIER[id_type]
    return "\n".join([header] + hashed_values) + "\n"


def _call_tiktok_upload(resource, calculate_type: str, file_content: str, file_name: str) -> str:
    """Isolates the file-upload external call so it can be monkeypatched
    wholesale in tests without `requests`/network access. Returns the
    uploaded file's `file_path` reference."""
    import requests

    url = f"{resource.api_base_url}/dmp/custom_audience/file/upload/"
    response = requests.post(
        url,
        headers=resource.get_headers(),
        data={"advertiser_id": resource.advertiser_id, "calculate_type": calculate_type},
        files={"file": (file_name, file_content.encode("utf-8"), "text/csv")},
        timeout=60,
    )
    response.raise_for_status()
    payload = response.json()
    data = payload.get("data") or {}
    file_path = data.get("file_path") or (data.get("file_paths") or [None])[0]
    if not file_path:
        raise dg.Failure(f"TikTok file upload did not return a file_path. Response: {payload}")
    return file_path


def _call_tiktok_update(resource, custom_audience_id: str, file_paths: List[str], action: str) -> dict:
    """Isolates the audience-update external call so it can be monkeypatched
    wholesale in tests without `requests`/network access."""
    import requests

    url = f"{resource.api_base_url}/dmp/custom_audience/update/"
    response = requests.post(
        url,
        headers=resource.get_headers(),
        json={
            "advertiser_id": resource.advertiser_id,
            "custom_audience_id": custom_audience_id,
            "file_paths": file_paths,
            "action": action,
        },
        timeout=60,
    )
    response.raise_for_status()
    return response.json()


class TikTokCustomAudienceUpsertComponent(dg.Component, dg.Model, dg.Resolvable):
    """Upsert (add or remove) rows from an upstream DataFrame into a TikTok
    Custom Audience.

    Example:
        ```yaml
        type: dagster_component_templates.TikTokCustomAudienceUpsertComponent
        attributes:
          asset_name: tiktok_high_ltv_customers_audience
          upstream_asset_key: dbt_marts_high_ltv_customers
          resource_key: tiktok_ads_resource
          custom_audience_id: "1234567890123456789"
          fields_map:
            email: email
            phone_e164: phone
          operation: add
        ```

    `fields_map` maps upstream column -> identifier type (`email` or
    `phone`). Unlike Google Ads/Meta, TikTok's file format is one
    identifier type per file — this component uploads one file per
    distinct type present in `fields_map`, then references all of them in
    a single audience-update call.
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
        default="tiktok_ads_resource",
        description="Resource key registered by TikTokAdsResourceComponent.",
    )

    custom_audience_id: str = Field(
        description=(
            "Target TikTok Custom Audience ID. Must already exist — create it "
            "once via TikTok Ads Manager (Assets -> Audiences -> Create Audience "
            "-> Customer File) or the Marketing API; this component only "
            "uploads members, it does not create audiences."
        ),
    )
    fields_map: Dict[str, str] = Field(
        description=(
            "Upstream column -> identifier type. Supported types: 'email', "
            "'phone'. Values are hashed (SHA-256) before upload — never sent "
            "to TikTok in plaintext."
        ),
    )
    operation: str = Field(
        default="add",
        description=(
            "'add' (default) appends matching users to the audience (TikTok "
            "action=APPEND) — the normal activation case. 'remove' removes "
            "them (action=REMOVE) — useful for suppression."
        ),
    )
    batch_size: int = Field(
        default=50000,
        description="Max upstream rows per run (safety cap).",
    )

    group_name: Optional[str] = Field(default="tiktok_ads", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(
        default=None, description="Asset kinds (auto-includes 'tiktok_ads')."
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("tiktok_ads")

        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError(
                "TikTokCustomAudienceUpsertComponent: supply exactly one of "
                "`upstream_asset_key` OR `source:` (got both or neither)."
            )

        if self.operation not in ("add", "remove"):
            raise ValueError(
                f"TikTokCustomAudienceUpsertComponent: operation must be "
                f"'add' or 'remove', got {self.operation!r}."
            )

        bad_types = set(self.fields_map.values()) - _SUPPORTED_IDENTIFIER_TYPES
        if bad_types:
            raise ValueError(
                f"TikTokCustomAudienceUpsertComponent: fields_map has "
                f"unsupported identifier type(s) {sorted(bad_types)}. "
                f"Supported: {sorted(_SUPPORTED_IDENTIFIER_TYPES)}."
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
            raise ValueError(f"TikTokCustomAudienceUpsertComponent source kind={kind!r} not supported (sql / csv / inline)")

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

            required_cols = set(_self.fields_map.keys())
            missing_cols = [c for c in required_cols if c not in df.columns]
            if missing_cols:
                raise dg.Failure(
                    f"Columns not in upstream: {missing_cols}. Available: {list(df.columns)}"
                )

            values_by_type = _collect_hashed_values_by_type(df.to_dict("records"), _self.fields_map)

            resource = getattr(context.resources, _self.resource_key)
            action = "APPEND" if _self.operation == "add" else "REMOVE"

            file_paths: List[str] = []
            rows_submitted_by_type: Dict[str, int] = {}
            for id_type, hashed_values in values_by_type.items():
                if not hashed_values:
                    continue
                if len(hashed_values) < _MIN_RECOMMENDED_FILE_ENTRIES:
                    context.log.warning(
                        f"TikTok recommends at least {_MIN_RECOMMENDED_FILE_ENTRIES} entries "
                        f"per file for matching; {id_type} file has only {len(hashed_values)}. "
                        f"Uploading anyway — TikTok may ignore or reject a small file."
                    )
                file_content = _build_identifier_file_content(id_type, hashed_values)
                file_path = _call_tiktok_upload(
                    resource,
                    _CALCULATE_TYPE_BY_IDENTIFIER[id_type],
                    file_content,
                    file_name=f"{id_type}_{_self.custom_audience_id}.csv",
                )
                file_paths.append(file_path)
                rows_submitted_by_type[id_type] = len(hashed_values)

            total_submitted = sum(rows_submitted_by_type.values())
            if not file_paths:
                context.log.warning(
                    "No rows had a valid identifier (email/phone) — nothing to upload."
                )
                return dg.MaterializeResult(
                    metadata={
                        "rows_submitted": dg.MetadataValue.int(0),
                    }
                )

            update_result = _call_tiktok_update(resource, _self.custom_audience_id, file_paths, action)

            context.log.info(
                f"TikTok Custom Audience {_self.operation}: audience={_self.custom_audience_id} "
                f"files_uploaded={len(file_paths)} rows_submitted_by_type={rows_submitted_by_type} "
                f"action={action}. Matching can take 24-48h on TikTok's side."
            )

            return dg.MaterializeResult(
                metadata={
                    "custom_audience_id": dg.MetadataValue.text(_self.custom_audience_id),
                    "operation": dg.MetadataValue.text(_self.operation),
                    "action": dg.MetadataValue.text(action),
                    "files_uploaded": dg.MetadataValue.int(len(file_paths)),
                    "rows_submitted": dg.MetadataValue.int(total_submitted),
                    "rows_submitted_by_type": dg.MetadataValue.json(rows_submitted_by_type),
                    "update_response": dg.MetadataValue.json(update_result),
                }
            )

        common_kwargs = dict(
            key=dg.AssetKey.from_user_string(_self.asset_name),
            group_name=_self.group_name,
            kinds=kinds,
            owners=_self.owners,
            tags=_self.tags,
            description=_self.description or (
                f"Upsert DataFrame rows into TikTok Custom Audience "
                f"{_self.custom_audience_id} (operation={_self.operation})."
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
