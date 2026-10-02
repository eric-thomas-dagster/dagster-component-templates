"""DataFrame -> Pinterest Customer List (Audience) upsert.

Mirrors an upstream DataFrame into an existing Pinterest Customer List --
the underlying object a `CUSTOMER_LIST`-type Pinterest Audience points to
-- via the update-customer-list endpoint:

  `PATCH /ad_accounts/:ad_account_id/customer_lists/:customer_list_id`

confirmed against Pinterest's own OpenAPI description
(github.com/pinterest/api-description, v5/openapi.yaml): the `operation_type`
field accepts exactly `ADD` / `REMOVE` (the `UserListOperationType` schema),
and the request body carries either a comma-separated `records` string
(single identifier type at a time, matching the older customer-list-via-CSV
flow) or a `records_v2` array of per-row objects supporting multiple
identifier fields per row (`email`, `hashed_phone_number`, `maid`, ...) --
this component uses `records_v2` since `fields_map` can mix identifier
types the same way Google Ads/Meta/X do (one row, multiple identifiers).

This component only uploads members into an existing Customer List -- it
does not create the list or convert it into an Audience; both are one-time
setup steps.

Identifiers MUST be hashed before leaving your infrastructure. Although
Pinterest's API technically accepts cleartext email addresses (its schema
says "Emails must be lowercase and can be plain text or hashed"), this
component always SHA-256 hashes before sending, consistent with every other
ad-platform audience-activation component in this repo. Supported
identifier types: `email`, `phone`.

Normalization rules (verified independently per identifier type -- Pinterest
diverges from Google Ads/TikTok/X here, it does NOT use their E.164-with-'+'
convention):
  - email: trim whitespace, lowercase, then SHA-256 -- Pinterest's own
    customer-list schema explicitly requires lowercase emails.
  - phone: digits only (country code + number), with any '+', spaces,
    dashes, parentheses, letters, and leading zeros removed, then SHA-256 --
    confirmed against Pinterest's own conversions/enhanced-match
    documentation, which is explicit that phone numbers are "only digits
    with country code, area code, and number... with any symbols, letters,
    spaces and leading zeros removed." This matches Meta's digits-only
    convention, NOT Google Ads/TikTok/X's E.164-with-'+' one.

Processing is asynchronous on Pinterest's side: updating a customer list
returns its new `status` (`PROCESSING` / `READY` / `TOO_SMALL` / `UPLOADING`)
immediately, but matching records against Pinterest's user base to produce
an audience usable for targeting can take up to 48-72 hours. A list with
fewer than 100 matched Pinterest accounts reports `TOO_SMALL` and cannot be
used for targeting -- this component surfaces `status` in its metadata so
that condition is visible without a separate API call.

Pairs with:
  - ``pinterest_ads_resource`` -- OAuth2 bearer token + ad account ID (required)
"""
import hashlib
import re
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field

_SUPPORTED_IDENTIFIER_TYPES = {"email", "phone"}
_RECORD_FIELD_BY_IDENTIFIER = {"email": "email", "phone": "hashed_phone_number"}
_OPERATION_TYPE_BY_OPERATION = {"add": "ADD", "remove": "REMOVE"}


def _normalize_email(email: str) -> str:
    """Pinterest's customer-list schema requires lowercase emails; trimming
    whitespace is standard precaution and not contradicted by Pinterest's
    docs."""
    return email.strip().lower()


def _normalize_phone(phone: str) -> str:
    """Pinterest wants digits only (country code + area code + number) --
    NOT E.164 with a leading '+'. Confirmed against Pinterest's own
    conversions/enhanced-match documentation: "only digits with country
    code, area code, and number... with any symbols, letters, spaces and
    leading zeros removed." This matches Meta's convention, not Google
    Ads/TikTok/X's."""
    digits = re.sub(r"\D", "", phone.strip())
    return digits.lstrip("0")


def _sha256_hex(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _hash_email(email: str) -> str:
    return _sha256_hex(_normalize_email(email))


def _hash_phone(phone: str) -> str:
    return _sha256_hex(_normalize_phone(phone))


def _build_records_v2(
    df_records: List[Dict[str, Any]], fields_map: Dict[str, str]
) -> tuple:
    """Pure, SDK-independent extraction: rows -> (records_v2, rows_skipped_no_identifier).

    Each output row is a dict like {"email": "<hash>", "hashed_phone_number": "<hash>"}
    -- Pinterest's `records_v2` format is row-aligned (one object per
    customer, optionally carrying multiple identifier fields), the same
    shape Google Ads/Meta/X use, unlike TikTok's one-file-per-type upload."""
    records_v2: List[Dict[str, str]] = []
    rows_skipped_no_identifier = 0
    for row in df_records:
        record: Dict[str, str] = {}
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
            field_name = _RECORD_FIELD_BY_IDENTIFIER[id_type]
            record[field_name] = hashed
        if record:
            records_v2.append(record)
        else:
            rows_skipped_no_identifier += 1
    return records_v2, rows_skipped_no_identifier


def _call_pinterest_update(resource, customer_list_id: str, operation_type: str, records_v2: List[Dict[str, str]]) -> dict:
    """Isolates the external customer-list-update call so it can be
    monkeypatched wholesale in tests without `requests`/network access."""
    import requests

    url = f"{resource.api_base_url}/ad_accounts/{resource.ad_account_id}/customer_lists/{customer_list_id}"
    response = requests.patch(
        url,
        headers=resource.get_headers(),
        json={"operation_type": operation_type, "records_v2": records_v2},
        timeout=60,
    )
    response.raise_for_status()
    return response.json()


class PinterestAudienceUpsertComponent(dg.Component, dg.Model, dg.Resolvable):
    """Upsert (add or remove) rows from an upstream DataFrame into an
    existing Pinterest Customer List (the object a `CUSTOMER_LIST`-type
    Pinterest Audience points to).

    Example:
        ```yaml
        type: dagster_component_templates.PinterestAudienceUpsertComponent
        attributes:
          asset_name: pinterest_high_ltv_customers_audience
          upstream_asset_key: dbt_marts_high_ltv_customers
          resource_key: pinterest_ads_resource
          customer_list_id: "643"
          fields_map:
            email: email
            phone_digits: phone
          operation: add
        ```

    `fields_map` maps upstream column -> identifier type (`email` or
    `phone`). A single row can supply both -- Pinterest's `records_v2`
    format is row-aligned the same way Google Ads/Meta/X's are, unlike
    TikTok's one-file-per-identifier-type upload.
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
        default="pinterest_ads_resource",
        description="Resource key registered by PinterestAdsResourceComponent.",
    )

    customer_list_id: str = Field(
        description=(
            "Target Pinterest Customer List ID. Must already exist -- create it "
            "once via Pinterest Ads Manager (Ads -> Audiences -> Create audience "
            "-> Customer list) or the Ads API's `customer_lists` create endpoint, "
            "then convert it into a `CUSTOMER_LIST`-type Audience; this component "
            "only uploads members, it does not create the list or the audience."
        ),
    )
    fields_map: Dict[str, str] = Field(
        description=(
            "Upstream column -> identifier type. Supported types: 'email', "
            "'phone'. Values are hashed (SHA-256) before upload -- never sent "
            "to Pinterest in plaintext."
        ),
    )
    operation: str = Field(
        default="add",
        description=(
            "'add' (default) adds matching records to the customer list (Pinterest "
            "operation_type=ADD) -- the normal activation case. 'remove' removes "
            "them (operation_type=REMOVE) -- useful for suppression."
        ),
    )
    batch_size: int = Field(
        default=50000,
        description="Max upstream rows per run (safety cap).",
    )

    group_name: Optional[str] = Field(default="pinterest_ads", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(
        default=None, description="Asset kinds (auto-includes 'pinterest_ads')."
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("pinterest_ads")

        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError(
                "PinterestAudienceUpsertComponent: supply exactly one of "
                "`upstream_asset_key` OR `source:` (got both or neither)."
            )

        if self.operation not in ("add", "remove"):
            raise ValueError(
                f"PinterestAudienceUpsertComponent: operation must be "
                f"'add' or 'remove', got {self.operation!r}."
            )

        bad_types = set(self.fields_map.values()) - _SUPPORTED_IDENTIFIER_TYPES
        if bad_types:
            raise ValueError(
                f"PinterestAudienceUpsertComponent: fields_map has "
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
            raise ValueError(f"PinterestAudienceUpsertComponent source kind={kind!r} not supported (sql / csv / inline)")

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

            records_v2, rows_skipped_no_identifier = _build_records_v2(df.to_dict("records"), _self.fields_map)

            if not records_v2:
                context.log.warning(
                    "No rows had a valid identifier (email/phone) — nothing to upload."
                )
                return dg.MaterializeResult(
                    metadata={
                        "rows_submitted": dg.MetadataValue.int(0),
                        "rows_skipped_no_identifier": dg.MetadataValue.int(rows_skipped_no_identifier),
                    }
                )

            resource = getattr(context.resources, _self.resource_key)
            operation_type = _OPERATION_TYPE_BY_OPERATION[_self.operation]

            response = _call_pinterest_update(resource, _self.customer_list_id, operation_type, records_v2)

            status = response.get("status")
            num_uploaded = response.get("num_uploaded_user_records")
            num_removed = response.get("num_removed_user_records")
            num_batches = response.get("num_batches")

            context.log.info(
                f"Pinterest Customer List {_self.operation}: customer_list_id={_self.customer_list_id} "
                f"rows_submitted={len(records_v2)} rows_skipped_no_identifier={rows_skipped_no_identifier} "
                f"operation_type={operation_type} status={status}. Matching against Pinterest's user "
                f"base to produce a targetable audience can take up to 48-72h on Pinterest's side; a "
                f"status of TOO_SMALL means fewer than 100 Pinterest accounts matched so far."
            )

            return dg.MaterializeResult(
                metadata={
                    "customer_list_id": dg.MetadataValue.text(_self.customer_list_id),
                    "operation": dg.MetadataValue.text(_self.operation),
                    "operation_type": dg.MetadataValue.text(operation_type),
                    "rows_submitted": dg.MetadataValue.int(len(records_v2)),
                    "rows_skipped_no_identifier": dg.MetadataValue.int(rows_skipped_no_identifier),
                    "status": dg.MetadataValue.text(str(status) if status is not None else "UNKNOWN"),
                    "num_uploaded_user_records": dg.MetadataValue.int(int(num_uploaded)) if num_uploaded is not None else dg.MetadataValue.int(0),
                    "num_removed_user_records": dg.MetadataValue.int(int(num_removed)) if num_removed is not None else dg.MetadataValue.int(0),
                    "num_batches": dg.MetadataValue.int(int(num_batches)) if num_batches is not None else dg.MetadataValue.int(0),
                    "update_response": dg.MetadataValue.json(response),
                }
            )

        common_kwargs = dict(
            key=dg.AssetKey.from_user_string(_self.asset_name),
            group_name=_self.group_name,
            kinds=kinds,
            owners=_self.owners,
            tags=_self.tags,
            description=_self.description or (
                f"Upsert DataFrame rows into Pinterest Customer List "
                f"{_self.customer_list_id} (operation={_self.operation})."
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
