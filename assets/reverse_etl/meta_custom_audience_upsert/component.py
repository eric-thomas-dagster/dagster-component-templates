"""DataFrame -> Meta (Facebook/Instagram) Custom Audience upsert.

Mirrors an upstream DataFrame into an existing Meta Custom Audience via the
native `/{custom_audience_id}/users` endpoint (wrapped by the
`facebook-business` SDK's `CustomAudience.add_users` / `.remove_users`).

Meta's API takes a `schema` (ordered list of identifier types, e.g.
`["EMAIL", "PHONE"]`) and `data` (rows of hashed values positionally
aligned with that schema — a row missing one identifier type still needs
an empty-string placeholder at that position, not an omitted column).
This component builds that schema once from the distinct identifier types
in `fields_map`, then rows missing every identifier are skipped entirely;
rows missing just one still contribute their other identifier(s).

Identifiers MUST be hashed before leaving your infrastructure — Meta never
sees plaintext PII. Supported identifier types: `email`, `phone`.
Normalization follows Meta's documented rules (distinct from Google Ads'):
  - email: trim, lowercase, then SHA-256
  - phone: strip EVERYTHING but digits (no leading '+', unlike Google Ads'
    E.164 convention), then SHA-256 — include the country code digits with
    no symbols, e.g. "14155552671" not "+1 (415) 555-2671"

Pairs with:
  - ``facebook_ads_resource`` — App/token auth + Facebook Business SDK (required)
"""
import hashlib
import re
from typing import Any, Dict, List, Optional, Tuple

import dagster as dg
from pydantic import Field

_SUPPORTED_IDENTIFIER_TYPES = {"email", "phone"}
_IDENTIFIER_TYPE_TO_SCHEMA_KEY = {"email": "EMAIL", "phone": "PHONE"}
_ROWS_PER_REQUEST = 10000


def _normalize_email(email: str) -> str:
    return email.strip().lower()


def _normalize_phone(phone: str) -> str:
    """Meta wants digits only — no leading '+', unlike Google Ads' E.164
    convention. Include the country code digits, e.g. "14155552671"."""
    return re.sub(r"\D", "", phone.strip())


def _sha256_hex(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _hash_email(email: str) -> str:
    return _sha256_hex(_normalize_email(email))


def _hash_phone(phone: str) -> str:
    return _sha256_hex(_normalize_phone(phone))


def _build_schema(fields_map: Dict[str, str]) -> List[str]:
    """Distinct identifier types present in fields_map, in first-seen order
    (dict iteration order in fields_map, which is insertion order in YAML)."""
    schema: List[str] = []
    for id_type in fields_map.values():
        key = _IDENTIFIER_TYPE_TO_SCHEMA_KEY[id_type]
        if key not in schema:
            schema.append(key)
    return schema


def _row_to_schema_values(
    row: Dict[str, Any], fields_map: Dict[str, str], schema: List[str]
) -> Optional[List[str]]:
    """Pure, SDK-independent row -> positional hashed-value list aligned
    with `schema`, or None if the row has zero valid identifiers. A missing
    identifier at a given schema position becomes "" (Meta requires every
    row to have the same column count as the schema, not a sparse subset)."""
    values_by_key: Dict[str, str] = {}
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
        key = _IDENTIFIER_TYPE_TO_SCHEMA_KEY[id_type]
        values_by_key[key] = _hash_email(raw_str) if id_type == "email" else _hash_phone(raw_str)
    if not values_by_key:
        return None
    return [values_by_key.get(k, "") for k in schema]


def _call_facebook_api(resource, custom_audience_id: str, schema: List[str], rows: List[List[str]], operation: str):
    """Isolates the one real external-API boundary (the `facebook-business`
    SDK call) so it can be monkeypatched wholesale in tests without the
    package being installed -- mirrors this repo's "mock only the paid/
    external call" test convention."""
    resource.get_api()  # ensures FacebookAdsApi.init() has run
    from facebook_business.adobjects.customaudience import CustomAudience

    audience = CustomAudience(fbid=custom_audience_id)
    if operation == "add":
        return audience.add_users(schema=schema, users=rows, is_raw=False)
    return audience.remove_users(schema=schema, users=rows)


class MetaCustomAudienceUpsertComponent(dg.Component, dg.Model, dg.Resolvable):
    """Upsert (add or remove) rows from an upstream DataFrame into a Meta
    (Facebook/Instagram) Custom Audience.

    Example:
        ```yaml
        type: dagster_component_templates.MetaCustomAudienceUpsertComponent
        attributes:
          asset_name: meta_high_ltv_customers_audience
          upstream_asset_key: dbt_marts_high_ltv_customers
          resource_key: facebook_ads_resource
          custom_audience_id: "23850000000000000"
          fields_map:
            email: email
            phone_digits: phone
          operation: add
        ```

    `fields_map` maps upstream column -> identifier type (`email` or
    `phone`). Each row can supply multiple identifiers across different
    columns to improve match rate.
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
        default="facebook_ads_resource",
        description="Resource key registered by FacebookAdsResourceComponent.",
    )

    custom_audience_id: str = Field(
        description=(
            "Target Meta Custom Audience ID. Must already exist — create it "
            "once via Meta Ads Manager (Audiences -> Create Audience -> "
            "Custom Audience -> Customer list) or the Marketing API; this "
            "component only uploads members, it does not create audiences."
        ),
    )
    fields_map: Dict[str, str] = Field(
        description=(
            "Upstream column -> identifier type. Supported types: 'email', "
            "'phone'. Values are hashed (SHA-256) before upload — never sent "
            "to Meta in plaintext."
        ),
    )
    operation: str = Field(
        default="add",
        description=(
            "'add' (default) adds matching users to the audience — the normal "
            "activation case. 'remove' removes them — useful for suppression "
            "(e.g. stop retargeting customers who already converted)."
        ),
    )
    batch_size: int = Field(
        default=50000,
        description="Max upstream rows per run (safety cap).",
    )

    group_name: Optional[str] = Field(default="meta_ads", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(
        default=None, description="Asset kinds (auto-includes 'meta_ads')."
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("meta_ads")

        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError(
                "MetaCustomAudienceUpsertComponent: supply exactly one of "
                "`upstream_asset_key` OR `source:` (got both or neither)."
            )

        if self.operation not in ("add", "remove"):
            raise ValueError(
                f"MetaCustomAudienceUpsertComponent: operation must be "
                f"'add' or 'remove', got {self.operation!r}."
            )

        bad_types = set(self.fields_map.values()) - _SUPPORTED_IDENTIFIER_TYPES
        if bad_types:
            raise ValueError(
                f"MetaCustomAudienceUpsertComponent: fields_map has "
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
            raise ValueError(f"MetaCustomAudienceUpsertComponent source kind={kind!r} not supported (sql / csv / inline)")

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

            schema = _build_schema(_self.fields_map)
            rows: List[List[str]] = []
            skipped_no_identifier = 0
            for _, row in df.iterrows():
                values = _row_to_schema_values(row.to_dict(), _self.fields_map, schema)
                if values is None:
                    skipped_no_identifier += 1
                    continue
                rows.append(values)

            if not rows:
                context.log.warning(
                    "No rows had a valid identifier (email/phone) — nothing to upload."
                )
                return dg.MaterializeResult(
                    metadata={
                        "rows_submitted": dg.MetadataValue.int(0),
                        "rows_skipped_no_identifier": dg.MetadataValue.int(skipped_no_identifier),
                    }
                )

            resource = getattr(context.resources, _self.resource_key)
            requests_made = 0
            for chunk_start in range(0, len(rows), _ROWS_PER_REQUEST):
                chunk = rows[chunk_start:chunk_start + _ROWS_PER_REQUEST]
                _call_facebook_api(resource, _self.custom_audience_id, schema, chunk, _self.operation)
                requests_made += 1

            context.log.info(
                f"Meta Custom Audience {_self.operation}: audience={_self.custom_audience_id} "
                f"rows_submitted={len(rows)} rows_skipped_no_identifier={skipped_no_identifier} "
                f"requests={requests_made} schema={schema}."
            )

            return dg.MaterializeResult(
                metadata={
                    "custom_audience_id": dg.MetadataValue.text(_self.custom_audience_id),
                    "operation": dg.MetadataValue.text(_self.operation),
                    "schema": dg.MetadataValue.json(schema),
                    "rows_submitted": dg.MetadataValue.int(len(rows)),
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
                f"Upsert DataFrame rows into Meta Custom Audience "
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
