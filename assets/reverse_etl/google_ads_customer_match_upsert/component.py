"""DataFrame -> Google Ads Customer Match audience upsert.

Mirrors an upstream DataFrame into an existing Google Ads Customer Match
user list via the `OfflineUserDataJobService` — the native audience-upload
API Google Ads offers for activation (syncing warehouse segments into ad
targeting/suppression audiences).

Flow per run:
  1. `create_offline_user_data_job` — opens a new job against the target
     user list.
  2. `add_offline_user_data_job_operations` — uploads hashed identifiers in
     chunks of up to 10,000 operations per request.
  3. `run_offline_user_data_job` — submits the job for processing.

Customer Match jobs are asynchronous on Google's side (processing can take
hours); this component reports the job resource name and submitted/skipped
counts, not per-row success/failure — check Google Ads UI (Tools &
Settings -> Audience Manager -> Customer Match list -> Job History) for
detailed processing status, same convention as this repo's Salesforce Bulk
2.0 reverse-ETL component.

Identifiers MUST be hashed before leaving your infrastructure — Google
never sees plaintext PII. Supported identifier types: `email`, `phone`.
Normalization follows Google's documented rules:
  - email: trim, lowercase, then SHA-256
  - phone: strip everything but digits and a leading '+', then SHA-256
    (must include country code, e.g. "+14155552671")

Pairs with:
  - ``google_ads_resource`` — OAuth2 connection + GoogleAdsClient (required)
"""
import hashlib
import re
from typing import Any, Dict, List, Optional, Tuple

import dagster as dg
from pydantic import Field

_SUPPORTED_IDENTIFIER_TYPES = {"email", "phone"}
_OPERATIONS_PER_REQUEST = 10000


def _normalize_email(email: str) -> str:
    return email.strip().lower()


def _normalize_phone(phone: str) -> str:
    """Strip everything but digits and a leading '+'. Google requires E.164
    format (country code included) for hashed_phone_number to match."""
    stripped = re.sub(r"[^\d+]", "", phone.strip())
    digits_only = stripped.lstrip("+")
    return "+" + digits_only


def _sha256_hex(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _hash_email(email: str) -> str:
    return _sha256_hex(_normalize_email(email))


def _hash_phone(phone: str) -> str:
    return _sha256_hex(_normalize_phone(phone))


def _extract_identifiers_for_row(
    row: Dict[str, Any], fields_map: Dict[str, str]
) -> List[Tuple[str, str]]:
    """Pure, SDK-independent extraction: upstream row -> [(id_type, hashed_value), ...].

    Skips columns with null/empty values. A row can contribute more than one
    identifier (e.g. both email and phone) — Google matches on ANY identifier
    present on a UserData, so supplying more improves match rate.
    """
    identifiers: List[Tuple[str, str]] = []
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
        if id_type == "email":
            identifiers.append(("email", _hash_email(raw_str)))
        elif id_type == "phone":
            identifiers.append(("phone", _hash_phone(raw_str)))
    return identifiers


class GoogleAdsCustomerMatchUpsertComponent(dg.Component, dg.Model, dg.Resolvable):
    """Upsert (add or remove) rows from an upstream DataFrame into a Google
    Ads Customer Match user list.

    Example:
        ```yaml
        type: dagster_component_templates.GoogleAdsCustomerMatchUpsertComponent
        attributes:
          asset_name: google_ads_churned_customers_audience
          upstream_asset_key: dbt_marts_churned_customers
          resource_key: google_ads_resource
          user_list_resource_name: "customers/1234567890/userLists/987654321"
          fields_map:
            email: email
            phone_e164: phone
          operation: add
        ```

    `fields_map` maps upstream column -> identifier type (`email` or
    `phone`). Each row can supply multiple identifiers across different
    columns to improve match rate. Unmapped/empty values are skipped per
    identifier, not per row — a row with a valid email but a blank phone
    still contributes its email.
    """

    asset_name: str = Field(description="Output Dagster asset name.")

    # Two source shapes — supply exactly one.
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
        default="google_ads_resource",
        description="Resource key registered by GoogleAdsResourceComponent.",
    )

    user_list_resource_name: str = Field(
        description=(
            "Target Customer Match user list's full resource name, e.g. "
            "'customers/1234567890/userLists/987654321'. Must already exist — "
            "create it once via the Google Ads UI (Audience Manager) or the "
            "Google Ads API; this component only uploads members, it does not "
            "create user lists."
        ),
    )
    fields_map: Dict[str, str] = Field(
        description=(
            "Upstream column -> identifier type. Supported types: 'email', "
            "'phone'. Values are hashed (SHA-256) before upload — never sent "
            "to Google in plaintext."
        ),
    )
    operation: str = Field(
        default="add",
        description=(
            "'add' (default) adds matching users to the list — the normal "
            "activation case. 'remove' removes them — useful for suppression "
            "(e.g. stop retargeting customers who already converted or churned)."
        ),
    )
    batch_size: int = Field(
        default=50000,
        description="Max upstream rows per run (safety cap).",
    )

    group_name: Optional[str] = Field(default="google_ads", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(
        default=None, description="Asset kinds (auto-includes 'google_ads')."
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("google_ads")

        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError(
                "GoogleAdsCustomerMatchUpsertComponent: supply exactly one of "
                "`upstream_asset_key` OR `source:` (got both or neither)."
            )

        if self.operation not in ("add", "remove"):
            raise ValueError(
                f"GoogleAdsCustomerMatchUpsertComponent: operation must be "
                f"'add' or 'remove', got {self.operation!r}."
            )

        bad_types = set(self.fields_map.values()) - _SUPPORTED_IDENTIFIER_TYPES
        if bad_types:
            raise ValueError(
                f"GoogleAdsCustomerMatchUpsertComponent: fields_map has "
                f"unsupported identifier type(s) {sorted(bad_types)}. "
                f"Supported: {sorted(_SUPPORTED_IDENTIFIER_TYPES)}."
            )

        use_source = self.source is not None
        extra_rks: set = set()
        if use_source and (self.source.get("kind") or "").lower() == "sql":
            _rk = self.source.get("resource_key")
            if _rk:
                extra_rks.add(_rk)

        # ── Source resolver (self-contained per no-shared-code rule) ──────
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
            raise ValueError(f"GoogleAdsCustomerMatchUpsertComponent source kind={kind!r} not supported (sql / csv / inline)")

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

            resource = getattr(context.resources, _self.resource_key)
            client = resource.get_client()
            offline_user_data_job_service = client.get_service("OfflineUserDataJobService")

            # Build operations first so we never create a job with zero ops.
            operations = []
            skipped_no_identifier = 0
            for _, row in df.iterrows():
                row_dict = row.to_dict()
                identifiers = _extract_identifiers_for_row(row_dict, _self.fields_map)
                if not identifiers:
                    skipped_no_identifier += 1
                    continue
                op = client.get_type("OfflineUserDataJobOperation")
                user_data = op.create if _self.operation == "add" else op.remove
                for id_type, hashed_value in identifiers:
                    user_identifier = client.get_type("UserIdentifier")
                    if id_type == "email":
                        user_identifier.hashed_email = hashed_value
                    elif id_type == "phone":
                        user_identifier.hashed_phone_number = hashed_value
                    user_data.user_identifiers.append(user_identifier)
                operations.append(op)

            if not operations:
                context.log.warning(
                    "No rows had a valid identifier (email/phone) — nothing to upload."
                )
                return dg.MaterializeResult(
                    metadata={
                        "rows_submitted": dg.MetadataValue.int(0),
                        "rows_skipped_no_identifier": dg.MetadataValue.int(skipped_no_identifier),
                    }
                )

            job_type_enum = client.enums.OfflineUserDataJobTypeEnum.CUSTOMER_MATCH_USER_LIST
            job = client.get_type("OfflineUserDataJob")
            job.type_ = job_type_enum
            job.customer_match_user_list_metadata.user_list = _self.user_list_resource_name

            create_response = offline_user_data_job_service.create_offline_user_data_job(
                customer_id=resource.customer_id,
                job=job,
            )
            job_resource_name = create_response.resource_name

            for chunk_start in range(0, len(operations), _OPERATIONS_PER_REQUEST):
                chunk = operations[chunk_start:chunk_start + _OPERATIONS_PER_REQUEST]
                request = client.get_type("AddOfflineUserDataJobOperationsRequest")
                request.resource_name = job_resource_name
                request.operations.extend(chunk)
                request.enable_partial_failure = True
                offline_user_data_job_service.add_offline_user_data_job_operations(request=request)

            offline_user_data_job_service.run_offline_user_data_job(resource_name=job_resource_name)

            context.log.info(
                f"Google Ads Customer Match {_self.operation}: job={job_resource_name} "
                f"rows_submitted={len(operations)} rows_skipped_no_identifier={skipped_no_identifier} "
                f"user_list={_self.user_list_resource_name}. Processing is asynchronous on "
                f"Google's side — check Audience Manager -> Job History for results."
            )

            return dg.MaterializeResult(
                metadata={
                    "job_resource_name": dg.MetadataValue.text(job_resource_name),
                    "user_list_resource_name": dg.MetadataValue.text(_self.user_list_resource_name),
                    "operation": dg.MetadataValue.text(_self.operation),
                    "rows_submitted": dg.MetadataValue.int(len(operations)),
                    "rows_skipped_no_identifier": dg.MetadataValue.int(skipped_no_identifier),
                }
            )

        common_kwargs = dict(
            key=dg.AssetKey.from_user_string(_self.asset_name),
            group_name=_self.group_name,
            kinds=kinds,
            owners=_self.owners,
            tags=_self.tags,
            description=_self.description or (
                f"Upsert DataFrame rows into Google Ads Customer Match list "
                f"{_self.user_list_resource_name} (operation={_self.operation})."
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
