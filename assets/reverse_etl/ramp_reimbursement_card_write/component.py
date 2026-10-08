"""DataFrame -> Ramp writes: reimbursements + card management.

One component, four `mode`s, because they all share the same OAuth2
client_credentials connection (``ramp_resource``) and the same dual-source
(`upstream_asset_key`/`source`) + per-row success/error/skip-count
convention used elsewhere in this repo's reverse_etl components (see
``asana_task_create``, ``okta_user_upsert``):

  - ``mileage_reimbursement``  -- POST /developer/v1/reimbursements/mileage
  - ``receipt_reimbursement``  -- POST /developer/v1/reimbursements/submit-receipt
  - ``virtual_card_create``    -- POST .../cards/vault (Vault API -- see below)
  - ``card_update``            -- PATCH /developer/v1/cards/physical/{card_id}

**Read this before using `virtual_card_create` in production.** Creating a
virtual card with a retrievable PAN/CVV goes through Ramp's Vault API,
which Ramp's own docs gate explicitly:

    Ramp reviews your use case, security controls, and PCI handling
    before the Vault API can return full PANs and CVVs in production.
    All customers can use the Vault API in Sandbox. Submit a Developer
    API support ticket to begin the review.

So this mode will work against Ramp's Sandbox environment for every
developer once the right scopes are enabled, but will likely 403/permission-
error in production until Ramp has manually approved your app for Vault
API access -- this is a genuine Ramp-side access-tier gate, not a bug here.
See ``ramp_resource``'s README for the full detail.

**There is no `card_update` spend-limit field, by design.** Ramp's
Developer API documents exactly one way to modify an existing card --
`PATCH /developer/v1/cards/physical/{card_id}`, accepting only
`display_name`, `fund_id`, and `automatic_routing_enabled`. There is no
documented endpoint, for physical OR virtual cards, that changes a spend
limit after creation. `card_update` mode reflects exactly that -- it does
not accept (and could not honor) a spend_limit column.

**Security:** `virtual_card_create` never writes a card's PAN/CVV into
Dagster metadata or logs -- only `card_id`, `spend_limit_id`, and a masked
last-4 are retained. Ramp's own docs: "Do not store or log PANs or CVVs."

Pairs with:
  - ``ramp_resource`` -- OAuth2 client_credentials connection (required)
  - ``ramp_ingestion`` -- the READ-side counterpart (dlt-based bulk pull;
    uses a separately pre-minted static bearer token, not this resource)
"""
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field

_SUPPORTED_MODES = {
    "mileage_reimbursement",
    "receipt_reimbursement",
    "virtual_card_create",
    "card_update",
}
_VALID_DISTANCE_UNITS = {"KILOMETERS", "MILES"}
_VALID_INTERVALS = {"ANNUAL", "DAILY", "MONTHLY", "QUARTERLY", "TERTIARY", "TOTAL", "WEEKLY", "YEARLY"}


def _is_blank(value: Any) -> bool:
    import math

    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    if isinstance(value, str) and not value.strip():
        return True
    return False


class RampReimbursementCardWriteComponent(dg.Component, dg.Model, dg.Resolvable):
    """Write rows of an upstream DataFrame into Ramp as reimbursements or
    card operations, per `mode`.

    Example:
        ```yaml
        type: dagster_component_templates.RampReimbursementCardWriteComponent
        attributes:
          asset_name: ramp_mileage_reimbursements
          upstream_asset_key: approved_mileage_claims
          resource_key: ramp_resource
          mode: mileage_reimbursement
          reimbursee_id_column: ramp_user_id
          trip_date_column: trip_date
          distance_column: miles_driven
          memo_column: trip_memo
        ```
    """

    asset_name: str = Field(description="Output Dagster asset name.")

    upstream_asset_key: Optional[str] = Field(
        default=None,
        description="Upstream Dagster asset providing the DataFrame. Mutually exclusive with `source:`.",
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
        default="ramp_resource",
        description="Resource key registered by RampResourceComponent.",
    )

    mode: str = Field(
        description=(
            "One of 'mileage_reimbursement', 'receipt_reimbursement', "
            "'virtual_card_create', 'card_update'. Each mode calls a different "
            "Ramp endpoint with a different required-column set -- see README."
        ),
    )

    # --- mileage_reimbursement ------------------------------------------
    reimbursee_id_column: Optional[str] = Field(
        default=None,
        description="Upstream column holding the Ramp user ID of the reimbursement recipient. Required for mileage_reimbursement and receipt_reimbursement.",
    )
    trip_date_column: Optional[str] = Field(
        default=None, description="Upstream column holding the trip date (ISO date string). Required for mileage_reimbursement."
    )
    distance_column: Optional[str] = Field(
        default=None, description="Upstream column holding the mileage distance. Required for mileage_reimbursement."
    )
    distance_units: str = Field(
        default="MILES", description="'MILES' or 'KILOMETERS' -- applies to every row in mileage_reimbursement mode."
    )
    start_location_column: Optional[str] = Field(default=None, description="Upstream column holding the trip start location (mileage_reimbursement).")
    end_location_column: Optional[str] = Field(default=None, description="Upstream column holding the trip end location (mileage_reimbursement).")
    memo_column: Optional[str] = Field(default=None, description="Upstream column holding a free-text memo (mileage_reimbursement).")
    spend_allocation_id_column: Optional[str] = Field(default=None, description="Upstream column holding a Ramp spend_allocation_id (mileage_reimbursement).")
    waypoints_column: Optional[str] = Field(
        default=None,
        description="Upstream column holding a comma-separated list of waypoint addresses (mileage_reimbursement).",
    )

    # --- receipt_reimbursement -------------------------------------------
    receipt_file_path_column: Optional[str] = Field(
        default=None,
        description="Upstream column holding a local filesystem path to the receipt image/PDF. Required for receipt_reimbursement.",
    )
    reimbursement_id_column: Optional[str] = Field(
        default=None,
        description=(
            "Upstream column holding an existing Ramp reimbursement_id to attach the receipt to. "
            "If omitted/blank for a row, Ramp attempts to auto-create a draft reimbursement via OCR "
            "on the receipt image (receipt_reimbursement)."
        ),
    )
    idempotency_key_column: Optional[str] = Field(
        default=None,
        description=(
            "Upstream column holding an idempotency key for the receipt upload. If unset, a "
            "deterministic key is derived per row from (reimbursee_id, receipt_file_path, "
            "reimbursement_id) so re-running the same row is idempotent (receipt_reimbursement)."
        ),
    )

    # --- virtual_card_create (Vault API) ---------------------------------
    user_id_column: Optional[str] = Field(
        default=None, description="Upstream column holding the Ramp user_id (cardholder). Required for virtual_card_create."
    )
    limit_amount_column: Optional[str] = Field(
        default=None, description="Upstream column holding the spend limit amount. Required for virtual_card_create."
    )
    interval: Optional[str] = Field(
        default=None,
        description=(
            "Spend limit interval, one of ANNUAL/DAILY/MONTHLY/QUARTERLY/TERTIARY/TOTAL/WEEKLY/YEARLY. "
            "Applies to every row. Required for virtual_card_create."
        ),
    )
    currency_code: str = Field(default="USD", description="Currency code for the spend limit (virtual_card_create).")
    display_name_column: Optional[str] = Field(
        default=None,
        description="Upstream column holding a display name. Used by both virtual_card_create (new card) and card_update (rename existing card).",
    )
    spend_program_id_column: Optional[str] = Field(default=None, description="Upstream column holding a Ramp spend_program_id (virtual_card_create).")
    transaction_amount_limit_column: Optional[str] = Field(
        default=None, description="Upstream column holding a per-transaction amount limit (virtual_card_create)."
    )
    lock_date_column: Optional[str] = Field(default=None, description="Upstream column holding an ISO lock_date for the spend limit (virtual_card_create).")
    spending_restrictions_extra: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Static (same for every row) extra spending_restrictions fields to merge in, e.g. "
            "{'allowed_categories': [...], 'blocked_vendors': [...]} (virtual_card_create)."
        ),
    )

    # --- card_update (physical cards only) --------------------------------
    card_id_column: Optional[str] = Field(
        default=None, description="Upstream column holding the Ramp card_id to update. Required for card_update."
    )
    fund_id_column: Optional[str] = Field(default=None, description="Upstream column holding a new fund_id to attach to the card (card_update).")
    automatic_routing_enabled_column: Optional[str] = Field(
        default=None, description="Upstream column holding a boolean for automatic_routing_enabled (card_update)."
    )

    max_rows: int = Field(default=10000, description="Overall safety cap on rows per run.")

    group_name: Optional[str] = Field(default="ramp", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds (auto-includes 'ramp').")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("ramp")

        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError(
                "RampReimbursementCardWriteComponent: supply exactly one of "
                "`upstream_asset_key` OR `source:` (got both or neither)."
            )

        if self.mode not in _SUPPORTED_MODES:
            raise ValueError(
                f"RampReimbursementCardWriteComponent: mode must be one of "
                f"{sorted(_SUPPORTED_MODES)} -- got {self.mode!r}."
            )

        if self.mode == "mileage_reimbursement":
            missing = [
                f for f in ("reimbursee_id_column", "trip_date_column", "distance_column")
                if not getattr(self, f)
            ]
            if missing:
                raise ValueError(f"mode=mileage_reimbursement requires: {missing}")
            if self.distance_units not in _VALID_DISTANCE_UNITS:
                raise ValueError(f"distance_units must be one of {sorted(_VALID_DISTANCE_UNITS)}, got {self.distance_units!r}")
        elif self.mode == "receipt_reimbursement":
            missing = [f for f in ("reimbursee_id_column", "receipt_file_path_column") if not getattr(self, f)]
            if missing:
                raise ValueError(f"mode=receipt_reimbursement requires: {missing}")
        elif self.mode == "virtual_card_create":
            missing = [f for f in ("user_id_column", "limit_amount_column") if not getattr(self, f)]
            if missing:
                raise ValueError(f"mode=virtual_card_create requires: {missing}")
            if not self.interval:
                raise ValueError("mode=virtual_card_create requires `interval` to be set.")
            if self.interval not in _VALID_INTERVALS:
                raise ValueError(f"interval must be one of {sorted(_VALID_INTERVALS)}, got {self.interval!r}")
        elif self.mode == "card_update":
            if not self.card_id_column:
                raise ValueError("mode=card_update requires `card_id_column`.")
            if not any([self.display_name_column, self.fund_id_column, self.automatic_routing_enabled_column]):
                raise ValueError(
                    "mode=card_update requires at least one of display_name_column/fund_id_column/"
                    "automatic_routing_enabled_column -- Ramp's API has no spend_limit field to update "
                    "here (see README: there is no documented way to change an existing card's spend "
                    "limit via Ramp's Developer API)."
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
            raise ValueError(f"RampReimbursementCardWriteComponent source kind={kind!r} not supported (sql / csv / inline)")

        def _required_cols() -> set:
            if _self.mode == "mileage_reimbursement":
                return {_self.reimbursee_id_column, _self.trip_date_column, _self.distance_column}
            if _self.mode == "receipt_reimbursement":
                return {_self.reimbursee_id_column, _self.receipt_file_path_column}
            if _self.mode == "virtual_card_create":
                return {_self.user_id_column, _self.limit_amount_column}
            if _self.mode == "card_update":
                return {_self.card_id_column}
            return set()

        def _row_value(row: Dict[str, Any], col: Optional[str]):
            if not col:
                return None
            v = row.get(col)
            return None if _is_blank(v) else v

        def _handle_mileage(svc, row: Dict[str, Any]):
            reimbursee_id = _row_value(row, _self.reimbursee_id_column)
            trip_date = _row_value(row, _self.trip_date_column)
            distance = _row_value(row, _self.distance_column)
            if reimbursee_id is None or trip_date is None or distance is None:
                return "skip", None
            waypoints_raw = _row_value(row, _self.waypoints_column)
            waypoints = [w.strip() for w in str(waypoints_raw).split(",") if w.strip()] if waypoints_raw else None
            svc.create_mileage_reimbursement(
                reimbursee_id=str(reimbursee_id),
                trip_date=str(trip_date),
                distance=distance,
                distance_units=_self.distance_units,
                start_location=_row_value(row, _self.start_location_column),
                end_location=_row_value(row, _self.end_location_column),
                memo=_row_value(row, _self.memo_column),
                spend_allocation_id=_row_value(row, _self.spend_allocation_id_column),
                waypoints=waypoints,
            )
            return "ok", None

        def _handle_receipt(svc, row: Dict[str, Any]):
            import uuid

            reimbursee_id = _row_value(row, _self.reimbursee_id_column)
            receipt_path = _row_value(row, _self.receipt_file_path_column)
            if reimbursee_id is None or receipt_path is None:
                return "skip", None
            reimbursement_id = _row_value(row, _self.reimbursement_id_column)
            idempotency_key = _row_value(row, _self.idempotency_key_column)
            if idempotency_key is None:
                idempotency_key = str(
                    uuid.uuid5(
                        uuid.NAMESPACE_URL,
                        f"ramp-receipt:{reimbursee_id}:{receipt_path}:{reimbursement_id or ''}",
                    )
                )
            svc.upload_reimbursement_receipt(
                reimbursee_id=str(reimbursee_id),
                receipt_file_path=str(receipt_path),
                idempotency_key=str(idempotency_key),
                reimbursement_id=str(reimbursement_id) if reimbursement_id is not None else None,
            )
            return "ok", None

        def _handle_virtual_card(svc, row: Dict[str, Any]):
            user_id = _row_value(row, _self.user_id_column)
            limit_amount = _row_value(row, _self.limit_amount_column)
            if user_id is None or limit_amount is None:
                return "skip", None
            transaction_amount_limit_val = _row_value(row, _self.transaction_amount_limit_column)
            transaction_amount_limit = (
                {"amount": transaction_amount_limit_val, "currency_code": _self.currency_code}
                if transaction_amount_limit_val is not None
                else None
            )
            resp = svc.create_virtual_card(
                user_id=str(user_id),
                limit_amount=limit_amount,
                interval=_self.interval,
                currency_code=_self.currency_code,
                display_name=_row_value(row, _self.display_name_column),
                spend_program_id=_row_value(row, _self.spend_program_id_column),
                lock_date=_row_value(row, _self.lock_date_column),
                transaction_amount_limit=transaction_amount_limit,
                spending_restrictions_extra=_self.spending_restrictions_extra,
            )
            # SECURITY: never retain pan/cvv/expiration -- only card_id,
            # spend_limit_id, and a masked last-4 survive into the summary
            # this component returns. Ramp's own docs: "Do not store or log
            # PANs or CVVs."
            card = resp.get("card") or {}
            pan = card.get("pan")
            summary = {
                "card_id": card.get("id"),
                "spend_limit_id": resp.get("spend_limit_id"),
                "last4": pan[-4:] if pan else None,
            }
            return "ok", summary

        def _handle_card_update(svc, row: Dict[str, Any]):
            card_id = _row_value(row, _self.card_id_column)
            if card_id is None:
                return "skip", None
            display_name = _row_value(row, _self.display_name_column)
            fund_id = _row_value(row, _self.fund_id_column)
            automatic_routing_enabled_raw = _row_value(row, _self.automatic_routing_enabled_column)
            automatic_routing_enabled = (
                bool(automatic_routing_enabled_raw) if automatic_routing_enabled_raw is not None else None
            )
            if display_name is None and fund_id is None and automatic_routing_enabled is None:
                return "skip", None
            svc.update_physical_card(
                card_id=str(card_id),
                display_name=str(display_name) if display_name is not None else None,
                fund_id=str(fund_id) if fund_id is not None else None,
                automatic_routing_enabled=automatic_routing_enabled,
            )
            return "ok", None

        _HANDLERS = {
            "mileage_reimbursement": _handle_mileage,
            "receipt_reimbursement": _handle_receipt,
            "virtual_card_create": _handle_virtual_card,
            "card_update": _handle_card_update,
        }

        def _run_write(context, upstream):
            import pandas as pd
            if not isinstance(upstream, pd.DataFrame):
                df = pd.DataFrame([upstream]) if isinstance(upstream, dict) else pd.DataFrame(upstream)
            else:
                df = upstream

            if len(df) == 0:
                context.log.warning("Upstream DataFrame is empty -- nothing to write.")
                return dg.MaterializeResult(metadata={"rows_total": dg.MetadataValue.int(0), "mode": dg.MetadataValue.text(_self.mode)})

            if len(df) > _self.max_rows:
                context.log.warning(f"Upstream has {len(df)} rows; capped at max_rows={_self.max_rows}.")
                df = df.head(_self.max_rows)

            required_cols = {c for c in _required_cols() if c}
            missing_cols = [c for c in required_cols if c not in df.columns]
            if missing_cols:
                raise dg.Failure(f"Columns not in upstream: {missing_cols}. Available: {list(df.columns)}")

            svc = getattr(context.resources, _self.resource_key)
            handler = _HANDLERS[_self.mode]

            success_count = 0
            skipped_count = 0
            errors: List[str] = []
            created_cards: List[Dict[str, Any]] = []

            for i, row in df.iterrows():
                row_dict = row.to_dict()
                try:
                    status, extra = handler(svc, row_dict)
                except Exception as e:  # noqa: BLE001
                    errors.append(f"row {i}: {type(e).__name__}: {e}")
                    continue
                if status == "skip":
                    skipped_count += 1
                    continue
                success_count += 1
                if extra:
                    created_cards.append(extra)

            context.log.info(
                f"Ramp {_self.mode}: {success_count} succeeded, {len(errors)} errors, "
                f"{skipped_count} skipped (missing required value)."
            )
            if errors:
                context.log.error("First few errors:\n" + "\n".join(errors[:5]))

            metadata = {
                "mode": dg.MetadataValue.text(_self.mode),
                "rows_total": dg.MetadataValue.int(len(df)),
                "rows_succeeded": dg.MetadataValue.int(success_count),
                "rows_errored": dg.MetadataValue.int(len(errors)),
                "rows_skipped": dg.MetadataValue.int(skipped_count),
            }
            if errors:
                metadata["first_errors"] = dg.MetadataValue.json(errors[:5])
            if created_cards:
                # Already redacted of pan/cvv/expiration -- see _handle_virtual_card.
                metadata["created_cards"] = dg.MetadataValue.json(created_cards[:50])
            return dg.MaterializeResult(metadata=metadata)

        common_kwargs = dict(
            key=dg.AssetKey.from_user_string(_self.asset_name),
            group_name=_self.group_name,
            kinds=kinds,
            owners=_self.owners,
            tags=_self.tags,
            description=_self.description or f"Write upstream DataFrame rows into Ramp (mode={_self.mode}).",
        )

        if use_source:
            required_rks = {_self.resource_key} | extra_rks

            @dg.asset(required_resource_keys=required_rks, **common_kwargs)
            def _asset(context: dg.AssetExecutionContext):
                df = _resolve_source_df(context)
                return _run_write(context, df)
        else:
            @dg.asset(
                ins={"upstream": dg.AssetIn(key=dg.AssetKey.from_user_string(_self.upstream_asset_key))},
                required_resource_keys={_self.resource_key},
                **common_kwargs,
            )
            def _asset(context: dg.AssetExecutionContext, upstream):
                return _run_write(context, upstream)

        return dg.Definitions(assets=[_asset])
