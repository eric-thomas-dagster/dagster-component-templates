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



# BEGIN upstream-asset-key normalization (inlined per component; keep copies identical — see tests/test_upstream_keys.py)
from typing import List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def normalize_upstream_keys(single, plural=None) -> List[str]:
    """Merge ``upstream_asset_key`` / ``upstream_asset_keys`` into one clean list.

    Each argument may be a str, a list/tuple of str, or None. Entries are
    stripped, empties dropped, and duplicates removed (order preserved).
    """
    out: List[str] = []
    for value in (single, plural):
        if value is None:
            continue
        items = [value] if isinstance(value, str) else value
        if not isinstance(items, (list, tuple)):
            raise ValueError(
                f"upstream asset key(s) must be a string or a list of strings, got {value!r}"
            )
        for item in items:
            if not isinstance(item, str):
                raise ValueError(f"upstream asset keys must be strings, got {item!r}")
            item = item.strip()
            if item and item not in out:
                out.append(item)
    return out


def coerce_upstream_input(data, *, component: str, mode: str, optional: bool = True):
    """Rewrite raw component input so both spellings/shapes land on the declared field(s).

    mode "single": declared field is ``upstream_asset_key`` (str); >1 key is an error.
    mode "multi":  declared field is ``upstream_asset_keys`` (list).
    mode "both":   both fields are declared; everything is merged into the plural one.
    """
    if not isinstance(data, dict):
        return data
    if "upstream_asset_key" not in data and "upstream_asset_keys" not in data:
        return data
    keys = normalize_upstream_keys(data.get("upstream_asset_key"), data.get("upstream_asset_keys"))
    out = {k: v for k, v in data.items() if k not in ("upstream_asset_key", "upstream_asset_keys")}
    if mode == "single":
        if len(keys) > 1:
            raise ValueError(f"{component} takes exactly one upstream asset, got {keys}")
        out["upstream_asset_key"] = keys[0] if keys else (None if optional else "")
    else:
        out["upstream_asset_keys"] = keys or (None if optional else [])
    return out
# END upstream-asset-key normalization



# BEGIN field-shape normalization (inlined per component; keep copies identical — see tests/test_field_shapes.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def coerce_field_shapes(data, *, lists=(), csv=(), tags=()):
    """Accept the shapes people (and LLMs) naturally write for common boilerplate fields."""
    if not isinstance(data, dict):
        return data
    out = dict(data)
    for k in lists:  # List[str]: "a" / "a, b" -> ["a", "b"]; strip + drop empties
        v = out.get(k)
        if isinstance(v, str):
            v = v.split(",")
        if isinstance(v, (list, tuple)):
            out[k] = [x.strip() if isinstance(x, str) else x for x in v if not (isinstance(x, str) and not x.strip())]
    for k in csv:  # comma-separated str given as a list -> "a,b"
        v = out.get(k)
        if isinstance(v, (list, tuple, set)):
            out[k] = ",".join(str(x).strip() for x in v)
    for k in tags:  # Dict[str, str]: scalars -> str, ["k=v"] / ["k:v"] -> {k: v}
        v = out.get(k)
        if isinstance(v, (list, tuple)) and v and all(isinstance(x, str) and ("=" in x or ":" in x) for x in v):
            v = dict((p.strip() for p in (x.split("=", 1) if "=" in x else x.split(":", 1))) for x in v)
        if isinstance(v, dict):
            out[k] = {str(a).strip(): (str(b).lower() if isinstance(b, bool) else str(b)) for a, b in v.items() if b is not None}
    return out
# END field-shape normalization


class RampReimbursementCardWriteComponent(dg.Component, dg.Model, dg.Resolvable):
    """Write rows of an upstream DataFrame into Ramp as reimbursements or
    card operations, per `mode`.

    Example:
        ```yaml
        type: dagster_community_components.RampReimbursementCardWriteComponent
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

    upstream_asset_key: Optional[Union[str, List[str]]] = Field(
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

    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', 'dynamic', or None for unpartitioned. A partitioned run sends only the rows in its partition.",
    )

    partition_cron_schedule: Optional[str] = Field(
        default=None,
        description=(
            "Required when partition_type='cron'. A cron expression restricting the date axis to a "
            "custom cadence, e.g. '0 0 * * 1-5' for weekdays only (business-day cadence). Built on "
            "Dagster's TimeWindowPartitionsDefinition(cron_schedule=...)."
        ),
    )
    partition_cron_fmt: Optional[str] = Field(
        default="%Y-%m-%d",
        description="strftime format for partition keys produced by partition_type='cron'. Only change this for a sub-daily cron_schedule.",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.",
    )
    partition_values: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning, e.g. 'customer_a,customer_b'.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition (when partition_type='dynamic'), e.g. 'tenants'.",
    )
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Multi-axis partition spec: list of {name, type, start, values, dynamic_partition_name} dicts. Overrides the flat partition fields.",
    )
    partition_date_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter the rows to send to the current date partition (time partitions).",
    )
    partition_static_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter the rows to send to the current static/dynamic partition key.",
    )
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds (auto-includes 'ramp').")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('owners', 'kinds'), csv=(), tags=('tags',))

    @model_validator(mode="before")
    @classmethod
    def _coerce_upstream_asset_keys(cls, data):
        return coerce_upstream_input(data, component=cls.__name__, mode="single", optional=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values, self.dynamic_partition_name, self.partition_dimensions
        ,
    cron_schedule=self.partition_cron_schedule,
    cron_fmt=self.partition_cron_fmt or "%Y-%m-%d",
)
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
            partitions_def=partitions_def,
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
                df = _filter_df_to_partition(df, context, _self.partition_date_column, _self.partition_static_column)
                return _run_write(context, df)
        else:
            @dg.asset(
                ins={"upstream": dg.AssetIn(key=dg.AssetKey.from_user_string(_self.upstream_asset_key))},
                required_resource_keys={_self.resource_key},
                **common_kwargs,
            )
            def _asset(context: dg.AssetExecutionContext, upstream):
                return _run_write(context, _filter_df_to_partition(upstream, context, _self.partition_date_column, _self.partition_static_column))

        return dg.Definitions(assets=[_asset])


# BEGIN reverse-etl partition helpers (inlined per component; keep copies identical, see tests/test_partition_port_reverse_etl.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from dagster import (  # noqa: E402,F401
    DailyPartitionsDefinition,
    DynamicPartitionsDefinition,
    HourlyPartitionsDefinition,
    MonthlyPartitionsDefinition,
    MultiPartitionsDefinition,
    StaticPartitionsDefinition,
    WeeklyPartitionsDefinition,
)


def _build_partitions_def(
    partition_type,
    partition_start,
    partition_values,
    dynamic_partition_name,
    partition_dimensions,
    cron_schedule=None,
    cron_fmt="%Y-%m-%d",
):
    """Construct a Dagster partitions_def from the canonical partition fields.

    Strict: raises ValueError on misconfigured combinations rather than
    silently picking a default. Specifically:
      - time-based partition_type without partition_start
      - partition_type=multi without partition_values
      - partition_type=dynamic without dynamic_partition_name
      - both partition_dimensions AND flat fields set (ambiguous intent)
    """
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )

    if partition_dimensions and partition_type:
        raise ValueError(
            "Set either partition_type (flat-fields shape) or "
            "partition_dimensions (multi-axis shape), not both."
        )

    def _build_axis(spec):
        t = spec.get("type")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily":
            return DailyPartitionsDefinition(start_date=spec["start"])
        if t == "weekly":
            return WeeklyPartitionsDefinition(start_date=spec["start"])
        if t == "monthly":
            return MonthlyPartitionsDefinition(start_date=spec["start"])
        if t == "hourly":
            return HourlyPartitionsDefinition(start_date=spec["start"])
        if t == "cron":
            from dagster import TimeWindowPartitionsDefinition
            if not spec.get("start"):
                raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
            if not spec.get("cron_schedule"):
                raise ValueError("partition dimension type='cron' requires 'cron_schedule'")
            return TimeWindowPartitionsDefinition(
                start=spec["start"], fmt=spec.get("fmt") or "%Y-%m-%d", cron_schedule=spec["cron_schedule"],
            )
        if t == "static":
            vals = spec.get("values") or []
            if isinstance(vals, str):
                vals = [v.strip() for v in vals.split(",") if v.strip()]
            if not vals:
                raise ValueError("partition dimension type='static' requires non-empty 'values'")
            return StaticPartitionsDefinition(list(vals))
        if t == "dynamic":
            name = spec.get("dynamic_partition_name") or spec.get("name")
            if not name:
                raise ValueError("partition dimension type='dynamic' requires a name")
            return DynamicPartitionsDefinition(name=name)
        raise ValueError(f"unknown partition type: {t!r}")

    if partition_dimensions:
        if len(partition_dimensions) == 1:
            return _build_axis(partition_dimensions[0])
        axes = {d["name"]: _build_axis(d) for d in partition_dimensions}
        return MultiPartitionsDefinition(axes)

    if not partition_type:
        return None
    if isinstance(partition_values, (list, tuple)):
        _values = [str(v).strip() for v in partition_values if str(v).strip()]
    else:
        _values = [v.strip() for v in (str(partition_values) if partition_values else "").split(",") if v.strip()]
    if partition_type in ("daily", "weekly", "monthly", "hourly", "cron") and not partition_start:
        raise ValueError(
            f"partition_type={partition_type!r} requires partition_start (ISO date, e.g. '2024-01-01')."
        )
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "cron":
        from dagster import TimeWindowPartitionsDefinition
        if not cron_schedule:
            raise ValueError(
                "partition_type='cron' requires cron_schedule, e.g. '0 0 * * 1-5' for weekdays only."
            )
        return TimeWindowPartitionsDefinition(start=partition_start, fmt=cron_fmt, cron_schedule=cron_schedule)
    if partition_type == "static":
        if not _values:
            raise ValueError("partition_type='static' requires partition_values (comma-separated).")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError(
                "partition_type='dynamic' requires dynamic_partition_name."
            )
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    if partition_type == "multi":
        if not _values:
            raise ValueError("partition_type='multi' requires partition_values (comma-separated).")
        if not partition_start:
            raise ValueError("partition_type='multi' requires partition_start (the date axis start).")
        return MultiPartitionsDefinition({
            "date": DailyPartitionsDefinition(start_date=partition_start),
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")


def _filter_df_to_partition(df, context, partition_date_column, partition_static_column):
    """Keep the rows that belong to the run's partition(s). A no-op for an unpartitioned run or a non-DataFrame input.

    Time partitions use `context.partition_time_window` (which spans the whole range of a backfill run) against
    `partition_date_column`; static/dynamic/multi partitions match `partition_static_column` against
    `context.partition_keys`. Without the relevant column the input is returned unchanged."""
    import pandas as pd

    if not hasattr(df, "columns") or not (context.has_partition_key or context.has_partition_key_range):
        return df
    if partition_date_column and partition_date_column in df.columns:
        try:
            window = context.partition_time_window
        except Exception:
            return df  # static/dynamic/multi partition: no time window
        dates = pd.to_datetime(df[partition_date_column], errors="coerce", utc=True)
        return df[(dates >= pd.Timestamp(window.start)) & (dates < pd.Timestamp(window.end))]
    if partition_static_column and partition_static_column in df.columns:
        keys = {str(k) for k in context.partition_keys}
        return df[df[partition_static_column].astype(str).isin(keys)]
    return df
# END reverse-etl partition helpers
