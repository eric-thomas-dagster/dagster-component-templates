"""DataFrame -> Metronome usage event send (reverse-ETL billing activation).

For every upstream row: `POST {base_url}/ingest` with a JSON array of usage
events (via `MetronomeResource.get_client()` -- a real authenticated
`requests.Session`). Metronome accepts between 1 and 100 events per
request, so rows are chunked into `batch_size` (<=100) requests.

Verified against docs.metronome.com (2026-10) -- this is the load-bearing
part of this component, since getting it wrong risks duplicate billing:

  - A usage event is a JSON object with required fields `transaction_id`,
    `customer_id`, `timestamp`, `event_type`, and optional `properties`.
    Request body is a bare JSON array (NOT `{"events": [...]}`), e.g.:
    `POST /v1/ingest  [{"transaction_id": "...", "customer_id": "...",
    "event_type": "...", "timestamp": "2026-03-09T12:00:00Z",
    "properties": {...}}, ...]`
  - `transaction_id` is the idempotency key: once Metronome accepts an
    event with a given `transaction_id`, it ignores (de-duplicates)
    subsequent events with the same `transaction_id` for the next 34 days.
    This is exactly why `transaction_id` MUST be stable/unique per logical
    event, not regenerated on every send attempt -- this component computes
    it once per row *before* the retry loop so retries of the same chunk
    are safe no-ops on Metronome's side rather than duplicate charges.
  - `customer_id` accepts EITHER a Metronome customer UUID OR an "ingest
    alias" (an internal identifier mapped to a customer via
    `ingest_aliases` at customer-creation time) -- same field, no separate
    alias field needed.
  - `timestamp` must be RFC 3339 with a 4-digit year (e.g.
    "2026-03-09T12:00:00Z").
  - Retry semantics straight from Metronome's own docs: ALWAYS retry a
    failed `/ingest` call on a network error or 5xx until you get a 200
    (the `transaction_id` idempotency key makes this safe). A 429 means a
    rate limit was hit -- back off with exponentially increasing delay and
    retry. A 4xx other than 429 should NOT be retried (it's a malformed
    request, retrying won't fix it). This component bounds the "always
    retry" guidance to `max_retries` (with exponential backoff) rather than
    retrying forever, and raises loudly when that budget is exhausted --
    unlike this repo's Twilio SMS send sibling, which isolates per-row
    failures (a bad phone number) and keeps going, a failed Metronome batch
    after exhausting retries usually indicates a systemic problem (bad
    auth, network partition) where silently dropping billing events would
    be worse than failing the run.

Pairs with:
  - ``metronome_resource`` -- static Bearer API key auth (required)
"""
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


def _json_safe(value: Any) -> Any:
    """Coerce a single upstream cell value into something `json.dumps`
    (via requests' `json=`) can serialize -- pandas/numpy scalars and NaN
    are the usual offenders in a reverse-ETL properties payload."""
    import math

    if value is None:
        return None
    try:
        if isinstance(value, float) and math.isnan(value):
            return None
    except Exception:  # noqa: BLE001
        pass
    if hasattr(value, "item"):  # numpy scalar (int64, float64, bool_, ...)
        try:
            return value.item()
        except Exception:  # noqa: BLE001
            pass
    if hasattr(value, "isoformat"):  # pandas.Timestamp / datetime / date
        return value.isoformat()
    if isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def _default_transaction_id(customer_id: Any, event_type: str, timestamp: str, row_index: Any) -> str:
    """Deterministic fallback transaction_id when no `transaction_id_column`
    is configured -- a stable uuid5 hash of the event's identifying fields
    so that re-sending the SAME logical row (e.g. a retried chunk, or a
    rerun over unchanged upstream data) produces the SAME transaction_id,
    letting Metronome's own dedup do its job instead of double-billing."""
    name = f"{customer_id}:{event_type}:{timestamp}:{row_index}"
    return str(uuid.uuid5(uuid.NAMESPACE_URL, name))


def _call_metronome_ingest(resource, events: List[Dict[str, Any]]):
    """Isolates the one real external-API boundary (`POST {base_url}/ingest`)
    so it can be monkeypatched wholesale in tests without the real
    `requests` network call ever firing -- mirrors this repo's "mock only
    the paid/external call" test convention. Body is a bare JSON array."""
    session = resource.get_client()
    base_url = getattr(resource, "base_url", None) or "https://api.metronome.com/v1"
    return session.post(f"{base_url}/ingest", json=events)


class MetronomeUsageEventSendComponent(dg.Component, dg.Model, dg.Resolvable):
    """Send a Metronome usage event (`POST /ingest`) for each row of an
    upstream DataFrame -- usage-based billing activation.

    Example:
        ```yaml
        type: dagster_component_templates.MetronomeUsageEventSendComponent
        attributes:
          asset_name: metronome_api_usage_events
          upstream_asset_key: dbt_marts_api_calls_today
          resource_key: metronome_resource
          customer_id_column: metronome_customer_id
          event_type: api_call
          timestamp_column: called_at
          transaction_id_column: request_id
          properties_columns: [endpoint, status_code, duration_ms]
          batch_size: 100
        ```

    !! Every materialization sends REAL usage events that affect REAL
    customer invoices. `transaction_id_column` should reference a column
    that is stable and unique per logical event (e.g. a request/event ID
    from your source system) -- this is what makes reruns and HTTP retries
    safe rather than a source of duplicate billing.
    """

    asset_name: str = Field(description="Output Dagster asset name.")

    # Two source shapes -- supply exactly one (same convention as this
    # repo's other reverse-ETL components).
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
        default="metronome_resource",
        description="Resource key registered by MetronomeResourceComponent.",
    )

    customer_id_column: str = Field(
        description=(
            "Upstream column holding the Metronome customer identifier -- "
            "either a Metronome customer UUID or an ingest alias (Metronome "
            "accepts both in the same `customer_id` event field)."
        ),
    )
    event_type: Optional[str] = Field(
        default=None,
        description=(
            "Static event_type applied to every row (the billable-metric "
            "grouping key). Mutually exclusive with `event_type_column`."
        ),
    )
    event_type_column: Optional[str] = Field(
        default=None,
        description=(
            "Upstream column holding a per-row event_type. Mutually exclusive "
            "with the static `event_type`."
        ),
    )
    timestamp_column: Optional[str] = Field(
        default=None,
        description=(
            "Upstream column holding the event timestamp. If unset, the "
            "current UTC time at send time is used for every row (fine for "
            "near-real-time activation, NOT recommended for backfills -- set "
            "this explicitly when replaying historical usage)."
        ),
    )
    transaction_id_column: Optional[str] = Field(
        default=None,
        description=(
            "Upstream column holding a stable, unique-per-event identifier "
            "(e.g. a request ID). STRONGLY recommended: this becomes the "
            "event's `transaction_id`, Metronome's idempotency key. Without "
            "it, a deterministic hash of (customer_id, event_type, timestamp, "
            "row position) is used instead -- safe for retries of the same "
            "run, but less robust across reruns than a real source-system ID."
        ),
    )
    properties_columns: Optional[List[str]] = Field(
        default=None,
        description=(
            "Upstream columns to include in the event's `properties` object. "
            "Defaults to every column EXCEPT customer_id_column/"
            "event_type_column/timestamp_column/transaction_id_column."
        ),
    )

    batch_size: int = Field(
        default=100,
        ge=1,
        le=100,
        description=(
            "Rows per /ingest request. Metronome accepts at most 100 events "
            "per request -- this is validated, not just a default."
        ),
    )
    max_rows_per_run: Optional[int] = Field(
        default=None,
        description=(
            "Optional blast-radius safety cap on total rows sent per "
            "materialization (independent of batch_size, which only governs "
            "HTTP chunk size). Unset means no cap beyond the upstream data."
        ),
    )
    max_retries: int = Field(
        default=5,
        ge=0,
        description=(
            "Max retry attempts per batch on a network error, 5xx, or 429 "
            "before raising. Metronome's own guidance is to always retry "
            "5xx/network errors until a 200 -- this bounds that to a finite "
            "budget with exponential backoff rather than retrying forever."
        ),
    )
    initial_backoff_seconds: float = Field(
        default=1.0,
        ge=0.0,
        description="Backoff before the first retry. Doubles (x backoff_multiplier) each subsequent attempt.",
    )
    backoff_multiplier: float = Field(
        default=2.0,
        ge=1.0,
        description="Multiplier applied to the backoff delay after each retry.",
    )

    group_name: Optional[str] = Field(default="metronome", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(
        default=None, description="Asset kinds (auto-includes 'metronome')."
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("metronome")

        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError(
                "MetronomeUsageEventSendComponent: supply exactly one of "
                "`upstream_asset_key` OR `source:` (got both or neither)."
            )
        if bool(self.event_type) == bool(self.event_type_column):
            raise ValueError(
                "MetronomeUsageEventSendComponent: supply exactly one of "
                "`event_type` OR `event_type_column` (got both or neither)."
            )
        if not self.customer_id_column:
            raise ValueError("MetronomeUsageEventSendComponent: customer_id_column must be non-empty.")

        use_source = self.source is not None
        extra_rks: set = set()
        if use_source and (self.source.get("kind") or "").lower() == "sql":
            _rk = self.source.get("resource_key")
            if _rk:
                extra_rks.add(_rk)

        # -- Source resolver (self-contained per no-shared-code rule) -----
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
            raise ValueError(f"MetronomeUsageEventSendComponent source kind={kind!r} not supported (sql / csv / inline)")

        def _send_batch_with_retry(context, resource, batch):
            attempt = 0
            backoff = _self.initial_backoff_seconds
            while True:
                try:
                    resp = _call_metronome_ingest(resource, batch)
                except Exception as e:  # noqa: BLE001 -- network error, not an HTTP status
                    attempt += 1
                    if attempt > _self.max_retries:
                        raise dg.Failure(
                            f"Metronome /ingest network error after {_self.max_retries} "
                            f"retries: {type(e).__name__}: {e}"
                        )
                    context.log.warning(
                        f"Metronome /ingest network error (attempt {attempt}/{_self.max_retries}): "
                        f"{e}. Retrying in {backoff}s."
                    )
                    time.sleep(backoff)
                    backoff *= _self.backoff_multiplier
                    continue

                status = getattr(resp, "status_code", None)
                if status == 200:
                    return resp
                if status == 429:
                    attempt += 1
                    if attempt > _self.max_retries:
                        raise dg.Failure(
                            f"Metronome /ingest rate-limited (429) after {_self.max_retries} retries."
                        )
                    retry_after = None
                    headers = getattr(resp, "headers", None)
                    if headers:
                        try:
                            retry_after = float(headers.get("Retry-After"))
                        except (TypeError, ValueError):
                            retry_after = None
                    wait = retry_after if retry_after is not None else backoff
                    context.log.warning(
                        f"Metronome /ingest rate-limited (429, attempt {attempt}/{_self.max_retries}). "
                        f"Backing off {wait}s."
                    )
                    time.sleep(wait)
                    backoff *= _self.backoff_multiplier
                    continue
                if status is not None and 500 <= status < 600:
                    attempt += 1
                    if attempt > _self.max_retries:
                        raise dg.Failure(
                            f"Metronome /ingest server error ({status}) after {_self.max_retries} "
                            f"retries. Metronome's own guidance is to retry 5xx until a 200 -- "
                            f"retry budget exhausted instead of retrying forever."
                        )
                    context.log.warning(
                        f"Metronome /ingest server error {status} (attempt {attempt}/{_self.max_retries}). "
                        f"Retrying in {backoff}s."
                    )
                    time.sleep(backoff)
                    backoff *= _self.backoff_multiplier
                    continue
                # Any other 4xx: Metronome's docs say NOT to retry -- malformed
                # request, retrying the same bytes won't help.
                body_text = getattr(resp, "text", "")
                raise dg.Failure(
                    f"Metronome /ingest rejected batch with status {status} (not retryable): "
                    f"{str(body_text)[:500]}"
                )

        def _run_send(context, upstream):
            import pandas as pd
            if not isinstance(upstream, pd.DataFrame):
                df = pd.DataFrame([upstream]) if isinstance(upstream, dict) else pd.DataFrame(upstream)
            else:
                df = upstream

            resource = getattr(context.resources, _self.resource_key)

            if len(df) == 0:
                context.log.warning("Upstream DataFrame is empty -- nothing to send.")
                return dg.MaterializeResult(
                    metadata={
                        "events_sent": dg.MetadataValue.int(0),
                        "batches_sent": dg.MetadataValue.int(0),
                    }
                )

            for required_col in (_self.customer_id_column,):
                if required_col not in df.columns:
                    raise dg.Failure(
                        f"customer_id_column={required_col!r} not in upstream. "
                        f"Available: {list(df.columns)}"
                    )
            if _self.event_type_column and _self.event_type_column not in df.columns:
                raise dg.Failure(
                    f"event_type_column={_self.event_type_column!r} not in upstream. "
                    f"Available: {list(df.columns)}"
                )
            if _self.timestamp_column and _self.timestamp_column not in df.columns:
                raise dg.Failure(
                    f"timestamp_column={_self.timestamp_column!r} not in upstream. "
                    f"Available: {list(df.columns)}"
                )
            if _self.transaction_id_column and _self.transaction_id_column not in df.columns:
                raise dg.Failure(
                    f"transaction_id_column={_self.transaction_id_column!r} not in upstream. "
                    f"Available: {list(df.columns)}"
                )

            if _self.max_rows_per_run is not None and len(df) > _self.max_rows_per_run:
                context.log.warning(
                    f"Upstream has {len(df)} rows; capped at max_rows_per_run={_self.max_rows_per_run}."
                )
                df = df.head(_self.max_rows_per_run)

            reserved = {
                _self.customer_id_column,
                _self.event_type_column,
                _self.timestamp_column,
                _self.transaction_id_column,
            } - {None}
            prop_cols = _self.properties_columns or [c for c in df.columns if c not in reserved]

            def _is_blank(v) -> bool:
                if v is None:
                    return True
                try:
                    if isinstance(v, float) and pd.isna(v):
                        return True
                except Exception:  # noqa: BLE001
                    pass
                return str(v).strip() == ""

            events: List[Dict[str, Any]] = []
            skipped_no_customer = 0
            for i, row in df.iterrows():
                row_dict = row.to_dict()
                customer_id = row_dict.get(_self.customer_id_column)
                if _is_blank(customer_id):
                    skipped_no_customer += 1
                    continue

                event_type = (
                    row_dict.get(_self.event_type_column)
                    if _self.event_type_column
                    else _self.event_type
                )
                if _self.timestamp_column:
                    raw_ts = row_dict.get(_self.timestamp_column)
                    timestamp = _json_safe(raw_ts)
                    if not isinstance(timestamp, str):
                        timestamp = str(timestamp)
                else:
                    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

                if _self.transaction_id_column:
                    transaction_id = str(row_dict.get(_self.transaction_id_column))
                else:
                    transaction_id = _default_transaction_id(customer_id, str(event_type), timestamp, i)

                properties = {c: _json_safe(row_dict.get(c)) for c in prop_cols}

                events.append({
                    "transaction_id": transaction_id,
                    "customer_id": str(customer_id).strip(),
                    "event_type": str(event_type),
                    "timestamp": timestamp,
                    "properties": properties,
                })

            context.log.info(
                f"Prepared {len(events)} Metronome usage events "
                f"(skipped {skipped_no_customer} rows with no customer_id)."
            )

            events_sent = 0
            batches_sent = 0
            for start in range(0, len(events), _self.batch_size):
                batch = events[start:start + _self.batch_size]
                _send_batch_with_retry(context, resource, batch)
                events_sent += len(batch)
                batches_sent += 1

            context.log.info(
                f"Metronome usage event send: events_sent={events_sent} "
                f"batches_sent={batches_sent} skipped_no_customer_id={skipped_no_customer}."
            )

            return dg.MaterializeResult(
                metadata={
                    "events_sent": dg.MetadataValue.int(events_sent),
                    "batches_sent": dg.MetadataValue.int(batches_sent),
                    "rows_skipped_no_customer_id": dg.MetadataValue.int(skipped_no_customer),
                }
            )

        common_kwargs = dict(
            key=dg.AssetKey.from_user_string(_self.asset_name),
            group_name=_self.group_name,
            kinds=kinds,
            owners=_self.owners,
            tags=_self.tags,
            description=_self.description or (
                f"Send a Metronome usage event via POST /ingest for each upstream row "
                f"(customer_id column {_self.customer_id_column!r}). Sends REAL usage "
                f"events that affect REAL customer invoices."
            ),
        )

        if use_source:
            required_rks = {_self.resource_key} | extra_rks

            @dg.asset(required_resource_keys=required_rks, **common_kwargs)
            def _asset(context: dg.AssetExecutionContext):
                df = _resolve_source_df(context)
                return _run_send(context, df)
        else:
            @dg.asset(
                ins={"upstream": dg.AssetIn(key=dg.AssetKey.from_user_string(_self.upstream_asset_key))},
                required_resource_keys={_self.resource_key},
                **common_kwargs,
            )
            def _asset(context: dg.AssetExecutionContext, upstream):
                return _run_send(context, upstream)

        return dg.Definitions(assets=[_asset])
