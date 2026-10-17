"""ContextReceiptBigQuerySyncComponent -- ship context receipts from the local SQLite ContextReceiptStore into BigQuery.

The receipt store is a single-host SQLite file. A BigQuery shop wants the same receipts in the warehouse (to join retrievals and outcomes
with the rest of its data, and to compute item feedback by SQL). This asset tails the store and loads the five canonical receipt tables
(`context_receipts`, `context_receipt_items`, `context_receipt_excluded`, `context_receipt_events`, `context_item_scores`) into a dataset.

Incremental: the cursors live in BigQuery itself (`context_sync_state`), so the sync works from any host or an ephemeral runner.
Idempotent: a crash between loading rows and advancing the cursor replays the batch without duplicating anything.

Which events are shipped: `ContextReceiptStore.read_since` returns, per page, the new receipts (+items, excluded) and `events` -- EVERY event
(outcome, evidence, output_attached) with its real `event_id`, `occurred_at` and raw `payload_json` -- and `event_cursor` advances over all of them.
So the events table is shipped straight from the page: no per-receipt re-fetch, and a late evidence/output event on an old receipt is picked up
by the event cursor like any other event (no reconcile window). Events are deduplicated in BigQuery by `event_id`.
"""

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


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


# BEGIN receipt-warehouse-ddl (canonical; inlined verbatim, byte-identical, by every component that creates or reads the receipt tables -- drift-tested)
RECEIPT_TABLE_NAMES = ("context_receipts", "context_receipt_items", "context_receipt_excluded", "context_receipt_events", "context_item_scores")
RECEIPT_DIALECTS = ("bigquery", "snowflake", "databricks", "postgres", "mysql", "duckdb", "sqlite")


def receipt_table(dataset: str, name: str, prefix: str = "", dialect: str = "bigquery") -> str:
    """Table reference for a receipt table. `dataset` is the namespace: `project.dataset` (bigquery), `database.schema` (snowflake),
    `catalog.schema` (databricks), `schema` (postgres/duckdb), `database` (mysql); "" for sqlite or the connection default."""
    if name not in RECEIPT_TABLE_NAMES:
        raise ValueError(f"unknown receipt table {name!r}; valid: {RECEIPT_TABLE_NAMES}")
    if dialect not in RECEIPT_DIALECTS:
        raise ValueError(f"unknown dialect {dialect!r}; valid: {RECEIPT_DIALECTS}")
    parts = [p for p in dataset.strip("`").split(".") if p] + [f"{prefix}{name}"]
    if dialect == "bigquery":
        return "`" + ".".join(parts) + "`"          # BigQuery reads `a.b.c` as one path
    if dialect in ("databricks", "mysql"):
        return ".".join(f"`{p}`" for p in parts)
    if dialect in ("postgres", "duckdb", "sqlite"):
        return ".".join(f'"{p}"' for p in parts)
    return ".".join(parts)                          # snowflake: unquoted, case-insensitive


_RECEIPT_TYPES = {  # (string, long string, integer, float, timestamp)
    "bigquery": ("STRING", "STRING", "INT64", "FLOAT64", "TIMESTAMP"),
    "snowflake": ("VARCHAR", "VARCHAR", "BIGINT", "DOUBLE", "TIMESTAMP_TZ"),
    "databricks": ("STRING", "STRING", "BIGINT", "DOUBLE", "TIMESTAMP"),
    "postgres": ("TEXT", "TEXT", "BIGINT", "DOUBLE PRECISION", "TIMESTAMPTZ"),
    "mysql": ("VARCHAR(255)", "LONGTEXT", "BIGINT", "DOUBLE", "DATETIME(6)"),   # UTC by convention
    "duckdb": ("VARCHAR", "VARCHAR", "BIGINT", "DOUBLE", "TIMESTAMPTZ"),
    "sqlite": ("TEXT", "TEXT", "INTEGER", "REAL", "TEXT"),                       # ISO-8601 UTC strings
}


def receipt_table_ddl(dataset: str, prefix: str = "", dialect: str = "bigquery") -> list:
    """CREATE TABLE IF NOT EXISTS statements for the receipt tables (same columns as the SQLite store, per resources/context_receipt_store/SPEC.md).
    Event payloads stay JSON text in `payload_json`; priors and feedback are computed from them (SQL: JSON_VALUE and friends, or in Python)."""
    if dialect not in RECEIPT_DIALECTS:
        raise ValueError(f"unknown dialect {dialect!r}; valid: {RECEIPT_DIALECTS}")
    S, L, I, F, T = _RECEIPT_TYPES[dialect]
    K = "`rank`" if dialect == "mysql" else "rank"        # RANK is reserved in MySQL 8
    t = lambda n: receipt_table(dataset, n, prefix, dialect)  # noqa: E731
    return [
        f"""CREATE TABLE IF NOT EXISTS {t('context_receipts')} (
  receipt_id {S} NOT NULL, created_at {T} NOT NULL, context_name {S} NOT NULL, context_version {S}, policy_version {S},
  caller_id {S}, purpose {S}, query_hash {S}, eligible_count {I}, excluded_count {I}, dagster_asset_key {S},
  dagster_run_id {S}, trace_id {S}, mcp_session_id {S}, mcp_request_id {S}, dagster_partition_key {S})""",
        f"""CREATE TABLE IF NOT EXISTS {t('context_receipt_items')} (
  receipt_id {S} NOT NULL, item_id {S} NOT NULL, source_asset_key {S}, source_version {S}, source_partition {S},
  source_code_version {S}, {K} {I}, score {F}, selection {S})""",
        f"""CREATE TABLE IF NOT EXISTS {t('context_receipt_excluded')} (
  receipt_id {S} NOT NULL, item_id {S} NOT NULL, reason {S} NOT NULL, source_asset_key {S}, source_version {S},
  source_partition {S}, source_code_version {S})""",
        f"""CREATE TABLE IF NOT EXISTS {t('context_receipt_events')} (
  event_id {S} NOT NULL, receipt_id {S} NOT NULL, event_type {S} NOT NULL, occurred_at {T} NOT NULL, payload_json {L} NOT NULL)""",
        f"""CREATE TABLE IF NOT EXISTS {t('context_item_scores')} (
  item_id {S} NOT NULL, retrievals {I} NOT NULL, outcome_weight {F} NOT NULL, outcome_sum {F} NOT NULL, prior {F} NOT NULL,
  updated_at {T} NOT NULL)""",
    ]


# Columns added after the first release: (table, column, kind). A deployed table created from an older DDL lacks them, so
# creators probe each one and run the ALTER only when the probe fails (never a destructive step; existing rows read NULL).
RECEIPT_ADDED_COLUMNS = (("context_receipts", "dagster_partition_key", "string"),)


def receipt_column_upgrades(dataset: str, prefix: str = "", dialect: str = "bigquery") -> list:
    """``[(probe_sql, alter_sql)]`` per late-added column: run ``probe_sql``; if it raises (column missing), run ``alter_sql``."""
    if dialect not in RECEIPT_DIALECTS:
        raise ValueError(f"unknown dialect {dialect!r}; valid: {RECEIPT_DIALECTS}")
    S, L, I, F, T = _RECEIPT_TYPES[dialect]
    out = []
    for table, column, kind in RECEIPT_ADDED_COLUMNS:
        ref = receipt_table(dataset, table, prefix, dialect)
        out.append((f"SELECT {column} FROM {ref} WHERE 1 = 0", f"ALTER TABLE {ref} ADD COLUMN {column} {S if kind == 'string' else L}"))
    return out
# END receipt-warehouse-ddl


STATE_TABLE = "context_sync_state"
_RECEIPT_COLS = ("receipt_id", "created_at", "context_name", "context_version", "policy_version", "caller_id", "purpose", "query_hash",
                 "eligible_count", "excluded_count", "dagster_asset_key", "dagster_run_id", "trace_id", "mcp_session_id", "mcp_request_id",
                 "dagster_partition_key")
_ITEM_COLS = ("receipt_id", "item_id", "source_asset_key", "source_version", "source_partition", "source_code_version", "rank", "score", "selection")
_EXCLUDED_COLS = ("receipt_id", "item_id", "reason", "source_asset_key", "source_version", "source_partition", "source_code_version")
_EVENT_COLS = ("event_id", "receipt_id", "event_type", "occurred_at", "payload_json")
_ID_BATCH = 5000  # ids per DELETE / SELECT ... UNNEST(@ids) statement


def _state_ddl(dataset: str, prefix: str) -> str:
    return (f"CREATE TABLE IF NOT EXISTS `{dataset}.{prefix}{STATE_TABLE}` "
            "(component_name STRING NOT NULL, receipt_cursor INT64 NOT NULL, event_cursor INT64 NOT NULL, updated_at TIMESTAMP NOT NULL)")


def _query(client: Any, bq: Any, sql: str, **params: Any) -> List[Any]:
    """Run one BigQuery statement with named parameters (a list value becomes an ARRAY<STRING>); returns the rows."""
    qp = []
    for name, value in params.items():
        if isinstance(value, list):
            qp.append(bq.ArrayQueryParameter(name, "STRING", value))
        elif isinstance(value, bool):
            qp.append(bq.ScalarQueryParameter(name, "BOOL", value))
        elif isinstance(value, int):
            qp.append(bq.ScalarQueryParameter(name, "INT64", value))
        else:
            qp.append(bq.ScalarQueryParameter(name, "STRING", value))
    job = client.query(sql, job_config=bq.QueryJobConfig(query_parameters=qp)) if qp else client.query(sql)
    return list(job.result())


def _chunks(values: List[str], size: int = _ID_BATCH):
    for i in range(0, len(values), size):
        yield values[i:i + size]


def _load(client: Any, bq: Any, table: str, rows: List[Dict[str, Any]]) -> int:
    """Append rows with a LOAD job (not streaming inserts: streamed rows sit in a buffer BigQuery will not DELETE for up to ~90 minutes,
    which would break the replay path). The table already exists with its schema, so ISO-8601 strings load into TIMESTAMP columns."""
    if not rows:
        return 0
    job = client.load_table_from_json(rows, table.strip("`"), job_config=bq.LoadJobConfig(write_disposition="WRITE_APPEND"))
    job.result()
    return len(rows)


def _read_state(client: Any, bq: Any, dataset: str, prefix: str, name: str) -> Dict[str, int]:
    rows = _query(client, bq, f"SELECT receipt_cursor, event_cursor FROM `{dataset}.{prefix}{STATE_TABLE}` WHERE component_name = @n "
                              "ORDER BY updated_at DESC, receipt_cursor DESC, event_cursor DESC LIMIT 1", n=name)
    return {"receipt_cursor": int(rows[0]["receipt_cursor"]), "event_cursor": int(rows[0]["event_cursor"])} if rows else {"receipt_cursor": 0, "event_cursor": 0}


def _write_state(client: Any, bq: Any, dataset: str, prefix: str, name: str, receipt_cursor: int, event_cursor: int) -> None:
    """Insert the new cursor row FIRST, then prune older rows: there is never a moment with no state row (a crash between the two leaves an
    extra old row, which the ORDER BY ... LIMIT 1 read ignores)."""
    now = datetime.now(timezone.utc).isoformat()
    _load(client, bq, f"`{dataset}.{prefix}{STATE_TABLE}`", [{"component_name": name, "receipt_cursor": int(receipt_cursor), "event_cursor": int(event_cursor), "updated_at": now}])
    _query(client, bq, f"DELETE FROM `{dataset}.{prefix}{STATE_TABLE}` WHERE component_name = @n AND updated_at < TIMESTAMP(@now)", n=name, now=now)


def _row(src: Any, cols) -> Dict[str, Any]:
    return {c: src.get(c) for c in cols}


def _sync_batch(client: Any, bq: Any, dataset: str, prefix: str, batch: Dict[str, Any]) -> Dict[str, Any]:
    """Load one read_since() page. Re-runnable: receipts/items/excluded are replaced per receipt_id (DELETE then LOAD), events are inserted
    only when their event_id is not already in the table."""
    t = lambda n: receipt_table(dataset, n, prefix)  # noqa: E731
    receipts = batch["receipts"]
    receipt_ids = [r["receipt_id"] for r in receipts]

    for chunk in _chunks(receipt_ids):
        for name in ("context_receipts", "context_receipt_items", "context_receipt_excluded"):
            _query(client, bq, f"DELETE FROM {t(name)} WHERE receipt_id IN UNNEST(@ids)", ids=chunk)
    shipped = {
        "context_receipts": _load(client, bq, t("context_receipts"), [_row(r, _RECEIPT_COLS) for r in receipts]),
        "context_receipt_items": _load(client, bq, t("context_receipt_items"), [_row(i, _ITEM_COLS) for r in receipts for i in r.get("items", [])]),
        "context_receipt_excluded": _load(client, bq, t("context_receipt_excluded"), [_row(e, _EXCLUDED_COLS) for r in receipts for e in r.get("excluded", [])]),
    }

    events = list({e["event_id"]: {c: e[c] for c in _EVENT_COLS} for e in batch.get("events", [])}.values())  # the store's own rows, as-is
    existing = set()
    for chunk in _chunks([e["event_id"] for e in events]):
        existing |= {r["event_id"] for r in _query(client, bq, f"SELECT event_id FROM {t('context_receipt_events')} WHERE event_id IN UNNEST(@ids)", ids=chunk)}
    new_events = [e for e in events if e["event_id"] not in existing]
    shipped["context_receipt_events"] = _load(client, bq, t("context_receipt_events"), new_events)
    newest = [r["created_at"] for r in receipts] + [e["occurred_at"] for e in new_events]
    return {"shipped": shipped, "newest": max(newest) if newest else None}


def sync_receipts(
    store: Any, client: Any, bq: Any, *, dataset: str, table_prefix: str = "", component_name: str = "context_receipt_bigquery_sync",
    batch_size: int = 1000, max_batches: int = 50,
) -> Dict[str, Any]:
    """Tail `store` into BigQuery until drained or `max_batches`; cursors persist in BigQuery after every batch."""
    if "." not in dataset:
        dataset = f"{client.project}.{dataset}"
    for ddl in receipt_table_ddl(dataset, table_prefix) + [_state_ddl(dataset, table_prefix)]:
        _query(client, bq, ddl)
    for probe, alter in receipt_column_upgrades(dataset, table_prefix):   # a destination table made by an older release lacks late-added columns
        try:
            _query(client, bq, probe)
        except Exception:  # noqa: BLE001  (column missing: the probe's SELECT fails)
            _query(client, bq, alter)
    start = _read_state(client, bq, dataset, table_prefix, component_name)
    rc, ec = start["receipt_cursor"], start["event_cursor"]
    totals = {n: 0 for n in RECEIPT_TABLE_NAMES if n != "context_item_scores"}
    newest: List[str] = []
    batches, drained = 0, False
    while batches < max_batches:
        batch = store.read_since(rc, ec, batch_size)
        if not batch["receipts"] and not batch["events"]:
            drained = True
            break
        res = _sync_batch(client, bq, dataset, table_prefix, batch)
        for k, v in res["shipped"].items():
            totals[k] += v
        if res["newest"]:
            newest.append(res["newest"])
        rc, ec = int(batch["receipt_cursor"]), int(batch["event_cursor"])
        _write_state(client, bq, dataset, table_prefix, component_name, rc, ec)  # AFTER the load: crash before this line replays the batch idempotently
        batches += 1
        if len(batch["receipts"]) < batch_size and len(batch["events"]) < batch_size:
            drained = True
            break
    lag = None
    if newest:
        ts = datetime.fromisoformat(max(newest).replace("Z", "+00:00"))
        ts = ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
        lag = max(0.0, (datetime.now(timezone.utc) - ts).total_seconds())
    return {"rows_shipped": totals, "cursors_before": start, "cursors_after": {"receipt_cursor": rc, "event_cursor": ec},
            "batches": batches, "drained": drained, "lag_seconds": lag}


class ContextReceiptBigQuerySyncComponent(dg.Component, dg.Model, dg.Resolvable):
    """Ship context receipts (retrievals, items, exclusions, outcome/evidence/output events) from a ContextReceiptStore into BigQuery tables.
    Incremental with cursors stored in BigQuery; safe to re-run after a crash."""

    asset_name: str = Field(default="context_receipt_bigquery_sync", description="Dagster asset name")
    store_resource_key: str = Field(
        description="Resource key of the registered ContextReceiptStoreComponent resource (an object with read_since, returning receipts and all events).",
    )
    bigquery_resource_key: str = Field(
        description="Resource key of a dagster_gcp BigQueryResource (used as `with resource.get_client() as client`).",
    )
    dataset: str = Field(
        description="Destination dataset, `project.dataset` (or just `dataset` to use the BigQuery client's default project). The five receipt tables and `context_sync_state` live here.",
    )
    table_prefix: str = Field(default="", description="Optional prefix for every table name in the dataset (e.g. 'prod_').")
    sync_name: str = Field(
        default="default",
        description="Name of this sync in `context_sync_state`; use different names if several stores ship into one dataset's state table (they should use different table_prefix values).",
    )
    batch_size: int = Field(default=1000, description="Receipts / events read from the store per batch.")
    max_batches: int = Field(default=50, description="Maximum batches per run; the next run continues from the stored cursor.")
    reconcile_recent_receipts: int = Field(
        default=0,
        description="Deprecated, ignored (kept so existing configs still load). Late evidence/output events are now shipped by the event cursor, so no reconcile window is needed.",
    )
    group_name: Optional[str] = Field(default="context", description="Asset group")
    description: Optional[str] = Field(default=None, description="Asset description")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners (team:name or emails)")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds for the catalog (default: ['bigquery'])")
    asset_tags: Optional[Dict[str, str]] = Field(default=None, description="Tags added to the asset")
    freshness_max_lag_minutes: Optional[int] = Field(default=None, description="Maximum acceptable lag in minutes before the asset is considered stale. Defines a FreshnessPolicy.")
    freshness_cron: Optional[str] = Field(default=None, description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5' (weekdays at 9am).")
    cron_schedule: Optional[str] = Field(default=None, description="Optional cron expression; when set, a schedule runs this sync (e.g. '*/10 * * * *').")
    retry_policy_max_retries: Optional[int] = Field(default=None, description="Max retries on asset failure. Defines a RetryPolicy. Useful for transient network failures, rate limits, etc.")
    retry_policy_delay_seconds: Optional[int] = Field(default=None, description="Seconds between retries (default 1).")
    retry_policy_backoff: str = Field(default="exponential", description="Backoff strategy: 'linear' or 'exponential'.")
    deps: Optional[List[str]] = Field(default=None, description="Lineage-only upstream asset keys (no data passed at runtime).")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('owners', 'kinds', 'deps'), csv=(), tags=('asset_tags',))

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        if self.batch_size < 1 or self.max_batches < 1:
            raise ValueError("batch_size and max_batches must be >= 1")
        if self.retry_policy_backoff.lower() not in ("linear", "exponential"):
            raise ValueError("retry_policy_backoff must be 'linear' or 'exponential'")
        if self.reconcile_recent_receipts:
            import warnings
            warnings.warn("reconcile_recent_receipts is deprecated and ignored: late events are shipped by the event cursor", DeprecationWarning, stacklevel=2)
        comp = self
        asset_key = dg.AssetKey.from_user_string(self.asset_name)
        tags = {**(self.asset_tags or {}), **{f"dagster/kind/{k}": "" for k in (self.kinds or ["bigquery"])}}
        freshness = _freshness_policy_compat(self.freshness_max_lag_minutes, self.freshness_cron) if self.freshness_max_lag_minutes is not None else None
        retry = None
        if self.retry_policy_max_retries is not None:
            retry = dg.RetryPolicy(max_retries=self.retry_policy_max_retries, delay=self.retry_policy_delay_seconds or 1,
                                   backoff=dg.Backoff[self.retry_policy_backoff.upper()])
        kwargs: Dict[str, Any] = {}
        if freshness is not None:
            kwargs["freshness_policy"] = freshness
        if retry is not None:
            kwargs["retry_policy"] = retry

        @dg.asset(
            key=asset_key, group_name=self.group_name, description=self.description or "Context receipts synced from the receipt store into BigQuery.",
            owners=self.owners or None, tags=tags, deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
            required_resource_keys={self.store_resource_key, self.bigquery_resource_key}, **kwargs,
        )
        def _sync(context: dg.AssetExecutionContext):
            from google.cloud import bigquery as bq
            store = getattr(context.resources, comp.store_resource_key)
            with getattr(context.resources, comp.bigquery_resource_key).get_client() as client:
                result = sync_receipts(
                    store, client, bq, dataset=comp.dataset, table_prefix=comp.table_prefix, component_name=comp.sync_name,
                    batch_size=comp.batch_size, max_batches=comp.max_batches,
                )
            md: Dict[str, Any] = {f"rows_{k}": dg.MetadataValue.int(v) for k, v in result["rows_shipped"].items()}
            md.update({
                "dagster/row_count": dg.MetadataValue.int(sum(result["rows_shipped"].values())),
                "receipt_cursor_before": dg.MetadataValue.int(result["cursors_before"]["receipt_cursor"]),
                "event_cursor_before": dg.MetadataValue.int(result["cursors_before"]["event_cursor"]),
                "receipt_cursor_after": dg.MetadataValue.int(result["cursors_after"]["receipt_cursor"]),
                "event_cursor_after": dg.MetadataValue.int(result["cursors_after"]["event_cursor"]),
                "batches": dg.MetadataValue.int(result["batches"]),
                "drained": dg.MetadataValue.bool(result["drained"]),
            })
            if result["lag_seconds"] is not None:
                md["lag_seconds"] = dg.MetadataValue.float(result["lag_seconds"])
            if not result["drained"]:
                context.log.warning("max_batches reached before the store was drained; the next run continues from the stored cursors")
            return dg.MaterializeResult(metadata=md)

        schedules = []
        if self.cron_schedule:
            schedules.append(dg.ScheduleDefinition(name=f"{self.asset_name}_schedule", cron_schedule=self.cron_schedule, target=dg.AssetSelection.assets(asset_key)))
        return dg.Definitions(assets=[_sync], schedules=schedules)


# BEGIN freshness-policy compat (inlined per component; keep copies identical, see tests/test_freshness_policy_compat.py)
def _freshness_policy_compat(max_lag_minutes, cron_schedule=None):
    """Build a FreshnessPolicy on the current Dagster API (time_window / cron) and on the older constructor."""
    from datetime import timedelta

    from dagster import FreshnessPolicy

    lag = timedelta(minutes=max_lag_minutes)
    if hasattr(FreshnessPolicy, "time_window"):
        if cron_schedule:
            return FreshnessPolicy.cron(deadline_cron=cron_schedule, lower_bound_delta=lag)
        return FreshnessPolicy.time_window(fail_window=lag)
    return FreshnessPolicy(maximum_lag_minutes=max_lag_minutes, cron_schedule=cron_schedule)
# END freshness-policy compat