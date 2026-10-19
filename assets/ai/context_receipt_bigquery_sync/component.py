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

import logging
import inspect
import weakref
import contextlib
import random
import time
import uuid
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


# ── transient-failure retry (inlined per component; keep self-contained) ───────────────────────────────────────────

LOAD_JOBS_PER_TABLE_PER_DAY = 1500   # BigQuery quota: load jobs per table per day (https://cloud.google.com/bigquery/quotas#load_jobs)
LOAD_JOBS_WARN_PER_RUN = 200
_TRANSIENT_CLASS_NAMES = {"ServiceUnavailable", "InternalServerError", "TooManyRequests", "BadGateway", "GatewayTimeout", "DeadlineExceeded"}
_TRANSIENT_HTTP_CODES = (429, 500, 502, 503, 504)
_QUOTA_REASONS = {"rateLimitExceeded", "quotaExceeded"}
_BACKOFF_BASE_SECONDS = 1.0
_sleep = time.sleep          # tests replace these two for deterministic, instant backoff
_rng = random.Random()
log = logging.getLogger("context_receipt_bigquery_sync")


def _class_names(exc: BaseException) -> set:
    return {c.__name__ for c in type(exc).__mro__}


def _is_transient_bq(exc: BaseException) -> bool:
    """Transient BigQuery/API failures, recognised by class name / attribute (no google import): ServiceUnavailable, InternalServerError,
    TooManyRequests, BadGateway, GatewayTimeout, DeadlineExceeded (or an HTTP 429/500/502/503/504 `code`), connection resets/timeouts, and a 403
    whose errors[0]['reason'] is rateLimitExceeded / quotaExceeded. Every other error (bad SQL, 404, a real 403, ...) is not retried."""
    for e in (exc, exc.__cause__):
        if e is None:
            continue
        if isinstance(e, (ConnectionError, TimeoutError)) or _class_names(e) & _TRANSIENT_CLASS_NAMES:
            return True
        code = getattr(e, "code", None)
        if isinstance(code, int) and code in _TRANSIENT_HTTP_CODES:
            return True
        if code == 403 or "Forbidden" in _class_names(e):
            errs = getattr(e, "errors", None) or []
            first = errs[0] if isinstance(errs, (list, tuple)) and errs else None
            if isinstance(first, dict) and first.get("reason") in _QUOTA_REASONS:
                return True
    return False


def _is_missing_column_error(exc: BaseException) -> bool:
    """The probe `SELECT col ... WHERE 1 = 0` fails like this when the column does not exist (BigQuery: 400 BadRequest 'Unrecognized name')."""
    return bool(_class_names(exc) & {"BadRequest", "NotFound", "BinderException", "CatalogException"}) or "unrecognized name" in str(exc).lower()


class _Runtime:
    """The client, the `bigquery` module, the retry knobs and the per-run counters (load jobs submitted, retries taken)."""

    def __init__(self, client: Any, bq: Any, max_attempts: int = 4, max_backoff: float = 30.0):
        self.client, self.bq, self.max_attempts, self.max_backoff = client, bq, max_attempts, float(max_backoff)
        self.load_jobs = 0
        self.retries = 0

    def retry(self, fn):
        """Run ``fn``; on a transient error wait (exponential backoff, FULL jitter, capped) and try again, up to ``max_attempts`` in all.
        The final failure re-raises the ORIGINAL exception, annotated with the attempt count (``.retry_attempts`` and an exception note)."""
        for n in range(self.max_attempts):
            try:
                return fn()
            except Exception as exc:  # noqa: BLE001
                if not _is_transient_bq(exc):
                    raise
                if n + 1 >= self.max_attempts:
                    exc.retry_attempts = n + 1
                    if hasattr(exc, "add_note"):
                        exc.add_note(f"context_receipt_bigquery_sync: gave up after {n + 1} attempt(s)")
                    raise
                self.retries += 1
                _sleep(_rng.uniform(0.0, min(self.max_backoff, _BACKOFF_BASE_SECONDS * (2 ** n))))


def _query(rt: "_Runtime", sql: str, **params: Any) -> List[Any]:
    """Run one BigQuery statement with named parameters (a list value becomes an ARRAY<STRING>); returns the rows. Transient errors retry
    (every statement here is idempotent: CREATE IF NOT EXISTS, SELECT, DELETE by id)."""
    bq = rt.bq
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

    def attempt() -> List[Any]:
        job = rt.client.query(sql, job_config=bq.QueryJobConfig(query_parameters=qp)) if qp else rt.client.query(sql)
        return list(job.result())

    return rt.retry(attempt)


def _chunks(values: List[str], size: int = _ID_BATCH):
    for i in range(0, len(values), size):
        yield values[i:i + size]


def _load(rt: "_Runtime", table: str, rows: List[Dict[str, Any]]) -> int:
    """Append rows with a LOAD job (not streaming inserts: rows written by streaming (insertAll / Storage Write API) sit in a buffer BigQuery will not DELETE for up to ~30 minutes (the ~90-minute figure is for copy operations),
    which would break the replay path). The table already exists with its schema, so ISO-8601 strings load into TIMESTAMP columns.

    A transient error can arrive AFTER BigQuery accepted the job (timeout reading the response), and a blind retry would append the rows twice.
    So every load carries a client-chosen job id that is reused across retries: resubmitting it answers 409 Conflict, and we then wait on
    the job that already exists instead of creating a second one. A job that finished with an error is abandoned and the next attempt gets a fresh id."""
    if not rows:
        return 0
    cur: Dict[str, Any] = {"jid": None, "job": None}

    def attempt() -> None:
        if cur["jid"] is None:
            cur["jid"], cur["job"] = f"ctxsync_{uuid.uuid4().hex}", None
            rt.load_jobs += 1
        try:
            try:
                job = rt.client.load_table_from_json(rows, table.strip("`"), job_id=cur["jid"], job_config=rt.bq.LoadJobConfig(write_disposition="WRITE_APPEND"))
            except Exception as exc:  # noqa: BLE001
                if "Conflict" not in _class_names(exc):
                    raise
                job = rt.client.get_job(cur["jid"], location=getattr(rt.client, "location", None))   # an earlier attempt got through
            cur["job"] = job
            job.result()
        except Exception:
            if getattr(cur["job"], "error_result", None):   # the job itself failed for good: never wait on it again
                cur["jid"] = None
            raise

    rt.retry(attempt)
    return len(rows)


def _read_state(rt: "_Runtime", dataset: str, prefix: str, name: str) -> Dict[str, int]:
    rows = _query(rt, f"SELECT receipt_cursor, event_cursor FROM `{dataset}.{prefix}{STATE_TABLE}` WHERE component_name = @n "
                      "ORDER BY updated_at DESC, receipt_cursor DESC, event_cursor DESC LIMIT 1", n=name)
    return {"receipt_cursor": int(rows[0]["receipt_cursor"]), "event_cursor": int(rows[0]["event_cursor"])} if rows else {"receipt_cursor": 0, "event_cursor": 0}


def _write_state(rt: "_Runtime", dataset: str, prefix: str, name: str, receipt_cursor: int, event_cursor: int) -> None:
    """Insert the new cursor row FIRST, then prune older rows: there is never a moment with no state row (a crash between the two leaves an
    extra old row, which the ORDER BY ... LIMIT 1 read ignores)."""
    now = datetime.now(timezone.utc).isoformat()
    _load(rt, f"`{dataset}.{prefix}{STATE_TABLE}`", [{"component_name": name, "receipt_cursor": int(receipt_cursor), "event_cursor": int(event_cursor), "updated_at": now}])
    _query(rt, f"DELETE FROM `{dataset}.{prefix}{STATE_TABLE}` WHERE component_name = @n AND updated_at < TIMESTAMP(@now)", n=name, now=now)


def _row(src: Any, cols) -> Dict[str, Any]:
    return {c: src.get(c) for c in cols}


def _sync_batch(rt: "_Runtime", dataset: str, prefix: str, batch: Dict[str, Any]) -> Dict[str, Any]:
    """Load one read_since() page. Re-runnable: receipts/items/excluded are replaced per receipt_id (DELETE then LOAD), events are inserted
    only when their event_id is not already in the table."""
    t = lambda n: receipt_table(dataset, n, prefix)  # noqa: E731
    receipts = batch["receipts"]
    receipt_ids = [r["receipt_id"] for r in receipts]

    for chunk in _chunks(receipt_ids):
        for name in ("context_receipts", "context_receipt_items", "context_receipt_excluded"):
            _query(rt, f"DELETE FROM {t(name)} WHERE receipt_id IN UNNEST(@ids)", ids=chunk)
    shipped = {
        "context_receipts": _load(rt, t("context_receipts"), [_row(r, _RECEIPT_COLS) for r in receipts]),
        "context_receipt_items": _load(rt, t("context_receipt_items"), [_row(i, _ITEM_COLS) for r in receipts for i in r.get("items", [])]),
        "context_receipt_excluded": _load(rt, t("context_receipt_excluded"), [_row(e, _EXCLUDED_COLS) for r in receipts for e in r.get("excluded", [])]),
    }

    events = list({e["event_id"]: {c: e[c] for c in _EVENT_COLS} for e in batch.get("events", [])}.values())  # the store's own rows, as-is
    existing = set()
    for chunk in _chunks([e["event_id"] for e in events]):
        existing |= {r["event_id"] for r in _query(rt, f"SELECT event_id FROM {t('context_receipt_events')} WHERE event_id IN UNNEST(@ids)", ids=chunk)}
    new_events = [e for e in events if e["event_id"] not in existing]
    shipped["context_receipt_events"] = _load(rt, t("context_receipt_events"), new_events)
    newest = [r["created_at"] for r in receipts] + [e["occurred_at"] for e in new_events]
    return {"shipped": shipped, "newest": max(newest) if newest else None}


# --- _vendor_client_session: begin (identical in every component that takes a vendor-client resource; drift-tested) ---
_GENERATOR_CLIENTS: "weakref.WeakKeyDictionary" = weakref.WeakKeyDictionary()   # generator -> the client it yielded


@contextlib.contextmanager
def _vendor_client_session(obj):
    """Yield a vendor client (BigQuery-style: .query / .execute_query) from whatever Dagster hands the asset for a resource key. With
    required_resource_keys, dagster-gcp's BigQueryResource (an IAttachDifferentObjectToOpContext) arrives as a bare GENERATOR that yields the
    client; injected as a parameter it is the resource whose .get_client() is a @contextmanager; a plain client is used as is.

    A bare generator is ONE shared object (context.resources hands every caller in the step the same one) and can only be advanced once, so the
    client it yielded is remembered for the later calls and the generator is left to Dagster / the garbage collector: closing it after the
    first use would make the second use in the same step raise StopIteration. Its `with` exit (credential teardown) runs when it is collected."""
    def _is_client(o):
        return hasattr(o, "query") or hasattr(o, "execute_query")

    def _client_of(gen):
        if gen not in _GENERATOR_CLIENTS:
            _GENERATOR_CLIENTS[gen] = next(gen)
        return _GENERATOR_CLIENTS[gen]

    if inspect.isgenerator(obj):
        yield _client_of(obj)
        return
    if hasattr(obj, "get_client"):
        made = obj.get_client()
        if inspect.isgenerator(made):
            try:
                yield next(made)
            finally:
                made.close()        # our own generator: runs its `with` exit / credential teardown
        elif not _is_client(made) and hasattr(made, "__enter__"):
            with made as client:
                yield client
        else:
            yield made
        return
    yield obj
# --- _vendor_client_session: end ---


def sync_receipts(
    store: Any, client: Any, bq: Any, *, dataset: str, table_prefix: str = "", component_name: str = "context_receipt_bigquery_sync",
    batch_size: int = 1000, max_batches: int = 50, retry_max_attempts: int = 4, retry_max_backoff_seconds: float = 30.0, logger: Any = None,
) -> Dict[str, Any]:
    """Tail `store` into BigQuery until drained or `max_batches`; cursors persist in BigQuery after every batch.
    Transient BigQuery errors retry per statement; ``load_jobs`` in the result counts the load jobs this run submitted."""
    logger = logger or log
    rt = _Runtime(client, bq, retry_max_attempts, retry_max_backoff_seconds)
    if "." not in dataset:
        dataset = f"{client.project}.{dataset}"
    for ddl in receipt_table_ddl(dataset, table_prefix) + [_state_ddl(dataset, table_prefix)]:
        _query(rt, ddl)
    for probe, alter in receipt_column_upgrades(dataset, table_prefix):   # a destination table made by an older release lacks late-added columns
        try:
            _query(rt, probe)
        except Exception as exc:  # noqa: BLE001  (column missing: the probe's SELECT fails)
            if not _is_missing_column_error(exc):   # a transient error that outlived the retries, a crash, a 403...: not evidence the column is missing
                raise
            _query(rt, alter)
    start = _read_state(rt, dataset, table_prefix, component_name)
    rc, ec = start["receipt_cursor"], start["event_cursor"]
    totals = {n: 0 for n in RECEIPT_TABLE_NAMES if n != "context_item_scores"}
    newest: List[str] = []
    batches, drained = 0, False
    while batches < max_batches:
        batch = store.read_since(rc, ec, batch_size)
        if not batch["receipts"] and not batch["events"]:
            drained = True
            break
        res = _sync_batch(rt, dataset, table_prefix, batch)
        for k, v in res["shipped"].items():
            totals[k] += v
        if res["newest"]:
            newest.append(res["newest"])
        rc, ec = int(batch["receipt_cursor"]), int(batch["event_cursor"])
        _write_state(rt, dataset, table_prefix, component_name, rc, ec)  # AFTER the load: crash before this line replays the batch idempotently
        batches += 1
        if len(batch["receipts"]) < batch_size and len(batch["events"]) < batch_size:
            drained = True
            break
    lag = None
    if newest:
        ts = datetime.fromisoformat(max(newest).replace("Z", "+00:00"))
        ts = ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
        lag = max(0.0, (datetime.now(timezone.utc) - ts).total_seconds())
    if rt.load_jobs > LOAD_JOBS_WARN_PER_RUN:
        logger.warning(f"this run submitted {rt.load_jobs} BigQuery load jobs; the quota is {LOAD_JOBS_PER_TABLE_PER_DAY:,} load jobs per table per day "
                       f"and every batch loads into each table once (plus the state table): raise batch_size, lower max_batches or run less often")
    return {"rows_shipped": totals, "cursors_before": start, "cursors_after": {"receipt_cursor": rc, "event_cursor": ec},
            "batches": batches, "drained": drained, "lag_seconds": lag, "load_jobs": rt.load_jobs, "retries": rt.retries}


class ContextReceiptBigQuerySyncComponent(dg.Component, dg.Model, dg.Resolvable):
    """Ship context receipts (retrievals, items, exclusions, outcome/evidence/output events) from a ContextReceiptStore into BigQuery tables.
    Incremental with cursors stored in BigQuery; safe to re-run after a crash."""

    asset_name: str = Field(default="context_receipt_bigquery_sync", description="Dagster asset name")
    store_resource_key: str = Field(
        description="Resource key of the registered ContextReceiptStoreComponent resource (an object with read_since, returning receipts and all events).",
    )
    bigquery_resource_key: str = Field(
        description="Resource key of a dagster_gcp BigQueryResource (entered through its get_client(); also accepts the generator Dagster injects for required_resource_keys, or a bare client).",
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
    retry_max_attempts: int = Field(default=4, description="Tries per BigQuery statement/load job on transient errors (ServiceUnavailable, InternalServerError, TooManyRequests, BadGateway, DeadlineExceeded, 403 rateLimitExceeded/quotaExceeded), with exponential backoff and full jitter. 1 disables retrying. The final failure raises the original error.")
    retry_max_backoff_seconds: float = Field(default=30.0, description="Upper bound on any single wait between retries.")
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
        if isinstance(self.retry_max_attempts, bool) or not 1 <= self.retry_max_attempts <= 20:
            raise ValueError("retry_max_attempts must be an integer between 1 and 20")
        if not 0 < self.retry_max_backoff_seconds <= 3600:
            raise ValueError("retry_max_backoff_seconds must be > 0 and <= 3600")
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
            with _vendor_client_session(getattr(context.resources, comp.bigquery_resource_key)) as client:
                result = sync_receipts(
                    store, client, bq, dataset=comp.dataset, table_prefix=comp.table_prefix, component_name=comp.sync_name,
                    batch_size=comp.batch_size, max_batches=comp.max_batches,
                    retry_max_attempts=comp.retry_max_attempts, retry_max_backoff_seconds=comp.retry_max_backoff_seconds, logger=context.log,
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
                "load_jobs": dg.MetadataValue.int(result["load_jobs"]),
                "retries": dg.MetadataValue.int(result["retries"]),
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