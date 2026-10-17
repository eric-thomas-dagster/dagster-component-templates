"""ReceiptOutcomeIngestionComponent -- turns real feedback data into context-receipt outcomes.

The context-receipts learning loop (``record_outcome`` -> ``item_feedback`` -> rerank priors -> ``recipe_outcomes`` check) only learns
when something calls ``store.record_outcome``. This asset is that something: it reads feedback rows from a warehouse query, files
(csv/json/jsonl/parquet) or an HTTP JSON API, maps each row to ``(receipt_id, signal, value, outcome_ref)`` and records it.

Design points
  * Idempotent: every outcome carries a deterministic ``outcome_ref``; the store supersedes by (receipt, outcome_ref), so re-ingesting
    the same feedback never double-counts and a changed rating (same ref) replaces the old one.
  * Incremental: the cursor (max ``occurred_at`` handled) lives in this asset's own materialization metadata (``cursor``), read back
    through ``context.instance`` -- no local state files. Reads are inclusive (``>= cursor``): boundary rows are re-read, which is safe
    because of the idempotency above.
  * Honest accounting: unknown receipt ids and unmappable rows are counted and sampled in metadata, never crash the run (unless
    ``on_bad_row`` / ``on_unknown_receipt`` is ``fail``).
  * Writes are synchronous (not AsyncReceiptWriter) so the cursor only advances past outcomes that are durably recorded.
"""

import csv
import glob as _glob
import hashlib
import json
import math
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import dagster as dg
from pydantic import Field

# Mirrors resources/context_receipt_store OUTCOME_SIGNALS (inlined: components are self-contained). The store stays the authority:
# a signal not listed here needs an explicit value, exactly as in ``record_outcome``.
OUTCOME_SIGNALS = {
    "thumbs_up": 1.0,
    "thumbs_down": -1.0,
    "accepted": 0.5,
    "edited": -0.3,
    "escalated": -0.7,
    "reopened": -0.7,
    "closed_won": 1.0,
    "closed_lost": -0.5,
    "reformulated": -0.2,
}
SOURCE_KINDS = ("sql", "file", "http_json")
SOURCE_KEYS = {
    "sql": {"kind", "resource_key", "query", "database_url_env_var"},
    "file": {"kind", "path", "format", "records_path", "allow_empty"},
    "http_json": {"kind", "url", "headers", "headers_env", "records_path", "since_param", "since_format", "next_url_path",
                  "page_param", "max_pages", "timeout_seconds"},
}
MAPPING_KEYS = {"receipt_id", "signal", "signal_value", "value", "value_map", "scale", "outcome_ref", "source", "occurred_at",
                "skip_values"}
DEFAULT_SINCE = "1970-01-01T00:00:00+00:00"


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


# ── small helpers ────────────────────────────────────────────────────────────────────────────────────────────────

def _missing(v: Any) -> bool:
    if v is None:
        return True
    if isinstance(v, str):
        return not v.strip()
    try:
        return bool(v != v)  # NaN / pandas NA
    except (TypeError, ValueError):
        return False


def _get(row: Dict[str, Any], name: str) -> Any:
    """Column/field lookup: literal key first, then a dotted path into nested dicts/lists (``satisfaction_rating.score``)."""
    if name in row:
        return row[name]
    cur: Any = row
    for part in name.split("."):
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        elif isinstance(cur, list) and part.isdigit() and int(part) < len(cur):
            cur = cur[int(part)]
        else:
            return None
    return cur


def _norm_key(v: Any) -> str:
    """Normalize a raw cell / map key so 5, 5.0, '5' and ' Good ' vs 'good' all match."""
    if isinstance(v, bool):
        return str(v).lower()
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return str(int(v)) if float(v).is_integer() else str(v)
    s = str(v).strip().lower()
    try:
        f = float(s)
        return str(int(f)) if f.is_integer() else s
    except ValueError:
        return s


def _to_dt(v: Any) -> Optional[datetime]:
    """datetime | pandas Timestamp | ISO string | epoch seconds/ms -> aware UTC datetime; None when unparseable."""
    if _missing(v):
        return None
    try:
        if isinstance(v, datetime):
            dt = v
        elif isinstance(v, (int, float)) and not isinstance(v, bool):
            dt = datetime.fromtimestamp(float(v) / (1000.0 if abs(float(v)) > 1e11 else 1.0), tz=timezone.utc)
        else:
            s = str(v).strip()
            try:
                f = float(s)
                dt = datetime.fromtimestamp(f / (1000.0 if abs(f) > 1e11 else 1.0), tz=timezone.utc)
            except ValueError:
                dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except (ValueError, OverflowError, OSError):
        return None


def _sql_literal(value: Any) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _dotted(obj: Any, path: str) -> Any:
    cur = obj
    for part in path.split("."):
        if isinstance(cur, dict):
            cur = cur.get(part)
        elif isinstance(cur, list) and part.isdigit() and int(part) < len(cur):
            cur = cur[int(part)]
        else:
            return None
    return cur


# ── sources ──────────────────────────────────────────────────────────────────────────────────────────────────────

def _read_sql(cfg: Dict[str, Any], since: str, context) -> List[Dict[str, Any]]:
    """Run ``query`` (``{since}`` bound as an escaped string literal) through a Dagster resource exposing get_engine() (SQLAlchemy),
    get_connection() (DB-API context manager: DuckDB/Postgres/Snowflake) or get_client() (BigQuery-style .query().to_dataframe())."""
    import pandas as pd

    sql = cfg["query"].replace("{since}", _sql_literal(since))
    resource = getattr(context.resources, cfg["resource_key"])
    if hasattr(resource, "get_engine"):
        df = pd.read_sql(sql, resource.get_engine())
    elif hasattr(resource, "get_connection"):
        with resource.get_connection() as conn:  # a @contextmanager (dagster_duckdb.DuckDBResource): must be entered
            df = pd.read_sql(sql, conn)
    elif hasattr(resource, "get_client"):
        client = resource.get_client()
        if not hasattr(client, "query"):
            raise ValueError(f"resource {cfg['resource_key']!r}: get_client() returned {type(client).__name__} with no .query()")
        df = client.query(sql).to_dataframe()
    else:
        raise ValueError(
            f"resource {cfg['resource_key']!r} ({type(resource).__name__}) must expose get_engine(), get_connection() or get_client()"
        )
    df = df.astype(object).where(df.notna(), None)
    return df.to_dict("records")


def _records_at(data: Any, records_path: Optional[str], where: str) -> List[Dict[str, Any]]:
    node = _dotted(data, records_path) if records_path else data
    if node is None and records_path:
        raise ValueError(f"{where}: records_path {records_path!r} not found in the response")
    if isinstance(node, dict):
        node = [node]
    if not isinstance(node, list):
        raise ValueError(f"{where}: expected a list of records, got {type(node).__name__}")
    return [r for r in node if isinstance(r, dict)]


def _read_file(cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    pattern = os.path.expanduser(cfg["path"])
    paths = sorted(_glob.glob(pattern, recursive=True)) if any(c in pattern for c in "*?[") else ([pattern] if os.path.exists(pattern) else [])
    paths = [p for p in paths if os.path.isfile(p)]
    if not paths and not cfg.get("allow_empty"):
        raise FileNotFoundError(f"source.path {cfg['path']!r} matched no files (set source.allow_empty: true to treat that as zero rows)")
    rows: List[Dict[str, Any]] = []
    for p in paths:
        fmt = (cfg.get("format") or Path(p).suffix.lstrip(".")).lower()
        if fmt in ("csv", "tsv"):
            with open(p, newline="", encoding="utf-8-sig") as f:
                rows.extend(dict(r) for r in csv.DictReader(f, delimiter="\t" if fmt == "tsv" else ","))
        elif fmt in ("jsonl", "ndjson"):
            with open(p, encoding="utf-8") as f:
                for n, line in enumerate(f, 1):
                    if line.strip():
                        try:
                            obj = json.loads(line)
                        except json.JSONDecodeError as exc:
                            raise ValueError(f"{p}:{n}: invalid JSON line ({exc})") from exc
                        if isinstance(obj, dict):
                            rows.append(obj)
        elif fmt == "json":
            with open(p, encoding="utf-8") as f:
                rows.extend(_records_at(json.load(f), cfg.get("records_path"), p))
        elif fmt == "parquet":
            import pandas as pd
            df = pd.read_parquet(p)
            df = df.astype(object).where(df.notna(), None)
            rows.extend(df.to_dict("records"))
        else:
            raise ValueError(f"{p}: unsupported file format {fmt!r} (csv, tsv, json, jsonl, parquet)")
    return rows


def _resolve_headers(cfg: Dict[str, Any]) -> Dict[str, str]:
    headers = {str(k): str(v) for k, v in (cfg.get("headers") or {}).items()}
    for name, env in (cfg.get("headers_env") or {}).items():
        val = os.environ.get(str(env))
        if not val:
            raise ValueError(f"source.headers_env: environment variable {env!r} (for header {name!r}) is not set")
        headers[str(name)] = val  # secret: never logged, never put in metadata or error text
    return headers


def _http_get_json(url: str, headers: Dict[str, str], timeout: float) -> Any:
    last: Optional[Exception] = None
    for attempt in range(3):
        try:
            req = urllib.request.Request(url, headers={"Accept": "application/json", **headers})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            if exc.code not in (429, 500, 502, 503, 504):
                raise ValueError(f"GET {url.split('?')[0]} failed: HTTP {exc.code}") from None
            last = exc
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            last = exc
        time.sleep(0.2 * (attempt + 1))
    raise ValueError(f"GET {url.split('?')[0]} failed after 3 attempts: {type(last).__name__}") from None


def _with_param(url: str, name: str, value: Any) -> str:
    parts = urllib.parse.urlsplit(url)
    q = [(k, v) for k, v in urllib.parse.parse_qsl(parts.query, keep_blank_values=True) if k != name]
    q.append((name, str(value)))
    return urllib.parse.urlunsplit(parts._replace(query=urllib.parse.urlencode(q)))


def _read_http(cfg: Dict[str, Any], since: str, context) -> List[Dict[str, Any]]:
    headers = _resolve_headers(cfg)
    timeout = float(cfg.get("timeout_seconds", 30))
    max_pages = int(cfg.get("max_pages", 50))
    url = cfg["url"]
    if cfg.get("since_param"):
        sv: Any = since
        if cfg.get("since_format", "iso") == "epoch":
            sv = int((_to_dt(since) or datetime(1970, 1, 1, tzinfo=timezone.utc)).timestamp())
        url = _with_param(url, cfg["since_param"], sv)
    rows: List[Dict[str, Any]] = []
    page = 1
    for n in range(max_pages):
        if n == 0 and cfg.get("page_param"):
            url = _with_param(url, cfg["page_param"], page)
        data = _http_get_json(url, headers, timeout)
        recs = _records_at(data, cfg.get("records_path"), "http_json")
        rows.extend(recs)
        nxt = _dotted(data, cfg["next_url_path"]) if cfg.get("next_url_path") else None
        if cfg.get("next_url_path"):
            if not nxt:
                break
            url = urllib.parse.urljoin(url, str(nxt))
        elif cfg.get("page_param"):
            if not recs:
                break
            page += 1
            url = _with_param(url, cfg["page_param"], page)
        else:
            break
    else:
        context.log.warning(f"http_json: stopped at max_pages={max_pages}; more pages may exist (they are picked up next run via the cursor)")
    return rows


# ── row -> outcome ──────────────────────────────────────────────────────────────────────────────────────────────

class _Bad(Exception):
    pass


def _resolve_outcome(row: Dict[str, Any], m: Dict[str, Any], vmap: Dict[str, Any], skip: set) -> Optional[Tuple[str, str, float, Optional[str]]]:
    """-> (receipt_id, signal, value, explicit_ref) or None for a deliberately skipped row; raises _Bad with the reason."""
    rid = _get(row, m["receipt_id"])
    if _missing(rid):
        raise _Bad(f"missing receipt_id ({m['receipt_id']!r})")
    rid = str(rid).strip()

    sig: Optional[str] = m.get("signal_value")
    val: Optional[float] = None

    def lookup(raw: Any) -> Any:
        return vmap.get(_norm_key(raw))

    def number(x: Any) -> float:
        if isinstance(x, bool):
            raise _Bad(f"non-numeric value {x!r}")
        try:
            f = float(x)
        except (TypeError, ValueError):
            raise _Bad(f"non-numeric value {x!r}") from None
        if not math.isfinite(f):
            raise _Bad(f"non-finite value {x!r}")
        return f

    for col, is_value_col in ((m.get("signal"), False), (m.get("value"), True)):
        if not col:
            continue
        raw = _get(row, col)
        if _missing(raw):
            raise _Bad(f"missing {col!r}")
        if _norm_key(raw) in skip:
            return None
        hit = lookup(raw)
        if isinstance(hit, str):
            sig = hit
        elif hit is not None:
            val = number(hit)
        elif is_value_col:
            val = number(raw)
            sc = m.get("scale")
            if sc:
                lo, hi = float(sc["min"]), float(sc["max"])
                if not lo <= val <= hi:
                    raise _Bad(f"rating {raw!r} outside scale [{lo:g}, {hi:g}]")
                val = -1.0 + 2.0 * (val - lo) / (hi - lo)
        else:
            sig = str(raw).strip()
    if sig is None and val is not None:
        sig = "rating"
    if sig is None:
        raise _Bad("no signal (set mapping.signal or mapping.signal_value)")
    if val is None:
        if sig not in OUTCOME_SIGNALS:
            raise _Bad(f"unmapped signal {sig!r} (add it to value_map, or give a numeric value)")
    elif not -1.0 <= val <= 1.0:
        raise _Bad(f"value {val:g} outside [-1, 1]")
    ref = _get(row, m["outcome_ref"]) if m.get("outcome_ref") else None
    return rid, sig, (val if val is not None else OUTCOME_SIGNALS[sig]), (None if _missing(ref) else str(ref).strip())


def _outcome_ref(label: str, rid: str, explicit: Optional[str], row: Dict[str, Any], m: Dict[str, Any], sig: str, val: float) -> str:
    """Deterministic. With an explicit key column the ref is ``<label>:<key>`` so a changed rating by the same key supersedes the old
    one. Without one it hashes the row's identity (label, receipt, timestamp, signal, value): identical feedback re-read = same ref."""
    if explicit:
        return f"{label}:{explicit}"
    ts = _get(row, m["occurred_at"]) if m.get("occurred_at") else ""
    payload = json.dumps([label, rid, "" if _missing(ts) else str(ts), sig, round(val, 9)], separators=(",", ":"))
    return "roi:" + hashlib.sha256(payload.encode()).hexdigest()[:24]


def ingest_feedback(store: Any, rows: List[Dict[str, Any]], mapping: Dict[str, Any], *, cursor: Optional[datetime], on_bad_row: str,
                    on_unknown_receipt: str, max_rows: Optional[int]) -> Dict[str, Any]:
    """Pure-ish core (store + rows in, report out): validate every row first, then write. Returns counts, samples and the new cursor."""
    label = mapping.get("source") or "feedback"
    vmap = {_norm_key(k): v for k, v in (mapping.get("value_map") or {}).items()}
    skip = {_norm_key(x) for x in (mapping.get("skip_values") or [])}
    ts_col = mapping.get("occurred_at")
    report: Dict[str, Any] = {"rows_read": len(rows), "rows_before_cursor": 0, "skipped_rows": 0, "bad": [], "unknown": [], "outcomes_recorded": 0,
                              "superseded_in_run": 0, "bad_total": 0, "cursor": None}

    prepared: List[Tuple[Optional[datetime], int, Dict[str, Any]]] = []
    bad = report["bad"]
    for i, row in enumerate(rows):
        dt = None
        if ts_col:
            dt = _to_dt(_get(row, ts_col))
            if dt is None:
                report["bad_total"] += 1
                bad.append({"row": i, "reason": f"missing/unparseable occurred_at ({ts_col!r}): {_get(row, ts_col)!r}"})
                continue
            if cursor is not None and dt < cursor:
                report["rows_before_cursor"] += 1
                continue
        prepared.append((dt, i, row))
    prepared.sort(key=lambda t: (t[0] is None, t[0] or datetime.min.replace(tzinfo=timezone.utc), t[1]))  # oldest first: last writer wins on a ref
    if max_rows is not None:
        prepared = prepared[:max_rows]

    plan: List[Tuple[Optional[datetime], str, str, float, str]] = []
    for dt, i, row in prepared:
        try:
            res = _resolve_outcome(row, mapping, vmap, skip)
        except _Bad as exc:
            report["bad_total"] += 1
            bad.append({"row": i, "reason": str(exc)})
            continue
        if res is None:
            report["skipped_rows"] += 1
            continue
        rid, sig, val, explicit = res
        plan.append((dt, rid, sig, val, _outcome_ref(label, rid, explicit, row, mapping, sig, val)))

    if report["bad_total"] and on_bad_row == "fail":
        raise ValueError(f"{report['bad_total']} bad feedback row(s); first: {bad[:5]} (nothing was recorded; on_bad_row: skip to tolerate)")

    unknown: Dict[str, None] = {}
    seen: set = set()
    newest: Optional[datetime] = cursor
    pending_unknown_fail = False
    for dt, rid, sig, val, ref in plan:
        if rid in unknown:
            continue
        try:
            store.record_outcome(rid, signal=sig, value=val, source=label, outcome_ref=ref)
        except KeyError:
            unknown[rid] = None
            pending_unknown_fail = True
        else:
            report["outcomes_recorded"] += 1
            if (rid, ref) in seen:
                report["superseded_in_run"] += 1
            seen.add((rid, ref))
        if dt is not None and (newest is None or dt > newest):
            newest = dt
    report["unknown"] = list(unknown)
    if pending_unknown_fail and on_unknown_receipt == "fail":
        raise ValueError(f"{len(unknown)} feedback row(s) reference unknown receipt ids, e.g. {list(unknown)[:5]} "
                         f"(known ones were recorded; the cursor was not advanced)")
    # Bad-timestamp rows never move the cursor; unknown-receipt rows do (the receipt is not coming back -- documented).
    report["cursor"] = newest.isoformat() if newest is not None else None
    return report


# ── component ────────────────────────────────────────────────────────────────────────────────────────────────────

class ReceiptOutcomeIngestionComponent(dg.Component, dg.Model, dg.Resolvable):
    """Ingest real feedback (warehouse table, files, or an HTTP JSON API) as context-receipt outcomes, idempotently and incrementally.

    Each feedback row must carry the ``receipt_id`` the application received when it served the answer (see the README for how an app
    carries it through). Outcomes are recorded with deterministic ``outcome_ref``s, so re-ingesting never double-counts, and the
    cursor lives in this asset's own materialization metadata."""

    asset_name: str = Field(default="receipt_outcome_ingestion", description="Dagster asset name")
    store_resource_key: str = Field(default="context_receipts", description="Resource key of the ContextReceiptStore (the context_receipt_store component's resource_key).")
    source: Dict[str, Any] = Field(
        description=(
            "Where feedback comes from. kind: sql {resource_key, query with a {since} placeholder} | file {path (glob ok), format?, "
            "records_path?, allow_empty?} | http_json {url, headers?, headers_env? {Header: ENV_VAR}, records_path?, since_param?, "
            "since_format? iso|epoch, next_url_path?, page_param?, max_pages?}."
        ),
    )
    mapping: Dict[str, Any] = Field(
        description=(
            "How a row becomes an outcome. receipt_id (REQUIRED column/field), signal (column) or signal_value (constant), value (column, "
            "number in [-1,1]), value_map {raw: signal-or-number}, scale {min, max} (linear map of a numeric rating to -1..1), skip_values "
            "[raw values to ignore], outcome_ref (column, e.g. user id, so a changed rating supersedes), source (label), occurred_at "
            "(column; drives the incremental cursor)."
        ),
    )
    on_bad_row: str = Field(default="skip", description="'skip' (count + sample in metadata) or 'fail' (raise before recording anything).")
    on_unknown_receipt: str = Field(default="skip", description="'skip' (count + sample in metadata) or 'fail' (record the known ones, then raise).")
    max_rows: Optional[int] = Field(default=None, description="Process at most this many rows per run, oldest first by occurred_at; the cursor lets the next run continue.")
    initial_since: str = Field(default=DEFAULT_SINCE, description="Value bound to {since} / since_param on the first run (no cursor yet).")
    cron_schedule: Optional[str] = Field(default=None, description="Optional cron expression; when set, a schedule materializes this asset.")
    group_name: Optional[str] = Field(default="context_receipts", description="Asset group")
    description: Optional[str] = Field(default=None, description="Asset description")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners (team:name or emails)")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds for the catalog (default: ['python', 'feedback'])")
    asset_tags: Optional[Dict[str, str]] = Field(default=None, description="Tags added to the asset")
    deps: Optional[List[str]] = Field(default=None, description="Upstream asset keys this ingestion depends on (e.g. the table-producing asset).")
    freshness_max_lag_minutes: Optional[int] = Field(default=None, description="FreshnessPolicy max lag minutes.")
    freshness_cron: Optional[str] = Field(default=None, description="FreshnessPolicy cron schedule.")
    retry_policy_max_retries: Optional[int] = Field(default=None, description="RetryPolicy max retries.")
    retry_policy_delay_seconds: Optional[int] = Field(default=None, description="Seconds between retries.")
    retry_policy_backoff: str = Field(default="exponential", description="'linear' or 'exponential'.")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('owners', 'kinds', 'deps'), csv=(), tags=('asset_tags',))

    def _validate(self) -> None:
        src, m = self.source, self.mapping
        kind = src.get("kind")
        if kind not in SOURCE_KINDS:
            raise ValueError(f"source.kind must be one of {SOURCE_KINDS}, got {kind!r}")
        extra = set(src) - SOURCE_KEYS[kind]
        if extra:
            raise ValueError(f"unknown source key(s) for kind {kind!r}: {sorted(extra)}")
        need = {"sql": ("resource_key", "query"), "file": ("path",), "http_json": ("url",)}[kind]
        for k in need:
            if not src.get(k):
                raise ValueError(f"source.{k} is required for kind {kind!r}")
        extra = set(m) - MAPPING_KEYS
        if extra:
            raise ValueError(f"unknown mapping key(s): {sorted(extra)}")
        if not m.get("receipt_id"):
            raise ValueError("mapping.receipt_id is required: feedback rows must reference the receipt id the app carried through")
        if not (m.get("signal") or m.get("signal_value") or m.get("value")):
            raise ValueError("mapping needs signal, signal_value or value")
        if m.get("signal") and m.get("signal_value"):
            raise ValueError("mapping.signal and mapping.signal_value are mutually exclusive")
        sc = m.get("scale")
        if sc is not None and (not isinstance(sc, dict) or set(sc) != {"min", "max"} or not float(sc["min"]) < float(sc["max"])):
            raise ValueError("mapping.scale must be {min, max} with min < max")
        for k, v in (m.get("value_map") or {}).items():
            if isinstance(v, bool) or not (isinstance(v, str) or (isinstance(v, (int, float)) and -1 <= v <= 1)):
                raise ValueError(f"mapping.value_map[{k!r}] must be a signal name or a number in [-1, 1], got {v!r}")
        if self.on_bad_row not in ("skip", "fail") or self.on_unknown_receipt not in ("skip", "fail"):
            raise ValueError("on_bad_row / on_unknown_receipt must be 'skip' or 'fail'")
        if self.max_rows is not None and self.max_rows < 1:
            raise ValueError("max_rows must be >= 1")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        self._validate()
        src, mapping = dict(self.source), dict(self.mapping)
        store_key, on_bad, on_unknown, max_rows = self.store_resource_key, self.on_bad_row, self.on_unknown_receipt, self.max_rows
        initial_since = self.initial_since
        asset_key = dg.AssetKey.from_user_string(self.asset_name)
        tags = {**(self.asset_tags or {}), **{f"dagster/kind/{k}": "" for k in (self.kinds or ["python", "feedback"])}}
        required = {store_key} | ({src["resource_key"]} if src["kind"] == "sql" else set())

        freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy
            if self.freshness_cron:
                freshness_policy = FreshnessPolicy.cron(deadline_cron=self.freshness_cron,
                                                        lower_bound_delta=timedelta(minutes=self.freshness_max_lag_minutes))
            else:
                freshness_policy = FreshnessPolicy.time_window(fail_window=timedelta(minutes=self.freshness_max_lag_minutes))
        retry_policy = None
        if self.retry_policy_max_retries is not None:
            retry_policy = dg.RetryPolicy(max_retries=self.retry_policy_max_retries, delay=self.retry_policy_delay_seconds or 1,
                                          backoff=dg.Backoff[self.retry_policy_backoff.upper()])
        kw: Dict[str, Any] = {}
        if freshness_policy is not None:
            kw["freshness_policy"] = freshness_policy
        if retry_policy is not None:
            kw["retry_policy"] = retry_policy
        if self.deps:
            kw["deps"] = [dg.AssetKey.from_user_string(d) for d in self.deps]

        @dg.asset(key=asset_key, group_name=self.group_name, owners=self.owners or None, tags=tags, required_resource_keys=required,
                  description=self.description or "Feedback ingested as context-receipt outcomes (idempotent, incremental).", **kw)
        def _ingest(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            store = getattr(context.resources, store_key)
            prev_cursor_s: Optional[str] = None
            event = context.instance.get_latest_materialization_event(asset_key)
            if event is not None and event.asset_materialization is not None:
                md = event.asset_materialization.metadata.get("cursor")
                prev_cursor_s = getattr(md, "value", None) if md is not None else None
            cursor = _to_dt(prev_cursor_s) if prev_cursor_s else None
            since = cursor.isoformat() if cursor else initial_since

            kind = src["kind"]
            rows = _read_sql(src, since, context) if kind == "sql" else _read_file(src) if kind == "file" else _read_http(src, since, context)
            rep = ingest_feedback(store, rows, mapping, cursor=cursor, on_bad_row=on_bad, on_unknown_receipt=on_unknown, max_rows=max_rows)

            new_cursor = rep["cursor"] or prev_cursor_s or ""
            lag = None
            if new_cursor and _to_dt(new_cursor):
                lag = max(0.0, (datetime.now(timezone.utc) - _to_dt(new_cursor)).total_seconds())
            md: Dict[str, Any] = {
                "source_kind": kind,
                "rows_read": dg.MetadataValue.int(rep["rows_read"]),
                "rows_before_cursor": dg.MetadataValue.int(rep["rows_before_cursor"]),
                "outcomes_recorded": dg.MetadataValue.int(rep["outcomes_recorded"]),
                "superseded_in_run": dg.MetadataValue.int(rep["superseded_in_run"]),
                "skipped_rows": dg.MetadataValue.int(rep["skipped_rows"]),
                "unknown_receipts": dg.MetadataValue.int(len(rep["unknown"])),
                "unknown_receipt_examples": dg.MetadataValue.json(rep["unknown"][:5]),
                "bad_rows": dg.MetadataValue.int(rep["bad_total"]),
                "bad_row_examples": dg.MetadataValue.json(rep["bad"][:5]),
                "cursor": dg.MetadataValue.text(new_cursor),
            }
            if lag is not None:
                md["lag_seconds"] = dg.MetadataValue.float(lag)
            if rep["unknown"]:
                context.log.warning(f"{len(rep['unknown'])} unknown receipt id(s), e.g. {rep['unknown'][:5]}")
            if rep["bad_total"]:
                context.log.warning(f"{rep['bad_total']} bad row(s), e.g. {rep['bad'][:5]}")
            return dg.MaterializeResult(metadata=md)

        schedules = []
        if self.cron_schedule:
            schedules.append(dg.ScheduleDefinition(name=f"{self.asset_name}_schedule", cron_schedule=self.cron_schedule,
                                                   target=dg.AssetSelection.assets(asset_key)))
        return dg.Definitions(assets=[_ingest], schedules=schedules)
