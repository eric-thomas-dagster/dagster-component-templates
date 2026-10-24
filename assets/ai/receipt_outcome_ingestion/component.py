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
import contextlib
import email.utils
import glob as _glob
import hashlib
import inspect
import weakref
import http.client
import json
import math
import os
import random
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
                  "page_param", "max_pages", "timeout_seconds", "order"},
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
    if isinstance(v, int):
        return str(v)       # exact, and float(10**400) would raise OverflowError
    if isinstance(v, float):
        return str(int(v)) if math.isfinite(v) and v.is_integer() else str(v)
    s = str(v).strip().lower()
    try:
        f = float(s)
        return str(int(f)) if math.isfinite(f) and f.is_integer() else s
    except (ValueError, OverflowError):
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


# ── transient-failure retry (inlined per component; keep self-contained) ────────────────────────────────────────────

RETRYABLE_HTTP = (429, 500, 502, 503, 504)
_BACKOFF_BASE_SECONDS = 0.5
_sleep = time.sleep          # tests replace these two for deterministic, instant backoff
_rng = random.Random()


def _jitter_delay(attempt: int, max_backoff: float) -> float:
    """Exponential backoff with FULL jitter: uniform(0, min(max_backoff, base * 2**attempt)); ``attempt`` is 0 after the first failure."""
    return _rng.uniform(0.0, min(float(max_backoff), _BACKOFF_BASE_SECONDS * (2 ** attempt)))


def _parse_retry_after(value: Optional[str], now: Optional[datetime] = None) -> Optional[float]:
    """``Retry-After``: delta-seconds or an HTTP-date -> seconds to wait (>= 0); None when absent/unparseable."""
    if value is None or not str(value).strip():
        return None
    v = str(value).strip()
    try:
        secs = float(v)
        return max(0.0, secs) if math.isfinite(secs) else None
    except ValueError:
        pass
    try:
        when = email.utils.parsedate_to_datetime(v)
    except (TypeError, ValueError):
        return None
    if when is None:
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    return max(0.0, (when - (now or datetime.now(timezone.utc))).total_seconds())


class _Transient(Exception):
    """Raised by an attempt to say 'retry me'; carries the server's Retry-After seconds (if any) and a safe, secret-free description."""

    def __init__(self, what: str, retry_after: Optional[float] = None):
        super().__init__(what)
        self.what, self.retry_after = what, retry_after


def _retrying(attempt_fn, *, max_attempts: int, max_backoff: float, describe: str):
    """Call ``attempt_fn()`` up to ``max_attempts`` times, sleeping (full-jitter backoff, or Retry-After capped at ``max_backoff``) between
    tries when it raises _Transient. Anything else propagates immediately. Final failure: ValueError naming the attempt count."""
    last: Optional[_Transient] = None
    for n in range(max_attempts):
        try:
            return attempt_fn()
        except _Transient as exc:
            last = exc
            if n + 1 >= max_attempts:
                break
            delay = min(exc.retry_after, float(max_backoff)) if exc.retry_after is not None else _jitter_delay(n, max_backoff)
            _sleep(delay)
    raise ValueError(f"{describe} failed after {max_attempts} attempt(s): {last.what}") from None


_SQL_TRANSIENT_NAMES = {"OperationalError", "InterfaceError", "DisconnectionError", "ConnectionError", "ConnectionException",
                        "ConnectionResetError", "TimeoutError", "ReadTimeout", "ConnectTimeout"}


def _is_transient_sql_error(exc: BaseException) -> bool:
    """Connection-level failures only (by class name / OSError, so no driver import): a bad query (syntax, missing table/column) is not retried."""
    for e in (exc, getattr(exc, "orig", None), exc.__cause__):
        if e is None:
            continue
        if isinstance(e, OSError) or type(e).__name__ in _SQL_TRANSIENT_NAMES:
            return True
    return False


# ── sources ──────────────────────────────────────────────────────────────────────────────────────────────────────

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


def _read_sql(cfg: Dict[str, Any], since: str, context, retry: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """Run ``query`` (``{since}`` bound as an escaped string literal) through a Dagster resource exposing get_engine() (SQLAlchemy),
    get_connection() (DB-API context manager: DuckDB/Postgres/Snowflake) or get_client() (BigQuery-style .query().to_dataframe())."""
    sql = cfg["query"].replace("{since}", _sql_literal(since))
    resource = getattr(context.resources, cfg["resource_key"])
    retry = retry or {"max_attempts": 1, "max_backoff": 0}

    def attempt() -> "pd.DataFrame":
        try:
            return _run_sql(resource, sql, cfg)
        except Exception as exc:  # noqa: BLE001
            if _is_transient_sql_error(exc):
                raise _Transient(type(exc).__name__) from exc
            raise

    # a read-only SELECT is safe to re-run; connection blips get at most 3 tries however high retry_max_attempts is
    df = _retrying(attempt, max_attempts=min(3, retry["max_attempts"]), max_backoff=retry["max_backoff"], describe="sql source query")
    df = df.astype(object).where(df.notna(), None)
    return df.to_dict("records")


def _run_sql(resource: Any, sql: str, cfg: Dict[str, Any]):
    import pandas as pd

    if hasattr(resource, "get_engine"):
        return pd.read_sql(sql, resource.get_engine())
    elif hasattr(resource, "get_connection"):
        with resource.get_connection() as conn:  # a @contextmanager (dagster_duckdb.DuckDBResource): must be entered
            return pd.read_sql(sql, conn)
    elif inspect.isgenerator(resource) or hasattr(resource, "get_client") or hasattr(resource, "query"):
        with _vendor_client_session(resource) as client:
            if not hasattr(client, "query"):
                raise ValueError(f"resource {cfg['resource_key']!r}: get_client() returned {type(client).__name__} with no .query()")
            return client.query(sql).to_dataframe()
    else:
        raise ValueError(
            f"resource {cfg['resource_key']!r} ({type(resource).__name__}) must expose get_engine(), get_connection() or get_client()"
        )


def _records_at(data: Any, records_path: Optional[str], where: str) -> List[Dict[str, Any]]:
    node = _dotted(data, records_path) if records_path else data
    if node is None and records_path:
        raise ValueError(f"{where}: records_path {records_path!r} not found in the response")
    if isinstance(node, dict):
        node = [node]
    if not isinstance(node, list):
        raise ValueError(f"{where}: expected a list of records, got {type(node).__name__}")
    return [r for r in node if isinstance(r, dict)]


def _file_signature(p: str) -> Tuple[int, int]:
    st = os.stat(p)
    return st.st_mtime_ns, st.st_size


def _read_one_file(p: str, fmt: str, cfg: Dict[str, Any], bad: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if fmt in ("csv", "tsv"):
        with open(p, newline="", encoding="utf-8-sig") as f:
            rows.extend(dict(r) for r in csv.DictReader(f, delimiter="\t" if fmt == "tsv" else ","))
    elif fmt in ("jsonl", "ndjson"):
        with open(p, encoding="utf-8", errors="replace") as f:
            for n, line in enumerate(f, 1):
                if not line.strip():
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError as exc:   # a half-written trailing line, or corruption: a bad row, not a crash
                    bad.append({"row": f"{p}:{n}", "reason": f"invalid JSON line ({exc.msg})"})
                    continue
                if isinstance(obj, dict):
                    rows.append(obj)
                else:
                    bad.append({"row": f"{p}:{n}", "reason": f"JSON line is a {type(obj).__name__}, not an object"})
    elif fmt == "json":
        with open(p, encoding="utf-8") as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError as exc:
                raise ValueError(f"{p}: invalid JSON ({exc.msg} at line {exc.lineno})") from None
        rows.extend(_records_at(data, cfg.get("records_path"), p))
    elif fmt == "parquet":
        import pandas as pd
        df = pd.read_parquet(p)
        df = df.astype(object).where(df.notna(), None)
        rows.extend(df.to_dict("records"))
    else:
        raise ValueError(f"{p}: unsupported file format {fmt!r} (csv, tsv, json, jsonl, parquet)")
    return rows


def _read_file(cfg: Dict[str, Any], retry: Optional[Dict[str, Any]] = None) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """-> (rows, bad_lines). jsonl lines that do not parse are returned in ``bad_lines`` (counted as bad rows by the caller).
    A csv/json/parquet file whose (mtime, size) changed while it was being read (a writer mid-flight) is re-read after a backoff;
    jsonl is append-friendly and tolerates it (the partial last line is just a bad row until the writer finishes)."""
    retry = retry or {"max_attempts": 3, "max_backoff": 5}
    pattern = os.path.expanduser(cfg["path"])
    paths = sorted(_glob.glob(pattern, recursive=True)) if any(c in pattern for c in "*?[") else ([pattern] if os.path.exists(pattern) else [])
    paths = [p for p in paths if os.path.isfile(p)]
    if not paths and not cfg.get("allow_empty"):
        raise FileNotFoundError(f"source.path {cfg['path']!r} matched no files (set source.allow_empty: true to treat that as zero rows)")
    rows: List[Dict[str, Any]] = []
    bad: List[Dict[str, Any]] = []
    for p in paths:
        fmt = (cfg.get("format") or Path(p).suffix.lstrip(".")).lower()
        got: Dict[str, Any] = {}

        def attempt() -> None:
            before = _file_signature(p)
            local_bad: List[Dict[str, Any]] = []
            try:
                got["rows"] = _read_one_file(p, fmt, cfg, local_bad)
            except Exception as exc:  # noqa: BLE001
                if fmt != "jsonl" and _file_signature(p) != before:   # unreadable BECAUSE it is being rewritten: retry
                    raise _Transient("file changed while being read") from exc
                raise
            if fmt not in ("jsonl", "ndjson") and _file_signature(p) != before:
                raise _Transient("file changed while being read")
            got["bad"] = local_bad

        _retrying(attempt, max_attempts=retry["max_attempts"], max_backoff=retry["max_backoff"], describe=f"reading {p}")
        rows.extend(got["rows"])
        bad.extend(got["bad"])
    return rows, bad


def _resolve_headers(cfg: Dict[str, Any]) -> Dict[str, str]:
    headers = {str(k): str(v) for k, v in (cfg.get("headers") or {}).items()}
    for name, env in (cfg.get("headers_env") or {}).items():
        val = os.environ.get(str(env))
        if not val:
            raise ValueError(f"source.headers_env: environment variable {env!r} (for header {name!r}) is not set")
        headers[str(name)] = val  # secret: never logged, never put in metadata or error text
    return headers


def _origin(url: str) -> Tuple[str, str, int]:
    """(scheme, host, port) with default ports filled in, for same-origin comparisons."""
    p = urllib.parse.urlsplit(url)
    scheme = (p.scheme or "").lower()
    try:
        port = p.port
    except ValueError:
        port = None
    return scheme, (p.hostname or "").lower(), port or {"http": 80, "https": 443}.get(scheme, 0)


class _SameOriginRedirect(urllib.request.HTTPRedirectHandler):
    """urllib re-sends every custom header (our secret ``headers_env`` ones) to wherever a 3xx points. Refuse cross-origin redirects."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        if _origin(req.full_url) != _origin(urllib.parse.urljoin(req.full_url, newurl)):
            raise ValueError(f"refusing cross-origin redirect from {_origin(req.full_url)[1]!r} to "
                             f"{_origin(urllib.parse.urljoin(req.full_url, newurl))[1]!r}: configured headers are never sent to another host")
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def _http_get_json(url: str, headers: Dict[str, str], timeout: float, retry: Optional[Dict[str, Any]] = None) -> Any:
    """GET + parse JSON. Retries connection errors, timeouts and HTTP 429/500/502/503/504 (``Retry-After`` honoured, capped by the max
    backoff); every other 4xx fails at once. Error text names the status and the URL without its query string -- never headers/secrets."""
    retry = retry or {"max_attempts": 4, "max_backoff": 30}
    where = f"GET {url.split('?')[0]}"
    opener = urllib.request.build_opener(_SameOriginRedirect)

    def attempt() -> Any:
        req = urllib.request.Request(url, headers={"Accept": "application/json", **headers})
        try:
            with opener.open(req, timeout=timeout) as resp:
                body = resp.read()
        except urllib.error.HTTPError as exc:
            if exc.code in RETRYABLE_HTTP:
                raise _Transient(f"HTTP {exc.code}", _parse_retry_after(exc.headers.get("Retry-After") if exc.headers else None)) from None
            raise ValueError(f"{where} failed: HTTP {exc.code} (not retried)") from None
        except (urllib.error.URLError, OSError, http.client.HTTPException) as exc:   # refused/reset/DNS/timeout/truncated body
            reason = getattr(exc, "reason", exc)
            raise _Transient(f"{type(reason).__name__} (connection or timeout)") from None
        try:
            return json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError(f"{where}: response is not valid JSON ({type(exc).__name__}); first bytes: {body[:40]!r}") from None

    return _retrying(attempt, max_attempts=retry["max_attempts"], max_backoff=retry["max_backoff"], describe=where)


def _with_param(url: str, name: str, value: Any) -> str:
    parts = urllib.parse.urlsplit(url)
    q = [(k, v) for k, v in urllib.parse.parse_qsl(parts.query, keep_blank_values=True) if k != name]
    q.append((name, str(value)))
    return urllib.parse.urlunsplit(parts._replace(query=urllib.parse.urlencode(q)))


def _read_http(cfg: Dict[str, Any], since: str, context, retry: Optional[Dict[str, Any]] = None) -> Tuple[List[Dict[str, Any]], bool]:
    """-> (rows, truncated). ``truncated`` is True when ``max_pages`` stopped the read while more pages may exist."""
    headers = _resolve_headers(cfg)
    timeout = float(cfg.get("timeout_seconds", 30))
    max_pages = int(cfg.get("max_pages", 50))
    url = cfg["url"]
    origin = _origin(url)
    truncated = False
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
        data = _http_get_json(url, headers, timeout, retry)
        recs = _records_at(data, cfg.get("records_path"), "http_json")
        rows.extend(recs)
        nxt = _dotted(data, cfg["next_url_path"]) if cfg.get("next_url_path") else None
        if cfg.get("next_url_path"):
            if not nxt:
                break
            nxt_url = urllib.parse.urljoin(url, str(nxt))
            if _origin(nxt_url) != origin:
                raise ValueError(f"http_json: next_url_path returned a URL on host {_origin(nxt_url)[1]!r}, not {origin[1]!r}; "
                                 f"refusing to follow it (configured headers are never sent to another host)")
            url = nxt_url
        elif cfg.get("page_param"):
            if not recs:
                break
            page += 1
            url = _with_param(url, cfg["page_param"], page)
        else:
            break
    else:
        truncated = True
        if cfg.get("order", "asc") == "desc":
            context.log.warning(f"http_json: stopped at max_pages={max_pages} on a newest-first (order: desc) API; unread records are OLDER than "
                                f"what was read, so the cursor only advances once a run reaches the previous cursor. Raise max_pages so a "
                                f"run can reach it.")
        else:
            context.log.warning(f"http_json: stopped at max_pages={max_pages}; more (newer) pages may exist and are picked up next run via the cursor")
    return rows, truncated


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
        except (TypeError, ValueError, OverflowError):
            raise _Bad(f"non-numeric or out-of-range value {str(x)[:40]!r}") from None
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
                    on_unknown_receipt: str, max_rows: Optional[int], read_truncated: bool = False, order: str = "asc",
                    unknown_receipt_grace_hours: float = 24.0, now: Optional[datetime] = None,
                    read_bad: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
    """Pure-ish core (store + rows in, report out): validate every row first, then write. Returns counts, samples and the new cursor.

    Cursor safety (every unread / not-yet-resolvable record must stay at or ahead of the new cursor; reads are inclusive):
      * ``max_rows`` cuts oldest-first but never inside a timestamp tie group, and always makes progress past the previous cursor;
      * ``read_truncated`` + ``order == "desc"``: unread records are OLDER than what was read, so the cursor does not advance unless
        the read reached the previous cursor;
      * an unknown-receipt row younger than ``unknown_receipt_grace_hours`` holds the cursor just before it so it is retried."""
    now = now or datetime.now(timezone.utc)
    label = mapping.get("source") or "feedback"
    vmap = {_norm_key(k): v for k, v in (mapping.get("value_map") or {}).items()}
    skip = {_norm_key(x) for x in (mapping.get("skip_values") or [])}
    ts_col = mapping.get("occurred_at")
    report: Dict[str, Any] = {"rows_read": len(rows), "rows_before_cursor": 0, "skipped_rows": 0, "bad": [], "unknown": [], "outcomes_recorded": 0,
                              "superseded_in_run": 0, "bad_total": 0, "cursor": None, "unknown_pending": [], "truncated": False,
                              "cursor_held_reason": None}

    prepared: List[Tuple[Optional[datetime], int, Dict[str, Any]]] = []
    min_read: Optional[datetime] = None
    bad = report["bad"]
    for b in (read_bad or []):   # lines the reader could not parse (corrupt / half-written jsonl)
        report["bad_total"] += 1
        bad.append(b)
    for i, row in enumerate(rows):
        dt = None
        if ts_col:
            dt = _to_dt(_get(row, ts_col))
            if dt is None:
                report["bad_total"] += 1
                bad.append({"row": i, "reason": f"missing/unparseable occurred_at ({ts_col!r}): {_get(row, ts_col)!r}"})
                continue
            if min_read is None or dt < min_read:
                min_read = dt
            if cursor is not None and dt < cursor:
                report["rows_before_cursor"] += 1
                continue
        prepared.append((dt, i, row))
    prepared.sort(key=lambda t: (t[0] is None, t[0] or datetime.min.replace(tzinfo=timezone.utc), t[1]))  # oldest first: last writer wins on a ref
    if max_rows is not None and len(prepared) > max_rows:
        end = max_rows
        while True:
            while end < len(prepared) and prepared[end][0] is not None and prepared[end][0] == prepared[end - 1][0]:
                end += 1  # never cut inside a timestamp tie group
            if cursor is not None and end < len(prepared) and prepared[end - 1][0] == cursor:
                end += 1  # the batch is all re-read boundary rows: take the next group too, or the cursor could never move
                continue
            break
        report["truncated"] = end < len(prepared)
        prepared = prepared[:end]

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
    grace = timedelta(hours=max(0.0, float(unknown_receipt_grace_hours)))
    hold: Optional[datetime] = None  # earliest young unknown-receipt row: the cursor must stay just before it
    pending: Dict[str, None] = {}
    for dt, rid, sig, val, ref in plan:
        is_unknown = rid in unknown
        if not is_unknown:
            try:
                store.record_outcome(rid, signal=sig, value=val, source=label, outcome_ref=ref)
            except KeyError:
                unknown[rid] = None
                pending_unknown_fail = True
                is_unknown = True
            else:
                report["outcomes_recorded"] += 1
                if (rid, ref) in seen:
                    report["superseded_in_run"] += 1
                seen.add((rid, ref))
        if is_unknown and dt is not None and now - dt < grace:
            pending[rid] = None
            if hold is None or dt < hold:
                hold = dt
        if dt is not None and (newest is None or dt > newest):
            newest = dt
    report["unknown"] = list(unknown)
    report["unknown_pending"] = list(pending)
    if pending_unknown_fail and on_unknown_receipt == "fail":
        raise ValueError(f"{len(unknown)} feedback row(s) reference unknown receipt ids, e.g. {list(unknown)[:5]} "
                         f"(known ones were recorded; the cursor was not advanced)")
    # Bad-timestamp rows never move the cursor. Unknown-receipt rows older than the grace window are dropped (cursor passes them);
    # younger ones hold the cursor back so they are retried (re-recording is safe: outcome_ref idempotency).
    if read_truncated and order == "desc" and (cursor is None or min_read is None or min_read > cursor):
        newest = cursor  # unread records are older than everything read: advancing would skip them forever
        report["cursor_held_reason"] = "desc_truncated_before_previous_cursor"
    if hold is not None:
        held = hold - timedelta(microseconds=1)
        if newest is None or held < newest:
            newest = max(held, cursor) if cursor is not None else held
            if report["cursor_held_reason"] is None:
                report["cursor_held_reason"] = "unknown_receipt_within_grace"
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
            "since_format? iso|epoch, next_url_path? (must stay on the same host), page_param?, max_pages?, order? asc|desc (default asc; desc = newest-first API)}."
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
    unknown_receipt_grace_hours: float = Field(default=24.0, description="A feedback row whose receipt id is unknown but whose occurred_at is younger than this is retried (the cursor is held just before it; counted in unknown_receipts_pending) because the receipt may not be durable yet (async writer, multi-host). Older unknown-receipt rows are dropped and the cursor moves past them. 0 disables the retry.")
    retry_max_attempts: int = Field(default=4, description="Tries per source read (HTTP page / file / SQL) on transient failures: connection errors, timeouts, HTTP 429/500/502/503/504 (exponential backoff with full jitter; Retry-After honoured). SQL reads use at most 3. Other 4xx are never retried. 1 disables retrying.")
    retry_max_backoff_seconds: float = Field(default=30.0, description="Upper bound on any single wait between retries, including a server's Retry-After.")
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
        if kind == "http_json" and src.get("order", "asc") not in ("asc", "desc"):
            raise ValueError("source.order must be 'asc' or 'desc'")
        if self.unknown_receipt_grace_hours < 0:
            raise ValueError("unknown_receipt_grace_hours must be >= 0")
        if isinstance(self.retry_max_attempts, bool) or not 1 <= self.retry_max_attempts <= 20:
            raise ValueError("retry_max_attempts must be an integer between 1 and 20")
        if not 0 < self.retry_max_backoff_seconds <= 3600:
            raise ValueError("retry_max_backoff_seconds must be > 0 and <= 3600")
        if kind == "http_json" and "timeout_seconds" in src:
            ts = src["timeout_seconds"]
            if isinstance(ts, bool) or not isinstance(ts, (int, float)) or not 0 < ts <= 3600:
                raise ValueError("source.timeout_seconds must be a number > 0 and <= 3600")
        if self.max_rows is not None and self.max_rows < 1:
            raise ValueError("max_rows must be >= 1")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        self._validate()
        src, mapping = dict(self.source), dict(self.mapping)
        store_key, on_bad, on_unknown, max_rows = self.store_resource_key, self.on_bad_row, self.on_unknown_receipt, self.max_rows
        initial_since = self.initial_since
        retry_cfg = {"max_attempts": self.retry_max_attempts, "max_backoff": self.retry_max_backoff_seconds}
        grace_hours = self.unknown_receipt_grace_hours
        asset_key = dg.AssetKey.from_user_string(self.asset_name)
        tags = {**(self.asset_tags or {}), **{f"dagster/kind/{k}": "" for k in (self.kinds or ["python", "feedback"])}}
        tags.setdefault("insights/metrics", "receipt_outcome_ingestion")  # opt-in for insights_custom_metrics_sync `presets: auto`
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

        def _ingest_body(context: dg.AssetExecutionContext, fs: Dict[str, str]) -> dg.MaterializeResult:
            fs["step"] = "read_source"
            store = getattr(context.resources, store_key)
            prev_cursor_s: Optional[str] = None
            event = context.instance.get_latest_materialization_event(asset_key)
            if event is not None and event.asset_materialization is not None:
                md = event.asset_materialization.metadata.get("cursor")
                prev_cursor_s = getattr(md, "value", None) if md is not None else None
            cursor = _to_dt(prev_cursor_s) if prev_cursor_s else None
            since = cursor.isoformat() if cursor else initial_since

            kind = src["kind"]
            truncated = False
            read_bad: List[Dict[str, Any]] = []
            # The whole read finishes BEFORE anything is recorded, and the cursor lives in this run's materialization: a read that fails
            # after its retries (e.g. page 2 of 3) raises here, so nothing is recorded and no new cursor is written -- the next run
            # starts again from the old cursor (re-reading is safe: outcome_ref idempotency).
            fs["step"] = f"read_{kind}"
            if kind == "http_json":
                rows, truncated = _read_http(src, since, context, retry_cfg)
            elif kind == "sql":
                rows = _read_sql(src, since, context, retry_cfg)
            else:
                rows, read_bad = _read_file(src, retry_cfg)
            fs["step"] = "ingest_feedback"
            rep = ingest_feedback(store, rows, mapping, cursor=cursor, on_bad_row=on_bad, on_unknown_receipt=on_unknown, max_rows=max_rows,
                                  read_truncated=truncated, order=src.get("order", "asc"), unknown_receipt_grace_hours=grace_hours,
                                  read_bad=read_bad)

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
                "unknown_receipts_pending": dg.MetadataValue.int(len(rep["unknown_pending"])),
                "read_truncated": truncated,
                "cursor_held_reason": rep["cursor_held_reason"] or "",
                "unknown_receipt_examples": dg.MetadataValue.json(rep["unknown"][:5]),
                "bad_rows": dg.MetadataValue.int(rep["bad_total"]),
                "bad_row_examples": dg.MetadataValue.json(rep["bad"][:5]),
                "cursor": dg.MetadataValue.text(new_cursor),
            }
            if lag is not None:
                md["lag_seconds"] = dg.MetadataValue.float(lag)
            if rep["unknown"]:
                context.log.warning(f"{len(rep['unknown'])} unknown receipt id(s), e.g. {rep['unknown'][:5]}")
            if rep["unknown_pending"]:
                context.log.warning(f"{len(rep['unknown_pending'])} unknown receipt id(s) are younger than unknown_receipt_grace_hours="
                                    f"{grace_hours:g}; the cursor is held back so they are retried next run")
            if rep["cursor_held_reason"] == "desc_truncated_before_previous_cursor":
                context.log.warning("cursor NOT advanced: the newest-first read stopped (max_pages) before reaching the previous cursor; "
                                    "raise source.max_pages so a run can reach it")
            if rep["bad_total"]:
                context.log.warning(f"{rep['bad_total']} bad row(s), e.g. {rep['bad'][:5]}")
            return dg.MaterializeResult(metadata=md)

        @dg.asset(key=asset_key, group_name=self.group_name, owners=self.owners or None, tags=tags, required_resource_keys=required,
                  description=self.description or "Feedback ingested as context-receipt outcomes (idempotent, incremental).", **kw)
        def _ingest(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            fs: Dict[str, str] = {"step": "read_source"}
            try:
                return _ingest_body(context, fs)
            except Exception as exc:
                _fo_emit(context, exc, counter=_FO_COUNTER, asset_keys=[asset_key], failed_step=fs["step"])
                raise

        schedules = []
        if self.cron_schedule:
            schedules.append(dg.ScheduleDefinition(name=f"{self.asset_name}_schedule", cron_schedule=self.cron_schedule,
                                                   target=dg.AssetSelection.assets(asset_key)))
        return dg.Definitions(assets=[_ingest], schedules=schedules)


# BEGIN failure-observation contract (inlined per component; keep copies identical -- see tests/test_failure_observation_contract.py)
# Open-source Dagster never emits ASSET_FAILED_TO_MATERIALIZE when an asset step fails, so a failed asset is visible only as a failed
# RUN. This contract logs an AssetObservation per asset the failing step was producing (metadata: failure_status, failure_reason,
# error_class, error_message [redacted, <=500 chars], will_retry, failed_step, run_id, plus the component's numeric `<prefix>_failures`)
# BEFORE the original exception is re-raised untouched. Emission can never mask the failure: it has its own try/except.
import re as _fo_re

_FO_REASONS = ("FAILED_TO_MATERIALIZE", "POLICY_VIOLATION", "BUDGET_EXCEEDED", "APPROVAL_REJECTED", "UPSTREAM_ERROR", "UNKNOWN")
_FO_URL_CRED = _fo_re.compile(r"([A-Za-z][A-Za-z0-9+.\-]*://)[^/\s:@]+:[^/\s@]+@")
_FO_AUTH = _fo_re.compile(r"(?i)(authorization[\"']?\s*[:=]\s*[\"']?)(?:(bearer|basic|token)\s+)?[^\s\"',;]+")
_FO_BEARER = _fo_re.compile(r"(?i)\b(bearer|basic)\s+[A-Za-z0-9._~+/=\-]{8,}")
_FO_KV = _fo_re.compile(
    r"(?i)\b([\w.\-]*(?:password|passwd|pwd|secret|token|api[_\-]?key|apikey|access[_\-]?key|private[_\-]?key|credential|auth[_\-]?key)[\w.\-]*"
    r"[\"']?\s*[:=]\s*[\"']?)[^\s\"',;&]+")
_FO_TOKENS = _fo_re.compile(
    r"\b(?:sk-[A-Za-z0-9_\-]{16,}|gh[pousr]_[A-Za-z0-9]{20,}|xox[abprs]-[A-Za-z0-9\-]{10,}|AKIA[0-9A-Z]{16}|AIza[0-9A-Za-z_\-]{30,}"
    r"|eyJ[A-Za-z0-9_\-]{8,}\.[A-Za-z0-9_\-]{8,}\.[A-Za-z0-9_\-]*)")
_FO_BLOB = _fo_re.compile(r"[A-Za-z0-9+/_=\-]{40,}")


def _fo_blob(m) -> str:
    s = m.group(0)
    # a long unbroken run with a digit and a letter, and not mostly path separators, is a key / base64 / hash, not prose or a path
    if any(c.isdigit() for c in s) and any(c.isalpha() for c in s) and s.count("/") / len(s) < 0.05:
        return "[REDACTED]"
    return s


def _fo_redact(text: Any, limit: int = 500) -> str:
    """Strip credentials from an error message, THEN truncate (so a secret cut in half can never survive)."""
    s = str(text)[:5000]
    s = _FO_URL_CRED.sub(r"\1[REDACTED]@", s)
    s = _FO_AUTH.sub(lambda m: m.group(1) + ((m.group(2) + " ") if m.group(2) else "") + "[REDACTED]", s)
    s = _FO_BEARER.sub(lambda m: m.group(1) + " [REDACTED]", s)
    s = _FO_KV.sub(lambda m: m.group(1) + "[REDACTED]", s)
    s = _FO_TOKENS.sub("[REDACTED]", s)
    s = _FO_BLOB.sub(_fo_blob, s)
    return s[:limit]


def _fo_reason(exc: BaseException) -> str:
    """Map an exception to a Dagster-AssetMaterializationFailureReason-style name."""
    try:
        import dagster as dg
        if isinstance(exc, dg.Failure):
            for k, v in (exc.metadata or {}).items():
                if k == "failure_reason" and str(getattr(v, "value", v)) in _FO_REASONS:
                    return str(getattr(v, "value", v))
        names = {c.__name__ for c in type(exc).__mro__}
        if "ApprovalRejected" in names:
            return "APPROVAL_REJECTED"
        if "PolicyViolation" in names:
            return "BUDGET_EXCEEDED" if _fo_re.match(r"\s*\[policy:(budget|breaker)\]", str(exc)) else "POLICY_VIOLATION"
        if "BudgetExceeded" in names:
            return "BUDGET_EXCEEDED"
        if "UpstreamError" in names:
            return "UPSTREAM_ERROR"
        return "FAILED_TO_MATERIALIZE"
    except Exception:
        return "UNKNOWN"


def _fo_will_retry(context: Any, exc: BaseException) -> Optional[str]:
    """'true'/'false' when the run's retry policy makes it knowable (op RetryPolicy, else run-retry tags), else None."""
    try:
        import dagster as dg
        if isinstance(exc, dg.Failure) and exc.allow_retries is False:
            return "false"
        oc = getattr(context, "op_execution_context", context)
        pol = oc.op_def.retry_policy
        if pol is not None:
            return "true" if oc.retry_number < pol.max_retries else "false"
        tags = oc.run.tags or {}
        if tags.get("dagster/max_retries") is not None:
            return "true" if int(tags.get("dagster/retry_number", 0)) < int(tags["dagster/max_retries"]) else "false"
    except Exception:
        pass
    return None


def _fo_partitions(context: Any) -> list:
    try:
        if context.has_partition_key:
            return [context.partition_key]
        if context.has_partition_key_range:
            ks = list(context.partition_keys)
            return ks if 0 < len(ks) <= 100 else [None]
    except Exception:
        pass
    return [None]


def _fo_emit(context: Any, exc: BaseException, *, counter: Dict[str, Any], asset_keys: Any, failed_step: Any = None,
             reason: Optional[str] = None, skip: Any = ()) -> None:
    """Log the failure AssetObservation(s). NEVER raises: a problem here is a warning, the caller re-raises the original.
    `asset_keys`: list or callable; `failed_step`: str, {"step": id} or callable; `skip`: asset keys already materialized
    (the marker "*" = all of them)."""
    try:
        import dagster as dg
        if isinstance(exc, dg.RetryRequested):
            return  # a requested retry is not a failure
        if "*" in skip:
            return
        keys = [k for k in (asset_keys() if callable(asset_keys) else asset_keys) if k not in skip]
        failed_step = failed_step() if callable(failed_step) else (failed_step.get("step") if isinstance(failed_step, dict) else failed_step)
        msg = getattr(exc, "description", None) if isinstance(exc, dg.Failure) else None
        md: Dict[str, Any] = {
            "failure_status": dg.MetadataValue.text("failed"),
            "failure_reason": dg.MetadataValue.text(reason or _fo_reason(exc)),
            "error_class": dg.MetadataValue.text(type(exc).__name__),
            "error_message": dg.MetadataValue.text(_fo_redact(msg or str(exc) or type(exc).__name__)),
        }
        wr = _fo_will_retry(context, exc)
        if wr is not None:
            md["will_retry"] = dg.MetadataValue.text(wr)
        if failed_step:
            md["failed_step"] = dg.MetadataValue.text(str(failed_step))
        try:
            md["run_id"] = dg.MetadataValue.text(str(context.run.run_id))
        except Exception:
            pass
        md.update(counter)
        parts = _fo_partitions(context)
        for key in keys:
            for p in parts:
                try:
                    context.log_event(dg.AssetObservation(asset_key=key, partition=p, metadata=md))
                except Exception as e:  # noqa: BLE001
                    context.log.warning(f"failure observation for {key} could not be logged: {e!r}")
    except Exception as e:  # noqa: BLE001
        try:
            context.log.warning(f"failure observation could not be emitted: {e!r}")
        except Exception:
            pass


def _fo_gen(context: Any, gen: Any, *, counter: Dict[str, Any], asset_keys: Any, output_key: Any = None, failed_step: Any = None):
    """Pass a generator asset body's events through; on an exception log the failure observation for every asset that had not
    been yielded yet, then re-raise the ORIGINAL exception (bare `raise`). `output_key(name)` maps a yielded Output's name to its
    asset key (default: any Output/MaterializeResult marks every key done); `failed_step` is a dict {"step": id} or a callable."""
    done: set = set()
    try:
        for ev in gen:
            name = getattr(ev, "output_name", None)
            if name is not None or type(ev).__name__ == "MaterializeResult":
                k = getattr(ev, "asset_key", None) or (output_key(name) if output_key else None)
                done.add(k if k is not None else "*")
            yield ev
    except Exception as exc:
        _fo_emit(context, exc, counter=counter, asset_keys=asset_keys, failed_step=failed_step, skip=done)
        raise
# END failure-observation contract

# Insights counter for this component (a numeric MetadataValue, so Insights can chart failures).
_FO_COUNTER = {"outcome_ingestion_failures": dg.MetadataValue.int(1)}
