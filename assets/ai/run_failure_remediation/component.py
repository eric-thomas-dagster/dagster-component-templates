"""RunFailureRemediationComponent -- self-healing runs with a safety gate.

Two Dagster sensors (and a dynamic-partitions definition) close the loop from a failed run to an approved fix:

1. `<prefix>_failure_sensor` (a real `run_failure_sensor`) gathers EVIDENCE for every failed run -- run id, job, failed steps, error
   class + message tail, the assets that were planned but not materialized, the latest materialization of their upstream assets,
   the job's recent run history and the outcomes of earlier remediations -- redacts secrets, writes it to
   `<evidence_dir>/<run_id>.json`, registers a dynamic partition named `<run_id>` and requests a run of the TRIAGE job for that
   partition (`run_key=run_id`, so a re-tick never double-launches). Rate limit, per-(job, error signature) cooldown, ignore list and
   no-recursion guards keep a flapping job from spawning a triage per failure.
2. `<prefix>_approval_sensor` watches `<proposals_dir>` for the proposal the triage pipeline writes
   (`{run_id, summary, root_cause, confidence, actions: [{kind, params, reason}]}`), turns each into a pending approval record in
   `<approval_dir>/pending/<run_id>.json` (the format agentic_pipeline's `approve` op writes and agent_review_queue lists), and --
   only when a token bound to that exact proposal's fingerprint is present -- executes the approved actions from an ALLOW-LIST,
   recording the result in `<results_dir>/<run_id>.json`. After `outcome_check_after_minutes` it records whether the retried / rerun
   runs succeeded (`outcome: fixed | not_fixed`), which feeds `recent_outcomes` in the next triage's evidence.

Nothing executes without a valid approval unless `auto_approve` is explicitly configured.
"""

import base64  # noqa: F401  (kept for readers: long base64 blobs are one of the redaction classes)
import hashlib
import json
import os
import re
import threading
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

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


# ── constants ────────────────────────────────────────────────────────────────

KNOWN_ACTIONS = ("retry_run", "rerun_partition", "materialize_assets", "open_ticket", "notify")
DEFAULT_ALLOWED_ACTIONS = ["retry_run", "rerun_partition", "open_ticket"]

TAG_PROPOSAL = "dagster_remediation/proposal"  # the failed run's id a remediation run belongs to
TAG_KIND = "dagster_remediation/kind"
TAG_RUN_KEY = "dagster_remediation/run_key"
TAG_TRIAGE = "dagster_remediation/triage"
TAG_FAILED_RUN = "dagster_remediation/failed_run_id"

_TERMINAL = ("SUCCESS", "FAILURE", "CANCELED")


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(t: Optional[datetime] = None) -> str:
    return (t or _now()).isoformat()


def _parse_ts(value: Any) -> Optional[datetime]:
    if not value:
        return None
    try:
        t = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return t if t.tzinfo else t.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _clip(s: Any, n: int) -> str:
    s = "" if s is None else str(s)
    return s if len(s) <= n else s[: n - 1] + "…"


def _tail(s: Any, n: int) -> str:
    s = "" if s is None else str(s)
    return s if len(s) <= n else "…" + s[-(n - 1):]


# ── redaction ────────────────────────────────────────────────────────────────

REDACTED = "[REDACTED]"
_SECRET_WORDS = r"(?:password|passwd|pwd|secret|token|api[_-]?key|apikey|access[_-]?key|private[_-]?key|credential|auth)"
_SECRET_KEY_RE = re.compile(_SECRET_WORDS, re.I)
_ENV_SECRET_NAME_RE = re.compile(r"(KEY|TOKEN|SECRET|PASSWORD|PASSWD|CREDENTIAL)", re.I)
_UUID_RE = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")

_PEM_RE = re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----.*?(?:-----END [A-Z ]*PRIVATE KEY-----|$)", re.S)
_AUTH_HEADER_RE = re.compile(r"(?i)\b(authorization|proxy-authorization|x-api-key|x-auth-token)(\s*[\"']?\s*[:=]\s*[\"']?)(?:(?:bearer|basic|token|digest)\s+)?[^\s\"',;]+")
_BEARER_RE = re.compile(r"(?i)\b(bearer|basic)\s+[A-Za-z0-9._~+/=-]{8,}")
_KV_RE = re.compile(
    r"(?i)\b([\w.-]*" + _SECRET_WORDS + r"[\w.-]*)([\"']?\s*[=:]\s*)(\"[^\"]*\"|'[^']*'|[^\s,;&}\]\"']+)"
)
_URL_CRED_RE = re.compile(r"([A-Za-z][A-Za-z0-9+.-]*://)([^\s/:@]+):([^\s/@]+)@")
_PREFIX_RE = re.compile(
    r"\b(sk-ant-[A-Za-z0-9_-]{12,}|sk-[A-Za-z0-9_-]{16,}|ghp_[A-Za-z0-9]{20,}|gho_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,}"
    r"|xox[abprs]-[A-Za-z0-9-]{10,}|AKIA[0-9A-Z]{16}|ASIA[0-9A-Z]{16}|AIza[0-9A-Za-z_-]{30,}"
    r"|eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,})"
)
_BLOB_RE = re.compile(r"[A-Za-z0-9+/_=-]{32,}")


def _blob_sub(m: "re.Match[str]") -> str:
    s = m.group(0)
    if _UUID_RE.match(s) or s.startswith("/") or s.count("/") >= 3:
        return s  # a run id / a filesystem path, not a credential
    if not (re.search(r"[0-9]", s) and re.search(r"[A-Za-z]", s)):
        return s  # all letters (a long identifier) or all digits
    return REDACTED


def _env_secret_values() -> List[str]:
    vals = []
    for k, v in os.environ.items():
        if v and len(v) >= 8 and _ENV_SECRET_NAME_RE.search(k):
            vals.append(v)
    return sorted(set(vals), key=len, reverse=True)


def redact_text(text: str, extra_patterns: Optional[List[str]] = None, env_values: Optional[List[str]] = None) -> str:
    """Remove credentials from free text: private keys, Authorization/Bearer headers, `password=...`-style pairs, URL userinfo,
    well-known token prefixes (sk-, ghp_, AKIA, xox*, JWTs), long base64-looking blobs, any user pattern, and the literal value of
    every secret-looking environment variable."""
    if not isinstance(text, str) or not text:
        return text
    t = text
    for v in (env_values if env_values is not None else _env_secret_values()):
        t = t.replace(v, REDACTED)
    t = _PEM_RE.sub(REDACTED, t)
    t = _AUTH_HEADER_RE.sub(lambda m: f"{m.group(1)}{m.group(2)}{REDACTED}", t)
    t = _BEARER_RE.sub(lambda m: f"{m.group(1)} {REDACTED}", t)
    t = _URL_CRED_RE.sub(lambda m: f"{m.group(1)}{m.group(2)}:{REDACTED}@", t)
    t = _KV_RE.sub(lambda m: f"{m.group(1)}{m.group(2)}{REDACTED}", t)
    t = _PREFIX_RE.sub(REDACTED, t)
    for p in extra_patterns or []:
        t = re.sub(p, REDACTED, t)
    t = _BLOB_RE.sub(_blob_sub, t)
    return t


def redact_obj(obj: Any, extra_patterns: Optional[List[str]] = None, env_values: Optional[List[str]] = None,
               skip_keys: Tuple[str, ...] = ()) -> Any:
    """Recursively redact strings; a value under a secret-looking KEY is replaced wholesale. `skip_keys` names structural fields
    (ids, fingerprints, data versions) that are never credentials and must stay usable."""
    ev = env_values if env_values is not None else _env_secret_values()
    if isinstance(obj, str):
        return redact_text(obj, extra_patterns, ev)
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            ks = str(k)
            if ks in skip_keys:
                out[k] = v
            elif _SECRET_KEY_RE.search(ks) and isinstance(v, (str, int, float)) and not isinstance(v, bool):
                out[k] = REDACTED
            else:
                out[k] = redact_obj(v, extra_patterns, ev, skip_keys)
        return out
    if isinstance(obj, (list, tuple)):
        return [redact_obj(v, extra_patterns, ev, skip_keys) for v in obj]
    return obj


_SKIP_KEYS = ("run_id", "failed_run_id", "error_signature", "fingerprint", "data_version", "asset_key", "key", "partition",
              "root_run_id", "parent_run_id", "outcome_ref", "run_ids", "new_run_ids", "run_key", "run_keys",
              "proposal_fingerprint")


# ── storage (local dir or any fsspec URI) ────────────────────────────────────

class _Store:
    """Local directory or an fsspec URI (s3://, gs://, abfs://, memory://). JSON documents by relative name."""

    def __init__(self, root: str):
        self.is_uri = "://" in root
        if self.is_uri:
            import fsspec
            self.fs, self.root = fsspec.core.url_to_fs(root)
            self.root = self.root.rstrip("/")
        else:
            self.fs, self.root = None, str(Path(root).expanduser().resolve())
        self.display = root.rstrip("/")

    def path(self, *parts: str) -> str:
        return "/".join([self.root, *parts]) if self.is_uri else os.path.join(self.root, *parts)

    def shown(self, *parts: str) -> str:
        return "/".join([self.display, *parts]) if self.is_uri else self.path(*parts)

    def exists(self, *parts: str) -> bool:
        p = self.path(*parts)
        return self.fs.exists(p) if self.is_uri else os.path.exists(p)

    def read_text(self, *parts: str) -> str:
        p = self.path(*parts)
        if self.is_uri:
            with self.fs.open(p, "r") as f:
                return f.read()
        with open(p) as f:
            return f.read()

    def read_json(self, *parts: str) -> Optional[Any]:
        try:
            return json.loads(self.read_text(*parts))
        except (OSError, ValueError):
            return None

    def write_json(self, parts: Tuple[str, ...], obj: Any) -> None:
        text = json.dumps(obj, indent=2, default=str, ensure_ascii=False)
        p = self.path(*parts)
        if self.is_uri:
            self.fs.makedirs(p.rsplit("/", 1)[0], exist_ok=True)
            with self.fs.open(p, "w") as f:
                f.write(text)
            return
        os.makedirs(os.path.dirname(p), exist_ok=True)
        tmp = f"{p}.{uuid.uuid4().hex[:8]}.tmp"
        with open(tmp, "w") as f:
            f.write(text)
        os.replace(tmp, p)  # a reader never sees a half-written document

    def create_exclusive(self, parts: Tuple[str, ...], text: str) -> bool:
        """Create the file only if it does not exist (True), else False. Local: temp file + atomic `os.link` (nobody ever sees an
        empty file); fsspec: exclusive-create (`xb`) on backends that honour it."""
        p = self.path(*parts)
        if self.is_uri:
            self.fs.makedirs(p.rsplit("/", 1)[0], exist_ok=True)
            try:
                with self.fs.open(p, "xb") as f:
                    f.write(text.encode("utf-8"))
                return True
            except FileExistsError:
                return False
        os.makedirs(os.path.dirname(p), exist_ok=True)
        tmp = f"{p}.{uuid.uuid4().hex[:8]}.tmp"
        with open(tmp, "w") as f:
            f.write(text)
        try:
            os.link(tmp, p)
            return True
        except FileExistsError:
            return False
        finally:
            os.remove(tmp)

    def list_names(self, *parts: str) -> List[str]:
        """File names (not paths) directly under the directory; [] when it does not exist."""
        d = self.path(*parts)
        try:
            if self.is_uri:
                return [x.rstrip("/").rsplit("/", 1)[-1] for x in self.fs.ls(d, detail=False)]
            return os.listdir(d)
        except (OSError, FileNotFoundError):
            return []

    def remove(self, *parts: str) -> None:
        p = self.path(*parts)
        try:
            self.fs.rm(p) if self.is_uri else os.remove(p)
        except (OSError, FileNotFoundError):
            pass

    def list_json(self, *parts: str) -> List[Tuple[str, float]]:
        """[(stem, mtime)] for *.json directly under the directory, sorted by name."""
        d = self.path(*parts) if parts else self.root
        out: List[Tuple[str, float]] = []
        if self.is_uri:
            if not self.fs.exists(d):
                return []
            for f in sorted(self.fs.glob(f"{d}/*.json")):
                try:
                    m = self.fs.modified(f).timestamp()
                except Exception:
                    m = 0.0
                out.append((f.rsplit("/", 1)[-1][:-5], m))
            return out
        p = Path(d)
        if not p.exists():
            return []
        for f in sorted(p.glob("*.json")):
            out.append((f.stem, f.stat().st_mtime))
        return out


# ── rate-limit / cooldown state: append-only, one small file per event ─────────
#
# Layout under <evidence_dir>/_state/ratelimit/:   <epoch_ms:013d>-<run_id>-<kind>.json     kind: triage | cooldown | suppressed
#
# The old design was ONE json document read-modified-written by every sensor tick, so two ticks at the same instant (detect: both,
# or two daemon threads) could lose an entry. Here every event is its own file written once with exclusive-create, so there is no
# shared document to lose an update to; a count is just a listing. A claimant writes its events, re-lists, and keeps its claim only
# if it ranks inside the limit (events sorted by file name = time, then run id); a loser withdraws its files. Inside one process
# (all sensors of a code location, any number of threads) the claim is additionally serialized by a per-directory lock, which makes
# the limit exact there. Across processes the write-then-verify ranking is what holds the limit; its one theoretical gap is a
# claimant whose clock reading is older than a rival's already-completed list-and-win, i.e. a window of the few milliseconds between
# reading the clock and the exclusive-create landing. Expired events are deleted as a side effect of every claim (compaction).

_RL_DIR = ("_state", "ratelimit")
_RL_LOCKS: Dict[str, threading.Lock] = {}
_RL_LOCKS_GUARD = threading.Lock()


class _RateLimiter:
    def __init__(self, store: "_Store"):
        self.store = store
        with _RL_LOCKS_GUARD:
            self._lock = _RL_LOCKS.setdefault(store.display, threading.Lock())

    @staticmethod
    def _ms(t: datetime) -> int:
        return int(t.timestamp() * 1000)

    def _append(self, kind: str, run_id: str, now: datetime, payload: Dict[str, Any]) -> str:
        ms = self._ms(now)
        while True:  # same ms + same run + same kind is only ever a retry of ourselves: bump the ms, never overwrite
            name = f"{ms:013d}-{_approval_key(run_id)}-{kind}.json"
            if self.store.create_exclusive(_RL_DIR + (name,), json.dumps({"run_id": run_id, "kind": kind, **payload}, default=str)):
                return name
            ms += 1

    def _withdraw(self, *names: Optional[str]) -> None:
        for n in names:
            if n:
                self.store.remove(*_RL_DIR, n)

    def _events(self) -> List[Tuple[int, str, str]]:
        """[(epoch_ms, kind, file name)] sorted by name (= by time, then run id)."""
        out = []
        for n in self.store.list_names(*_RL_DIR):
            m = re.match(r"^(\d{13})-.+-(triage|cooldown|suppressed)\.json$", n)
            if m:
                out.append((int(m.group(1)), m.group(2), n))
        return sorted(out, key=lambda e: e[2])

    def _cooldown(self, name: str) -> Optional[Dict[str, Any]]:
        d = self.store.read_json(*_RL_DIR, name)
        return d if isinstance(d, dict) else None

    def _compact(self, evs: List[Tuple[int, str, str]], now: datetime, cooldown_minutes: float) -> None:
        now_ms = self._ms(now)
        for ms, kind, n in evs:
            if kind == "triage" and ms <= now_ms - 3_600_000:
                self._withdraw(n)
            elif kind == "suppressed" and ms <= now_ms - int(max(cooldown_minutes, 60.0) * 60_000):
                self._withdraw(n)
            elif kind == "cooldown":
                d = self._cooldown(n)
                until = _parse_ts((d or {}).get("until"))
                if d is not None and (until is None or until <= now):
                    self._withdraw(n)

    def _active_cooldowns(self, evs: List[Tuple[int, str, str]], key: str, now: datetime) -> List[Tuple[str, Dict[str, Any]]]:
        out = []
        for _, kind, n in evs:
            if kind != "cooldown":
                continue
            d = self._cooldown(n)
            until = _parse_ts((d or {}).get("until"))
            if d and d.get("key") == key and until and until > now:
                out.append((n, d))
        return out

    def suppressed_count(self, key: str) -> int:
        n = 0
        for _, kind, name in self._events():
            if kind == "suppressed" and (self.store.read_json(*_RL_DIR, name) or {}).get("key") == key:
                n += 1
        return n

    def triage_count(self, now: datetime) -> int:
        lo = self._ms(now) - 3_600_000
        return sum(1 for ms, kind, _ in self._events() if kind == "triage" and ms > lo)

    def admit(self, key: str, run_id: str, now: datetime, max_per_hour: int, cooldown_minutes: float) -> Tuple[str, Optional[str]]:
        """Claim one triage slot for `run_id`. Returns ("ok", None) | ("cooldown", until_iso) | ("rate_limited", None)."""
        with self._lock:
            evs = self._events()
            self._compact(evs, now, cooldown_minutes)
            evs = self._events()
            live = self._active_cooldowns(evs, key, now)
            if live:
                self._append("suppressed", run_id, now, {"key": key})
                return "cooldown", str(live[0][1].get("until"))
            lo = self._ms(now) - 3_600_000
            if sum(1 for ms, kind, _ in evs if kind == "triage" and ms > lo) >= int(max_per_hour):
                return "rate_limited", None  # already full: deny without writing (a late claimant never outranks a settled winner)
            t = self._append("triage", run_id, now, {"key": key})
            c = None
            if cooldown_minutes > 0:
                c = self._append("cooldown", run_id, now, {"key": key, "until": _iso(now + timedelta(minutes=float(cooldown_minutes)))})
            evs = self._events()
            earlier = [(n, d) for n, d in self._active_cooldowns(evs, key, now) if c and n < c]  # a rival's cooldown claim that ranks first
            if earlier:
                self._withdraw(t, c)
                self._append("suppressed", run_id, now, {"key": key})
                return "cooldown", str(earlier[0][1].get("until"))
            window = [n for ms, kind, n in evs if kind == "triage" and ms > lo]
            if window.index(t) >= int(max_per_hour):
                self._withdraw(t, c)
                return "rate_limited", None
            return "ok", None


# ── agentic_pipeline-compatible approval primitives (inlined minimal subset) ──
# Must stay byte-compatible with agentic_pipeline's `_approval_key`, `_approval_fingerprint` and token/pending shapes;
# tests/test_agentic_compat.py loads both components and proves it.

def _approval_key(raw: Any) -> str:
    raw = str(raw)
    safe = re.sub(r"[^A-Za-z0-9._-]+", "_", raw)
    if safe != raw:
        safe = f"{safe}-{hashlib.sha1(raw.encode('utf-8')).hexdigest()[:8]}"
    return safe


def _approval_fingerprint(step_id: str, text: str, summary: str, bind_to: Optional[List[str]] = None) -> str:
    bind_to = bind_to or ["text", "summary"]
    blob = json.dumps({"step": step_id, **({"text": text} if "text" in bind_to else {}),
                       **({"summary": summary} if "summary" in bind_to else {})}, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:32]


def _token_state(tok: Optional[Dict[str, Any]], fingerprint: str, max_age_hours: Optional[float],
                 modified: Optional[datetime] = None) -> Tuple[str, Optional[str]]:
    """valid | none | consumed | expired | stale -- same states, same reasons as agentic_pipeline's reuse=once gate."""
    if tok is None:
        return "none", None
    if tok.get("consumed_at"):
        return "consumed", f"consumed: this approval was already used by a run at {tok['consumed_at']}; a fresh one is needed"
    if max_age_hours is not None:
        at = _parse_ts(tok.get("approved_at")) or modified
        if at is None:
            return "expired", "approval expired: its age cannot be determined (no approved_at, no modification time)"
        age_h = (_now() - at).total_seconds() / 3600.0
        if age_h > float(max_age_hours):
            return "expired", f"approval expired: given {age_h:.1f}h ago, max_age_hours={max_age_hours:g}"
    fp = tok.get("fingerprint")
    if not fp:
        return "stale", "token carries no fingerprint, so it is not bound to what was reviewed (write the token from the pending record)"
    if fp != fingerprint:
        return "stale", "input changed since approval"
    return "valid", None


# ── proposal parsing + action validation ─────────────────────────────────────

_ASSET_KEY_RE = re.compile(r"^[A-Za-z0-9_.\-]+(/[A-Za-z0-9_.\-]+)*$")
_SEVERITIES = ("low", "medium", "high", "critical")
_PARAM_KEYS = {
    "retry_run": {"run_id"},
    "rerun_partition": {"asset_keys", "partition_key"},
    "materialize_assets": {"asset_keys", "partition_key"},
    "open_ticket": {"title", "body", "severity"},
    "notify": {"message"},
}


def _extract_json_object(text: str) -> Optional[Dict[str, Any]]:
    t = text.strip()
    m = re.match(r"^```(?:json)?\s*(.*?)\s*```$", t, re.S)
    if m:
        t = m.group(1)
    try:
        v = json.loads(t)
        return v if isinstance(v, dict) else None
    except ValueError:
        pass
    i, j = t.find("{"), t.rfind("}")
    if i >= 0 and j > i:
        try:
            v = json.loads(t[i:j + 1])
            return v if isinstance(v, dict) else None
        except ValueError:
            return None
    return None


def normalize_proposal(raw: Any, stem: str, redact: Any = None) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """Accept the documented flat shape, an agentic_pipeline step dict (`extracted: {...}` / `text: "<json>"`), or a fenced JSON
    string. Returns (proposal, None) or (None, reason). `run_id` defaults to the file name and must equal it when present."""
    obj = raw
    if isinstance(obj, str):
        obj = _extract_json_object(obj)
    if not isinstance(obj, dict):
        return None, "proposal is not a JSON object"
    if "actions" not in obj:
        for k in ("extracted", "data", "proposal", "output"):
            if isinstance(obj.get(k), dict) and "actions" in obj[k]:
                obj = obj[k]
                break
        else:
            if isinstance(obj.get("text"), str):
                inner = _extract_json_object(obj["text"])
                if inner is not None:
                    obj = inner
    actions = obj.get("actions")
    if not isinstance(actions, list):
        return None, "proposal has no `actions` list"
    rid = obj.get("run_id", stem)
    if not isinstance(rid, str) or rid != stem:
        return None, f"proposal run_id {rid!r} does not match its file name {stem!r}"
    rd = redact or (lambda t: t)
    conf = obj.get("confidence")
    try:
        conf = None if conf is None else max(0.0, min(1.0, float(conf)))
    except (TypeError, ValueError):
        conf = None
    acts = []
    for a in actions:
        if not isinstance(a, dict):
            acts.append({"kind": None, "params": {}, "reason": "", "_invalid": "action is not an object"})
            continue
        params = a.get("params") if a.get("params") is not None else {}
        if isinstance(params, dict):  # free-text params only: ids / asset keys / partition keys must stay exact
            params = {k: (rd(v) if k in ("title", "body", "message") and isinstance(v, str) else v) for k, v in params.items()}
        acts.append({"kind": a.get("kind"), "params": params, "reason": rd(str(a.get("reason") or ""))})
    prop = {"run_id": rid, "summary": rd(str(obj.get("summary") or "")), "root_cause": rd(str(obj.get("root_cause") or "")),
            "confidence": conf, "actions": acts}
    if isinstance(obj.get("receipt_id"), str) and obj["receipt_id"]:
        prop["receipt_id"] = obj["receipt_id"]
    return prop, None


def proposal_text(prop: Dict[str, Any]) -> str:
    return json.dumps(prop, sort_keys=True, ensure_ascii=False)


def proposal_summary(prop: Dict[str, Any]) -> str:
    kinds = ", ".join(str(a.get("kind")) for a in prop["actions"]) or "no actions"
    conf = "" if prop.get("confidence") is None else f" (confidence {prop['confidence']:.2f})"
    return _clip(f"{prop['summary'] or prop['root_cause'] or 'remediation proposal'}{conf} -> {kinds}", 600)


def _asset_key_list(v: Any, field: str, required: bool) -> Tuple[Optional[List[str]], Optional[str]]:
    if v is None:
        return ([], None) if not required else (None, f"{field} is required")
    if not isinstance(v, list) or any(not isinstance(x, str) or not _ASSET_KEY_RE.match(x) for x in v):
        return None, f"{field} must be a list of asset key strings like 'schema/table'"
    if len(v) > 50:
        return None, f"{field} has more than 50 entries"
    if required and not v:
        return None, f"{field} must not be empty"
    return list(v), None


def validate_action(action: Dict[str, Any], run_id: str, allowed: List[str], *, webhook_configured: bool,
                    allowed_materialize: Optional[List[str]]) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """Static validation (no instance needed). Returns (normalized action, None) or (None, rejection reason)."""
    if action.get("_invalid"):
        return None, action["_invalid"]
    kind = action.get("kind")
    if not isinstance(kind, str) or kind not in KNOWN_ACTIONS:
        return None, f"unknown action kind {kind!r}; known kinds: {', '.join(KNOWN_ACTIONS)}"
    if kind not in allowed:
        return None, f"action kind {kind!r} is not in allowed_actions {allowed}"
    params = action.get("params")
    if not isinstance(params, dict):
        return None, "params must be an object"
    extra = sorted(set(params) - _PARAM_KEYS[kind])
    if extra:
        return None, f"unexpected params {extra} for {kind}; allowed: {sorted(_PARAM_KEYS[kind])}"
    p: Dict[str, Any] = {}
    if kind == "retry_run":
        if params.get("run_id") not in (None, run_id):
            return None, "retry_run may only retry the run this proposal is about"
    elif kind in ("rerun_partition", "materialize_assets"):
        keys, err = _asset_key_list(params.get("asset_keys"), "asset_keys", required=(kind == "materialize_assets"))
        if err:
            return None, err
        if keys:
            p["asset_keys"] = keys
        pk = params.get("partition_key")
        if pk is not None:
            if not isinstance(pk, str) or not pk or len(pk) > 200 or re.search(r"[\x00-\x1f]", pk):
                return None, "partition_key must be a non-empty string without control characters (max 200)"
            p["partition_key"] = pk
        if kind == "materialize_assets" and allowed_materialize is not None:
            bad = [k for k in keys or [] if k not in allowed_materialize]
            if bad:
                return None, f"asset keys {bad} are not in allowed_materialize_assets"
    elif kind == "open_ticket":
        title = params.get("title")
        if not isinstance(title, str) or not title.strip() or len(title) > 200:
            return None, "title is required (non-empty string, max 200 chars)"
        body = params.get("body", "")
        if not isinstance(body, str) or len(body) > 4000:
            return None, "body must be a string of at most 4000 chars"
        sev = params.get("severity", "medium")
        if sev not in _SEVERITIES:
            return None, f"severity must be one of {list(_SEVERITIES)}"
        p = {"title": title.strip(), "body": body, "severity": sev}
    elif kind == "notify":
        msg = params.get("message")
        if not isinstance(msg, str) or not msg.strip() or len(msg) > 2000:
            return None, "message is required (non-empty string, max 2000 chars)"
        if not webhook_configured:
            return None, "notify needs webhook_url_env_var configured (and set in the environment)"
        p = {"message": msg.strip()}
    return {"kind": kind, "params": p, "reason": _clip(action.get("reason"), 1000)}, None


# ── error signature ──────────────────────────────────────────────────────────

def error_signature(job_name: str, error_class: str, message: str, step_keys: List[str]) -> str:
    """Stable id for 'the same failure': job + root error class + the message with ids/numbers normalized + failed steps."""
    first = (message or "").strip().splitlines()[0] if (message or "").strip() else ""
    norm = re.sub(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}", "<uuid>", first)
    norm = re.sub(r"0x[0-9a-fA-F]+", "<hex>", norm)
    norm = re.sub(r"\d+", "<n>", norm)[:200]
    blob = json.dumps([job_name, error_class, norm, sorted(step_keys)], ensure_ascii=False)
    return hashlib.sha1(blob.encode("utf-8")).hexdigest()[:12]


# ── instance helpers ─────────────────────────────────────────────────────────

def _run_asset_events(instance: Any, run_id: str) -> Tuple[List[Tuple[str, Optional[str]]], List[Tuple[str, Optional[str]]]]:
    """(planned, materialized) as [(asset_key_string, partition)] from the run's event log."""
    from dagster import DagsterEventType as T
    planned: List[Tuple[str, Optional[str]]] = []
    done = set()
    for e in instance.all_logs(run_id, of_type={T.ASSET_MATERIALIZATION_PLANNED, T.ASSET_MATERIALIZATION}):
        d = e.dagster_event
        if d is None:
            continue
        ak = getattr(d, "asset_key", None)
        if ak is None:
            continue
        item = (ak.to_user_string(), getattr(d, "partition", None))
        if d.event_type_value == "ASSET_MATERIALIZATION_PLANNED":
            if item not in planned:
                planned.append(item)
        else:
            done.add(item)
    return planned, sorted(done, key=lambda x: (x[0], x[1] or ""))


def _error_chain(err: Any) -> List[Any]:
    chain, seen = [], 0
    while err is not None and seen < 10:
        chain.append(err)
        err = getattr(err, "cause", None)
        seen += 1
    return chain


_RUNS_SCAN_LIMIT = 1000
EXTERNAL_SOURCE_TAG = "dagster/external_job_source"  # set by dagster-airlift (value "airflow") on runs it records via add_historical_run
EXTERNAL_TAG_PREFIXES = ("dagster-airlift/",)


def _external_source(run: Any) -> Optional[str]:
    """Name of the external orchestrator a run was merely RECORDED from (e.g. 'airflow'), else None."""
    tags = getattr(run, "tags", None) or {}
    if tags.get(EXTERNAL_SOURCE_TAG):
        return str(tags[EXTERNAL_SOURCE_TAG])
    if any(str(k).startswith(EXTERNAL_TAG_PREFIXES) for k in tags):
        return "airflow"
    return None


def _parse_json(v: Any) -> Optional[Dict[str, Any]]:
    try:
        o = json.loads(v) if v else None
    except ValueError:
        return None
    return o if isinstance(o, dict) else None


class _RecordContext:
    """The slice of RunFailureSensorContext the engine reads, built from a run record (no failure event exists)."""

    def __init__(self, ctx: Any, run: Any, record: Any):
        self._ctx, self.dagster_run, self.record = ctx, run, record
        self.instance, self.log = ctx.instance, ctx.log

    @property
    def repository_def(self) -> Any:
        return self._ctx.repository_def

    @property
    def failure_event(self) -> Any:
        raise AttributeError("no failure event: this run was detected from its run record")


def _status(run_or_status: Any) -> str:
    s = getattr(run_or_status, "status", run_or_status)
    return str(getattr(s, "value", s))


# ── the engine ───────────────────────────────────────────────────────────────

class _Engine:
    def __init__(self, c: "RunFailureRemediationComponent"):
        self.c = c
        self.evidence = _Store(c.evidence_dir)
        self.limiter = _RateLimiter(self.evidence)
        self.proposals = _Store(c.proposals_dir)
        self.approvals = _Store(c.approval_dir)
        self.results = _Store(c.results_dir)
        self.tickets = _Store(c.tickets_dir or f"{c.results_dir.rstrip('/')}/tickets")
        self.allowed = list(c.allowed_actions)
        self.ignore = set(c.ignore_jobs or [])
        self.only = set(c.only_jobs or [])
        self.only_tags = dict(c.only_tags or {})
        self.job_names = [f"{c.name_prefix}_remediate_{n}" for n in (c.remediation_jobs or {})]

    # -- redaction shortcuts --
    def rt(self, s: Any) -> Any:
        return redact_text(s, self.c.redact_patterns) if isinstance(s, str) else s

    def ro(self, o: Any) -> Any:
        return redact_obj(o, self.c.redact_patterns, skip_keys=_SKIP_KEYS)

    def webhook_url(self, env_name: Optional[str]) -> Optional[str]:
        return os.environ.get(env_name) if env_name else None

    # ═══════════ failure sensor ═══════════

    def build_evidence(self, context: Any, source: str = "events") -> Dict[str, Any]:
        run, inst = context.dagster_run, context.instance
        n_hist = int(self.c.history_runs)
        events = inst.all_logs(run.run_id, of_type=self._failure_event_types())
        steps: List[Dict[str, Any]] = []
        run_failure_message = None
        for e in events:
            d = e.dagster_event
            if d is None:
                continue
            if d.event_type_value == "STEP_FAILURE":
                err = getattr(d.event_specific_data, "error", None)
                chain = _error_chain(err)
                root = chain[-1] if chain else None
                steps.append({
                    "step_key": d.step_key,
                    "wrapper_error_class": chain[0].cls_name if chain else None,
                    "error_class": root.cls_name if root else None,
                    "message_tail": self.rt(_tail((root.message if root else d.message or "").strip(), int(self.c.log_tail_chars))),
                    "stack_tail": self.rt(_tail("".join(root.stack[-3:]) if root and root.stack else "", 1500)),
                })
            elif d.event_type_value in ("PIPELINE_FAILURE", "RUN_FAILURE"):
                run_failure_message = self.rt(_tail(d.message or "", 1000))
        try:
            fe = context.failure_event
            run_failure_message = run_failure_message or self.rt(_tail(fe.message or "", 1000))
        except Exception:
            pass
        planned, done = _run_asset_events(inst, run.run_id)
        done_set = set(done)
        failed_assets = [{"asset_key": k, "partition": p} for k, p in planned if (k, p) not in done_set]
        step_keys = [s["step_key"] for s in steps if s["step_key"]]
        root_cls = steps[0]["error_class"] if steps else None
        root_msg = steps[0]["message_tail"] if steps else (run_failure_message or "")
        sig = error_signature(run.job_name, root_cls or "", root_msg, step_keys)
        partition = (run.tags or {}).get("dagster/partition")
        started, ended = self._run_times(inst, run)
        ext = _external_source(run)
        ev: Dict[str, Any] = {
            "run_id": run.run_id, "job_name": run.job_name, "status": _status(run), "partition": partition,
            "source": source, "limited": not steps,
            "external_source": ext,
            "started_at": started, "ended_at": ended,
            "failure_reason": (run.tags or {}).get("dagster/failure_reason"),
            "created_at": _iso(),
            "tags": {k: v for k, v in (run.tags or {}).items()},
            "failed_step_keys": step_keys,
            "errors": steps,
            "run_failure_message": run_failure_message,
            "failed_assets": failed_assets,
            "materialized_assets": [{"asset_key": k, "partition": p} for k, p in done],
            "error_signature": sig,
            "upstream": self._upstream(context, failed_assets),
            "recent_runs": self._history(inst, run, n_hist),
            "recent_outcomes": self.recent_outcomes(run.job_name, sig, int(self.c.recent_outcomes)),
            "remediation_contract": {
                "proposal_path": self.proposals.shown(f"{run.run_id}.json"),
                "proposal_shape": {"run_id": run.run_id, "summary": "", "root_cause": "", "confidence": "0..1",
                                   "actions": [{"kind": f"one of {self.allowed}", "params": {}, "reason": ""}]},
                "allowed_actions": self.allowed,
            },
        }
        return self.ro(ev)

    @staticmethod
    def _run_times(inst: Any, run: Any) -> Tuple[Optional[str], Optional[str]]:
        try:
            recs = inst.get_run_records(filters=dg.RunsFilter(run_ids=[run.run_id]), limit=1)
        except Exception:
            return None, None
        if not recs:
            return None, None
        r = recs[0]
        f = lambda t: datetime.fromtimestamp(t, tz=timezone.utc).isoformat() if t else None  # noqa: E731
        return f(r.start_time), f(r.end_time)

    @staticmethod
    def _failure_event_types() -> Any:
        from dagster import DagsterEventType as T
        return {T.STEP_FAILURE, T.PIPELINE_FAILURE}

    def _upstream(self, context: Any, failed_assets: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        try:
            graph = context.repository_def.asset_graph
        except Exception:
            return out
        seen = set()
        for fa in failed_assets:
            try:
                key = dg.AssetKey.from_user_string(fa["asset_key"])
                if not graph.has(key):
                    continue
                parents = sorted(graph.get(key).parent_keys, key=lambda k: k.to_user_string())
            except Exception:
                continue
            for pk in parents:
                if pk in seen or pk == key:
                    continue
                seen.add(pk)
                entry: Dict[str, Any] = {"asset_key": pk.to_user_string(), "downstream_failed_asset": fa["asset_key"],
                                         "last_materialization": None}
                try:
                    ev = context.instance.get_latest_materialization_event(pk)
                    if ev is not None and ev.asset_materialization is not None:
                        am = ev.asset_materialization
                        entry["last_materialization"] = {
                            "timestamp": datetime.fromtimestamp(ev.timestamp, tz=timezone.utc).isoformat(),
                            "run_id": ev.run_id, "partition": am.partition,
                            "data_version": (am.tags or {}).get("dagster/data_version"),
                        }
                except Exception:
                    pass
                out.append(entry)
        return out

    def _history(self, inst: Any, run: Any, n: int) -> List[Dict[str, Any]]:
        out = []
        try:
            recs = inst.get_run_records(filters=dg.RunsFilter(job_name=run.job_name), limit=n + 1)
        except Exception:
            return out
        for r in recs:
            if r.dagster_run.run_id == run.run_id:
                continue
            out.append({"run_id": r.dagster_run.run_id, "status": _status(r.dagster_run),
                        "partition": (r.dagster_run.tags or {}).get("dagster/partition"),
                        "started_at": datetime.fromtimestamp(r.start_time, tz=timezone.utc).isoformat() if r.start_time else None,
                        "ended_at": datetime.fromtimestamp(r.end_time, tz=timezone.utc).isoformat() if r.end_time else None})
            if len(out) >= n:
                break
        return out

    def recent_outcomes(self, job_name: str, sig: str, n: int) -> List[Dict[str, Any]]:
        """The last `n` remediation outcomes for the same job or the same error signature -- so the next triage can learn."""
        if n <= 0:
            return []
        rows = []
        for stem, mtime in sorted(self.results.list_json(), key=lambda x: -x[1])[:200]:
            res = self.results.read_json(f"{stem}.json")
            if not isinstance(res, dict) or not res.get("outcome"):
                continue
            if res.get("job_name") != job_name and res.get("error_signature") != sig:
                continue
            rows.append({"run_id": res.get("run_id") or stem, "job_name": res.get("job_name"),
                         "error_signature": res.get("error_signature"),
                         "same_signature": res.get("error_signature") == sig,
                         "actions": [a.get("kind") for a in res.get("actions", [])],
                         "outcome": res.get("outcome"), "outcome_detail": res.get("outcome_detail"),
                         "root_cause": res.get("root_cause"), "summary": res.get("summary"), "at": res.get("outcome_at")})
        rows.sort(key=lambda r: str(r.get("at") or ""), reverse=True)
        return rows[:n]

    def failure_tick(self, context: Any, triage_job_name: str, partitions_def: Any) -> Any:
        """The run_failure_sensor body (detect: events | both)."""
        kind, payload = self.decide(context, triage_job_name, partitions_def, defer_rate_limit=False)
        return payload

    def _mark_seen(self, run_id: str, decision: str) -> None:
        """Durable per-run 'decided' marker shared by the event sensor and the run-record sensor (one tiny file per run, so two
        sensors never overwrite each other's state and `detect: both` can never judge a run twice)."""
        self.evidence.write_json(("_state", "seen", f"{run_id}.json"), {"run_id": run_id, "decision": decision, "at": _iso()})

    def is_seen(self, run_id: str) -> bool:
        return self.evidence.exists("_state", "seen", f"{run_id}.json") or self.evidence.exists(f"{run_id}.json")

    def decide(self, context: Any, triage_job_name: str, partitions_def: Any, *, defer_rate_limit: bool,
               source: str = "events") -> Tuple[str, Any]:
        """Shared triage decision for one failed run. Returns ("skip", SkipReason) | ("defer", SkipReason) |
        ("request", SensorResult). `defer` (only when defer_rate_limit) leaves the run undecided so a later tick retries it."""
        c, run = self.c, context.dagster_run
        job = run.job_name or ""

        def skip(reason: str, decision: str = "skipped") -> Tuple[str, Any]:
            context.log.info(f"run_failure_remediation: skipping {run.run_id} ({job}): {reason}")
            self._mark_seen(run.run_id, f"{decision}: {reason}")
            return "skip", dg.SkipReason(reason)

        if job == triage_job_name:
            return skip("failure of the triage job itself (no recursion)")
        if job in self.ignore:
            return skip(f"job {job!r} is in ignore_jobs")
        if self.only and job not in self.only:
            return skip(f"job {job!r} is not in only_jobs")
        if self.only_tags:
            tags = run.tags or {}
            miss = [k for k, v in self.only_tags.items() if k not in tags or (v not in ("", "*") and tags[k] != v)]
            if miss:
                return skip(f"run does not carry the only_tags {miss}")
        if (run.tags or {}).get(TAG_PROPOSAL) and not c.triage_remediation_runs:
            return skip("run was launched by a remediation action (triage_remediation_runs is false)")
        if (run.tags or {}).get(TAG_TRIAGE):
            return skip("failure of a triage run (no recursion)")

        evidence = self.build_evidence(context, source=source)
        sig = evidence["error_signature"]
        now = _now()
        ckey = f"{job}|{sig}"
        already = self.evidence.exists(f"{run.run_id}.json")  # a re-tick of a failure we already triaged
        if not already:
            verdict, until = self.limiter.admit(ckey, run.run_id, now, int(c.max_triage_per_hour), float(c.cooldown_minutes))
            if verdict == "cooldown":
                return skip(f"cooldown: job {job!r} / signature {sig} was triaged recently (until {until})", "suppressed")
            if verdict == "rate_limited":
                reason = f"rate limit: max_triage_per_hour={c.max_triage_per_hour} reached"
                if defer_rate_limit:
                    context.log.info(f"run_failure_remediation: deferring {run.run_id} ({job}): {reason}")
                    return "defer", dg.SkipReason(reason)
                return skip(reason, "rate_limited")

        self.evidence.write_json((f"{run.run_id}.json",), evidence)
        self._mark_seen(run.run_id, "triaged")

        requests = []
        try:
            exists = context.instance.has_dynamic_partition(partitions_def.name, run.run_id)
        except Exception:
            exists = False
        if not exists:
            requests.append(partitions_def.build_add_request([run.run_id]))
        tags = {TAG_TRIAGE: "true", TAG_FAILED_RUN: run.run_id, "dagster_remediation/job": job, "dagster_remediation/signature": sig}
        rr = dg.RunRequest(run_key=run.run_id, partition_key=run.run_id, tags=tags,
                           run_config=dict(c.triage_run_config) if c.triage_run_config else None)
        return "request", dg.SensorResult(run_requests=[rr], dynamic_partitions_requests=requests)

    # ═══════════ run-record sensor (detect: runs | both) ═══════════

    def runs_tick(self, context: Any, triage_job_name: str, partitions_def: Any) -> Any:
        """Poll run RECORDS for FAILURE runs -- sees runs that never emitted a RUN_FAILURE event (written straight to run storage).

        Not a high-water-mark cursor on purpose: a run's `update_timestamp` is the time of its last EVENT, which for historical /
        externally observed runs is in the past and can be persisted long after later runs were seen (verified with
        `add_historical_run` + `report_dagster_event`), and a row can reach FAILURE after rows with a higher id. So every tick
        scans the FAILURE runs updated within `runs_lookback_hours` and judges those without a durable per-run marker, oldest
        (update_timestamp, run_id) first, at most `runs_max_per_tick` at a time. The Dagster cursor only records when the sensor
        was first armed: failures that already existed then are marked seen, not triaged (same as the event sensor, which starts
        from 'now')."""
        c, inst = self.c, context.instance
        cur = _parse_json(getattr(context, "cursor", None)) or {}
        now = _now()
        window = now - timedelta(hours=float(c.runs_lookback_hours))
        recs = inst.get_run_records(filters=dg.RunsFilter(statuses=[dg.DagsterRunStatus.FAILURE], updated_after=window),
                                    limit=_RUNS_SCAN_LIMIT, order_by="update_timestamp", ascending=False)
        recs = sorted(recs, key=lambda r: (r.update_timestamp, r.dagster_run.run_id))
        if not cur.get("armed_at"):
            for r in recs:
                self._mark_seen(r.dagster_run.run_id, "preexisting when the run-record sensor was armed")
            return dg.SensorResult(cursor=json.dumps({"armed_at": _iso(now)}),
                                   skip_reason=f"armed; {len(recs)} pre-existing failure(s) marked seen")
        requests: List[Any] = []
        partitions: List[Any] = []
        triaged = 0
        for r in recs:
            run = r.dagster_run
            if self.is_seen(run.run_id):
                continue
            if triaged >= int(c.runs_max_per_tick):
                break
            rctx = _RecordContext(context, run, r)
            kind, payload = self.decide(rctx, triage_job_name, partitions_def, defer_rate_limit=True, source="run_record")
            if kind == "defer":
                break  # rate limited: leave this and every newer run undecided for a later tick
            if kind == "request":
                triaged += 1
                requests.extend(payload.run_requests)
                partitions.extend(payload.dynamic_partitions_requests or [])
        cursor = json.dumps(cur)
        if requests:
            return dg.SensorResult(run_requests=requests, dynamic_partitions_requests=partitions, cursor=cursor)
        return dg.SensorResult(cursor=cursor, skip_reason="no new failed run records")

    # ═══════════ approval + action sensor ═══════════

    def static_actions(self, prop: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        cap = int(self.c.max_actions_per_proposal)
        usable, rejected = [], []
        webhook = bool(self.webhook_url(self.c.webhook_url_env_var))
        for i, a in enumerate(prop["actions"]):
            if i >= cap:
                rejected.append({"index": i, "kind": a.get("kind"), "status": "rejected",
                                 "reason": f"more than max_actions_per_proposal={cap} actions"})
                continue
            norm, err = validate_action(a, prop["run_id"], self.allowed, webhook_configured=webhook,
                                        allowed_materialize=self.c.allowed_materialize_assets)
            if err:
                rejected.append({"index": i, "kind": a.get("kind") if isinstance(a.get("kind"), str) else None,
                                 "status": "rejected", "reason": err})
            else:
                usable.append({"index": i, **norm})
        return usable, rejected

    def pending_record(self, prop: Dict[str, Any], fp: str, state: str, reason: Optional[str], usable: List[Dict[str, Any]],
                       rejected: List[Dict[str, Any]]) -> Dict[str, Any]:
        key = _approval_key(prop["run_id"])
        summary = proposal_summary(prop)
        preview = json.dumps({**prop, "rejected_actions": rejected} if rejected else prop, indent=2, ensure_ascii=False)
        return {
            "step": self.c.approval_step_id, "key": key, "requested_at": _iso(), "fingerprint": fp,
            "state": state, "reason": reason, "reuse": "once", "max_age_hours": self.c.approval_max_age_hours,
            "expires_at": None, "on_timeout": "pending", "summary": summary, "preview": _clip(preview, 2000),
            "approve_by_writing": {"path": self.approvals.shown(f"{key}.json"),
                                   "json": {"approved": True, "approver": "<you>", "reason": "<why>",
                                            "fingerprint": fp, "approved_at": "<iso timestamp>"}},
            "source": "run_failure_remediation", "run_id": prop["run_id"],
        }

    def read_token(self, key: str) -> Optional[Dict[str, Any]]:
        tok = self.approvals.read_json(f"{key}.json")
        return tok if isinstance(tok, dict) and "approved" in tok else None

    def auto_qualifies(self, prop: Dict[str, Any], usable: List[Dict[str, Any]], rejected: List[Dict[str, Any]]) -> Tuple[bool, str]:
        aa = self.c.auto_approve
        if not aa:
            return False, "auto_approve not configured"
        if rejected or not usable:
            return False, "proposal has rejected actions"
        if any(a["kind"] not in aa["kinds"] for a in usable):
            return False, "proposal has action kinds outside auto_approve.kinds"
        conf = prop.get("confidence")
        if conf is None or conf < float(aa.get("min_confidence", 0.8)):
            return False, f"confidence {conf} below auto_approve.min_confidence"
        hour_ago = _now() - timedelta(hours=1)
        n = 0
        for stem, _m in self.results.list_json():
            r = self.results.read_json(f"{stem}.json")
            if isinstance(r, dict) and r.get("auto_approved") and (_parse_ts(r.get("requested_at")) or hour_ago) > hour_ago:
                n += 1
        if n >= int(aa.get("max_per_hour", 3)):
            return False, f"auto_approve.max_per_hour={aa.get('max_per_hour', 3)} reached"
        return True, "ok"

    # -- action execution --

    def _graph(self, context: Any) -> Any:
        try:
            return context.repository_def.asset_graph
        except Exception:
            return None

    def _check_request(self, graph: Any, instance: Any, keys: List[str], partition_key: Optional[str]) -> Optional[str]:
        """Everything the sensor framework would raise on when it resolves the request -- caught here so one bad action cannot
        wedge the sensor tick."""
        if graph is None:
            return "the code location's asset graph is not available to the sensor"
        defs_ = set()
        for k in keys:
            ak = dg.AssetKey.from_user_string(k)
            if not graph.has(ak):
                return f"asset {k!r} is not defined in this code location"
            defs_.add(graph.get(ak).partitions_def)
        if len(defs_) > 1:
            return "selected assets have different partitions definitions; cannot launch them in one run"
        pdef = next(iter(defs_)) if defs_ else None
        if pdef is not None:
            if partition_key is None:
                return "the selected assets are partitioned but no partition key is available"
            try:
                ok = pdef.has_partition_key(partition_key, dynamic_partitions_store=instance)
            except TypeError:
                ok = pdef.has_partition_key(partition_key)
            if not ok:
                return f"partition {partition_key!r} does not exist"
        elif partition_key is not None:
            return f"assets are unpartitioned but partition {partition_key!r} was requested"
        return None

    def _job_for(self, context: Any, keys: List[str]) -> Optional[str]:
        """The first remediation job whose selection covers every key (the sensor may only request runs for its own jobs)."""
        want = {dg.AssetKey.from_user_string(k) for k in keys}
        for name in self.job_names:
            try:
                have = set(context.repository_def.get_job(name).asset_layer.selected_asset_keys)
            except Exception:
                continue
            if want <= have:
                return name
        return None

    def _planned_requests(self, context: Any, action: Dict[str, Any], run_id: str) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        """-> ([request spec], None) or ([], reason). A spec: {run_key, asset_keys, partition_key, tags, use_run_config}."""
        inst, graph = context.instance, self._graph(context)
        kind, params = action["kind"], action["params"]
        idx = action["index"]
        base_tags = {TAG_PROPOSAL: run_id, TAG_KIND: kind}
        if kind == "materialize_assets":
            keys, pk = params["asset_keys"], params.get("partition_key")
            err = self._check_request(graph, inst, keys, pk)
            if err:
                return [], err
            job = self._job_for(context, keys)
            if job is None:
                return [], f"no remediation job covers {keys}; add them to remediation_jobs"
            return [{"run_key": f"{run_id}:{idx}:{kind}", "asset_keys": keys, "partition_key": pk, "use_run_config": False,
                     "job_name": job, "tags": {**base_tags, TAG_RUN_KEY: f"{run_id}:{idx}:{kind}"}}], None

        run = inst.get_run_by_id(run_id)
        if run is None:
            return [], f"run {run_id} not found on this instance"
        ext = _external_source(run)
        if ext and kind in ("retry_run", "rerun_partition"):
            return [], (f"run {run_id} was recorded from an external orchestrator ({ext}); there is no Dagster job to re-execute, "
                        f"so {kind} is not available (re-run it in {ext}; open_ticket / notify still work)")
        if kind == "retry_run":
            if _status(run) != "FAILURE":
                return [], f"run {run_id} is {_status(run)}, not FAILURE; nothing to retry"
            root = (run.tags or {}).get(TAG_PROPOSAL, run_id)
            prior = [r for r in inst.get_runs(filters=dg.RunsFilter(tags={TAG_PROPOSAL: root, TAG_KIND: "retry_run"}))]
            if len(prior) >= int(self.c.max_retries_per_run):
                return [], f"retry limit reached: {len(prior)} retry run(s) already launched for this failure (max_retries_per_run={self.c.max_retries_per_run})"
            planned, done = _run_asset_events(inst, run_id)
            done_set = set(done)
            todo = [(k, p) for k, p in planned if (k, p) not in done_set]
            if not todo:
                sel = sorted(k.to_user_string() for k in (run.asset_selection or []))
                part = (run.tags or {}).get("dagster/partition")
                todo = [(k, part) for k in sel]
            if not todo:
                return [], ("no asset keys are recorded for this run (an op-level job?); retry_run launches asset runs from this "
                            "code location and cannot retry it")
        else:  # rerun_partition
            planned, _done = _run_asset_events(inst, run_id)
            tag_part = (run.tags or {}).get("dagster/partition")
            pk = params.get("partition_key") or tag_part
            keys = params.get("asset_keys") or sorted({k for k, _ in planned} or {k.to_user_string() for k in (run.asset_selection or [])})
            if not keys:
                return [], "no asset keys are recorded for this run and none were given"
            pmap = {}
            for k, p in planned:
                pmap.setdefault(k, p)
            todo = []
            for k in keys:
                ak = dg.AssetKey.from_user_string(k)
                part = pk if (graph is not None and graph.has(ak) and graph.get(ak).partitions_def is not None) else None
                todo.append((k, part))
        groups: Dict[Optional[str], List[str]] = {}
        for k, p in todo:
            groups.setdefault(p, []).append(k)
        specs = []
        for gi, (p, keys) in enumerate(sorted(groups.items(), key=lambda kv: str(kv[0]))):
            keys = sorted(set(keys))
            err = self._check_request(graph, inst, keys, p)
            if err:
                return [], err
            rk = f"{run_id}:{idx}:{kind}" + (f":{gi}" if len(groups) > 1 else "")
            job = self._job_for(context, keys)
            if job is None:
                return [], f"no remediation job covers {keys}; add them to remediation_jobs"
            specs.append({"run_key": rk, "asset_keys": keys, "partition_key": p, "use_run_config": True, "job_name": job,
                          "tags": {**base_tags, TAG_RUN_KEY: rk, "dagster/parent_run_id": run_id,
                                   "dagster/root_run_id": (run.tags or {}).get("dagster/root_run_id") or run.root_run_id or run_id}})
        return specs, None

    def spec_to_request(self, context: Any, spec: Dict[str, Any], run_id: str) -> Any:
        run_config = None
        if spec.get("use_run_config"):
            run = context.instance.get_run_by_id(run_id)
            run_config = dict(run.run_config) if run is not None and run.run_config else None
        tags = dict(spec["tags"])
        return dg.RunRequest(run_key=spec["run_key"], job_name=spec["job_name"], asset_selection=[dg.AssetKey.from_user_string(k) for k in spec["asset_keys"]],
                             partition_key=spec.get("partition_key"), tags=tags, run_config=run_config)

    def _post(self, env_name: Optional[str], payload: Dict[str, Any]) -> Tuple[bool, str]:
        url = self.webhook_url(env_name)
        if not url:
            return False, "webhook not configured"
        import urllib.error
        import urllib.request
        if not re.match(r"^https?://", url):
            return False, "webhook url must be http(s)"
        req = urllib.request.Request(url, data=json.dumps(payload).encode("utf-8"), method="POST",
                                     headers={"Content-Type": "application/json", "User-Agent": "dagster-run-failure-remediation"})
        try:
            with urllib.request.urlopen(req, timeout=float(self.c.webhook_timeout_seconds)) as r:  # nosec - operator-configured URL
                return 200 <= r.status < 300, f"HTTP {r.status}"
        except urllib.error.HTTPError as e:
            return False, f"HTTP {e.code}"
        except Exception as e:  # never include str(e): urllib embeds the URL
            return False, type(e).__name__

    def execute(self, context: Any, prop: Dict[str, Any], fp: str, usable: List[Dict[str, Any]], rejected: List[Dict[str, Any]],
                tok: Dict[str, Any], auto: bool) -> Tuple[Dict[str, Any], List[Any]]:
        run_id = prop["run_id"]
        meta = self.evidence.read_json(f"{run_id}.json") or {}
        records: List[Dict[str, Any]] = list(rejected)
        requests: List[Any] = []
        specs_all: List[Dict[str, Any]] = []
        for a in usable:
            rec: Dict[str, Any] = {"index": a["index"], "kind": a["kind"], "params": self.ro(a["params"]), "reason": self.rt(a["reason"])}
            try:
                if a["kind"] in ("retry_run", "rerun_partition", "materialize_assets"):
                    specs, err = self._planned_requests(context, a, run_id)
                    if err:
                        rec.update(status="rejected", reason=err)
                    else:
                        rec.update(status="requested", run_keys=[s["run_key"] for s in specs])
                        specs_all.extend(specs)
                        requests.extend(self.spec_to_request(context, s, run_id) for s in specs)
                elif a["kind"] == "open_ticket":
                    ticket = self.ro({"run_id": run_id, "job_name": meta.get("job_name"), "title": a["params"]["title"],
                                      "body": a["params"]["body"], "severity": a["params"]["severity"],
                                      "proposal_summary": prop["summary"], "root_cause": prop["root_cause"], "created_at": _iso()})
                    self.tickets.write_json((f"{run_id}-{a['index']}.json",), ticket)
                    rec.update(status="done", ticket_path=self.tickets.shown(f"{run_id}-{a['index']}.json"))
                    if self.c.ticket_webhook_url_env_var:
                        ok, info = self._post(self.c.ticket_webhook_url_env_var, ticket)
                        rec["webhook"] = {"ok": ok, "info": info}
                else:  # notify
                    ok, info = self._post(self.c.webhook_url_env_var, self.ro({"run_id": run_id, "message": a["params"]["message"],
                                                                               "summary": prop["summary"]}))
                    rec.update(status="done" if ok else "failed", webhook={"ok": ok, "info": info})
            except Exception as e:
                rec.update(status="failed", reason=self.rt(f"{type(e).__name__}: {e}"))
            records.append(rec)
        records.sort(key=lambda r: r["index"])
        has_runs = any(r.get("status") == "requested" for r in records)
        now = _now()
        result = {
            "run_id": run_id, "status": "requested" if has_runs else "executed", "proposal_fingerprint": fp,
            "job_name": meta.get("job_name"), "error_signature": meta.get("error_signature"),
            "summary": self.rt(prop["summary"]), "root_cause": self.rt(prop["root_cause"]), "confidence": prop.get("confidence"),
            "receipt_id": prop.get("receipt_id"),
            "approver": tok.get("approver"), "approved_at": tok.get("approved_at"), "auto_approved": bool(auto),
            "requested_at": _iso(now), "actions": records, "run_specs": specs_all,
            "outcome": None if has_runs else "not_applicable", "outcome_detail": None,
            "outcome_due_at": _iso(now + timedelta(minutes=float(self.c.outcome_check_after_minutes))) if has_runs else None,
            "new_run_ids": [],
        }
        if not has_runs:
            result["outcome_at"] = _iso(now)
        return result, requests

    def _runs_by_key(self, inst: Any, run_id: str) -> Dict[str, Any]:
        found: Dict[str, Any] = {}
        for r in inst.get_runs(filters=dg.RunsFilter(tags={TAG_PROPOSAL: run_id})):
            k = (r.tags or {}).get(TAG_RUN_KEY)
            if k and (k not in found):
                found[k] = r
        return found

    def approval_tick(self, context: Any) -> Any:
        requests: List[Any] = []
        executed_now: set = set()
        log = context.log
        # 1. proposals -> pending records -> execution
        for stem, _m in self.proposals.list_json():
            raw = self.proposals.read_json(f"{stem}.json")
            if raw is None:
                try:
                    raw = self.proposals.read_text(f"{stem}.json")
                except OSError:
                    continue
            prop, err = normalize_proposal(raw, stem, self.rt)
            res = self.results.read_json(f"{stem}.json")
            raw_fp = hashlib.sha256(json.dumps(raw, sort_keys=True, default=str).encode()).hexdigest()[:32]
            if isinstance(res, dict) and res.get("status") != "invalid":
                continue
            if err:
                if not (isinstance(res, dict) and res.get("proposal_fingerprint") == raw_fp):
                    self.results.write_json((f"{stem}.json",), {"run_id": stem, "status": "invalid", "reason": self.rt(err),
                                                                 "proposal_fingerprint": raw_fp, "requested_at": _iso()})
                    log.warning(f"run_failure_remediation: invalid proposal {stem}: {err}")
                continue
            fp = _approval_fingerprint(self.c.approval_step_id, proposal_text(prop), proposal_summary(prop))
            if isinstance(res, dict) and res.get("proposal_fingerprint") == fp:
                continue  # already judged invalid as written
            usable, rejected = self.static_actions(prop)
            if not usable:
                self.results.write_json((f"{stem}.json",), {
                    "run_id": stem, "status": "rejected", "proposal_fingerprint": fp, "summary": self.rt(prop["summary"]),
                    "root_cause": self.rt(prop["root_cause"]), "requested_at": _iso(), "actions": rejected,
                    "reason": "no action passed validation / the allow-list", "outcome": "not_applicable", "outcome_at": _iso()})
                continue
            key = _approval_key(prop["run_id"])
            tok = self.read_token(key)
            tstate, why = _token_state(tok, fp, self.c.approval_max_age_hours, None)
            auto = False
            if tstate != "valid":
                ok, auto_why = self.auto_qualifies(prop, usable, rejected)
                if ok:
                    tok = {"approved": True, "approver": "auto_approve", "reason": "auto_approve policy", "fingerprint": fp,
                           "approved_at": _iso()}
                    self.approvals.write_json((f"{key}.json",), tok)
                    tstate, auto = "valid", True
                else:
                    pend_path = ("pending", f"{key}.json")
                    cur = self.approvals.read_json(*pend_path)
                    new_state = tstate if tok is not None else "new"
                    if not (isinstance(cur, dict) and cur.get("fingerprint") == fp and cur.get("state") == new_state and cur.get("reason") == why):
                        self.approvals.write_json(pend_path, self.pending_record(prop, fp, new_state, why, usable, rejected))
                        log.info(f"run_failure_remediation: pending approval for {stem} -> {self.approvals.shown('pending', key + '.json')}")
                    continue
            if not bool(tok.get("approved")):
                self.results.write_json((f"{stem}.json",), {
                    "run_id": stem, "status": "rejected_by_approver", "proposal_fingerprint": fp, "approver": tok.get("approver"),
                    "approval_reason": self.rt(str(tok.get("reason") or "")), "summary": self.rt(prop["summary"]),
                    "requested_at": _iso(), "actions": rejected, "outcome": "not_applicable", "outcome_at": _iso()})
                self._consume(key, tok)
                self.approvals.remove("pending", f"{key}.json")
                continue
            result, reqs = self.execute(context, prop, fp, usable, rejected, tok, auto)
            self.results.write_json((f"{stem}.json",), result)  # record BEFORE handing requests out; run_keys make re-yield safe
            self._consume(key, tok)
            self.approvals.remove("pending", f"{key}.json")
            requests.extend(reqs)
            executed_now.add(stem)
            log.info(f"run_failure_remediation: executed approved proposal {stem} ({result['status']})")

        # 2. re-yield requests whose runs were never observed (a crashed earlier tick; run_key makes it idempotent), then
        # 3. outcome checks for results whose window has passed
        for stem, _m in self.results.list_json():
            res = self.results.read_json(f"{stem}.json")
            if not isinstance(res, dict) or res.get("status") != "requested" or stem in executed_now:
                continue
            found = self._runs_by_key(context.instance, stem)
            due = _parse_ts(res.get("outcome_due_at")) or _now()
            if _now() < due:
                fresh = [s for s in res.get("run_specs", []) if s["run_key"] not in found]
                requests.extend(self.spec_to_request(context, s, stem) for s in fresh)
            if _now() >= due:
                self._finish_outcome(context, stem, res, found)
        if requests:
            return dg.SensorResult(run_requests=requests)
        return dg.SkipReason("nothing to do")

    def _consume(self, key: str, tok: Dict[str, Any]) -> None:
        cur = self.read_token(key)
        if cur is not None and cur.get("fingerprint") == tok.get("fingerprint") and not cur.get("consumed_at"):
            self.approvals.write_json((f"{key}.json",), {**cur, "consumed_at": _iso(), "consumed_by": "run_failure_remediation"})

    def _finish_outcome(self, context: Any, stem: str, res: Dict[str, Any], found: Dict[str, Any]) -> None:
        want = [s["run_key"] for s in res.get("run_specs", [])]
        statuses = {k: _status(found[k]) for k in want if k in found}
        in_flight = [k for k, s in statuses.items() if s not in _TERMINAL]
        due = _parse_ts(res.get("outcome_due_at")) or _now()
        if in_flight and _now() < due + timedelta(hours=6):
            return  # still running: look again next tick (bounded by a 6h grace)
        missing = [k for k in want if k not in found]
        if missing:
            outcome, detail = "not_fixed", f"{len(missing)} remediation run(s) never started"
        elif in_flight:
            outcome, detail = "not_fixed", "remediation run(s) still running after the grace period"
        elif all(s == "SUCCESS" for s in statuses.values()):
            outcome, detail = "fixed", "all remediation runs succeeded"
        else:
            bad = {k: s for k, s in statuses.items() if s != "SUCCESS"}
            outcome, detail = "not_fixed", f"remediation run(s) did not succeed: {sorted(set(bad.values()))}"
        res = dict(res)
        res.update(status="completed", outcome=outcome, outcome_detail=detail, outcome_at=_iso(),
                   new_run_ids=[found[k].run_id for k in want if k in found])
        res["run_statuses"] = statuses
        self.results.write_json((f"{stem}.json",), res)
        self._record_receipt(context, res)

    def _record_receipt(self, context: Any, res: Dict[str, Any]) -> None:
        key = self.c.store_resource_key
        rid = res.get("receipt_id")
        if not key or not rid:
            return
        try:
            store = getattr(context.resources, key, None)
        except Exception:
            store = None
        fn = getattr(store, "record_outcome", None) if store is not None else None
        if not callable(fn):
            return
        fixed = res.get("outcome") == "fixed"
        try:
            fn(rid, signal="remediation_fixed" if fixed else "remediation_not_fixed", value=1.0 if fixed else -1.0,
               source="run_failure_remediation", outcome_ref=res.get("run_id"))
        except Exception as e:
            context.log.warning(f"run_failure_remediation: record_outcome failed ({type(e).__name__}); outcome kept in results only")


# ── component ────────────────────────────────────────────────────────────────

class RunFailureRemediationComponent(dg.Component, dg.Model, dg.Resolvable):
    """Self-healing runs with a safety gate: a run-failure sensor gathers redacted evidence and requests a triage run on a
    dynamic partition; a second sensor turns the triage pipeline's proposal into a pending approval and, only once a token bound to
    that exact proposal is present, executes the allow-listed actions and records whether they fixed the failure."""

    name_prefix: str = Field(default="run_remediation", description="Prefix for the sensor names (<prefix>_failure_sensor, <prefix>_approval_sensor).")
    evidence_dir: str = Field(description="Where evidence JSON is written (<dir>/<run_id>.json): a local path or fsspec URI (s3://, gs://, abfs://). The triage pipeline reads it (agentic_pipeline: `source: {kind: file, path: <dir>/{partition_key}.json}`, local paths only).")
    proposals_dir: str = Field(description="Where the triage pipeline writes proposals (<dir>/<run_id>.json): {run_id, summary, root_cause, confidence, actions:[{kind, params, reason}]}.")
    approval_dir: str = Field(description="Approval directory (same layout as agentic_pipeline's `approve` op): pending records in <dir>/pending/<run_id>.json, tokens at <dir>/<run_id>.json. agent_review_queue can list it.")
    results_dir: str = Field(description="Where execution results + outcomes are recorded (<dir>/<run_id>.json).")
    tickets_dir: Optional[str] = Field(default=None, description="Where `open_ticket` writes ticket JSON. Default: <results_dir>/tickets.")
    triage_job_name: str = Field(default="run_failure_triage_job", description="Name of the TRIAGE job this component defines over `triage_asset_selection` (a job request per failed run, partition = the failed run id).")
    triage_asset_selection: str = Field(description="Asset selection string for the triage job (e.g. 'key:run_triage*' or 'group:triage'): the dynamically partitioned assets of your triage pipeline.")
    triage_run_config: Optional[Dict[str, Any]] = Field(default=None, description="Run config passed to every triage run request.")
    dynamic_partitions_name: str = Field(default="run_failures", description="Name of the DynamicPartitionsDefinition the failed run ids are registered on. The triage assets must use this same name (agentic_pipeline: partition_type: dynamic, dynamic_partition_name: <this>).")
    only_jobs: Optional[List[str]] = Field(default=None, description="Only triage failures of these jobs (default: all jobs in all code locations).")
    ignore_jobs: Optional[List[str]] = Field(default=None, description="Never triage failures of these jobs. The triage job itself is always ignored (no recursion).")
    detect: str = Field(default="events", description="How failed runs are detected. `events` (default): Dagster's run_failure_sensor, which listens to RUN_FAILURE events. `runs`: a regular sensor polling run RECORDS with status FAILURE -- also sees runs written straight to run storage without a failure event (e.g. `add_historical_run(..., status=FAILURE)`). `both`: both sensors; a run is never triaged twice. dagster-airlift's Airflow runs DO emit the event, so `events` already covers them (see the README).")
    only_tags: Optional[Dict[str, str]] = Field(default=None, description="Only triage failed runs carrying ALL of these tags: {tag_key: value}; value '*' (or '') matches any value. E.g. {'dagster-airlift/airflow-dag-id': '*'} or {'dagster/external_job_source': 'airflow'}. Applies to both detection modes.")
    runs_lookback_hours: float = Field(default=24.0, description="detect runs|both: how far back (by the run's last update time) the run-record sensor looks for failed runs it has not judged yet. A failure persisted later than this after it happened is not seen.")
    runs_max_per_tick: int = Field(default=10, description="detect runs|both: at most this many new failed runs are triaged per tick (oldest first); the rest wait for the next tick.")
    triage_remediation_runs: bool = Field(default=False, description="Also triage failures of runs this component launched. Off by default so a failing retry cannot start another triage.")
    max_triage_per_hour: int = Field(default=10, description="Rate limit: at most this many triage runs requested per rolling hour.")
    cooldown_minutes: float = Field(default=60.0, description="After a triage for (job, error signature), further failures with the same signature are suppressed for this long.")
    history_runs: int = Field(default=10, description="How many recent runs of the same job to include in the evidence.")
    recent_outcomes: int = Field(default=5, description="How many recent remediation outcomes (same job or error signature) to include in the evidence.")
    log_tail_chars: int = Field(default=2000, description="Max characters of each step's error message kept (the tail).")
    redact_patterns: Optional[List[str]] = Field(default=None, description="Extra regexes to redact from evidence, results, tickets and webhook payloads (on top of the built-in token/key/password/Authorization/base64/env-secret rules).")
    allowed_actions: List[str] = Field(default_factory=lambda: list(DEFAULT_ALLOWED_ACTIONS), description="Allow-list of action kinds a proposal may use: retry_run, rerun_partition, materialize_assets, open_ticket, notify. Anything else is rejected with a recorded reason. Default: the safe ones (retry_run, rerun_partition, open_ticket).")
    allowed_materialize_assets: Optional[List[str]] = Field(default=None, description="When materialize_assets is allowed, restrict it to these asset keys (e.g. ['raw/orders']). Default: any asset in the code location.")
    max_actions_per_proposal: int = Field(default=5, description="Actions beyond this count in one proposal are rejected.")
    max_retries_per_run: int = Field(default=2, description="retry_run launches at most this many retries for one failure (the chain's root run).")
    remediation_jobs: Optional[Dict[str, str]] = Field(default=None, description="The jobs retry_run / rerun_partition / materialize_assets launch runs through: {name: asset selection string}, e.g. {core: 'group:core', daily: 'group:daily'}. Each becomes job `<name_prefix>_remediate_<name>`. A Dagster sensor can only request runs for the jobs it targets, and one job needs assets with one partitions definition (unpartitioned assets mix freely), so group your assets accordingly. Without this, those three actions are rejected with a recorded reason (open_ticket / notify still work).")
    approval_step_id: str = Field(default="remediation", description="Step id used in the approval fingerprint / pending record (shown by agent_review_queue).")
    approval_max_age_hours: Optional[float] = Field(default=None, description="An approval token older than this is expired. Default: no expiry.")
    auto_approve: Optional[Dict[str, Any]] = Field(default=None, description="OFF by default. {kinds: [retry_run], max_per_hour: 3, min_confidence: 0.8}: proposals whose every action is in `kinds` and whose confidence is high enough are approved by the policy itself (token approver: auto_approve), at most max_per_hour per rolling hour.")
    webhook_url_env_var: Optional[str] = Field(default=None, description="Name of the environment variable holding the webhook URL used by `notify` (never logged or written).")
    ticket_webhook_url_env_var: Optional[str] = Field(default=None, description="Name of the environment variable holding a webhook URL that also receives each `open_ticket` ticket (never logged or written).")
    webhook_timeout_seconds: float = Field(default=10.0, description="Timeout for webhook POSTs.")
    outcome_check_after_minutes: float = Field(default=30.0, description="How long after an action launched runs to judge whether it fixed the failure (outcome: fixed | not_fixed).")
    store_resource_key: Optional[str] = Field(default=None, description="Optional resource key of a ContextReceiptStore: when a proposal carries a `receipt_id`, the outcome is also recorded with `record_outcome`. Skipped cleanly when absent.")
    minimum_interval_seconds: int = Field(default=30, description="Sensor tick interval.")
    default_status: str = Field(default="STOPPED", description="Initial sensor status: STOPPED (default) or RUNNING.")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=("only_jobs", "ignore_jobs", "allowed_actions", "allowed_materialize_assets", "redact_patterns"),
                                   csv=(), tags=("only_tags",))

    # -- validation shared by build_defs and tests --
    def validate_config(self) -> None:
        bad = [a for a in self.allowed_actions if a not in KNOWN_ACTIONS]
        if bad:
            raise ValueError(f"allowed_actions has unknown kinds {bad}; known: {list(KNOWN_ACTIONS)}")
        if self.default_status.upper() not in ("RUNNING", "STOPPED"):
            raise ValueError("default_status must be RUNNING or STOPPED")
        for p in self.redact_patterns or []:
            re.compile(p)
        if "notify" in self.allowed_actions and not self.webhook_url_env_var:
            raise ValueError("allowed_actions includes notify but webhook_url_env_var is not set")
        if self.auto_approve is not None:
            aa = self.auto_approve
            extra = set(aa) - {"kinds", "max_per_hour", "min_confidence"}
            if extra:
                raise ValueError(f"auto_approve has unknown keys {sorted(extra)}")
            kinds = aa.get("kinds")
            if not isinstance(kinds, list) or not kinds:
                raise ValueError("auto_approve.kinds must be a non-empty list")
            if any(k not in self.allowed_actions for k in kinds):
                raise ValueError("auto_approve.kinds must be a subset of allowed_actions")
            if int(aa.get("max_per_hour", 3)) < 1:
                raise ValueError("auto_approve.max_per_hour must be >= 1")
            if not 0.0 <= float(aa.get("min_confidence", 0.8)) <= 1.0:
                raise ValueError("auto_approve.min_confidence must be in [0, 1]")
        for n in (self.remediation_jobs or {}):
            if not re.match(r"^[A-Za-z0-9_]+$", n):
                raise ValueError(f"remediation_jobs name {n!r} must be letters/digits/underscores")
        if self.max_triage_per_hour < 1:
            raise ValueError("max_triage_per_hour must be >= 1")
        if self.detect not in ("events", "runs", "both"):
            raise ValueError("detect must be one of events, runs, both")
        if self.runs_lookback_hours <= 0 or self.runs_max_per_tick < 1:
            raise ValueError("runs_lookback_hours must be > 0 and runs_max_per_tick >= 1")

    def partitions_def(self) -> "dg.DynamicPartitionsDefinition":
        """The dynamic partitions definition failed run ids are registered on (share it with your triage assets)."""
        return dg.DynamicPartitionsDefinition(name=self.dynamic_partitions_name)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        self.validate_config()
        engine = _Engine(self)
        pdef = self.partitions_def()
        status = dg.DefaultSensorStatus.RUNNING if self.default_status.upper() == "RUNNING" else dg.DefaultSensorStatus.STOPPED
        triage_job = dg.define_asset_job(name=self.triage_job_name, selection=self.triage_asset_selection)

        sensors: List[Any] = []
        if self.detect in ("events", "both"):
            @dg.run_failure_sensor(
                name=f"{self.name_prefix}_failure_sensor", monitor_all_code_locations=True, request_job=triage_job,
                minimum_interval_seconds=self.minimum_interval_seconds, default_status=status,
                description="Gathers redacted evidence for a failed run and requests a triage run on a dynamic partition named after the run id.",
            )
            def failure_sensor(context: dg.RunFailureSensorContext):
                return engine.failure_tick(context, self.triage_job_name, pdef)

            sensors.append(failure_sensor)

        if self.detect in ("runs", "both"):
            @dg.sensor(
                name=f"{self.name_prefix}_failed_runs_sensor", job=triage_job,
                minimum_interval_seconds=self.minimum_interval_seconds, default_status=status,
                description="Polls run records for FAILURE runs (including ones recorded without a failure event) and requests a triage run per new failure.",
            )
            def failed_runs_sensor(context: dg.SensorEvaluationContext):
                return engine.runs_tick(context, self.triage_job_name, pdef)

            sensors.append(failed_runs_sensor)

        resource_keys = {self.store_resource_key} if self.store_resource_key else set()
        rem_jobs = [dg.define_asset_job(name=f"{self.name_prefix}_remediate_{n}", selection=sel)
                    for n, sel in (self.remediation_jobs or {}).items()]

        @dg.sensor(
            name=f"{self.name_prefix}_approval_sensor", jobs=rem_jobs or None,
            minimum_interval_seconds=self.minimum_interval_seconds, default_status=status,
            required_resource_keys=resource_keys or None,
            description="Turns triage proposals into pending approvals and executes the approved, allow-listed actions.",
        )
        def approval_sensor(context: dg.SensorEvaluationContext):
            return engine.approval_tick(context)

        return dg.Definitions(jobs=[triage_job, *rem_jobs], sensors=[*sensors, approval_sensor])
