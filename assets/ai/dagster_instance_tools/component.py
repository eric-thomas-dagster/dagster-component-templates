"""DagsterInstanceToolsComponent -- "ask the pipeline": READ-ONLY tools over a Dagster instance for agents.

An agent can answer "why is this asset stale / failing?" with REAL Dagster data: recent failed runs, a run's steps and
(redacted) logs, an asset's last materialization / partitions / check results, lineage, stale assets, and (optionally) how
answers built from an asset turned out (a ContextReceiptStore).

How agents consume it (verified against agentic_pipeline in this repo): `tool_use_loop` / `mcp_call` / `agent_call` obtain
tools ONLY from MCP servers (stdio / http / sse / fastmcp) -- there is no plain-Python-callable tool mechanism. So this file
is BOTH
  1. an importable toolset (`DagsterInstanceTools`, plus the `DagsterInstanceToolsComponent` that registers it as a Dagster
     resource), and
  2. a self-contained stdio MCP server: `python <path-to>/component.py` (reads DAGSTER_HOME to find the instance).
See README.md for the exact agentic_pipeline YAML.

Tools (all return compact JSON-able dicts; errors come back as {"error": ...}, never exceptions):
  list_recent_failures, get_run_summary, get_run_logs, asset_status, asset_lineage, stale_assets, receipt_stats.

Safety: tools only call read APIs on the instance. Log lines / error text / tag values pass through `redact_text`
(tokens, passwords, Authorization headers, well-known key shapes, long base64/hex blobs). `allow` is a hard allow-list,
`max_result_chars` bounds every result, and every call writes one audit log line (tool + args, never results).
"""

import json
import logging
import math
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional

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

import re  # noqa: E402

_AUDIT = logging.getLogger("dagster_instance_tools.audit")

ALL_TOOLS = (
    "list_recent_failures", "get_run_summary", "get_run_logs", "asset_status", "asset_lineage", "stale_assets", "receipt_stats",
)

# ── redaction ───────────────────────────────────────────────────────────────────────────────────────────────────────
_REDACTED = "[REDACTED]"
_SECRET_KEY = r"(?:[\w.-]*(?:token|secret|passw(?:or)?d|passwd|pwd|api[_-]?key|apikey|access[_-]?key|private[_-]?key|client[_-]?secret|credential|authorization|auth|cookie|session[_-]?id)[\w.-]*)"
_REDACT_PATTERNS = [
    # Authorization: Bearer xxx / Basic xxx / Token xxx (header or log line) -- keep the scheme, drop the credential
    (re.compile(r"(?i)\b(authorization\s*[:=]\s*)(?:(bearer|basic|token|digest|negotiate)\s+)?[^\s,;'\"]+(?:\s+[^\s,;'\"]{8,})?"),
     lambda m: f"{m.group(1)}{(m.group(2) + ' ') if m.group(2) else ''}{_REDACTED}"),
    (re.compile(r"(?i)\b(bearer)\s+[A-Za-z0-9._~+/=-]{8,}"), lambda m: f"{m.group(1)} {_REDACTED}"),
    # url userinfo: scheme://user:pass@host
    (re.compile(r"(?i)\b([a-z][a-z0-9+.-]*://[^\s:/@]+:)[^\s@/]+(@)"), lambda m: f"{m.group(1)}{_REDACTED}{m.group(2)}"),
    # key=value / key: value / "key": "value" for secret-looking keys
    (re.compile(r"(?i)(['\"]?" + _SECRET_KEY + r"['\"]?\s*[:=]\s*)(['\"]?)(?!\[REDACTED\]|(?:bearer|basic|token|digest|negotiate)\s)([^\s,;'\"}\]]+)(\2)"),
     lambda m: f"{m.group(1)}{m.group(2)}{_REDACTED}{m.group(4)}"),
    # well-known credential shapes
    (re.compile(r"\bsk-[A-Za-z0-9_-]{16,}"), lambda m: _REDACTED),
    (re.compile(r"\b(?:ghp|gho|ghu|ghs|ghr|github_pat)_[A-Za-z0-9_]{16,}"), lambda m: _REDACTED),
    (re.compile(r"\bxox[abprs]-[A-Za-z0-9-]{10,}"), lambda m: _REDACTED),
    (re.compile(r"\b(?:AKIA|ASIA)[0-9A-Z]{16}\b"), lambda m: _REDACTED),
    (re.compile(r"\bAIza[0-9A-Za-z_-]{30,}"), lambda m: _REDACTED),
    (re.compile(r"\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]*"), lambda m: _REDACTED),  # JWT
    (re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----.*?(?:-----END [A-Z ]*PRIVATE KEY-----|$)", re.S), lambda m: _REDACTED),
]
# long opaque blob: 32+ base64/hex-ish chars containing BOTH a letter and a digit, not part of a path/identifier
_BLOB = re.compile(r"(?<![A-Za-z0-9+/=_.\-])[A-Za-z0-9+/_]{32,}={0,2}(?![A-Za-z0-9+/=_.\-])")


def _blob_sub(m: "re.Match[str]") -> str:
    s = m.group(0)
    return _REDACTED if (re.search(r"[A-Za-z]", s) and re.search(r"\d", s)) else s


def redact_text(text: Any) -> str:
    """Mask credential-looking substrings in free text (log lines, error messages). Never raises."""
    if text is None:
        return ""
    s = text if isinstance(text, str) else str(text)
    for pat, repl in _REDACT_PATTERNS:
        s = pat.sub(repl, s)
    return _BLOB.sub(_blob_sub, s)


_NO_REDACT_KEYS = frozenset({"data_version", "code_version", "run_id", "last_run_id", "asset_key", "asset_keys", "step_key", "failed_steps"})
_SECRET_KEY_RE = re.compile(_SECRET_KEY, re.I)


def redact_obj(obj: Any, _key: Optional[str] = None) -> Any:
    """Deep-redact every string in a JSON-able structure. Values under secret-looking dict keys are masked outright;
    identifier fields (run ids, data/code versions, asset/step keys) are left alone so results stay useful."""
    if isinstance(obj, str):
        return obj if _key in _NO_REDACT_KEYS else redact_text(obj)
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            ks = str(k)
            if isinstance(v, (str, int, float)) and not isinstance(v, bool) and _SECRET_KEY_RE.fullmatch(ks) and ks not in _NO_REDACT_KEYS:
                out[ks] = _REDACTED
            else:
                out[ks] = redact_obj(v, ks)
        return out
    if isinstance(obj, (list, tuple)):
        return [redact_obj(v, _key) for v in obj]
    return obj


# ── small helpers ───────────────────────────────────────────────────────────────────────────────────────────────────
class ToolError(Exception):
    """A user-facing, safe error message (returned to the agent as {"error": ...})."""


def _iso(ts: Optional[float]) -> Optional[str]:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(timespec="seconds") if ts else None


def _first_line(s: Any, n: int = 300) -> str:
    line = (str(s or "").strip().splitlines() or [""])[0]
    return redact_text(line)[:n]


def _num(value: Any, name: str, lo: float, hi: float, default: float) -> float:
    if value is None:
        return default
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        raise ToolError(f"{name} must be a number")
    try:
        v = float(value)
    except ValueError:
        raise ToolError(f"{name} must be a number, got {value!r}")
    if math.isnan(v):
        raise ToolError(f"{name} must be a number")
    return max(lo, min(hi, v))


def parse_asset_key(value: Any) -> "dg.AssetKey":
    """'a/b/c', 'a.b.c' is NOT split (dots are legal in names); a JSON list '["a","b"]' also works."""
    if isinstance(value, (list, tuple)) and value and all(isinstance(p, str) and p for p in value):
        return dg.AssetKey(list(value))
    if not isinstance(value, str) or not value.strip():
        raise ToolError("asset_key is required (e.g. 'my_asset' or 'schema/table')")
    v = value.strip()
    if v.startswith("["):
        try:
            return parse_asset_key(json.loads(v))
        except (ValueError, ToolError):
            raise ToolError(f"asset_key {value!r} is not a valid JSON list of strings")
    parts = [p for p in v.split("/")]
    if any(not p for p in parts):
        raise ToolError(f"asset_key {value!r} has an empty path segment")
    return dg.AssetKey(parts)


def _key_str(key: "dg.AssetKey") -> str:
    return "/".join(key.path)


def _error_chain(err: Any) -> Dict[str, Any]:
    """SerializableErrorInfo -> innermost cause's class + first message line (the wrapper class is noise)."""
    if err is None:
        return {"error_class": None, "error_message": None}
    inner = err
    for _ in range(10):
        nxt = getattr(inner, "cause", None) or getattr(inner, "context", None)
        if nxt is None:
            break
        inner = nxt
    msg = getattr(inner, "message", "") or ""
    # SerializableErrorInfo.message is "ValueError: boom\n"; strip the leading class prefix
    cls = getattr(inner, "cls_name", None)
    first = _first_line(msg)
    if cls and first.startswith(f"{cls}:"):
        first = first[len(cls) + 1:].strip()
    return {"error_class": cls, "error_message": first}


_LEVELS = {"DEBUG": 10, "INFO": 20, "WARNING": 30, "WARN": 30, "ERROR": 40, "CRITICAL": 50}


# ── the toolset ─────────────────────────────────────────────────────────────────────────────────────────────────────
class DagsterInstanceTools:
    """Read-only tools over a DagsterInstance. `call(tool, args)` is the one safe entry point (allow-list, audit, size cap,
    redaction, error shaping); the `t_*` methods are the raw implementations."""

    def __init__(
        self,
        instance: "dg.DagsterInstance",
        *,
        definitions: Optional["dg.Definitions"] = None,
        allow: Optional[List[str]] = None,
        max_result_chars: int = 8000,
        receipt_store: Any = None,
        audit: Optional[Callable[[str], None]] = None,
    ):
        self.instance = instance
        self._definitions = definitions
        self._asset_graph = None
        if allow is None:
            allow = list(ALL_TOOLS)
        unknown = sorted(set(allow) - set(ALL_TOOLS))
        if unknown:
            raise ValueError(f"unknown tool(s) in allow: {unknown}; valid: {list(ALL_TOOLS)}")
        self.allow = frozenset(allow)
        self.max_result_chars = max(200, int(max_result_chars))
        self.receipt_store = receipt_store
        self._audit = audit or (lambda line: _AUDIT.info(line))

    # -- dispatch ------------------------------------------------------------------------------------------------
    def tool_names(self) -> List[str]:
        return [t for t in ALL_TOOLS if t in self.allow]

    def call(self, tool: str, args: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        args = dict(args or {})
        try:
            self._audit(f"dagster_instance_tools call tool={tool} args={redact_text(json.dumps(args, default=str))[:300]}")
        except Exception:  # an audit sink must never break a call
            pass
        if tool not in ALL_TOOLS:
            return {"error": f"unknown tool {tool!r}; available: {self.tool_names()}"}
        if tool not in self.allow:
            return {"error": f"tool {tool!r} is not enabled (allowed: {self.tool_names()})"}
        try:
            result = getattr(self, f"t_{tool}")(**args)
        except ToolError as e:
            return {"error": redact_text(str(e))}
        except TypeError as e:
            return {"error": f"bad arguments for {tool}: {redact_text(str(e))}"}
        except Exception as e:  # noqa: BLE001 -- surface as data, the agent loop must not crash
            return {"error": f"{tool} failed: {type(e).__name__}: {_first_line(e)}"}
        return self._fit(redact_obj(result))

    def _fit(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """Bound the serialized size: repeatedly halve the longest list / clip the longest string; last resort = preview."""
        cap = self.max_result_chars
        if len(json.dumps(result, default=str)) <= cap:
            return result
        result = json.loads(json.dumps(result, default=str))
        for _ in range(60):
            if len(json.dumps(result)) <= cap - 40:
                result["truncated"] = True
                return result
            if not self._shrink_once(result):
                break
        text = json.dumps(result)[: max(0, cap - 120)]
        return {"truncated": True, "preview": text}

    @staticmethod
    def _shrink_once(root: Any) -> bool:
        best: Any = None  # (size, container, key, kind)
        stack = [root]
        while stack:
            node = stack.pop()
            items = node.items() if isinstance(node, dict) else enumerate(node)
            for k, v in items:
                if isinstance(v, list) and len(v) > 1:
                    size = len(json.dumps(v))
                    if best is None or size > best[0]:
                        best = (size, node, k, "list")
                if isinstance(v, str) and len(v) > 200:
                    if best is None or len(v) > best[0]:
                        best = (len(v), node, k, "str")
                if isinstance(v, (dict, list)):
                    stack.append(v)
        if best is None:
            return False
        _, node, k, kind = best
        if kind == "list":
            node[k] = node[k][: max(1, len(node[k]) // 2)]
        else:
            node[k] = node[k][: max(100, len(node[k]) // 2)] + "...[clipped]"
        return True

    # -- definitions / graph -------------------------------------------------------------------------------------
    def _graph(self):
        if self._definitions is None:
            return None
        if self._asset_graph is None:
            self._asset_graph = self._definitions.resolve_asset_graph()
        return self._asset_graph

    def _partitions_def(self, key: "dg.AssetKey"):
        g = self._graph()
        if g is None or not g.has(key):
            return None
        return g.get(key).partitions_def

    # -- run helpers ---------------------------------------------------------------------------------------------
    def _get_run(self, run_id: Any):
        if not isinstance(run_id, str) or not run_id.strip():
            raise ToolError("run_id is required")
        run = self.instance.get_run_by_id(run_id.strip())
        if run is None:
            raise ToolError(f"run not found: {run_id!r}")
        return run

    def _step_assets(self, run_id: str) -> Dict[str, List[str]]:
        """step_key -> asset keys, from the run's ASSET_MATERIALIZATION_PLANNED events (works without definitions)."""
        out: Dict[str, List[str]] = {}
        try:
            recs = self.instance.get_records_for_run(run_id, of_type=dg.DagsterEventType.ASSET_MATERIALIZATION_PLANNED).records
        except Exception:
            return out
        for r in recs:
            ev = r.event_log_entry.dagster_event
            data = getattr(ev, "event_specific_data", None)
            if ev is not None and getattr(data, "asset_key", None) is not None:
                out.setdefault(ev.step_key or "?", [])
                ks = _key_str(data.asset_key)
                if ks not in out[ev.step_key or "?"]:
                    out[ev.step_key or "?"].append(ks)
        return out

    def _failed_steps(self, run_id: str, step_assets: Dict[str, List[str]]) -> List[Dict[str, Any]]:
        out = []
        seen = set()
        recs = self.instance.get_records_for_run(run_id, of_type=dg.DagsterEventType.STEP_FAILURE).records
        for r in recs:
            ev = r.event_log_entry.dagster_event
            sk = ev.step_key if ev else None
            if sk in seen:
                continue
            seen.add(sk)
            err = getattr(ev.event_specific_data, "error", None) if ev else None
            out.append({"step_key": sk, **_error_chain(err), "asset_keys": step_assets.get(sk, [])})
        return out

    def _run_asset_failures(self, run_id: str) -> List[Dict[str, Any]]:
        """Per-asset failure observations (and any real failure events) the run itself logged: reason, error class, will_retry."""
        out: List[Dict[str, Any]] = []
        seen = set()
        try:
            for et in (dg.DagsterEventType.ASSET_FAILED_TO_MATERIALIZE, dg.DagsterEventType.ASSET_OBSERVATION):
                for r in self.instance.get_records_for_run(run_id, of_type=et).records:
                    ev = r.event_log_entry.dagster_event
                    if et == dg.DagsterEventType.ASSET_OBSERVATION:
                        o = ev.event_specific_data.asset_observation
                        md = {k: getattr(v, "value", v) for k, v in (o.metadata or {}).items()}
                        if md.get("failure_status") != "failed":
                            continue
                        row = {"asset_key": _key_str(o.asset_key), "partition": o.partition, "reason": md.get("failure_reason"),
                               "error_class": md.get("error_class"), "error_message": _first_line(md.get("error_message") or ""),
                               "failed_step": md.get("failed_step")}
                        wr = md.get("will_retry")
                        row["will_retry"] = (wr == "true") if wr in ("true", "false") else None
                    else:
                        d = ev.event_specific_data
                        err = _error_chain(getattr(d, "error", None))
                        row = {"asset_key": _key_str(d.asset_key), "partition": d.partition, "reason": getattr(d.reason, "value", None),
                               **err, "failed_step": ev.step_key, "will_retry": d.asset_materialization_failure.will_retry}
                    sig = (row["asset_key"], row["partition"], row["error_class"], row["failed_step"])
                    if sig not in seen:  # one row per failing attempt would repeat under retries
                        seen.add(sig)
                        out.append(row)
        except Exception:
            pass
        return out

    # -- tools ---------------------------------------------------------------------------------------------------
    def t_list_recent_failures(self, hours: Any = 24, limit: Any = 10) -> Dict[str, Any]:
        hours_f = _num(hours, "hours", 0.01, 24 * 90, 24)
        n = int(_num(limit, "limit", 1, 50, 10))
        since = datetime.now(timezone.utc) - timedelta(hours=hours_f)
        records = self.instance.get_run_records(
            dg.RunsFilter(statuses=[dg.DagsterRunStatus.FAILURE], created_after=since), limit=n, order_by="create_timestamp"
        )
        runs = []
        for rec in records:
            run = rec.dagster_run
            sa = self._step_assets(run.run_id)
            failed = self._failed_steps(run.run_id, sa)
            keys = sorted({a for f in failed for a in f["asset_keys"]} or {_key_str(k) for k in (run.asset_selection or [])})
            runs.append({
                "run_id": run.run_id, "job_name": run.job_name, "status": run.status.value,
                "started_at": _iso(rec.start_time), "ended_at": _iso(rec.end_time),
                "failed_steps": failed, "asset_keys": keys, "asset_failures": self._run_asset_failures(run.run_id),
            })
        return {"window_hours": hours_f, "count": len(runs), "runs": runs}

    def t_get_run_summary(self, run_id: Any = None) -> Dict[str, Any]:
        run = self._get_run(run_id)
        rec = next(iter(self.instance.get_run_records(dg.RunsFilter(run_ids=[run.run_id]), limit=1)), None)
        sa = self._step_assets(run.run_id)
        steps = []
        for s in self.instance.get_run_step_stats(run.run_id)[:100]:
            row = {"step_key": s.step_key, "status": s.status.value if s.status else None,
                   "duration_s": round(s.end_time - s.start_time, 3) if s.end_time and s.start_time else None,
                   "attempts": len(s.attempts_list) if s.attempts_list else None, "asset_keys": sa.get(s.step_key, [])}
            steps.append(row)
        tags = {}
        for k, v in sorted((run.tags or {}).items())[:30]:
            tags[str(k)] = _REDACTED if _SECRET_KEY_RE.fullmatch(str(k)) else redact_text(str(v))[:200]
        failed = self._failed_steps(run.run_id, sa)
        for f in failed:  # one extra, bounded, redacted error excerpt per failed step (the wrapper trace is long; keep the tail)
            f["error_detail"] = self._step_error_detail(run.run_id, f["step_key"])
        return {
            "run_id": run.run_id, "job_name": run.job_name, "status": run.status.value,
            "created_at": _iso(rec.create_timestamp.timestamp() if rec and hasattr(rec.create_timestamp, "timestamp") else None),
            "started_at": _iso(rec.start_time) if rec else None, "ended_at": _iso(rec.end_time) if rec else None,
            "duration_s": round(rec.end_time - rec.start_time, 3) if rec and rec.end_time and rec.start_time else None,
            "asset_keys": sorted({a for v in sa.values() for a in v}),
            "steps": steps, "failed_steps": failed, "tags": tags,
        }

    def _step_error_detail(self, run_id: str, step_key: Optional[str], max_chars: int = 1200) -> str:
        for r in self.instance.get_records_for_run(run_id, of_type=dg.DagsterEventType.STEP_FAILURE).records:
            ev = r.event_log_entry.dagster_event
            if ev and ev.step_key == step_key:
                err = getattr(ev.event_specific_data, "error", None)
                inner = err
                for _ in range(10):
                    nxt = getattr(inner, "cause", None)
                    if nxt is None:
                        break
                    inner = nxt
                text = (getattr(inner, "to_string", lambda: "")() or "").strip()
                return redact_text(text[-max_chars:])
        return ""

    def t_get_run_logs(self, run_id: Any = None, level: Any = "INFO", max_lines: Any = 100) -> Dict[str, Any]:
        run = self._get_run(run_id)
        lvl_name = str(level or "INFO").upper()
        if lvl_name not in _LEVELS:
            raise ToolError(f"level must be one of {sorted(_LEVELS)}")
        min_level = _LEVELS[lvl_name]
        n = int(_num(max_lines, "max_lines", 1, 500, 100))
        lines = []
        total = 0
        for e in self.instance.all_logs(run.run_id):
            if (e.level or 0) < min_level:
                continue
            msg = e.user_message or ""
            if e.dagster_event is None and not msg:
                continue
            total += 1
            lines.append(f"{_iso(e.timestamp)} {logging.getLevelName(e.level)} {e.step_key or '-'}: {redact_text(msg)[:500]}")
        return {"run_id": run.run_id, "level": lvl_name, "total_matching": total, "returned": min(n, total),
                "lines": lines[-n:], "note": "event-log messages only (compute stdout/stderr captures are not included)"}

    def _materialization_info(self, key: "dg.AssetKey"):
        recs = self.instance.get_asset_records([key])
        entry = recs[0].asset_entry if recs else None
        rec = entry.last_materialization_record if entry else None
        return entry, rec

    def _check_results(self, key: "dg.AssetKey") -> List[Dict[str, Any]]:
        names = set()
        g = self._graph()
        if g is not None and g.has(key):
            names |= {ck.name for ck in g.asset_check_keys if ck.asset_key == key}
        # without definitions: check names come from the events of the asset's last materialization run ...
        _, mrec = self._materialization_info(key)
        if mrec is not None and mrec.run_id:
            for et in (dg.DagsterEventType.ASSET_CHECK_EVALUATION, dg.DagsterEventType.ASSET_CHECK_EVALUATION_PLANNED):
                try:
                    for r in self.instance.get_records_for_run(mrec.run_id, of_type=et).records:
                        d = r.event_log_entry.dagster_event
                        data = getattr(d, "event_specific_data", None)
                        if getattr(data, "asset_key", None) == key:
                            names.add(getattr(data, "check_name", None) or getattr(data, "name", None))
                except Exception:
                    pass
        # ... plus a best-effort scan of recent check evaluations (covers checks that ran in their own runs)
        try:
            import warnings
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                for r in self.instance.get_event_records(dg.EventRecordsFilter(dg.DagsterEventType.ASSET_CHECK_EVALUATION), limit=500):
                    data = r.event_log_entry.dagster_event.event_specific_data
                    if data.asset_key == key:
                        names.add(data.check_name)
        except Exception:
            pass
        names.discard(None)
        out = []
        summaries = self.instance.event_log_storage.get_asset_check_summary_records([dg.AssetCheckKey(key, n) for n in sorted(names)]) if names else {}
        for ck, summ in summaries.items():
            last = summ.last_check_execution_record
            row: Dict[str, Any] = {"check": ck.name, "status": last.status.value if last else "NOT_RUN", "run_id": summ.last_run_id}
            ev = getattr(getattr(last, "event", None), "dagster_event", None) if last else None
            data = getattr(ev, "event_specific_data", None)
            if data is not None:
                row["passed"] = data.passed
                row["severity"] = data.severity.value if data.severity else None
                row["description"] = _first_line(data.description or "")
                row["metadata"] = {k: str(getattr(v, "value", v))[:100] for k, v in list((data.metadata or {}).items())[:10]}
            out.append(row)
        return out

    def _latest_failed_run_after(self, key: "dg.AssetKey", after_ts: Optional[float]) -> Optional[Dict[str, Any]]:
        """Most recent FAILED run that planned this asset after its last good materialization (works without definitions)."""
        recs = self.instance.fetch_planned_materializations(key, limit=25).records
        for r in recs:  # newest first
            if after_ts and r.timestamp <= after_ts:
                break
            run = self.instance.get_run_by_id(r.run_id) if r.run_id else None
            if run is not None and run.status == dg.DagsterRunStatus.FAILURE:
                sa = self._step_assets(run.run_id)
                steps = [f for f in self._failed_steps(run.run_id, sa) if _key_str(key) in f["asset_keys"]] or self._failed_steps(run.run_id, sa)
                return {"run_id": run.run_id, "at": _iso(r.timestamp), "failed_steps": steps[:3]}
        return None

    # -- failures: Dagster (OSS) never emits ASSET_FAILED_TO_MATERIALIZE, so a failed asset is visible only as a failed RUN. Components that
    # follow the failure observation contract (docs/failure_observation_contract.md) log an AssetObservation with failure_* metadata
    # before re-raising; this reads either source, newest evidence first.
    def _materialized_after(self, key: "dg.AssetKey", partition: Optional[str], ts: float) -> bool:
        """True if the asset (that partition, when the failure names one) materialized after `ts`."""
        try:
            f = dg.EventRecordsFilter(event_type=dg.DagsterEventType.ASSET_MATERIALIZATION, asset_key=key, after_cursor=None,
                                      asset_partitions=[partition] if partition else None)
            recs = self.instance.get_event_records(f, limit=1, ascending=False)
            return bool(recs) and recs[0].timestamp > ts
        except Exception:
            return False

    def _last_failure(self, key: "dg.AssetKey", after_ts: Optional[float] = None) -> Optional[Dict[str, Any]]:
        """The most recent failure of `key` newer than `after_ts` (its last good materialization), or None.
        1. a real ASSET_FAILED_TO_MATERIALIZE event (forward compatible: only storages with can_store_asset_failure_events keep one);
        2. else the newest failure observation (failure_status == 'failed') logged by a component following the contract;
        3. else the failed run that planned the asset (no reason / will_retry: Dagster does not record them)."""
        try:
            recs = self.instance.get_event_records(
                dg.EventRecordsFilter(event_type=dg.DagsterEventType.ASSET_FAILED_TO_MATERIALIZE, asset_key=key), limit=5, ascending=False)
            for r in recs or []:
                data = r.event_log_entry.dagster_event.event_specific_data
                fail = data.asset_materialization_failure
                err = _error_chain(getattr(data, "error", None))
                # an unpartitioned failure is cleared by any later materialization; a partition's only by that partition's own
                if fail.partition is None and after_ts and r.timestamp <= after_ts:
                    continue
                if self._materialized_after(key, fail.partition, r.timestamp):
                    continue
                return {"source": "asset_failed_to_materialize_event", "asset_key": _key_str(key), "at": _iso(r.timestamp),
                        "partition": fail.partition, "reason": getattr(fail.reason, "value", str(fail.reason)),
                        "error_class": err["error_class"], "error_message": err["error_message"], "run_id": r.run_id,
                        "will_retry": fail.will_retry}
        except Exception:
            pass
        try:
            recs = self.instance.get_event_records(
                dg.EventRecordsFilter(event_type=dg.DagsterEventType.ASSET_OBSERVATION, asset_key=key), limit=100, ascending=False)
            for r in recs or []:
                o = r.event_log_entry.dagster_event.asset_observation_data.asset_observation
                md = {k: getattr(v, "value", v) for k, v in (o.metadata or {}).items()}
                if md.get("failure_status") != "failed":
                    continue
                if o.partition is None and after_ts and r.timestamp <= after_ts:
                    continue
                if self._materialized_after(key, o.partition, r.timestamp):
                    continue
                wr = md.get("will_retry")
                return {"source": "failure_observation", "asset_key": _key_str(key), "at": _iso(r.timestamp), "partition": o.partition,
                        "reason": md.get("failure_reason"), "error_class": md.get("error_class"),
                        "error_message": _first_line(md.get("error_message") or ""), "run_id": md.get("run_id") or r.run_id,
                        "failed_step": md.get("failed_step"), "will_retry": (wr == "true") if wr in ("true", "false") else None}
        except Exception:
            pass
        failed = self._latest_failed_run_after(key, after_ts)
        if failed is None:
            return None
        step = (failed.get("failed_steps") or [{}])[0]
        return {"source": "failed_run", "asset_key": _key_str(key), "at": failed["at"], "partition": None, "reason": None,
                "error_class": step.get("error_class"), "error_message": step.get("error_message"), "run_id": failed["run_id"],
                "failed_step": step.get("step_key"), "will_retry": None}

    def t_asset_status(self, asset_key: Any = None) -> Dict[str, Any]:
        key = parse_asset_key(asset_key)
        entry, rec = self._materialization_info(key)
        g = self._graph()
        known_in_defs = bool(g is not None and g.has(key))
        if entry is None and not known_in_defs:
            # an asset that only failed has no asset record in some storages; still check for planned-and-failed runs
            failed = self._latest_failed_run_after(key, None) or self._last_failure(key, None)
            if failed is None:
                return {"asset_key": _key_str(key), "found": False,
                        "message": "no materialization events, no failed runs and not in the loaded definitions"}
        out: Dict[str, Any] = {"asset_key": _key_str(key), "found": True, "in_loaded_definitions": known_in_defs if g is not None else None}
        mat = rec.asset_materialization if rec else None
        tags = (mat.tags or {}) if mat else {}
        out["last_materialization"] = None if rec is None else {
            "at": _iso(rec.timestamp), "run_id": rec.run_id, "partition": rec.partition_key,
            "data_version": tags.get("dagster/data_version"), "code_version": tags.get("dagster/code_version"),
            "age_hours": round((time.time() - rec.timestamp) / 3600, 2),
        }
        out["latest_failed_run_since"] = self._latest_failed_run_after(key, rec.timestamp if rec else None)
        out["last_failure"] = self._last_failure(key, rec.timestamp if rec else None)
        out["partitions"] = self._partition_summary(key)
        out["checks"] = self._check_results(key)
        try:
            fr = self.instance.get_freshness_state_records([key])
            st = fr.get(key) if fr else None
            if st is not None:
                out["freshness"] = {"state": getattr(st.freshness_state, "value", str(st.freshness_state)), "updated_at": _iso(getattr(st, "updated_at", None) and st.updated_at.timestamp())}
        except Exception:
            pass
        return out

    def _partition_summary(self, key: "dg.AssetKey") -> Optional[Dict[str, Any]]:
        pdef = self._partitions_def(key)
        if pdef is None:
            try:
                mat = self.instance.get_materialized_partitions(key)
            except Exception:
                return None
            if not mat:
                return None
            ms = sorted(mat)
            return {"materialized": len(ms), "materialized_sample": ms[-10:], "failed": None, "missing": None,
                    "note": "partition definition unavailable (no definitions loaded): total/missing/failed unknown"}
        keys = list(pdef.get_partition_keys(dynamic_partitions_store=self.instance))
        cap = 20000
        truncated = len(keys) > cap
        keys = keys[-cap:]
        status = self.instance.get_status_by_partition(key, keys, pdef) or {}
        by: Dict[str, List[str]] = {"MATERIALIZED": [], "FAILED": [], "MATERIALIZING": [], "missing": []}
        for k in keys:
            s = status.get(k)
            by[s.value if s is not None else "missing"].append(k)
        return {
            "total": len(keys), "materialized": len(by["MATERIALIZED"]), "failed": len(by["FAILED"]),
            "in_progress": len(by["MATERIALIZING"]), "missing": len(by["missing"]),
            "failed_sample": by["FAILED"][-10:], "missing_sample": by["missing"][:10], "considered_latest_only": truncated,
        }

    def t_asset_lineage(self, asset_key: Any = None, depth: Any = 2, direction: Any = "upstream") -> Dict[str, Any]:
        key = parse_asset_key(asset_key)
        d = int(_num(depth, "depth", 1, 5, 2))
        direction = str(direction or "upstream").lower()
        if direction not in ("upstream", "downstream", "both"):
            raise ToolError("direction must be 'upstream', 'downstream' or 'both'")
        g = self._graph()
        edges: List[Dict[str, str]] = []
        if g is not None and g.has(key):
            def walk(start, step):
                seen, frontier = {start}, [start]
                for _ in range(d):
                    nxt = []
                    for k in frontier:
                        for p in sorted(step(k), key=lambda x: x.path):
                            edges.append({"from": _key_str(p), "to": _key_str(k)} if step is parents else {"from": _key_str(k), "to": _key_str(p)})
                            if p not in seen:
                                seen.add(p)
                                nxt.append(p)
                    frontier = nxt
                return seen - {start}

            def parents(k):
                return g.get(k).parent_keys

            def children(k):
                return g.get(k).child_keys

            up = walk(key, parents) if direction in ("upstream", "both") else set()
            down = walk(key, children) if direction in ("downstream", "both") else set()
            return {"asset_key": _key_str(key), "source": "asset_graph", "depth": d, "direction": direction,
                    "upstream": sorted(_key_str(k) for k in up), "downstream": sorted(_key_str(k) for k in down), "edges": edges[:200]}
        if g is not None:
            raise ToolError(f"asset {_key_str(key)!r} is not in the loaded definitions")
        if direction != "upstream":
            raise ToolError("downstream lineage needs the code-location asset graph: configure `definitions_module` "
                            "(only upstream lineage is available from the event log)")
        # event-log fallback: each materialization's data provenance records the upstream data versions it was built from
        seen, frontier = {key}, [key]
        for _ in range(d):
            nxt = []
            for k in frontier:
                rec = self.instance.get_latest_data_version_record(k)
                if rec is None:
                    continue
                from dagster._core.definitions.data_version import extract_data_provenance_from_entry
                prov = extract_data_provenance_from_entry(rec.event_log_entry)
                for p in sorted((prov.input_data_versions if prov else {}), key=lambda x: x.path):
                    edges.append({"from": _key_str(p), "to": _key_str(k)})
                    if p not in seen:
                        seen.add(p)
                        nxt.append(p)
            frontier = nxt
        return {"asset_key": _key_str(key), "source": "event_log_provenance", "depth": d, "direction": "upstream",
                "upstream": sorted(_key_str(k) for k in seen - {key}), "downstream": None, "edges": edges[:200],
                "note": "derived from materialization provenance (only assets materialized with tracked inputs); no definitions loaded"}

    def t_stale_assets(self, limit: Any = 20, max_age_hours: Any = None) -> Dict[str, Any]:
        n = int(_num(limit, "limit", 1, 100, 20))
        g = self._graph()
        if g is not None:
            try:
                from dagster._core.loader import LoadingContextForTest
                from dagster._core.definitions.data_version import CachingStaleStatusResolver, StaleStatus
                resolver = CachingStaleStatusResolver(self.instance, g, LoadingContextForTest(self.instance))
                rows = []
                for k in sorted(g.get_all_asset_keys(), key=lambda x: x.path):
                    if g.get(k).is_external:
                        continue
                    try:
                        status = resolver.get_status(k)
                    except Exception:
                        continue
                    if status == StaleStatus.STALE:
                        causes = [{"category": c.category.value, "reason": _first_line(c.reason)} for c in resolver.get_stale_causes(k)[:3]]
                        rows.append({"asset_key": _key_str(k), "status": "STALE", "causes": causes})
                for row in rows[:n]:
                    k = dg.AssetKey.from_user_string(row["asset_key"])
                    _, mrec = self._materialization_info(k)
                    row["last_failure"] = self._last_failure(k, mrec.timestamp if mrec else None)
                return {"source": "data_version_staleness", "count": len(rows), "assets": rows[:n]}
            except ImportError:
                pass
        age = _num(max_age_hours, "max_age_hours", 0.01, 24 * 365, 24)
        now = time.time()
        rows = []
        for rec in self.instance.get_asset_records():
            last = rec.asset_entry.last_materialization_record
            ts = last.timestamp if last else None
            key = rec.asset_entry.asset_key
            failed = self._latest_failed_run_after(key, ts) if (ts is None or now - ts <= age * 3600 * 4) else None
            old = ts is None or (now - ts) > age * 3600
            if old or failed:
                rows.append({"asset_key": _key_str(key), "last_materialized_at": _iso(ts),
                             "age_hours": round((now - ts) / 3600, 2) if ts else None,
                             "reason": "latest run failed after last materialization" if failed else f"older than {age:g}h",
                             "failed_run_id": failed["run_id"] if failed else None,
                             "last_failure": self._last_failure(key, ts)})
        rows.sort(key=lambda r: -(r["age_hours"] if r["age_hours"] is not None else 1e9))
        return {"source": "age_based", "max_age_hours": age, "count": len(rows), "assets": rows[:n],
                "note": "no definitions loaded: staleness judged by time since last materialization / a later failed run"}

    def t_receipt_stats(self, asset_key: Any = None, window_hours: Any = 168) -> Dict[str, Any]:
        if self.receipt_store is None:
            raise ToolError("receipt_stats is not configured (no receipt store)")
        key = parse_asset_key(asset_key)
        hrs = _num(window_hours, "window_hours", 0.01, 24 * 365, 168)
        since = (datetime.now(timezone.utc) - timedelta(hours=hrs)).isoformat()
        usage = self.receipt_store.asset_usage_since(_key_str(key), since)
        by_cv = self.receipt_store.outcomes_by_code_version(_key_str(key), since)
        return {"asset_key": _key_str(key), "window_hours": hrs, "usage": _jsonable(usage), "outcomes_by_code_version": _jsonable(by_cv)}


def _jsonable(o: Any, _d: int = 0) -> Any:
    if _d > 6:
        return str(o)
    if isinstance(o, dict):
        return {str(k): _jsonable(v, _d + 1) for k, v in list(o.items())[:50]}
    if isinstance(o, (list, tuple, set, frozenset)):
        return [_jsonable(v, _d + 1) for v in list(o)[:30]]
    if isinstance(o, (str, int, float, bool)) or o is None:
        return o
    return str(o)


# ── definitions loading (server / resource processes) ──────────────────────────────────────────────────────────────
def load_definitions(spec: Optional[str]) -> Optional["dg.Definitions"]:
    """`pkg.module` or `pkg.module:attr` (default attr `defs`; a callable is called). None when spec is empty."""
    if not spec:
        return None
    import importlib
    mod_name, _, attr = spec.partition(":")
    obj = getattr(importlib.import_module(mod_name), attr or "defs")
    if callable(obj) and not isinstance(obj, dg.Definitions):
        obj = obj()
    if not isinstance(obj, dg.Definitions):
        raise ValueError(f"{spec!r} did not resolve to dagster.Definitions (got {type(obj).__name__})")
    return obj


def _load_receipt_store(spec: Optional[str]):
    """`pkg.module:factory` -> factory() returning an object with asset_usage_since / outcomes_by_code_version."""
    if not spec:
        return None
    import importlib
    mod_name, _, attr = spec.partition(":")
    if not attr:
        raise ValueError("receipt store spec must be 'pkg.module:factory'")
    return getattr(importlib.import_module(mod_name), attr)()


# ── Dagster component: registers the toolset as a resource ──────────────────────────────────────────────────────────
class DagsterInstanceToolsComponent(dg.Component, dg.Model, dg.Resolvable):
    """Registers read-only Dagster-instance tools as a resource (default key `dagster_instance_tools`).

    Use inside assets/ops: `context.resources.dagster_instance_tools.call("asset_status", {"asset_key": "orders"})`.
    For agentic_pipeline (MCP only) run THIS FILE as the stdio MCP server -- see README.md."""

    resource_key: str = Field(default="dagster_instance_tools", description="Resource key the toolset is registered under")
    allow: Optional[List[str]] = Field(
        default=None,
        description=f"Hard allow-list of tools (default: all). Valid: {', '.join(ALL_TOOLS)}.",
    )
    max_result_chars: int = Field(default=8000, description="Upper bound on the serialized size of any tool result.")
    definitions_module: Optional[str] = Field(
        default=None,
        description="Optional 'pkg.module[:attr]' that yields your Definitions (attr defaults to `defs`). Enables downstream lineage, "
                    "partition totals/missing, data-version staleness. Without it those degrade to event-log-only answers.",
    )
    store_resource_key: Optional[str] = Field(
        default=None,
        description="Optional resource key of a ContextReceiptStore; enables the receipt_stats tool.",
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=("allow",), csv=(), tags=())

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        allow = list(self.allow) if self.allow else None
        if allow is not None:
            bad = sorted(set(allow) - set(ALL_TOOLS))
            if bad:
                raise ValueError(f"unknown tool(s) in allow: {bad}; valid: {list(ALL_TOOLS)}")
        store_key = self.store_resource_key
        max_chars, defs_module = int(self.max_result_chars), self.definitions_module
        if allow is not None and "receipt_stats" in allow and not store_key:
            raise ValueError("allow includes receipt_stats but store_resource_key is not set")
        if store_key is None:
            allow = [t for t in (allow if allow is not None else ALL_TOOLS) if t != "receipt_stats"]

        def _init(init_context):
            instance = init_context.instance or dg.DagsterInstance.get()
            store = getattr(init_context.resources, store_key) if store_key else None
            return DagsterInstanceTools(
                instance, definitions=load_definitions(defs_module), allow=allow, max_result_chars=max_chars, receipt_store=store,
            )

        res = dg.ResourceDefinition(
            resource_fn=_init, required_resource_keys={store_key} if store_key else set(),
            description="Read-only Dagster instance tools for agents.",
        )
        return dg.Definitions(resources={self.resource_key: res})


# ── stdio MCP server entrypoint:  python component.py  (reads DAGSTER_HOME) ─────────────────────────────────────────
def build_mcp_server(tools: DagsterInstanceTools):
    """FastMCP server (official `mcp` package) exposing exactly the allowed tools."""
    from mcp.server.fastmcp import FastMCP

    mcp = FastMCP("dagster-instance-tools")
    allowed = set(tools.tool_names())

    def _tool(fn):
        if fn.__name__ in allowed:
            mcp.tool()(fn)
        return fn

    @_tool
    def list_recent_failures(hours: float = 24, limit: int = 10) -> dict:
        """Recent FAILED Dagster runs (newest first) with failed step keys, error class + first message line, and asset keys."""
        return tools.call("list_recent_failures", {"hours": hours, "limit": limit})

    @_tool
    def get_run_summary(run_id: str) -> dict:
        """One run: status, timings, steps, failed-step error detail (truncated, redacted), tags."""
        return tools.call("get_run_summary", {"run_id": run_id})

    @_tool
    def get_run_logs(run_id: str, level: str = "INFO", max_lines: int = 100) -> dict:
        """Tail of a run's event-log messages at or above `level` (secrets redacted)."""
        return tools.call("get_run_logs", {"run_id": run_id, "level": level, "max_lines": max_lines})

    @_tool
    def asset_status(asset_key: str) -> dict:
        """Asset health: last materialization (time/run/data+code version), partition counts, later failed run, latest check results."""
        return tools.call("asset_status", {"asset_key": asset_key})

    @_tool
    def asset_lineage(asset_key: str, depth: int = 2, direction: str = "upstream") -> dict:
        """Upstream/downstream assets (downstream needs definitions_module; upstream falls back to event-log provenance)."""
        return tools.call("asset_lineage", {"asset_key": asset_key, "depth": depth, "direction": direction})

    @_tool
    def stale_assets(limit: int = 20, max_age_hours: float = 24) -> dict:
        """Assets that are stale (data-version staleness with definitions; else older than max_age_hours or with a later failed run)."""
        return tools.call("stale_assets", {"limit": limit, "max_age_hours": max_age_hours})

    @_tool
    def receipt_stats(asset_key: str, window_hours: float = 168) -> dict:
        """How answers built from this asset turned out (ContextReceiptStore): usage + mean outcome per code version."""
        return tools.call("receipt_stats", {"asset_key": asset_key, "window_hours": window_hours})

    return mcp


def main(argv: Optional[List[str]] = None) -> None:
    """Env: DAGSTER_HOME (required), DAGSTER_INSTANCE_TOOLS_ALLOW (csv), _MAX_CHARS, _DEFINITIONS_MODULE, _RECEIPT_STORE (pkg.mod:factory)."""
    logging.basicConfig(level=logging.INFO, stream=sys.stderr, format="%(message)s")  # stdout is the MCP channel
    if not os.environ.get("DAGSTER_HOME"):
        raise SystemExit("DAGSTER_HOME must point at the Dagster instance directory (the one with dagster.yaml)")
    allow_env = os.environ.get("DAGSTER_INSTANCE_TOOLS_ALLOW")
    allow = [t.strip() for t in allow_env.split(",") if t.strip()] if allow_env else None
    store = _load_receipt_store(os.environ.get("DAGSTER_INSTANCE_TOOLS_RECEIPT_STORE"))
    if store is None:
        allow = [t for t in (allow if allow is not None else ALL_TOOLS) if t != "receipt_stats"]
    tools = DagsterInstanceTools(
        dg.DagsterInstance.get(),
        definitions=load_definitions(os.environ.get("DAGSTER_INSTANCE_TOOLS_DEFINITIONS_MODULE")),
        allow=allow,
        max_result_chars=int(os.environ.get("DAGSTER_INSTANCE_TOOLS_MAX_CHARS", "8000")),
        receipt_store=store,
    )
    build_mcp_server(tools).run(transport="stdio")


if __name__ == "__main__":
    main()
