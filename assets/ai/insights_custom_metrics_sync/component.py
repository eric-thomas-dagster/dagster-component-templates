"""InsightsCustomMetricsSyncComponent -- register Dagster+ Insights custom metrics automatically.

A Dagster+ Insights *custom metric* is defined by a `metadata_key` (+ unit type, display name, description). Defining one is UI-only
today. This component does it through the Dagster+ GraphQL API (the same `customMetrics` / `createCustomMetric` /
`updateCustomMetric` / `deleteCustomMetric` operations as cli/sync_custom_metrics), as an idempotent asset (+ optional schedule):

  * match by `metadata_key`; create when missing; update ONLY when display_name / description / unit_type differ (no needless writes)
  * never delete unless `prune: true` AND `prune_prefixes` is given -- only keys starting with a listed prefix are ever deleted,
    so metrics created by hand in the UI survive
  * `dry_run` (default true) reads the current definitions and reports the plan without any mutation
  * `on_error: warn` (default) -- a Dagster+ outage or a bad token must not break unrelated runs; `fail` raises

Once defined, Insights aggregates a metric across every asset whose materialization emits a numeric MetadataValue under that key.
`presets` are the numeric metadata keys this repo's own components really emit (a test AST-checks that they still do).

CLI (no Dagster+ access needed): `python component.py --manifest-only --preset agentic_pipeline` prints a manifest for
cli/sync_custom_metrics.
"""
import json
import random
import re
import sys
import time
import urllib.error
import urllib.request
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import dagster as dg
from pydantic import Field, model_validator

INSIGHTS_TAG_KEY = "insights/metrics"  # asset tag: value = a preset name (one name; Dagster tag values cannot contain commas)
VALID_UNIT_TYPES = ("INTEGER", "FLOAT", "TIME_MS", "TIME_SECONDS", "BYTES")
_KEY_RE = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_.\-/]{0,199}$")


def _m(key: str, unit: str, name: str, desc: str) -> Dict[str, str]:
    return {"metadata_key": key, "unit_type": unit, "display_name": name, "description": desc}


# preset name -> metrics. Every key is emitted as a numeric MetadataValue by that component (checked by tests/test_presets.py).
# NOTE: keys are GLOBAL per deployment in Insights: `retries`, `lag_seconds`, `total_tokens` ... aggregate across EVERY asset that
# emits them, not just ours. Pick presets selectively (see README).
PRESETS: Dict[str, List[Dict[str, str]]] = {
    "agentic_pipeline": [
        _m("agentic_cost_usd", "FLOAT", "Agentic cost (USD)", "LLM spend of an agentic_pipeline run."),
        _m("agentic_tokens", "INTEGER", "Agentic tokens", "Total tokens used by an agentic_pipeline run."),
        _m("agentic_llm_calls", "INTEGER", "Agentic LLM calls", "LLM calls made by an agentic_pipeline run."),
        _m("agentic_tool_calls", "INTEGER", "Agentic tool calls", "Tool calls made by an agentic_pipeline run."),
        _m("agentic_latency_ms", "TIME_MS", "Agentic latency", "Wall-clock latency of an agentic_pipeline run."),
        _m("agentic_budget_violations", "INTEGER", "Agentic budget violations", "Budget policy violations in a run."),
        _m("agentic_spent_today_usd", "FLOAT", "Agentic spent today (USD)", "Cumulative spend today against the daily budget."),
        _m("agentic_remaining_today_usd", "FLOAT", "Agentic remaining today (USD)", "Remaining daily budget."),
    ],
    "context_engineering_pipeline": [
        _m("receipts_quarantined", "INTEGER", "Chunks quarantined", "Chunks quarantined by receipt feedback."),
        _m("receipts_chunks_with_feedback", "INTEGER", "Chunks with feedback", "Chunks that have receipt outcome feedback."),
        _m("receipts_pruned", "INTEGER", "Chunks pruned", "Chunks pruned because they were never retrieved."),
        _m("reused_embeddings", "INTEGER", "Reused embeddings", "Embeddings reused from the prior incremental build."),
        _m("new_embeddings", "INTEGER", "New embeddings", "Embeddings computed fresh in an incremental build."),
        _m("reused_categories", "INTEGER", "Reused categories", "Category labels reused from the prior build."),
        _m("new_categories", "INTEGER", "New categories", "Category labels computed fresh."),
        _m("reused_enrichments", "INTEGER", "Reused enrichments", "Enrichments reused from the prior build."),
        _m("new_enrichments", "INTEGER", "New enrichments", "Enrichments computed fresh."),
    ],
    "receipts_sync": [
        _m("rows_context_receipts", "INTEGER", "Receipt rows shipped", "context_receipts rows shipped to the warehouse."),
        _m("rows_context_receipt_items", "INTEGER", "Receipt item rows shipped", "context_receipt_items rows shipped."),
        _m("rows_context_receipt_excluded", "INTEGER", "Receipt excluded rows shipped", "context_receipt_excluded rows shipped."),
        _m("rows_context_receipt_events", "INTEGER", "Receipt event rows shipped", "context_receipt_events rows shipped."),
        _m("load_jobs", "INTEGER", "Receipt load jobs", "Warehouse load jobs run by a receipts sync."),
        _m("retries", "INTEGER", "Retries", "Retries performed (GLOBAL key: any asset emitting `retries` is aggregated)."),
        _m("lag_seconds", "TIME_SECONDS", "Lag", "Age of the newest shipped/ingested record (GLOBAL key)."),
    ],
    "receipt_outcome_ingestion": [
        _m("outcomes_recorded", "INTEGER", "Outcomes recorded", "Outcome rows recorded against receipts."),
        _m("unknown_receipts_pending", "INTEGER", "Unknown receipts pending", "Outcomes referencing receipts not seen yet."),
        _m("bad_rows", "INTEGER", "Bad outcome rows", "Outcome rows rejected as malformed."),
        _m("lag_seconds", "TIME_SECONDS", "Lag", "Age of the newest shipped/ingested record (GLOBAL key)."),
    ],
    "rag_pipeline": [
        _m("receipts_recorded", "INTEGER", "RAG receipts recorded", "Retrieval receipts recorded by a rag_pipeline run."),
        _m("receipts_failed", "INTEGER", "RAG receipts failed", "Rows whose receipt could not be recorded."),
        _m("shadow_served", "INTEGER", "Shadow served", "Rows served by the shadow recipe."),
        _m("shadow_fallbacks", "INTEGER", "Shadow fallbacks", "Rows where the shadow recipe fell back."),
        _m("total_cost_usd", "FLOAT", "RAG cost (USD)", "LLM cost of a rag_pipeline run (GLOBAL key)."),
        _m("total_latency_ms", "TIME_MS", "RAG latency", "Total step latency of a rag_pipeline run (GLOBAL key)."),
        _m("total_tokens", "INTEGER", "RAG tokens", "Total tokens of a rag_pipeline run (GLOBAL key)."),
        _m("cache_read_tokens", "INTEGER", "Cache read tokens", "Prompt-cache read tokens (GLOBAL key)."),
        _m("cache_creation_tokens", "INTEGER", "Cache creation tokens", "Prompt-cache creation tokens (GLOBAL key)."),
    ],
    # TODO: run_failure_remediation -- component.py did not exist when this was written.
}

# --------------------------------------------------------------------------------------------------------------------------
# GraphQL operations: ported verbatim from cli/sync_custom_metrics/sync_custom_metrics.py (introspected against live Dagster+).
# --------------------------------------------------------------------------------------------------------------------------
Q_LIST_METRICS = """
  query ListCustomMetrics {
    customMetrics {
      id
      metadataKey
      displayName
      description
      unitType
    }
  }
"""

Q_CREATE_METRIC = """
  mutation CreateCustomMetric($customMetricInput: CustomMetricInput!) {
    createCustomMetric(customMetricInput: $customMetricInput) {
      __typename
      ... on CreateCustomMetricSuccess { customMetric { id metadataKey } }
      ... on CustomMetricError { message }
      ... on PythonError { message stack }
      ... on UnauthorizedError { message }
    }
  }
"""

Q_UPDATE_METRIC = """
  mutation UpdateCustomMetric($customMetricId: String!, $customMetricUpdateInput: CustomMetricUpdateInput!) {
    updateCustomMetric(customMetricId: $customMetricId, customMetricUpdateInput: $customMetricUpdateInput) {
      __typename
      ... on UpdateCustomMetricSuccess { customMetric { id metadataKey } }
      ... on CustomMetricError { message }
      ... on PythonError { message stack }
      ... on UnauthorizedError { message }
    }
  }
"""

Q_DELETE_METRIC = """
  mutation DeleteCustomMetric($customMetricId: String!) {
    deleteCustomMetric(customMetricId: $customMetricId) {
      __typename
      ... on DeleteCustomMetricSuccess { customMetricId }
      ... on CustomMetricError { message }
      ... on PythonError { message }
      ... on UnauthorizedError { message }
    }
  }
"""


class InsightsSyncError(RuntimeError):
    """A sync failure. `fatal` means the remaining work should be abandoned (auth, transport, malformed response)."""

    def __init__(self, message: str, fatal: bool = True):
        super().__init__(message)
        self.fatal = fatal


# --------------------------------------------------------------------------------------------------------------------------
# Validation
# --------------------------------------------------------------------------------------------------------------------------
def validate_metric(entry: Any, where: str = "metric") -> Dict[str, Optional[str]]:
    if not isinstance(entry, dict):
        raise ValueError(f"{where}: must be a mapping with metadata_key and unit_type")
    key = entry.get("metadata_key")
    if not isinstance(key, str) or not _KEY_RE.match(key):
        raise ValueError(f"{where}: invalid metadata_key {key!r} (letters, digits, _ . - /; max 200 chars; no whitespace)")
    unit = entry.get("unit_type")
    if unit not in VALID_UNIT_TYPES:
        raise ValueError(f"{where} ({key}): invalid unit_type {unit!r}; must be one of {list(VALID_UNIT_TYPES)}")
    unknown = set(entry) - {"metadata_key", "unit_type", "display_name", "description"}
    if unknown:
        raise ValueError(f"{where} ({key}): unknown field(s) {sorted(unknown)}")
    return {"metadata_key": key, "unit_type": unit,
            "display_name": entry.get("display_name") or None, "description": entry.get("description") or None}


def resolve_metrics(presets: List[str], metrics: List[Dict[str, Any]]) -> List[Dict[str, Optional[str]]]:
    """Presets first, then explicit entries (an explicit entry overrides a preset entry with the same key). Order-stable."""
    out: Dict[str, Dict[str, Optional[str]]] = {}
    for p in presets or []:
        if p not in PRESETS:
            raise ValueError(f"unknown preset {p!r}; available: {sorted(PRESETS)}")
        for i, e in enumerate(PRESETS[p]):
            v = validate_metric(e, f"preset {p}[{i}]")
            out[v["metadata_key"]] = v
    for i, e in enumerate(metrics or []):
        v = validate_metric(e, f"metrics[{i}]")
        out[v["metadata_key"]] = v
    return list(out.values())


def validate_deployment_url(url: str) -> str:
    u = urlparse(url or "")
    loopback = u.hostname in ("localhost", "127.0.0.1", "::1")
    if not u.hostname or u.scheme not in ("https", "http") or (u.scheme == "http" and not loopback):
        raise ValueError("deployment_url must be a full https URL like https://<org>.dagster.cloud/<deployment> "
                         f"(got {url!r}; plain http is only allowed for localhost)")
    return url.rstrip("/")


# --------------------------------------------------------------------------------------------------------------------------
# GraphQL client with transient-error retry
# --------------------------------------------------------------------------------------------------------------------------
class _Client:
    def __init__(self, deployment_url: str, token: str, *, token_env_var: str = "DAGSTER_CLOUD_API_TOKEN", timeout: float = 30.0,
                 max_retries: int = 4, backoff_seconds: float = 1.0, max_backoff_seconds: float = 30.0,
                 sleep: Callable[[float], None] = time.sleep, rng: Optional[random.Random] = None, logger: Any = None):
        self.endpoint = deployment_url.rstrip("/") + "/graphql"
        self._token, self.token_env_var = token, token_env_var
        self.timeout, self.max_retries = timeout, max_retries
        self.backoff, self.max_backoff = backoff_seconds, max_backoff_seconds
        self.sleep, self.rng, self.logger = sleep, rng or random.Random(), logger
        self.requests = 0

    def _redact(self, text: str) -> str:
        return text.replace(self._token, "***") if self._token else text

    def _delay(self, attempt: int, retry_after: Optional[str]) -> float:
        if retry_after:
            try:
                return min(max(float(retry_after), 0.0), self.max_backoff)
            except ValueError:
                pass
        base = min(self.backoff * (2 ** attempt), self.max_backoff)
        return base * (0.5 + self.rng.random() / 2)  # jitter in [0.5, 1.0) of the exponential step

    def call(self, query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
        body = json.dumps({"query": query, "variables": variables}).encode("utf-8")
        for attempt in range(self.max_retries + 1):
            req = urllib.request.Request(self.endpoint, data=body, method="POST", headers={
                "Content-Type": "application/json", "Dagster-Cloud-Api-Token": self._token})
            retry_after, why = None, ""
            try:
                self.requests += 1
                with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                    payload = json.loads(resp.read().decode("utf-8"))
                break
            except urllib.error.HTTPError as e:
                text = self._redact(e.read().decode("utf-8", errors="replace")[:300])
                if e.code in (401, 403):
                    raise InsightsSyncError(
                        f"Dagster+ rejected the API token (HTTP {e.code}); check the token in env var {self.token_env_var} "
                        "has access to this deployment") from None
                if e.code == 429 or 500 <= e.code < 600:
                    retry_after, why = e.headers.get("Retry-After") if e.headers else None, f"HTTP {e.code}"
                else:
                    raise InsightsSyncError(f"Dagster+ GraphQL HTTP {e.code}: {text}") from None
            except (urllib.error.URLError, TimeoutError, ConnectionError, OSError) as e:
                why = f"{type(e).__name__}: {self._redact(str(e))[:200]}"
            except ValueError:
                raise InsightsSyncError("Dagster+ returned a non-JSON response; is deployment_url correct?") from None
            if attempt >= self.max_retries:
                raise InsightsSyncError(f"Dagster+ unreachable after {self.max_retries + 1} attempts ({why})") from None
            delay = self._delay(attempt, retry_after)
            if self.logger:
                self.logger.warning(f"Dagster+ transient error ({why}); retry {attempt + 1}/{self.max_retries} in {delay:.1f}s")
            self.sleep(delay)
        if payload.get("errors"):
            raise InsightsSyncError(f"GraphQL errors: {self._redact(json.dumps(payload['errors']))[:600]}")
        return payload.get("data") or {}


def _check(top: Dict[str, Any], success_type: str, what: str) -> Dict[str, Any]:
    t = top.get("__typename")
    if t == success_type:
        return top
    msg = top.get("message") or json.dumps(top)[:300]
    raise InsightsSyncError(f"{what}: {t or 'unexpected response'}: {msg}", fatal=(t in ("UnauthorizedError", None)))


# --------------------------------------------------------------------------------------------------------------------------
# The sync
# --------------------------------------------------------------------------------------------------------------------------
def plan_sync(desired: List[Dict[str, Optional[str]]], existing: List[Dict[str, Any]], prune: bool,
              prune_prefixes: List[str]) -> Dict[str, Any]:
    by_key = {e["metadataKey"]: e for e in existing}
    create, update, unchanged = [], [], []
    for d in desired:
        ex = by_key.get(d["metadata_key"])
        if ex is None:
            create.append(d)
            continue
        merged = {  # unspecified optional fields keep the server's current value (never null them out)
            "displayName": d["display_name"] if d["display_name"] is not None else ex.get("displayName"),
            "description": d["description"] if d["description"] is not None else ex.get("description"),
            "unitType": d["unit_type"],
        }
        current = {"displayName": ex.get("displayName"), "description": ex.get("description"), "unitType": ex.get("unitType")}
        if merged == current:
            unchanged.append(d["metadata_key"])
        else:
            update.append({"id": ex["id"], "metadata_key": d["metadata_key"], "input": merged,
                           "changed": sorted(k for k in merged if merged[k] != current[k])})
    delete = []
    if prune and prune_prefixes:
        keys = {d["metadata_key"] for d in desired}
        delete = [e for e in existing if e["metadataKey"] not in keys and any(e["metadataKey"].startswith(p) for p in prune_prefixes)]
    return {"create": create, "update": update, "unchanged": unchanged, "delete": delete}


def sync_metrics(client: _Client, desired: List[Dict[str, Optional[str]]], *, dry_run: bool, prune: bool = False,
                 prune_prefixes: Optional[List[str]] = None, logger: Any = None) -> Dict[str, Any]:
    """Returns a report; raises InsightsSyncError (fatal) for auth/transport problems. Per-metric CustomMetricError is collected into
    report['errors'] and the remaining metrics are still processed."""
    data = client.call(Q_LIST_METRICS, {})
    existing = data.get("customMetrics") or []
    plan = plan_sync(desired, existing, prune, prune_prefixes or [])
    report: Dict[str, Any] = {"dry_run": dry_run, "created": [], "updated": [], "deleted": [], "unchanged": plan["unchanged"],
                              "planned": {"create": [d["metadata_key"] for d in plan["create"]],
                                          "update": [u["metadata_key"] for u in plan["update"]],
                                          "delete": [e["metadataKey"] for e in plan["delete"]]},
                              "errors": []}
    if dry_run:
        return report

    def attempt(label: str, fn: Callable[[], None]) -> None:
        try:
            fn()
        except InsightsSyncError as e:
            if e.fatal:
                raise
            report["errors"].append(str(e))
            if logger:
                logger.warning(str(e))

    for d in plan["create"]:
        inp = {"metadataKey": d["metadata_key"], "unitType": d["unit_type"]}
        if d["display_name"] is not None:
            inp["displayName"] = d["display_name"]
        if d["description"] is not None:
            inp["description"] = d["description"]

        def _create(inp=inp, key=d["metadata_key"]):
            res = client.call(Q_CREATE_METRIC, {"customMetricInput": inp})
            _check(res.get("createCustomMetric") or {}, "CreateCustomMetricSuccess", f"create {key}")
            report["created"].append(key)
        attempt("create", _create)
    for u in plan["update"]:
        def _update(u=u):
            res = client.call(Q_UPDATE_METRIC, {"customMetricId": u["id"], "customMetricUpdateInput": u["input"]})
            _check(res.get("updateCustomMetric") or {}, "UpdateCustomMetricSuccess", f"update {u['metadata_key']}")
            report["updated"].append(u["metadata_key"])
        attempt("update", _update)
    for e in plan["delete"]:
        def _delete(e=e):
            res = client.call(Q_DELETE_METRIC, {"customMetricId": e["id"]})
            _check(res.get("deleteCustomMetric") or {}, "DeleteCustomMetricSuccess", f"delete {e['metadataKey']}")
            report["deleted"].append(e["metadataKey"])
        attempt("delete", _delete)
    return report


# --------------------------------------------------------------------------------------------------------------------------
# Manifest output (compatible with cli/sync_custom_metrics)
# --------------------------------------------------------------------------------------------------------------------------
def render_manifest(metrics: List[Dict[str, Optional[str]]]) -> str:
    lines = ["metrics:"]
    for m in metrics:
        lines.append(f"  - metadata_key: {m['metadata_key']}")
        for k in ("display_name", "description"):
            if m.get(k):
                lines.append(f"    {k}: {json.dumps(m[k])}")  # JSON strings are valid YAML scalars
        lines.append(f"    unit_type: {m['unit_type']}")
    return "\n".join(lines) + "\n"


# --------------------------------------------------------------------------------------------------------------------------
# Schema-drift guard: validate every operation we send against the server's GraphQL introspection result.
# Self-contained (no graphql-core needed); a tiny parser for the operation shapes above.
# --------------------------------------------------------------------------------------------------------------------------
OPERATIONS: Dict[str, str] = {"ListCustomMetrics": Q_LIST_METRICS, "CreateCustomMetric": Q_CREATE_METRIC,
                              "UpdateCustomMetric": Q_UPDATE_METRIC, "DeleteCustomMetric": Q_DELETE_METRIC}

# The input objects we send: keys always present, and keys present only when the user supplied a value.
SENT_INPUTS: Dict[str, Dict[str, List[str]]] = {
    "CustomMetricInput": {"always": ["metadataKey", "unitType"], "optional": ["displayName", "description"]},
    "CustomMetricUpdateInput": {"always": ["displayName", "description", "unitType"], "optional": []},
}


def _typeref_fragment(depth: int = 7) -> str:
    inner = "kind name"
    for _ in range(depth):
        inner = f"kind name ofType {{ {inner} }}"
    return inner


INTROSPECTION_QUERY = (
    "query IntrospectionQuery { __schema { queryType { name } mutationType { name } types { ...FullType } } }\n"
    "fragment FullType on __Type { kind name fields(includeDeprecated: true) { name args { ...InputValue } type { ...TypeRef } } "
    "inputFields { ...InputValue } interfaces { ...TypeRef } enumValues(includeDeprecated: true) { name } possibleTypes { ...TypeRef } }\n"
    "fragment InputValue on __InputValue { name type { ...TypeRef } defaultValue }\n"
    "fragment TypeRef on __Type { " + _typeref_fragment() + " }"
)

_TOKEN_RE = re.compile(r'\s*(?:(\.\.\.)|([{}()\[\]:!=$@,])|("(?:[^"\\]|\\.)*")|(-?\d+(?:\.\d+)?)|([A-Za-z_][A-Za-z0-9_]*))')


def _tokens(text: str) -> List[str]:
    out, pos = [], 0
    text = re.sub(r"#[^\n]*", "", text)
    while pos < len(text):
        if not text[pos:].strip():
            break
        m = _TOKEN_RE.match(text, pos)
        if not m:
            raise ValueError(f"cannot tokenize GraphQL near {text[pos:pos + 20]!r}")
        out.append(next(g for g in m.groups() if g is not None))
        pos = m.end()
    return out


def parse_operation(text: str) -> Dict[str, Any]:
    """Parse one query/mutation: {type, name, vars: {name: 'Type!'}, selections: [...]}. Supports fields, args (variables or
    scalar/enum literals), `... on T {}` fragments -- the subset the component sends."""
    t, i = _tokens(text), 0

    def peek():
        return t[i] if i < len(t) else None

    def take(expect=None):
        nonlocal i
        tok = t[i]
        if expect and tok != expect:
            raise ValueError(f"expected {expect!r}, got {tok!r}")
        i += 1
        return tok

    def parse_type() -> str:
        if peek() == "[":
            take("[")
            s = "[" + parse_type() + "]"
            take("]")
        else:
            s = take()
        if peek() == "!":
            take()
            s += "!"
        return s

    def parse_value():
        if peek() == "$":
            take()
            return ("var", take())
        if peek() in ("[", "{"):
            depth = 0
            while True:
                tok = take()
                depth += tok in ("[", "{")
                depth -= tok in ("]", "}")
                if depth == 0:
                    break
            return ("lit", None)
        take()
        return ("lit", None)

    def parse_selection_set() -> List[Dict[str, Any]]:
        take("{")
        sels = []
        while peek() != "}":
            if peek() == ",":
                take(); continue
            if peek() == "...":
                take()
                on = None
                if peek() == "on":
                    take(); on = take()
                sels.append({"kind": "inline", "on": on, "sel": parse_selection_set()})
                continue
            name = take()
            if peek() == ":":  # alias
                take(); name = take()
            args = {}
            if peek() == "(":
                take("(")
                while peek() != ")":
                    if peek() == ",":
                        take(); continue
                    an = take(); take(":"); args[an] = parse_value()
                take(")")
            sel = parse_selection_set() if peek() == "{" else None
            sels.append({"kind": "field", "name": name, "args": args, "sel": sel})
        take("}")
        return sels

    op = take()
    if op not in ("query", "mutation"):
        raise ValueError(f"unsupported operation type {op!r}")
    name = take() if peek() not in ("(", "{") else None
    vars_: Dict[str, str] = {}
    if peek() == "(":
        take("(")
        while peek() != ")":
            if peek() == ",":
                take(); continue
            take("$"); vn = take(); take(":"); vars_[vn] = parse_type()
        take(")")
    return {"type": op, "name": name, "vars": vars_, "selections": parse_selection_set()}


def _ref_str(ref: Dict[str, Any]) -> str:
    if ref["kind"] == "NON_NULL":
        return _ref_str(ref["ofType"]) + "!"
    if ref["kind"] == "LIST":
        return "[" + _ref_str(ref["ofType"]) + "]"
    return ref["name"]


def _named(ref: Dict[str, Any]) -> str:
    while ref.get("ofType"):
        ref = ref["ofType"]
    return ref["name"]


def verify_operations_against_schema(introspection_result: Dict[str, Any], operations: Optional[Dict[str, str]] = None,
                                     sent_inputs: Optional[Dict[str, Dict[str, List[str]]]] = None) -> List[str]:
    """Pure: returns a list of problems (empty = every operation and input we send is valid against the server schema).
    `introspection_result` is the GraphQL response (`{"data": {"__schema": ...}}`) or its `data`/`__schema` part."""
    sch = introspection_result
    sch = sch.get("data", sch)
    sch = sch.get("__schema", sch)
    types = {t["name"]: t for t in sch.get("types", []) if t.get("name")}
    roots = {"query": (sch.get("queryType") or {}).get("name"), "mutation": (sch.get("mutationType") or {}).get("name")}
    problems: List[str] = []

    def offers(t: Dict[str, Any]) -> str:
        return str(sorted(f["name"] for f in (t.get("fields") or [])))

    def walk(op: str, parent: str, sels: List[Dict[str, Any]], path: str) -> None:
        pt = types.get(parent)
        if pt is None:
            problems.append(f"{op}: {path}: type {parent!r} does not exist on the server")
            return
        for s in sels:
            if s["kind"] == "inline":
                on = s["on"] or parent
                members = [_named(p) for p in (pt.get("possibleTypes") or [])]
                if pt["kind"] in ("UNION", "INTERFACE"):
                    if on != parent and on not in members:
                        problems.append(f"{op}: {path}: '... on {on}' is not a member of {pt['kind'].lower()} {parent}; server members: {sorted(members)}")
                        if on not in types:
                            problems.append(f"{op}: {path}: type {on!r} does not exist on the server")
                            continue
                elif on != parent:
                    problems.append(f"{op}: {path}: '... on {on}' inside object type {parent}")
                    continue
                if on in types:
                    walk(op, on, s["sel"], f"{path}.<{on}>")
                continue
            name, here = s["name"], f"{path}.{s['name']}"
            if name == "__typename":
                continue
            if pt["kind"] == "UNION":
                problems.append(f"{op}: {here}: cannot select a field directly on union {parent} (only __typename and fragments)")
                continue
            f = next((x for x in (pt.get("fields") or []) if x["name"] == name), None)
            if f is None:
                problems.append(f"{op}: {here}: field not on {parent}; server offers {offers(pt)}")
                continue
            check_args(op, here, f, s["args"], op_vars[op])
            rt = types.get(_named(f["type"]))
            composite = rt is not None and rt["kind"] in ("OBJECT", "INTERFACE", "UNION")
            if s["sel"] is not None and not composite:
                problems.append(f"{op}: {here}: has a sub-selection but its type {_ref_str(f['type'])} is a leaf")
            elif s["sel"] is None and composite:
                problems.append(f"{op}: {here}: type {_ref_str(f['type'])} needs a sub-selection")
            elif s["sel"] is not None:
                walk(op, rt["name"], s["sel"], here)

    def check_args(op: str, here: str, f: Dict[str, Any], args: Dict[str, Any], vars_: Dict[str, str]) -> None:
        sargs = {a["name"]: a for a in (f.get("args") or [])}
        for an, (kind, val) in args.items():
            if an not in sargs:
                problems.append(f"{op}: {here}({an}:): argument not accepted; server offers {sorted(sargs)}")
                continue
            if kind == "var":
                want = _ref_str(sargs[an]["type"])
                have = vars_.get(val)
                if have is None:
                    problems.append(f"{op}: {here}({an}: ${val}): variable is not declared")
                elif have != want:
                    problems.append(f"{op}: {here}({an}: ${val}): variable declared as {have} but the server argument is {want}")
        for an, a in sargs.items():
            if a["type"]["kind"] == "NON_NULL" and a.get("defaultValue") is None and an not in args:
                problems.append(f"{op}: {here}: required argument {an}: {_ref_str(a['type'])} is not provided")

    op_vars: Dict[str, Dict[str, str]] = {}
    for opname, text in (operations if operations is not None else OPERATIONS).items():
        try:
            parsed = parse_operation(text)
        except (ValueError, IndexError) as e:
            problems.append(f"{opname}: could not parse the operation ({e})")
            continue
        op_vars[opname] = parsed["vars"]
        for vn, vt in parsed["vars"].items():
            base = vt.strip("[]!")
            if base not in types:
                problems.append(f"{opname}: variable ${vn}: type {base!r} does not exist on the server")
        root = roots.get(parsed["type"])
        if not root:
            problems.append(f"{opname}: server has no {parsed['type']} root type")
            continue
        walk(opname, root, parsed["selections"], parsed["type"])

    for tname, spec in (sent_inputs if sent_inputs is not None else SENT_INPUTS).items():
        t = types.get(tname)
        if t is None or t["kind"] != "INPUT_OBJECT":
            problems.append(f"input {tname}: input type does not exist on the server")
            continue
        fields = {x["name"]: x for x in (t.get("inputFields") or [])}
        sent = set(spec["always"]) | set(spec["optional"])
        for k in sorted(sent):
            if k not in fields:
                problems.append(f"input {tname}.{k}: key we send is not accepted; server offers {sorted(fields)}")
        for k, x in fields.items():
            if x["type"]["kind"] == "NON_NULL" and x.get("defaultValue") is None and k not in spec["always"]:
                problems.append(f"input {tname}.{k}: server requires {_ref_str(x['type'])} but we do not always send it")
        ut = fields.get("unitType")
        et = types.get(_named(ut["type"])) if ut else None
        if et is not None and et["kind"] == "ENUM":
            have = {v["name"] for v in (et.get("enumValues") or [])}
            missing = [u for u in VALID_UNIT_TYPES if u not in have]
            if missing:
                problems.append(f"input {tname}.unitType: enum {et['name']} lacks {missing}; server offers {sorted(have)}")
    return problems


def introspect(client: Any) -> Dict[str, Any]:
    return client.call(INTROSPECTION_QUERY, {})


def check_schema(client: Any, logger: Any = None) -> Optional[List[str]]:
    """Problems list ([] = clean), or None when introspection is unavailable (never blocks syncing)."""
    try:
        data = introspect(client)
        if "__schema" not in data:
            raise InsightsSyncError("introspection returned no __schema")
    except InsightsSyncError as e:
        if logger:
            logger.warning(f"schema verification skipped (introspection unavailable): {e}")
        return None
    return verify_operations_against_schema(data)



def discover_presets(context: Any) -> Tuple[List[str], List[str], str]:
    """Scan the assets visible to a running step for the `insights/metrics` tag.
    Returns (known preset names, unknown tag values, scope) where scope is 'repository' (the whole code location) or 'job'
    (only the assets of the job this run executes). `context.repository_def` is only set for runs launched from a code location
    (how Dagster+ launches them); in-process execution (dg.materialize, execute_in_process) has none, so we fall back to the
    run's job graph -- which for a run selecting only this asset contains only this asset."""
    scope, graph = "repository", None
    try:
        graph = context.repository_def.asset_graph
    except Exception:  # noqa: BLE001 -- dagster raises a CheckError when no repository is set on the step context
        scope = "job"
        graph = context.job_def.asset_layer.asset_graph
    found, unknown = [], []
    for key in sorted(graph.get_all_asset_keys(), key=lambda k: k.to_user_string()):
        raw = (graph.get(key).tags or {}).get(INSIGHTS_TAG_KEY)
        for name in [x.strip() for x in str(raw or "").split(",") if x.strip()]:
            bucket = found if name in PRESETS else unknown
            if name not in bucket:
                bucket.append(name)
    return sorted(found), sorted(unknown), scope


class InsightsCustomMetricsSyncComponent(dg.Component, dg.Model, dg.Resolvable):
    """Idempotently upsert Dagster+ Insights custom-metric definitions (presets for this repo's components + explicit entries)."""

    asset_name: str = Field(default="insights_custom_metrics_sync", description="Dagster asset name")
    deployment_url: str = Field(description="Full Dagster+ deployment URL, e.g. https://acme.dagster.cloud/prod. Required: not derived from the agent environment (see README).")
    token_env_var: str = Field(default="DAGSTER_CLOUD_API_TOKEN", description="Env var holding a Dagster+ API token with permission to manage custom metrics.")
    presets: List[str] = Field(default_factory=list, description=f"Preset names: {', '.join(sorted(PRESETS))}. `auto` (alone or in the list) = at run time, register the presets named by the `{INSIGHTS_TAG_KEY}` tag of the assets in the code location.")
    metrics: List[Dict[str, Any]] = Field(default_factory=list, description="Explicit entries: {metadata_key, unit_type, display_name?, description?}.")
    dry_run: bool = Field(default=True, description="Plan only: reads current definitions, performs NO mutation. Set false to write.")
    prune: bool = Field(default=False, description="Delete metrics not in the desired set -- ONLY keys starting with a prune_prefixes entry.")
    prune_prefixes: List[str] = Field(default_factory=list, description="Safelist for prune. Required (non-empty) when prune is true.")
    on_error: str = Field(default="warn", description="warn (log + record in metadata, run succeeds) or fail (raise).")
    timeout_seconds: float = Field(default=30.0, description="Per-request HTTP timeout.")
    max_retries: int = Field(default=4, description="Retries for HTTP 429/5xx/network errors (jittered exponential backoff, honours Retry-After).")
    retry_backoff_seconds: float = Field(default=1.0, description="Base backoff.")
    verify_schema: str = Field(default="warn", description="off | warn | strict (legacy true=strict, false=off). warn (default): introspect once, record schema_check/schema_problems and log a WARNING on drift, but still sync. strict: on drift make no writes, status schema_drift (raises when on_error=fail). Introspection being unavailable never blocks.")
    cron_schedule: Optional[str] = Field(default=None, description="Optional cron expression to re-run the sync.")
    group_name: Optional[str] = Field(default="agents", description="Asset group")

    @model_validator(mode="before")
    @classmethod
    def _presets_shape(cls, data):
        if isinstance(data, dict) and isinstance(data.get("presets"), str):
            data = {**data, "presets": [x.strip() for x in data["presets"].split(",") if x.strip()]}
        if isinstance(data, dict) and isinstance(data.get("verify_schema"), bool):
            data = {**data, "verify_schema": "strict" if data["verify_schema"] else "off"}
        return data

    def _static_presets(self) -> List[str]:
        return [p for p in self.presets if p != "auto"]

    def _desired(self) -> List[Dict[str, Optional[str]]]:
        return resolve_metrics(self._static_presets(), self.metrics)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        if self.on_error not in ("warn", "fail"):
            raise ValueError("on_error must be 'warn' or 'fail'")
        url = validate_deployment_url(self.deployment_url)
        desired = self._desired()
        auto = "auto" in self.presets
        if not desired and not auto:
            raise ValueError("nothing to sync: give at least one preset or metric (or presets: auto)")
        if self.prune and not [p for p in self.prune_prefixes if p]:
            raise ValueError("prune: true requires a non-empty prune_prefixes safelist (UI-created metrics must never be deleted)")
        if self.verify_schema not in ("off", "warn", "strict"):
            raise ValueError("verify_schema must be 'off', 'warn' or 'strict'")
        if self.max_retries < 0 or self.timeout_seconds <= 0:
            raise ValueError("max_retries must be >= 0 and timeout_seconds > 0")
        comp = self

        @dg.asset(name=self.asset_name, group_name=self.group_name, kinds={"dagster"},
                  description="Registers Dagster+ Insights custom metric definitions (idempotent upsert by metadata_key).")
        def _sync(context: dg.AssetExecutionContext):
            token = dg.EnvVar(comp.token_env_var).get_value()
            report: Dict[str, Any] = {"dry_run": comp.dry_run, "created": [], "updated": [], "deleted": [], "unchanged": [],
                                      "planned": {"create": [], "update": [], "delete": []}, "errors": []}
            status, msg = "ok", ""
            discovered: List[str] = []
            unknown_tags: List[str] = []
            scope = None
            schema_check, schema_problems, blocked = "skipped", [], False
            todo = desired
            if auto:
                discovered, unknown_tags, scope = discover_presets(context)
                todo = resolve_metrics(comp._static_presets() + discovered, comp.metrics)
                context.log.info(f"auto: discovered presets {discovered} (scope={scope})")
                if unknown_tags:
                    context.log.warning(f"auto: ignoring unknown {INSIGHTS_TAG_KEY} tag value(s) {unknown_tags}")
                if not todo:
                    msg = (f"no assets tagged '{INSIGHTS_TAG_KEY}' were found (scope={scope}); nothing to register"
                           + ("" if scope == "repository" else " -- only this run's job graph was visible; launch from a code location"))
                    context.log.warning(msg)
            try:
                if todo:
                    if not token:
                        raise InsightsSyncError(f"env var {comp.token_env_var} is empty or unset")
                    client = _Client(url, token, token_env_var=comp.token_env_var, timeout=comp.timeout_seconds,
                                     max_retries=comp.max_retries, backoff_seconds=comp.retry_backoff_seconds, logger=context.log)
                    problems = check_schema(client, context.log) if comp.verify_schema != "off" else None
                    schema_check = "skipped" if comp.verify_schema == "off" else ("unavailable" if problems is None else "drift" if problems else "pass")
                    if problems:
                        schema_problems = problems
                        if comp.verify_schema == "strict":
                            blocked = True
                            raise InsightsSyncError("Dagster+ GraphQL schema drift; no changes were made: " + "; ".join(problems)[:600], fatal=False)
                        context.log.warning("Dagster+ GraphQL schema drift detected (verify_schema=warn, syncing anyway): " + "; ".join(problems)[:600])
                    report = sync_metrics(client, todo, dry_run=comp.dry_run, prune=comp.prune,
                                          prune_prefixes=[p for p in comp.prune_prefixes if p], logger=context.log)
                    if report["errors"]:
                        raise InsightsSyncError(f"{len(report['errors'])} metric operation(s) failed: " + "; ".join(report["errors"])[:500],
                                                fatal=False)
            except InsightsSyncError as e:
                if comp.on_error == "fail":
                    raise
                status = "schema_drift" if blocked else "failed"
                report["errors"] = report["errors"] or [str(e)]
                context.log.warning(f"insights custom metric sync did not complete (on_error=warn): {e}")
            md_auto: Dict[str, Any] = {}
            if auto:
                md_auto = {"auto_discovered": dg.MetadataValue.json(discovered), "auto_scope": scope or "",
                           "auto_unknown_tag_values": dg.MetadataValue.json(unknown_tags)}
            return dg.MaterializeResult(metadata={
                **md_auto, "message": msg,
                "status": status, "schema_check": schema_check, "schema_problems": dg.MetadataValue.json(schema_problems), "dry_run": comp.dry_run, "deployment_url": url,
                "created": len(report["created"]), "updated": len(report["updated"]), "deleted": len(report["deleted"]),
                "unchanged": len(report["unchanged"]),
                "planned": dg.MetadataValue.json(report["planned"]),
                "errors": dg.MetadataValue.json(report["errors"]),
            })

        schedules = []
        if self.cron_schedule:
            schedules.append(dg.ScheduleDefinition(name=f"{self.asset_name}_schedule", cron_schedule=self.cron_schedule,
                                                   target=dg.AssetSelection.assets(self.asset_name)))
        return dg.Definitions(assets=[_sync], schedules=schedules)


def _cli(argv: List[str]) -> int:
    import argparse
    ap = argparse.ArgumentParser(description="Print a cli/sync_custom_metrics manifest for presets.")
    ap.add_argument("--manifest-only", action="store_true", help="print the YAML manifest and exit")
    ap.add_argument("--preset", action="append", default=[], help=f"one of: {', '.join(sorted(PRESETS))} (repeatable)")
    ap.add_argument("--list-presets", action="store_true")
    ap.add_argument("--verify-schema", action="store_true", help="introspect a live deployment and check our operations against it")
    ap.add_argument("--deployment-url")
    ap.add_argument("--token-env", default="DAGSTER_CLOUD_API_TOKEN")
    a = ap.parse_args(argv)
    if a.verify_schema:
        import os
        token = os.environ.get(a.token_env)
        if not a.deployment_url or not token:
            print(f"ERROR: --deployment-url and a non-empty env var {a.token_env} are required", file=sys.stderr)
            return 2
        try:
            client = _Client(validate_deployment_url(a.deployment_url), token, token_env_var=a.token_env)
            data = introspect(client)
        except (ValueError, InsightsSyncError) as e:
            print(f"ERROR: could not introspect: {e}", file=sys.stderr)
            return 2
        problems = verify_operations_against_schema(data)
        if problems:
            print("DRIFT:\n" + "\n".join(f"  - {x}" for x in problems))
            return 1
        print("PASS: every operation and input matches the server schema")
        return 0
    if a.list_presets:
        print("\n".join(f"{k}: {len(v)} metric(s)" for k, v in sorted(PRESETS.items())))
        return 0
    if not a.manifest_only:
        ap.error("only --manifest-only / --list-presets are supported here; run the component inside Dagster to sync")
    try:
        sys.stdout.write(render_manifest(resolve_metrics(a.preset, [])))
    except ValueError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(_cli(sys.argv[1:]))
