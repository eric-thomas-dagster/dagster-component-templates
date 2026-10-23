"""AgenticPipelineComponent — one YAML, whole agentic pipeline.

Same "pipeline component" shape as `polars_pipeline`, `warehouse_pipeline`,
`pyspark_pipeline`, `snowpark_pipeline`, `ml_pipeline`: **one YAML file
declares the whole pipeline**, `steps:` list defines the DAG in reading order,
`outputs:` declares which step outputs become first-class Dagster assets and
where side text/JSON files get written.

Example (route → answer → critique → synthesize):

    type: dagster_community_components.AgenticPipelineComponent
    attributes:
      asset_name_prefix: research_bot
      source:
        kind: literal
        text: "Explain how a transformer attention head works, plus one concrete example."
      steps:
        - {id: routed,   op: route,
                         router: {model: gpt-4o-mini, api_key_env_var: OPENAI_API_KEY},
                         specialists:
                           - {name: technical, description: "Deep technical CS / ML questions.",
                              model: gpt-4o, api_key_env_var: OPENAI_API_KEY,
                              system_prompt: "You are a senior ML engineer. Be precise."}
                           - {name: general,   description: "General knowledge.",
                              model: gpt-4o-mini, api_key_env_var: OPENAI_API_KEY,
                              system_prompt: "You are a helpful assistant."}
                         fallback: general}
        - {id: critiqued, op: critique_loop, source: routed,
                          drafter: {model: gpt-4o, api_key_env_var: OPENAI_API_KEY,
                                    system_prompt: "You are a technical writer."},
                          critic:  {model: gpt-4o, api_key_env_var: OPENAI_API_KEY,
                                    system_prompt: "Critique for clarity + accuracy."},
                          iterations: 2}
        - {id: debated,   op: debate, source: critiqued,
                          proposers:
                            - {model: gpt-4o, api_key_env_var: OPENAI_API_KEY,
                               system_prompt: "Argue this is a great answer."}
                            - {model: gpt-4o, api_key_env_var: OPENAI_API_KEY,
                               system_prompt: "Argue this answer needs improvement."}
                          arbitrator: {model: gpt-4o, api_key_env_var: OPENAI_API_KEY,
                                       system_prompt: "Pick the stronger argument."}}
      outputs:
        assets: [routed, critiqued, debated]
        text_sinks:
          - {from: critiqued, path: /tmp/final_answer.txt}

Standardization — that's the point. Every agentic pipeline in the org uses
the same YAML shape, the same ops, the same output conventions.

Op coverage (v2 = 6):

- llm_call       — single LLM call; uses source step's text as user prompt
- route          — router picks best specialist; specialist answers
- debate         — N proposers → arbitrator picks winner
- critique_loop  — drafter → critic → drafter, N iterations
- synthesize     — merge multiple upstream step texts into one summary
- mcp_call       — direct MCP tool call (stdio / http / sse), no LLM;
                   turns "fetch grounding data" into a first-class asset with
                   lineage + metadata; string `tool_args` support `{text}`
                   substitution against source
- plan / best_of_n / cascade / verify / eval / decide / approve — reasoning and
                   governance ops (plan-and-replan, verifier-selected or voted
                   candidates, cheapest-model-first escalation, grounding check,
                   rubric eval, named-outcome decisions, human gate with a review
                   queue); any step also accepts `when:` and a narrowing `policy:`
- delegate       — dynamic sibling of `route`/`agent_call`: an LLM picks the
                   best-matching agent from a DISCOVERED pool (sibling
                   AgentCardComponent instances in this project, and/or an
                   external agent-card manifest) instead of a fixed inline
                   list, then invokes the pick over MCP or plain HTTP

State model:

Every step's output is a dict `{"text": str, ...op-specific fields}`. Steps
read text from a prior step via `source: <id>` (default = the pipeline's
original input, NOT "whatever step ran last" — dependency structure is
always declared, never implied by position in the list, so two steps that
don't reference each other are independent branches off the same input
regardless of the order they're written in or executed in). Use typed
`inputs: {name: {from: <id>}}` for multi-parent fan-in.
The `assets:` list picks which step outputs become first-class Dagster
assets; each is emitted as a dict with the full op output preserved.
"""
import json
import os
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Literal, Optional, Union

import dagster as dg
from pydantic import Field


# ── Ingestion ────────────────────────────────────────────────────────

import builtins as _builtins  # noqa: E402

# exception groups are built in from Python 3.11; on 3.10 anyio/mcp raise the `exceptiongroup` backport's class
_BaseExceptionGroup = getattr(_builtins, "BaseExceptionGroup", None)
if _BaseExceptionGroup is None:
    try:
        from exceptiongroup import BaseExceptionGroup as _BaseExceptionGroup
    except ImportError:  # nothing can be a group here; keep `except`/isinstance clauses valid
        class _BaseExceptionGroup(BaseException):  # type: ignore[no-redef]
            exceptions: tuple = ()



def _require_env(name: str, purpose: Optional[str] = None) -> str:
    """Read an env var via dagster.EnvVar, with an actionable message when it is unset."""
    from dagster import EnvVar

    _p = _pol()
    if _p is not None:
        _p.check_env(name)
    value = EnvVar(name).get_value()
    if value is None:
        raise KeyError(
            f"Environment variable {name!r}{f' (for {purpose})' if purpose else ''} is not set. Set it in the environment of the "
            "Dagster code location (or point the *_env_var field at one that is)."
        )
    return value



# ── Agent policy: guardrails at the component boundary ───────────────────────
#
# An optional `policy:` block limits what the pipeline will do ON THE AGENT'S BEHALF: which env vars it may
# read (and hand to MCP subprocesses), which paths it may read/write, which hosts it may call, which tools the
# model may use (and with what arguments), which components it may invoke, and how much it may spend. With no
# `policy:` every check below is a no-op, so existing pipelines behave exactly as before.
#
# This is enforcement at the component's boundary -- the pipeline refuses to do the thing -- not an OS sandbox.
# An MCP stdio server is a subprocess that runs with the worker's OS permissions; the policy decides which env
# vars it is given and which of its tools the model may call, not what the server code does once running. For
# hard isolation run the code location in a container.

import contextlib
import contextvars
import fnmatch
import logging
from types import SimpleNamespace
from urllib.parse import urlparse

_POLICY_SECTIONS = {
    "on_violation": None,
    "env": {"allow"},
    "filesystem": {"read", "write"},
    "network": {"allow_hosts"},
    "tools": {"allow", "deny", "max_calls_per_run", "arg_rules"},
    "components": {"allow"},
    "budget": {"max_cost_usd", "max_tokens", "max_llm_calls", "max_seconds"},
}
_ARG_RULE_KEYS = {"tool", "arg", "under", "hosts", "allow_regex", "deny_regex"}
# What a stdio MCP subprocess always gets when `policy.env.allow` is set (so `npx`, `uvx`, etc. still run).
_SAFE_SUBPROCESS_ENV = (
    "PATH", "HOME", "USER", "LOGNAME", "SHELL", "TERM", "LANG", "LC_ALL", "TMPDIR", "TEMP", "TMP",
    "SYSTEMROOT", "COMSPEC", "PATHEXT",
)


class PolicyViolation(RuntimeError):
    """The pipeline tried to do something its `policy:` does not allow (and `on_violation` is `fail`)."""


def _validate_policy(cfg: Any) -> Dict[str, Any]:
    """Reject typos loudly: a misspelled `filesystm:` silently allowing everything would be a security footgun."""
    if cfg in (None, {}):
        return {}
    if not isinstance(cfg, dict):
        raise ValueError(f"policy must be a mapping, got {type(cfg).__name__}")
    for key, val in cfg.items():
        if key not in _POLICY_SECTIONS:
            raise ValueError(f"unknown policy key {key!r}; valid: {sorted(_POLICY_SECTIONS)}")
        if key == "on_violation":
            if val not in ("fail", "warn"):
                raise ValueError("policy.on_violation must be 'fail' or 'warn'")
            continue
        if not isinstance(val, dict):
            raise ValueError(f"policy.{key} must be a mapping")
        bad = set(val) - _POLICY_SECTIONS[key]
        if bad:
            raise ValueError(f"unknown policy.{key} key(s) {sorted(bad)}; valid: {sorted(_POLICY_SECTIONS[key])}")
        for sub, v in val.items():
            if sub in ("max_cost_usd", "max_tokens", "max_llm_calls", "max_seconds", "max_calls_per_run"):
                if not isinstance(v, (int, float)) or isinstance(v, bool) or v < 0:
                    raise ValueError(f"policy.{key}.{sub} must be a non-negative number")
            elif sub == "arg_rules":
                if not isinstance(v, list):
                    raise ValueError("policy.tools.arg_rules must be a list")
                for rule in v:
                    if not isinstance(rule, dict) or "arg" not in rule or set(rule) - _ARG_RULE_KEYS:
                        raise ValueError(
                            f"each policy.tools.arg_rules entry needs `arg` and only keys {sorted(_ARG_RULE_KEYS)}; got {rule!r}"
                        )
            elif not isinstance(v, list) or not all(isinstance(x, str) for x in v):
                raise ValueError(f"policy.{key}.{sub} must be a list of strings")
    return cfg


class _Policy:
    """One run's view of the policy. `usage` is a plain dict that lives in the pipeline state so counters
    accumulate across every step (and across per-step ops in graph mode)."""

    def __init__(self, cfg: Dict[str, Any], usage: Dict[str, Any], parent: Optional["_Policy"] = None, scoped: bool = False):
        self.cfg = cfg or {}
        self.usage = usage
        self.parent = parent  # a step-level policy narrows its parent: every check must pass BOTH
        usage.setdefault("started_at", time.time())
        for k, zero in (("llm_calls", 0), ("tool_calls", 0), ("tokens", 0), ("cost_usd", 0.0)):
            usage.setdefault(k, zero)
        usage.setdefault("violations", [])
        # a scoped (per-step) policy budgets only what THIS step spends
        self._base = {k: usage[k] for k in ("llm_calls", "tool_calls", "tokens", "cost_usd")} if scoped else None
        self._t0 = time.time()

    def _spent(self) -> Dict[str, Any]:
        u = self.usage
        if self._base is None:
            return {**{k: u[k] for k in ("llm_calls", "tool_calls", "tokens", "cost_usd")}, "elapsed": time.time() - u["started_at"]}
        return {**{k: u[k] - self._base[k] for k in self._base}, "elapsed": time.time() - self._t0}

    def _mode(self) -> str:
        return self.cfg.get("on_violation") or (self.parent._mode() if self.parent else "fail")

    def _sec(self, name: str) -> Dict[str, Any]:
        return self.cfg.get(name) or {}

    def violate(self, kind: str, detail: str, *, hard: bool = True) -> None:
        """Record a violation; raise unless on_violation is 'warn' (or the caller handles refusal itself)."""
        self.usage["violations"].append({"kind": kind, "detail": detail, "at": _now_iso()})
        if hard and self._mode() != "warn":
            raise PolicyViolation(f"[policy:{kind}] {detail}")
        logging.getLogger(__name__).warning("[policy:%s] %s", kind, detail)

    # environment
    def env_allowed(self, name: str) -> bool:
        if self.parent is not None and not self.parent.env_allowed(name):
            return False
        allow = self._sec("env").get("allow")
        return allow is None or any(fnmatch.fnmatchcase(name, p) for p in allow)

    def check_env(self, name: str) -> None:
        if self.parent is not None:
            self.parent.check_env(name)
        if not self.env_allowed(name):
            self.violate("env", f"reading environment variable {name!r} is not allowed (policy.env.allow)")

    # filesystem
    @staticmethod
    def _under(path: str, roots: List[str]) -> bool:
        real = os.path.realpath(os.path.abspath(str(path)))
        for r in roots:
            base = os.path.realpath(os.path.abspath(re.sub(r"(/\*{1,2})+$", "", str(r)) or "."))
            if real == base or real.startswith(base + os.sep):
                return True
        return False

    def check_path(self, path: Any, mode: str) -> None:
        if self.parent is not None:
            self.parent.check_path(path, mode)
        roots = self._sec("filesystem").get(mode)
        if roots is not None and not self._under(str(path), roots):
            self.violate("filesystem", f"{mode} of {str(path)!r} is outside policy.filesystem.{mode} {list(roots)}")

    # network
    @staticmethod
    def _host_ok(url: str, patterns: List[str]) -> bool:
        host = (urlparse(str(url)).hostname or "").lower()
        return bool(host) and any(fnmatch.fnmatchcase(host, p.lower()) for p in patterns)

    def check_url(self, url: Any) -> None:
        if self.parent is not None:
            self.parent.check_url(url)
        hosts = self._sec("network").get("allow_hosts")
        if hosts is not None and not self._host_ok(str(url), hosts):
            self.violate("network", f"call to {str(url)!r} is not in policy.network.allow_hosts {list(hosts)}")

    # components
    def check_component(self, component_type: str) -> None:
        if self.parent is not None:
            self.parent.check_component(component_type)
        allow = self._sec("components").get("allow")
        name = str(component_type).rsplit(".", 1)[-1]
        if allow is not None and not any(fnmatch.fnmatchcase(name, p) for p in allow):
            self.violate("components", f"invoking component {name!r} is not in policy.components.allow {list(allow)}")

    # budget
    def before_llm_call(self) -> None:
        if self.parent is not None:
            self.parent.before_llm_call()
        b, u = self._sec("budget"), self._spent()
        if b.get("max_llm_calls") is not None and u["llm_calls"] >= b["max_llm_calls"]:
            self.violate("budget", f"policy.budget.max_llm_calls={b['max_llm_calls']} reached")
        if b.get("max_seconds") is not None and u["elapsed"] > b["max_seconds"]:
            self.violate("budget", f"policy.budget.max_seconds={b['max_seconds']} exceeded")
        if b.get("max_cost_usd") is not None and u["cost_usd"] >= b["max_cost_usd"]:
            self.violate("budget", f"policy.budget.max_cost_usd={b['max_cost_usd']} reached (spent {u['cost_usd']:.4f})")
        if b.get("max_tokens") is not None and u["tokens"] >= b["max_tokens"]:
            self.violate("budget", f"policy.budget.max_tokens={b['max_tokens']} reached (used {u['tokens']})")

    def after_llm_call(self, cost_usd: Optional[float], tokens: Optional[int]) -> None:
        if self.parent is not None:
            self.parent.after_llm_call(cost_usd, tokens)  # the root accumulates the shared counters exactly once
        else:
            u0 = self.usage
            u0["llm_calls"] += 1
            u0["tokens"] += int(tokens or 0)
            u0["cost_usd"] += float(cost_usd or 0.0)
        u = self._spent()
        b = self._sec("budget")
        if b.get("max_cost_usd") is not None and u["cost_usd"] > b["max_cost_usd"]:
            self.violate("budget", f"policy.budget.max_cost_usd={b['max_cost_usd']} exceeded (spent {u['cost_usd']:.4f})")
        if b.get("max_tokens") is not None and u["tokens"] > b["max_tokens"]:
            self.violate("budget", f"policy.budget.max_tokens={b['max_tokens']} exceeded (used {u['tokens']})")

    # tools -- the model can only act through the tools the pipeline gives it
    def tool_visible(self, *names: str) -> bool:
        """Static allow/deny (no counters): used to decide which tools the model is even shown."""
        if self.parent is not None and not self.parent.tool_visible(*names):
            return False
        t = self._sec("tools")
        if any(fnmatch.fnmatchcase(n, p) for n in names if n for p in (t.get("deny") or [])):
            return False
        allow = t.get("allow")
        return allow is None or any(fnmatch.fnmatchcase(n, p) for n in names if n for p in allow)

    def tool_refusal(self, tool: str, real_name: str, args: Any) -> Optional[str]:
        """Why this specific call must be refused, or None. Never raises: the refusal goes back to the model."""
        t = self._sec("tools")
        if self.parent is not None:
            inherited = self.parent.tool_refusal(tool, real_name, args)
            if inherited:
                return inherited
        if not self.tool_visible(tool, real_name):
            return f"tool {tool!r} is not permitted by policy.tools (allow/deny)"
        cap = t.get("max_calls_per_run")
        if cap is not None and self._spent()["tool_calls"] >= cap:
            return f"policy.tools.max_calls_per_run={cap} reached"
        args = args if isinstance(args, dict) else {}
        for rule in t.get("arg_rules") or []:
            if not any(fnmatch.fnmatchcase(n, rule.get("tool", "*")) for n in (tool, real_name) if n):
                continue
            arg = rule["arg"]
            if arg not in args:
                continue
            val = args[arg]
            sval = val if isinstance(val, str) else json.dumps(val, default=str)
            if "under" in rule and not self._under(sval, rule["under"]):
                return f"argument {arg!r}={sval!r} is outside {list(rule['under'])}"
            if "hosts" in rule and not self._host_ok(sval, rule["hosts"]):
                return f"argument {arg!r}={sval!r} is not an allowed host {list(rule['hosts'])}"
            if "allow_regex" in rule and not re.search(rule["allow_regex"], sval):
                return f"argument {arg!r} does not match required pattern {rule['allow_regex']!r}"
            if "deny_regex" in rule and re.search(rule["deny_regex"], sval):
                return f"argument {arg!r} matches forbidden pattern {rule['deny_regex']!r}"
        return None

    def guard_tool(self, tool: str, real_name: str, args: Any) -> Optional[str]:
        """Returns a refusal reason (caller must NOT run the tool) or None (allowed; counted)."""
        reason = self.tool_refusal(tool, real_name, args)
        if reason:
            self.violate("tools", reason, hard=False)
            if self._mode() == "warn":
                reason = None  # warn mode: record + log, but let the call through
        if reason is None:
            self.usage["tool_calls"] += 1
        return reason


@contextlib.contextmanager
def _policy_scope(cfg: Optional[Dict[str, Any]], usage: Dict[str, Any]):
    """Activate the policy for code that runs OUTSIDE a step (file/database ingest, sinks). No-op without a policy."""
    if not cfg:
        yield None
        return
    policy = _Policy(cfg, usage)
    token = _ACTIVE_POLICY.set(policy)
    try:
        yield policy
    finally:
        try:
            _ACTIVE_POLICY.reset(token)
        except ValueError:  # generator finalised in a different context; nothing active to restore
            pass


_ACTIVE_POLICY: "contextvars.ContextVar[Optional[_Policy]]" = contextvars.ContextVar("agentic_policy", default=None)
_REASONING_TRACE: "contextvars.ContextVar[Optional[list]]" = contextvars.ContextVar("agentic_reasoning", default=None)


def _pol() -> Optional[_Policy]:
    return _ACTIVE_POLICY.get()


def _env_get(name: str, default: Optional[str] = None) -> Optional[str]:
    """os.environ.get, subject to policy.env.allow."""
    p = _pol()
    if p is not None:
        p.check_env(name)
    return os.environ.get(name, default)


def _pol_path(path: Any, mode: str) -> None:
    p = _pol()
    if p is not None:
        p.check_path(path, mode)


def _pol_url(url: Any) -> None:
    p = _pol()
    if p is not None and url:
        p.check_url(url)


def _pol_component(component_type: str) -> None:
    p = _pol()
    if p is not None:
        p.check_component(component_type)


def _mcp_subprocess_env(extra: Optional[Dict[str, Any]] = None) -> Dict[str, str]:
    """Env handed to a stdio MCP server: the full process env (historical behaviour) unless policy.env.allow is
    set, in which case only allowed vars + a minimal baseline. The server's own YAML `env:` always applies."""
    p = _pol()
    if p is not None and p._sec("env").get("allow") is not None:
        base = {k: v for k, v in os.environ.items() if k in _SAFE_SUBPROCESS_ENV or p.env_allowed(k)}
    else:
        base = dict(os.environ)
    base.update({str(k): str(v) for k, v in (extra or {}).items()})
    return base


# ── Model-output normalisation (Qwen / DeepSeek-R1 style local models) ────────

_THINK_BLOCK = re.compile(r"<think>(.*?)</think>", re.S | re.I)
_TOOL_CALL_BLOCK = re.compile(r"<tool_call>\s*(\{.*?\})\s*</tool_call>", re.S)


def _split_reasoning(content: str) -> "tuple[str, Optional[str]]":
    """Split `<think>…</think>` reasoning out of model text -> (answer, reasoning).

    Handles the three shapes seen in practice: a closed block, an UNCLOSED block (output truncated by
    max_tokens mid-thought -> everything after `<think>` is reasoning and there is no answer yet), and a bare
    closing tag (some chat templates put the opening tag in the prompt)."""
    if not content or "think>" not in content.lower():
        return content, None
    parts = [m.strip() for m in _THINK_BLOCK.findall(content) if m.strip()]
    rest = _THINK_BLOCK.sub("", content)
    m = re.search(r"<think>(.*)$", rest, re.S | re.I)
    if m:  # unclosed
        if m.group(1).strip():
            parts.append(m.group(1).strip())
        rest = rest[: m.start()]
    elif re.search(r"</think>", rest, re.I) and not parts:  # bare closing tag
        before, after = re.split(r"</think>", rest, maxsplit=1, flags=re.I)
        if before.strip():
            parts.append(before.strip())
        rest = after
    return rest.strip(), ("\n\n".join(parts) if parts else None)


def _parse_text_tool_calls(content: str) -> "tuple[str, list]":
    """Some Qwen/Hermes-style models emit `<tool_call>{"name":…,"arguments":{…}}</tool_call>` as plain text when the
    server did not parse it into structured tool_calls. Recover those -> (remaining text, [{name, arguments(json str)}])."""
    calls = []
    for raw in _TOOL_CALL_BLOCK.findall(content or ""):
        try:
            obj = json.loads(raw)
        except json.JSONDecodeError:
            continue
        name = obj.get("name")
        if not name:
            continue
        args = obj.get("arguments", obj.get("parameters", {}))
        calls.append({"name": name, "arguments": args if isinstance(args, str) else json.dumps(args)})
    return (_TOOL_CALL_BLOCK.sub("", content).strip() if calls else content), calls


def _ollama_chat_model(model: str, has_tools: bool) -> str:
    """LiteLLM's `ollama/` route uses /api/generate (no tool support); `ollama_chat/` uses /api/chat, which does."""
    if has_tools and model.lower().startswith("ollama/"):
        return "ollama_chat/" + model.split("/", 1)[1]
    return model


def _note_reasoning(model: str, text: Optional[str]) -> None:
    trace = _REASONING_TRACE.get()
    if trace is not None and text:
        trace.append({"model": model, "text": text})


def _apply_partition_template(s: str, partition_key: Optional[str]) -> str:
    """Substitute `{partition_key}` into a string. No-op when unpartitioned."""
    if partition_key is None or "{partition_key}" not in s:
        return s
    return s.replace("{partition_key}", str(partition_key))


def _parse_partition_key(partition_key: Optional[str], parser: Optional[str]) -> Dict[str, Any]:
    """Invert a `partition_key_parser` template against `partition_key` to
    extract named fields. Supports typed placeholders: `{name}` (str, default),
    `{name:int}`, `{name:float}`, `{name:bool}` — the extracted value is
    coerced to that type in the returned dict. Callers use this so
    substitution can preserve type when a target string is exactly a single
    `{partition.<name>}` placeholder (e.g. tool_args expecting int).

    Returns {} on any mismatch — substitution then leaves placeholders in
    place so the gap is visible in logs.
    """
    if not partition_key or not parser:
        return {}
    import re
    # Match `{name}` or `{name:type}`.
    field_re = re.compile(r"\{([a-zA-Z_][a-zA-Z0-9_]*)(?::([a-z]+))?\}")
    # Split parser into literal + (field_name, type) pieces.
    parts: list = []
    last = 0
    for m in field_re.finditer(parser):
        parts.append(("lit", parser[last:m.start()]))
        parts.append(("field", m.group(1), m.group(2) or "str"))
        last = m.end()
    parts.append(("lit", parser[last:]))
    if not any(p[0] == "field" for p in parts):
        return {}
    regex_parts = []
    field_types: Dict[str, str] = {}
    field_seq = [i for i, p in enumerate(parts) if p[0] == "field"]
    for i, p in enumerate(parts):
        if p[0] == "lit":
            regex_parts.append(re.escape(p[1]))
        else:
            name, typ = p[1], p[2]
            field_types[name] = typ
            is_last = i == field_seq[-1]
            # Next literal char guides the greedy stop for middle fields.
            next_lit = parts[i + 1][1] if i + 1 < len(parts) and parts[i + 1][0] == "lit" else ""
            regex_parts.append(f"(?P<{name}>.+)" if is_last else f"(?P<{name}>[^{re.escape(next_lit[:1]) if next_lit else ''}]+)")
    pattern = "^" + "".join(regex_parts) + "$"
    m = re.match(pattern, partition_key)
    if not m:
        return {}
    raw = m.groupdict()
    typed: Dict[str, Any] = {}
    for k, v in raw.items():
        t = field_types.get(k, "str")
        try:
            if t == "int":
                typed[k] = int(v)
            elif t == "float":
                typed[k] = float(v)
            elif t == "bool":
                typed[k] = v.lower() in ("true", "1", "yes")
            else:
                typed[k] = v
        except (ValueError, TypeError):
            typed[k] = v  # fall back to string on coercion failure
    return typed


def _apply_ctx_substitutions(
    s: Any,
    partition_key: Optional[str],
    partition_fields: Optional[Dict[str, Any]],
) -> Any:
    """Apply run-context substitutions: `{partition_key}` + `{partition.<name>}`.

    Returns the input unchanged if it's not a string. When the input is
    EXACTLY a single `{partition.<name>}` placeholder (nothing else) and
    `partition_fields[name]` has a non-string type (int / float / bool),
    return the typed value rather than str-coerced — this is what makes
    `tool_args: {issue_number: "{partition.issue_number:int}"}` land as
    int 30000 in the MCP call instead of the string "30000".

    Ports (`{port_name}`) are handled separately by `_substitute_ports`
    since they're per-op inputs, not run-level context.
    """
    if not isinstance(s, str):
        return s
    # Type-preserving fast-path: whole value is `{partition.<name>}`.
    if partition_fields and s.startswith("{partition.") and s.endswith("}") and s.count("{") == 1:
        name = s[len("{partition."):-1]
        if name in partition_fields:
            return partition_fields[name]
    # Normal string substitution.
    if partition_key is not None and "{partition_key}" in s:
        s = s.replace("{partition_key}", str(partition_key))
    if partition_fields:
        for k, v in partition_fields.items():
            s = s.replace("{partition." + k + "}", str(v))
    return s


def _ctx_of(state: Dict[str, Any]) -> Dict[str, Any]:
    """Fetch the run-context stash from state (partition_key + parsed fields).
    Empty dict when the pipeline isn't running under a partition."""
    return state.get("__ctx__") or {}


_SRC_PLACEHOLDER_RE = re.compile(r"\{(partition_key|partition_start|partition_end|partition\.[A-Za-z0-9_\-]+)\}")


def _partition_window(partition_key: Optional[str], partitions_def) -> Optional[Dict[str, str]]:
    """{"partition_start", "partition_end"} (ISO) for a time-window partition (or the time dimension of a multi-partition),
    else None."""
    if partition_key is None or partitions_def is None:
        return None
    from dagster import MultiPartitionsDefinition, TimeWindowPartitionsDefinition
    try:
        if isinstance(partitions_def, TimeWindowPartitionsDefinition):
            w = partitions_def.time_window_for_partition_key(partition_key)
        elif isinstance(partitions_def, MultiPartitionsDefinition):
            kbd = getattr(partition_key, "keys_by_dimension", None)
            if kbd is None:
                return None
            w = None
            for d in partitions_def.partitions_defs:
                if isinstance(d.partitions_def, TimeWindowPartitionsDefinition):
                    w = d.partitions_def.time_window_for_partition_key(kbd[d.name])
                    break
        else:
            return None
    except Exception:
        return None
    return {"partition_start": w.start.isoformat(), "partition_end": w.end.isoformat()} if w is not None else None


def _source_placeholders(s: Any) -> List[str]:
    """Partition placeholders in a source string ({{ and }} are literal-brace escapes)."""
    return _SRC_PLACEHOLDER_RE.findall(str(s).replace("{{", "\0").replace("}}", "\0"))


def _bind_source_sql(sql: str, partition_key: Optional[str], partition_fields: Optional[Dict[str, Any]],
                     window: Optional[Dict[str, str]]) -> str:
    """Bind {partition_key} / {partition.<dim>} / {partition_start} / {partition_end} into a `database` source query as COMPLETE
    single-quoted SQL string literals (embedded quotes doubled; `{{` / `}}` are literal braces) -- same semantics as
    context_engineering_pipeline's warehouse_query source. Write `WHERE day = {partition_key}`, not `'{partition_key}'`: a
    placeholder already wrapped in quotes is rejected rather than double-quoted. The query runs through sqlalchemy.text(), so
    colons inside bound values are escaped (`\\:`) to avoid being read as bind parameters."""
    names = _source_placeholders(sql)
    unescape = lambda t: t.replace("{{", "{").replace("}}", "}")  # noqa: E731
    if not names:
        return unescape(sql)
    bindings: Dict[str, Any] = {}
    if partition_key is not None:
        bindings["partition_key"] = partition_key
    for k, v in (partition_fields or {}).items():
        bindings[f"partition.{k}"] = v
    bindings.update(window or {})
    for n in set(names):
        if n not in bindings:
            raise ValueError(f"source.query: placeholder {{{n}}} is not available for this run; available: {sorted(bindings)}")
        if re.search(rf"[\'\"`]\{{{re.escape(n)}\}}|\{{{re.escape(n)}\}}[\'\"`]", sql):
            raise ValueError(f"source.query: {{{n}}} is replaced by a complete quoted string literal; remove the quotes around it")
    protected = sql.replace("{{", "\0L").replace("}}", "\0R")

    def quote(m):
        v = str(bindings[m.group(1)]).replace("'", "''").replace(":", "\\:")
        return f"'{v}'"
    out = _SRC_PLACEHOLDER_RE.sub(quote, protected)
    return out.replace("\0L", "{").replace("\0R", "}")


def _resolve_source_headers(source_config: dict, sub) -> Dict[str, str]:
    """`url` source request headers. `headers` = {name: value} with partition placeholders templated; `headers_env` =
    {name: ENV_VAR} resolved at run time via dagster.EnvVar so tokens never sit in YAML. Values are never logged; an unset
    env var raises naming the variable and the header (not the value)."""
    out: Dict[str, str] = {}
    for k, v in (source_config.get("headers") or {}).items():
        out[str(k)] = str(sub(str(v)))
    for header_name, env_var in (source_config.get("headers_env") or {}).items():
        out[str(header_name)] = _require_env(str(env_var), purpose=f"source.headers_env header {header_name!r}")
    return out


def _ingest(
    source_config: dict,
    context,
    partition_key: Optional[str] = None,
    partition_fields: Optional[Dict[str, str]] = None,
    partitions_def=None,
) -> Dict[str, Any]:
    """Return the initial state dict: {"text": str, "source_kind": str, ...}."""
    kind = source_config.get("kind", "literal")
    if partitions_def is None and partition_key is not None:
        try:  # partitions attached via post_processing are only visible on the running asset
            partitions_def = context.assets_def.partitions_def
        except Exception:
            pass
    window = _partition_window(partition_key, partitions_def)

    def _sub(s: Any) -> Any:
        s = _apply_ctx_substitutions(s, partition_key, partition_fields)
        if window and isinstance(s, str):
            for k, v in window.items():
                s = s.replace("{" + k + "}", v)
        return s

    if kind == "literal":
        raw = source_config["text"]
        # YAML may parse `text: 30000` as int / `text: 3.14` as float —
        # downstream ops assume text is a string, so coerce here.
        text = _sub(raw if isinstance(raw, str) else str(raw))
        return {"text": text, "source_kind": "literal"}

    if kind == "file":
        path = _sub(source_config["path"])
        _pol_path(path, "read")
        with open(path) as f:
            text = f.read()
        return {"text": text, "source_kind": "file", "source_path": path}

    if kind == "url":
        import requests
        url = _sub(source_config["url"])
        _pol_url(url)
        headers = _resolve_source_headers(source_config, _sub)
        response = requests.get(url, headers=headers or None, timeout=source_config.get("timeout", 30))
        response.raise_for_status()
        return {"text": response.text, "source_kind": "url", "source_url": url}

    if kind == "database":
        import os
        import sqlalchemy
        import pandas as pd

        database_url = source_config.get("database_url")
        database_url_env_var = source_config.get("database_url_env_var")
        if database_url:
            url = database_url
        elif database_url_env_var:
            url = _env_get(database_url_env_var)
            if not url:
                raise EnvironmentError(f"Env var {database_url_env_var!r} is not set")
        else:
            raise ValueError("source kind=database requires 'database_url' or 'database_url_env_var'")

        query = _bind_source_sql(source_config["query"], partition_key, partition_fields, window)
        row_limit = source_config.get("row_limit", 500)

        engine = sqlalchemy.create_engine(url)
        try:
            with engine.begin() as conn:
                df = pd.read_sql(sqlalchemy.text(query), conn)
        finally:
            engine.dispose()

        truncated = len(df) > row_limit
        if truncated:
            df = df.head(row_limit)

        cols = list(df.columns)
        text = (
            "| " + " | ".join(cols) + " |\n"
            "| " + " | ".join(["---"] * len(cols)) + " |\n" +
            "\n".join("| " + " | ".join(str(v) for v in row) + " |" for row in df.itertuples(index=False))
        )
        if truncated:
            text += f"\n\n_(truncated to {row_limit} rows)_"

        return {
            "text": text,
            "source_kind": "database",
            "source_row_count": len(df),
        }

    # upstream_asset ingestion happens outside this function (it's a compute input).
    raise ValueError(
        f"unknown source kind: {kind!r}. valid: literal | file | url | database | upstream_asset"
    )


# ── LLM helper ───────────────────────────────────────────────────────

def _completion(
    *,
    model: str,
    system_prompt: Optional[str],
    user_prompt: str,
    api_key_env_var: Optional[str],
    api_base_env_var: Optional[str],
    temperature: float,
    max_tokens: int,
    tools: Optional[List[Dict[str, Any]]] = None,
    tool_choice: Optional[str] = None,
    reasoning_effort: Optional[str] = None,
    thinking_budget: Optional[int] = None,
    prompt_caching: bool = False,
) -> Dict[str, Any]:
    """Thin LiteLLM wrapper. Returns {"content": str, "tool_calls": [...], "usage": {...}}.

    Reasoning params (both optional, silently dropped on non-reasoning models):
      - reasoning_effort: 'low' | 'medium' | 'high' — OpenAI o1/o3, Gemini 2.5+
      - thinking_budget: int — max reasoning-trace tokens for Gemini 2.5+
                         (native) or Anthropic thinking mode (auto-mapped to
                         `thinking={type: enabled, budget_tokens: N}`).

    Prompt caching (`prompt_caching=True`): wraps the system prompt with
    Anthropic's `cache_control: {type: ephemeral}` block. Only applied to
    Anthropic-family models — silently ignored on OpenAI / Gemini / etc.
    First hit warms the cache (~25% surcharge on that call); subsequent
    calls within 5 min read from cache (~90% cheaper on the cached prefix).
    """
    try:
        import litellm
    except ImportError:
        raise ImportError("agentic_pipeline requires litellm: pip install 'litellm>=1.30.0'")

    # Silently drop params a specific model doesn't accept (e.g. Claude
    # Sonnet 5 only allows temperature=1; setting anything else without
    # this flag raises UnsupportedParamsError). Setting it here — not
    # globally at import time — keeps the change local to LLM calls
    # from this component.
    litellm.drop_params = True

    # Provider detection is used for reasoning-param routing AND for the
    # Anthropic-only prompt_caching wrapping (compute once, reuse below).
    # Widened to also catch the BARE (unprefixed) form of each model
    # family, not just already-prefixed ones -- confirmed live this
    # component only ever received bare names in practice (every config
    # its own docstrings/examples use is a bare "gpt-4o"/"gpt-4o-mini"),
    # and `is_gemini` previously could never be true for the form a user
    # would actually type.
    m_lower = model.lower()
    is_openai_ish = m_lower.startswith(("gpt-", "o1", "o3", "o4", "openai/", "azure/", "groq/"))
    is_gemini = m_lower.startswith(("gemini/", "google/", "vertex_ai/gemini", "gemini-"))
    is_anthropic = (
        "claude" in m_lower
        or m_lower.startswith(("anthropic/", "bedrock/anthropic."))
    )

    messages: List[Dict[str, Any]] = []
    if system_prompt:
        # Anthropic prompt caching: wrap the system prompt with a
        # cache_control block. LiteLLM translates this to Anthropic's
        # native cache_control header on the outbound request. Only
        # sends the wrapped shape to Anthropic — string shape used
        # everywhere else (OpenAI etc. don't accept the block shape).
        if prompt_caching and is_anthropic:
            messages.append({
                "role": "system",
                "content": [
                    {"type": "text", "text": system_prompt,
                     "cache_control": {"type": "ephemeral"}}
                ],
            })
        else:
            messages.append({"role": "system", "content": system_prompt})
    messages.append({"role": "user", "content": user_prompt})

    # NOT an Anthropic-specific quirk -- confirmed live against the real
    # litellm package: `gpt-4o` resolves bare because OpenAI is the one
    # family litellm's own provider-detection treats as an implicit
    # default; bare `claude-3-5-sonnet-20241022`, `gemini-2.0-flash`,
    # `command-r-plus`, and `mistral-large-latest` ALL raise the same
    # "LLM Provider NOT provided" error without an explicit prefix. Worse
    # for Gemini specifically: `gemini-2.5-pro` bare doesn't error at all
    # -- it silently resolves to the `vertex_ai` route (GCP project/
    # location auth) instead of the direct Google AI Studio API an
    # `api_key_env_var` is actually meant for, so relying on litellm's
    # implicit fallback there is actively unsafe, not just inconsistent.
    # is_anthropic/is_gemini above already detect the bare forms (for
    # prompt-caching/reasoning-param gating) -- reusing that detection to
    # also fix the provider routing itself, rather than just the two
    # families this was first caught on, covers every model choice
    # Designer's own planner actually offers (Claude, Gemini; GPT-4o
    # already works unprefixed) plus the other common bare-name
    # families below. An unrecognized family (someone's own
    # OpenAI-compatible endpoint, a brand new provider) passes through
    # unprefixed exactly as before -- this only ever ADDS a prefix it's
    # confident about, never guesses.
    _BARE_PREFIX_BY_FAMILY: List[tuple] = [
        ("anthropic/", is_anthropic, ("anthropic/", "bedrock/anthropic.")),
        ("gemini/", is_gemini, ("gemini/", "google/", "vertex_ai/")),
        ("cohere/", m_lower.startswith("command"), ("cohere/",)),
        ("mistral/", m_lower.startswith(("mistral-", "open-mixtral", "open-mistral", "codestral")), ("mistral/",)),
        ("deepseek/", m_lower.startswith("deepseek"), ("deepseek/",)),
        ("xai/", m_lower.startswith("grok"), ("xai/",)),
    ]
    effective_model = model
    for prefix, detected, already_prefixed in _BARE_PREFIX_BY_FAMILY:
        if detected and not m_lower.startswith(already_prefixed):
            effective_model = f"{prefix}{model}"
            break

    kwargs: Dict[str, Any] = {
        "model": effective_model,
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max_tokens,
    }
    if api_key_env_var:
        kwargs["api_key"] = _require_env(api_key_env_var)
    if api_base_env_var:
        kwargs["api_base"] = _require_env(api_base_env_var)
        _pol_url(kwargs["api_base"])
    if tools:
        kwargs["tools"] = tools
    if tool_choice:
        kwargs["tool_choice"] = tool_choice
    if tools and m_lower.startswith("ollama/"):
        # `ollama/` is /api/generate (no tool support); `ollama_chat/` is /api/chat. Ollama's chat API also has no
        # tool_choice="required", so ask for "auto" (callers already fall back when the model emits no tool call).
        kwargs["model"] = _ollama_chat_model(effective_model, True)
        if kwargs.get("tool_choice") == "required":
            kwargs["tool_choice"] = "auto"
    # Reasoning params are provider-specific. LiteLLM's `drop_params=True`
    # doesn't reliably strip them when forwarding to a provider that
    # doesn't accept them (thinking_budget → OpenAI reaches the wire and
    # errors), so filter client-side by model family (detected above).
    if reasoning_effort is not None and (is_openai_ish or is_gemini):
        kwargs["reasoning_effort"] = reasoning_effort
    if thinking_budget is not None:
        if is_gemini:
            kwargs["thinking_budget"] = int(thinking_budget)
        elif is_anthropic:
            kwargs["thinking"] = {"type": "enabled", "budget_tokens": int(thinking_budget)}

    _p = _pol()
    if _p is not None:
        _p.before_llm_call()
    call_started_at = time.time()
    response = litellm.completion(**kwargs)
    latency_ms = int((time.time() - call_started_at) * 1000)
    msg = response.choices[0].message
    # Reasoning models (Qwen3, DeepSeek-R1, ...) often inline their chain of thought as <think>…</think>. Keep it
    # out of the answer (downstream JSON parsing / routing would choke on it) and surface it as step metadata.
    content, _think = _split_reasoning(msg.content or "")

    # Surface reasoning traces when the provider returns them (Claude
    # thinking blocks, DeepSeek reasoning_content, some LiteLLM-wrapped
    # providers expose it on the message). Silently None for models that
    # don't emit reasoning.
    reasoning_content = getattr(msg, "reasoning_content", None) or None
    if not reasoning_content:
        # LiteLLM sometimes shoves it into `provider_specific_fields`.
        psf = getattr(msg, "provider_specific_fields", None)
        if isinstance(psf, dict):
            reasoning_content = psf.get("reasoning_content")
    reasoning_content = reasoning_content or _think
    _note_reasoning(effective_model, reasoning_content)

    tool_calls_out = []
    for tc in (msg.tool_calls or []):
        tool_calls_out.append({
            "name": tc.function.name,
            "arguments": tc.function.arguments,
        })
    if tools and not tool_calls_out:
        # Qwen/Hermes-style models sometimes emit <tool_call>{...}</tool_call> as plain text; recover it.
        content, _text_calls = _parse_text_tool_calls(content)
        tool_calls_out.extend(_text_calls)

    usage = None
    u = getattr(response, "usage", None)
    if u is not None:
        try:
            usage = u.model_dump()
        except AttributeError:
            try:
                usage = dict(u)
            except (TypeError, ValueError):
                usage = None

    # LiteLLM ships a maintained pricing table; fall back to None on providers
    # it doesn't know about (self-hosted / niche models).
    cost_usd = None
    try:
        cost_usd = float(litellm.completion_cost(completion_response=response))
    except Exception:
        cost_usd = None

    tokens_total = None
    cache_read_tokens = 0
    cache_creation_tokens = 0
    if usage is not None:
        # Both litellm and openai use the same key name.
        tt = usage.get("total_tokens")
        if isinstance(tt, (int, float)):
            tokens_total = int(tt)
        # LiteLLM surfaces Anthropic's cache-hit counters here for
        # anthropic/claude models when prompt_caching is on. Silently 0
        # when caching is off OR provider is non-Anthropic.
        for k in ("cache_read_input_tokens", "cache_read"):
            v = usage.get(k)
            if isinstance(v, (int, float)):
                cache_read_tokens = int(v)
                break
        for k in ("cache_creation_input_tokens", "cache_creation"):
            v = usage.get(k)
            if isinstance(v, (int, float)):
                cache_creation_tokens = int(v)
                break

    if _p is not None:
        _p.after_llm_call(cost_usd, tokens_total)

    return {
        "content": content,
        "tool_calls": tool_calls_out,
        "usage": usage,
        "cost_usd": cost_usd,
        "latency_ms": latency_ms,
        "tokens_total": tokens_total,
        "temperature": temperature,
        "reasoning_content": reasoning_content,
        "cache_read_tokens": cache_read_tokens,
        "cache_creation_tokens": cache_creation_tokens,
    }


_ROOT_SOURCE_ID = "source"
"""The reserved state key holding the pipeline's initial ingested entry.

A step that omits `source:` (and has no `sources:`/`inputs:`) defaults to
this — the pipeline's original input — NOT "whatever step happened to run
immediately before it." Dependency structure is declared, never implied by
position in the `steps:` list: two steps that don't reference each other
are independent regardless of execution order, and a step that wants a
specific prior step's output must say so explicitly via `source: <id>` or
typed `inputs: {name: {from: <id>}}`.
"""


def _get_source_text(state: Dict[str, Any], source_id: str) -> str:
    """Extract text from a prior step's output."""
    if source_id not in state:
        raise ValueError(f"step source={source_id!r} not found; known steps: {sorted(state.keys())}")
    entry = state[source_id]
    if isinstance(entry, dict):
        if "text" not in entry:
            raise ValueError(f"step {source_id!r} output has no 'text' field (keys: {list(entry.keys())})")
        return entry["text"]
    return str(entry)


def _last_step_id(state: Dict[str, Any]) -> str:
    """The most recent step id in state. Used only by `sub_pipeline`'s
    `output_step_id` default (a nested sub-pipeline's natural return value
    is its last step, the same way a function returns its last expression —
    unrelated to cross-step dependency resolution, which never implies a
    dependency from list position; see `_ROOT_SOURCE_ID`).
    Skips reserved keys prefixed with `__` (e.g. `__ctx__`)."""
    for k in reversed(list(state.keys())):
        if not (isinstance(k, str) and k.startswith("__")):
            return k
    raise ValueError("no user step outputs in state")


def _build_backfill_policy(partitions_def, policy: Optional[str], max_per_run: Optional[int]):
    """Validate `backfill_policy` / `backfill_policy_max_partitions_per_run` and return a dagster BackfillPolicy (or None).

    This pipeline resolves `{partition_key}` / `{partition.<name>}` in its source, steps, sinks and approval keys from the
    ONE partition a run was launched for, and its output is one value per partition. A run that covers several partitions
    (single_run, or multi_run with max_partitions_per_run > 1) has no single partition key, so those substitutions and the
    per-partition approval tokens would silently collapse. Only one partition per run is therefore supported."""
    where = "AgenticPipelineComponent"
    if max_per_run is not None and policy != "multi_run":
        raise ValueError(f"{where}: backfill_policy_max_partitions_per_run only applies to backfill_policy='multi_run'")
    if policy is None:
        return None
    if policy not in ("single_run", "multi_run"):
        raise ValueError(f"{where}: backfill_policy must be 'single_run' or 'multi_run', got {policy!r}")
    if partitions_def is None:
        return None  # ignored when the component is not partitioned, same as the other components
    if policy == "single_run":
        raise ValueError(
            f"{where}: backfill_policy='single_run' is not supported. A single run covering a whole partition range has no "
            "single partition_key, so {partition_key} templates, per-partition approval keys and sink paths cannot be resolved; "
            "use backfill_policy='multi_run' (one partition per run, optionally a cap on concurrent runs is set in the "
            "backfill itself).")
    if max_per_run is not None and (isinstance(max_per_run, bool) or not isinstance(max_per_run, int) or max_per_run < 1):
        raise ValueError(f"{where}: backfill_policy_max_partitions_per_run must be an integer >= 1")
    if max_per_run not in (None, 1):
        raise ValueError(
            f"{where}: backfill_policy_max_partitions_per_run={max_per_run} is not supported: a run with several partitions has "
            "no single partition_key (see backfill_policy='single_run'). Leave it unset or use 1.")
    return dg.BackfillPolicy.multi_run(max_partitions_per_run=1)


def _step_strings(obj: Any):
    if isinstance(obj, str):
        yield obj
    elif isinstance(obj, dict):
        for v in obj.values():
            yield from _step_strings(v)
    elif isinstance(obj, (list, tuple)):
        for v in obj:
            yield from _step_strings(v)


def _validate_source_partitioning(source_config: Dict[str, Any], steps: List[Dict[str, Any]], partitions_def) -> None:
    """A partitioned pipeline whose INPUT never mentions the partition reads the same input in every partition.

    Per source kind (what can be templated, and what an identical read means):
      * upstream_asset -- nothing to template: a partitioned upstream maps partition-to-partition automatically. No check.
      * literal -- constant text can never differ between partitions: RAISE, unless some step consumes the partition
        (a prompt_template / tool_args / ... with a partition placeholder), which makes a fixed source a deliberate choice.
      * file / url -- WARN: the content behind a static path/URL may legitimately change between runs (a "latest" snapshot),
        but a backfill cannot reproduce past partitions from it. Raising would forbid that real use.
      * database -- WARN: the query CAN be bound to the partition ({partition_key}, {partition_start}/{partition_end}); without
        it every partition reads the whole table. (Not an error: a deliberate full re-read per partition is possible.)
    Placeholders that cannot resolve (e.g. {partition_start} without a time-window partition) always raise."""
    kind = source_config.get("kind", "literal")
    if source_config.get("full_read_per_partition") is not None:
        if kind not in ("file", "url", "database"):
            raise ValueError(f"source.full_read_per_partition has no meaning for kind={kind!r} (only file | url | database)")
        if not isinstance(source_config["full_read_per_partition"], bool):
            raise ValueError("source.full_read_per_partition must be true or false")
    field = {"literal": "text", "file": "path", "url": "url", "database": "query"}.get(kind)
    if field is None:
        return
    value = str(source_config.get(field, ""))
    names = set(_source_placeholders(value))
    if names & {"partition_start", "partition_end"}:
        from dagster import MultiPartitionsDefinition, TimeWindowPartitionsDefinition
        timed = isinstance(partitions_def, TimeWindowPartitionsDefinition) or (
            isinstance(partitions_def, MultiPartitionsDefinition)
            and any(isinstance(d.partitions_def, TimeWindowPartitionsDefinition) for d in partitions_def.partitions_defs))
        if not timed:
            raise ValueError(f"source.{field}: {{partition_start}} / {{partition_end}} need a daily/weekly/monthly/hourly partition "
                             "(or a multi-partition with a time dimension)")
    if partitions_def is None:
        if names and kind == "database":
            raise ValueError(f"source.query uses partition placeholder(s) {sorted(names)} but no partitioning is configured")
        return
    if names:
        return
    used_in_steps = any(
        _source_placeholders(t) for st in steps for k, v in st.items()
        if not (st.get("op") == "approve" and k in ("key", "approval_dir")) for t in _step_strings(v))
    if kind == "literal" and not used_in_steps:
        raise ValueError(
            "partitioned pipeline, but the literal source text has no {partition_key} / {partition.<name>} / {partition_start} / "
            "{partition_end} placeholder and no step uses one either: every partition would process the identical input. "
            "Template the source (or a step's prompt), or drop the partitioning.")
    if kind in ("file", "url", "database") and not (kind != "database" and used_in_steps) \
            and not source_config.get("full_read_per_partition"):
        logging.getLogger(__name__).warning(
            "agentic_pipeline: partitioned, but source.%s (kind=%s) has no partition placeholder ({partition_key}, "
            "{partition.<name>}, {partition_start}, {partition_end}): every partition reads the SAME input%s. "
            "If that is deliberate (a 'latest snapshot'), set source.full_read_per_partition: true to silence this.",
            field, kind, " (the whole query result)" if kind == "database" else "")


def _validate_approve_partitioning(steps: List[Dict[str, Any]], partitioned: bool) -> None:
    """Approval tokens live at `<approval_dir>/<key>.json`, so under partitioning every `approve` step needs a key that is
    unique per partition AND per step, or one human decision silently approves other partitions / other gates."""
    if not partitioned:
        return
    approves = [st for st in steps if st.get("op") == "approve"]
    for st in approves:
        k = st.get("key")
        if k and "{partition" not in str(k):
            raise ValueError(
                f"approve step {st.get('id')!r}: key {k!r} is the same for every partition, so one approval would clear all of them; "
                "include {partition_key} (or a {partition.<name>}) in the key, or omit `key` to use the partition key")
    if sum(1 for st in approves if not st.get("key")) > 1:
        raise ValueError(
            "several approve steps without an explicit `key` under partitioning would share one token per partition (the "
            "default key is the partition key); give each a key such as '{partition_key}-<step id>'")


def _run_is_partitioned(context) -> bool:
    """True when this run targets partitions although the component declared none (partitions attached via post_processing)."""
    try:
        if context.has_partition_key_range:
            return True
    except Exception:
        pass
    try:
        return context.assets_def.partitions_def is not None
    except Exception:
        return False


def _require_one_partition(context, partitioned: bool) -> None:
    """A partitioned run must be for exactly one partition; otherwise partition_key would be None and every
    `{partition_key}` template / approval key would silently resolve as if unpartitioned.

    `partitioned` is what the component's own fields declared at load. Partitions attached later (post_processing) are
    invisible then, so the run itself is also asked: a run that targets a partition RANGE (or an asset whose partitions_def
    was set elsewhere) without a single partition key is rejected the same way."""
    if context.has_partition_key:
        return
    if partitioned or _run_is_partitioned(context):
        raise RuntimeError(
            "AgenticPipelineComponent: this run covers several partitions (or none), but the pipeline resolves "
            "{partition_key}, sink paths and approval keys per single partition. Launch one partition per run.")


def _build_partitions_def(
    partition_type, partition_start, partition_values,
    dynamic_partition_name, partition_dimensions,
    cron_schedule=None,
    cron_fmt="%Y-%m-%d",
):
    """Build a Dagster PartitionsDefinition from the first-class fields.

    Mirrors the pattern used by HumanApprovalGateComponent — same
    field names, same shape, same coercions (comma-string OR list for
    `partition_values`).
    """
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )
    if partition_dimensions and partition_type:
        raise ValueError("Set either partition_type or partition_dimensions, not both.")

    def _build_axis(spec):
        t = spec.get("type")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily": return DailyPartitionsDefinition(start_date=spec["start"])
        if t == "weekly": return WeeklyPartitionsDefinition(start_date=spec["start"])
        if t == "monthly": return MonthlyPartitionsDefinition(start_date=spec["start"])
        if t == "hourly": return HourlyPartitionsDefinition(start_date=spec["start"])
        if t == "cron":
            from dagster import TimeWindowPartitionsDefinition
            if not spec.get("start"):
                raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
            if not spec.get("cron_schedule"):
                raise ValueError("partition dimension type='cron' requires 'cron_schedule'")
            return TimeWindowPartitionsDefinition(start=spec["start"], fmt=spec.get("fmt") or "%Y-%m-%d", cron_schedule=spec["cron_schedule"])
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
        return MultiPartitionsDefinition({d["name"]: _build_axis(d) for d in partition_dimensions})

    if not partition_type:
        return None
    if isinstance(partition_values, (list, tuple)):
        _values = [str(v).strip() for v in partition_values if str(v).strip()]
    else:
        _values = [v.strip() for v in (str(partition_values) if partition_values else "").split(",") if v.strip()]
    if partition_type in ("daily", "weekly", "monthly", "hourly", "cron") and not partition_start:
        raise ValueError(f"partition_type={partition_type!r} requires partition_start (ISO date).")
    if partition_type == "daily": return DailyPartitionsDefinition(start_date=partition_start)
    if partition_type == "weekly": return WeeklyPartitionsDefinition(start_date=partition_start)
    if partition_type == "monthly": return MonthlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "hourly": return HourlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "cron":
        from dagster import TimeWindowPartitionsDefinition
        if not cron_schedule:
            raise ValueError("partition_type='cron' requires cron_schedule, e.g. '0 0 * * 1-5' for weekdays only.")
        return TimeWindowPartitionsDefinition(start=partition_start, fmt=cron_fmt, cron_schedule=cron_schedule)
    if partition_type == "static":
        if not _values: raise ValueError("partition_type='static' requires partition_values.")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    raise ValueError(f"unknown partition_type: {partition_type!r}")


def _resolve_inputs(step: dict, state: Dict[str, Any]) -> Dict[str, str]:
    """Resolve `inputs:` field into a `{port_name: text}` dict for template substitution.

    Shape (any op can use this):

        inputs:
          <port_name>: {from: <step_id>}      # read prior step's text output
          <port_name>: {literal: <value>}     # inline literal value (str-coerced)

    Returns an empty dict if `inputs:` is absent — every op falls back to
    its legacy single-source `source:` / `sources:` behavior in that case,
    so old YAML keeps working unchanged.

    This is the "typed named I/O" primitive that lets any step join from
    any number of upstream steps by port name — matches the fan-out /
    fan-in shape common in agentic-orchestration graphs (Prefect-style
    execution plans, LangGraph joins, etc.).
    """
    raw = step.get("inputs") or {}
    if not raw:
        return {}
    if not isinstance(raw, dict):
        raise ValueError(
            f"step {step.get('id')!r}: `inputs:` must be a dict mapping port "
            f"name → {{from: <step_id>}} | {{literal: <value>}}."
        )
    resolved: Dict[str, str] = {}
    for name, spec in raw.items():
        if not isinstance(spec, dict):
            raise ValueError(
                f"step {step.get('id')!r}: input {name!r} must be "
                f"{{from: <step_id>}} or {{literal: <value>}}, got {type(spec).__name__}."
            )
        if "from" in spec:
            source_id = spec["from"]
            if source_id not in state:
                raise ValueError(
                    f"step {step.get('id')!r}: input {name!r} references "
                    f"unknown upstream step {source_id!r}. Known: {sorted(state.keys())}"
                )
            resolved[name] = _get_source_text(state, source_id)
        elif "literal" in spec:
            resolved[name] = str(spec["literal"])
        else:
            raise ValueError(
                f"step {step.get('id')!r}: input {name!r} needs `from:` or `literal:`."
            )
    return resolved


def _substitute_ports(template: str, ports: Dict[str, str]) -> str:
    """Substitute `{port_name}` placeholders in a template with resolved text.

    Uses plain str.replace (not `.format`) so `{` / `}` inside prompts
    (e.g. JSON braces) don't blow up. Ports missing from `ports` are
    left as `{port_name}` in the output — the LLM sees it as literal,
    which is a useful debugging signal when a template references a
    port name that wasn't declared.
    """
    if not template or not ports:
        return template
    for k, v in ports.items():
        template = template.replace("{" + k + "}", v)
    return template


# ── Op executors ─────────────────────────────────────────────────────

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sum_cost(*results) -> Optional[float]:
    xs = [r.get("cost_usd") for r in results if r and r.get("cost_usd") is not None]
    return round(sum(xs), 6) if xs else None


def _sum_latency(*results) -> int:
    return sum((r.get("latency_ms") or 0) for r in results if r)


def _sum_tokens(*results) -> Optional[int]:
    xs = [r.get("tokens_total") for r in results if r and r.get("tokens_total") is not None]
    return sum(xs) if xs else None


def _do_llm_call(step: dict, state: Dict[str, Any], context) -> Dict[str, Any]:
    """Single LLM call.

    Two input modes (mutually compatible — inputs takes precedence for
    named substitution, source still resolves for the `{text}` fallback):

    1. Legacy single-source: `source: <step_id>` (or omit for last step) →
       prompt_template's `{text}` placeholder substitutes with that step's text.

    2. Typed named inputs: `inputs: {port_name: {from: step_id} | {literal: value}}` →
       each port name becomes a `{port_name}` placeholder in prompt_template
       AND system_prompt. Enables multi-input joins from arbitrary upstream
       steps (the "any op joins from any prior op" shape).

    prompt_template supports both — `{text}` for legacy source, `{port_name}`
    for typed inputs.
    """
    materialized_at = _now_iso()
    source_id = step.get("source", _ROOT_SOURCE_ID)
    src_text = _get_source_text(state, source_id)

    inputs = _resolve_inputs(step, state)

    # {partition_key} + {partition.<name>} were substituted upstream in
    # _run_step. Only {text} + {port_name} are per-op here.
    prompt_template = step.get("prompt_template", "{text}")
    user_prompt = prompt_template.replace("{text}", src_text)
    user_prompt = _substitute_ports(user_prompt, inputs)
    system_prompt = step.get("system_prompt")
    if system_prompt and inputs:
        system_prompt = _substitute_ports(system_prompt, inputs)

    temperature = step.get("temperature", 0.0)

    result = _completion(
        model=step["model"],
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        api_key_env_var=step.get("api_key_env_var"),
        api_base_env_var=step.get("api_base_env_var"),
        temperature=temperature,
        max_tokens=step.get("max_tokens", 2048),
        reasoning_effort=step.get("reasoning_effort"),
        thinking_budget=step.get("thinking_budget"),
        prompt_caching=bool(step.get("prompt_caching", False)),
    )
    return {
        "text": result["content"],
        "model": step["model"],
        "model_fingerprint": f"{step['model']}@t{temperature}",
        "materialized_at": materialized_at,
        "cost_usd": _sum_cost(result),
        "latency_ms": _sum_latency(result),
        "tokens_total": _sum_tokens(result),
        "n_llm_calls": 1,
        "usage": result["usage"],
        "prompt": user_prompt,
        "inputs_used": list(inputs.keys()) if inputs else None,
        "op": "llm_call",
    }


def _do_route(step: dict, state: Dict[str, Any], context) -> Dict[str, Any]:
    """Router picks best specialist; specialist answers.

    router: {model, api_key_env_var, [system_prompt, temperature, max_tokens]}
    specialists: [{name, description, model, [api_key_env_var, system_prompt, temperature, max_tokens]}]
    fallback: <specialist_name>  (optional)
    """
    import json

    materialized_at = _now_iso()
    source_id = step.get("source", _ROOT_SOURCE_ID)
    src_text = _get_source_text(state, source_id)

    router = step["router"]
    specialists = step["specialists"]
    fallback = step.get("fallback")
    include_reasoning = step.get("include_reasoning", True)

    names = [s["name"] for s in specialists]
    if len(set(names)) != len(names):
        raise ValueError(f"specialist names must be unique: got {names}")
    for n in names:
        if not n.replace("_", "").isalnum():
            raise ValueError(
                f"specialist name {n!r} must be a valid identifier — router uses it as tool name."
            )
    if fallback is not None and fallback not in names:
        raise ValueError(f"fallback={fallback!r} not in specialists {names}")

    # One tool per specialist. Router calls exactly one.
    tools = []
    for s in specialists:
        params = {"type": "object", "properties": {}, "required": []}
        if include_reasoning:
            params["properties"]["reasoning"] = {
                "type": "string",
                "description": "One-sentence reason this specialist fits.",
            }
            params["required"].append("reasoning")
        tools.append({
            "type": "function",
            "function": {
                "name": s["name"],
                "description": s["description"],
                "parameters": params,
            },
        })

    router_system = router.get("system_prompt") or _default_router_prompt(specialists)
    context.log.info(f"[route:{step.get('id', '?')}] routing through {router['model']} across {len(specialists)} specialists")

    router_result = _completion(
        model=router["model"],
        system_prompt=router_system,
        user_prompt=src_text,
        api_key_env_var=router.get("api_key_env_var"),
        api_base_env_var=router.get("api_base_env_var"),
        temperature=router.get("temperature", 0.0),
        max_tokens=router.get("max_tokens", 512),
        tools=tools,
        tool_choice="required",
        reasoning_effort=router.get("reasoning_effort"),
        thinking_budget=router.get("thinking_budget"),
        prompt_caching=bool(router.get("prompt_caching", False)),
    )

    selected = None
    reasoning = None
    routing_source = "router_tool_call"
    if router_result["tool_calls"]:
        pick = router_result["tool_calls"][0]["name"]
        if pick in names:
            selected = pick
            if include_reasoning:
                try:
                    args = json.loads(router_result["tool_calls"][0]["arguments"] or "{}")
                    reasoning = args.get("reasoning")
                except json.JSONDecodeError:
                    reasoning = None
        else:
            context.log.warning(f"[route] router picked unknown specialist {pick!r}")

    if selected is None:
        if fallback is None:
            raise RuntimeError(f"[route] router failed to pick a valid specialist and no fallback set")
        context.log.warning(f"[route] falling back to {fallback!r}")
        selected = fallback
        routing_source = "fallback"

    specialist = next(s for s in specialists if s["name"] == selected)
    context.log.info(f"[route] selected specialist={selected} → {specialist['model']}")

    specialist_result = _completion(
        model=specialist["model"],
        system_prompt=specialist.get("system_prompt"),
        user_prompt=src_text,
        api_key_env_var=specialist.get("api_key_env_var"),
        api_base_env_var=specialist.get("api_base_env_var"),
        temperature=specialist.get("temperature", 0.0),
        max_tokens=specialist.get("max_tokens", 2048),
        reasoning_effort=specialist.get("reasoning_effort"),
        thinking_budget=specialist.get("thinking_budget"),
        prompt_caching=bool(specialist.get("prompt_caching", False)),
    )

    return {
        "text": specialist_result["content"],
        "selected_specialist": selected,
        "router_reasoning": reasoning,
        "routing_source": routing_source,
        "router_model": router["model"],
        "specialist_model": specialist["model"],
        "model_fingerprint": f"{router['model']}→{specialist['model']}",
        "materialized_at": materialized_at,
        "cost_usd": _sum_cost(router_result, specialist_result),
        "latency_ms": _sum_latency(router_result, specialist_result),
        "tokens_total": _sum_tokens(router_result, specialist_result),
        "n_llm_calls": 2,
        "usage": {"router": router_result["usage"], "specialist": specialist_result["usage"]},
        "op": "route",
    }


def _evaluate_condition(src_text: str, cond: Dict[str, Any]) -> bool:
    """Deterministic condition evaluator for `conditional_route`.

    Recognized predicates in `cond`:
      regex:      Python regex against src_text (re.search, case-insensitive).
      contains:   substring test (case-insensitive).
      equals:     exact match after strip().
      jsonpath:   dotted key/index path against src_text parsed as JSON.
                  e.g. `$.labels[0]` on `{"labels":["bug"]}` → "bug".
                  Non-JSON src_text → False. Pair with `equals:` / `contains:`
                  on `value:` to test the resolved node, e.g.
                  `{jsonpath: "$.priority", equals: "p0"}`.
    Exactly one comparison predicate must be present (regex | contains |
    equals | jsonpath+value).
    """
    keys = set(cond.keys())
    if "regex" in keys:
        return re.search(cond["regex"], src_text, re.IGNORECASE) is not None
    if "contains" in keys:
        return cond["contains"].lower() in src_text.lower()
    if "equals" in keys:
        return src_text.strip() == cond["equals"]
    if "jsonpath" in keys:
        try:
            payload = json.loads(src_text)
        except (json.JSONDecodeError, ValueError):
            return False
        path = cond["jsonpath"]
        if path.startswith("$."):
            path = path[2:]
        elif path.startswith("$"):
            path = path[1:].lstrip(".")
        node: Any = payload
        for part in path.split("."):
            if not part:
                continue
            # Array-index chunks: `labels[0]` → `labels` then `[0]`.
            while "[" in part and part.endswith("]"):
                base, idx = part[:-1].split("[", 1)
                if base:
                    if not isinstance(node, dict) or base not in node:
                        return False
                    node = node[base]
                try:
                    node = node[int(idx)]
                except (ValueError, IndexError, TypeError):
                    return False
                part = ""
            if part:
                if not isinstance(node, dict) or part not in node:
                    return False
                node = node[part]
        node_str = json.dumps(node) if not isinstance(node, str) else node
        if "equals" in keys:  # unreachable (already returned above) — kept for clarity
            return node_str == cond["equals"]
        if "value_equals" in keys:
            return node_str.strip() == str(cond["value_equals"])
        if "value_contains" in keys:
            return str(cond["value_contains"]).lower() in node_str.lower()
        # Bare jsonpath: truthy check on resolved node.
        return bool(node)
    raise ValueError(
        f"conditional_route: condition must contain one of "
        f"`regex` | `contains` | `equals` | `jsonpath`; got {sorted(keys)}"
    )


def _do_conditional_route(step: dict, state: Dict[str, Any], context) -> Dict[str, Any]:
    """Deterministic router — picks specialist by regex / contains / equals /
    JSON-path against upstream text (no router LLM). Then the picked
    specialist runs a normal LLM call. Sibling of `route`, but the pick
    is a code path, not a model call.

    conditions:  [{when: {regex|contains|equals|jsonpath: ...}, then: <specialist_name>}, ...]
                 (evaluated in order — first match wins)
    default:     <specialist_name>   (required — runs when no condition matches)
    specialists: [{name, description, model, [api_key_env_var, system_prompt,
                   temperature, max_tokens]}]

    Use when you want branching that's reviewable in code / cheap / testable:
    label-based triage (`labels: ["bug"]` → bug_specialist), priority tags
    (regex `p[01]` → high_priority), size-based routing, etc. When the
    signal is soft ("does this issue read as a question?"), prefer `route`.
    """
    materialized_at = _now_iso()
    source_id = step.get("source", _ROOT_SOURCE_ID)
    src_text = _get_source_text(state, source_id)

    conditions = step.get("conditions") or []
    default = step.get("default")
    specialists = step.get("specialists") or []
    if not specialists:
        raise ValueError("conditional_route requires `specialists: [...]`.")
    if not default:
        raise ValueError(
            "conditional_route requires `default: <specialist_name>` — "
            "the specialist that runs when no condition matches."
        )

    names = [s["name"] for s in specialists]
    if len(set(names)) != len(names):
        raise ValueError(f"specialist names must be unique: got {names}")
    if default not in names:
        raise ValueError(f"default={default!r} not in specialists {names}")
    for cond in conditions:
        then = cond.get("then")
        if not then:
            raise ValueError(f"conditional_route: condition {cond!r} missing `then: <specialist>`")
        if then not in names:
            raise ValueError(
                f"conditional_route: condition points at unknown specialist {then!r}. Known: {names}"
            )

    selected = None
    match_index = -1
    matched_condition: Optional[Dict[str, Any]] = None
    for i, cond in enumerate(conditions):
        when = cond.get("when") or {}
        if not when:
            raise ValueError(f"conditional_route: condition {cond!r} missing `when: {{...}}`")
        try:
            if _evaluate_condition(src_text, when):
                selected = cond["then"]
                match_index = i
                matched_condition = cond
                break
        except Exception as e:  # noqa: BLE001 — surface bad conditions clearly
            raise ValueError(
                f"conditional_route: condition #{i} ({cond!r}) failed to evaluate: {e}"
            ) from e

    routing_source = "condition_match"
    if selected is None:
        selected = default
        routing_source = "default"

    specialist = next(s for s in specialists if s["name"] == selected)
    context.log.info(
        f"[conditional_route:{step.get('id', '?')}] picked={selected!r} "
        f"via={routing_source} (condition #{match_index})"
    )

    specialist_result = _completion(
        model=specialist["model"],
        system_prompt=specialist.get("system_prompt"),
        user_prompt=src_text,
        api_key_env_var=specialist.get("api_key_env_var"),
        api_base_env_var=specialist.get("api_base_env_var"),
        temperature=specialist.get("temperature", 0.0),
        max_tokens=specialist.get("max_tokens", 2048),
        reasoning_effort=specialist.get("reasoning_effort"),
        thinking_budget=specialist.get("thinking_budget"),
        prompt_caching=bool(specialist.get("prompt_caching", False)),
    )

    return {
        "text": specialist_result["content"],
        "selected_specialist": selected,
        "routing_source": routing_source,
        "match_index": match_index,
        "matched_condition": matched_condition,
        "specialist_model": specialist["model"],
        "model_fingerprint": f"[deterministic]→{specialist['model']}",
        "materialized_at": materialized_at,
        "cost_usd": _sum_cost(specialist_result),
        "latency_ms": _sum_latency(specialist_result),
        "tokens_total": _sum_tokens(specialist_result),
        "n_llm_calls": 1,
        "usage": {"specialist": specialist_result["usage"]},
        "op": "conditional_route",
    }


def _do_debate(step: dict, state: Dict[str, Any], context) -> Dict[str, Any]:
    """N proposers each write a proposal; arbitrator picks the winner.

    proposers:  [{model, [system_prompt, api_key_env_var, temperature, max_tokens]}]
    arbitrator: {model, [system_prompt, api_key_env_var, temperature, max_tokens]}
    """
    import json

    materialized_at = _now_iso()
    source_id = step.get("source", _ROOT_SOURCE_ID)
    src_text = _get_source_text(state, source_id)

    proposers = step["proposers"]
    arbitrator = step["arbitrator"]

    if len(proposers) < 2:
        raise ValueError(f"debate requires >=2 proposers; got {len(proposers)}")

    context.log.info(f"[debate:{step.get('id', '?')}] {len(proposers)} proposers → arbitrator {arbitrator['model']}")

    proposals = []
    proposer_usage = []
    proposer_results = []
    for i, p in enumerate(proposers):
        context.log.info(f"[debate] proposer {i} ({p['model']}) writing proposal")
        r = _completion(
            model=p["model"],
            system_prompt=p.get("system_prompt"),
            user_prompt=src_text,
            api_key_env_var=p.get("api_key_env_var"),
            api_base_env_var=p.get("api_base_env_var"),
            temperature=p.get("temperature", 0.7),
            max_tokens=p.get("max_tokens", 2048),
            reasoning_effort=p.get("reasoning_effort"),
            thinking_budget=p.get("thinking_budget"),
            prompt_caching=bool(p.get("prompt_caching", False)),
        )
        proposals.append({"index": i, "model": p["model"], "text": r["content"]})
        proposer_usage.append(r["usage"])
        proposer_results.append(r)

    # Arbitrator picks via function call (one tool per proposal index).
    tools = []
    for i in range(len(proposals)):
        tools.append({
            "type": "function",
            "function": {
                "name": f"pick_{i}",
                "description": f"Select proposal {i} as the winner.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "reasoning": {"type": "string", "description": "Why this proposal wins."}
                    },
                    "required": ["reasoning"],
                },
            },
        })

    arb_prompt_parts = [f"Original question / context:\n{src_text}\n\nProposals to judge:\n"]
    for p in proposals:
        arb_prompt_parts.append(f"\n--- Proposal {p['index']} (model={p['model']}) ---\n{p['text']}\n")
    arb_prompt_parts.append(
        f"\nCall exactly one of pick_0..pick_{len(proposals) - 1} with a one-sentence reasoning."
    )
    arb_prompt = "".join(arb_prompt_parts)

    arb_system = arbitrator.get("system_prompt") or (
        "You are an impartial judge. Read the original question and every proposal, "
        "then pick the strongest by calling exactly one of the provided pick_N tools."
    )

    arb_result = _completion(
        model=arbitrator["model"],
        system_prompt=arb_system,
        user_prompt=arb_prompt,
        api_key_env_var=arbitrator.get("api_key_env_var"),
        api_base_env_var=arbitrator.get("api_base_env_var"),
        temperature=arbitrator.get("temperature", 0.0),
        max_tokens=arbitrator.get("max_tokens", 512),
        tools=tools,
        tool_choice="required",
        reasoning_effort=arbitrator.get("reasoning_effort"),
        thinking_budget=arbitrator.get("thinking_budget"),
        prompt_caching=bool(arbitrator.get("prompt_caching", False)),
    )

    winner_index = None
    arb_reasoning = None
    if arb_result["tool_calls"]:
        tool_name = arb_result["tool_calls"][0]["name"]
        if tool_name.startswith("pick_"):
            try:
                winner_index = int(tool_name.split("_", 1)[1])
                args = json.loads(arb_result["tool_calls"][0]["arguments"] or "{}")
                arb_reasoning = args.get("reasoning")
            except (ValueError, json.JSONDecodeError):
                winner_index = None

    if winner_index is None or winner_index not in range(len(proposals)):
        context.log.warning("[debate] arbitrator failed to pick; defaulting to proposal 0")
        winner_index = 0
        arb_reasoning = arb_reasoning or "(arbitrator failed to pick, defaulted to proposal 0)"

    winner = proposals[winner_index]
    context.log.info(f"[debate] winner=proposal {winner_index} ({winner['model']})")

    proposer_models = ",".join(p["model"] for p in proposers)
    return {
        "text": winner["text"],
        "winner_index": winner_index,
        "winner_model": winner["model"],
        "arbitrator_reasoning": arb_reasoning,
        "all_proposals": proposals,
        "arbitrator_model": arbitrator["model"],
        "model_fingerprint": f"[{proposer_models}]→{arbitrator['model']}",
        "materialized_at": materialized_at,
        "cost_usd": _sum_cost(*proposer_results, arb_result),
        "latency_ms": _sum_latency(*proposer_results, arb_result),
        "tokens_total": _sum_tokens(*proposer_results, arb_result),
        "n_llm_calls": len(proposer_results) + 1,
        "usage": {"proposers": proposer_usage, "arbitrator": arb_result["usage"]},
        "op": "debate",
    }


_SCORE_RE = re.compile(r"SCORE\s*[:=]\s*(\d+(?:\.\d+)?)\s*/\s*100", re.IGNORECASE)


def _extract_critic_score(text: str) -> Optional[float]:
    """Pull `SCORE: N/100` (case-insensitive, tolerant of `SCORE=` / whitespace)
    out of a critic response. Returns None if not present or unparseable —
    callers treat "no score" as "keep iterating"."""
    if not text:
        return None
    m = _SCORE_RE.search(text)
    if not m:
        return None
    try:
        return float(m.group(1))
    except ValueError:
        return None


def _do_critique_loop(step: dict, state: Dict[str, Any], context) -> Dict[str, Any]:
    """Drafter writes; critic reviews; drafter revises. Repeat.

    drafter:   {model, [system_prompt, api_key_env_var, temperature, max_tokens]}
    critic:    {model, [system_prompt, api_key_env_var, temperature, max_tokens]}
    iterations: 2   (number of critique/revise cycles; >=1). Hard cap even when
                    `until_score_gte:` is set — think of it as the timeout.
    until_score_gte: Optional[float] (0-100). When set, appends a scoring
                    instruction to the critic's system prompt; after each
                    critique, extracts `SCORE: N/100` and stops early (skipping
                    the revise step) when N >= threshold. The current draft is
                    already good enough. Preserves `iterations` as an upper bound.
    """
    materialized_at = _now_iso()
    source_id = step.get("source", _ROOT_SOURCE_ID)
    src_text = _get_source_text(state, source_id)

    drafter = step["drafter"]
    critic = step["critic"]
    iterations = int(step.get("iterations", 2))
    if iterations < 1:
        raise ValueError(f"critique_loop iterations must be >=1; got {iterations}")

    until_score_gte = step.get("until_score_gte")
    if until_score_gte is not None:
        try:
            until_score_gte = float(until_score_gte)
        except (TypeError, ValueError) as e:
            raise ValueError(
                f"critique_loop until_score_gte must be a number (0-100); got {step['until_score_gte']!r}"
            ) from e
        if not (0 <= until_score_gte <= 100):
            raise ValueError(
                f"critique_loop until_score_gte must be between 0 and 100; got {until_score_gte}"
            )

    all_llm_results = []
    stop_reason = "iterations_exhausted"
    final_score: Optional[float] = None

    context.log.info(
        f"[critique_loop:{step.get('id', '?')}] drafter={drafter['model']} "
        f"critic={critic['model']} iterations={iterations}"
    )

    # Initial draft.
    draft_result = _completion(
        model=drafter["model"],
        system_prompt=drafter.get("system_prompt"),
        user_prompt=src_text,
        api_key_env_var=drafter.get("api_key_env_var"),
        api_base_env_var=drafter.get("api_base_env_var"),
        temperature=drafter.get("temperature", 0.0),
        max_tokens=drafter.get("max_tokens", 2048),
        reasoning_effort=drafter.get("reasoning_effort"),
        thinking_budget=drafter.get("thinking_budget"),
        prompt_caching=bool(drafter.get("prompt_caching", False)),
    )
    current_draft = draft_result["content"]
    history = [{"iteration": 0, "phase": "initial_draft", "text": current_draft}]
    drafter_usage = [draft_result["usage"]]
    critic_usage = []
    all_llm_results.append(draft_result)

    for i in range(iterations):
        # Critic reviews.
        base_system = (
            critic.get("system_prompt")
            or "You are a careful reviewer. Give short, specific, actionable critique."
        )
        if until_score_gte is not None:
            critic_system = (
                base_system
                + f"\n\nEnd your response with a line exactly of the form `SCORE: N/100` where N is your quality rating "
                + f"of the current draft (0-100). Iteration stops early when N >= {until_score_gte:g}."
            )
        else:
            critic_system = base_system

        critic_prompt = (
            f"Original task:\n{src_text}\n\n"
            f"Current draft:\n{current_draft}\n\n"
            f"Provide specific, actionable critique. Focus on what to improve, not what's good. "
            f"If the draft is already excellent, say so explicitly."
        )
        critique_result = _completion(
            model=critic["model"],
            system_prompt=critic_system,
            user_prompt=critic_prompt,
            api_key_env_var=critic.get("api_key_env_var"),
            api_base_env_var=critic.get("api_base_env_var"),
            temperature=critic.get("temperature", 0.0),
            max_tokens=critic.get("max_tokens", 1024),
            reasoning_effort=critic.get("reasoning_effort"),
            thinking_budget=critic.get("thinking_budget"),
            prompt_caching=bool(critic.get("prompt_caching", False)),
        )
        critique = critique_result["content"]
        entry = {"iteration": i + 1, "phase": "critique", "text": critique}
        if until_score_gte is not None:
            score = _extract_critic_score(critique)
            if score is not None:
                entry["score"] = score
                final_score = score
            else:
                context.log.warning(
                    f"[critique_loop iter={i + 1}] until_score_gte={until_score_gte} set but critic omitted `SCORE: N/100`; continuing"
                )
        history.append(entry)
        critic_usage.append(critique_result["usage"])
        all_llm_results.append(critique_result)

        # Early termination: critic gave a high-enough score → don't revise;
        # the current draft is already the answer.
        if until_score_gte is not None and final_score is not None and final_score >= until_score_gte:
            stop_reason = f"score_gte_threshold (score={final_score:g} >= {until_score_gte:g})"
            context.log.info(
                f"[critique_loop iter={i + 1}] stopping early: critic score {final_score:g}/100 >= {until_score_gte:g}/100"
            )
            break

        # Drafter revises.
        revise_prompt = (
            f"Original task:\n{src_text}\n\n"
            f"Your previous draft:\n{current_draft}\n\n"
            f"Reviewer critique:\n{critique}\n\n"
            f"Revise the draft to address the critique. Output only the revised draft."
        )
        revise_result = _completion(
            model=drafter["model"],
            system_prompt=drafter.get("system_prompt"),
            user_prompt=revise_prompt,
            api_key_env_var=drafter.get("api_key_env_var"),
            api_base_env_var=drafter.get("api_base_env_var"),
            temperature=drafter.get("temperature", 0.0),
            max_tokens=drafter.get("max_tokens", 2048),
            reasoning_effort=drafter.get("reasoning_effort"),
            thinking_budget=drafter.get("thinking_budget"),
        )
        current_draft = revise_result["content"]
        history.append({"iteration": i + 1, "phase": "revised_draft", "text": current_draft})
        drafter_usage.append(revise_result["usage"])
        all_llm_results.append(revise_result)

    iterations_done = sum(1 for h in history if h.get("phase") == "critique")
    return {
        "text": current_draft,
        "iterations_done": iterations_done,
        "iterations_max": iterations,
        "stop_reason": stop_reason,
        "final_score": final_score,
        "until_score_gte": until_score_gte,
        "history": history,
        "drafter_model": drafter["model"],
        "critic_model": critic["model"],
        "model_fingerprint": f"{drafter['model']}//{critic['model']}×{iterations_done}"
            + (f"@score>={until_score_gte:g}" if until_score_gte is not None else ""),
        "materialized_at": materialized_at,
        "cost_usd": _sum_cost(*all_llm_results),
        "latency_ms": _sum_latency(*all_llm_results),
        "tokens_total": _sum_tokens(*all_llm_results),
        "n_llm_calls": len(all_llm_results),
        "usage": {"drafter": drafter_usage, "critic": critic_usage},
        "op": "critique_loop",
    }


def _do_synthesize(step: dict, state: Dict[str, Any], context) -> Dict[str, Any]:
    """Merge multiple upstream step outputs into one via an LLM.

    Two input modes (pick one):

    1. Legacy list: `sources: [<step_id>, ...]` → each becomes a labeled
       section, prompt_template's `{combined}` + `{n_sources}` substitute.

    2. Typed named inputs: `inputs: {port_name: {from: step_id} | {literal: value}}` →
       each port name becomes a `{port_name}` placeholder in prompt_template
       AND system_prompt. Named join — clearer than positional `sources:`
       when the synthesizer needs to reason about specific upstream roles.

    Shared: model, [system_prompt, api_key_env_var, temperature, max_tokens].
    """
    materialized_at = _now_iso()
    inputs = _resolve_inputs(step, state)
    source_ids = step.get("sources") or []
    if not inputs and not source_ids:
        raise ValueError(
            "synthesize needs either `inputs: {port: {from: id}, ...}` or "
            "`sources: [step_id, ...]` (>=1 upstream)."
        )

    temperature = step.get("temperature", 0.0)

    if inputs:
        prompt_template = step.get(
            "prompt_template",
            # Default: label each input with its port name
            "\n\n".join(f"## {k}\n{{{k}}}" for k in inputs.keys()),
        )
        user_prompt = _substitute_ports(prompt_template, inputs)
        system_prompt = step.get("system_prompt")
        if system_prompt:
            system_prompt = _substitute_ports(system_prompt, inputs)
    else:
        parts = []
        for sid in source_ids:
            parts.append(f"--- {sid} ---\n{_get_source_text(state, sid)}")
        combined = "\n\n".join(parts)

        prompt_template = step.get(
            "prompt_template",
            "You have {n_sources} labeled sections below. Synthesize them into a single coherent response.\n\n{combined}",
        )
        user_prompt = prompt_template.replace("{combined}", combined).replace("{n_sources}", str(len(source_ids)))
        system_prompt = step.get("system_prompt")

    result = _completion(
        model=step["model"],
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        api_key_env_var=step.get("api_key_env_var"),
        api_base_env_var=step.get("api_base_env_var"),
        temperature=temperature,
        max_tokens=step.get("max_tokens", 4096),
        reasoning_effort=step.get("reasoning_effort"),
        thinking_budget=step.get("thinking_budget"),
        prompt_caching=bool(step.get("prompt_caching", False)),
    )

    return {
        "text": result["content"],
        "sources_used": list(source_ids),
        "inputs_used": list(inputs.keys()) if inputs else None,
        "model": step["model"],
        "model_fingerprint": f"{step['model']}@t{temperature}",
        "materialized_at": materialized_at,
        "cost_usd": _sum_cost(result),
        "latency_ms": _sum_latency(result),
        "tokens_total": _sum_tokens(result),
        "n_llm_calls": 1,
        "usage": result["usage"],
        "op": "synthesize",
    }


# ── mcp_call op ──────────────────────────────────────────────────────

def _resolve_mcp_headers(cfg: Dict[str, Any], server_name: str) -> Dict[str, str]:
    """http/sse headers: `headers` = literal, `headers_env` = deferred env lookup."""
    import os
    headers: Dict[str, str] = {}
    for k, v in (cfg.get("headers") or {}).items():
        headers[k] = str(v)
    for header_name, env_var in (cfg.get("headers_env") or {}).items():
        val = _env_get(env_var)
        if val is None:
            raise ValueError(
                f"MCP server {server_name!r} references env var {env_var!r} for header "
                f"{header_name!r}, but it isn't set."
            )
        headers[header_name] = val
    return headers


def _validate_tool_args_against_schema(
    tool_args: Dict[str, Any], schema: Optional[Dict[str, Any]], tool_name: str
) -> None:
    """Validate rendered MCP tool_args against the tool's own real,
    server-declared `inputSchema` -- a required field on every MCP `Tool`
    (see mcp.types.Tool) -- before making the live call. Raises with the
    validation error pre-call instead of letting a mismatched call reach the
    server (a confusing live MCP error, or worse, silently-wrong data the
    server happens to tolerate)."""
    if not schema:
        return
    import jsonschema

    try:
        jsonschema.validate(instance=tool_args, schema=schema)
    except jsonschema.ValidationError as e:
        raise ValueError(
            f"delegate: tool_args for MCP tool {tool_name!r} failed its server's "
            f"declared inputSchema: {e.message} (path: {list(e.path)}). "
            f"Rendered args: {tool_args!r}"
        ) from e


async def _call_mcp_tool_async(
    *,
    log,
    server_cfg: Dict[str, Any],
    tool_name: str,
    tool_args: Dict[str, Any],
    parse_as: str,
    validate_input_schema: bool = False,
) -> Dict[str, Any]:
    """Async MCP tool call — supports stdio / http / sse transports.

    Mirrors the helper in `mcp_tool_picker/component.py` — kept local to
    keep this component's imports self-contained.

    validate_input_schema: when True, lists the server's tools, finds
    `tool_name`'s real inputSchema, and validates `tool_args` against it
    before calling. Only set True by `delegate` (a dynamically-picked agent
    might have a tool_args_template that doesn't actually match the real
    tool) -- `mcp_call` passes a hand-authored, already-tested tool_args, so
    it keeps the prior behavior (False) by default.
    """
    from contextlib import AsyncExitStack

    name = server_cfg.get("name") or "server"
    transport = server_cfg.get("type", "stdio")

    async with AsyncExitStack() as stack:
        if transport == "stdio":
            from mcp import ClientSession, StdioServerParameters
            from mcp.client.stdio import stdio_client

            cmd = server_cfg.get("command") or []
            if not cmd:
                raise ValueError(f"MCP server {name!r} is stdio but command is empty.")
            if isinstance(cmd, str):
                # Common LLM-composed mistake: `command: "npx -y @foo"` (string)
                # rather than the required list form. Fail fast with a clear
                # message rather than letting `cmd[0]` return the letter 'n'
                # and confusing the eventual FileNotFoundError.
                raise ValueError(
                    f"MCP server {name!r} `command:` must be a LIST of "
                    f"strings, got a bare string: {cmd!r}. Use YAML list "
                    f"syntax: `command: [npx, -y, \"@modelcontextprotocol/server-github\"]` "
                    f"or the block form: `command:\\n  - npx\\n  - -y\\n  - \"@...\"`."
                )
            # Forward the full parent-process environment to the stdio
            # subprocess. Without this, the mcp Python library applies a
            # tight security whitelist (HOME/LOGNAME/PATH/SHELL/TERM/USER
            # on Unix) and silently strips everything else — including
            # `GITHUB_PERSONAL_ACCESS_TOKEN`, `OPENAI_API_KEY`, and any
            # other var the MCP server needs. YAML `env:` values override
            # inherited ones (last-write-wins).
            _base_env = _mcp_subprocess_env(server_cfg.get("env"))
            params = StdioServerParameters(
                command=cmd[0], args=list(cmd[1:]), env=_base_env
            )
            log.info(f"[mcp:{name}] starting stdio server: {' '.join(cmd)}")
            read, write = await stack.enter_async_context(stdio_client(params))
            session = await stack.enter_async_context(ClientSession(read, write))
            await session.initialize()
        elif transport in ("http", "streamable_http", "streamable-http"):
            from mcp import ClientSession
            from mcp.client.streamable_http import streamablehttp_client

            url = server_cfg.get("url")
            if not url:
                raise ValueError(f"MCP server {name!r} is http but url is empty.")
            headers = _resolve_mcp_headers(server_cfg, name)
            _pol_url(url)
            read, write, _sid = await stack.enter_async_context(
                streamablehttp_client(url, headers=headers or None)
            )
            session = await stack.enter_async_context(ClientSession(read, write))
            await session.initialize()
        elif transport == "sse":
            from mcp import ClientSession
            from mcp.client.sse import sse_client

            url = server_cfg.get("url")
            if not url:
                raise ValueError(f"MCP server {name!r} is sse but url is empty.")
            headers = _resolve_mcp_headers(server_cfg, name)
            _pol_url(url)
            read, write = await stack.enter_async_context(
                sse_client(url, headers=headers or None)
            )
            session = await stack.enter_async_context(ClientSession(read, write))
            await session.initialize()
        elif transport == "fastmcp":
            # FastMCP v2 client — auto-transport detection (URL or
            # `mcp.json`-style config dict), first-class auth (bearer +
            # OAuth), better resilience than the raw MCP SDK. Preferred for
            # remote / production MCP servers; the raw http/sse transports
            # above remain available for compat.
            try:
                from fastmcp import Client as FastMCPClient
            except ImportError as e:
                raise ImportError(
                    "type: fastmcp requires the fastmcp package: "
                    "pip install 'fastmcp>=2.0'"
                ) from e

            url = server_cfg.get("url")
            config = server_cfg.get("config")  # optional inline mcp.json-style dict
            headers = _resolve_mcp_headers(server_cfg, name)

            # Optional bearer-token auth: `bearer_env: MCP_TOKEN` reads the
            # env var and adds `Authorization: Bearer <value>` at call time.
            bearer_env = server_cfg.get("bearer_env")
            if bearer_env and _env_get(bearer_env):
                headers = {
                    **(headers or {}),
                    "Authorization": f"Bearer {_require_env(bearer_env)}",
                }

            if config:
                client_ctx = FastMCPClient(config)
            elif url:
                if headers:
                    from fastmcp.client.transports import StreamableHttpTransport
                    transport_obj = StreamableHttpTransport(url=url, headers=headers)
                    client_ctx = FastMCPClient(transport_obj)
                else:
                    client_ctx = FastMCPClient(url)
            else:
                raise ValueError(
                    f"MCP server {name!r} is fastmcp but neither `url` nor "
                    f"`config` is set."
                )

            log.info(f"[mcp:{name}] fastmcp client connecting to {url or 'inline config'}")
            client = await stack.enter_async_context(client_ctx)

            # Shim so the shared `session.call_tool(...)` path below works —
            # FastMCP's `Client` exposes call_tool directly, no ClientSession.
            class _FastMCPShim:
                async def call_tool(self, tool_name, tool_args):
                    return await client.call_tool(tool_name, tool_args)
            session = _FastMCPShim()
        else:
            raise ValueError(f"MCP server {name!r} has unknown transport: {transport!r}")

        if validate_input_schema:
            # FastMCP's shim only implements call_tool (see _FastMCPShim
            # above) -- list_tools has to go through the real `client` for
            # that one transport; the other three use the real ClientSession
            # directly.
            list_target = client if transport == "fastmcp" else session
            tools_result = await list_target.list_tools()
            tool_list = tools_result.tools if hasattr(tools_result, "tools") else tools_result
            matching = next(
                (
                    t for t in tool_list
                    if (getattr(t, "name", None) or (t.get("name") if isinstance(t, dict) else None)) == tool_name
                ),
                None,
            )
            if matching is None:
                available = [getattr(t, "name", None) or (t.get("name") if isinstance(t, dict) else None) for t in tool_list]
                raise ValueError(
                    f"MCP server {name!r} has no tool named {tool_name!r} -- available: {available}"
                )
            schema = getattr(matching, "inputSchema", None) or (
                matching.get("inputSchema") if isinstance(matching, dict) else None
            )
            _validate_tool_args_against_schema(tool_args, schema, tool_name)

        call_result = await session.call_tool(tool_name, tool_args)
        parts = []
        for c in call_result.content:
            text = getattr(c, "text", None)
            parts.append(text if text is not None else str(c))
        raw = "\n".join(parts) if parts else ""
        is_error = bool(getattr(call_result, "isError", False))

    if parse_as == "text":
        return {"value": raw, "raw": raw, "is_error": is_error, "kind": "text"}
    if parse_as in ("json", "auto"):
        try:
            value = json.loads(raw)
            return {"value": value, "raw": raw, "is_error": is_error, "kind": "json"}
        except (json.JSONDecodeError, ValueError):
            if parse_as == "json":
                raise
            return {"value": raw, "raw": raw, "is_error": is_error, "kind": "text"}
    return {"value": raw, "raw": raw, "is_error": is_error, "kind": "text"}


def _do_mcp_call(step: dict, state: Dict[str, Any], context) -> Dict[str, Any]:
    """Direct MCP tool call — no LLM, deterministic step in the pipeline.

    server: {name, type: stdio|http|sse|fastmcp, command|url, env|headers|headers_env}
    mcp_tool_name: <tool name as the MCP server exposes it>
    tool_args: {arg_name: value}      (string values are `{text}`-templated against source)
    parse_as: 'auto' | 'json' | 'text'  (default 'auto')

    Use for the "fetch grounding data before the LLM specialists fan out"
    pattern — swap a URL/file `source:` for a first-class MCP step so the
    fetch is a Dagster asset with lineage + metadata.
    """
    import asyncio
    import os

    materialized_at = _now_iso()
    source_id = step.get("source", _ROOT_SOURCE_ID)
    src_text = _get_source_text(state, source_id) if source_id else ""

    inputs = _resolve_inputs(step, state)

    server_cfg = step.get("server") or {}
    if not server_cfg:
        raise ValueError("mcp_call requires `server: {name, type, ...}`")
    tool_name = step.get("mcp_tool_name")
    if not tool_name:
        raise ValueError("mcp_call requires `mcp_tool_name: <name-on-the-server>`")

    # {partition_key} + {partition.<name>} were already substituted deep in
    # `step` at dispatch time (see _run_step). {text} + {port_name} are per-op.
    raw_args = step.get("tool_args") or {}
    resolved_args: Dict[str, Any] = {}
    for k, v in raw_args.items():
        if isinstance(v, str):
            v = v.replace("{text}", src_text)
            v = _substitute_ports(v, inputs)
            resolved_args[k] = v
        else:
            resolved_args[k] = v

    parse_as = step.get("parse_as", "auto")

    context.log.info(
        f"[mcp_call:{step.get('id')}] tool={tool_name} args={resolved_args}"
    )

    _p = _pol()
    if _p is not None:
        _refusal = _p.guard_tool(f"{server_cfg.get('name', 'mcp')}__{tool_name}", tool_name, resolved_args)
        if _refusal:
            raise PolicyViolation(f"[policy:tools] mcp_call step {step.get('id')!r}: {_refusal}")

    t0 = time.time()
    try:
        result = asyncio.run(
            _call_mcp_tool_async(
                log=context.log,
                server_cfg=server_cfg,
                tool_name=tool_name,
                tool_args=resolved_args,
                parse_as=parse_as,
            )
        )
    except _BaseExceptionGroup as eg:
        # Recursively unwrap nested anyio ExceptionGroups (mcp lib nests
        # session-level and stdio_client-level task groups) so the real
        # underlying error — subprocess exit, MCP protocol error, tool-arg
        # validation — surfaces in the Dagster UI instead of a generic
        # "unhandled errors in a TaskGroup".
        def _first_leaf(exc):
            while isinstance(exc, _BaseExceptionGroup) and exc.exceptions:
                exc = exc.exceptions[0]
            return exc

        inner = _first_leaf(eg)
        context.log.error(
            f"[mcp_call:{step.get('id')}] failed: {type(inner).__name__}: {inner}"
        )
        raise inner from eg
    latency_ms = int((time.time() - t0) * 1000)

    # Stringify JSON value so downstream steps' `source: <this step id>`
    # can consume it as text.
    if result["kind"] == "json":
        text = json.dumps(result["value"], indent=2, default=str)
    else:
        text = result["raw"]

    if result["is_error"]:
        context.log.warning(
            f"[mcp_call:{step.get('id')}] server returned isError=True; treating as failure"
        )

    return {
        "text": text,
        "raw": result["raw"],
        "value": result["value"],
        "kind": result["kind"],
        "is_error": result["is_error"],
        "tool_name": tool_name,
        "server_name": server_cfg.get("name") or "server",
        "transport": server_cfg.get("type", "stdio"),
        "tool_args_resolved": resolved_args,
        "materialized_at": materialized_at,
        "latency_ms": latency_ms,
        "op": "mcp_call",
    }


# ── tool_use_loop op ────────────────────────────────────────────────
#
# Open-ended tool-use loop — LLM picks a tool, tool runs, LLM sees the
# result, picks the next tool, ..., emits a final answer. Bounded by
# `max_iterations`. Tools come from MCP servers declared on the step.
#
# This is the shape LangGraph is known for, done as a first-class Dagster
# asset: one asset materializes, containing the final answer + the full
# tool-call trajectory in metadata. Cost/latency/tokens are rolled up
# across every LLM call + tool call in the loop.


async def _discover_mcp_tools_async(
    log, mcp_servers: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """For each configured MCP server, connect + list_tools() + capture
    the JSON schema for each. Returns a flat list of
    `{server, name, description, input_schema}` — the `server` field lets
    us route tool calls back to the right MCP client during the loop.
    """
    from contextlib import AsyncExitStack
    tools: List[Dict[str, Any]] = []
    for server_cfg in mcp_servers:
        name = server_cfg.get("name") or "server"
        transport = server_cfg.get("type", "stdio")
        log.info(f"[tool_use_loop] discovering tools on {name!r} ({transport})")

        async with AsyncExitStack() as stack:
            # Reuse the same connection paths as _call_mcp_tool_async —
            # copy-paste rather than refactor since this helper is only
            # for the initial discovery step (one call per server, at
            # loop start).
            if transport == "stdio":
                from mcp import ClientSession, StdioServerParameters
                from mcp.client.stdio import stdio_client
                cmd = server_cfg.get("command") or []
                if not cmd or isinstance(cmd, str):
                    raise ValueError(
                        f"MCP server {name!r} stdio needs `command: [...]` (LIST of strings)."
                    )
                _base_env = _mcp_subprocess_env(server_cfg.get("env"))
                params = StdioServerParameters(
                    command=cmd[0], args=list(cmd[1:]), env=_base_env
                )
                read, write = await stack.enter_async_context(stdio_client(params))
                session = await stack.enter_async_context(ClientSession(read, write))
                await session.initialize()
            elif transport in ("http", "streamable_http", "streamable-http"):
                from mcp import ClientSession
                from mcp.client.streamable_http import streamablehttp_client
                url = server_cfg.get("url")
                if not url:
                    raise ValueError(f"MCP server {name!r} http needs url.")
                headers = _resolve_mcp_headers(server_cfg, name)
                _pol_url(url)
                read, write, _sid = await stack.enter_async_context(
                    streamablehttp_client(url, headers=headers or None)
                )
                session = await stack.enter_async_context(ClientSession(read, write))
                await session.initialize()
            elif transport == "sse":
                from mcp import ClientSession
                from mcp.client.sse import sse_client
                url = server_cfg.get("url")
                if not url:
                    raise ValueError(f"MCP server {name!r} sse needs url.")
                headers = _resolve_mcp_headers(server_cfg, name)
                _pol_url(url)
                read, write = await stack.enter_async_context(
                    sse_client(url, headers=headers or None)
                )
                session = await stack.enter_async_context(ClientSession(read, write))
                await session.initialize()
            elif transport == "fastmcp":
                try:
                    from fastmcp import Client as FastMCPClient
                except ImportError as e:
                    raise ImportError(
                        "type: fastmcp requires the fastmcp package: pip install 'fastmcp>=2.0'"
                    ) from e
                url = server_cfg.get("url")
                config = server_cfg.get("config")
                headers = _resolve_mcp_headers(server_cfg, name)
                bearer_env = server_cfg.get("bearer_env")
                if bearer_env and _env_get(bearer_env):
                    headers = {**(headers or {}), "Authorization": f"Bearer {_require_env(bearer_env)}"}
                if config:
                    client_ctx = FastMCPClient(config)
                elif url:
                    if headers:
                        from fastmcp.client.transports import StreamableHttpTransport
                        client_ctx = FastMCPClient(StreamableHttpTransport(url=url, headers=headers))
                    else:
                        client_ctx = FastMCPClient(url)
                else:
                    raise ValueError(f"MCP server {name!r} fastmcp needs url or config.")
                client = await stack.enter_async_context(client_ctx)

                class _FastMCPShim:
                    async def list_tools(_self):
                        return await client.list_tools()
                session = _FastMCPShim()
            else:
                raise ValueError(f"MCP server {name!r} unknown transport: {transport!r}")

            tools_result = await session.list_tools()
            # `.tools` on ListToolsResult is a list of Tool objects with
            # `.name`, `.description`, `.inputSchema`. FastMCP variants
            # can return a bare list.
            listed = getattr(tools_result, "tools", None) or tools_result
            for t in listed:
                tools.append({
                    "server": name,
                    "name": getattr(t, "name", None) or t["name"],
                    "description": getattr(t, "description", None) or t.get("description", ""),
                    "input_schema": (
                        getattr(t, "inputSchema", None)
                        or t.get("inputSchema")
                        or {"type": "object", "properties": {}}
                    ),
                })
    return tools


def _mcp_tools_to_openai_schema(
    mcp_tools: List[Dict[str, Any]], allowed: Optional[List[str]]
) -> List[Dict[str, Any]]:
    """MCP tool defs → OpenAI-style tool_call schema. Namespaces the tool
    name with `{server}__{name}` so the loop can route the call back
    without ambiguity when two servers expose tools of the same name."""
    schemas = []
    for t in mcp_tools:
        if allowed and t["name"] not in allowed:
            continue
        namespaced = f"{t['server']}__{t['name']}"
        schemas.append({
            "type": "function",
            "function": {
                "name": namespaced,
                "description": t["description"] or "",
                "parameters": t["input_schema"] or {"type": "object", "properties": {}},
            },
        })
    return schemas


def _do_tool_use_loop(step: dict, state: Dict[str, Any], context) -> Dict[str, Any]:
    """Open-ended tool-use loop. LLM picks tools, executes them via MCP,
    sees results, picks next tool — bounded by `max_iterations` OR the
    LLM emitting a final answer with no tool call OR the LLM calling
    the synthetic `finalize` tool.

    Emits ONE Dagster asset with the final answer text in `text` +
    the full tool-call trajectory in `tool_call_trace` metadata. Cost /
    latency / tokens are aggregated across all LLM calls + tool calls
    in the loop.

    Config:
      model, api_key_env_var: LLM the agent runs on
      mcp_servers: list of MCP server configs (same shape as mcp_call.server)
      max_iterations: hard cap on loop iterations (default 10)
      system_prompt: agent instructions
      allowed_tools: optional allowlist (defaults to all discovered tools)
      finalize_tool_name: synthetic tool name the LLM calls when done
                         (default "finalize")
    """
    import asyncio
    import time as _time

    try:
        import litellm
    except ImportError:
        raise ImportError("agentic_pipeline tool_use_loop requires litellm.")
    litellm.drop_params = True

    materialized_at = _now_iso()
    source_id = step.get("source", _ROOT_SOURCE_ID)
    src_text = _get_source_text(state, source_id) if source_id else ""
    inputs = _resolve_inputs(step, state)

    model = step["model"]
    api_key_env_var = step.get("api_key_env_var")
    api_base_env_var = step.get("api_base_env_var")
    max_iterations = int(step.get("max_iterations", 10))
    temperature = step.get("temperature", 0.0)
    max_tokens_per_call = step.get("max_tokens", 2048)
    reasoning_effort = step.get("reasoning_effort")
    thinking_budget = step.get("thinking_budget")
    prompt_caching = bool(step.get("prompt_caching", False))
    finalize_name = step.get("finalize_tool_name", "finalize")

    system_prompt = step.get("system_prompt") or (
        "You are a tool-using agent. You have access to MCP tools. "
        "Call tools to gather information; when you have enough to answer, "
        f"call the special `{finalize_name}` tool with your final answer. "
        "Do not answer without calling `finalize` — the caller only sees "
        "what you pass to `finalize`."
    )
    system_prompt = _substitute_ports(system_prompt, inputs)

    user_prompt = step.get("prompt_template", "{text}").replace("{text}", src_text)
    user_prompt = _substitute_ports(user_prompt, inputs)

    mcp_servers = step.get("mcp_servers") or []
    if not mcp_servers:
        raise ValueError("tool_use_loop requires `mcp_servers: [...]`.")

    context.log.info(
        f"[tool_use_loop:{step.get('id', '?')}] discovering tools across "
        f"{len(mcp_servers)} MCP server(s)…"
    )
    mcp_tools = asyncio.run(_discover_mcp_tools_async(context.log, mcp_servers))
    allowed = step.get("allowed_tools")
    tool_schemas = _mcp_tools_to_openai_schema(mcp_tools, allowed)
    # Synthetic finalize tool — LLM calls this when done.
    tool_schemas.append({
        "type": "function",
        "function": {
            "name": finalize_name,
            "description": "Return your final answer to the caller and end the loop. Call this when you have enough information.",
            "parameters": {
                "type": "object",
                "properties": {
                    "answer": {"type": "string", "description": "The final answer text."}
                },
                "required": ["answer"],
            },
        },
    })
    # Index tools by their namespaced name for O(1) lookup during the loop.
    tool_index = {f"{t['server']}__{t['name']}": t for t in mcp_tools}
    if allowed:
        tool_index = {k: v for k, v in tool_index.items() if v["name"] in allowed}

    _p = _pol()
    if _p is not None:
        # The model can only act through the tools it is shown: hide denied / non-allowlisted ones entirely.
        tool_index = {k: v for k, v in tool_index.items() if _p.tool_visible(k, v["name"])}
        tool_schemas = [
            t for t in tool_schemas
            if t["function"]["name"] == finalize_name or t["function"]["name"] in tool_index
        ]

    context.log.info(
        f"[tool_use_loop:{step.get('id', '?')}] {len(tool_index)} tool(s) "
        f"available: {sorted(tool_index.keys())[:10]}"
        + (" …" if len(tool_index) > 10 else "")
    )

    # Provider detection (used for reasoning-param routing + cache_control below).
    _ml = model.lower()
    _is_openai_ish = _ml.startswith(("gpt-", "o1", "o3", "o4", "openai/", "azure/", "groq/"))
    _is_gemini = _ml.startswith(("gemini/", "google/", "vertex_ai/gemini"))
    _is_anthropic = (
        "claude" in _ml or _ml.startswith(("anthropic/", "bedrock/anthropic."))
    )

    # Message history — accumulates across iterations. When prompt_caching
    # is on AND provider is Anthropic, wrap the system prompt with
    # cache_control so the (typically-long) system prompt + tool schemas
    # are cached across every iteration of this loop. Cache lives ~5 min;
    # subsequent iterations hit it directly.
    if prompt_caching and _is_anthropic:
        system_msg: Dict[str, Any] = {
            "role": "system",
            "content": [
                {"type": "text", "text": system_prompt,
                 "cache_control": {"type": "ephemeral"}}
            ],
        }
    else:
        system_msg = {"role": "system", "content": system_prompt}
    messages: List[Dict[str, Any]] = [
        system_msg,
        {"role": "user", "content": user_prompt},
    ]

    trajectory: List[Dict[str, Any]] = []
    total_cost_usd = 0.0
    total_llm_latency_ms = 0
    total_tool_latency_ms = 0
    total_tokens_in = 0
    total_tokens_out = 0
    n_llm_calls = 0
    n_tool_calls = 0
    final_answer: Optional[str] = None
    stop_reason = "max_iterations"

    for iteration in range(1, max_iterations + 1):
        kwargs: Dict[str, Any] = {
            "model": _ollama_chat_model(model, True),
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens_per_call,
            "tools": tool_schemas,
            "tool_choice": "auto",
        }
        # Provider-aware reasoning-param routing (see _completion()).
        if reasoning_effort is not None and (_is_openai_ish or _is_gemini):
            kwargs["reasoning_effort"] = reasoning_effort
        if thinking_budget is not None:
            if _is_gemini:
                kwargs["thinking_budget"] = int(thinking_budget)
            elif _is_anthropic:
                kwargs["thinking"] = {"type": "enabled", "budget_tokens": int(thinking_budget)}
        if api_key_env_var and _env_get(api_key_env_var):
            kwargs["api_key"] = _require_env(api_key_env_var)
        if api_base_env_var and _env_get(api_base_env_var):
            kwargs["api_base"] = _require_env(api_base_env_var)
            _pol_url(kwargs["api_base"])

        _p = _pol()
        if _p is not None:
            _p.before_llm_call()
        t0 = _time.time()
        response = litellm.completion(**kwargs)
        llm_ms = int((_time.time() - t0) * 1000)
        n_llm_calls += 1
        total_llm_latency_ms += llm_ms
        iter_cost = None
        try:
            iter_cost = float(litellm.completion_cost(completion_response=response))
            total_cost_usd += iter_cost
        except Exception:  # noqa: BLE001
            pass
        usage = getattr(response, "usage", None) or {}
        total_tokens_in += (
            getattr(usage, "prompt_tokens", 0)
            or (usage.get("prompt_tokens", 0) if isinstance(usage, dict) else 0)
        )
        total_tokens_out += (
            getattr(usage, "completion_tokens", 0)
            or (usage.get("completion_tokens", 0) if isinstance(usage, dict) else 0)
        )

        msg = response.choices[0].message
        tool_calls = getattr(msg, "tool_calls", None) or []
        _iter_tokens = (
            getattr(usage, "total_tokens", None)
            or (usage.get("total_tokens") if isinstance(usage, dict) else None)
        )
        if _p is not None:
            _p.after_llm_call(iter_cost, _iter_tokens)
        _content_clean, _think = _split_reasoning(msg.content or "")
        _note_reasoning(model, _think)
        if not tool_calls:
            _content_clean, _text_calls = _parse_text_tool_calls(_content_clean)
            if _text_calls:  # Qwen/Hermes-style <tool_call> text the server did not parse
                tool_calls = [
                    SimpleNamespace(id=f"textcall_{iteration}_{i}", function=SimpleNamespace(name=c["name"], arguments=c["arguments"]))
                    for i, c in enumerate(_text_calls)
                ]

        if not tool_calls:
            # LLM emitted content without a tool call — treat as final answer.
            content = _content_clean
            trajectory.append({
                "iteration": iteration,
                "phase": "final_answer_no_tool",
                "text": content,
                "llm_latency_ms": llm_ms,
            })
            final_answer = content
            stop_reason = "final_answer_no_tool"
            context.log.info(
                f"[tool_use_loop iter={iteration}] LLM emitted final answer without tool call ({len(content)} chars); stopping."
            )
            break

        # Append the assistant's tool-call message to history so the LLM
        # sees what it just did on the next iteration.
        messages.append({
            "role": "assistant",
            "content": _content_clean,
            "tool_calls": [
                {
                    "id": tc.id,
                    "type": "function",
                    "function": {"name": tc.function.name, "arguments": tc.function.arguments},
                }
                for tc in tool_calls
            ],
        })

        # Handle each tool call in this iteration.
        # (LLMs typically emit ONE tool call per iteration in agent loops;
        # we handle N to be safe.)
        done_this_iter = False
        for tc in tool_calls:
            tool_name = tc.function.name
            try:
                tool_args = json.loads(tc.function.arguments or "{}")
            except json.JSONDecodeError:
                tool_args = {"_raw_args": tc.function.arguments}

            # finalize → capture answer + stop
            if tool_name == finalize_name:
                answer = tool_args.get("answer", "")
                trajectory.append({
                    "iteration": iteration,
                    "phase": "finalize",
                    "tool_name": finalize_name,
                    "tool_args": tool_args,
                    "text": answer,
                    "llm_latency_ms": llm_ms,
                })
                final_answer = answer
                stop_reason = "finalize_called"
                done_this_iter = True
                context.log.info(
                    f"[tool_use_loop iter={iteration}] finalize called ({len(answer)} chars); stopping."
                )
                break

            # Real MCP tool → route to correct server + call.
            tool_meta = tool_index.get(tool_name)
            if tool_meta is None:
                error_msg = f"unknown tool: {tool_name!r}"
                trajectory.append({
                    "iteration": iteration,
                    "phase": "tool_call_error",
                    "tool_name": tool_name,
                    "tool_args": tool_args,
                    "error": error_msg,
                })
                messages.append({
                    "role": "tool", "tool_call_id": tc.id, "content": f"ERROR: {error_msg}",
                })
                n_tool_calls += 1
                continue

            # Find the server_cfg for this tool + invoke via existing helper.
            server_cfg = next(
                (s for s in mcp_servers if s.get("name") == tool_meta["server"]),
                None,
            )
            if server_cfg is None:
                error_msg = f"internal: server config missing for tool {tool_name!r}"
                messages.append({"role": "tool", "tool_call_id": tc.id, "content": f"ERROR: {error_msg}"})
                trajectory.append({
                    "iteration": iteration, "phase": "tool_call_error",
                    "tool_name": tool_name, "tool_args": tool_args, "error": error_msg,
                })
                continue

            if _p is not None:
                _refusal = _p.guard_tool(tool_name, tool_meta["name"], tool_args)
                if _refusal:
                    messages.append({"role": "tool", "tool_call_id": tc.id, "content": f"ERROR: blocked by policy: {_refusal}"})
                    trajectory.append({
                        "iteration": iteration, "phase": "tool_call_blocked",
                        "tool_name": tool_name, "tool_args": tool_args, "error": _refusal,
                    })
                    context.log.warning(f"[tool_use_loop iter={iteration}] {tool_name} blocked by policy: {_refusal}")
                    n_tool_calls += 1
                    continue

            t_tool = _time.time()
            try:
                tool_result = asyncio.run(_call_mcp_tool_async(
                    log=context.log,
                    server_cfg=server_cfg,
                    tool_name=tool_meta["name"],  # un-namespaced real name
                    tool_args=tool_args,
                    parse_as="auto",
                ))
                tool_ms = int((_time.time() - t_tool) * 1000)
                total_tool_latency_ms += tool_ms
                n_tool_calls += 1

                # Feed the result back to the LLM as a tool message.
                result_text = tool_result.get("raw", "")
                messages.append({
                    "role": "tool", "tool_call_id": tc.id, "content": result_text[:20000],
                })
                trajectory.append({
                    "iteration": iteration,
                    "phase": "tool_call",
                    "tool_name": tool_name,
                    "tool_args": tool_args,
                    "tool_result_preview": result_text[:600],
                    "tool_result_kind": tool_result.get("kind"),
                    "tool_latency_ms": tool_ms,
                    "llm_latency_ms": llm_ms,
                })
                context.log.info(
                    f"[tool_use_loop iter={iteration}] {tool_name}({str(tool_args)[:80]}) → "
                    f"{tool_ms}ms {len(result_text)} chars"
                )
            except _BaseExceptionGroup as eg:
                def _first_leaf(exc):
                    while isinstance(exc, _BaseExceptionGroup) and exc.exceptions:
                        exc = exc.exceptions[0]
                    return exc
                inner = _first_leaf(eg)
                error_msg = f"{type(inner).__name__}: {inner}"
                messages.append({"role": "tool", "tool_call_id": tc.id, "content": f"ERROR: {error_msg}"})
                trajectory.append({
                    "iteration": iteration, "phase": "tool_call_error",
                    "tool_name": tool_name, "tool_args": tool_args, "error": error_msg,
                })
                context.log.warning(f"[tool_use_loop iter={iteration}] {tool_name} failed: {error_msg}")
            except Exception as e:  # noqa: BLE001
                error_msg = f"{type(e).__name__}: {e}"
                messages.append({"role": "tool", "tool_call_id": tc.id, "content": f"ERROR: {error_msg}"})
                trajectory.append({
                    "iteration": iteration, "phase": "tool_call_error",
                    "tool_name": tool_name, "tool_args": tool_args, "error": error_msg,
                })
                context.log.warning(f"[tool_use_loop iter={iteration}] {tool_name} failed: {error_msg}")

        if done_this_iter:
            break

    if final_answer is None:
        # Hit max_iterations without finalize. Use the last LLM content
        # or a fallback message.
        final_answer = (
            f"(loop terminated after {max_iterations} iterations without "
            f"calling `{finalize_name}`; showing raw trajectory)"
        )
        stop_reason = "max_iterations_hit"

    total_latency_ms = total_llm_latency_ms + total_tool_latency_ms

    return {
        "text": final_answer,
        "op": "tool_use_loop",
        "model": model,
        "model_fingerprint": f"{model}@t{temperature}",
        "n_iterations": len(trajectory) if stop_reason != "max_iterations_hit" else max_iterations,
        "n_llm_calls": n_llm_calls,
        "n_tool_calls": n_tool_calls,
        "stop_reason": stop_reason,
        "cost_usd": round(total_cost_usd, 6) if total_cost_usd > 0 else None,
        "latency_ms": total_latency_ms,
        "llm_latency_ms": total_llm_latency_ms,
        "tool_latency_ms": total_tool_latency_ms,
        "tokens_in": total_tokens_in or None,
        "tokens_out": total_tokens_out or None,
        "tokens_total": (total_tokens_in + total_tokens_out) or None,
        "tool_call_trace": trajectory,
        "tools_available": sorted(tool_index.keys()),
        "materialized_at": materialized_at,
    }


# ── Op dispatcher ────────────────────────────────────────────────────

def _do_handoff(step: dict, state: Dict[str, Any], context) -> Dict[str, Any]:
    """Framework-composition op. Hands off to a user-provided callable —
    bring your own LangGraph / AutoGen / CrewAI / Llama Index Agents /
    DSPy. The callable receives `initial_state` (post-substitution) as
    kwargs and returns a dict; we pull the final text from
    `output_text_key`. Framework's internal node-by-node lineage is
    lost (one asset materializes, containing the final answer +
    framework metadata) — that's the intentional trade at ONE step of a
    Dagster pipeline. Adjacent steps stay Dagster-native (fan-out, MCP,
    HITL gates), so this is composition, not wrapping.

    Config:
      entry_module     — Python module path with the callable
      entry_callable   — name of the callable in that module. Signature:
                         `def fn(**initial_state) -> dict`
      initial_state    — dict of kwargs passed to the callable. String
                         values get `{text}` (source) + `{port_name}`
                         (typed input) substitution before the call.
      output_text_key  — key in the returned dict whose value is the
                         final text downstream steps consume (default
                         `final_answer`)
      framework        — metadata-only label ("langgraph" / "autogen" /
                         "crewai" / "dspy" / etc.); surfaces in asset
                         metadata + logs; not enforced.

    Example (`initial_state` with source-text substitution):

        - id: complex_reasoning
          op: handoff
          framework: langgraph
          entry_module: my_project.agents
          entry_callable: run_deep_reasoning
          initial_state:
            prompt: "{text}"
            max_depth: 5
          output_text_key: final_answer

    User's callable — 5 lines of glue in their own project:

        def run_deep_reasoning(prompt: str, max_depth: int = 5) -> dict:
            graph = build_graph()  # user's LangGraph
            result = graph.invoke({"prompt": prompt, "max_depth": max_depth})
            return {"final_answer": result["output"], "n_nodes": result.get("n_nodes")}
    """
    import importlib
    import time as _time

    materialized_at = _now_iso()
    source_id = step.get("source", _ROOT_SOURCE_ID)
    src_text = _get_source_text(state, source_id) if source_id else ""
    inputs = _resolve_inputs(step, state)

    entry_module = step.get("entry_module")
    entry_callable = step.get("entry_callable")
    if not entry_module or not entry_callable:
        raise ValueError(
            "handoff requires `entry_module` + `entry_callable` — the Python "
            "module path + function name of a callable that takes "
            "**initial_state and returns a dict."
        )
    initial_state = dict(step.get("initial_state") or {})
    output_text_key = step.get("output_text_key", "final_answer")
    framework = step.get("framework")  # metadata only

    # {text} + {port_name} substitution on string values of initial_state.
    # {partition_key} + {partition.<name>} are already substituted by
    # _run_step's _deep_apply_ctx_substitutions.
    for k, v in list(initial_state.items()):
        if isinstance(v, str):
            v = v.replace("{text}", src_text)
            v = _substitute_ports(v, inputs)
            initial_state[k] = v

    try:
        mod = importlib.import_module(entry_module)
    except ImportError as e:
        raise ImportError(
            f"handoff: could not import {entry_module!r}: {e}. "
            f"Install the module (or its parent package) in the project's "
            f"venv, or fix the module path."
        ) from e
    fn = getattr(mod, entry_callable, None)
    if fn is None or not callable(fn):
        raise ValueError(
            f"handoff: {entry_callable!r} is not a callable in {entry_module!r}. "
            f"Available names: {[n for n in dir(mod) if callable(getattr(mod, n, None)) and not n.startswith('_')][:20]}"
        )

    context.log.info(
        f"[handoff:{step.get('id', '?')}] {framework or 'user'} → "
        f"{entry_module}.{entry_callable}({sorted(initial_state.keys())})"
    )
    t0 = _time.time()
    try:
        result = fn(**initial_state)
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(
            f"handoff to {entry_module}.{entry_callable} raised "
            f"{type(e).__name__}: {e}"
        ) from e
    latency_ms = int((_time.time() - t0) * 1000)

    # Extract downstream text via output_text_key.
    text = ""
    if isinstance(result, dict):
        raw_text = result.get(output_text_key, "")
        text = raw_text if isinstance(raw_text, str) else json.dumps(raw_text, default=str)
    elif isinstance(result, str):
        text = result
    else:
        text = str(result)

    context.log.info(
        f"[handoff:{step.get('id', '?')}] completed in {latency_ms}ms, "
        f"{len(text)} chars"
    )

    # Pass the whole framework result through as `framework_result` so
    # downstream steps or the emitting asset can inspect internal keys
    # (n_nodes_executed, tool_call_trace, cost_usd if the framework
    # tracks it, etc.).
    framework_result = result if isinstance(result, dict) else {"_result": result}
    # Best-effort roll-up: if the callable's return dict has these keys,
    # surface them at the top level so they land in Insights.
    rolled: Dict[str, Any] = {}
    for k in ("cost_usd", "n_nodes_executed", "n_llm_calls", "n_tool_calls", "tokens_total"):
        if isinstance(result, dict) and k in result:
            rolled[k] = result[k]

    return {
        "text": text,
        "framework": framework,
        "entry_module": entry_module,
        "entry_callable": entry_callable,
        "framework_result": framework_result,
        "latency_ms": latency_ms,
        "materialized_at": materialized_at,
        "op": "handoff",
        **rolled,
    }


# ── map op ────────────────────────────────────────────────────────────
#
# Fan-out an LLM call over each item in a list source (JSON array).
# Aggregates per-item results into one asset. Sequential by default;
# `max_concurrent` opt-in threading for I/O-bound workloads.


def _parse_items(text: str) -> List[Any]:
    """Best-effort parse of a source text into a list of items.
    Accepts a JSON array OR falls back to non-empty newlines."""
    try:
        parsed = json.loads(text)
        if isinstance(parsed, list):
            return parsed
        if isinstance(parsed, dict):
            # Common shape: a dict with a `data:` or `items:` list.
            for key in ("data", "items", "results", "values"):
                v = parsed.get(key)
                if isinstance(v, list):
                    return v
    except (json.JSONDecodeError, ValueError):
        pass
    # Fallback: split on newlines.
    return [ln for ln in text.splitlines() if ln.strip()]


def _do_map(step: dict, state: Dict[str, Any], context) -> Dict[str, Any]:
    """Apply an LLM call to each item in a list source; aggregate.

    Config:
      source / inputs   — usual upstream ref; source's text is parsed as JSON list
      model, api_key_env_var, system_prompt, temperature, max_tokens
      prompt_template   — supports `{item}` (current item) + `{index}` (0-based)
                          + `{n}` (total count) + `{text}` + `{port_name}`
      max_concurrent    — 1 (sequential; default) OR N (thread-pooled)
      output_schema     — optional JSON Schema (object). When set, each
                          per-item call is FORCED tool-calling (same
                          tool_choice="required" mechanism `extract` uses),
                          not a free-text completion — reliable structured
                          per-item output instead of hoping the model's
                          free text happens to be parseable. Each item's
                          `extracted` dict is validated the same way
                          `extract`'s `strict` does (missing required
                          fields raise).
      output_join       — 'newlines' (default) | 'jsonl' | 'none' | 'records'
                          ('none' returns empty text — downstream reads
                          items[]; 'records' REQUIRES output_schema — joins
                          `{**item, **extracted}` per item into one JSON
                          array, directly consumable downstream by
                          `invoke_component`/`map`/`reduce`'s own
                          _parse_items, for an agent-categorize →
                          deterministic-filter → agent-triage pipeline)
    """
    materialized_at = _now_iso()
    source_id = step.get("source", _ROOT_SOURCE_ID)
    src_text = _get_source_text(state, source_id) if source_id else ""
    inputs = _resolve_inputs(step, state)

    items = _parse_items(src_text)
    if not items:
        context.log.warning(f"[map:{step.get('id', '?')}] no items parsed from source")

    model = step["model"]
    api_key_env_var = step.get("api_key_env_var")
    api_base_env_var = step.get("api_base_env_var")
    temperature = step.get("temperature", 0.0)
    max_tokens = step.get("max_tokens", 1024)
    reasoning_effort = step.get("reasoning_effort")
    thinking_budget = step.get("thinking_budget")
    prompt_caching = bool(step.get("prompt_caching", False))
    prompt_template = step.get("prompt_template", "{item}")
    system_prompt = step.get("system_prompt")
    if system_prompt:
        system_prompt = _substitute_ports(system_prompt, inputs)
    max_concurrent = int(step.get("max_concurrent", 1))
    output_join = step.get("output_join", "newlines")
    output_schema = step.get("output_schema")
    strict = bool(step.get("strict", True))

    if output_join == "records" and not output_schema:
        raise ValueError("map: output_join='records' requires `output_schema` to be set.")

    tool = None
    if output_schema:
        if not isinstance(output_schema, dict):
            raise ValueError("map: output_schema must be a JSON Schema object (dict).")
        tool = {
            "type": "function",
            "function": {
                "name": "extract_data",
                "description": "Return extracted data matching the schema.",
                "parameters": output_schema,
            },
        }

    def _run_one(index_and_item):
        i, item = index_and_item
        item_str = json.dumps(item) if not isinstance(item, str) else item
        user_prompt = (
            prompt_template
            .replace("{item}", item_str)
            .replace("{index}", str(i))
            .replace("{n}", str(len(items)))
            .replace("{text}", src_text)
        )
        user_prompt = _substitute_ports(user_prompt, inputs)
        return i, item, _completion(
            model=model, system_prompt=system_prompt, user_prompt=user_prompt,
            api_key_env_var=api_key_env_var, api_base_env_var=api_base_env_var,
            temperature=temperature, max_tokens=max_tokens,
            reasoning_effort=reasoning_effort, thinking_budget=thinking_budget,
            prompt_caching=prompt_caching,
            tools=[tool] if tool else None,
            tool_choice="required" if tool else None,
        )

    def _to_result_entry(item, result):
        if not tool:
            return {"item": item, "text": result["content"]}
        if not result["tool_calls"]:
            raise RuntimeError(
                f"map: LLM did not emit a tool call for item {item!r}. This "
                f"usually means the model doesn't support forced tool calls "
                f"— try a different model."
            )
        raw_args = result["tool_calls"][0].get("arguments") or "{}"
        try:
            extracted = json.loads(raw_args)
        except json.JSONDecodeError as e:
            raise RuntimeError(
                f"map: LLM returned invalid JSON for item {item!r}: {e}. "
                f"Raw: {raw_args[:300]}"
            ) from e
        if strict:
            required = output_schema.get("required") or []
            missing = [k for k in required if k not in extracted]
            if missing:
                raise RuntimeError(
                    f"map (strict): missing required fields {missing} in "
                    f"extracted data for item {item!r}."
                )
        return {"item": item, "text": json.dumps(extracted), "extracted": extracted}

    results: List[Any] = [None] * len(items)  # per-item output blobs
    all_llm_results = []

    context.log.info(
        f"[map:{step.get('id', '?')}] {len(items)} item(s) × max_concurrent={max_concurrent}"
        + (" (structured)" if tool else "")
    )

    if max_concurrent > 1 and len(items) > 1:
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=max_concurrent) as pool:
            for i, item, result in pool.map(_run_one, enumerate(items)):
                results[i] = _to_result_entry(item, result)
                all_llm_results.append(result)
    else:
        for pair in enumerate(items):
            i, item, result = _run_one(pair)
            results[i] = _to_result_entry(item, result)
            all_llm_results.append(result)

    # Join per-item texts into the top-level `text` field.
    joined_text: str
    if output_join == "newlines":
        joined_text = "\n\n".join(r["text"] for r in results if r)
    elif output_join == "jsonl":
        joined_text = "\n".join(json.dumps(r) for r in results if r)
    elif output_join == "none":
        joined_text = ""
    elif output_join == "records":
        merged = []
        for r in results:
            if not r:
                continue
            base = r["item"] if isinstance(r["item"], dict) else {"item": r["item"]}
            merged.append({**base, **r["extracted"]})
        joined_text = json.dumps(merged, default=str)
    else:
        raise ValueError(
            f"map: output_join must be 'newlines' | 'jsonl' | 'none' | 'records'; got {output_join!r}"
        )

    return {
        "text": joined_text,
        "items": results,
        "n_items": len(items),
        "model": model,
        "model_fingerprint": f"{model}@t{temperature}×{len(items)}",
        "materialized_at": materialized_at,
        "cost_usd": _sum_cost(*all_llm_results),
        "latency_ms": _sum_latency(*all_llm_results),
        "tokens_total": _sum_tokens(*all_llm_results),
        "n_llm_calls": len(all_llm_results),
        "op": "map",
    }


# ── extract op ────────────────────────────────────────────────────────
#
# Structured JSON extraction — text → dict matching a schema. Uses
# tool_choice="required" with a single function that has the schema as
# parameters. More reliable than prompt-engineering "return JSON" —
# LiteLLM+model round-trips the JSON as a tool call.


def _do_extract(step: dict, state: Dict[str, Any], context) -> Dict[str, Any]:
    """Extract structured JSON from unstructured text using an output schema.

    Config:
      source / inputs        — upstream ref
      model, api_key_env_var — LLM to use
      output_schema          — JSON Schema (object) — the shape of the returned dict
      system_prompt          — override default ("You extract structured data...")
      prompt_template        — override default (`{text}`)
      strict                 — true (default; missing required fields raise) |
                               false (missing → None)
    """
    materialized_at = _now_iso()
    source_id = step.get("source", _ROOT_SOURCE_ID)
    src_text = _get_source_text(state, source_id) if source_id else ""
    inputs = _resolve_inputs(step, state)

    output_schema = step.get("output_schema")
    if not output_schema or not isinstance(output_schema, dict):
        raise ValueError(
            "extract requires `output_schema: {type: object, properties: {...}}` "
            "(a JSON Schema object)."
        )
    strict = bool(step.get("strict", True))

    system_prompt = step.get("system_prompt") or (
        "Extract structured metadata from the input text. Call the "
        "`extract_data` tool with the extracted fields. Return only the tool call."
    )
    system_prompt = _substitute_ports(system_prompt, inputs)
    user_prompt = step.get("prompt_template", "{text}").replace("{text}", src_text)
    user_prompt = _substitute_ports(user_prompt, inputs)

    tool = {
        "type": "function",
        "function": {
            "name": "extract_data",
            "description": "Return extracted data matching the schema.",
            "parameters": output_schema,
        },
    }

    result = _completion(
        model=step["model"],
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        api_key_env_var=step.get("api_key_env_var"),
        api_base_env_var=step.get("api_base_env_var"),
        temperature=step.get("temperature", 0.0),
        max_tokens=step.get("max_tokens", 1024),
        tools=[tool],
        tool_choice="required",
        reasoning_effort=step.get("reasoning_effort"),
        thinking_budget=step.get("thinking_budget"),
        prompt_caching=bool(step.get("prompt_caching", False)),
    )

    extracted: Optional[Dict[str, Any]] = None
    if result["tool_calls"]:
        raw_args = result["tool_calls"][0].get("arguments") or "{}"
        try:
            extracted = json.loads(raw_args)
        except json.JSONDecodeError as e:
            raise RuntimeError(
                f"extract: LLM returned invalid JSON in tool call: {e}. "
                f"Raw: {raw_args[:300]}"
            ) from e

    if extracted is None:
        raise RuntimeError(
            "extract: LLM did not emit a tool call. This usually means the "
            "model doesn't support forced tool calls — try a different model."
        )

    if strict:
        required = output_schema.get("required") or []
        missing = [k for k in required if k not in extracted]
        if missing:
            raise RuntimeError(
                f"extract (strict): missing required fields {missing} in extracted data."
            )

    return {
        "text": json.dumps(extracted, indent=2),
        "extracted": extracted,
        "output_schema": output_schema,
        "model": step["model"],
        "model_fingerprint": f"{step['model']}@extract",
        "materialized_at": materialized_at,
        "cost_usd": _sum_cost(result),
        "latency_ms": _sum_latency(result),
        "tokens_total": _sum_tokens(result),
        "n_llm_calls": 1,
        "op": "extract",
    }


# ── classify op ───────────────────────────────────────────────────────
#
# Text → label from a fixed set. Simplest, most common enterprise use case.
# Uses tool_choice="required" with a single-field enum parameter.


def _do_classify(step: dict, state: Dict[str, Any], context) -> Dict[str, Any]:
    """Classify source text into one of a fixed set of labels.

    Config:
      source / inputs        — upstream ref
      model, api_key_env_var — LLM
      labels                 — list of label strings (enum)
      include_rationale      — true (default) — include a one-sentence rationale
                               false → skip; smaller / cheaper prompt
      system_prompt          — override default
      prompt_template        — override default (`{text}`)
    """
    materialized_at = _now_iso()
    source_id = step.get("source", _ROOT_SOURCE_ID)
    src_text = _get_source_text(state, source_id) if source_id else ""
    inputs = _resolve_inputs(step, state)

    labels = step.get("labels")
    if not labels or not isinstance(labels, list):
        raise ValueError("classify requires `labels: [...]` (non-empty list of strings).")
    labels = [str(l) for l in labels]

    include_rationale = bool(step.get("include_rationale", True))

    props: Dict[str, Any] = {
        "label": {"type": "string", "enum": labels, "description": "The chosen label."},
    }
    required = ["label"]
    if include_rationale:
        props["rationale"] = {"type": "string", "description": "One-sentence reason for the choice."}
        required.append("rationale")

    tool = {
        "type": "function",
        "function": {
            "name": "classify",
            "description": f"Pick one label from: {', '.join(labels)}.",
            "parameters": {"type": "object", "properties": props, "required": required},
        },
    }

    system_prompt = step.get("system_prompt") or (
        f"You are a classifier. Read the input and call `classify` with one of: "
        f"{', '.join(labels)}."
    )
    system_prompt = _substitute_ports(system_prompt, inputs)
    user_prompt = step.get("prompt_template", "{text}").replace("{text}", src_text)
    user_prompt = _substitute_ports(user_prompt, inputs)

    result = _completion(
        model=step["model"],
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        api_key_env_var=step.get("api_key_env_var"),
        api_base_env_var=step.get("api_base_env_var"),
        temperature=step.get("temperature", 0.0),
        max_tokens=step.get("max_tokens", 256),
        tools=[tool],
        tool_choice="required",
        reasoning_effort=step.get("reasoning_effort"),
        thinking_budget=step.get("thinking_budget"),
        prompt_caching=bool(step.get("prompt_caching", False)),
    )

    label: Optional[str] = None
    rationale: Optional[str] = None
    if result["tool_calls"]:
        try:
            args = json.loads(result["tool_calls"][0].get("arguments") or "{}")
            label = args.get("label")
            rationale = args.get("rationale")
        except json.JSONDecodeError:
            label = None

    if label not in labels:
        raise RuntimeError(
            f"classify: LLM returned invalid label {label!r} (not in {labels})."
        )

    return {
        "text": label,
        "label": label,
        "rationale": rationale,
        "labels": labels,
        "model": step["model"],
        "model_fingerprint": f"{step['model']}@classify",
        "materialized_at": materialized_at,
        "cost_usd": _sum_cost(result),
        "latency_ms": _sum_latency(result),
        "tokens_total": _sum_tokens(result),
        "n_llm_calls": 1,
        "op": "classify",
    }


# ── reduce op ─────────────────────────────────────────────────────────
#
# LLM-fold over a list — chunk-by-chunk, prior summary + next chunk →
# updated summary. Solves the "list too big for one context window"
# problem without hand-unrolling.


def _do_reduce(step: dict, state: Dict[str, Any], context) -> Dict[str, Any]:
    """Fold an LLM over chunks of a list.

    Config:
      source / inputs           — upstream returning a JSON list
      model, api_key_env_var    — LLM
      chunk_size                — items per LLM call (default 10)
      initial_prompt_template   — first-chunk prompt.
                                  Placeholders: {items}, {n}
      fold_prompt_template      — subsequent-chunk prompt.
                                  Placeholders: {prior}, {items}, {n}, {chunk_index}, {n_chunks}
      system_prompt             — optional
    """
    materialized_at = _now_iso()
    source_id = step.get("source", _ROOT_SOURCE_ID)
    src_text = _get_source_text(state, source_id) if source_id else ""
    inputs = _resolve_inputs(step, state)

    items = _parse_items(src_text)
    if not items:
        raise ValueError("reduce: source parsed as empty list. Provide a JSON array.")

    chunk_size = int(step.get("chunk_size", 10))
    model = step["model"]
    api_key_env_var = step.get("api_key_env_var")
    api_base_env_var = step.get("api_base_env_var")
    temperature = step.get("temperature", 0.0)
    max_tokens = step.get("max_tokens", 2048)

    system_prompt = step.get("system_prompt")
    if system_prompt:
        system_prompt = _substitute_ports(system_prompt, inputs)

    initial_tmpl = step.get(
        "initial_prompt_template",
        "You have {n} items below. Summarize them into a single coherent paragraph.\n\n{items}",
    )
    fold_tmpl = step.get(
        "fold_prompt_template",
        "Prior summary (do NOT lose information from it):\n{prior}\n\n"
        "New items ({chunk_index}/{n_chunks}, {n} items in this batch):\n{items}\n\n"
        "Update the summary to include the new items. Return one coherent paragraph.",
    )

    def _render_items(chunk: List[Any]) -> str:
        return "\n".join(
            f"- {json.dumps(it) if not isinstance(it, str) else it}"
            for it in chunk
        )

    chunks = [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]
    n_chunks = len(chunks)
    context.log.info(
        f"[reduce:{step.get('id', '?')}] folding {len(items)} item(s) in {n_chunks} chunk(s)"
    )

    all_llm_results = []
    prior_summary: str = ""

    for i, chunk in enumerate(chunks):
        rendered = _render_items(chunk)
        if i == 0:
            user_prompt = (
                initial_tmpl
                .replace("{items}", rendered)
                .replace("{n}", str(len(chunk)))
            )
        else:
            user_prompt = (
                fold_tmpl
                .replace("{prior}", prior_summary)
                .replace("{items}", rendered)
                .replace("{n}", str(len(chunk)))
                .replace("{chunk_index}", str(i + 1))
                .replace("{n_chunks}", str(n_chunks))
            )
        user_prompt = _substitute_ports(user_prompt, inputs)
        result = _completion(
            model=model, system_prompt=system_prompt, user_prompt=user_prompt,
            api_key_env_var=api_key_env_var, api_base_env_var=api_base_env_var,
            temperature=temperature, max_tokens=max_tokens,
            reasoning_effort=step.get("reasoning_effort"),
            thinking_budget=step.get("thinking_budget"),
            prompt_caching=bool(step.get("prompt_caching", False)),
        )
        all_llm_results.append(result)
        prior_summary = result["content"]

    return {
        "text": prior_summary,
        "n_items": len(items),
        "n_chunks": n_chunks,
        "chunk_size": chunk_size,
        "model": model,
        "model_fingerprint": f"{model}@reduce×{n_chunks}",
        "materialized_at": materialized_at,
        "cost_usd": _sum_cost(*all_llm_results),
        "latency_ms": _sum_latency(*all_llm_results),
        "tokens_total": _sum_tokens(*all_llm_results),
        "n_llm_calls": len(all_llm_results),
        "op": "reduce",
    }


# ── self_reflect op ───────────────────────────────────────────────────
#
# ONE LLM call producing draft + critique + revised. Cost-sensitive
# alternative to critique_loop (which is 2N+1 calls). Prompt forces the
# model to structure its response so we can parse out the revised part.


_REFLECT_RE = re.compile(
    r"REVISED\s*:\s*(.+?)(?:$|\Z)", re.IGNORECASE | re.DOTALL,
)
_DRAFT_RE = re.compile(r"DRAFT\s*:\s*(.+?)(?:CRITIQUE|REVISED)", re.IGNORECASE | re.DOTALL)
_CRITIQUE_RE = re.compile(r"CRITIQUE\s*:\s*(.+?)(?:REVISED)", re.IGNORECASE | re.DOTALL)


def _do_self_reflect(step: dict, state: Dict[str, Any], context) -> Dict[str, Any]:
    """One-shot draft + self-critique + revised output.

    Config:
      source / inputs        — upstream ref
      model, api_key_env_var — LLM
      system_prompt          — override default (see below)
      prompt_template        — override default (`{text}`)

    The default system prompt asks for three-section output:
        DRAFT: ...
        CRITIQUE: ...
        REVISED: ...

    Parses the REVISED section as `text`; DRAFT + CRITIQUE surface in
    metadata. If the model returns a non-structured response, the entire
    content is returned as `text` and the metadata sections are None.
    """
    materialized_at = _now_iso()
    source_id = step.get("source", _ROOT_SOURCE_ID)
    src_text = _get_source_text(state, source_id) if source_id else ""
    inputs = _resolve_inputs(step, state)

    system_prompt = step.get("system_prompt") or (
        "You are a careful writer. Produce your response in EXACTLY three sections, "
        "each on its own set of lines:\n\n"
        "DRAFT: <your initial answer>\n\n"
        "CRITIQUE: <one paragraph of self-critique — what's weak in the draft>\n\n"
        "REVISED: <the revised answer, addressing the critique>"
    )
    system_prompt = _substitute_ports(system_prompt, inputs)
    user_prompt = step.get("prompt_template", "{text}").replace("{text}", src_text)
    user_prompt = _substitute_ports(user_prompt, inputs)

    result = _completion(
        model=step["model"],
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        api_key_env_var=step.get("api_key_env_var"),
        api_base_env_var=step.get("api_base_env_var"),
        temperature=step.get("temperature", 0.0),
        max_tokens=step.get("max_tokens", 4096),
        reasoning_effort=step.get("reasoning_effort"),
        thinking_budget=step.get("thinking_budget"),
        prompt_caching=bool(step.get("prompt_caching", False)),
    )
    content = result["content"] or ""

    draft = _DRAFT_RE.search(content)
    critique = _CRITIQUE_RE.search(content)
    revised = _REFLECT_RE.search(content)

    parsed = revised is not None
    text = revised.group(1).strip() if revised else content
    if not parsed:
        context.log.warning(
            f"[self_reflect:{step.get('id', '?')}] response didn't match DRAFT/CRITIQUE/REVISED structure; "
            f"returning full content as text"
        )

    return {
        "text": text,
        "draft": draft.group(1).strip() if draft else None,
        "critique": critique.group(1).strip() if critique else None,
        "revised": revised.group(1).strip() if revised else None,
        "parsed": parsed,
        "model": step["model"],
        "model_fingerprint": f"{step['model']}@self_reflect",
        "materialized_at": materialized_at,
        "cost_usd": _sum_cost(result),
        "latency_ms": _sum_latency(result),
        "tokens_total": _sum_tokens(result),
        "n_llm_calls": 1,
        "op": "self_reflect",
    }


# ── sub_pipeline op ───────────────────────────────────────────────────
#
# Invoke an inline sub-pipeline (a `steps:` list) as ONE step of the
# outer pipeline. Enables composition + reuse of common patterns
# without duplicating YAML. The sub-pipeline runs in the same process;
# its state is completely isolated from the outer pipeline's state.


def _do_sub_pipeline(step: dict, state: Dict[str, Any], context) -> Dict[str, Any]:
    """Execute an inline sub-pipeline. Returns the specified sub-step's output.

    Config:
      source / inputs   — upstream ref (its text becomes the sub-pipeline's initial source.text)
      steps             — sub-pipeline's step list (same schema as top-level `steps:`)
      output_step_id    — which sub-step's output flows back into this asset's `text`.
                          Defaults to the last step in the sub-pipeline.
      sub_source        — optional override: `{kind: literal|file|url|database, ...}`
                          If unset, source.text = the upstream text from `source:` / `inputs:`.
    """
    materialized_at = _now_iso()
    source_id = step.get("source", _ROOT_SOURCE_ID)
    src_text = _get_source_text(state, source_id) if source_id else ""
    inputs = _resolve_inputs(step, state)

    sub_steps = step.get("steps")
    if not sub_steps or not isinstance(sub_steps, list):
        raise ValueError("sub_pipeline requires `steps: [...]` (inline sub-pipeline).")

    # Isolated state — no leakage from outer pipeline.
    sub_state: Dict[str, Any] = {
        "source": {"text": src_text, "source_kind": "sub_pipeline_input"},
        "__ctx__": _ctx_of(state),  # inherit partition context
        "__personas__": state.get("__personas__") or {},  # inherit personas
        "__policy__": state.get("__policy__") or {},  # inherit policy; share ONE usage dict so budgets are run-wide
        "__policy_usage__": state.setdefault("__policy_usage__", {}),
        "__agents__": state.get("__agents__") or {},  # inherit agents
    }
    sub_source = step.get("sub_source")
    if sub_source:
        sub_ctx = _ctx_of(state)
        sub_entry = _ingest(
            sub_source, context,
            partition_key=sub_ctx.get("partition_key"),
            partition_fields=sub_ctx.get("partition_fields"),
        )
        sub_state["source"] = sub_entry

    # Merge outer inputs into sub-pipeline's source text via port substitution
    # in EACH sub-step's string fields is not possible transparently; instead
    # we make outer inputs available as pre-populated sub-state entries. Any
    # sub-step can read them via `source: <port_name>`.
    for port_name, port_value in inputs.items():
        # inputs values are already resolved to text (via _resolve_inputs);
        # wrap in a step-like dict so `_get_source_text(sub_state, port_name)` works.
        sub_state[port_name] = {"text": port_value if isinstance(port_value, str) else str(port_value)}

    total_llm_calls = 0
    total_cost = 0.0
    total_latency_ms = 0
    total_tokens = 0

    for sub_step in sub_steps:
        pre_keys = set(sub_state.keys())
        _run_step(sub_step, sub_state, context)
        # Sum cost/latency across the sub-step's output.
        new_key = next(iter(set(sub_state.keys()) - pre_keys), None)
        if new_key and isinstance(sub_state[new_key], dict):
            r = sub_state[new_key]
            if r.get("cost_usd") is not None:
                total_cost += float(r["cost_usd"])
            if r.get("latency_ms") is not None:
                total_latency_ms += int(r["latency_ms"])
            if r.get("tokens_total") is not None:
                total_tokens += int(r["tokens_total"])
            if r.get("n_llm_calls") is not None:
                total_llm_calls += int(r["n_llm_calls"])

    # Return the requested step's output text.
    output_step_id = step.get("output_step_id") or _last_step_id(sub_state)
    if output_step_id not in sub_state:
        raise ValueError(
            f"sub_pipeline: output_step_id {output_step_id!r} not in sub-pipeline state. "
            f"Available: {[k for k in sub_state if not k.startswith('__')]}"
        )
    output_entry = sub_state[output_step_id]
    output_text = (
        output_entry.get("text", "") if isinstance(output_entry, dict) else str(output_entry)
    )

    return {
        "text": output_text,
        "sub_steps_run": [s.get("id") for s in sub_steps if s.get("id")],
        "output_step_id": output_step_id,
        "cost_usd": total_cost if total_cost > 0 else None,
        "latency_ms": total_latency_ms,
        "tokens_total": total_tokens if total_tokens > 0 else None,
        "n_llm_calls": total_llm_calls,
        "model_fingerprint": f"sub_pipeline[{len(sub_steps)} steps]",
        "materialized_at": materialized_at,
        "op": "sub_pipeline",
    }


# ── agent_call op ─────────────────────────────────────────────────────
#
# Universal dispatcher for pre-built agents declared in the top-level
# `agents:` block. The step-level YAML is agent-kind-agnostic:
#
#     - id: security_review
#       op: agent_call
#       agent: cisobot                    # name in agents: block
#       prompt_template: "Review: {text}"
#       extra_context:                    # optional dict merged into payload
#         priority: "{partition.priority}"
#
# Three kinds handled: openai_assistant, remote_agent, handoff.


def _json_path_get(obj: Any, path: str) -> Any:
    """Very light JSON-path — supports dot navigation into dicts + [N]
    integer indexing into lists. `$.foo.bar[0].baz`. Returns None on any
    miss rather than raising."""
    if not path or path == "$":
        return obj
    if path.startswith("$."):
        path = path[2:]
    elif path.startswith("$"):
        path = path[1:].lstrip(".")
    node: Any = obj
    for part in path.split("."):
        if not part:
            continue
        while "[" in part and part.endswith("]"):
            base, idx = part[:-1].split("[", 1)
            if base:
                if not isinstance(node, dict) or base not in node:
                    return None
                node = node[base]
            try:
                node = node[int(idx)]
            except (ValueError, IndexError, TypeError):
                return None
            part = ""
        if part:
            if not isinstance(node, dict) or part not in node:
                return None
            node = node[part]
    return node


def _sub_agent_placeholders(value: Any, prompt: str, extra: Dict[str, Any]) -> Any:
    """Deep-substitute `{prompt}` and `{extra.<key>}` into all string
    values in a dict/list/scalar tree. Non-strings pass through."""
    if isinstance(value, str):
        result = value.replace("{prompt}", prompt)
        for k, v in (extra or {}).items():
            result = result.replace("{extra." + k + "}", str(v) if not isinstance(v, str) else v)
        return result
    if isinstance(value, list):
        return [_sub_agent_placeholders(x, prompt, extra) for x in value]
    if isinstance(value, dict):
        return {k: _sub_agent_placeholders(v, prompt, extra) for k, v in value.items()}
    return value


def _call_openai_assistant(agent_cfg: Dict[str, Any], prompt: str, context) -> Dict[str, Any]:
    """OpenAI Assistants API — create thread + add message + create run +
    poll to completion + extract latest assistant message."""
    try:
        from openai import OpenAI
    except ImportError as e:
        raise ImportError(
            "agent_call (kind=openai_assistant) requires the openai SDK: "
            "pip install 'openai>=1.30.0'"
        ) from e

    assistant_id = agent_cfg.get("assistant_id")
    if not assistant_id and agent_cfg.get("assistant_id_env_var"):
        assistant_id = _env_get(agent_cfg["assistant_id_env_var"])
    if not assistant_id:
        raise ValueError(
            "openai_assistant agent needs `assistant_id:` OR `assistant_id_env_var:`"
        )

    client_kwargs: Dict[str, Any] = {}
    if agent_cfg.get("api_key_env_var"):
        v = _env_get(agent_cfg["api_key_env_var"])
        if v:
            client_kwargs["api_key"] = v
    if agent_cfg.get("api_base_env_var"):
        v = _env_get(agent_cfg["api_base_env_var"])
        if v:
            client_kwargs["base_url"] = v
    _pol_url(client_kwargs.get("base_url") or "https://api.openai.com/v1")
    client = OpenAI(**client_kwargs)

    max_wait = int(agent_cfg.get("max_wait_seconds", 300))
    thread_id = agent_cfg.get("thread_id")
    if not thread_id and agent_cfg.get("thread_id_env_var"):
        thread_id = _env_get(agent_cfg["thread_id_env_var"])

    t0 = time.time()
    if thread_id:
        context.log.info(f"[agent_call/openai_assistant] reusing thread {thread_id}")
        thread = client.beta.threads.retrieve(thread_id)
    else:
        thread = client.beta.threads.create()

    client.beta.threads.messages.create(thread_id=thread.id, role="user", content=prompt)
    run = client.beta.threads.runs.create_and_poll(
        thread_id=thread.id, assistant_id=assistant_id, timeout=max_wait,
    )
    latency_ms = int((time.time() - t0) * 1000)

    if run.status != "completed":
        raise RuntimeError(
            f"openai_assistant run status={run.status!r}: {getattr(run, 'last_error', None)}"
        )

    messages = client.beta.threads.messages.list(thread_id=thread.id, order="desc", limit=1)
    if not messages.data:
        raise RuntimeError("openai_assistant run completed but returned no messages")
    latest = messages.data[0]
    parts = [b.text.value for b in (latest.content or []) if getattr(b, "type", None) == "text"]
    text = "\n".join(parts)

    tokens_total = None
    usage = getattr(run, "usage", None)
    if usage is not None:
        try:
            tokens_total = int(usage.total_tokens)
        except (AttributeError, TypeError):
            tokens_total = None

    return {
        "text": text,
        "thread_id": thread.id,
        "run_id": run.id,
        "assistant_id": assistant_id,
        "status": run.status,
        "latency_ms": latency_ms,
        "tokens_total": tokens_total,
        "n_llm_calls": 1,
    }


def _call_remote_agent(
    agent_cfg: Dict[str, Any], prompt: str, extra: Dict[str, Any], context,
) -> Dict[str, Any]:
    """Generic HTTP agent — sync POST/GET/PUT OR async poll pattern.
    Auth via bearer OR arbitrary headers (literal + env-backed)."""
    import requests

    url = agent_cfg.get("url")
    if not url and agent_cfg.get("url_env_var"):
        url = _env_get(agent_cfg["url_env_var"])
    if not url:
        raise ValueError("remote_agent needs `url:` OR `url_env_var:`")

    method = str(agent_cfg.get("method", "POST")).upper()
    timeout = int(agent_cfg.get("timeout_seconds", 60))

    headers: Dict[str, str] = {}
    if agent_cfg.get("auth_bearer_env_var"):
        tok = _env_get(agent_cfg["auth_bearer_env_var"])
        if not tok:
            raise ValueError(
                f"remote_agent auth_bearer_env_var {agent_cfg['auth_bearer_env_var']!r} not set"
            )
        headers["Authorization"] = f"Bearer {tok}"
    for k, v in (agent_cfg.get("headers") or {}).items():
        headers[str(k)] = str(v)
    for header_name, env_var in (agent_cfg.get("headers_env") or {}).items():
        val = _env_get(env_var)
        if val is None:
            raise ValueError(
                f"remote_agent headers_env {header_name!r} references env {env_var!r} but it's unset"
            )
        headers[header_name] = val

    payload = agent_cfg.get("payload_template")
    body_str = agent_cfg.get("body_template")
    content_type = agent_cfg.get("content_type", "application/json")

    request_kwargs: Dict[str, Any] = {"headers": headers, "timeout": timeout}
    if payload is not None:
        request_kwargs["json"] = _sub_agent_placeholders(payload, prompt, extra)
    elif body_str is not None:
        request_kwargs["data"] = _sub_agent_placeholders(body_str, prompt, extra)
        headers.setdefault("Content-Type", content_type)
    else:
        if method == "POST":
            request_kwargs["json"] = {"prompt": prompt, **(extra or {})}

    t0 = time.time()
    context.log.info(f"[agent_call/remote_agent] {method} {url}")
    _pol_url(url)
    resp = requests.request(method, url, **request_kwargs)
    resp.raise_for_status()
    body = resp.json() if resp.headers.get("content-type", "").startswith("application/json") else {"_raw": resp.text}

    poll_url_path = agent_cfg.get("poll_url_path")
    poll_url: Optional[str] = _json_path_get(body, poll_url_path) if poll_url_path else None
    if poll_url:
        status_path = agent_cfg.get("poll_terminal_status_path", "$.status")
        success = set(agent_cfg.get("poll_terminal_success", ["completed", "succeeded", "done"]))
        failure = set(agent_cfg.get("poll_terminal_failure", ["failed", "cancelled", "error"]))
        poll_interval = int(agent_cfg.get("poll_interval_seconds", 5))
        poll_timeout = int(agent_cfg.get("poll_timeout_seconds", 300))

        deadline = time.time() + poll_timeout
        while time.time() < deadline:
            time.sleep(poll_interval)
            _pol_url(poll_url)
            pr = requests.get(poll_url, headers=headers, timeout=timeout)
            pr.raise_for_status()
            body = pr.json()
            status = _json_path_get(body, status_path)
            context.log.info(f"[agent_call/remote_agent] poll status={status!r}")
            if status in success:
                break
            if status in failure:
                raise RuntimeError(f"remote_agent async job failed: status={status!r}")
        else:
            raise RuntimeError(f"remote_agent async job did not complete within {poll_timeout}s")

    latency_ms = int((time.time() - t0) * 1000)

    text_path = agent_cfg.get("response_text_path", "$.text")
    text = _json_path_get(body, text_path)
    if text is None:
        text = json.dumps(body, indent=2, default=str)[:2000]
        context.log.warning(
            f"[agent_call/remote_agent] response_text_path {text_path!r} extracted no value; "
            f"returning raw body preview instead"
        )

    return {
        "text": str(text),
        "url": url,
        "method": method,
        "status_code": resp.status_code,
        "response_body": body,
        "polled": bool(poll_url),
        "latency_ms": latency_ms,
    }


def _call_handoff_agent(
    agent_cfg: Dict[str, Any], prompt: str, extra: Dict[str, Any], context,
) -> Dict[str, Any]:
    """Handoff kind — invoke a Python callable. Same shape as the
    stand-alone `handoff` op, packaged as an agents:-block entry so
    step-level YAML matches openai_assistant / remote_agent."""
    import importlib

    entry_module = agent_cfg.get("entry_module")
    entry_callable = agent_cfg.get("entry_callable")
    if not entry_module or not entry_callable:
        raise ValueError("handoff agent needs `entry_module` + `entry_callable`")
    output_text_key = agent_cfg.get("output_text_key", "final_answer")
    framework = agent_cfg.get("framework")

    initial_state = {"prompt": prompt}
    initial_state.update(extra or {})

    try:
        mod = importlib.import_module(entry_module)
    except ImportError as e:
        raise ImportError(f"handoff agent: could not import {entry_module!r}: {e}") from e
    fn = getattr(mod, entry_callable, None)
    if fn is None or not callable(fn):
        raise ValueError(f"handoff agent: {entry_callable!r} is not callable in {entry_module!r}")

    t0 = time.time()
    context.log.info(f"[agent_call/handoff] {framework or 'user'} → {entry_module}.{entry_callable}")
    result = fn(**initial_state)
    latency_ms = int((time.time() - t0) * 1000)

    if isinstance(result, dict):
        text_val = result.get(output_text_key, "")
        text = text_val if isinstance(text_val, str) else json.dumps(text_val, default=str)
    elif isinstance(result, str):
        text = result
    else:
        text = str(result)

    return {
        "text": text,
        "framework": framework,
        "entry_module": entry_module,
        "entry_callable": entry_callable,
        "framework_result": result if isinstance(result, dict) else {"_result": result},
        "latency_ms": latency_ms,
    }


_AGENT_KIND_HANDLERS = {
    "openai_assistant": _call_openai_assistant,
    "remote_agent": _call_remote_agent,
    "handoff": _call_handoff_agent,
}


def _do_agent_call(step: dict, state: Dict[str, Any], context) -> Dict[str, Any]:
    """Dispatch to a pre-built agent declared in the top-level `agents:` block.

    Config:
      agent           — name in `agents:` (required)
      prompt_template — user prompt (default `{text}`). Placeholders:
                        `{text}` (source), `{port_name}` (typed inputs),
                        `{partition.<name>}` (composite partition key parts).
      source / inputs — usual upstream ref
      extra_context   — optional dict; merged into the agent's payload
                        (available as `{extra.<key>}` in payload_template).
    """
    materialized_at = _now_iso()
    source_id = step.get("source", _ROOT_SOURCE_ID)
    src_text = _get_source_text(state, source_id) if source_id else ""
    inputs = _resolve_inputs(step, state)

    agents = state.get("__agents__") or {}
    agent_name = step.get("agent")
    if not agent_name:
        raise ValueError("agent_call requires `agent: <name>` (a name in the top-level agents: block)")
    if agent_name not in agents:
        raise ValueError(
            f"agent_call: agent {agent_name!r} not in `agents:` block. "
            f"Available: {sorted(agents.keys())}"
        )
    agent_cfg = dict(agents[agent_name])
    kind = agent_cfg.pop("kind", None)
    if kind not in _AGENT_KIND_HANDLERS:
        raise ValueError(
            f"agent_call: agent {agent_name!r} has invalid kind {kind!r}. "
            f"Valid: {sorted(_AGENT_KIND_HANDLERS.keys())}"
        )

    prompt_template = step.get("prompt_template", "{text}")
    prompt = prompt_template.replace("{text}", src_text)
    prompt = _substitute_ports(prompt, inputs)
    extra_context = _sub_agent_placeholders(step.get("extra_context") or {}, src_text, {})
    for k, v in inputs.items():
        prompt = prompt.replace("{" + k + "}", v if isinstance(v, str) else str(v))

    handler = _AGENT_KIND_HANDLERS[kind]
    if kind == "openai_assistant":
        result = handler(agent_cfg, prompt, context)
    else:
        result = handler(agent_cfg, prompt, extra_context, context)

    return {
        **result,
        "agent": agent_name,
        "agent_kind": kind,
        "materialized_at": materialized_at,
        "op": "agent_call",
    }


# ── delegate op ──────────────────────────────────────────────────────
#
# Dynamic sibling of `route`/`agent_call`: instead of picking from a fixed,
# inline-YAML specialist/agent list, the candidate pool is DISCOVERED --
# from AgentCardComponent instances declared in the same project (sibling
# scan, same mechanism EnhancedDataQualityChecks uses to auto-inherit a
# checked asset's partitions_def) and/or an external agent-card manifest
# (same manifest_url/manifest_path dual-source loader as
# CatalogAgentComponent). The picker LLM reuses `route`'s exact
# forced-function-call mechanism, just over a dynamic list instead of a
# static one. Invocation reuses this file's own _call_mcp_tool_async /
# _call_remote_agent verbatim -- no new network-call code.


def _discover_sibling_agent_cards(context: dg.ComponentLoadContext) -> List[Dict[str, Any]]:
    """Scan sibling components in the same defs folder for AgentCardComponent
    instances and pull each one's `agent_card` metadata off its AssetSpec.

    Mirrors EnhancedDataQualityChecks._discover_sibling_assets
    (asset_checks/enhanced_data_quality_checks/component.py) almost exactly:
    same context.build_defs(parent_path) mechanism, same bare
    try/except: pass fallback so a broken sibling can't break this
    pipeline's own load. Must run at BUILD time (this needs a real
    ComponentLoadContext) -- build_defs() calls this once and threads the
    result into runtime state via __sibling_agent_cards__, since the
    runtime AssetExecutionContext _do_delegate actually runs with has no
    .build_defs()/.path of its own.
    """
    cards: List[Dict[str, Any]] = []
    try:
        parent_path = context.path.parent if hasattr(context.path, "parent") else None
        if not parent_path:
            return cards
        sibling_defs = context.build_defs(parent_path)
        if sibling_defs:
            # AgentCardComponent/AgentCardWorkspaceComponent emit bare
            # dg.AssetSpec entries (declare-only, no compute) -- those don't
            # have .keys/.get_asset_spec like an AssetsDefinition does, so
            # resolve_all_asset_specs() is needed here to normalize both
            # shapes instead of iterating sibling_defs.assets directly.
            for spec in sibling_defs.resolve_all_asset_specs():
                card = (spec.metadata or {}).get("agent_card") if spec else None
                if card and card.get("agent_id"):
                    cards.append(card)
    except Exception:
        pass
    return cards


def _load_external_agent_manifest(
    manifest_path: Optional[str], manifest_url: Optional[str], log
) -> List[Dict[str, Any]]:
    """Load a flat JSON array of agent-card dicts -- same manifest_path
    (local-file precedence) / manifest_url (urlopen) dual-source loader as
    CatalogAgentComponent (assets/ai/catalog_agent/component.py), reading
    agent cards instead of component-schema entries."""
    if not manifest_path and not manifest_url:
        return []
    try:
        if manifest_path:
            with open(manifest_path) as f:
                data = json.load(f)
        else:
            from urllib.request import urlopen
            _pol_url(manifest_url)
            with urlopen(manifest_url, timeout=30) as resp:
                data = json.load(resp)
        if isinstance(data, dict):
            data = data.get("agents") or data.get("cards") or []
        return [c for c in data if isinstance(c, dict) and c.get("agent_id")]
    except Exception as e:
        log.warning(f"[delegate] failed to load agent registry manifest: {e}")
        return []


def _do_delegate(step: dict, state: Dict[str, Any], context) -> Dict[str, Any]:
    """Dynamic sibling of `route`: an LLM picks the best-matching agent card
    from a DISCOVERED pool (sibling AgentCardComponent instances + an
    optional external manifest) instead of a fixed, inline-YAML list, then
    invokes the pick over MCP or plain HTTP.

    Config:
      task                 — free-text description of what this step needs
                             done (also passed to the invoked agent as its
                             instruction).
      required_tags        — optional list; pre-filters candidates by skill
                             tag (topic/domain) before the picker LLM ever
                             sees them.
      required_capabilities — optional list; pre-filters candidates by
                             card.capabilities (OUR OWN verb taxonomy --
                             e.g. critique, supervise, triage, translate,
                             summarize, classify, lookup, extract, generate,
                             code_review -- NOT part of the A2A or MCP
                             standard, just a convention for this repo. See
                             agent_card/README.md for the recommended starter
                             list). This is the lever that keeps delegate
                             usable at fleet scale: narrow hundreds of
                             registered agents down to the handful that can
                             even perform the kind of action needed, before
                             any LLM reasoning over prose descriptions.
      input_mode           — MIME type of what this step is actually sending
                             (default "text/plain", the common case for this
                             text-based pipeline). Candidates that DO declare
                             default_input_modes (a real, required A2A
                             AgentCard field) but don't list this mode are
                             dropped before picking. Cards that don't declare
                             modes at all are never excluded by this check
                             (permissive-if-undeclared).
      registry             — {manifest_url, manifest_path, discover_siblings=True,
                             max_candidates=20}. Sibling cards are discovered ONCE
                             at build time (see build_defs) and passed in via
                             state["__sibling_agent_cards__"] -- this op only
                             reads them, it does not call context.build_defs()
                             itself (that method only exists on the build-time
                             ComponentLoadContext, not the runtime
                             AssetExecutionContext this op actually runs with).
      picker                — {model, api_key_env_var, [api_base_env_var,
                             temperature, max_tokens, reasoning_effort,
                             thinking_budget]}
      fallback              — agent_id to use if the picker fails to pick validly.
      validate_mcp_schema   — default True. For an MCP-backed pick, lists the
                             server's tools and validates the rendered
                             tool_args against the tool's own real,
                             server-declared inputSchema (a required MCP
                             field) BEFORE calling -- catches a
                             tool_args_template that doesn't actually match
                             the real tool, with a clear error, instead of a
                             confusing live MCP failure or silently-wrong
                             data the server happens to tolerate. Set False
                             only if a server's schema introspection is
                             broken/unreliable.
      source / inputs       — usual upstream ref; optional (task: stands alone).

    Note on ceiling: required_tags/required_capabilities/input_mode and
    validate_mcp_schema are the real enforcement surfaces available (A2A
    modes + MCP inputSchema, plus this project's own capability-tag convention) --
    there's no standard for deeper semantic/structural matching than "does
    the declared MIME type match" / "does the JSON Schema validate." An
    agent can still be the wrong SEMANTIC fit for a task even if it passes
    every check here; that's still the picker LLM's job.
    """
    materialized_at = _now_iso()
    source_id = step.get("source", _ROOT_SOURCE_ID)
    src_text = _get_source_text(state, source_id) if source_id else ""
    inputs = _resolve_inputs(step, state)

    task = step.get("task")
    if not task:
        raise ValueError("delegate requires `task: <free text>`")
    # `{port_name}` substitution from `inputs:` (same primitive every other
    # op uses) -- the docstring above has always claimed this works; it
    # didn't, until now (found while building a 3-step debate example that
    # needed an arbitrator step's task to reference BOTH prior arguments by
    # name). Substituting here means {prompt} downstream (tool_args_template/
    # HTTP payload) already carries the joined content too, for free.
    task = _substitute_ports(task, inputs)

    registry = step.get("registry") or {}
    sibling_cards = state.get("__sibling_agent_cards__") or []
    if not registry.get("discover_siblings", True):
        sibling_cards = []
    external_cards = _load_external_agent_manifest(
        registry.get("manifest_path"), registry.get("manifest_url"), context.log
    )

    pool: Dict[str, Dict[str, Any]] = {}
    for card in external_cards:  # external first, siblings win on agent_id conflict
        pool[card["agent_id"]] = card
    for card in sibling_cards:
        pool[card["agent_id"]] = card

    candidates = list(pool.values())

    required_tags = set(step.get("required_tags") or [])
    if required_tags:
        candidates = [
            c for c in candidates
            if required_tags & {t for s in (c.get("skills") or []) for t in (s.get("tags") or [])}
        ]

    required_capabilities = set(step.get("required_capabilities") or [])
    if required_capabilities:
        candidates = [
            c for c in candidates
            if required_capabilities & set(c.get("capabilities") or [])
        ]

    input_mode = step.get("input_mode", "text/plain")
    candidates = [
        c for c in candidates
        if not c.get("default_input_modes") or input_mode in c["default_input_modes"]
    ]

    max_candidates = registry.get("max_candidates", 20)
    candidates = candidates[:max_candidates]

    if not candidates:
        raise RuntimeError(
            "delegate: no agent cards available (checked sibling AgentCardComponent "
            "instances + registry.manifest_url/manifest_path, after required_tags/"
            "required_capabilities/input_mode filtering)."
        )

    ids = [c["agent_id"] for c in candidates]
    if len(set(ids)) != len(ids):
        raise ValueError(f"delegate: duplicate agent_id in discovered pool: {ids}")

    # One tool per candidate card -- same forced-function-call mechanism as
    # `route`, just over a dynamic list instead of a static `specialists:`.
    # Surfacing capabilities/modes here is a soft guard (helps the picker
    # avoid an implausible pick) on top of the hard filters above -- it does
    # NOT fetch live MCP schemas per candidate (that would mean one network
    # round-trip per candidate just to build this prompt, which doesn't
    # scale to hundreds of agents). Live schema validation happens once,
    # after a pick is made, in the invocation step below.
    tools = []
    for c in candidates:
        skills_desc = "; ".join(
            f"{s.get('name', s.get('id', ''))}: {s.get('description', '')}"
            for s in (c.get("skills") or [])
        )
        description = f"{c.get('description', '')} Skills: {skills_desc}"
        if c.get("capabilities"):
            description += f" Capabilities: {', '.join(c['capabilities'])}."
        if c.get("default_input_modes") or c.get("default_output_modes"):
            description += (
                f" Accepts: {', '.join(c.get('default_input_modes') or ['any'])}."
                f" Returns: {', '.join(c.get('default_output_modes') or ['any'])}."
            )
        description = description[:1000]
        tools.append({
            "type": "function",
            "function": {
                "name": c["agent_id"],
                "description": description,
                "parameters": {
                    "type": "object",
                    "properties": {
                        "reasoning": {
                            "type": "string",
                            "description": "One-sentence reason this agent fits the task.",
                        },
                    },
                    "required": ["reasoning"],
                },
            },
        })

    picker = step.get("picker") or {}
    picker_model = picker.get("model")
    if not picker_model:
        raise ValueError("delegate requires `picker: {model: ...}`")

    picker_system = (
        "You are an agent router. Given a task, pick the ONE registered "
        "agent best suited to perform it, from the function list provided. "
        "You must call exactly one function."
    )
    context.log.info(
        f"[delegate:{step.get('id', '?')}] picking among {len(candidates)} "
        f"registered agents via {picker_model}"
    )

    picker_result = _completion(
        model=picker_model,
        system_prompt=picker_system,
        user_prompt=task if not src_text else f"{task}\n\nContext:\n{src_text}",
        api_key_env_var=picker.get("api_key_env_var"),
        api_base_env_var=picker.get("api_base_env_var"),
        temperature=picker.get("temperature", 0.0),
        max_tokens=picker.get("max_tokens", 300),
        tools=tools,
        tool_choice="required",
        reasoning_effort=picker.get("reasoning_effort"),
        thinking_budget=picker.get("thinking_budget"),
    )

    selected = None
    reasoning = None
    picking_source = "picker_tool_call"
    if picker_result["tool_calls"]:
        pick = picker_result["tool_calls"][0]["name"]
        if pick in ids:
            selected = pick
            try:
                args = json.loads(picker_result["tool_calls"][0]["arguments"] or "{}")
                reasoning = args.get("reasoning")
            except json.JSONDecodeError:
                reasoning = None
        else:
            context.log.warning(f"[delegate] picker chose unknown agent_id {pick!r}")

    fallback = step.get("fallback")
    if selected is None:
        if fallback is None or fallback not in ids:
            raise RuntimeError(
                f"delegate: picker failed to choose a valid agent and no valid "
                f"fallback set (candidates: {ids})"
            )
        context.log.warning(f"[delegate] falling back to {fallback!r}")
        selected = fallback
        picking_source = "fallback"

    card = next(c for c in candidates if c["agent_id"] == selected)
    invocation = card.get("invocation") or {}
    context.log.info(f"[delegate] selected agent_id={selected}")

    t0 = time.time()
    if invocation.get("mcp_server"):
        import asyncio
        server_cfg = invocation["mcp_server"]
        tool_name = invocation.get("tool_name")
        if not tool_name:
            raise ValueError(
                f"delegate: agent {selected!r} invocation.mcp_server set but "
                f"no invocation.tool_name"
            )
        args_template = invocation.get("tool_args_template") or {"task": "{prompt}"}
        tool_args = _sub_agent_placeholders(args_template, task, {"src_text": src_text})
        try:
            mcp_result = asyncio.run(
                _call_mcp_tool_async(
                    log=context.log, server_cfg=server_cfg, tool_name=tool_name,
                    tool_args=tool_args, parse_as="auto",
                    validate_input_schema=step.get("validate_mcp_schema", True),
                )
            )
        except _BaseExceptionGroup as eg:
            # Recursively unwrap nested anyio ExceptionGroups (mcp lib nests
            # session-level and stdio_client-level task groups) -- same
            # unwrapping as _do_mcp_call, for the same reason.
            def _first_leaf(exc):
                while isinstance(exc, _BaseExceptionGroup) and exc.exceptions:
                    exc = exc.exceptions[0]
                return exc

            inner = _first_leaf(eg)
            context.log.error(f"[delegate] mcp call failed: {type(inner).__name__}: {inner}")
            raise inner from eg
        text = (
            json.dumps(mcp_result["value"], indent=2, default=str)
            if mcp_result["kind"] == "json" else mcp_result["raw"]
        )
        invocation_mode = "mcp"
        invocation_detail = {"tool_name": tool_name, "transport": server_cfg.get("type", "stdio")}
    elif invocation.get("http"):
        extra_context = {"src_text": src_text}
        remote_result = _call_remote_agent(invocation["http"], task, extra_context, context)
        text = remote_result["text"]
        invocation_mode = "http"
        invocation_detail = {"url": remote_result["url"], "status_code": remote_result["status_code"]}
    else:
        raise ValueError(
            f"delegate: agent {selected!r} card has no invocation.mcp_server "
            f"or invocation.http"
        )
    latency_ms = int((time.time() - t0) * 1000)

    return {
        "text": text,
        "picked_agent_id": selected,
        "picker_reasoning": reasoning,
        "picking_source": picking_source,
        "invocation_mode": invocation_mode,
        "invocation_detail": invocation_detail,
        "candidates_considered": ids,
        "materialized_at": materialized_at,
        "latency_ms": latency_ms,
        "op": "delegate",
    }


def _resolve_component_class(component_type: str):
    """Resolve a dotted `component_type` string to a real class, at STEP
    EXECUTION time (inside a running op) -- NOT via a full `dg` CLI /
    component-tree bootstrap, which this environment can't always do.

    For `dagster_community_components.<ClassName>` (how every component in
    this repo is addressed, in YAML `type:` and everywhere else), resolves
    via that package's own `_CLASS_PATHS` registry + `_PACKAGE_ROOT`, trying
    both the installed-package layout (`assets/` nested under the package)
    and this repo's own dev layout (`assets/` as a sibling of the package
    dir) -- both are real, valid layouts depending on how this is deployed.

    For anything else, falls back to a plain `importlib.import_module` +
    getattr (same convention as the `handoff` op's entry_module/entry_callable).
    """
    import importlib
    import importlib.util
    from pathlib import Path

    if component_type.startswith("dagster_community_components."):
        class_name = component_type.rsplit(".", 1)[1]
        import dagster_community_components as dcc

        rel_path = dcc._CLASS_PATHS.get(class_name)
        if not rel_path:
            raise ValueError(
                f"invoke_component: {class_name!r} is not registered in "
                f"dagster_community_components._CLASS_PATHS."
            )
        pkg_dir = Path(dcc.__file__).resolve().parent
        for candidate_root in (pkg_dir, pkg_dir.parent):
            full_path = candidate_root / rel_path
            if full_path.exists():
                spec = importlib.util.spec_from_file_location(class_name, full_path)
                mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(mod)
                return getattr(mod, class_name)
        raise ImportError(
            f"invoke_component: could not locate {class_name}'s component.py at "
            f"either {pkg_dir / rel_path} or {pkg_dir.parent / rel_path}."
        )

    module_path, _, class_name = component_type.rpartition(".")
    if not module_path:
        raise ValueError(
            f"invoke_component: component_type must be a dotted path "
            f"'module.ClassName', got {component_type!r}"
        )
    try:
        mod = importlib.import_module(module_path)
    except ImportError as e:
        raise ImportError(f"invoke_component: could not import {module_path!r}: {e}") from e
    cls = getattr(mod, class_name, None)
    if cls is None:
        raise ValueError(f"invoke_component: {class_name!r} not found in {module_path!r}")
    return cls


def _do_invoke_component(step: dict, state: Dict[str, Any], context) -> Dict[str, Any]:
    """Invoke an EXISTING component's real asset compute function directly,
    in-process, against the pipeline's current in-flight data -- without
    registering it as a Dagster asset or touching the asset graph/lineage at
    all. This is how a deterministic transform already in the component
    catalog (filter, rank, dedupe, ...) gets reused mid-pipeline, between two
    agent steps, instead of reimplementing its logic here.

    Mechanism: resolve `component_type` (see _resolve_component_class),
    instantiate it with `attributes` (same shape as its own YAML config),
    call its REAL build_defs() to get the real AssetsDefinition, then call
    THAT directly as a function -- Dagster assets are directly callable,
    the same mechanism Dagster's own testing docs use -- with a
    `dagster.build_asset_context()` (a real, public Dagster testing utility
    for invoking an asset's body outside of a real run) and the pipeline's
    current data converted to a DataFrame. No real run, no materialization
    event, no new node in the asset graph.

    Config:
      component_type  — dotted path, e.g.
                        "dagster_community_components.FilterComponent".
      attributes      — dict passed to the component's constructor (same
                        fields as its own YAML `attributes:` block).
                        IMPORTANT: most components' own Pydantic model
                        requires asset-graph-specific fields (e.g.
                        FilterComponent's `asset_name` + exactly one of
                        `upstream_asset_key`/`source`) even though this op
                        bypasses the graph entirely -- their VALUES don't
                        matter here (no real asset gets registered), only
                        that they satisfy whatever that component's own
                        validation requires. Pass placeholders for those.
      input_kwarg     — which of the target asset's input names receives
                        the data. Auto-detected when the asset declares
                        exactly one input (the common case for a
                        single-upstream transform); required if it
                        declares more than one or zero.
      asset_name      — if the target's build_defs() returns more than one
                        asset, the (Dagster) asset name to pick.
      resources       — optional dict forwarded to
                        dg.build_asset_context(resources=...), for a target
                        that needs a real Dagster resource (e.g. a
                        warehouse client) to run.
      partition_key   — optional, forwarded to build_asset_context.
      source / inputs — usual upstream ref; its text is parsed as a JSON
                        list (same _parse_items as `map`/`reduce`) and
                        turned into a DataFrame for the target's input.
    """
    import pandas as pd

    materialized_at = _now_iso()
    source_id = step.get("source", _ROOT_SOURCE_ID)
    src_text = _get_source_text(state, source_id) if source_id else ""

    component_type = step.get("component_type")
    if not component_type:
        raise ValueError("invoke_component requires `component_type: <dotted path>`")
    attributes = dict(step.get("attributes") or {})

    _pol_component(component_type)

    cls = _resolve_component_class(component_type)
    try:
        comp = cls(**attributes)
    except Exception as e:
        raise ValueError(
            f"invoke_component: failed to instantiate {component_type!r} with "
            f"attributes={attributes!r}: {e}"
        ) from e

    try:
        defs = comp.build_defs(None)
    except Exception as e:
        raise ValueError(
            f"invoke_component: {component_type!r}.build_defs() failed: {e}. "
            f"Check that `attributes` satisfies that component's own required "
            f"fields (asset-graph-specific ones included, even if their value "
            f"doesn't matter for direct invocation)."
        ) from e

    assets_defs = [a for a in (defs.assets or []) if hasattr(a, "op")]
    if not assets_defs:
        raise ValueError(
            f"invoke_component: {component_type!r}'s build_defs() produced no "
            f"invokable AssetsDefinition (a declare-only/external component "
            f"isn't valid here -- there's no compute function to call)."
        )
    asset_selector = step.get("asset_name")
    if asset_selector:
        matching = [a for a in assets_defs if asset_selector in [k.to_user_string() for k in a.keys]]
        if not matching:
            available = [k.to_user_string() for a in assets_defs for k in a.keys]
            raise ValueError(
                f"invoke_component: no asset named {asset_selector!r} in "
                f"{component_type!r}'s build_defs() (available: {available})"
            )
        assets_def = matching[0]
    elif len(assets_defs) == 1:
        assets_def = assets_defs[0]
    else:
        available = [k.to_user_string() for a in assets_defs for k in a.keys]
        raise ValueError(
            f"invoke_component: {component_type!r}'s build_defs() produced "
            f"{len(assets_defs)} assets ({available}); set `asset_name` to pick one."
        )

    input_names = list(assets_def.op.ins.keys())
    input_kwarg = step.get("input_kwarg")
    if not input_kwarg:
        if len(input_names) == 1:
            input_kwarg = input_names[0]
        elif len(input_names) == 0:
            input_kwarg = None
        else:
            raise ValueError(
                f"invoke_component: {component_type!r}'s asset takes multiple "
                f"inputs {input_names} -- set `input_kwarg` to pick which one "
                f"receives this step's data."
            )

    items = _parse_items(src_text) if src_text else []
    df = pd.DataFrame(items)

    resources = step.get("resources") or None
    partition_key = step.get("partition_key")
    asset_ctx = dg.build_asset_context(resources=resources, partition_key=partition_key)

    call_kwargs = {input_kwarg: df} if input_kwarg else {}
    context.log.info(
        f"[invoke_component:{step.get('id', '?')}] {component_type} "
        f"({len(items)} input row(s) → {input_kwarg or 'no input'})"
    )
    t0 = time.time()
    try:
        result = assets_def(asset_ctx, **call_kwargs)
    except Exception as e:
        raise RuntimeError(
            f"invoke_component: {component_type!r} raised during direct "
            f"invocation: {e}"
        ) from e
    latency_ms = int((time.time() - t0) * 1000)

    # Defensive Output/MaterializeResult unwrap -- same convention several
    # components (e.g. FilterComponent, for ITS OWN upstream input) already
    # use for authors who annotate `-> Output` instead of the value type.
    if hasattr(result, "value") and hasattr(result, "metadata"):
        result = result.value
    try:
        import polars as pl
        if isinstance(result, pl.DataFrame):
            result = result.to_pandas()
    except ImportError:
        pass

    if isinstance(result, pd.DataFrame):
        records = result.to_dict(orient="records")
    elif isinstance(result, list):
        records = result
    elif isinstance(result, dict):
        records = [result]
    else:
        records = None

    if records is not None:
        text = json.dumps(records, default=str)
        items_out = [{"item": r, "text": json.dumps(r, default=str)} for r in records]
        n_items = len(records)
    else:
        text = str(result)
        items_out = None
        n_items = None

    return {
        "text": text,
        "items": items_out,
        "n_items": n_items,
        "component_type": component_type,
        "asset_name": [k.to_user_string() for k in assets_def.keys][0] if assets_def.keys else None,
        "input_kwarg": input_kwarg,
        "n_input_rows": len(items),
        "materialized_at": materialized_at,
        "latency_ms": latency_ms,
        "op": "invoke_component",
    }


# ── Reasoning & governance ops ───────────────────────────────────────────────
#
#   verify     check an answer against evidence (grounding): verdict + score + unsupported claims
#   eval       score an output against a weighted rubric (LLM-as-judge), optional reference, hard gate
#   best_of_n  N candidates -> pick the best by a verifier (or majority vote / self-consistency)
#   cascade    cheapest model first, escalate to stronger models until a verifier accepts
#   plan       plan -> execute (optionally with tools) -> verify each step -> REPLAN on failure -> synthesize
#   decide     pick one of a fixed set of NAMED outcomes with rationale + cited evidence (a decision record)
#   approve    human gate: pending record (the review queue), wait window, safe default on timeout
#
# plus a generic `when:` guard on any step ("review only the exceptions") and a per-step `policy:` that narrows the
# pipeline policy (a step can only get MORE restricted than the pipeline, never less).

_PLAN_JSON_HINT = (
    'Return ONLY JSON of the form {"steps": [{"id": "s1", "task": "what to do"}, ...]} -- no prose, no code fences.'
)


_JSON_META_KEYS = {"candidates", "escalations", "plan", "steps", "scores", "rationales", "unsupported_claims", "issues", "evidence"}


def _code_version(obj: Any) -> str:
    """Stable short hash of a config fragment -> Dagster `code_version` (so editing a step marks exactly it, and what depends on
    it, stale) and the `pipeline_version` shown on every materialization."""
    import hashlib
    return hashlib.sha1(json.dumps(obj, sort_keys=True, default=str).encode()).hexdigest()[:12]


def _parse_json_loose(text: Optional[str]) -> Any:
    """Parse the JSON a model produced even when it wrapped it in a code fence or added a sentence around it."""
    t = (text or "").strip()
    if not t:
        return None
    fenced = re.search(r"```(?:json)?\s*(.*?)```", t, re.S | re.I)
    if fenced:
        t = fenced.group(1).strip()
    try:
        return json.loads(t)
    except (json.JSONDecodeError, ValueError):
        pass
    m = re.search(r"(\{.*\}|\[.*\])", t, re.S)
    if m:
        try:
            return json.loads(m.group(1))
        except (json.JSONDecodeError, ValueError):
            return None
    return None


def _llm(cfg: Dict[str, Any], system: Optional[str], user: str, *, max_tokens: Optional[int] = None,
         temperature: Optional[float] = None, default_max_tokens: int = 1024) -> Dict[str, Any]:
    """One model call from a step / sub-config dict (model, api_key_env_var, api_base_env_var, temperature, ...)."""
    if not cfg.get("model"):
        raise ValueError("this op needs a `model:` (on the step or on the sub-config that makes the call)")
    return _completion(
        model=cfg["model"], system_prompt=system, user_prompt=user,
        api_key_env_var=cfg.get("api_key_env_var"), api_base_env_var=cfg.get("api_base_env_var"),
        temperature=cfg.get("temperature", 0.0) if temperature is None else temperature,
        max_tokens=max_tokens or cfg.get("max_tokens", default_max_tokens),
        reasoning_effort=cfg.get("reasoning_effort"), thinking_budget=cfg.get("thinking_budget"),
        prompt_caching=bool(cfg.get("prompt_caching", False)),
    )


def _base_llm_cfg(step: Dict[str, Any], sub: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """A sub-config (planner/executor/verifier/...) inherits the step's model settings unless it sets its own."""
    base = {k: step[k] for k in ("model", "api_key_env_var", "api_base_env_var", "temperature", "max_tokens",
                                 "reasoning_effort", "thinking_budget", "prompt_caching") if step.get(k) is not None}
    base.update(sub or {})
    return base


def _agg(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    return {"cost_usd": _sum_cost(*results), "latency_ms": _sum_latency(*results),
            "tokens_total": _sum_tokens(*results), "n_llm_calls": len(results)}


def _clip(s: Any, n: int = 1500) -> str:
    s = "" if s is None else str(s)
    return s if len(s) <= n else s[:n] + f"… [+{len(s) - n} chars]"


# ── shared verifier (best_of_n, cascade, plan) ───────────────────────────────

def _run_verifier(vcfg: Dict[str, Any], candidate: str, task: str, step: Dict[str, Any]) -> Dict[str, Any]:
    """Score one candidate. kinds: regex | json | contains | llm (default when a model is available).
    Returns {passed, score(0..1), reason, llm_results[]}."""
    kind = vcfg.get("kind") or ("llm" if (vcfg.get("model") or step.get("model")) else "regex")
    text = candidate or ""
    if kind == "regex":
        ok = re.search(vcfg["pattern"], text, re.I | re.S) is not None
        return {"passed": ok, "score": 1.0 if ok else 0.0, "reason": f"regex {vcfg['pattern']!r} {'matched' if ok else 'did not match'}", "llm_results": []}
    if kind == "contains":
        needles = vcfg["contains"] if isinstance(vcfg["contains"], list) else [vcfg["contains"]]
        hit = [n for n in needles if str(n).lower() in text.lower()]
        ok = len(hit) == len(needles) if vcfg.get("all", True) else bool(hit)
        return {"passed": ok, "score": len(hit) / max(len(needles), 1), "reason": f"contains {hit} of {needles}", "llm_results": []}
    if kind == "json":
        obj = _parse_json_loose(text)
        keys = vcfg.get("required_keys") or []
        ok = obj is not None and (not keys or (isinstance(obj, dict) and all(k in obj for k in keys)))
        return {"passed": ok, "score": 1.0 if ok else 0.0, "reason": "valid JSON" + (f" with keys {keys}" if keys and ok else "") if ok else "not valid JSON or missing required keys", "llm_results": []}
    if kind != "llm":
        raise ValueError(f"unknown verifier kind {kind!r}; valid: llm, regex, json, contains")
    cfg = _base_llm_cfg(step, vcfg)
    rubric = vcfg.get("rubric") or "The answer is correct, complete, and directly addresses the task."
    system = (
        "You are a strict verifier. Judge the CANDIDATE against the TASK and this rubric:\n"
        f"{rubric}\n\n"
        'Return ONLY JSON: {"score": <number 0 to 1>, "reason": "<one sentence>"}. 1 = fully meets the rubric, 0 = not at all.'
    )
    r = _llm(cfg, system, f"TASK:\n{_clip(task, 4000)}\n\nCANDIDATE:\n{_clip(text, 6000)}", max_tokens=vcfg.get("max_tokens", 300), temperature=0.0)
    obj = _parse_json_loose(r["content"]) or {}
    try:
        score = max(0.0, min(1.0, float(obj.get("score"))))
    except (TypeError, ValueError):
        score = 0.0
    min_score = float(vcfg.get("min_score", 0.7))
    return {"passed": score >= min_score, "score": score, "reason": str(obj.get("reason") or "(verifier gave no reason)"), "llm_results": [r]}


# ── verify ───────────────────────────────────────────────────────────────────

def _do_verify(step: dict, state: Dict[str, Any], context) -> Dict[str, Any]:
    """Check an answer against evidence. `source` = the answer; evidence from `evidence_source: <step id | [ids]>` or
    `inputs: {evidence: …}`. Returns text (the answer, passed through) + verdict / score / passed / unsupported_claims.

    Config: model, min_score (default 0.7), on_fail: continue | fail (default continue)."""
    materialized_at = _now_iso()
    source_id = step.get("source", _ROOT_SOURCE_ID)
    answer = _get_source_text(state, source_id)
    inputs = _resolve_inputs(step, state)
    ev_ids = step.get("evidence_source") or []
    ev_ids = [ev_ids] if isinstance(ev_ids, str) else list(ev_ids)
    evidence_parts = [f"[{i}]\n{_get_source_text(state, i)}" for i in ev_ids]
    if "evidence" in inputs:
        evidence_parts.append(inputs["evidence"])
    if not evidence_parts:
        raise ValueError("verify needs evidence: set `evidence_source: <step id>` (or a list) or `inputs: {evidence: {from: …}}`")
    cfg = _base_llm_cfg(step)
    system = step.get("system_prompt") or (
        "You verify an ANSWER strictly against the EVIDENCE. A claim is supported only if the evidence states or clearly "
        "implies it; anything else is unsupported. Return ONLY JSON: "
        '{"verdict": "supported" | "partially_supported" | "unsupported", "score": <0 to 1, share of the answer that is supported>, '
        '"unsupported_claims": ["…"], "issues": ["…"]}'
    )
    r = _llm(cfg, system, f"EVIDENCE:\n{_clip(chr(10).join(evidence_parts), 12000)}\n\nANSWER:\n{_clip(answer, 6000)}", default_max_tokens=800)
    obj = _parse_json_loose(r["content"]) or {}
    try:
        score = max(0.0, min(1.0, float(obj.get("score"))))
    except (TypeError, ValueError):
        score = 0.0
    verdict = obj.get("verdict") if obj.get("verdict") in ("supported", "partially_supported", "unsupported") else "unsupported"
    min_score = float(step.get("min_score", 0.7))
    passed = score >= min_score and verdict != "unsupported"
    if not passed and step.get("on_fail", "continue") == "fail":
        raise RuntimeError(f"[verify:{step.get('id')}] failed: verdict={verdict} score={score:.2f} (< {min_score}); "
                           f"unsupported={obj.get('unsupported_claims')}")
    return {
        "text": answer, "op": "verify", "verdict": verdict, "score": round(score, 3), "passed": passed,
        "unsupported_claims": obj.get("unsupported_claims") or [], "issues": obj.get("issues") or [],
        "model": cfg["model"], "model_fingerprint": f"{cfg['model']}@verify", "materialized_at": materialized_at,
        "usage": r.get("usage"), **_agg([r]),
    }


# ── eval ─────────────────────────────────────────────────────────────────────

def _do_eval(step: dict, state: Dict[str, Any], context) -> Dict[str, Any]:
    """Score an output on a weighted rubric (LLM-as-judge).

    Config: criteria: [name | {name, description, weight}], scale (default 5), reference_source (optional expected answer),
    fail_below (hard gate on the weighted mean, in `scale` units), model."""
    materialized_at = _now_iso()
    source_id = step.get("source", _ROOT_SOURCE_ID)
    output = _get_source_text(state, source_id)
    raw = step.get("criteria") or ["overall quality"]
    criteria: List[Dict[str, Any]] = []
    for c in raw:
        criteria.append({"name": c, "description": "", "weight": 1.0} if isinstance(c, str)
                        else {"name": c["name"], "description": c.get("description", ""), "weight": float(c.get("weight", 1.0))})
    scale = int(step.get("scale", 5))
    ref = _get_source_text(state, step["reference_source"]) if step.get("reference_source") else None
    cfg = _base_llm_cfg(step)
    rubric = "\n".join(f"- {c['name']}" + (f": {c['description']}" if c["description"] else "") for c in criteria)
    system = step.get("system_prompt") or (
        f"You are an impartial evaluator. Score the OUTPUT on each criterion from 1 (poor) to {scale} (excellent), "
        'with a one-sentence rationale. Return ONLY JSON: {"scores": {"<criterion>": {"score": <int>, "rationale": "…"}}}'
        f"\n\nCriteria:\n{rubric}"
    )
    user = f"OUTPUT:\n{_clip(output, 8000)}" + (f"\n\nREFERENCE (expected):\n{_clip(ref, 4000)}" if ref else "")
    r = _llm(cfg, system, user, default_max_tokens=900)
    obj = _parse_json_loose(r["content"]) or {}
    got = obj.get("scores") if isinstance(obj.get("scores"), dict) else {}
    scores: Dict[str, float] = {}
    rationales: Dict[str, str] = {}
    for c in criteria:
        entry = got.get(c["name"], {})
        try:
            scores[c["name"]] = max(1.0, min(float(scale), float(entry.get("score") if isinstance(entry, dict) else entry)))
        except (TypeError, ValueError):
            scores[c["name"]] = 1.0  # an unscored criterion counts against the output rather than silently passing
        rationales[c["name"]] = str(entry.get("rationale", "")) if isinstance(entry, dict) else ""
    total_w = sum(c["weight"] for c in criteria) or 1.0
    overall = sum(scores[c["name"]] * c["weight"] for c in criteria) / total_w
    fail_below = step.get("fail_below")
    passed = fail_below is None or overall >= float(fail_below)
    if not passed:
        raise RuntimeError(f"[eval:{step.get('id')}] overall {overall:.2f}/{scale} is below fail_below={fail_below}: {scores}")
    return {
        "text": output, "op": "eval", "overall": round(overall, 3), "scale": scale, "scores": scores, "rationales": rationales,
        "passed": passed, "model": cfg["model"], "model_fingerprint": f"{cfg['model']}@eval", "materialized_at": materialized_at,
        "usage": r.get("usage"), **_agg([r]),
    }


# ── best_of_n ────────────────────────────────────────────────────────────────

def _norm_answer(t: str) -> str:
    return re.sub(r"\s+", " ", (t or "").strip().lower()).strip(" .")


def _do_best_of_n(step: dict, state: Dict[str, Any], context) -> Dict[str, Any]:
    """Generate N candidates and keep the best. select: verifier (score each, highest wins) | vote (majority of normalised
    answers -- self-consistency) | first_valid. Config: n, models (cycle for an ensemble), temperature (default 0.8),
    verifier {kind: llm|regex|json|contains, …, min_score}, parallel (default true), on_none_pass: best | fail."""
    import concurrent.futures
    materialized_at = _now_iso()
    source_id = step.get("source", _ROOT_SOURCE_ID)
    src_text = _get_source_text(state, source_id) if source_id else ""
    inputs = _resolve_inputs(step, state)
    n = int(step.get("n", 3))
    if n < 1:
        raise ValueError("best_of_n needs n >= 1")
    user = _substitute_ports(step.get("prompt_template", "{text}").replace("{text}", src_text), inputs)
    system = step.get("system_prompt")
    if system:
        system = _substitute_ports(system, inputs)
    models = step.get("models") or [step.get("model")]
    verifier = step.get("verifier")
    select = step.get("select") or ("verifier" if verifier else "vote")
    if select == "verifier" and not verifier:
        raise ValueError("best_of_n select=verifier needs a `verifier:` block")
    temperature = step.get("temperature", 0.8)

    def gen(i: int) -> Dict[str, Any]:
        cfg = _base_llm_cfg(step, {"model": models[i % len(models)]})
        r = _llm(cfg, system, user, temperature=temperature, default_max_tokens=2048)
        return {"index": i, "model": cfg["model"], "text": r["content"], "result": r}

    ctx = contextvars.copy_context()  # worker threads must see the active policy + reasoning trace
    if step.get("parallel", True) and n > 1:
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(n, int(step.get("max_workers", 4)))) as ex:
            cands = list(ex.map(lambda i: ctx.copy().run(gen, i), range(n)))
    else:
        cands = [gen(i) for i in range(n)]
    llm_results = [c["result"] for c in cands]

    if verifier:
        for c in cands:
            v = _run_verifier(verifier, c["text"], user, step)
            c.update(score=v["score"], passed=v["passed"], reason=v["reason"])
            llm_results.extend(v["llm_results"])
    agreement = None
    if select == "vote":
        groups: Dict[str, List[int]] = {}
        for c in cands:
            groups.setdefault(_norm_answer(c["text"]), []).append(c["index"])
        best_key = max(groups, key=lambda k: (len(groups[k]), -groups[k][0]))
        winner = cands[groups[best_key][0]]
        agreement = round(len(groups[best_key]) / n, 3)
        for c in cands:
            c.setdefault("score", None)
    elif select == "first_valid":
        valid = [c for c in cands if c.get("passed")]
        winner = valid[0] if valid else cands[0]
    else:
        winner = max(cands, key=lambda c: (c["score"], -c["index"]))
    any_passed = any(c.get("passed") for c in cands) if verifier else True
    if verifier and not any_passed and step.get("on_none_pass", "best") == "fail":
        raise RuntimeError(f"[best_of_n:{step.get('id')}] none of {n} candidates passed the verifier; best score {winner['score']:.2f}")
    return {
        "text": winner["text"], "op": "best_of_n", "selected_index": winner["index"], "selected_model": winner["model"],
        "select": select, "n": n, "any_passed": any_passed, "agreement": agreement,
        "candidates": [{"index": c["index"], "model": c["model"], "text": _clip(c["text"], 400), "score": c.get("score"),
                        "passed": c.get("passed"), "reason": c.get("reason")} for c in cands],
        "model": ",".join(sorted({str(m) for m in models})), "model_fingerprint": f"{'+'.join(sorted({str(m) for m in models}))}@best_of_{n}",
        "materialized_at": materialized_at, **_agg(llm_results),
    }


# ── cascade ──────────────────────────────────────────────────────────────────

def _do_cascade(step: dict, state: Dict[str, Any], context) -> Dict[str, Any]:
    """Try `models` cheapest-first; escalate to the next only when the previous answer fails `accept` (a verifier) or errors.
    The point is cost control with evidence: `escalations` records exactly which model answered and why earlier ones didn't.
    Config: models (ordered), accept {kind: llm|regex|json|contains, …} (omit = accept any non-empty answer),
    on_exhausted: last | fail (default last)."""
    materialized_at = _now_iso()
    source_id = step.get("source", _ROOT_SOURCE_ID)
    src_text = _get_source_text(state, source_id) if source_id else ""
    inputs = _resolve_inputs(step, state)
    models = step.get("models") or []
    if len(models) < 1:
        raise ValueError("cascade needs `models: [cheapest, …, strongest]`")
    user = _substitute_ports(step.get("prompt_template", "{text}").replace("{text}", src_text), inputs)
    system = step.get("system_prompt")
    if system:
        system = _substitute_ports(system, inputs)
    accept = step.get("accept")
    llm_results: List[Dict[str, Any]] = []
    escalations: List[Dict[str, Any]] = []
    answer, used, accepted = "", None, False
    for i, model in enumerate(models):
        cfg = _base_llm_cfg(step, {"model": model})
        try:
            r = _llm(cfg, system, user, default_max_tokens=2048)
        except PolicyViolation:
            raise
        except Exception as e:  # noqa: BLE001 -- a failing cheap model is exactly when to escalate
            escalations.append({"model": model, "error": f"{type(e).__name__}: {e}", "accepted": False})
            continue
        llm_results.append(r)
        answer, used = r["content"], model
        if accept:
            v = _run_verifier(accept, answer, user, step)
            llm_results.extend(v["llm_results"])
            ok, score, reason = v["passed"], v["score"], v["reason"]
        else:
            ok, score, reason = bool(answer.strip()), None, "non-empty answer" if answer.strip() else "empty answer"
        escalations.append({"model": model, "accepted": ok, "score": score, "reason": reason, "cost_usd": r.get("cost_usd")})
        if ok:
            accepted = True
            break
    if not accepted and step.get("on_exhausted", "last") == "fail":
        raise RuntimeError(f"[cascade:{step.get('id')}] no model in {models} produced an accepted answer")
    return {
        "text": answer, "op": "cascade", "model_used": used, "accepted": accepted, "n_escalations": max(len(escalations) - 1, 0),
        "escalations": escalations, "model": used or models[0], "model_fingerprint": f"{'>'.join(map(str, models))}@cascade",
        "materialized_at": materialized_at, **_agg(llm_results),
    }


# ── plan (plan -> execute -> verify -> replan -> synthesize) ──────────────────

def _do_plan(step: dict, state: Dict[str, Any], context) -> Dict[str, Any]:
    """Plan-and-execute with replanning. A planner breaks the goal into ordered tasks; an executor (a plain LLM call, or a tool
    loop when `executor.mcp_servers` is set) carries each out with the results so far; an optional `verifier` checks each task's
    output and a failure triggers a REPLAN of the remaining work (up to max_replans); finally a synthesis call writes the answer.

    Config: model (default for all roles), planner {…}, executor {…, mcp_servers}, verifier {…}, goal_template ('{text}'),
    max_steps (8), max_replans (2), synthesize (true), on_fail: continue | fail (default continue)."""
    materialized_at = _now_iso()
    source_id = step.get("source", _ROOT_SOURCE_ID)
    src_text = _get_source_text(state, source_id) if source_id else ""
    inputs = _resolve_inputs(step, state)
    goal = _substitute_ports(step.get("goal_template", "{text}").replace("{text}", src_text), inputs)
    planner_cfg = _base_llm_cfg(step, step.get("planner"))
    executor_cfg = _base_llm_cfg(step, step.get("executor"))
    verifier = step.get("verifier")
    max_steps, max_replans = int(step.get("max_steps", 8)), int(step.get("max_replans", 2))
    llm_results: List[Dict[str, Any]] = []
    extra_cost: List[Dict[str, Any]] = []

    def plan_tasks(done: List[Dict[str, Any]], failure: Optional[Dict[str, Any]]) -> List[Dict[str, str]]:
        if failure is None:
            system = f"You are a careful planner. Break the goal into at most {max_steps} concrete, ordered steps. {_PLAN_JSON_HINT}"
            user = f"GOAL:\n{goal}"
        else:
            system = (f"You are replanning after a step failed. Plan ONLY the remaining work (at most {max_steps - len(done)} steps), "
                      f"taking the completed results into account and avoiding what just failed. {_PLAN_JSON_HINT}")
            done_txt = "\n".join(f"- {d['id']}: {d['task']} -> {_clip(d['output'], 600)}" for d in done if d["ok"]) or "(none)"
            user = (f"GOAL:\n{goal}\n\nCOMPLETED:\n{done_txt}\n\nFAILED STEP: {failure['task']}\nITS OUTPUT: {_clip(failure['output'], 800)}\n"
                    f"WHY IT FAILED: {failure['reason']}")
        r = _llm(planner_cfg, system, user, default_max_tokens=900)
        llm_results.append(r)
        obj = _parse_json_loose(r["content"])
        steps_raw = obj.get("steps") if isinstance(obj, dict) else obj if isinstance(obj, list) else None
        tasks = []
        for i, s in enumerate(steps_raw or []):
            task = s.get("task") if isinstance(s, dict) else str(s)
            if task:
                tasks.append({"id": (s.get("id") if isinstance(s, dict) and s.get("id") else f"s{len(done) + i + 1}"), "task": str(task)})
        if not tasks:
            context.log.warning(f"[plan:{step.get('id', '?')}] planner returned no parseable steps; falling back to a single step")
            tasks = [{"id": f"s{len(done) + 1}", "task": goal}]
        return tasks[: max(max_steps - len(done), 1)]

    def execute(task: Dict[str, str], done: List[Dict[str, Any]]) -> "tuple[str, Optional[str]]":
        ctx_txt = "\n".join(f"{d['id']} ({d['task']}): {_clip(d['output'], 800)}" for d in done if d["ok"]) or "(nothing yet)"
        prompt = f"OVERALL GOAL:\n{goal}\n\nCOMPLETED STEPS:\n{ctx_txt}\n\nCURRENT STEP ({task['id']}):\n{task['task']}"
        try:
            if executor_cfg.get("mcp_servers"):
                sub = {**executor_cfg, "id": f"{step.get('id', 'plan')}__{task['id']}", "op": "tool_use_loop", "prompt_template": "{text}"}
                out = _do_tool_use_loop(sub, {"source": {"text": prompt}, "__ctx__": state.get("__ctx__", {})}, context)
                extra_cost.append(out)
                return out["text"], None
            r = _llm(executor_cfg, executor_cfg.get("system_prompt") or (
                "You are carrying out ONE step of a larger plan. Use the completed steps as context and respond with the result of "
                "this step only."), prompt, default_max_tokens=2048)
            llm_results.append(r)
            return r["content"], None
        except PolicyViolation:
            raise
        except Exception as e:  # noqa: BLE001
            return "", f"{type(e).__name__}: {e}"

    initial = plan_tasks([], None)
    queue, done, n_replans, status = list(initial), [], 0, "completed"
    while queue:
        if len(done) >= max_steps:
            status = "max_steps"
            break
        task = queue.pop(0)
        out, err = execute(task, done)
        ok, reason = err is None, err
        if ok and verifier:
            v = _run_verifier(verifier, out, f"{goal}\n\nSTEP: {task['task']}", step)
            llm_results.extend(v["llm_results"])
            ok, reason = v["passed"], v["reason"]
        done.append({"id": task["id"], "task": task["task"], "output": out, "ok": ok, "reason": reason})
        if not ok:
            if n_replans >= max_replans:
                status = "failed"
                break
            n_replans += 1
            queue = plan_tasks(done, done[-1])
            context.log.info(f"[plan:{step.get('id', '?')}] step {task['id']} failed ({reason}); replanned -> {len(queue)} step(s)")
    good = [d for d in done if d["ok"]]
    if step.get("synthesize", True) and good:
        r = _llm(planner_cfg, "Compose the final answer to the GOAL from the step results. Be complete and do not invent facts "
                 "that the results do not support.", f"GOAL:\n{goal}\n\nRESULTS:\n" + "\n\n".join(f"{d['id']} - {d['task']}:\n{_clip(d['output'], 2500)}" for d in good),
                 default_max_tokens=2048)
        llm_results.append(r)
        text = r["content"]
    else:
        text = good[-1]["output"] if good else ""
    if status == "failed" and step.get("on_fail", "continue") == "fail":
        raise RuntimeError(f"[plan:{step.get('id')}] failed after {n_replans} replan(s); last: {done[-1]['reason']}")
    agg = _agg(llm_results)
    for o in extra_cost:  # tool-loop executors report their own totals
        agg["cost_usd"] = round((agg["cost_usd"] or 0) + (o.get("cost_usd") or 0), 6) or None
        agg["latency_ms"] += o.get("latency_ms") or 0
        agg["tokens_total"] = (agg["tokens_total"] or 0) + (o.get("tokens_total") or 0) or None
        agg["n_llm_calls"] += o.get("n_llm_calls") or 0
    return {
        "text": text, "op": "plan", "status": status, "n_replans": n_replans, "n_steps_executed": len(done),
        "plan": initial, "steps": [{**d, "output": _clip(d["output"], 800)} for d in done],
        "model": planner_cfg.get("model"), "model_fingerprint": f"{planner_cfg.get('model')}@plan", "materialized_at": materialized_at, **agg,
    }


# ── decide ───────────────────────────────────────────────────────────────────

def _do_decide(step: dict, state: Dict[str, Any], context) -> Dict[str, Any]:
    """A decision record: choose ONE of a fixed set of named outcomes, with rationale and the evidence it rests on.

    Config: outcomes: {name: "when to choose it"}, evidence_source: step id | [ids] (their text is shown to the model and the model
    cites quotes), default_outcome (used when the model's answer is not one of the outcomes; omit to fail), model.
    Result keys (usable by `when:` and downstream steps): outcome, reason, confidence, evidence."""
    materialized_at = _now_iso()
    source_id = step.get("source", _ROOT_SOURCE_ID)
    src_text = _get_source_text(state, source_id) if source_id else ""
    outcomes = step.get("outcomes") or {}
    if len(outcomes) < 2:
        raise ValueError("decide needs `outcomes:` with at least two named outcomes: {name: 'when to choose it'}")
    ev_ids = step.get("evidence_source") or []
    ev_ids = [ev_ids] if isinstance(ev_ids, str) else list(ev_ids)
    evidence_txt = "\n\n".join(f"[{i}]\n{_clip(_get_source_text(state, i), 4000)}" for i in ev_ids)
    cfg = _base_llm_cfg(step)
    menu = "\n".join(f"- {name}: {desc}" for name, desc in outcomes.items())
    system = step.get("system_prompt") or (
        "You make a single decision. Choose EXACTLY ONE outcome from the list and justify it from the evidence. Return ONLY JSON: "
        '{"outcome": "<one of the names>", "reason": "<why>", "confidence": <0 to 1>, '
        '"evidence": [{"source": "<evidence id or input>", "quote": "<short exact quote>"}]}\n\nOutcomes:\n' + menu
    )
    user = f"INPUT:\n{_clip(src_text, 6000)}" + (f"\n\nEVIDENCE:\n{evidence_txt}" if evidence_txt else "")
    r = _llm(cfg, system, user, default_max_tokens=700)
    obj = _parse_json_loose(r["content"]) or {}
    outcome = obj.get("outcome")
    fell_back = False
    if outcome not in outcomes:
        if step.get("default_outcome") in outcomes:
            outcome, fell_back = step["default_outcome"], True
        else:
            raise RuntimeError(f"[decide:{step.get('id')}] model chose {obj.get('outcome')!r}, not one of {sorted(outcomes)}")
    try:
        conf = max(0.0, min(1.0, float(obj.get("confidence"))))
    except (TypeError, ValueError):
        conf = None
    return {
        "text": f"{outcome}: {obj.get('reason', '')}".strip(": "), "op": "decide", "outcome": outcome, "reason": str(obj.get("reason") or ""),
        "confidence": conf, "evidence": obj.get("evidence") or [], "used_default_outcome": fell_back,
        "model": cfg["model"], "model_fingerprint": f"{cfg['model']}@decide", "materialized_at": materialized_at,
        "usage": r.get("usage"), **_agg([r]),
    }


# ── approve ──────────────────────────────────────────────────────────────────

class _ApprovalStore:
    """Read/write approval tokens + review-queue records on a local directory OR any fsspec URI (s3://, gs://, abfs://,
    memory://). Remote storage is what makes approvals usable where the worker filesystem is ephemeral (Dagster+ Serverless,
    containers): the reviewer writes the token to a bucket, the next run reads it. Install the driver you need (s3fs/gcsfs/adlfs)."""

    def __init__(self, root: str):
        self.is_uri = "://" in root
        if self.is_uri:
            import fsspec
            self.fs, self.root = fsspec.core.url_to_fs(root)
            self.root = self.root.rstrip("/")
        else:
            self.fs, self.root = None, os.path.abspath(os.path.expanduser(root))
        self.display = root.rstrip("/")

    def path(self, *parts: str) -> str:
        return "/".join([self.root, *parts]) if self.is_uri else os.path.join(self.root, *parts)

    def shown(self, *parts: str) -> str:
        """The path/URI a person would actually write to."""
        return "/".join([self.display, *parts]) if self.is_uri else self.path(*parts)

    def exists(self, p: str) -> bool:
        return self.fs.exists(p) if self.is_uri else os.path.exists(p)

    def read_text(self, p: str) -> str:
        if self.is_uri:
            with self.fs.open(p, "r") as f:
                return f.read()
        with open(p) as f:
            return f.read()

    def write_text(self, p: str, text: str) -> None:
        parent = p.rsplit("/", 1)[0] if self.is_uri else os.path.dirname(p)
        if self.is_uri:
            self.fs.makedirs(parent, exist_ok=True)
            with self.fs.open(p, "w") as f:
                f.write(text)
        else:
            os.makedirs(parent, exist_ok=True)
            with open(p, "w") as f:
                f.write(text)

    def modified(self, p: str) -> Optional[datetime]:
        """Last-modified time of a file (fallback age for tokens without `approved_at`); None when the store can't say."""
        try:
            if self.is_uri:
                m = self.fs.modified(p)
                return m if m.tzinfo else m.replace(tzinfo=timezone.utc)
            return datetime.fromtimestamp(os.path.getmtime(p), tz=timezone.utc)
        except (OSError, NotImplementedError, AttributeError, ValueError):
            return None

    def remove(self, p: str) -> None:
        try:
            self.fs.rm(p) if self.is_uri else os.remove(p)
        except (OSError, FileNotFoundError):
            pass


def _approval_key(raw: Any) -> str:
    """Filename-safe approval key. Sanitizing alone is lossy ("a/b" and "a_b" would both become "a_b" and so share one
    approval token), so a key that had to be changed carries a short hash of the original; an already-safe key is unchanged."""
    raw = str(raw)
    safe = re.sub(r"[^A-Za-z0-9._-]+", "_", raw)
    if safe != raw:
        import hashlib
        safe = f"{safe}-{hashlib.sha1(raw.encode('utf-8')).hexdigest()[:8]}"
    return safe


_APPROVE_REUSE = ("once", "until_changed", "always")


def _validate_approve_options(steps: List[Dict[str, Any]]) -> None:
    for st in steps:
        if st.get("op") != "approve":
            continue
        where = f"approve step {st.get('id')!r}"
        if st.get("reuse", "until_changed") not in _APPROVE_REUSE:
            raise ValueError(f"{where}: reuse must be one of {' | '.join(_APPROVE_REUSE)}, got {st.get('reuse')!r}")
        bt = st.get("bind_to", ["text", "summary"])
        if not isinstance(bt, list) or not bt or any(b not in ("text", "summary") for b in bt):
            raise ValueError(f"{where}: bind_to must be a non-empty list drawn from [text, summary], got {bt!r}")
        age = st.get("max_age_hours")
        if age is not None:
            try:
                ok = float(age) > 0
            except (TypeError, ValueError):
                ok = False
            if not ok:
                raise ValueError(f"{where}: max_age_hours must be a number > 0 (omit it for no expiry), got {age!r}")


def _approval_fingerprint(step_id: str, text: str, summary: str, bind_to: Optional[List[str]] = None) -> str:
    """What an approval is bound to: the step plus what the reviewer was shown (by default the full input text and the summary;
    `bind_to` narrows it, e.g. [summary] when the text is regenerated by a model on every run)."""
    import hashlib
    bind_to = bind_to or ["text", "summary"]
    blob = json.dumps({"step": step_id, **({"text": text} if "text" in bind_to else {}),
                       **({"summary": summary} if "summary" in bind_to else {})}, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:32]


def _approval_token_state(tok: Optional[Dict[str, Any]], fingerprint: str, reuse: str, max_age_hours: Optional[float],
                          modified: Optional[datetime], now: Optional[datetime] = None) -> "tuple[str, Optional[str]]":
    """Is this token a valid decision for the CURRENT request? -> (state, reason). state: valid | none | consumed | expired | stale.
    The reason is what the run log, the pending record and the review queue show."""
    if tok is None:
        return "none", None
    if tok.get("consumed_at"):
        return "consumed", f"consumed: this approval was already used by a run at {tok['consumed_at']}; a fresh one is needed"
    if max_age_hours is not None:
        at = None
        try:
            at = datetime.fromisoformat(str(tok["approved_at"]).replace("Z", "+00:00")) if tok.get("approved_at") else None
            at = at.replace(tzinfo=timezone.utc) if at is not None and at.tzinfo is None else at
        except ValueError:
            at = None
        at = at or modified
        if at is None:
            return "expired", "approval expired: its age cannot be determined (no approved_at, no modification time)"
        age_h = ((now or datetime.now(timezone.utc)) - at).total_seconds() / 3600.0
        if age_h > float(max_age_hours):
            return "expired", f"approval expired: given {age_h:.1f}h ago, max_age_hours={max_age_hours:g}"
    if reuse != "always":
        fp = tok.get("fingerprint")
        if not fp:
            return "stale", "token carries no fingerprint, so it is not bound to what was reviewed (write the token from the pending record)"
        if fp != fingerprint:
            return "stale", "input changed since approval"
    return "valid", None


def approve_pending(approval_dir: str, key: str, *, approved: bool = True, approver: str = "<you>", reason: str = "") -> Dict[str, Any]:
    """Approve (or reject) the pending record AS WRITTEN: copies its fingerprint into the token, so the decision is bound to
    exactly what was shown. Reviewer tooling (CLI/API/Slack bot) does the same by writing the token from the record's
    `approve_by_writing.json`. Raises FileNotFoundError when nothing is pending for the key."""
    store = _ApprovalStore(approval_dir)
    key = _approval_key(key)
    pend = json.loads(store.read_text(store.path("pending", f"{key}.json")))
    tok = {**(pend.get("approve_by_writing") or {}).get("json", {}), "approved": bool(approved), "approver": approver,
           "reason": reason, "fingerprint": pend["fingerprint"], "approved_at": _now_iso()}
    store.write_text(store.path(f"{key}.json"), json.dumps(tok, indent=2))
    return tok


def _do_approve(step: dict, state: Dict[str, Any], context) -> Dict[str, Any]:
    """Human gate inside the pipeline. A token `<approval_dir>/<key>.json` = {"approved": true|false, "approver": "...",
    "reason": "...", "fingerprint": "...", "approved_at": "<iso>"} -- written by anything (Slack bot, Retool, a CLI, another asset).

    An approval is bound to WHAT was approved: the pending record carries a `fingerprint` (hash of the step id + the input text +
    the summary the reviewer was shown) and its `approve_by_writing.json` is the token to write (`approve_pending()` does it). A
    token is honoured only while its fingerprint matches the current request, so re-running a partition whose input changed asks
    again instead of reusing an old yes. `bind_to` (default [text, summary]) picks what is fingerprinted; use [summary] when the
    source text is regenerated by a model on every run (otherwise each re-run would differ and need a new approval).

    Config: approval_dir, key (default: partition key or step id), summary (shown to the reviewer; default: a preview of the source),
    reuse: until_changed (default: honoured while the fingerprint matches) | once (consumed when a run passes the gate; every run
    needs a fresh approval) | always (any token approves every later run of the key, fingerprint ignored), max_age_hours (older
    token = expired; default no expiry), wait_seconds (default 0 = don't block), poll_seconds (5), on_timeout: reject | approve |
    pending (default reject -- the safe default), on_reject: stop | continue (default stop: raise, so nothing downstream runs).

    A token that exists but cannot be honoured (input changed / expired / consumed) is logged with the exact reason, the reason is
    written to the pending record (`state` + `reason`, shown by agent_review_queue), and approval is requested again. With
    on_timeout=reject (no person answered) the pending record stays in the queue marked `outcome: timeout_default_reject`, so it can
    still be approved as written for the next run."""
    materialized_at = _now_iso()
    source_id = step.get("source", _ROOT_SOURCE_ID)
    text = _get_source_text(state, source_id) if source_id else ""
    approval_dir = step.get("approval_dir")
    if not approval_dir:
        raise ValueError("approve needs `approval_dir:` (where approval tokens + the pending review queue live)")
    _validate_approve_options([step])
    reuse = step.get("reuse", "until_changed")
    max_age = float(step["max_age_hours"]) if step.get("max_age_hours") is not None else None
    key = _approval_key(step.get("key") or _ctx_of(state).get("partition_key") or step["id"])
    store = _ApprovalStore(approval_dir)
    token_path = store.path(f"{key}.json")
    pending_path = store.path("pending", f"{key}.json")
    wait_seconds = float(step.get("wait_seconds", 0))
    poll = max(float(step.get("poll_seconds", 5)), 0.05)
    on_timeout = step.get("on_timeout", "reject")
    if on_timeout not in ("reject", "approve", "pending"):
        raise ValueError("approve on_timeout must be reject | approve | pending")
    if store.is_uri:
        _pol_url(approval_dir)  # remote approvals are governed by policy.network.allow_hosts (the bucket name is the host)
    else:
        _pol_path(approval_dir, "read")
        _pol_path(pending_path, "write")
    summary = step.get("summary") or _clip(text, 600)
    fingerprint = _approval_fingerprint(step["id"], text, summary, step.get("bind_to"))

    def read_token() -> Optional[Dict[str, Any]]:
        if not store.exists(token_path):
            return None
        try:
            tok = json.loads(store.read_text(token_path))
            return tok if isinstance(tok, dict) and "approved" in tok else None
        except (OSError, json.JSONDecodeError):
            return None  # half-written token: treat as not there yet

    def judge() -> "tuple[Optional[Dict[str, Any]], str, Optional[str]]":
        tok = read_token()
        st, why = _approval_token_state(tok, fingerprint, reuse, max_age, store.modified(token_path) if tok else None)
        return tok, st, why

    tok, tstate, why = judge()
    waited = 0.0
    honoured = tok if tstate == "valid" else None
    if honoured is None:
        if tok is not None:
            context.log.warning(f"[approve:{step['id']}] existing token for {key!r} not honoured -- {why}; requesting approval again")
        expires = time.time() + wait_seconds
        store.write_text(pending_path, json.dumps({
            "step": step["id"], "key": key, "requested_at": materialized_at, "fingerprint": fingerprint,
            "state": tstate if tok is not None else "new", "reason": why,
            "reuse": reuse, "max_age_hours": max_age,
            "expires_at": datetime.fromtimestamp(expires, tz=timezone.utc).isoformat() if wait_seconds else None,
            "on_timeout": on_timeout, "summary": summary, "preview": _clip(text, 2000),
            "approve_by_writing": {"path": store.shown(f"{key}.json"),
                                   "json": {"approved": True, "approver": "<you>", "reason": "<why>",
                                            "fingerprint": fingerprint, "approved_at": "<iso timestamp>"}},
        }, indent=2))
        context.log.info(f"[approve:{step['id']}] pending -> {store.shown('pending', key + '.json')}; waiting up to {wait_seconds:g}s")
        t0 = time.time()
        while honoured is None and time.time() - t0 < wait_seconds:
            time.sleep(min(poll, max(wait_seconds - (time.time() - t0), 0.01)))
            tok, tstate, why = judge()
            honoured = tok if tstate == "valid" else None
        waited = round(time.time() - t0, 2)
    tok = honoured
    status, approved = None, None
    if tok is not None:
        approved = bool(tok.get("approved"))
        status = "approved" if approved else "rejected"
    elif on_timeout == "approve":
        approved, status = True, "timeout_default_approve"
    elif on_timeout == "reject":
        approved, status = False, "timeout_default_reject"
    else:
        approved, status = None, "pending"
    if tok is not None or on_timeout == "approve":
        store.remove(pending_path)  # decided: leaves the review queue (the token stays as the audit record)
    elif on_timeout == "reject":
        # Defaulted to "do not proceed", not decided by a person: the request STAYS in the queue (marked) so a reviewer can still
        # approve it as written and the next run goes through -- removing it would leave nothing to approve.
        try:
            rec = json.loads(store.read_text(pending_path))
            store.write_text(pending_path, json.dumps({**rec, "outcome": "timeout_default_reject", "decided_at": _now_iso()}, indent=2))
        except (OSError, json.JSONDecodeError):
            pass
    if tok is not None and reuse == "once":
        # Consume exactly the decision we honoured: re-read first so a fresh token a reviewer wrote meanwhile is never marked used.
        cur = read_token()
        if cur is not None and cur.get("fingerprint") == tok.get("fingerprint") and cur.get("approved_at") == tok.get("approved_at") \
                and not cur.get("consumed_at"):
            store.write_text(token_path, json.dumps({**cur, "consumed_at": _now_iso(), "consumed_by_step": step["id"]}, indent=2))
    if approved is False and step.get("on_reject", "stop") == "stop":
        detail = (tok or {}).get("reason") or (why if tok is None and why else None) or "not approved"
        raise RuntimeError(f"[approve:{step['id']}] {status}: {detail} -- stopping the pipeline here")
    return {
        "text": text if approved else "", "op": "approve", "approved": approved, "status": status,
        "approver": (tok or {}).get("approver"), "approval_reason": (tok or {}).get("reason"), "waited_seconds": waited,
        "approval_key": key, "approval_fingerprint": fingerprint, "approval_not_honoured_reason": None if tok is not None else why,
        "materialized_at": materialized_at, "n_llm_calls": 0,
    }


# ── `when:` guard + per-step policy narrowing ────────────────────────────────

def _when_ok(when: Dict[str, Any], state: Dict[str, Any]) -> "tuple[bool, str]":
    """Evaluate a step's `when:` guard. {source: <step id>, field: <result key, default: its text>, <predicate>, not: bool}
    predicates: equals | contains | regex | in: [..] | gt | gte | lt | lte | truthy | falsy. Returns (run?, why)."""
    if not isinstance(when, dict) or "source" not in when:
        raise ValueError("`when:` needs `source: <step id>` plus one predicate (equals/contains/regex/in/gt/gte/lt/lte/truthy/falsy)")
    sid = when["source"]
    if sid not in state:
        raise ValueError(f"when.source {sid!r} is not a prior step; known: {[k for k in state if not str(k).startswith('__')]}")
    entry = state[sid]
    field = when.get("field")
    val = (entry.get(field) if isinstance(entry, dict) else None) if field else (entry.get("text", "") if isinstance(entry, dict) else entry)
    sval = "" if val is None else str(val)
    if "equals" in when:
        hit = sval.strip().lower() == str(when["equals"]).strip().lower()
    elif "in" in when:
        hit = sval.strip().lower() in [str(x).strip().lower() for x in when["in"]]
    elif "contains" in when:
        hit = str(when["contains"]).lower() in sval.lower()
    elif "regex" in when:
        hit = re.search(when["regex"], sval, re.I) is not None
    elif "truthy" in when:
        hit = bool(val) is bool(when["truthy"])
    elif "falsy" in when:
        hit = (not val) is bool(when["falsy"])
    elif any(k in when for k in ("gt", "gte", "lt", "lte")):
        try:
            x = float(val)
        except (TypeError, ValueError):
            hit = False
        else:
            hit = all([("gt" not in when or x > float(when["gt"])), ("gte" not in when or x >= float(when["gte"])),
                       ("lt" not in when or x < float(when["lt"])), ("lte" not in when or x <= float(when["lte"]))])
    else:
        raise ValueError("`when:` needs a predicate: equals, in, contains, regex, truthy, falsy, gt/gte/lt/lte")
    if when.get("not"):
        hit = not hit
    return hit, f"when {sid}{('.' + field) if field else ''} -> {sval[:80]!r}"


_OPS = {
    "llm_call": _do_llm_call,
    "route": _do_route,
    "conditional_route": _do_conditional_route,
    "debate": _do_debate,
    "critique_loop": _do_critique_loop,
    "synthesize": _do_synthesize,
    "mcp_call": _do_mcp_call,
    "tool_use_loop": _do_tool_use_loop,
    "handoff": _do_handoff,
    "map": _do_map,
    "extract": _do_extract,
    "classify": _do_classify,
    "reduce": _do_reduce,
    "self_reflect": _do_self_reflect,
    "sub_pipeline": _do_sub_pipeline,
    "agent_call": _do_agent_call,
    "delegate": _do_delegate,
    "invoke_component": _do_invoke_component,
    "verify": _do_verify,
    "eval": _do_eval,
    "best_of_n": _do_best_of_n,
    "cascade": _do_cascade,
    "plan": _do_plan,
    "decide": _do_decide,
    "approve": _do_approve,
}


def _build_step_metadata(
    aid: str, entry: Any, partition_key: Optional[str]
) -> Dict[str, Any]:
    """Shared metadata builder for both the single-op multi_asset path
    and the per-step-ops graph_multi_asset path — same fields, same
    Insights-friendly shape, same op-specific rich blobs. Keys are
    prefixed with `{aid}__` so a multi-asset materialization event
    surfaces per-asset fields without collision."""
    import dagster as dg  # local import so this helper is safe to call
    md: Dict[str, Any] = {}
    if not isinstance(entry, dict):
        return md
    text = entry.get("text", "")
    md[f"{aid}__text"] = dg.MetadataValue.md(text[:2000] if text else "_(empty)_")
    if entry.get("cost_usd") is not None:
        md[f"{aid}__cost_usd"] = dg.MetadataValue.float(float(entry["cost_usd"]))
    if entry.get("latency_ms") is not None:
        md[f"{aid}__latency_ms"] = dg.MetadataValue.int(int(entry["latency_ms"]))
    if entry.get("tokens_total") is not None:
        md[f"{aid}__tokens_total"] = dg.MetadataValue.int(int(entry["tokens_total"]))
    if entry.get("n_llm_calls") is not None:
        md[f"{aid}__n_llm_calls"] = dg.MetadataValue.int(int(entry["n_llm_calls"]))
    if entry.get("model_fingerprint"):
        md[f"{aid}__model_fingerprint"] = dg.MetadataValue.text(str(entry["model_fingerprint"]))
    if entry.get("materialized_at"):
        try:
            md[f"{aid}__materialized_at"] = dg.MetadataValue.timestamp(
                datetime.fromisoformat(entry["materialized_at"])
            )
        except (TypeError, ValueError):
            md[f"{aid}__materialized_at"] = dg.MetadataValue.text(str(entry["materialized_at"]))
    if entry.get("op"):
        md[f"{aid}__op"] = dg.MetadataValue.text(str(entry["op"]))
    if partition_key is not None:
        md[f"{aid}__partition_key"] = dg.MetadataValue.text(str(partition_key))
    for k, v in entry.items():
        if k in ("text", "cost_usd", "latency_ms", "tokens_total",
                 "n_llm_calls", "model_fingerprint", "materialized_at", "op"):
            continue
        if k == "usage" and v is not None:
            md[f"{aid}__usage"] = dg.MetadataValue.json(v)
        elif k == "all_proposals" and v is not None:
            md[f"{aid}__proposals"] = dg.MetadataValue.json(v)
        elif k == "history" and v is not None:
            md[f"{aid}__history"] = dg.MetadataValue.json(v)
        elif k == "reasoning" and v:
            md[f"{aid}__reasoning"] = dg.MetadataValue.md(
                "\n\n---\n\n".join(f"**{r['model']}**\n\n{r['text']}" for r in v)[:20000]
            )
        elif k in ("policy_usage", "policy_violation_detail") and v:
            md[f"{aid}__{k}"] = dg.MetadataValue.json(v)
        elif k in _JSON_META_KEYS and v:
            md[f"{aid}__{k}"] = dg.MetadataValue.json(v)
        elif k == "tool_call_trace" and v is not None:
            md[f"{aid}__tool_call_trace"] = dg.MetadataValue.json(v)
        elif isinstance(v, (str, int, float, bool)):
            md[f"{aid}__{k}"] = dg.MetadataValue.text(str(v))
    return md


def _default_router_prompt(specialists: List[dict]) -> str:
    lines = [
        "You are a router. Read the user's input and call exactly ONE of the tools below — the one whose description best matches the input.",
        "",
        "Available specialists:",
    ]
    for s in specialists:
        lines.append(f"  - {s['name']}: {s['description']}")
    lines.append("")
    lines.append("Call the chosen specialist's tool. Do not answer the user's question yourself.")
    return "\n".join(lines)


def _deep_apply_ctx_substitutions(obj: Any, partition_key: Optional[str], partition_fields: Optional[Dict[str, str]]) -> Any:
    """Recursively substitute `{partition_key}` + `{partition.<name>}` in every
    string leaf of a step dict / list / scalar. Non-strings pass through
    unchanged. Port substitution (`{port_name}`) is NOT done here — it
    happens per-op after inputs are resolved."""
    if isinstance(obj, str):
        return _apply_ctx_substitutions(obj, partition_key, partition_fields)
    if isinstance(obj, list):
        return [_deep_apply_ctx_substitutions(x, partition_key, partition_fields) for x in obj]
    if isinstance(obj, dict):
        return {k: _deep_apply_ctx_substitutions(v, partition_key, partition_fields) for k, v in obj.items()}
    return obj


# ── Personas resolution ──────────────────────────────────────────────
#
# `personas:` is a top-level named-reusable-config block. Steps and
# sub-configs reference a persona by name (`persona: <name>`); the
# persona's fields are merged in at dispatch time. Inline fields on the
# step always win over persona-provided fields — the persona is a
# defaults-provider, not an override.
#
# Persona reference sites (recursive traversal):
#   step level: llm_call / classify / extract / reduce / self_reflect /
#               map / tool_use_loop
#   sub-config: route.router, route.specialists[*],
#               conditional_route.specialists[*],
#               debate.proposers[*], debate.arbitrator,
#               critique_loop.drafter, critique_loop.critic
#
# The bundle fields (all optional): model, api_key_env_var,
# api_base_env_var, system_prompt, temperature, max_tokens,
# reasoning_effort, thinking_budget, prompt_caching.

_PERSONA_FIELDS = (
    "model", "api_key_env_var", "api_base_env_var",
    "system_prompt", "temperature", "max_tokens",
    "reasoning_effort", "thinking_budget", "prompt_caching",
)


def _merge_persona(target: dict, persona: dict) -> dict:
    """Merge persona fields into `target`. Explicit inline fields on
    `target` win — persona is a defaults-provider. Non-persona fields on
    the persona bundle (e.g. accidentally-declared tools) are silently
    ignored so users can't stuff arbitrary things into a persona and
    have them leak into unrelated sub-configs."""
    merged = dict(target)
    for k in _PERSONA_FIELDS:
        if k in persona and merged.get(k) is None:
            merged[k] = persona[k]
    merged.pop("persona", None)  # consumed
    return merged


def _resolve_persona(node: Any, personas: Optional[Dict[str, Dict[str, Any]]]) -> Any:
    """Recursively walk a step dict, expanding `persona: <name>` references.
    Dicts and lists are traversed; scalars pass through."""
    if not personas:
        return node
    if isinstance(node, list):
        return [_resolve_persona(item, personas) for item in node]
    if not isinstance(node, dict):
        return node
    # Recurse first so nested `persona:` refs are handled at every level.
    walked = {k: _resolve_persona(v, personas) for k, v in node.items()}
    persona_name = walked.get("persona")
    if isinstance(persona_name, str):
        persona = personas.get(persona_name)
        if persona is None:
            raise ValueError(
                f"persona {persona_name!r} referenced but not declared in top-level "
                f"`personas:` block. Available: {sorted(personas.keys())}"
            )
        return _merge_persona(walked, persona)
    return walked


def _run_step(step: dict, state: Dict[str, Any], context) -> None:
    op = step.get("op")
    step_id = step.get("id")
    if not step_id:
        raise ValueError(f"every step must have an `id`; got {step!r}")
    if op not in _OPS:
        raise ValueError(f"unknown op: {op!r}. valid: {sorted(_OPS.keys())}")

    # Apply run-context substitutions ({partition_key}, {partition.<name>}) to
    # every string in the step dict before dispatch. Port substitution
    # (`{port_name}`) still happens per-op since it needs resolved inputs.
    _ctx = _ctx_of(state)
    step = _deep_apply_ctx_substitutions(step, _ctx.get("partition_key"), _ctx.get("partition_fields"))

    # Expand `persona: <name>` references — step-level AND every sub-config.
    # Personas live in the reserved `__personas__` state entry, put there by
    # the compute fn before it starts running steps.
    personas = state.get("__personas__")
    if personas:
        step = _resolve_persona(step, personas)

    when = step.get("when")
    if when:
        run_it, why = _when_ok(when, state)
        if not run_it:
            try:
                passthrough = _get_source_text(state, step.get("source", _ROOT_SOURCE_ID))
            except ValueError:
                passthrough = ""
            context.log.info(f"[step {step_id!r}] op={op!r} SKIPPED ({why})")
            state[step_id] = {"text": passthrough, "op": op, "skipped": True, "skip_reason": why,
                              "n_llm_calls": 0, "materialized_at": _now_iso()}
            return

    context.log.info(f"[step {step_id!r}] op={op!r}")

    # Agent policy (optional): installed for the duration of this step. Usage counters live in `state` (plain
    # dict) so budgets accumulate across steps -- including per-step ops, where state is passed between ops.
    policy_cfg = state.get("__policy__")
    step_policy_cfg = step.get("policy")
    p_token = None
    n_before = 0
    if policy_cfg or step_policy_cfg:
        _active = _pol()  # an enclosing scope (ingest/sinks body) already owns the run's counters: share them
        usage = state.setdefault("__policy_usage__", _active.usage if _active is not None else {})
        policy = _Policy(policy_cfg, usage) if policy_cfg else None
        if step_policy_cfg:
            # A step can only be MORE restricted than the pipeline: every check passes the pipeline policy first.
            policy = _Policy(_validate_policy(step_policy_cfg), usage, parent=policy, scoped=True)
        n_before = len(usage["violations"])
        p_token = _ACTIVE_POLICY.set(policy)
    r_token = _REASONING_TRACE.set([])
    try:
        result = _OPS[op](step, state, context)
        trace = _REASONING_TRACE.get()
    finally:
        _REASONING_TRACE.reset(r_token)
        if p_token is not None:
            _ACTIVE_POLICY.reset(p_token)
    if isinstance(result, dict):
        if trace:
            result["reasoning"] = trace
            result["reasoning_chars"] = sum(len(r["text"]) for r in trace)
        if policy_cfg or step_policy_cfg:
            u = state["__policy_usage__"]
            result["policy_violations"] = len(u["violations"]) - n_before
            result["policy_usage"] = {k: (round(u[k], 6) if k == "cost_usd" else u[k]) for k in ("llm_calls", "tool_calls", "tokens", "cost_usd")}
            if len(u["violations"]) > n_before:
                result["policy_violation_detail"] = u["violations"][n_before:]
    state[step_id] = result


# ── Component class ──────────────────────────────────────────────────


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


class AgenticPipelineComponent(dg.Component, dg.Model, dg.Resolvable):
    """Standardized agentic pipeline — one YAML, one asset (with named outputs).

    Same "single component, `steps:` list, multiple outputs" shape as the
    other pipeline components (polars_pipeline, warehouse_pipeline,
    pyspark_pipeline, snowpark_pipeline, ml_pipeline).

    9 ops:
      - llm_call:          single LLM call over source text
      - route:             router picks best specialist; specialist answers
      - conditional_route: deterministic branching (regex/contains/equals/
                           jsonpath) picks specialist; no router LLM
      - debate:            N proposers → arbitrator picks winner
      - critique_loop:     drafter → critic → drafter, N iterations;
                           optional `until_score_gte:` stops early on score
      - synthesize:        merge multiple upstream step texts into one
      - mcp_call:          direct MCP tool call (stdio/http/sse/fastmcp),
                           no LLM; `{text}` + `{port_name}` +
                           `{partition.<name>}` substitution in tool_args
      - tool_use_loop:     open-ended LLM+MCP tool loop, bounded by
                           max_iterations, one asset with full trajectory
      - handoff:           hand off to user-provided callable (LangGraph /
                           AutoGen / CrewAI / DSPy)
      - delegate:          dynamic sibling of route/agent_call -- an LLM
                           picks the best-matching agent from a DISCOVERED
                           pool (sibling AgentCardComponent instances +
                           optional external manifest) instead of a fixed
                           inline list, then invokes it over MCP or HTTP

    (Also implemented, not yet listed above: map, extract, classify, reduce,
    self_reflect, sub_pipeline, agent_call -- see the `_OPS` dispatch table
    for the authoritative, current op list.)

    Ops share YAML idioms with the other pipeline components:
      - `id:` names the step output for downstream reference
      - `source: <step_id>` picks upstream text (default = the pipeline's
        original input, not "whatever step ran last" -- see `_ROOT_SOURCE_ID`)
      - `outputs.assets: [step_ids]` picks which step outputs become assets
      - `outputs.text_sinks: [{from, path}]` writes text side files
      - `outputs.json_sinks: [{from, path}]` dumps full step output as JSON
    """

    asset_name_prefix: str = Field(
        description="Prefix for emitted asset names. Each step in outputs.assets becomes '{prefix}_{step_id}'.",
    )
    source: Dict[str, Any] = Field(
        description=(
            "Data source. Shapes: "
            "{kind: literal, text: '...'} | "
            "{kind: file, path: '...'} | "
            "{kind: url, url: '...'} | "
            "{kind: database, query: 'SELECT ...', database_url: '...' or database_url_env_var: '...', row_limit: 500} "
            "(query result rendered as a markdown table) | "
            "{kind: upstream_asset, upstream_asset_key: '...'}. "
            "All string fields are {partition_key}-templated."
        ),
    )
    steps: List[Dict[str, Any]] = Field(
        description=(
            "Ordered pipeline steps. Each step: {id, op, ...op-specific args}. "
            "\n\nTwo wiring modes (choose per step, they compose):\n"
            "  1. **Legacy single-source**: `source: <step_id>` reads that step's "
            "text into `{text}` in the prompt (default, when omitted: the pipeline's "
            "original input -- NOT whatever step ran last. Dependency structure is "
            "always declared, never implied by list position, so omitting `source:` "
            "on several steps makes them independent siblings off the same root, not "
            "an implicit chain). Reserved id `source` = initial pipeline source, "
            "explicit or implicit.\n"
            "  2. **Typed named inputs** (recommended for joins): "
            "`inputs: {<port_name>: {from: <step_id>} | {literal: <value>}}`. "
            "Each port becomes a `{<port_name>}` placeholder in `prompt_template` "
            "AND `system_prompt` (and, for `mcp_call`, in string `tool_args`). "
            "Any step can join from any number of prior steps by port name — the "
            "shape common in agentic-orchestration graphs (fan-out → typed-join).\n\n"
            "8 ops. LLM ops (llm_call/route/debate/critique_loop/synthesize) all "
            "support optional `max_tokens`, `temperature`, `system_prompt`, "
            "`prompt_template`:\n"
            "  - **llm_call**: {model, api_key_env_var}. One LLM call. Supports "
            "both `source:` and `inputs:` for multi-input joins.\n"
            "  - **route**: {router: {model, api_key_env_var}, specialists: [{name, "
            "description, model, api_key_env_var, system_prompt}], fallback: name}. "
            "Router picks specialist, specialist answers.\n"
            "  - **debate**: {proposers: [{model, api_key_env_var, system_prompt}], "
            "arbitrator: {model, api_key_env_var, system_prompt}}. N proposers, "
            "arbitrator picks winner.\n"
            "  - **critique_loop**: {drafter: {model, api_key_env_var, system_prompt}, "
            "critic: {model, api_key_env_var, system_prompt}, iterations: int}. "
            "Drafter → critic → drafter, N iterations.\n"
            "  - **synthesize**: {model, api_key_env_var, sources: [<step_ids>] | "
            "inputs: {port: {from: id}}}. Merge multiple upstream step outputs. "
            "Prefer `inputs:` for named typed joins (Prefect-style execution-plan "
            "shape); `sources:` for positional legacy shape.\n"
            "  - **mcp_call**: {server: {name, type: stdio|http|sse|fastmcp, "
            "command|url, env|headers|headers_env}, mcp_tool_name, tool_args, "
            "parse_as: auto|json|text}. Direct MCP tool call (no LLM); "
            "string `tool_args` support `{text}` substitution against source "
            "AND `{port_name}` substitution from `inputs:`.\n"
            "  - **tool_use_loop**: {model, api_key_env_var, mcp_servers: "
            "[...], max_iterations, [system_prompt, allowed_tools, "
            "finalize_tool_name, temperature, max_tokens]}. Open-ended "
            "tool-use loop — LLM sees MCP tools, picks one, tool runs, "
            "LLM sees result, picks next tool, etc. Bounded by "
            "max_iterations OR the LLM calling the synthetic "
            "`finalize` tool with its final answer. The shape LangGraph "
            "is known for, done as ONE Dagster asset with the final "
            "answer as `text` + full tool-call trajectory in metadata. "
            "Cost/latency/tokens roll up across every internal call."
        ),
    )
    outputs: Dict[str, Any] = Field(
        description=(
            "Output declaration. Shape: "
            "{assets: [<step_ids>], text_sinks: [{from, path}], json_sinks: [{from, path}]}. "
            "`assets:` step outputs become first-class Dagster assets; "
            "`text_sinks:` writes step text to disk; `json_sinks:` writes full step dict."
        ),
    )
    policy: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Guardrails on what this pipeline will do on the agent's behalf; omitted = no restrictions. Sections "
            "(all optional): `on_violation: fail|warn` (default fail); `env.allow: [NAME, 'PREFIX_*']` (env vars the "
            "pipeline may read, and the ONLY ones -- plus a minimal PATH/HOME baseline -- handed to MCP stdio "
            "subprocesses); `filesystem.read` / `filesystem.write: [paths]` (allowed roots for file sources and "
            "text/json sinks); `network.allow_hosts: [host, '*.corp.com']` (sources, MCP http/sse servers, remote "
            "agents, LLM api_base); `tools.allow` / `tools.deny: [patterns]` (the model is only shown permitted "
            "tools), `tools.max_calls_per_run: N`, `tools.arg_rules: [{tool, arg, under|hosts|allow_regex|deny_regex}]` "
            "(refuse calls whose arguments leave the sandbox: e.g. a path outside ./data); `components.allow: "
            "[ClassName]` (invoke_component targets); `budget.max_cost_usd|max_tokens|max_llm_calls|max_seconds`. "
            "Denied tool calls are returned to the model as an error so it can adapt; other violations raise "
            "PolicyViolation (or just log under on_violation: warn). Violations and usage are recorded as step "
            "metadata. NOTE: this is enforcement at the component boundary, not an OS sandbox -- an MCP stdio "
            "server still runs with the worker's OS permissions."
        ),
    )
    personas: Optional[Dict[str, Dict[str, Any]]] = Field(
        default=None,
        description=(
            "Named reusable LLM sub-configs. Each persona bundles "
            "`{model, api_key_env_var, api_base_env_var, system_prompt, "
            "temperature, max_tokens, reasoning_effort, thinking_budget}`. "
            "Reference from any step / sub-config via `persona: <name>`; "
            "declared fields on the persona are merged into the step's "
            "sub-config (explicit inline fields win). Applies to: step-level "
            "(llm_call, classify, extract, reduce, self_reflect, map, "
            "tool_use_loop) and sub-configs (route.router, "
            "route.specialists[*], debate.proposers[*], debate.arbitrator, "
            "critique_loop.drafter, critique_loop.critic, conditional_route."
            "specialists[*])."
        ),
    )
    agents: Optional[Dict[str, Dict[str, Any]]] = Field(
        default=None,
        description=(
            "Named pre-built agents. Each entry declares `kind:` plus "
            "kind-specific connection config. Reference from a step via "
            "`op: agent_call, agent: <name>`. Supported kinds: "
            "`openai_assistant` (assistant_id + api_key_env_var — creates "
            "thread + message + run + polls), `remote_agent` (arbitrary "
            "HTTP endpoint — bearer / headers auth, POST / GET, sync or "
            "async polling, JSON-path response extraction), `handoff` "
            "(user-provided Python callable — same shape as the `handoff` "
            "op). Use to unify tier-3 pre-built agents behind a stable "
            "step-level interface: swap where an agent lives (OpenAI → "
            "self-hosted → Vercel) by editing the `agents:` block; every "
            "step referencing that agent keeps working."
        ),
    )
    group_name: Optional[str] = Field(default="agents", description="Group name for emitted assets.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds. Default: ['llm', 'agent', 'pipeline'].")
    tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Additional tags on emitted assets.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    description: Optional[str] = Field(default=None, description="Description on emitted assets.")

    # ── Per-step ops (opt-in; changes the shape of the emitted job) ────
    #
    # When True, the pipeline is emitted as a `@dg.graph_multi_asset` with
    # one @op per step (plus an ingest op and an extract op) instead of a
    # single @multi_asset with one op. Trade-offs:
    #
    #   default (False, single @multi_asset):
    #     - One op per RUN in the Runs page → coarse-grained retry
    #       (if step 6 fails, the whole partition restarts)
    #     - Zero IO-manager overhead between steps (in-memory state dict)
    #
    #   per_step_ops=True (@dg.graph_multi_asset):
    #     - One op PER STEP visible in the Runs page → finer-grained retry
    #       (Dagster's native re-execution can restart from any failed op)
    #     - Each step's state dict passes through the IO manager (default
    #       pickle to filesystem). ~50-200KB per hop for text-heavy
    #       pipelines. Real but not painful.
    #
    # Also enables `can_subset` on the emitted graph — you can materialize
    # just a subset of the declared output assets without running the
    # whole pipeline.
    per_step_ops: bool = Field(
        default=False,
        description=(
            "When True, emit as a `@dg.graph_multi_asset` with one @op per "
            "step (visible in the Runs page + finer-grained retry). State "
            "dict flows through the IO manager between ops. Default False "
            "keeps the single-op @multi_asset shape (coarse retry, no IO "
            "overhead)."
        ),
    )

    # ── Partitioning (first-class fields, matching sibling components) ──
    # All emitted assets share a partitions_def built from these fields.
    # Use `{partition_key}` in source text / URL / file path / sink paths
    # to substitute per partition at compute time.
    partition_type: Optional[str] = Field(
        default=None,
        description=(
            "Partition type: 'daily' | 'weekly' | 'monthly' | 'hourly' | "
            "'static' | 'dynamic' | 'multi' | None (unpartitioned)."
        ),
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
        description="ISO date for time-based partition types (daily/weekly/monthly/hourly/multi).",
    )
    partition_values: Optional[Union[str, List[str]]] = Field(
        default=None,
        description=(
            "Comma-separated string OR list — the fixed partition keys "
            "for static/multi partitioning. e.g. ['NVDA', 'TSLA', 'META'] "
            "or 'NVDA,TSLA,META'."
        ),
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition when partition_type='dynamic'.",
    )
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description=(
            "Multi-axis partition spec: list of {name, type, start, values, "
            "dynamic_partition_name}. Set instead of partition_type for "
            "multi-dimensional partitioning."
        ),
    )
    backfill_policy: Optional[Literal["single_run", "multi_run"]] = Field(
        default=None,
        description=(
            "Backfill execution strategy for a partitioned pipeline. 'multi_run' launches one run per partition (the only "
            "shape this pipeline supports: each run resolves {partition_key}, sink paths and approval keys for exactly one "
            "partition). 'single_run' is REJECTED at load time because one run covering a partition range has no single "
            "partition_key. Leave unset for Dagster's default (one run per partition). Ignored when no partitioning is set."
        ),
    )
    backfill_policy_max_partitions_per_run: Optional[int] = Field(
        default=None,
        description="For backfill_policy='multi_run' only. Must be unset or 1: a run with several partitions has no single partition_key.",
    )
    partition_key_parser: Optional[str] = Field(
        default=None,
        description=(
            "Format template for parsing composite partition keys into named "
            "fields — e.g. '{owner}/{repo}#{issue_number}' means partition key "
            "'dagster-io/dagster#30000' → {owner: 'dagster-io', repo: 'dagster', "
            "issue_number: '30000'}. Each parsed field is then available as "
            "`{partition.<name>}` in tool_args, prompt_template, and "
            "system_prompt (in addition to the raw `{partition_key}`). "
            "Pair with PartitionedAssetLauncherJobComponent for the "
            "config-driven-partition pattern."
        ),
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('kinds', 'owners'), csv=(), tags=('tags',))

    @classmethod
    def get_form_config(cls):
        """Register as an App Managed Component so the Dagster / Dagster+
        UI can create + edit instances of this pipeline via a
        schema-driven form. Requires `dagster>=1.13.8` and the
        `flagComponentInstanceUI` feature flag."""
        from dagster.components.resolved.form_config import ComponentFormConfig

        return ComponentFormConfig(label="Agentic Pipeline", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _validate_policy(self.policy)  # typos in a security config must fail loudly, not silently allow everything
        prefix = self.asset_name_prefix
        steps = list(self.steps)
        source_config = dict(self.source)
        outputs = dict(self.outputs)
        asset_ids: List[str] = list(outputs.get("assets", []))
        text_sinks: List[Dict[str, Any]] = list(outputs.get("text_sinks", []) or [])
        json_sinks: List[Dict[str, Any]] = list(outputs.get("json_sinks", []) or [])
        partition_key_parser = self.partition_key_parser

        # `delegate` steps need AgentCardComponent siblings discovered once,
        # here, at build time -- context.build_defs() only exists on this
        # ComponentLoadContext, not on the runtime AssetExecutionContext the
        # pipeline actually executes with. Only pay for the scan if a
        # delegate step actually wants it (same laziness as
        # EnhancedDataQualityChecks, which only scans siblings when its own
        # `selections` config is set).
        discovered_sibling_cards: List[Dict[str, Any]] = []
        if any(
            s.get("op") == "delegate" and (s.get("registry") or {}).get("discover_siblings", True)
            for s in steps
        ):
            discovered_sibling_cards = _discover_sibling_agent_cards(context)

        if not asset_ids:
            raise ValueError("outputs.assets must list at least one step id.")

        group_name = self.group_name or "agents"
        base_kinds = list(self.kinds or ["llm", "agent"])
        base_tags = dict(self.tags or {})

        # Map step id → op so we can add the op name to that asset's kinds.
        # Router → kinds: [..., 'router'] etc. Lets users filter the asset
        # catalog by "show me every debate step" or "every route step".
        op_by_id = {s.get("id"): s.get("op") for s in steps if s.get("id") and s.get("op")}

        outs = {}
        for aid in asset_ids:
            per_asset_tags = dict(base_tags)
            for k in base_kinds:
                per_asset_tags[f"dagster/kind/{k}"] = ""
            op = op_by_id.get(aid)
            if op:
                per_asset_tags[f"dagster/kind/{op}"] = ""
            outs[f"{prefix}_{aid}"] = dg.AssetOut(
                code_version=_code_version({
                    "step": next((st for st in steps if st.get("id") == aid), None),
                    "personas": self.personas, "policy": self.policy,
                }),
                group_name=group_name,
                description=self.description,
                owners=self.owners or None,
                tags=per_asset_tags,
                # is_required=False lets can_subset=True omit outputs when
                # the caller materializes a strict subset. When ALL outputs
                # are selected (the default UX), every emission still fires
                # and Dagster verifies completeness — same as before.
                is_required=False,
            )

        ins: Dict[str, dg.AssetIn] = {}
        if source_config.get("kind") == "upstream_asset":
            ins["source"] = dg.AssetIn(
                key=dg.AssetKey.from_user_string(source_config["upstream_asset_key"])
            )

        # ── Internal asset deps — the lineage inside the multi_asset ──
        # The compute function reads step-to-step data via the pipeline's
        # in-memory state dict. But Dagster's asset graph won't SHOW the
        # dependencies between the emitted assets unless we declare
        # `internal_asset_deps` explicitly — otherwise every emitted
        # asset renders as an orphan node.
        #
        # For each emitted step, collect:
        # 1. Every upstream STEP id (from `source:` / `sources:` / typed
        #    `inputs:` `{from: <id>}` refs) that is also in `outputs.assets`.
        # 2. If the step reads from the reserved id `source` (or omits
        #    source and thus defaults to the initial pipeline source)
        #    AND the pipeline source is `upstream_asset`, include the
        #    upstream_asset_key as a parent — otherwise the emitted asset
        #    would show as disconnected from the pipeline's real input.
        emitted_set = set(asset_ids)
        step_by_id = {s.get("id"): s for s in steps if s.get("id")}
        # Reserved id `source` refers to the initial pipeline source.
        # When the source is `upstream_asset`, treat that reserved id as
        # a stand-in for the real upstream asset key.
        _source_upstream_key = (
            source_config["upstream_asset_key"]
            if source_config.get("kind") == "upstream_asset"
            else None
        )
        def _upstream_refs_for_step(step: dict) -> List[str]:
            """Return list of upstream refs (step ids OR the reserved 'source').

            Only includes explicit refs — the omitted-source fallback is
            handled by callers so they can decide how to resolve it.
            """
            ups: List[str] = []
            src = step.get("source")
            if isinstance(src, str):
                ups.append(src)
            for sid in (step.get("sources") or []):
                if isinstance(sid, str):
                    ups.append(sid)
            for _spec in (step.get("inputs") or {}).values():
                if isinstance(_spec, dict) and "from" in _spec:
                    _f = _spec["from"]
                    if isinstance(_f, str):
                        ups.append(_f)
            return ups

        def _resolve_step_parents(step_id: str, seen: Optional[set] = None) -> set:
            """Return the set of AssetKeys that are true asset-graph parents
            of the given emitted step. Walks upstream, skipping over any
            non-emitted intermediate step so lineage stays coherent.
            """
            if seen is None:
                seen = set()
            if step_id in seen:
                return set()
            seen.add(step_id)
            step = step_by_id.get(step_id) or {}
            parents: set = set()
            refs = _upstream_refs_for_step(step)
            # If the step has typed `inputs:` OR `sources:` OR an explicit
            # `source:`, those are authoritative. Otherwise it defaults to
            # the pipeline's root input (_ROOT_SOURCE_ID) — dependency
            # structure is never implied by list position, so a step with
            # no explicit ref is an independent branch off the root, not an
            # implicit continuation of whatever step precedes it.
            if not refs:
                refs = [_ROOT_SOURCE_ID]
            for ref in refs:
                if ref == _ROOT_SOURCE_ID:
                    if _source_upstream_key:
                        parents.add(dg.AssetKey.from_user_string(_source_upstream_key))
                elif ref in emitted_set:
                    parents.add(dg.AssetKey([f"{prefix}_{ref}"]))
                elif ref in step_by_id:
                    # Non-emitted intermediate step — inherit its parents.
                    parents.update(_resolve_step_parents(ref, seen))
                # else: unknown ref — ignore silently (compute will error)
            return parents

        internal_asset_deps: Dict[str, set] = {}
        for aid in asset_ids:
            _parents = _resolve_step_parents(aid)
            if _parents:
                internal_asset_deps[f"{prefix}_{aid}"] = _parents

        # ── Partitions_def from first-class fields ──
        # Mirrors HumanApprovalGateComponent's shape. All emitted assets
        # share one partitions_def (they come from ONE multi_asset).
        _partitions_def = _build_partitions_def(
            self.partition_type,
            self.partition_start,
            self.partition_values,
            self.dynamic_partition_name,
            self.partition_dimensions,
        
    cron_schedule=self.partition_cron_schedule,
    cron_fmt=self.partition_cron_fmt or "%Y-%m-%d",
)

        _backfill_policy = _build_backfill_policy(
            _partitions_def, self.backfill_policy, self.backfill_policy_max_partitions_per_run)
        _validate_approve_partitioning(steps, _partitions_def is not None)
        _validate_approve_options(steps)
        _validate_source_partitioning(source_config, steps, _partitions_def)

        # ── Branch: per_step_ops selects the emitted shape ────────────
        # (True → graph_multi_asset with N+2 ops; False → single multi_asset)
        if self.per_step_ops:
            return self._build_per_step_ops_defs(
                prefix=prefix, steps=steps, source_config=source_config,
                asset_ids=asset_ids, text_sinks=text_sinks, json_sinks=json_sinks,
                outs=outs, ins=ins, internal_asset_deps=internal_asset_deps,
                partitions_def=_partitions_def,
                partition_key_parser=partition_key_parser,
                discovered_sibling_cards=discovered_sibling_cards,
                backfill_policy=_backfill_policy,
            )

        # ── Step-dep DAG for can_subset resume ──
        # For any step_id, resolve the transitive-upstream set of OTHER step
        # ids that must have run before it. Used at runtime when the caller
        # subset-materializes a subset of outputs — we run only the needed
        # steps, not the whole pipeline.
        def _direct_step_deps(step: dict) -> List[str]:
            """Step ids this step needs before it can run (excluding 'source').
            A step with no explicit source/sources/inputs has no step-level
            dependency — it reads the root input, same as `_resolve_step_parents`."""
            refs = _upstream_refs_for_step(step)
            return [r for r in refs if r != _ROOT_SOURCE_ID and r in step_by_id]

        _step_direct_deps: Dict[str, List[str]] = {
            s["id"]: _direct_step_deps(s) for s in steps if s.get("id")
        }

        def _closure(target_ids: List[str]) -> List[str]:
            """Return step ids to run, in original `steps:` order, so that
            every target_id has its upstream deps materialized first."""
            need: set = set()
            stack: List[str] = list(target_ids)
            while stack:
                sid = stack.pop()
                if sid in need:
                    continue
                need.add(sid)
                for u in _step_direct_deps.get(sid, []):
                    if u not in need:
                        stack.append(u)
            # Preserve original declared order — steps are already
            # topologically ordered because `_run_step` requires upstreams
            # to be in `state` before dispatch.
            return [s["id"] for s in steps if s.get("id") in need]

        def _pipeline_impl(context: dg.AssetExecutionContext, **kwargs):
            _require_one_partition(context, _partitions_def is not None)
            partition_key = context.partition_key if context.has_partition_key else None
            if partition_key:
                context.log.info(f"partition-aware materialization: partition_key={partition_key!r}")

            # can_subset: figure out which asset outputs were requested. When
            # the caller selects a strict subset, we run only the steps
            # transitively needed for those outputs — dbt-style per-step
            # resume without splitting into N @asset decorators.
            selected_output_names = set(context.selected_output_names)
            all_output_names = set(outs.keys())
            is_subset = 0 < len(selected_output_names) < len(all_output_names)
            selected_step_ids = (
                [aid for aid in asset_ids if f"{prefix}_{aid}" in selected_output_names]
                if is_subset else list(asset_ids)
            )
            steps_to_run_ids = _closure(selected_step_ids) if is_subset else [s["id"] for s in steps if s.get("id")]
            if is_subset:
                context.log.info(
                    f"can_subset ACTIVE: selected {len(selected_step_ids)}/{len(asset_ids)} asset(s) "
                    f"({sorted(selected_step_ids)}); running {len(steps_to_run_ids)}/{len(steps)} step(s) "
                    f"({steps_to_run_ids}) — skipping the rest"
                )

            # Parse the composite partition_key into named fields BEFORE ingest
            # so source.text / source.path / source.url can reference
            # {partition.<name>} placeholders too.
            partition_fields = _parse_partition_key(partition_key, partition_key_parser)
            if partition_fields:
                context.log.info(
                    f"partition_key_parser matched: {partition_fields}"
                )

            # Ingest.
            if source_config.get("kind") == "upstream_asset":
                upstream = kwargs["source"]
                if isinstance(upstream, dict) and "text" in upstream:
                    initial_text = upstream["text"]
                elif isinstance(upstream, str):
                    initial_text = upstream
                else:
                    initial_text = str(upstream)
                initial_entry = {"text": initial_text, "source_kind": "upstream_asset"}
                context.log.info(f"ingested {len(initial_text)} chars from upstream asset")
            else:
                initial_entry = _ingest(
                    source_config, context,
                    partition_key=partition_key,
                    partition_fields=partition_fields,
                    partitions_def=_partitions_def,
                )
                context.log.info(
                    f"ingested {len(initial_entry.get('text', ''))} chars via {source_config.get('kind', 'literal')}"
                )

            # Stash run-context so every op can reach it via _ctx_of(state).
            # Reserved keys `__ctx__` / `__personas__` — never collide with
            # user step ids (`_last_step_id` skips `__`-prefixed keys).
            state: Dict[str, Any] = {
                "source": initial_entry,
                "__ctx__": {
                    "partition_key": partition_key,
                    "partition_fields": partition_fields,
                },
                "__personas__": self.personas or {},
                "__policy__": self.policy or {},
                "__agents__": self.agents or {},
                "__sibling_agent_cards__": discovered_sibling_cards,
            }

            steps_to_run_set = set(steps_to_run_ids)
            for step in steps:
                if step.get("id") in steps_to_run_set:
                    _run_step(step, state, context)

            # Text sinks (partition-aware paths). Creates parent dirs so
            # {partition_key}-templated subdirs Just Work — matters both
            # locally and for Serverless container filesystems.
            # When can_subset is active, only emit sinks whose upstream
            # step is in the selected/needed set.
            for sink in text_sinks:
                if sink["from"] not in state:
                    continue  # subset skipped this step; skip its sink too
                from_id = sink["from"]
                path = _apply_ctx_substitutions(sink["path"], partition_key, partition_fields)
                if from_id not in state:
                    raise ValueError(f"text_sinks: unknown step id {from_id!r}")
                text = state[from_id].get("text", "") if isinstance(state[from_id], dict) else str(state[from_id])
                parent = os.path.dirname(path)
                if parent:
                    os.makedirs(parent, exist_ok=True)
                _pol_path(path, "write")
                with open(path, "w") as f:
                    f.write(text)
                context.log.info(f"text_sink {from_id!r} → {path}")

            # JSON sinks (full step dict, partition-aware paths).
            for sink in json_sinks:
                import json
                from_id = sink["from"]
                if from_id not in state:
                    continue  # subset skipped this step; skip its sink too
                path = _apply_ctx_substitutions(sink["path"], partition_key, partition_fields)
                parent = os.path.dirname(path)
                if parent:
                    os.makedirs(parent, exist_ok=True)
                _pol_path(path, "write")
                with open(path, "w") as f:
                    json.dump(state[from_id], f, indent=2, default=str)
                context.log.info(f"json_sink {from_id!r} → {path}")

            # Emit assets in declared order — subset only emits SELECTED
            # step_ids (never the upstream-closure ones we had to run to
            # produce them; those would be "unselected outputs" and Dagster
            # rejects yielding those).
            asset_ids_to_emit = (
                [aid for aid in asset_ids if aid in set(selected_step_ids)]
                if is_subset else list(asset_ids)
            )
            missing = [aid for aid in asset_ids_to_emit if aid not in state]
            if missing:
                raise ValueError(f"outputs.assets references unknown step ids: {missing}")

            # Add materialization metadata for each asset output.
            #
            # Every step surfaces a common core of metadata (text, cost_usd,
            # latency_ms, model_fingerprint, materialized_at, n_llm_calls, op,
            # partition_key) — this is what makes Dagster's asset history the
            # thing you browse instead of job logs. Op-specific fields
            # (router_reasoning, all_proposals, history) come after.
            # can_subset requires yielding Output per selected asset — a
            # positional tuple return would need to line up with declared
            # outs order, which subset mode breaks. Yielding is the modern
            # multi_asset pattern anyway.
            for aid in asset_ids_to_emit:
                entry = state[aid]
                md: Dict[str, Any] = {}
                if isinstance(entry, dict):
                    text = entry.get("text", "")
                    md[f"{aid}__text"] = dg.MetadataValue.md(text[:2000] if text else "_(empty)_")

                    # Typed core fields — these show up prominently in the UI
                    # and are filterable across the asset's materialization history.
                    # Numeric fields — Dagster+ Insights turns these into
                    # dashboardable time-series with per-metric alerts.
                    if entry.get("cost_usd") is not None:
                        md[f"{aid}__cost_usd"] = dg.MetadataValue.float(float(entry["cost_usd"]))
                    if entry.get("latency_ms") is not None:
                        md[f"{aid}__latency_ms"] = dg.MetadataValue.int(int(entry["latency_ms"]))
                    if entry.get("tokens_total") is not None:
                        md[f"{aid}__tokens_total"] = dg.MetadataValue.int(int(entry["tokens_total"]))
                    if entry.get("n_llm_calls") is not None:
                        md[f"{aid}__n_llm_calls"] = dg.MetadataValue.int(int(entry["n_llm_calls"]))
                    if entry.get("model_fingerprint"):
                        md[f"{aid}__model_fingerprint"] = dg.MetadataValue.text(str(entry["model_fingerprint"]))
                    if entry.get("materialized_at"):
                        try:
                            md[f"{aid}__materialized_at"] = dg.MetadataValue.timestamp(
                                datetime.fromisoformat(entry["materialized_at"])
                            )
                        except (TypeError, ValueError):
                            md[f"{aid}__materialized_at"] = dg.MetadataValue.text(str(entry["materialized_at"]))
                    if entry.get("op"):
                        md[f"{aid}__op"] = dg.MetadataValue.text(str(entry["op"]))
                    if partition_key is not None:
                        md[f"{aid}__partition_key"] = dg.MetadataValue.text(str(partition_key))
                    if is_subset:
                        md[f"{aid}__subset_mode"] = dg.MetadataValue.text(
                            f"selected={sorted(selected_step_ids)}; ran={steps_to_run_ids}"
                        )

                    # Op-specific rich fields (JSON blobs the UI renders inline).
                    for k, v in entry.items():
                        if k in ("text", "cost_usd", "latency_ms", "tokens_total",
                                 "n_llm_calls", "model_fingerprint",
                                 "materialized_at", "op"):
                            continue
                        if k == "usage" and v is not None:
                            md[f"{aid}__usage"] = dg.MetadataValue.json(v)
                        elif k == "all_proposals" and v is not None:
                            md[f"{aid}__proposals"] = dg.MetadataValue.json(v)
                        elif k == "history" and v is not None:
                            md[f"{aid}__history"] = dg.MetadataValue.json(v)
                        elif k == "reasoning" and v:
                            md[f"{aid}__reasoning"] = dg.MetadataValue.md(
                                "\n\n---\n\n".join(f"**{r['model']}**\n\n{r['text']}" for r in v)[:20000]
                            )
                        elif k in ("policy_usage", "policy_violation_detail") and v:
                            md[f"{aid}__{k}"] = dg.MetadataValue.json(v)
                        elif k in _JSON_META_KEYS and v:
                            md[f"{aid}__{k}"] = dg.MetadataValue.json(v)
                        elif isinstance(v, (str, int, float, bool)):
                            md[f"{aid}__{k}"] = dg.MetadataValue.text(str(v))
                md[f"{aid}__pipeline_version"] = dg.MetadataValue.text(_code_version({
                    "steps": steps, "source": source_config, "outputs": outputs, "personas": self.personas,
                    "agents": self.agents, "policy": self.policy}))
                yield dg.Output(value=entry, output_name=f"{prefix}_{aid}", metadata=md)

        @dg.multi_asset(
            outs=outs,
            name=f"{prefix}_pipeline",
            ins=ins or None,
            internal_asset_deps=internal_asset_deps or None,
            partitions_def=_partitions_def,
            backfill_policy=_backfill_policy,
            can_subset=True,
        )
        def _pipeline(context: dg.AssetExecutionContext, **kwargs):
            # File/URL/database ingest and the text/json sinks run in here, outside any step, so the policy
            # must be active for the whole body (steps re-activate it with the same shared usage counters).
            usage: Dict[str, Any] = {}
            with _policy_scope(self.policy, usage):
                yield from _pipeline_impl(context, **kwargs)

        return dg.Definitions(assets=[_pipeline])

    # ── Per-step-ops build path ──────────────────────────────────────
    #
    # Same runtime semantics as the single-op path — same _run_step,
    # same op executors, same state dict shape. But each step becomes
    # its OWN Dagster @op, so the Runs page shows N ops instead of 1
    # and Dagster's built-in per-op retry can restart from any failed
    # step. State dict passes through the IO manager between ops.

    def _build_per_step_ops_defs(
        self,
        *,
        prefix: str,
        steps: List[Dict[str, Any]],
        source_config: Dict[str, Any],
        asset_ids: List[str],
        text_sinks: List[Dict[str, Any]],
        json_sinks: List[Dict[str, Any]],
        outs: Dict[str, Any],
        ins: Dict[str, Any],
        internal_asset_deps: Dict[str, set],
        partitions_def,
        partition_key_parser: Optional[str],
        discovered_sibling_cards: Optional[List[Dict[str, Any]]] = None,
        backfill_policy=None,
    ) -> dg.Definitions:
        _self = self  # captured for closure use in ops
        discovered_sibling_cards = discovered_sibling_cards or []

        # --- ingest op ---
        # Same behavior as the multi_asset's inline ingest: read source
        # config (literal / file / url / upstream_asset), build initial
        # state dict with `source` + `__ctx__`.
        source_kind = source_config.get("kind", "literal")
        upstream_asset_source = source_kind == "upstream_asset"

        if upstream_asset_source:
            # Special-case: ingest op has an asset input (the upstream).
            upstream_key = source_config["upstream_asset_key"]

            @dg.op(name=f"{prefix}_ingest", ins={"source": dg.In(dagster_type=Any)})
            def _ingest_op(context: dg.OpExecutionContext, source):
                _require_one_partition(context, partitions_def is not None)
                partition_key = context.partition_key if context.has_partition_key else None
                partition_fields = _parse_partition_key(partition_key, partition_key_parser)
                if isinstance(source, dict) and "text" in source:
                    initial_text = source["text"]
                elif isinstance(source, str):
                    initial_text = source
                else:
                    initial_text = str(source)
                initial_entry = {"text": initial_text, "source_kind": "upstream_asset"}
                context.log.info(
                    f"[{prefix}_ingest] {len(initial_text)} chars from upstream asset "
                    f"partition_key={partition_key!r}"
                )
                return {
                    "source": initial_entry,
                    "__ctx__": {
                        "partition_key": partition_key,
                        "partition_fields": partition_fields,
                    },
                    "__personas__": _self.personas or {},
                    "__policy__": _self.policy or {},
                    "__agents__": _self.agents or {},
                    "__sibling_agent_cards__": discovered_sibling_cards,
                }
        else:
            @dg.op(name=f"{prefix}_ingest")
            def _ingest_op(context: dg.OpExecutionContext):
                _require_one_partition(context, partitions_def is not None)
                partition_key = context.partition_key if context.has_partition_key else None
                partition_fields = _parse_partition_key(partition_key, partition_key_parser)
                _usage: Dict[str, Any] = {}
                with _policy_scope(_self.policy, _usage):
                    initial_entry = _ingest(
                        source_config, context,
                        partition_key=partition_key, partition_fields=partition_fields,
                        partitions_def=partitions_def,
                    )
                context.log.info(
                    f"[{prefix}_ingest] {len(initial_entry.get('text', ''))} chars "
                    f"via {source_kind} partition_key={partition_key!r}"
                )
                return {
                    "source": initial_entry,
                    "__ctx__": {
                        "partition_key": partition_key,
                        "partition_fields": partition_fields,
                    },
                    "__personas__": _self.personas or {},
                    "__policy__": _self.policy or {},
                    "__policy_usage__": _usage,
                    "__agents__": _self.agents or {},
                    "__sibling_agent_cards__": discovered_sibling_cards,
                }

        # --- step ops (one per step) ---
        # Each takes prior state, runs its step, returns updated state.
        # Names are namespaced with the prefix so multiple pipelines in
        # the same project don't collide.

        def _make_step_op(step: Dict[str, Any]):
            step_id = step["id"]
            op_name = f"{prefix}_{step_id}"

            @dg.op(name=op_name)
            def _step_op(context: dg.OpExecutionContext, state: Dict[str, Any]) -> Dict[str, Any]:
                # State comes in from the IO manager (a fresh dict each hop
                # is what we want anyway). _run_step mutates in place.
                new_state = dict(state)
                _run_step(step, new_state, context)
                return new_state

            return _step_op

        step_ops = [_make_step_op(step) for step in steps]

        # --- extract op (yields all declared asset outputs) ---
        # Also handles text_sinks + json_sinks (same as the single-op path).
        # Yields one Output per declared asset, with full metadata.
        extract_outs = {f"{prefix}_{aid}": dg.Out(is_required=False) for aid in asset_ids}

        @dg.op(name=f"{prefix}_extract", out=extract_outs)
        def _extract_op(context: dg.OpExecutionContext, state: Dict[str, Any]):
            partition_key = context.partition_key if context.has_partition_key else None
            partition_fields = _parse_partition_key(partition_key, partition_key_parser)

            with _policy_scope(_self.policy, state.setdefault("__policy_usage__", {})):
                # Sinks (same code as single-op path).
                for sink in text_sinks:
                    from_id = sink["from"]
                    path = _apply_ctx_substitutions(sink["path"], partition_key, partition_fields)
                    if from_id not in state:
                        raise ValueError(f"text_sinks: unknown step id {from_id!r}")
                    text = state[from_id].get("text", "") if isinstance(state[from_id], dict) else str(state[from_id])
                    parent = os.path.dirname(path)
                    if parent:
                        os.makedirs(parent, exist_ok=True)
                    _pol_path(path, "write")
                    with open(path, "w") as f:
                        f.write(text)
                    context.log.info(f"text_sink {from_id!r} → {path}")

                for sink in json_sinks:
                    import json as _json
                    from_id = sink["from"]
                    path = _apply_ctx_substitutions(sink["path"], partition_key, partition_fields)
                    if from_id not in state:
                        raise ValueError(f"json_sinks: unknown step id {from_id!r}")
                    parent = os.path.dirname(path)
                    if parent:
                        os.makedirs(parent, exist_ok=True)
                    _pol_path(path, "write")
                    with open(path, "w") as f:
                        _json.dump(state[from_id], f, indent=2, default=str)
                    context.log.info(f"json_sink {from_id!r} → {path}")

            # Emit each declared asset as its own Output with metadata.
            missing = [aid for aid in asset_ids if aid not in state]
            if missing:
                raise ValueError(f"outputs.assets references unknown step ids: {missing}")

            for aid in asset_ids:
                entry = state[aid]
                md = _build_step_metadata(aid, entry, partition_key)
                md[f"{aid}__pipeline_version"] = dg.MetadataValue.text(_code_version({
                    "steps": steps, "source": source_config, "outputs": dict(_self.outputs), "personas": _self.personas,
                    "agents": _self.agents, "policy": _self.policy}))
                yield dg.Output(
                    entry, output_name=f"{prefix}_{aid}", metadata=md,
                )

        # --- wire the graph ---
        # graph_multi_asset derives asset-to-asset deps from the graph
        # topology automatically — no `internal_asset_deps` needed here.
        # can_subset works natively because each asset comes from the
        # same terminal `_extract_op` yielding is_required=False Outputs.
        @dg.graph_multi_asset(
            outs=outs,
            name=f"{prefix}_pipeline",
            ins=ins or None,
            partitions_def=partitions_def,
            backfill_policy=backfill_policy,
            can_subset=True,
        )
        def _pipeline_graph(**kwargs):
            # Ingest → chain step ops → extract.
            if upstream_asset_source:
                state = _ingest_op(source=kwargs["source"])
            else:
                state = _ingest_op()
            for step_op in step_ops:
                state = step_op(state)
            # Extract op yields multiple named outputs; graph_multi_asset
            # picks them up by asset name.
            return _extract_op(state)

        return dg.Definitions(assets=[_pipeline_graph])
