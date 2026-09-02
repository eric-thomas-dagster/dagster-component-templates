"""SensitiveAssetComponent + `@sensitive` — redact PII / secrets from logs + metadata.

Wraps compute with a log-proxy that scrubs string args + dict fields
matching configured key patterns; also scrubs `MaterializeResult`
metadata before it lands in the event log. Every run emits an
`AssetObservation` with `sensitive_redacted_count` metadata so audits
can prove the scrub ran.

## Why this belongs in Dagster

- **The Dagster event log is where PII risk lives** — every
  `context.log.info` call and every `MaterializeResult` metadata dict
  ends up persisted. Wrapping compute with a redaction proxy stops
  bleed BEFORE persistence.
- **AssetObservation with redaction count** — every run leaves proof
  the redactor ran. Sensors alert when count drops unexpectedly.
- **Fits SOC2 audit playbook** — scoped to a per-asset attestation
  rather than a global logger config.

## Two shapes

- **`SensitiveAssetComponent`** (YAML)
- **`@sensitive` decorator** (Python)

## Match rules

Each `key` in the config is a case-insensitive glob against dict keys
+ substring against structured strings (e.g., `key=value`, `"key": "..."`
JSON fragments). Wildcards `*` and `?` supported.

Defaults if unspecified:

    ["password", "passwd", "secret", "*_secret",
     "token", "*_token", "api_key", "*_api_key",
     "ssn", "credit_card", "cvv", "authorization"]

## Redaction strategy

- `redact` (default) — replace value with `[REDACTED]`.
- `hash`   — replace with `sha256(value)[:8]`.
- `mask`   — replace with `***` + last-4 chars.

## Composes with

- `@lifecycle` — audit stage still runs on unredacted data; logs are safe.
- `@profile` — profiles still generated; matched columns are hashed instead of raw.
- `@log_prints` — captured print() output also flows through the redactor.
"""

import fnmatch
import functools
import hashlib
import importlib
import re
from typing import Any, Callable, Dict, List, Optional, Set

import dagster as dg
from pydantic import Field


_SENSITIVE_TAG = "sensitive_redacted_count"

DEFAULT_KEYS: List[str] = [
    "password", "passwd", "secret", "*_secret",
    "token", "*_token", "api_key", "*_api_key",
    "ssn", "credit_card", "cvv", "authorization",
]


def _redact(value: Any, strategy: str) -> str:
    s = str(value)
    if strategy == "hash":
        return "sha256:" + hashlib.sha256(s.encode("utf-8", "ignore")).hexdigest()[:8]
    if strategy == "mask":
        tail = s[-4:] if len(s) >= 4 else ""
        return "***" + tail
    return "[REDACTED]"


def _matches(key: str, patterns: List[str]) -> bool:
    k = key.lower()
    return any(fnmatch.fnmatchcase(k, p.lower()) for p in patterns)


def _scrub_value(v: Any, patterns: List[str], strategy: str, counter: List[int]) -> Any:
    """Recursively redact matching keys in dicts + values in strings."""
    if isinstance(v, dict):
        return _scrub_dict(v, patterns, strategy, counter)
    if isinstance(v, (list, tuple)):
        cls = type(v)
        return cls(_scrub_value(x, patterns, strategy, counter) for x in v)
    if isinstance(v, str):
        return _scrub_str(v, patterns, strategy, counter)
    return v


def _scrub_dict(d: Dict[str, Any], patterns: List[str], strategy: str, counter: List[int]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in d.items():
        if _matches(str(k), patterns):
            out[k] = _redact(v, strategy)
            counter[0] += 1
        else:
            out[k] = _scrub_value(v, patterns, strategy, counter)
    return out


_KV_PAIR_RE = re.compile(
    r"""(?ix)
    \b(?P<key>[a-z_][a-z0-9_-]{1,64})       # bare key
    \s*[:=]\s*
    (?P<val>
        "(?:[^"\\]|\\.)*"                    # double-quoted
      | '(?:[^'\\]|\\.)*'                    # single-quoted
      | [^\s,;)}\]]+                         # bare token
    )
    """
)


def _scrub_str(s: str, patterns: List[str], strategy: str, counter: List[int]) -> str:
    def _sub(m):
        key = m.group("key")
        val = m.group("val")
        if _matches(key, patterns):
            counter[0] += 1
            quoted = val.startswith(("'", '"')) and val.endswith(("'", '"'))
            new_val = _redact(val.strip("'\""), strategy)
            if quoted:
                return f'{key}="{new_val}"'
            return f"{key}={new_val}"
        return m.group(0)

    return _KV_PAIR_RE.sub(_sub, s)


class _ScrubbingLog:
    """Proxy for context.log that scrubs positional str args + dict extras."""

    def __init__(self, inner, patterns: List[str], strategy: str, counter: List[int]):
        self._inner = inner
        self._patterns = patterns
        self._strategy = strategy
        self._counter = counter

    def _scrub(self, x: Any) -> Any:
        return _scrub_value(x, self._patterns, self._strategy, self._counter)

    def _wrap(self, level_name: str) -> Callable:
        inner_fn = getattr(self._inner, level_name)

        def _call(msg, *args, **kwargs):
            msg = self._scrub(msg)
            args = tuple(self._scrub(a) for a in args)
            extra = kwargs.get("extra")
            if isinstance(extra, dict):
                kwargs["extra"] = self._scrub(extra)
            return inner_fn(msg, *args, **kwargs)

        return _call

    def __getattr__(self, name: str) -> Any:
        if name in ("info", "warning", "error", "debug", "critical", "warn"):
            return self._wrap(name)
        return getattr(self._inner, name)


def _emit_scrub_observation(context: Any, count: int) -> None:
    try:
        from dagster import AssetObservation
        asset_key = getattr(context, "asset_key", None) or dg.AssetKey(["sensitive_asset"])
        if hasattr(context, "log_event"):
            context.log_event(AssetObservation(
                asset_key=asset_key,
                tags={_SENSITIVE_TAG: str(count)},
                metadata={
                    "sensitive_redacted_count": dg.MetadataValue.int(int(count)),
                },
            ))
    except Exception:  # noqa: BLE001
        pass


class _SensitiveContextProxy:
    """Wraps a Dagster context so `.log` is our scrubbing proxy."""

    def __init__(self, inner, patterns: List[str], strategy: str, counter: List[int]):
        object.__setattr__(self, "_inner", inner)
        object.__setattr__(self, "_scrub_log", _ScrubbingLog(inner.log, patterns, strategy, counter))

    def __getattr__(self, name: str) -> Any:
        if name == "log":
            return object.__getattribute__(self, "_scrub_log")
        return getattr(object.__getattribute__(self, "_inner"), name)

    def __setattr__(self, name: str, value: Any) -> None:
        setattr(object.__getattribute__(self, "_inner"), name, value)


def _post_scrub_result(result: Any, patterns: List[str], strategy: str, counter: List[int]) -> Any:
    if isinstance(result, dg.MaterializeResult):
        md = result.metadata or {}
        clean_md: Dict[str, Any] = {}
        for k, v in md.items():
            if _matches(str(k), patterns):
                counter[0] += 1
                if hasattr(v, "value"):
                    clean_md[k] = dg.MetadataValue.text(_redact(getattr(v, "value", ""), strategy))
                else:
                    clean_md[k] = dg.MetadataValue.text(_redact(v, strategy))
            else:
                clean_md[k] = v
        return dg.MaterializeResult(
            asset_key=result.asset_key,
            metadata=clean_md,
            check_results=result.check_results,
            data_version=result.data_version,
            tags=result.tags,
        )
    return result


def sensitive(
    *,
    keys: Optional[List[str]] = None,
    strategy: str = "redact",
) -> Callable:
    """Redact configured field names from `context.log` calls + returned metadata.

    ```python
    @dg.asset
    @sensitive(keys=["password", "*_token", "ssn"])
    def user_export(context):
        context.log.info(f"processing user with ssn=123-45-6789")   # scrubbed
        return dg.MaterializeResult(
            metadata={"ssn": "123-45-6789", "row_count": 42},        # ssn redacted
        )
    ```

    Args:
        keys: Field-name globs. `*` and `?` supported. Case-insensitive.
            Defaults to a common PII/secrets list.
        strategy: `redact` (default) | `hash` | `mask`.
    """
    if strategy not in ("redact", "hash", "mask"):
        raise ValueError(f"strategy must be 'redact', 'hash', or 'mask'; got {strategy!r}")
    patterns = list(keys or DEFAULT_KEYS)

    def _decorator(fn: Callable) -> Callable:
        @functools.wraps(fn)
        def _wrapped(*args, **kwargs):
            context = None
            if args and hasattr(args[0], "log"):
                context = args[0]
            elif "context" in kwargs and hasattr(kwargs["context"], "log"):
                context = kwargs["context"]
            if context is None:
                raise RuntimeError("@sensitive requires a Dagster context.")

            counter = [0]
            proxy = _SensitiveContextProxy(context, patterns, strategy, counter)

            new_args = list(args)
            if new_args and new_args[0] is context:
                new_args[0] = proxy
            if "context" in kwargs and kwargs["context"] is context:
                kwargs = {**kwargs, "context": proxy}

            result = fn(*new_args, **kwargs)
            result = _post_scrub_result(result, patterns, strategy, counter)
            _emit_scrub_observation(context, counter[0])
            return result

        return _wrapped
    return _decorator


class SensitiveAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """YAML shape of `@sensitive`. Wraps a compute with log + metadata redaction."""

    asset_name: str = Field(description="Dagster asset name.")
    upstream_asset_key: Optional[str] = Field(default=None)
    compute: Dict[str, Any] = Field(description="`{kind: python, python: 'mod:fn'}`.")

    keys: Optional[List[str]] = Field(
        default=None,
        description="Case-insensitive globs against dict keys and structured strings. "
                    "Defaults to a common PII/secrets list (passwords, tokens, api_key, ssn, credit_card, cvv, authorization).",
    )
    strategy: str = Field(
        default="redact",
        description="'redact' (default) → [REDACTED]; 'hash' → sha256:xxxxxxxx; 'mask' → ***last4.",
    )

    group_name: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    tags: Optional[Dict[str, str]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None, description="Default: ['python', 'sensitive'].")

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Sensitive Asset", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        compute = dict(self.compute)
        patterns = list(self.keys or DEFAULT_KEYS)
        strategy_ = self.strategy
        if strategy_ not in ("redact", "hash", "mask"):
            raise ValueError(f"strategy must be 'redact', 'hash', or 'mask'; got {strategy_!r}")

        kinds_set = set(self.kinds or []) | {"python", "sensitive"}
        tag_map = dict(self.tags or {})
        for k in kinds_set:
            tag_map[f"dagster/kind/{k}"] = ""

        ins = {}
        if upstream_asset_key:
            ins["upstream"] = dg.AssetIn(key=dg.AssetKey.from_user_string(upstream_asset_key))

        @dg.asset(
            key=dg.AssetKey.from_user_string(asset_name),
            description=self.description or f"PII-redacted asset {asset_name}",
            group_name=self.group_name,
            owners=self.owners or [],
            tags=tag_map,
            kinds=kinds_set,
            ins=ins,
        )
        def _asset(context: dg.AssetExecutionContext, **kwargs):
            kind = (compute.get("kind") or "python").lower()
            if kind != "python":
                raise ValueError(f"SensitiveAssetComponent supports compute.kind=python only; got {kind!r}")
            ref = compute.get("python")
            if not ref or ":" not in ref:
                raise ValueError("compute.python must be 'module.path:function_name'")
            mod_path, fn_name = ref.rsplit(":", 1)
            fn = getattr(importlib.import_module(mod_path.strip()), fn_name.strip(), None)
            if not callable(fn):
                raise ValueError(f"compute.python {ref!r} not callable")

            counter = [0]
            proxy = _SensitiveContextProxy(context, patterns, strategy_, counter)

            import inspect
            sig = inspect.signature(fn)
            n_positional = sum(1 for p in sig.parameters.values()
                               if p.kind in (p.POSITIONAL_OR_KEYWORD, p.POSITIONAL_ONLY))
            if n_positional == 0:
                result = fn()
            elif n_positional == 1:
                result = fn(proxy)
            else:
                result = fn(proxy, kwargs.get("upstream"))

            result = _post_scrub_result(result, patterns, strategy_, counter)
            _emit_scrub_observation(context, counter[0])

            if isinstance(result, dg.MaterializeResult):
                return result
            return dg.MaterializeResult(
                metadata={"sensitive_redacted_count": dg.MetadataValue.int(int(counter[0]))}
            )

        return dg.Definitions(assets=[_asset])
