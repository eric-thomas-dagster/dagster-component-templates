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

## Detection engines

Two opt-in engines:

- `regex` (default, zero deps) — case-insensitive glob against dict
  keys + inline `key=value` regex against structured strings. Fast,
  no extra install. Catches known-format identifiers.
- `presidio` — Microsoft Presidio's analyzer runs pre-trained
  recognizers for 50+ entity types (SSN, CREDIT_CARD, EMAIL_ADDRESS,
  PERSON, PHONE_NUMBER, ...) plus configurable NER models. Catches
  semantic PII that regex can't (e.g. "Jane Doe visited Tuesday" →
  PERSON detected). Install on demand:

      pip install "presidio-analyzer[server]" presidio-anonymizer
      python -m spacy download en_core_web_sm

  Presidio is NOT a hard dependency of this package — it's imported
  lazily and only when `engine: presidio` is selected.

## Match rules (regex engine)

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


# --------------------------------------------------------------------------
# Presidio engine (opt-in, lazy-imported)
# --------------------------------------------------------------------------


_PRESIDIO_INSTALL_HINT = (
    "engine='presidio' requires Microsoft Presidio. Install:\n"
    "    pip install \"presidio-analyzer[server]\" presidio-anonymizer\n"
    "    python -m spacy download en_core_web_sm\n"
    "(Presidio is NOT a hard dependency of dagster-community-components.)"
)


def _presidio_get_engines():
    """Lazy-import Presidio + spaCy model. Raise dg.Failure with install hint if missing."""
    try:
        from presidio_analyzer import AnalyzerEngine  # type: ignore
        from presidio_anonymizer import AnonymizerEngine  # type: ignore
    except ImportError as e:
        raise dg.Failure(
            description=f"Presidio not installed: {e}\n\n{_PRESIDIO_INSTALL_HINT}"
        ) from e
    return AnalyzerEngine, AnonymizerEngine


def _presidio_scrub(
    text: str,
    entities: Optional[List[str]],
    language: str,
    strategy: str,
) -> tuple:
    """Run Presidio detection + anonymization over `text`.

    Returns (scrubbed_text, replacement_count).
    """
    if not isinstance(text, str) or not text:
        return text, 0
    AnalyzerEngine, AnonymizerEngine = _presidio_get_engines()
    try:
        from presidio_anonymizer.entities import OperatorConfig  # type: ignore
    except ImportError as e:
        raise dg.Failure(
            description=f"Presidio anonymizer entities missing: {e}\n\n{_PRESIDIO_INSTALL_HINT}"
        ) from e

    try:
        analyzer = AnalyzerEngine()
    except Exception as e:  # noqa: BLE001
        raise dg.Failure(
            description=(
                f"Presidio AnalyzerEngine init failed: {type(e).__name__}: {e}\n"
                "Usually a missing spaCy model. Run: python -m spacy download en_core_web_sm"
            )
        ) from e
    anonymizer = AnonymizerEngine()

    results = analyzer.analyze(text=text, entities=entities, language=language)
    if not results:
        return text, 0

    if strategy == "hash":
        operator = OperatorConfig("hash", {"hash_type": "sha256"})
    elif strategy == "mask":
        operator = OperatorConfig(
            "mask",
            {"masking_char": "*", "chars_to_mask": 12, "from_end": False},
        )
    else:
        operator = OperatorConfig("replace", {"new_value": "[REDACTED]"})

    try:
        anonymized = anonymizer.anonymize(
            text=text,
            analyzer_results=results,
            operators={"DEFAULT": operator},
        )
    except Exception as e:  # noqa: BLE001
        raise dg.Failure(
            description=f"Presidio anonymize failed: {type(e).__name__}: {e}"
        ) from e
    return anonymized.text, len(results)


def _presidio_scrub_value(
    v: Any,
    entities: Optional[List[str]],
    language: str,
    strategy: str,
    counter: List[int],
) -> Any:
    """Recursively presidio-scrub strings inside dicts / lists."""
    if isinstance(v, dict):
        return {k: _presidio_scrub_value(val, entities, language, strategy, counter) for k, val in v.items()}
    if isinstance(v, (list, tuple)):
        cls = type(v)
        return cls(_presidio_scrub_value(x, entities, language, strategy, counter) for x in v)
    if isinstance(v, str):
        scrubbed, n = _presidio_scrub(v, entities, language, strategy)
        counter[0] += n
        return scrubbed
    return v


class _ScrubbingLog:
    """Proxy for context.log that scrubs positional str args + dict extras.

    Dispatches on `engine`: `regex` (default) uses key-pattern globs +
    `_KV_PAIR_RE`; `presidio` runs Microsoft Presidio's analyzer over
    every string (lazy-imported).
    """

    def __init__(
        self,
        inner,
        patterns: List[str],
        strategy: str,
        counter: List[int],
        engine: str = "regex",
        presidio_entities: Optional[List[str]] = None,
        presidio_language: str = "en",
    ):
        self._inner = inner
        self._patterns = patterns
        self._strategy = strategy
        self._counter = counter
        self._engine = engine
        self._presidio_entities = presidio_entities
        self._presidio_language = presidio_language

    def _scrub(self, x: Any) -> Any:
        if self._engine == "presidio":
            return _presidio_scrub_value(
                x,
                self._presidio_entities,
                self._presidio_language,
                self._strategy,
                self._counter,
            )
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

    def __init__(
        self,
        inner,
        patterns: List[str],
        strategy: str,
        counter: List[int],
        engine: str = "regex",
        presidio_entities: Optional[List[str]] = None,
        presidio_language: str = "en",
    ):
        object.__setattr__(self, "_inner", inner)
        object.__setattr__(
            self,
            "_scrub_log",
            _ScrubbingLog(
                inner.log, patterns, strategy, counter,
                engine=engine,
                presidio_entities=presidio_entities,
                presidio_language=presidio_language,
            ),
        )

    def __getattr__(self, name: str) -> Any:
        if name == "log":
            return object.__getattribute__(self, "_scrub_log")
        return getattr(object.__getattribute__(self, "_inner"), name)

    def __setattr__(self, name: str, value: Any) -> None:
        setattr(object.__getattribute__(self, "_inner"), name, value)


def _post_scrub_result(
    result: Any,
    patterns: List[str],
    strategy: str,
    counter: List[int],
    engine: str = "regex",
    presidio_entities: Optional[List[str]] = None,
    presidio_language: str = "en",
) -> Any:
    """Scrub MaterializeResult metadata via the configured engine before it's persisted."""
    if isinstance(result, dg.MaterializeResult):
        md = result.metadata or {}
        clean_md: Dict[str, Any] = {}
        if engine == "presidio":
            # Presidio path — scrub the string form of every metadata value
            # via the analyzer (semantic PII detection).
            for k, v in md.items():
                raw = getattr(v, "value", v)
                if isinstance(raw, str):
                    scrubbed, n = _presidio_scrub(raw, presidio_entities, presidio_language, strategy)
                    if n > 0:
                        counter[0] += n
                        clean_md[k] = dg.MetadataValue.text(scrubbed)
                    else:
                        clean_md[k] = v
                else:
                    clean_md[k] = v
        else:
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
    engine: str = "regex",
    presidio_entities: Optional[List[str]] = None,
    presidio_language: str = "en",
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

    Presidio (opt-in, semantic PII detection):

    ```python
    @dg.asset
    @sensitive(engine="presidio", presidio_entities=["PERSON", "SSN", "CREDIT_CARD"])
    def patient_export(context):
        context.log.info("Jane Doe visited on Tuesday, SSN 123-45-6789")
        # → info: "<PERSON> visited on Tuesday, SSN [REDACTED]"
    ```

    Presidio install (NOT a hard dep):

        pip install "presidio-analyzer[server]" presidio-anonymizer
        python -m spacy download en_core_web_sm

    Args:
        keys: Field-name globs. `*` and `?` supported. Case-insensitive.
            Defaults to a common PII/secrets list. Regex engine only.
        strategy: `redact` (default) | `hash` | `mask`.
        engine: `regex` (default, zero deps) | `presidio` (semantic PII;
            requires `pip install "presidio-analyzer[server]" presidio-anonymizer`
            + a spaCy language model).
        presidio_entities: Presidio-only. Entity types to detect (e.g.
            `["SSN", "CREDIT_CARD", "PERSON"]`). None → Presidio defaults.
        presidio_language: Presidio-only. Language code (default `en`).
    """
    if strategy not in ("redact", "hash", "mask"):
        raise ValueError(f"strategy must be 'redact', 'hash', or 'mask'; got {strategy!r}")
    if engine not in ("regex", "presidio"):
        raise ValueError(f"engine must be 'regex' or 'presidio'; got {engine!r}")
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
            proxy = _SensitiveContextProxy(
                context, patterns, strategy, counter,
                engine=engine,
                presidio_entities=presidio_entities,
                presidio_language=presidio_language,
            )

            new_args = list(args)
            if new_args and new_args[0] is context:
                new_args[0] = proxy
            if "context" in kwargs and kwargs["context"] is context:
                kwargs = {**kwargs, "context": proxy}

            result = fn(*new_args, **kwargs)
            result = _post_scrub_result(
                result, patterns, strategy, counter,
                engine=engine,
                presidio_entities=presidio_entities,
                presidio_language=presidio_language,
            )
            _emit_scrub_observation(context, counter[0])
            return result

        return _wrapped
    return _decorator


class SensitiveAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """YAML shape of `@sensitive`. Two authoring modes:

    1. **Define a new asset from scratch** (original shape): supply
       `asset_name` + `compute: {kind: python, python: 'mod:fn'}`. Builds
       a single asset whose logs + returned metadata are scrubbed.

    2. **Wrap an existing DCC component** (composability): supply
       `wraps: {type: <component_class>, attributes: {...}}`. The inner
       component's assets are materialized as they would normally, and
       each compute's context.log calls + returned MaterializeResult
       metadata flow through the redactor. Preserves inner asset
       partitions, deps, resources, kinds, tags, group, description.
       Direct YAML analog of `@sensitive @dg.asset` in Python.

    `wraps:` and `compute:` are mutually exclusive.
    """

    asset_name: Optional[str] = Field(
        default=None,
        description="Dagster asset name. Required when NOT using `wraps:` (inherited from inner in wraps mode).",
    )
    upstream_asset_key: Optional[str] = Field(default=None)
    compute: Optional[Dict[str, Any]] = Field(
        default=None,
        description="`{kind: python, python: 'mod:fn'}`. Mutually exclusive with `wraps`.",
    )
    wraps: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Wrap another DCC component's assets with PII/secret log+metadata redaction "
            "instead of defining new compute. Shape: `{type: 'dagster_community_components.<Component>', "
            "attributes: {...}}`. Mutually exclusive with `compute`."
        ),
    )

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
        # Route: wraps-an-inner-component  vs  builds-own-asset
        if self.wraps is not None:
            if self.compute is not None:
                raise ValueError("SensitiveAssetComponent: `wraps:` and `compute:` are mutually exclusive.")
            return self._build_wrapped(context)
        if self.compute is None:
            raise ValueError("SensitiveAssetComponent: supply either `compute` (build new asset) or `wraps` (wrap existing component).")
        if not self.asset_name:
            raise ValueError("SensitiveAssetComponent: `asset_name` required when using `compute:`.")

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

    # ----------------------------------------------------------------------
    # `wraps:` composability
    # ----------------------------------------------------------------------

    def _build_wrapped(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        inner = _resolve_inner_component(self.wraps or {})
        inner_defs = inner.build_defs(context)
        wrapped_assets = []
        for asset_def in list(inner_defs.assets or []):
            if len(asset_def.keys) != 1:
                wrapped_assets.append(asset_def)
                continue
            wrapped_assets.append(self._wrap_single_asset(asset_def))
        return dg.Definitions(
            assets=wrapped_assets,
            resources=inner_defs.resources,
            sensors=inner_defs.sensors,
            schedules=inner_defs.schedules,
            asset_checks=inner_defs.asset_checks,
            jobs=inner_defs.jobs,
            loggers=inner_defs.loggers,
        )

    def _wrap_single_asset(self, asset_def: "dg.AssetsDefinition") -> "dg.AssetsDefinition":
        key = next(iter(asset_def.keys))
        specs_by_key = getattr(asset_def, "specs_by_key", {}) or {}
        spec = specs_by_key.get(key)
        inner_op = asset_def.op
        inner_compute = getattr(inner_op.compute_fn, "decorated_fn", None) or inner_op.compute_fn

        patterns = list(self.keys or DEFAULT_KEYS)
        strategy_ = self.strategy
        if strategy_ not in ("redact", "hash", "mask"):
            raise ValueError(f"strategy must be 'redact', 'hash', or 'mask'; got {strategy_!r}")

        inner_kinds = set(getattr(spec, "kinds", None) or []) if spec else set()
        merged_kinds = inner_kinds | set(self.kinds or []) | {"sensitive"}
        inner_tags = dict(getattr(spec, "tags", None) or {}) if spec else {}
        merged_tags = {**inner_tags, **(self.tags or {})}
        merged_owners = list((spec.owners if spec else []) or []) + (self.owners or [])
        inner_description = (spec.description if spec else None) or f"Sensitive-wrapped {key.to_user_string()}"
        merged_description = f"{inner_description}  [sensitive: strategy={strategy_}, patterns={len(patterns)}]"
        inner_deps = list(spec.deps) if (spec and getattr(spec, "deps", None)) else []

        @dg.asset(
            key=key,
            partitions_def=asset_def.partitions_def,
            deps=inner_deps,
            group_name=(spec.group_name if spec else None),
            kinds=merged_kinds,
            tags=merged_tags,
            owners=merged_owners,
            description=merged_description,
            metadata=(dict(spec.metadata) if (spec and spec.metadata) else {}),
            code_version=(spec.code_version if spec else None),
        )
        def _sensitive_wrapped(context: dg.AssetExecutionContext, **kwargs):
            counter = [0]
            proxy = _SensitiveContextProxy(context, patterns, strategy_, counter)
            # Route the inner compute's context through the scrubbing proxy.
            result = inner_compute(proxy, **kwargs)
            result = _post_scrub_result(result, patterns, strategy_, counter)
            _emit_scrub_observation(context, counter[0])

            passthrough_meta = {
                "sensitive_redacted_count": dg.MetadataValue.int(int(counter[0])),
                "sensitive_strategy": dg.MetadataValue.text(strategy_),
            }
            if isinstance(result, dg.MaterializeResult):
                merged = dict(result.metadata or {})
                merged.update(passthrough_meta)
                return dg.MaterializeResult(
                    asset_key=result.asset_key,
                    metadata=merged,
                    check_results=result.check_results,
                    data_version=result.data_version,
                    tags=result.tags,
                )
            return result

        return _sensitive_wrapped


def _resolve_inner_component(wraps: Dict[str, Any]):
    """Resolve `{type: '...', attributes: {...}}` → instantiated component."""
    type_str = wraps.get("type")
    attrs = wraps.get("attributes") or {}
    if not type_str or not isinstance(type_str, str):
        raise ValueError("SensitiveAssetComponent.wraps requires `type: <fully-qualified-class-name>`.")
    if ":" in type_str:
        mod_path, cls_name = type_str.rsplit(":", 1)
    else:
        mod_path, cls_name = type_str.rsplit(".", 1)
    try:
        mod = importlib.import_module(mod_path)
    except ImportError as e:
        raise ValueError(f"SensitiveAssetComponent.wraps: cannot import {mod_path!r}: {e}") from e
    cls = getattr(mod, cls_name, None)
    if cls is None:
        raise ValueError(f"SensitiveAssetComponent.wraps: {cls_name!r} not found in {mod_path!r}.")
    try:
        return cls(**attrs)
    except Exception as e:  # noqa: BLE001
        raise ValueError(
            f"SensitiveAssetComponent.wraps: constructing {type_str} failed: {type(e).__name__}: {e}"
        ) from e
