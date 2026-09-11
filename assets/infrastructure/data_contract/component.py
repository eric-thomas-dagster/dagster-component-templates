"""DataContractComponent + `@data_contract` — enforce data contracts at materialization.

A data contract is the formal agreement between a data producer and its
consumers: what columns exist, what types they are, which are nullable,
what ranges are allowed, how fresh it must be, who owns it, who consumes
it, and how it's versioned. This component enforces contracts as CODE —
every materialization validates the produced DataFrame against the
contract, emits one `AssetCheckResult` per rule, and either blocks
publish on violation OR materializes with failing checks that block
downstream via `AutomationCondition`.

## Consumer side — `@requires_contract`

The producer side (`@data_contract` / `DataContractComponent`) emits an
`AssetObservation` describing the contract on every materialization. The
consumer side (`@requires_contract` / `RequiresContractComponent`) reads
that observation from the event log, verifies a semver `min_version`
(and optional `require_columns`), and either passes through or raises
`dg.Failure` before the downstream compute runs.

## Breaking-change detection

`@data_contract(..., detect_breaking_changes=True)` looks up the PRIOR
contract observation for the same asset and diffs schemas. Dropped
columns, narrowed types (float→int), and nullable→non-nullable
transitions all emit an additional `contract_breaking_change=true`
observation with a markdown summary. Set `on_breaking_change="fail"` to
promote breaking diffs to a hard `dg.Failure`.

## JSON Schema import

`contract_from_json_schema(path_or_dict)` reads a JSON Schema file and
returns a DCC contract dict ready to pass to `@data_contract(contract=…)`
or as `contract:` in YAML — so teams that already publish schemas as
JSON Schema (OpenAPI, event bus, etc.) don't have to hand-mirror them.

## Why Dagster is the right home for this

Every enforcement primitive is a Dagster-native event:
- **Schema violations** → `AssetCheckResult(severity=ERROR)` per column.
- **Freshness violations** → `AssetCheckResult` computed against the
  prior materialization's timestamp (`context.instance` event log).
- **Row-count SLA** → `AssetCheckResult` against the prior row count
  (also from the event log; compares this materialization's row count
  against the last N).
- **Contract version** → set as `code_version` on the asset. Downstream
  consumers can detect version bumps automatically.
- **Contract metadata** → `AssetObservation` tagged with `contract_version`,
  `contract_owners`, `contract_consumers`. Searchable in the UI + agent
  planners can look up who owns a contract.

## Two shapes — component + decorator

### Component (`DataContractComponent`) — YAML

Define a new asset with contract enforcement baked in. YAML config
contains the full contract (schema, freshness, sla, version, owners,
consumers).

### Decorator (`@data_contract`) — Python

Wrap an EXISTING `@dg.asset`. Same enforcement engine, no YAML.

Both emit the same events + set the same `code_version`. Consumers
downstream can pin to a specific version via a separate
`@requires_contract` decorator (v2 — coming soon).

## Schema rules in v1

Each column entry supports:
- `name` (required)
- `type` — pandas dtype string (`int64` / `float64` / `string` /
  `bool` / `datetime64[ns]` / etc.)
- `nullable` — bool (default true)
- `unique` — bool (default false)
- `min` / `max` — numeric bounds
- `allowed_values` — list of accepted values (categoricals)
- `regex` — pattern the column must match (strings only)

## Enforcement modes

- `on_violation: block` (default) — raises `dg.Failure` when ANY check
  fails. Asset does NOT materialize; downstream blocked.
- `on_violation: warn` — asset materializes anyway; failing checks are
  visible in the UI; downstream can block via
  `AutomationCondition.eager()` on the asset checks.

## Freshness + SLA checks

`freshness_max_lag_minutes` — verify the materialization is happening
within the SLA window (checks partition timestamp or current time
against the last successful materialization from the event log).

`sla_max_row_count_drop_pct` — compare row count of this materialization
against the last successful one. If dropped by more than `pct%`, fails
the SLA check. Prevents silently-empty updates from reaching prod.
"""

import functools
import importlib
import json
import re
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import dagster as dg
from pydantic import Field


# --------------------------------------------------------------------------
# Contract validation engine
# --------------------------------------------------------------------------


def _validate_column(col_spec: Dict[str, Any], df) -> Dict[str, Any]:
    """Validate one column against its schema entry.

    Returns `{name, passed, description, metadata}` for the AssetCheckResult.
    """
    name = col_spec.get("name")
    if not name:
        return {"name": "schema_error", "passed": False,
                "description": "FAIL: schema entry missing 'name'", "metadata": {}}

    check_name = f"schema_{name}"

    if name not in df.columns:
        return {"name": check_name, "passed": False,
                "description": f"FAIL: column {name!r} missing from DataFrame",
                "metadata": {"col": name, "columns_present": list(df.columns)[:20]}}

    col = df[name]
    violations: List[str] = []
    md: Dict[str, Any] = {"col": name}

    # Type check.
    expected_type = col_spec.get("type")
    if expected_type:
        actual = str(col.dtype)
        md["actual_dtype"] = actual
        md["expected_dtype"] = expected_type
        if not _dtype_matches(actual, expected_type):
            violations.append(f"dtype={actual} (expected {expected_type})")

    # Nullability.
    nullable = col_spec.get("nullable", True)
    if not nullable:
        n_null = int(col.isna().sum())
        md["null_count"] = n_null
        if n_null > 0:
            violations.append(f"{n_null} null(s) in non-nullable column")

    # Uniqueness.
    if col_spec.get("unique"):
        n_total = int(len(col))
        n_distinct = int(col.nunique(dropna=False))
        md["distinct"] = n_distinct
        md["row_count"] = n_total
        if n_distinct != n_total:
            violations.append(f"{n_total - n_distinct} duplicate(s) in unique column")

    # Min / max bounds.
    if "min" in col_spec:
        try:
            actual_min = col.min()
            md["actual_min"] = str(actual_min)
            if actual_min < col_spec["min"]:
                violations.append(f"min={actual_min} < required {col_spec['min']}")
        except Exception:  # noqa: BLE001
            violations.append("could not compute min (non-numeric column?)")
    if "max" in col_spec:
        try:
            actual_max = col.max()
            md["actual_max"] = str(actual_max)
            if actual_max > col_spec["max"]:
                violations.append(f"max={actual_max} > allowed {col_spec['max']}")
        except Exception:  # noqa: BLE001
            violations.append("could not compute max (non-numeric column?)")

    # Allowed values (categoricals).
    allowed = col_spec.get("allowed_values")
    if allowed is not None:
        allowed_set = set(allowed)
        # Only compare non-null values.
        actual_set = set(col.dropna().unique().tolist())
        bad = actual_set - allowed_set
        if bad:
            violations.append(f"values not in allowed set: {sorted(list(bad))[:5]}")
        md["allowed_values"] = list(allowed_set)

    # Regex (string columns).
    pattern = col_spec.get("regex")
    if pattern:
        try:
            rx = re.compile(pattern)
            n_bad = int(col.dropna().astype(str).apply(
                lambda v: not bool(rx.match(v))
            ).sum())
            md["regex"] = pattern
            if n_bad:
                violations.append(f"{n_bad} value(s) fail regex {pattern!r}")
        except Exception as e:  # noqa: BLE001
            violations.append(f"regex compile failed: {e}")

    if not violations:
        return {"name": check_name, "passed": True,
                "description": f"col {name!r} conforms to schema",
                "metadata": md}
    return {"name": check_name, "passed": False,
            "description": f"FAIL: col {name!r}: " + "; ".join(violations),
            "metadata": md}


def _dtype_matches(actual: str, expected: str) -> bool:
    """Best-effort pandas-dtype comparison.

    - Exact match (e.g. `int64` == `int64`).
    - Family match (e.g. `string` matches `object` or `string[python]`;
      `int` matches any `int8/16/32/64`; `float` matches any `float16/32/64`).
    """
    if actual == expected:
        return True
    if expected == "string":
        # `str` covers the pandas 2.x StringDtype shorthand, `object` is legacy.
        return actual in ("object", "string", "string[python]", "string[pyarrow]", "large_string", "str")
    if expected == "int":
        return actual.startswith("int") or actual.startswith("uint")
    if expected == "float":
        return actual.startswith("float")
    if expected == "bool":
        return actual == "bool"
    if expected.startswith("datetime"):
        return actual.startswith("datetime")
    return False


# --------------------------------------------------------------------------
# Cross-run history lookups (Dagster instance event log)
# --------------------------------------------------------------------------

_CONTRACT_VERSION_TAG = "contract_version"
_CONTRACT_ROWCOUNT_TAG = "contract_row_count"


def _prior_row_count(context: Any, asset_key: Any) -> Optional[int]:
    """Look up the row count from the prior successful materialization.

    Uses the event log — looks at the most recent AssetMaterialization event
    for this asset and returns the reported `contract_row_count` metadata.
    """
    try:
        from dagster import EventRecordsFilter, DagsterEventType
        records = context.instance.get_event_records(
            event_records_filter=EventRecordsFilter(
                event_type=DagsterEventType.ASSET_MATERIALIZATION,
                asset_key=asset_key,
            ),
            limit=5, ascending=False,
        )
        for r in records:
            mat = r.asset_materialization
            if mat is None:
                continue
            md = mat.metadata or {}
            v = md.get(_CONTRACT_ROWCOUNT_TAG)
            if v is not None:
                # MetadataValue → int
                try:
                    return int(getattr(v, "value", v))
                except Exception:  # noqa: BLE001
                    continue
    except Exception:  # noqa: BLE001
        return None
    return None


def _last_materialization_timestamp(context: Any, asset_key: Any) -> Optional[float]:
    """Return the last successful materialization timestamp (epoch seconds)."""
    try:
        from dagster import EventRecordsFilter, DagsterEventType
        records = context.instance.get_event_records(
            event_records_filter=EventRecordsFilter(
                event_type=DagsterEventType.ASSET_MATERIALIZATION,
                asset_key=asset_key,
            ),
            limit=1, ascending=False,
        )
        for r in records:
            ts = r.timestamp
            if ts:
                return float(ts)
    except Exception:  # noqa: BLE001
        return None
    return None


# --------------------------------------------------------------------------
# Semver parsing (no external dep) + contract-observation history lookup
# --------------------------------------------------------------------------


def _parse_semver(v: str) -> Tuple[int, int, int]:
    """Parse `1.2.3` (or `v1.2.3`, or `1.2`) → `(1, 2, 3)`.

    Missing components default to 0. Non-numeric components raise ValueError.
    Ignores pre-release / build suffixes after `-` or `+`.
    """
    if v is None:
        raise ValueError("empty semver")
    s = str(v).strip()
    if s.startswith("v") or s.startswith("V"):
        s = s[1:]
    # Strip pre-release / build metadata.
    for sep in ("-", "+"):
        if sep in s:
            s = s.split(sep, 1)[0]
    parts = s.split(".")
    if len(parts) == 0 or len(parts) > 3:
        raise ValueError(f"invalid semver {v!r}")
    ints: List[int] = []
    for p in parts:
        if not p.isdigit():
            raise ValueError(f"invalid semver component {p!r} in {v!r}")
        ints.append(int(p))
    while len(ints) < 3:
        ints.append(0)
    return (ints[0], ints[1], ints[2])


def _semver_lt(a: str, b: str) -> bool:
    """Return True if semver `a` < semver `b`."""
    return _parse_semver(a) < _parse_semver(b)


def _latest_contract_observation(
    context: Any,
    asset_key: Any,
    before_now: bool = False,
) -> Optional[Dict[str, Any]]:
    """Look up the most recent `contract_snapshot`-carrying AssetObservation.

    Returns the deserialized contract dict (as stored in the observation's
    `contract_snapshot` metadata), plus its version — or None if no such
    observation exists.

    `before_now=True` — skip observations emitted in the current run
    (useful for breaking-change diffs where we've already emitted the
    current contract earlier in the same run).
    """
    try:
        from dagster import EventRecordsFilter, DagsterEventType
        instance = getattr(context, "instance", None)
        if instance is None:
            return None
        current_run_id = getattr(context, "run_id", None)
        records = instance.get_event_records(
            event_records_filter=EventRecordsFilter(
                event_type=DagsterEventType.ASSET_OBSERVATION,
                asset_key=asset_key,
            ),
            limit=25, ascending=False,
        )
        for r in records:
            obs = None
            de = getattr(r, "dagster_event", None)
            if de is not None:
                esd = getattr(de, "event_specific_data", None)
                obs = getattr(esd, "asset_observation", None) if esd is not None else None
            if obs is None:
                obs = getattr(r, "asset_observation", None)
            if obs is None:
                continue
            if before_now and current_run_id is not None:
                # Skip observations from the current run.
                run_id = getattr(r, "run_id", None) or getattr(de, "run_id", None) if de else None
                if run_id == current_run_id:
                    continue
            md = obs.metadata or {}
            snap = md.get("contract_snapshot")
            if snap is None:
                # Fallback: derive from tags if we only have the version tag.
                tags = obs.tags or {}
                ver = tags.get(_CONTRACT_VERSION_TAG)
                if ver:
                    return {"version": ver, "schema": []}
                continue
            # MetadataValue.json → .value; raw dict passes through
            raw = getattr(snap, "value", snap)
            if isinstance(raw, str):
                try:
                    raw = json.loads(raw)
                except Exception:  # noqa: BLE001
                    continue
            if isinstance(raw, dict):
                return raw
    except Exception:  # noqa: BLE001
        return None
    return None


def _diff_contracts(
    prior: Dict[str, Any], current: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """Return a list of breaking-change flags going `prior` → `current`.

    Flag kinds:
      - dropped_column       — present in prior, missing in current
      - narrowed_type        — column's type narrowed (e.g. float→int)
      - nullability_narrowed — nullable True → False
    """
    _NARROWING = {
        # prior_type → set of types that are STRICTLY NARROWER
        "float":         {"int", "bool"},
        "float64":       {"int", "int64", "int32", "int16", "int8", "bool"},
        "float32":       {"int", "int64", "int32", "int16", "int8", "bool"},
        "number":        {"int", "int64", "integer", "bool"},
        "int":           {"bool"},
        "int64":         {"int32", "int16", "int8", "bool"},
        "int32":         {"int16", "int8", "bool"},
        "integer":       {"bool"},
        "string":        {"int", "int64", "float", "float64", "bool"},
        "object":        {"int", "int64", "float", "float64", "bool", "string"},
    }
    flags: List[Dict[str, Any]] = []
    prior_cols = {c.get("name"): c for c in (prior.get("schema") or []) if c.get("name")}
    curr_cols = {c.get("name"): c for c in (current.get("schema") or []) if c.get("name")}

    for name, prior_col in prior_cols.items():
        if name not in curr_cols:
            flags.append({
                "kind": "dropped_column", "column": name,
                "detail": f"column {name!r} removed",
            })
            continue
        curr_col = curr_cols[name]
        prior_type = str(prior_col.get("type") or "").strip()
        curr_type = str(curr_col.get("type") or "").strip()
        if prior_type and curr_type and prior_type != curr_type:
            narrower = _NARROWING.get(prior_type, set())
            if curr_type in narrower:
                flags.append({
                    "kind": "narrowed_type", "column": name,
                    "prior": prior_type, "current": curr_type,
                    "detail": f"column {name!r}: {prior_type} → {curr_type} (narrowed)",
                })
        prior_null = prior_col.get("nullable", True)
        curr_null = curr_col.get("nullable", True)
        if prior_null and not curr_null:
            flags.append({
                "kind": "nullability_narrowed", "column": name,
                "detail": f"column {name!r}: nullable True → False (narrowed)",
            })
    return flags


def _render_breaking_summary(
    prior_version: str, current_version: str, flags: List[Dict[str, Any]],
) -> str:
    """Return a markdown summary of the diff."""
    lines = [
        f"# Contract breaking change: `{prior_version}` → `{current_version}`",
        "",
        f"**{len(flags)} breaking change(s) detected:**",
        "",
        "| Kind | Column | Detail |",
        "|---|---|---|",
    ]
    for f in flags:
        lines.append(
            f"| `{f.get('kind','')}` | `{f.get('column','')}` | {f.get('detail','')} |"
        )
    return "\n".join(lines)


# --------------------------------------------------------------------------
# Run the whole contract (schema + freshness + sla)
# --------------------------------------------------------------------------


def _resolve_custom_check(entry: Dict[str, Any]) -> Callable:
    """Turn a `contract.checks` entry into a callable `fn(df) -> bool|dict`.

    Supports:
      - `{'python': 'mod.path:fn_name'}` — resolved via importlib
      - `{'python': <callable>}` — used directly (inline Python)
    """
    py = entry.get("python")
    if callable(py):
        return py
    if isinstance(py, str) and ":" in py:
        import importlib
        mod_path, fn_name = py.rsplit(":", 1)
        return getattr(importlib.import_module(mod_path.strip()), fn_name.strip())
    raise ValueError(
        f"contract.checks entry {entry.get('name')!r} must have 'python': 'mod:fn' or a callable"
    )


def _run_custom_checks(df, contract: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Run each user-defined `contract.checks` entry against the DataFrame.

    Each entry's callable receives the DataFrame and returns EITHER:
      - `bool` — True=passed, False=failed (description falls back to entry's)
      - `dict` — `{passed: bool, description?: str, metadata?: dict}`

    Exceptions from the callable → check FAILS with the exception message
    as description. Never blocks other checks from running.
    """
    results = []
    for entry in (contract.get("checks") or []):
        name = entry.get("name") or "custom_check"
        default_desc = entry.get("description") or f"Custom check {name!r}"
        try:
            fn = _resolve_custom_check(entry)
            out = fn(df)
            if isinstance(out, bool):
                results.append({"name": name, "passed": out,
                                "description": default_desc, "metadata": {}})
            elif isinstance(out, dict) and "passed" in out:
                results.append({
                    "name": name,
                    "passed": bool(out["passed"]),
                    "description": out.get("description") or default_desc,
                    "metadata": out.get("metadata") or {},
                })
            else:
                results.append({
                    "name": name, "passed": False,
                    "description": f"custom check {name!r} returned {type(out).__name__}; expected bool or dict",
                    "metadata": {},
                })
        except Exception as e:  # noqa: BLE001
            results.append({
                "name": name, "passed": False,
                "description": f"custom check {name!r} raised {type(e).__name__}: {e}",
                "metadata": {"exception_class": type(e).__name__},
            })
    return results


def _run_contract(
    df,
    contract: Dict[str, Any],
    context: Any,
    asset_key: Any,
) -> List[Dict[str, Any]]:
    """Return list of check results (schema + custom + freshness + sla).

    Each entry: `{name, passed, description, metadata}`. Order:
    schema (one per column), then custom (one per contract.checks entry),
    then row-count SLA, then freshness.
    """
    results: List[Dict[str, Any]] = []

    # Schema checks — one per column entry.
    schema = contract.get("schema") or []
    for col_spec in schema:
        results.append(_validate_column(col_spec, df))

    # Custom checks — user-defined Python callables.
    results.extend(_run_custom_checks(df, contract))

    # Row-count SLA — compare against last materialization.
    sla_drop_pct = contract.get("sla_max_row_count_drop_pct")
    if sla_drop_pct is not None and context is not None and getattr(context, "instance", None):
        prior = _prior_row_count(context, asset_key)
        actual = int(len(df))
        if prior is not None and prior > 0:
            drop_pct = (prior - actual) / prior * 100.0
            passed = drop_pct <= float(sla_drop_pct)
            results.append({
                "name": "sla_row_count",
                "passed": passed,
                "description": (
                    f"row_count {actual} vs prior {prior} (drop {drop_pct:.1f}% "
                    f"<= allowed {sla_drop_pct}%)" if passed
                    else f"FAIL: row_count {actual} vs prior {prior} — "
                         f"drop {drop_pct:.1f}% > allowed {sla_drop_pct}%"
                ),
                "metadata": {
                    "actual_rows": actual,
                    "prior_rows": prior,
                    "drop_pct": round(drop_pct, 2),
                    "allowed_drop_pct": float(sla_drop_pct),
                },
            })
        else:
            results.append({
                "name": "sla_row_count",
                "passed": True,
                "description": "first materialization — no prior row_count to compare",
                "metadata": {"actual_rows": actual},
            })

    # Freshness — compare last materialization timestamp to now.
    fresh_lag = contract.get("freshness_max_lag_minutes")
    if fresh_lag is not None and context is not None and getattr(context, "instance", None):
        last_ts = _last_materialization_timestamp(context, asset_key)
        if last_ts is not None:
            lag_min = (time.time() - last_ts) / 60.0
            passed = lag_min <= float(fresh_lag)
            results.append({
                "name": "freshness",
                "passed": passed,
                "description": (
                    f"lag {lag_min:.1f}min <= max {fresh_lag}min" if passed
                    else f"FAIL: last materialization was {lag_min:.1f}min ago — "
                         f"exceeded max_lag={fresh_lag}min"
                ),
                "metadata": {
                    "lag_minutes": round(lag_min, 2),
                    "max_lag_minutes": float(fresh_lag),
                },
            })

    return results


def _emit_check_results(context: Any, results: List[Dict[str, Any]]):
    """Yield one AssetCheckResult per contract check."""
    for r in results:
        yield dg.AssetCheckResult(
            check_name=r["name"],
            passed=r["passed"],
            severity=dg.AssetCheckSeverity.ERROR if not r["passed"] else dg.AssetCheckSeverity.WARN,
            description=r["description"],
            metadata={k: _mdv(v) for k, v in (r.get("metadata") or {}).items()},
        )


def _mdv(v: Any):
    """Coerce a value to a typed MetadataValue."""
    if isinstance(v, bool):
        return dg.MetadataValue.bool(v)
    if isinstance(v, int):
        return dg.MetadataValue.int(v)
    if isinstance(v, float):
        return dg.MetadataValue.float(v)
    if isinstance(v, (list, dict)):
        return dg.MetadataValue.json(v)
    return dg.MetadataValue.text(str(v))


def _sanitize_tag_value(v: str) -> str:
    """Coerce a value to Dagster's strict tag-value allowlist ([A-Za-z0-9_.-],
    <=63 chars). Anything outside that set (e.g. '@' in emails, ':' in
    'team:<name>', ',' in joined lists) is replaced with '_'. Never raises.
    """
    s = re.sub(r"[^A-Za-z0-9_.\-]", "_", str(v))
    return s[:63]


def _emit_contract_observation(context: Any, asset_key: Any, contract: Dict[str, Any]):
    """Emit an AssetObservation tagged with contract version + owners +
    consumers, and metadata with the full contract snapshot so downstream
    (`@requires_contract`) and breaking-change diffs can inspect the whole
    schema.

    Tag VALUES are sanitized to Dagster's strict allowlist ([A-Za-z0-9_.-],
    <=63 chars) so contract owners can carry emails / 'team:X' prefixes
    without breaking the AssetObservation emission — the raw contract
    (with unmodified strings) is still available via the `contract_snapshot`
    metadata blob.
    """
    try:
        from dagster import AssetObservation
        owners = contract.get("owners") or []
        consumers = contract.get("consumers") or []
        tags = {
            _CONTRACT_VERSION_TAG: _sanitize_tag_value(contract.get("version") or ""),
            "contract_owners": _sanitize_tag_value(",".join(str(o) for o in owners)),
            "contract_consumers": _sanitize_tag_value(",".join(str(c) for c in consumers)),
            "data_contract": "true",
        }
        metadata = {
            "contract_snapshot": dg.MetadataValue.json(contract),
            _CONTRACT_VERSION_TAG: dg.MetadataValue.text(str(contract.get("version") or "")),
        }
        if hasattr(context, "log_event"):
            context.log_event(
                AssetObservation(asset_key=asset_key, tags=tags, metadata=metadata)
            )
    except Exception:  # noqa: BLE001
        pass


def _detect_and_emit_breaking_changes(
    context: Any,
    asset_key: Any,
    contract: Dict[str, Any],
    on_breaking_change: str,
) -> List[Dict[str, Any]]:
    """Look up the prior contract observation for `asset_key`, diff against
    `contract`, and emit an `AssetObservation` tagged
    `contract_breaking_change=true` if breaking flags fire.

    Returns the list of flags (empty if none / no prior contract).

    If `on_breaking_change == "fail"`, ALSO raises `dg.Failure` with the
    breaking flags in metadata.
    """
    prior = _latest_contract_observation(context, asset_key, before_now=True)
    if not prior:
        return []
    flags = _diff_contracts(prior, contract)
    if not flags:
        return []
    prior_version = str(prior.get("version") or "")
    current_version = str(contract.get("version") or "")
    summary_md = _render_breaking_summary(prior_version, current_version, flags)

    try:
        from dagster import AssetObservation
        if hasattr(context, "log_event"):
            context.log_event(AssetObservation(
                asset_key=asset_key,
                tags={
                    "contract_breaking_change": "true",
                    "contract_prior_version": prior_version,
                    "contract_current_version": current_version,
                },
                metadata={
                    "breaking_changes": dg.MetadataValue.json(flags),
                    "prior_version": dg.MetadataValue.text(prior_version),
                    "current_version": dg.MetadataValue.text(current_version),
                    "breaking_change_count": dg.MetadataValue.int(len(flags)),
                    "summary": dg.MetadataValue.md(summary_md),
                },
            ))
    except Exception:  # noqa: BLE001
        pass

    if on_breaking_change == "fail":
        raise dg.Failure(
            description=(
                f"data_contract breaking change {prior_version} → {current_version}: "
                + "; ".join(f["detail"] for f in flags)
            ),
            metadata={
                "breaking_changes": dg.MetadataValue.json(flags),
                "prior_version": dg.MetadataValue.text(prior_version),
                "current_version": dg.MetadataValue.text(current_version),
                "summary": dg.MetadataValue.md(summary_md),
            },
        )
    return flags


# --------------------------------------------------------------------------
# JSON Schema → DCC contract dict
# --------------------------------------------------------------------------

_JSON_SCHEMA_TYPE_MAP = {
    "string": "string",
    "integer": "int",
    "number": "float",
    "boolean": "bool",
    "array": "list",
    "object": "dict",
    "null": "string",
}


def contract_from_json_schema(
    schema: Union[str, Path, Dict[str, Any]],
    *,
    version: Optional[str] = None,
    owners: Optional[List[str]] = None,
    consumers: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Read a JSON Schema (file path OR dict) and return a DCC contract dict.

    Only top-level `type: object` schemas with a `properties` map are
    supported (the standard record/row shape). Walks `properties`, mapping
    JSON Schema types (`string`/`integer`/`number`/`boolean`/`array`/`object`)
    to pandas-friendly dtype names (`string`/`int`/`float`/`bool`/`list`/`dict`).

    Per-property behavior:
      - `type` → contract `type` (see map above); union types (`["string","null"]`)
        pick the non-null member and force `nullable=True`.
      - `required` (top-level list) → contract `nullable=False` for listed
        fields; every other field defaults `nullable=True`.
      - `pattern` (string field) → contract `regex`.
      - `enum` → contract `allowed_values`.
      - `minimum` / `maximum` → contract `min` / `max`.

    ```python
    from dagster_community_components import contract_from_json_schema, data_contract

    CONTRACT = contract_from_json_schema("orders.schema.json", version="1.0.0")

    @data_contract(contract=CONTRACT)
    @dg.asset
    def orders(context): ...
    ```
    """
    if isinstance(schema, (str, Path)):
        p = Path(schema)
        raw = json.loads(p.read_text())
    elif isinstance(schema, dict):
        raw = schema
    else:
        raise TypeError(
            f"contract_from_json_schema: expected str/Path/dict; got {type(schema).__name__}"
        )

    if raw.get("type") not in (None, "object"):
        raise ValueError(
            f"contract_from_json_schema: only top-level `type: object` schemas "
            f"are supported; got type={raw.get('type')!r}"
        )
    props = raw.get("properties") or {}
    if not isinstance(props, dict):
        raise ValueError(
            "contract_from_json_schema: schema `properties` must be a mapping"
        )
    required = set(raw.get("required") or [])

    columns: List[Dict[str, Any]] = []
    for col_name, spec in props.items():
        if not isinstance(spec, dict):
            continue
        js_type = spec.get("type")

        # Handle union types like ["string", "null"] → non-null member + nullable.
        forced_nullable = False
        if isinstance(js_type, list):
            non_null = [t for t in js_type if t != "null"]
            if "null" in js_type:
                forced_nullable = True
            js_type = non_null[0] if non_null else "string"

        dtype = _JSON_SCHEMA_TYPE_MAP.get(js_type or "", "string")

        # nullable — default True unless in `required`, and forced True by union-with-null.
        nullable = forced_nullable or (col_name not in required)

        col: Dict[str, Any] = {
            "name": col_name,
            "type": dtype,
            "nullable": nullable,
        }
        if "pattern" in spec:
            col["regex"] = spec["pattern"]
        if "enum" in spec and isinstance(spec["enum"], list):
            col["allowed_values"] = list(spec["enum"])
        if "minimum" in spec:
            col["min"] = spec["minimum"]
        if "maximum" in spec:
            col["max"] = spec["maximum"]
        columns.append(col)

    contract: Dict[str, Any] = {
        "version": version or raw.get("$id") or raw.get("title") or "1.0.0",
        "schema": columns,
    }
    if owners:
        contract["owners"] = list(owners)
    if consumers:
        contract["consumers"] = list(consumers)
    return contract


# --------------------------------------------------------------------------
# Public helper: derive AssetCheckSpecs from a contract dict.
#
# The compute-time enforcement (@data_contract) yields one AssetCheckResult
# per contract rule. For Dagster to render those in the check panel, the
# corresponding AssetCheckSpecs have to be declared on the asset itself
# (via @dg.asset(check_specs=[...])). Rather than making users hand-mirror
# every column into an AssetCheckSpec, this helper generates them from the
# contract — so the contract stays the single source of truth.
# --------------------------------------------------------------------------


def check_specs_for_contract(
    contract: Dict[str, Any],
    asset_name: str,
) -> "list":
    """Derive the AssetCheckSpecs a contract implies, so users can pass them
    to `@dg.asset(check_specs=…)` without duplicating what the contract
    already declares.

    Emits one spec per column in `contract['schema']`, one per entry in
    `contract['checks']` (user-defined custom checks), plus one each for
    `sla_max_row_count_drop_pct` and `freshness_max_lag_minutes` when set.

    ```python
    CONTRACT = {...}

    @dg.asset(check_specs=check_specs_for_contract(CONTRACT, "orders"))
    @data_contract(CONTRACT)
    def orders(context): ...
    ```
    """
    import dagster as dg
    specs = []
    for col_spec in (contract.get("schema") or []):
        n = col_spec.get("name")
        if n:
            specs.append(dg.AssetCheckSpec(
                name=f"schema_{n}",
                asset=dg.AssetKey.from_user_string(asset_name),
                description=f"Column {n!r} conforms to contract",
            ))
    for check_entry in (contract.get("checks") or []):
        n = check_entry.get("name")
        if n:
            specs.append(dg.AssetCheckSpec(
                name=n,
                asset=dg.AssetKey.from_user_string(asset_name),
                description=check_entry.get("description") or f"Custom contract check {n!r}",
            ))
    if contract.get("sla_max_row_count_drop_pct") is not None:
        specs.append(dg.AssetCheckSpec(
            name="sla_row_count",
            asset=dg.AssetKey.from_user_string(asset_name),
            description="Row count did not drop more than SLA allows",
        ))
    if contract.get("freshness_max_lag_minutes") is not None:
        specs.append(dg.AssetCheckSpec(
            name="freshness",
            asset=dg.AssetKey.from_user_string(asset_name),
            description="Materialization within contract freshness window",
        ))
    return specs


# --------------------------------------------------------------------------
# @data_contract decorator
# --------------------------------------------------------------------------


def _make_contract_compute(
    fn: Callable,
    contract: Dict[str, Any],
    on_violation: str,
    detect_breaking_changes: bool = False,
    on_breaking_change: str = "warn",
) -> Callable:
    """Wrap `fn` so calling it: (1) invokes the compute, (2) runs contract
    checks against the returned DataFrame, (3) yields one AssetCheckResult
    per rule + one Output with typed metadata + one AssetObservation, and
    (4) raises dg.Failure on block-mode violations.

    Shared by both @data_contract shapes (function-wrapping + AssetsDefinition-wrapping).
    """
    @functools.wraps(fn)
    def _wrapped(*args, **kwargs):
        import pandas as pd

        context = None
        if args and hasattr(args[0], "log"):
            context = args[0]
        elif "context" in kwargs and hasattr(kwargs["context"], "log"):
            context = kwargs["context"]
        if context is None:
            raise RuntimeError(
                "@data_contract requires a Dagster context — decorator "
                "must wrap a Dagster asset/op compute function."
            )

        df = fn(*args, **kwargs)
        if not isinstance(df, pd.DataFrame):
            raise TypeError(
                f"@data_contract: compute must return a pandas DataFrame; got {type(df).__name__}."
            )

        asset_key = getattr(context, "asset_key", None)

        # Breaking-change detection uses the PRIOR observation, so run
        # BEFORE we emit the current contract observation.
        if detect_breaking_changes:
            _detect_and_emit_breaking_changes(
                context, asset_key, contract, on_breaking_change,
            )

        results = _run_contract(df, contract, context, asset_key)
        all_passed = all(r["passed"] for r in results)

        for res in _emit_check_results(context, results):
            yield res
        _emit_contract_observation(context, asset_key, contract)

        metadata = {
            _CONTRACT_ROWCOUNT_TAG: dg.MetadataValue.int(len(df)),
            "contract_version": dg.MetadataValue.text(str(contract.get("version") or "")),
            "contract_check_summary": dg.MetadataValue.text(
                f"{sum(1 for r in results if r['passed'])}/{len(results)} passed"
            ),
            "contract_owners": dg.MetadataValue.json(contract.get("owners") or []),
            "contract_consumers": dg.MetadataValue.json(contract.get("consumers") or []),
            "all_passed": dg.MetadataValue.bool(all_passed),
        }

        if on_violation == "block" and not all_passed:
            failed = ", ".join(r["name"] for r in results if not r["passed"])
            raise dg.Failure(
                description=f"data_contract violation — failed checks: {failed}",
                metadata={
                    **metadata,
                    "failed_checks": dg.MetadataValue.json([
                        {"name": r["name"], "description": r["description"]}
                        for r in results if not r["passed"]
                    ]),
                },
            )

        yield dg.Output(df, metadata=metadata)

    return _wrapped


def _wrap_assets_definition(
    assets_def,
    contract: Dict[str, Any],
    on_violation: str,
    detect_breaking_changes: bool = False,
    on_breaking_change: str = "warn",
):
    """Rebuild a single-asset `@dg.asset` output with:
      - check_specs derived from the contract (no hand-mirroring)
      - compute wrapped to emit AssetCheckResults + do enforcement

    Preserves every attribute the user set on `@dg.asset` (group_name, tags,
    owners, partitions_def, code_version, ins, description, kinds, etc.) —
    only check_specs is *added* and only compute is *wrapped*.
    """
    if len(assets_def.keys) != 1:
        raise ValueError(
            "@data_contract on an AssetsDefinition supports single-asset shapes only. "
            "For @dg.multi_asset, apply @data_contract before @dg.multi_asset and "
            "pass check_specs=check_specs_for_contract(CONTRACT, name) explicitly."
        )

    asset_key = next(iter(assets_def.keys))
    asset_name = asset_key.to_user_string()
    spec = assets_def.get_asset_spec(asset_key)

    # Extract the raw user function so we can re-decorate with @dg.asset.
    node_def = assets_def.node_def
    compute = getattr(node_def, "compute_fn", None)
    raw_fn = getattr(compute, "decorated_fn", None)
    if raw_fn is None:
        raise RuntimeError(
            "@data_contract could not extract the compute function from the "
            "AssetsDefinition — apply @data_contract BEFORE @dg.asset instead, "
            "and use check_specs_for_contract() manually."
        )

    # Rebuild `ins` from the original asset's input wiring.
    ins = {
        input_name: dg.AssetIn(key=dep_key)
        for input_name, dep_key in assets_def.keys_by_input_name.items()
    }

    check_specs = check_specs_for_contract(contract, asset_name)
    wrapped_compute = _make_contract_compute(
        raw_fn, contract, on_violation,
        detect_breaking_changes=detect_breaking_changes,
        on_breaking_change=on_breaking_change,
    )

    return dg.asset(
        key=asset_key,
        description=spec.description,
        group_name=spec.group_name,
        owners=list(spec.owners) if spec.owners else None,
        tags=dict(spec.tags) if spec.tags else None,
        metadata=dict(spec.metadata) if spec.metadata else None,
        code_version=spec.code_version,
        partitions_def=assets_def.partitions_def,
        automation_condition=spec.automation_condition,
        kinds=set(spec.kinds) if spec.kinds else None,
        check_specs=check_specs,
        ins=ins if ins else None,
    )(wrapped_compute)


def data_contract(
    contract: Dict[str, Any],
    *,
    on_violation: str = "block",
    detect_breaking_changes: bool = False,
    on_breaking_change: str = "warn",
) -> Callable:
    """Enforce a data contract on a Dagster asset compute.

    Everything about the asset — the contract itself AND the fact that
    it's a Dagster asset — declared in one visible block. No module-level
    contract variable, no hand-written `AssetCheckSpec` list. When
    applied AFTER `@dg.asset`, the decorator reads the `AssetsDefinition`,
    derives check_specs from the contract, and rebuilds the asset with
    them merged in. `@dg.asset` keeps all its standard kwargs
    (`group_name`, `owners`, `tags`, `partitions_def`, `code_version`,
    `metadata`, `kinds`, `automation_condition`, `ins`, ...).

    ```python
    from dagster_community_components import data_contract

    @data_contract(
        contract={
            'version': '1.2.0',
            'owners': ['data-platform@example.com'],
            'consumers': ['analytics-team'],
            'schema': [
                {'name': 'order_id', 'type': 'int64',   'nullable': False, 'unique': True},
                {'name': 'amount',   'type': 'float64', 'nullable': False, 'min': 0, 'max': 1_000_000},
                {'name': 'currency', 'type': 'string',  'allowed_values': ['USD', 'EUR', 'GBP']},
                {'name': 'email',    'type': 'string',  'regex': '^[^@]+@[^@]+[.][^@]+$'},
            ],
            'checks': [   # custom asset checks — any Python callable
                {
                    'name': 'orders_total_matches_line_items',
                    'description': 'amount equals sum of line items',
                    'python': 'my_project.checks:validate_order_totals',
                },
            ],
            'freshness_max_lag_minutes': 60,
            'sla_max_row_count_drop_pct': 20,
        },
        on_violation='block',
    )
    @dg.asset(group_name='revenue', owners=['data-team@example.com'])
    def orders(context):
        return build_orders()
    ```

    Each entry in `contract['checks']` becomes its own AssetCheckSpec +
    runtime AssetCheckResult. The callable receives the DataFrame and
    returns either a `bool` (True = passed) or a
    `{'passed': bool, 'description'?: str, 'metadata'?: dict}` dict for
    richer failure reporting.

    **Custom-checks escape hatch — applied BEFORE `@dg.asset`.** Use when
    you need `AssetCheckSpec`s beyond what the contract implies. Requires
    the contract as a variable so you can splat contract-derived specs
    alongside your own:

    ```python
    from dagster_community_components import data_contract, check_specs_for_contract

    @dg.asset(check_specs=[
        *check_specs_for_contract(CONTRACT, 'orders'),
        dg.AssetCheckSpec(name='downstream_reconciliation', asset='orders'),
    ])
    @data_contract(CONTRACT, on_violation='block')
    def orders(context): ...
    ```

    **Enforcement semantics** (both shapes):
    - `on_violation='block'` (default) — any failing check → `dg.Failure`,
      asset does NOT materialize, downstream doesn't fire.
    - `on_violation='warn'` — asset materializes; failing checks visible in
      the check panel; downstream can gate via `AutomationCondition.eager()`.

    Every rule becomes one `AssetCheckResult` — visible in the asset-check
    panel with typed metadata (actual dtype, null count, drop_pct, etc.).
    """
    if on_violation not in ("block", "warn"):
        raise ValueError(f"on_violation must be 'block' or 'warn'; got {on_violation!r}")
    if on_breaking_change not in ("warn", "fail"):
        raise ValueError(
            f"on_breaking_change must be 'warn' or 'fail'; got {on_breaking_change!r}"
        )

    def _decorator(target):
        # Shape A: applied AFTER @dg.asset — target is an AssetsDefinition.
        if isinstance(target, dg.AssetsDefinition):
            return _wrap_assets_definition(
                target, contract, on_violation,
                detect_breaking_changes=detect_breaking_changes,
                on_breaking_change=on_breaking_change,
            )
        # Shape B: applied BEFORE @dg.asset — target is a raw function.
        if callable(target):
            return _make_contract_compute(
                target, contract, on_violation,
                detect_breaking_changes=detect_breaking_changes,
                on_breaking_change=on_breaking_change,
            )
        raise TypeError(
            f"@data_contract must decorate a function or AssetsDefinition; got {type(target).__name__}"
        )

    return _decorator


# --------------------------------------------------------------------------
# DataContractComponent — YAML-defined new asset
# --------------------------------------------------------------------------


class DataContractComponent(dg.Component, dg.Model, dg.Resolvable):
    """YAML shape of the data contract enforcement. Defines a new asset whose
    compute is referenced by `compute.python: 'mod:fn'`.

    For an EXISTING asset defined in Python, use the `@data_contract`
    decorator instead — same engine, no YAML.
    """

    asset_name: str = Field(description="Dagster asset name.")
    upstream_asset_key: Optional[str] = Field(
        default=None, description="Optional upstream asset passed to compute.",
    )
    compute: Dict[str, Any] = Field(
        description="`{kind: python, python: 'mod:fn'}`. Returns pandas DataFrame."
    )
    contract: Dict[str, Any] = Field(
        description=(
            "Contract config: `{version, owners, consumers, schema, "
            "freshness_max_lag_minutes?, sla_max_row_count_drop_pct?}`. "
            "schema is a list of `{name, type?, nullable?, unique?, "
            "min?, max?, allowed_values?, regex?}` entries — one per column."
        ),
    )
    on_violation: str = Field(
        default="block",
        description=(
            "'block' (default) raises dg.Failure on any check fail — asset does "
            "not materialize. 'warn' materializes anyway; downstream can block "
            "via AutomationCondition.eager() on the failing check."
        ),
    )
    detect_breaking_changes: bool = Field(
        default=False,
        description=(
            "Compare current contract to the prior emission via event log. "
            "Emits `contract_breaking_change` observation on dropped columns / "
            "narrowed types / nullable→non-nullable transitions."
        ),
    )
    on_breaking_change: str = Field(
        default="warn",
        description=(
            "When `detect_breaking_changes: true`, controls what happens on a "
            "breaking flag: 'warn' (default — just emit observation) or 'fail' "
            "(also raise dg.Failure and block materialization)."
        ),
    )

    # Catalog / governance
    group_name: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    tags: Optional[Dict[str, str]] = Field(default=None)
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds. Default: ['python', 'contract', 'governance'].",
    )

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Data Contract", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        compute = dict(self.compute)
        contract = dict(self.contract)
        on_violation = self.on_violation
        detect_breaking_changes = bool(self.detect_breaking_changes)
        on_breaking_change = self.on_breaking_change

        if on_violation not in ("block", "warn"):
            raise ValueError(f"on_violation must be block|warn; got {on_violation!r}")
        if on_breaking_change not in ("warn", "fail"):
            raise ValueError(
                f"on_breaking_change must be warn|fail; got {on_breaking_change!r}"
            )

        kinds_set = set(self.kinds or []) | {"python", "contract", "governance"}
        tag_map = dict(self.tags or {})
        for k in kinds_set:
            tag_map[f"dagster/kind/{k}"] = ""

        # Owners on the asset itself come from contract.owners if not overridden.
        owners = self.owners or contract.get("owners") or []

        # Contract → AssetCheckSpecs (helper is public — see @data_contract usage)
        check_specs = check_specs_for_contract(contract, asset_name)

        ins = {}
        if upstream_asset_key:
            ins["upstream"] = dg.AssetIn(key=dg.AssetKey.from_user_string(upstream_asset_key))

        contract_version = str(contract.get("version") or "")

        @dg.asset(
            key=dg.AssetKey.from_user_string(asset_name),
            description=self.description or f"Contract-enforced asset {asset_name} (v{contract_version})",
            group_name=self.group_name,
            owners=owners,
            tags=tag_map,
            kinds=kinds_set,
            check_specs=check_specs,
            ins=ins,
            code_version=contract_version or None,   # <-- contract version drives code_version
        )
        def _contract_asset(context: dg.AssetExecutionContext, **kwargs):
            import pandas as pd

            # Resolve compute.python callable.
            kind = (compute.get("kind") or "python").lower()
            if kind != "python":
                raise ValueError(f"DataContractComponent v1 supports compute.kind=python only; got {kind!r}")
            ref = compute.get("python")
            if not ref or ":" not in ref:
                raise ValueError("compute.python must be 'module.path:function_name'")
            mod_path, fn_name = ref.rsplit(":", 1)
            fn = getattr(importlib.import_module(mod_path.strip()), fn_name.strip(), None)
            if not callable(fn):
                raise ValueError(f"compute.python {ref!r} not callable")

            import inspect
            sig = inspect.signature(fn)
            n_positional = sum(1 for p in sig.parameters.values()
                               if p.kind in (p.POSITIONAL_OR_KEYWORD, p.POSITIONAL_ONLY))
            if n_positional == 0:
                df = fn()
            elif n_positional == 1:
                df = fn(context)
            else:
                df = fn(context, kwargs.get("upstream"))

            if not isinstance(df, pd.DataFrame):
                raise TypeError(f"compute must return a DataFrame; got {type(df).__name__}")

            asset_key = getattr(context, "asset_key", None)

            # Breaking-change detection uses the PRIOR observation, so run
            # BEFORE we emit the current contract observation.
            if detect_breaking_changes:
                _detect_and_emit_breaking_changes(
                    context, asset_key, contract, on_breaking_change,
                )

            results = _run_contract(df, contract, context, asset_key)
            all_passed = all(r["passed"] for r in results)

            # Yield AssetCheckResult per contract rule + emit contract observation.
            for res in _emit_check_results(context, results):
                yield res
            _emit_contract_observation(context, asset_key, contract)

            metadata = {
                _CONTRACT_ROWCOUNT_TAG: dg.MetadataValue.int(len(df)),
                "contract_version": dg.MetadataValue.text(contract_version),
                "contract_check_summary": dg.MetadataValue.text(
                    f"{sum(1 for r in results if r['passed'])}/{len(results)} passed"
                ),
                "contract_owners": dg.MetadataValue.json(contract.get("owners") or []),
                "contract_consumers": dg.MetadataValue.json(contract.get("consumers") or []),
                "all_passed": dg.MetadataValue.bool(all_passed),
            }

            if on_violation == "block" and not all_passed:
                failed = ", ".join(r["name"] for r in results if not r["passed"])
                raise dg.Failure(
                    description=f"data_contract violation — failed checks: {failed}",
                    metadata={
                        **metadata,
                        "failed_checks": dg.MetadataValue.json([
                            {"name": r["name"], "description": r["description"]}
                            for r in results if not r["passed"]
                        ]),
                    },
                )

            yield dg.Output(df, metadata=metadata)

        return dg.Definitions(assets=[_contract_asset])


# --------------------------------------------------------------------------
# @requires_contract — consumer-side enforcement
# --------------------------------------------------------------------------


def _enforce_contract_requirement(
    context: Any,
    upstream: str,
    min_version: Optional[str],
    require_columns: Optional[List[str]],
) -> Dict[str, Any]:
    """Look up the most recent `data_contract` observation for `upstream` and
    verify it meets the requirement. Raises `dg.Failure` on any mismatch;
    otherwise logs a `requires_contract_satisfied` observation and returns
    the resolved contract snapshot.
    """
    upstream_key = dg.AssetKey.from_user_string(upstream)
    contract = _latest_contract_observation(context, upstream_key)
    if contract is None:
        raise dg.Failure(
            description=(
                f"no data_contract on upstream asset {upstream!r} — cannot enforce "
                "requirement. Ensure the upstream asset is wrapped with @data_contract "
                "or a DataContractComponent and has been materialized at least once."
            ),
            metadata={
                "upstream": dg.MetadataValue.text(upstream),
                "required_min_version": dg.MetadataValue.text(str(min_version or "")),
            },
        )

    contract_version = str(contract.get("version") or "")
    version_ok = True
    if min_version:
        try:
            version_ok = not _semver_lt(contract_version, min_version)
        except ValueError as e:
            raise dg.Failure(
                description=(
                    f"requires_contract: could not parse contract version {contract_version!r} "
                    f"against required {min_version!r}: {e}"
                ),
                metadata={
                    "upstream": dg.MetadataValue.text(upstream),
                    "upstream_contract_version": dg.MetadataValue.text(contract_version),
                    "required_min_version": dg.MetadataValue.text(min_version),
                },
            ) from e
        if not version_ok:
            raise dg.Failure(
                description=(
                    f"upstream contract version {contract_version} < required {min_version}"
                ),
                metadata={
                    "upstream": dg.MetadataValue.text(upstream),
                    "upstream_contract_version": dg.MetadataValue.text(contract_version),
                    "required_min_version": dg.MetadataValue.text(min_version),
                },
            )

    missing_cols: List[str] = []
    if require_columns:
        schema = contract.get("schema") or []
        present = {c.get("name") for c in schema if c.get("name")}
        missing_cols = [c for c in require_columns if c not in present]
        if missing_cols:
            raise dg.Failure(
                description=(
                    f"upstream contract missing required column(s): "
                    f"{', '.join(missing_cols)}"
                ),
                metadata={
                    "upstream": dg.MetadataValue.text(upstream),
                    "upstream_contract_version": dg.MetadataValue.text(contract_version),
                    "required_columns": dg.MetadataValue.json(list(require_columns)),
                    "missing_columns": dg.MetadataValue.json(missing_cols),
                    "present_columns": dg.MetadataValue.json(sorted(list(present))),
                },
            )

    # Emit success observation on the DOWNSTREAM asset (the consumer).
    try:
        from dagster import AssetObservation
        asset_key = getattr(context, "asset_key", None)
        if hasattr(context, "log_event") and asset_key is not None:
            context.log_event(AssetObservation(
                asset_key=asset_key,
                tags={
                    "requires_contract_satisfied": "true",
                    "upstream": upstream,
                    "upstream_contract_version": contract_version,
                    "required_min_version": str(min_version or ""),
                    "version_ok": "true" if version_ok else "false",
                },
                metadata={
                    "upstream": dg.MetadataValue.text(upstream),
                    "upstream_contract_version": dg.MetadataValue.text(contract_version),
                    "required_min_version": dg.MetadataValue.text(str(min_version or "")),
                    "required_columns": dg.MetadataValue.json(list(require_columns or [])),
                    "version_ok": dg.MetadataValue.bool(version_ok),
                },
            ))
    except Exception:  # noqa: BLE001
        pass

    return contract


def requires_contract(
    upstream: str,
    *,
    min_version: Optional[str] = None,
    require_columns: Optional[List[str]] = None,
) -> Callable:
    """Consumer-side counterpart to `@data_contract`.

    Before the wrapped compute runs, look up the most recent
    `data_contract` observation for `upstream` and verify:

    - A contract observation exists on `upstream` (else Failure).
    - `min_version` (semver `X.Y.Z`) — upstream contract version must
      be `>= min_version` (else Failure).
    - `require_columns` — every listed column must be declared in the
      upstream contract's schema (else Failure).

    On success, emits `AssetObservation(requires_contract_satisfied=true)`
    on the downstream (consumer) asset — searchable in the event log.

    ```python
    from dagster_community_components import requires_contract

    @dg.asset(deps=["orders"])
    @requires_contract(
        upstream="orders",
        min_version="1.2.0",
        require_columns=["order_id", "user_id", "amount"],
    )
    def daily_revenue(context, orders):
        ...
    ```
    """
    def _decorator(fn: Callable) -> Callable:
        @functools.wraps(fn)
        def _wrapped(*args, **kwargs):
            context = None
            if args and hasattr(args[0], "log"):
                context = args[0]
            elif "context" in kwargs and hasattr(kwargs["context"], "log"):
                context = kwargs["context"]
            if context is None:
                raise RuntimeError(
                    "@requires_contract requires a Dagster context — decorator "
                    "must wrap a Dagster asset/op compute function."
                )
            _enforce_contract_requirement(context, upstream, min_version, require_columns)
            return fn(*args, **kwargs)

        return _wrapped

    return _decorator


class RequiresContractComponent(dg.Component, dg.Model, dg.Resolvable):
    """YAML shape of `@requires_contract` — consumer-side contract enforcement.

    Two authoring modes:

    1. **Wrap an existing DCC component** via `wraps: {type, attributes}`.
       The inner component's asset(s) each get their compute gated by the
       upstream-contract check.

    2. **Define a new asset from scratch** via `asset_name` + `compute`.
       Behaves the same, but launches a fresh asset that runs the referenced
       Python callable AFTER the contract check.

    Either mode raises `dg.Failure` before compute if:
      - No `data_contract` observation exists on `upstream`
      - `min_version` (semver) is higher than the upstream contract's version
      - `require_columns` names any column not in the upstream contract's schema

    On success, emits an `AssetObservation(requires_contract_satisfied=true)`
    on the downstream asset.
    """

    asset_name: Optional[str] = Field(
        default=None,
        description="Dagster asset name. Required when NOT using `wraps:` (inherited in wraps mode).",
    )
    compute: Optional[Dict[str, Any]] = Field(
        default=None,
        description="`{kind: python, python: 'mod:fn'}`. Mutually exclusive with `wraps`.",
    )
    wraps: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Wrap another DCC component's asset(s) with a contract requirement instead of "
            "defining new compute. Shape: `{type: 'dagster_community_components.<Component>', "
            "attributes: {...}}`. Mutually exclusive with `compute`."
        ),
    )

    upstream: str = Field(
        description="Upstream asset key whose data_contract to enforce.",
    )
    min_version: Optional[str] = Field(
        default=None,
        description="Semver X.Y.Z — upstream contract must be >= this. Omit to only require presence.",
    )
    require_columns: Optional[List[str]] = Field(
        default=None,
        description="Optional list of column names that MUST be declared in the upstream contract's schema.",
    )

    # Governance
    group_name: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    tags: Optional[Dict[str, str]] = Field(default=None)
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds. Default: ['python', 'contract', 'consumer'].",
    )

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Requires Contract", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        if self.wraps is not None and self.compute is not None:
            raise ValueError(
                "RequiresContractComponent: supply exactly ONE of `wraps` or `compute`."
            )
        if self.wraps is not None:
            return self._build_wrapped(context)
        if self.compute is None:
            raise ValueError(
                "RequiresContractComponent: supply either `compute` or `wraps`."
            )
        return self._build_new_asset(context)

    def _build_new_asset(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        if not self.asset_name:
            raise ValueError(
                "RequiresContractComponent: `asset_name` required when using `compute:`."
            )
        asset_name = self.asset_name
        compute = dict(self.compute or {})
        upstream = self.upstream
        min_version = self.min_version
        require_columns = list(self.require_columns or []) or None

        kinds_set = set(self.kinds or []) | {"python", "contract", "consumer"}
        tag_map = dict(self.tags or {})
        for k in kinds_set:
            tag_map[f"dagster/kind/{k}"] = ""

        upstream_key = dg.AssetKey.from_user_string(upstream)

        @dg.asset(
            key=dg.AssetKey.from_user_string(asset_name),
            description=(
                self.description
                or f"Consumer asset {asset_name} — requires contract on {upstream!r}"
                + (f" (>= v{min_version})" if min_version else "")
            ),
            group_name=self.group_name,
            owners=self.owners or [],
            tags=tag_map,
            kinds=kinds_set,
            deps=[upstream_key],
        )
        def _consumer_asset(context: dg.AssetExecutionContext):
            _enforce_contract_requirement(
                context, upstream, min_version, require_columns,
            )

            kind = (compute.get("kind") or "python").lower()
            if kind != "python":
                raise ValueError(
                    f"RequiresContractComponent v1 supports compute.kind=python only; "
                    f"got {kind!r}"
                )
            ref = compute.get("python")
            if not ref or ":" not in ref:
                raise ValueError("compute.python must be 'module.path:function_name'")
            mod_path, fn_name = ref.rsplit(":", 1)
            fn = getattr(importlib.import_module(mod_path.strip()), fn_name.strip(), None)
            if not callable(fn):
                raise ValueError(f"compute.python {ref!r} not callable")

            import inspect
            sig = inspect.signature(fn)
            n_positional = sum(1 for p in sig.parameters.values()
                               if p.kind in (p.POSITIONAL_OR_KEYWORD, p.POSITIONAL_ONLY))
            if n_positional == 0:
                out = fn()
            else:
                out = fn(context)

            return dg.MaterializeResult(
                metadata={
                    "requires_contract_upstream": dg.MetadataValue.text(upstream),
                    "required_min_version": dg.MetadataValue.text(str(min_version or "")),
                    "required_columns": dg.MetadataValue.json(list(require_columns or [])),
                    "consumer_output_type": dg.MetadataValue.text(type(out).__name__),
                }
            )

        return dg.Definitions(assets=[_consumer_asset])

    def _build_wrapped(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        inner = _resolve_inner_component_for_requires(self.wraps or {})
        inner_defs = inner.build_defs(context)

        upstream = self.upstream
        min_version = self.min_version
        require_columns = list(self.require_columns or []) or None

        wrapped_assets = []
        for asset_def in list(inner_defs.assets or []):
            if len(asset_def.keys) != 1:
                wrapped_assets.append(asset_def)
                continue
            wrapped_assets.append(
                self._wrap_single_asset(asset_def, upstream, min_version, require_columns)
            )

        return dg.Definitions(
            assets=wrapped_assets,
            resources=inner_defs.resources,
            sensors=inner_defs.sensors,
            schedules=inner_defs.schedules,
            asset_checks=inner_defs.asset_checks,
            jobs=inner_defs.jobs,
            loggers=inner_defs.loggers,
        )

    def _wrap_single_asset(
        self,
        asset_def: "dg.AssetsDefinition",
        upstream: str,
        min_version: Optional[str],
        require_columns: Optional[List[str]],
    ) -> "dg.AssetsDefinition":
        key = next(iter(asset_def.keys))
        specs_by_key = getattr(asset_def, "specs_by_key", {}) or {}
        spec = specs_by_key.get(key)

        inner_op = asset_def.op
        inner_compute = getattr(inner_op.compute_fn, "decorated_fn", None) or inner_op.compute_fn

        inner_kinds = set(getattr(spec, "kinds", None) or []) if spec else set()
        merged_kinds = inner_kinds | set(self.kinds or []) | {"contract", "consumer"}
        inner_tags = dict(getattr(spec, "tags", None) or {}) if spec else {}
        merged_tags = {**inner_tags, **(self.tags or {})}
        merged_owners = list((spec.owners if spec else []) or []) + (self.owners or [])
        inner_description = (spec.description if spec else None) or f"Consumer {key.to_user_string()}"
        merged_description = (
            f"{inner_description}  [requires_contract: {upstream}"
            + (f" >= v{min_version}" if min_version else "")
            + "]"
        )
        inner_deps = list(spec.deps) if (spec and getattr(spec, "deps", None)) else []
        upstream_key = dg.AssetKey.from_user_string(upstream)
        if not any(getattr(d, "asset_key", None) == upstream_key for d in inner_deps):
            inner_deps.append(dg.AssetDep(upstream_key))

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
        def _requires_wrapped(context: dg.AssetExecutionContext, **kwargs):
            _enforce_contract_requirement(
                context, upstream, min_version, require_columns,
            )
            return inner_compute(context, **kwargs)

        return _requires_wrapped


def _resolve_inner_component_for_requires(wraps: Dict[str, Any]):
    """Resolve `{type: 'mod.path.ClassName' OR 'mod.path:ClassName', attributes: {...}}` → component instance.

    Local to this module — the throttle_asset copy stays independent per DCC's
    'no shared code between components' rule.
    """
    type_str = wraps.get("type")
    attrs = wraps.get("attributes") or {}
    if not type_str or not isinstance(type_str, str):
        raise ValueError(
            "RequiresContractComponent.wraps requires `type: <fully-qualified-class-name>`."
        )
    if ":" in type_str:
        mod_path, cls_name = type_str.rsplit(":", 1)
    else:
        mod_path, cls_name = type_str.rsplit(".", 1)
    try:
        mod = importlib.import_module(mod_path)
    except ImportError as e:
        raise ValueError(
            f"RequiresContractComponent.wraps: cannot import module {mod_path!r}: {e}"
        ) from e
    cls = getattr(mod, cls_name, None)
    if cls is None:
        raise ValueError(
            f"RequiresContractComponent.wraps: {cls_name!r} not found in {mod_path!r}."
        )
    try:
        return cls(**attrs)
    except Exception as e:  # noqa: BLE001
        raise ValueError(
            f"RequiresContractComponent.wraps: constructing {type_str} failed: "
            f"{type(e).__name__}: {e}"
        ) from e
