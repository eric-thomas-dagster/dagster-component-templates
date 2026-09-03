"""DataContractComponent + `@data_contract` — enforce data contracts at materialization.

A data contract is the formal agreement between a data producer and its
consumers: what columns exist, what types they are, which are nullable,
what ranges are allowed, how fresh it must be, who owns it, who consumes
it, and how it's versioned. This component enforces contracts as CODE —
every materialization validates the produced DataFrame against the
contract, emits one `AssetCheckResult` per rule, and either blocks
publish on violation OR materializes with failing checks that block
downstream via `AutomationCondition`.

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
import re
import time
from typing import Any, Callable, Dict, List, Optional

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
# Run the whole contract (schema + freshness + sla)
# --------------------------------------------------------------------------


def _run_contract(
    df,
    contract: Dict[str, Any],
    context: Any,
    asset_key: Any,
) -> List[Dict[str, Any]]:
    """Return list of check results (schema + freshness + sla).

    Each entry: `{name, passed, description, metadata}`. First one is
    always the row-count SLA (if configured); then per-column schema
    checks; then freshness (if configured).
    """
    results: List[Dict[str, Any]] = []

    # Schema checks — one per column entry.
    schema = contract.get("schema") or []
    for col_spec in schema:
        results.append(_validate_column(col_spec, df))

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


def _emit_contract_observation(context: Any, asset_key: Any, contract: Dict[str, Any]):
    """Emit an AssetObservation tagged with contract version + owners +
    consumers so downstream + agents can look up who owns this asset."""
    try:
        from dagster import AssetObservation
        tags = {
            _CONTRACT_VERSION_TAG: str(contract.get("version") or ""),
            "contract_owners": ",".join(contract.get("owners") or []),
            "contract_consumers": ",".join(contract.get("consumers") or []),
        }
        if hasattr(context, "log_event"):
            context.log_event(AssetObservation(asset_key=asset_key, tags=tags))
    except Exception:  # noqa: BLE001
        pass


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

    Emits one spec per column in `contract['schema']` plus one each for
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


def _make_contract_compute(fn: Callable, contract: Dict[str, Any], on_violation: str) -> Callable:
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
    assets_def, contract: Dict[str, Any], on_violation: str,
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
    wrapped_compute = _make_contract_compute(raw_fn, contract, on_violation)

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
                {'name': 'amount',   'type': 'float64', 'nullable': False, 'min': 0},
                {'name': 'currency', 'type': 'string',  'allowed_values': ['USD', 'EUR', 'GBP']},
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

    def _decorator(target):
        # Shape A: applied AFTER @dg.asset — target is an AssetsDefinition.
        if isinstance(target, dg.AssetsDefinition):
            return _wrap_assets_definition(target, contract, on_violation)
        # Shape B: applied BEFORE @dg.asset — target is a raw function.
        if callable(target):
            return _make_contract_compute(target, contract, on_violation)
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

        if on_violation not in ("block", "warn"):
            raise ValueError(f"on_violation must be block|warn; got {on_violation!r}")

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
