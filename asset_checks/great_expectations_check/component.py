"""Great Expectations Check Component.

Runs a Great Expectations expectation suite against an asset and surfaces
EACH expectation as its own Dagster asset check. Mirrors the pattern that
the official `dagster-soda` integration uses for SodaCL: one external check
→ one Dagster asset check, so they're alertable, retriable, and visible
individually in the catalog UI.
"""
from pathlib import Path
from typing import Dict, List, Optional
import json

import dagster as dg
from pydantic import Field


class GreatExpectationsCheckComponent(dg.Component, dg.Model, dg.Resolvable):
    """Run a GE expectation suite as N Dagster asset checks (one per expectation).

    Example:
        ```yaml
        type: dagster_component_templates.GreatExpectationsCheckComponent
        attributes:
          asset_key: warehouse/orders
          ge_context_root_dir: ./great_expectations
          datasource_name: my_postgres
          data_asset_name: orders
          expectation_suite_name: orders.critical
          # Optional: force every check to WARN (overrides per-expectation severity)
          # severity_override: WARN
        ```

    Per-expectation severity is read from `expectation.meta.severity` in the
    suite JSON — set `meta: {severity: "WARN"}` on a specific expectation to
    let it warn instead of fail. Pass `severity_override` on the component
    to force the same severity across all expectations regardless.
    """

    asset_key: str = Field(description="Asset key to attach the checks to")
    ge_context_root_dir: str = Field(description="Path to the Great Expectations project root")
    datasource_name: str = Field(description="GE datasource name")
    data_asset_name: str = Field(description="GE data asset name")
    expectation_suite_name: str = Field(description="GE expectation suite name")
    severity_override: Optional[str] = Field(
        default=None,
        description=(
            "Optional severity override: 'WARN' or 'ERROR'. When unset (default), "
            "each Dagster check inherits severity from the expectation's "
            "`meta.severity` field, defaulting to ERROR if not set."
        ),
    )
    data_connector_name: str = Field(
        default="default_inferred_data_connector_name",
        description="GE data connector name (rarely needs changing)",
    )

    @classmethod
    def get_description(cls) -> str:
        return "Run a GE expectation suite — one Dagster asset check per expectation."

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        from dagster import (
            AssetCheckResult,
            AssetCheckSeverity,
            AssetCheckSpec,
            AssetKey,
            asset_check,
            multi_asset_check,
        )

        asset_key = AssetKey(self.asset_key.split("/"))
        _self = self

        # Try to pre-load the suite at build_defs time so we can declare one
        # Dagster check per expectation. If we can't read the suite (path not
        # ready yet, JSON malformed, GE v2 vs v3 differences), fall back to
        # one aggregate asset_check that yields the suite's pass/fail.
        suite_specs = _build_specs_from_suite(
            ge_context_root_dir=self.ge_context_root_dir,
            suite_name=self.expectation_suite_name,
            asset_key=asset_key,
        )

        if suite_specs:
            @multi_asset_check(
                specs=suite_specs,
                description=f"GE suite '{self.expectation_suite_name}' — {len(suite_specs)} expectations",
            )
            def ge_checks(context):
                try:
                    import great_expectations as gx
                except ImportError:
                    for spec in suite_specs:
                        yield AssetCheckResult(
                            check_name=spec.name,
                            passed=False,
                            severity=AssetCheckSeverity.ERROR,
                            metadata={"error": "great-expectations not installed"},
                        )
                    return

                try:
                    ge_context = gx.get_context(context_root_dir=_self.ge_context_root_dir)
                    validator = ge_context.get_validator(
                        datasource_name=_self.datasource_name,
                        data_connector_name=_self.data_connector_name,
                        data_asset_name=_self.data_asset_name,
                        expectation_suite_name=_self.expectation_suite_name,
                    )
                    validation_result = validator.validate()
                except Exception as e:
                    # Whole-suite failure — yield FAIL for every spec so the
                    # catalog reflects the outage.
                    for spec in suite_specs:
                        yield AssetCheckResult(
                            check_name=spec.name,
                            passed=False,
                            severity=AssetCheckSeverity.ERROR,
                            metadata={"error": f"GE validation failed: {e}"},
                        )
                    return

                # Map results back to specs by check name (deterministic order
                # preserved from suite parse → results).
                results_by_name = {}
                for er in validation_result.results:
                    name = _name_for_expectation(er.expectation_config.to_json_dict())
                    results_by_name[name] = er

                for spec in suite_specs:
                    er = results_by_name.get(spec.name)
                    if er is None:
                        # Suite was edited between build_defs and runtime.
                        yield AssetCheckResult(
                            check_name=spec.name,
                            passed=False,
                            severity=AssetCheckSeverity.WARN,
                            metadata={"error": "expectation no longer in suite"},
                        )
                        continue
                    yield AssetCheckResult(
                        check_name=spec.name,
                        passed=bool(er.success),
                        severity=_resolve_severity(
                            override=_self.severity_override,
                            expectation_meta=er.expectation_config.meta or {},
                        ),
                        metadata={
                            "expectation_type": er.expectation_config.expectation_type,
                            "kwargs": str(er.expectation_config.kwargs),
                            "result": str(er.result),
                        },
                    )

            return dg.Definitions(asset_checks=[ge_checks])

        # Fallback path: suite not parseable at build_defs time.
        @asset_check(
            asset=asset_key,
            description=f"Run GE suite: {self.expectation_suite_name} (aggregate)",
        )
        def ge_check_aggregate(context):
            severity = _resolve_severity(
                override=_self.severity_override, expectation_meta={}
            )
            try:
                import great_expectations as gx
            except ImportError:
                return AssetCheckResult(
                    passed=False,
                    severity=severity,
                    metadata={"error": "great-expectations not installed"},
                )
            try:
                ge_context = gx.get_context(context_root_dir=_self.ge_context_root_dir)
                validator = ge_context.get_validator(
                    datasource_name=_self.datasource_name,
                    data_connector_name=_self.data_connector_name,
                    data_asset_name=_self.data_asset_name,
                    expectation_suite_name=_self.expectation_suite_name,
                )
                result = validator.validate()
            except Exception as e:
                return AssetCheckResult(
                    passed=False, severity=severity, metadata={"error": str(e)}
                )
            stats = result.statistics
            return AssetCheckResult(
                passed=bool(result.success),
                severity=severity,
                metadata={
                    "evaluated_expectations": stats.get("evaluated_expectations", 0),
                    "successful_expectations": stats.get("successful_expectations", 0),
                    "unsuccessful_expectations": stats.get("unsuccessful_expectations", 0),
                    "suite_name": _self.expectation_suite_name,
                    "note": "Suite couldn't be pre-parsed at build_defs time — running as one aggregate check. Make sure the suite JSON exists at <ge_context_root_dir>/expectations/<name>.json for per-expectation checks.",
                },
            )

        return dg.Definitions(asset_checks=[ge_check_aggregate])


# ── helpers ────────────────────────────────────────────────────────────────


def _name_for_expectation(exp_dict: Dict) -> str:
    """Build a stable, human-readable name for one expectation."""
    etype = exp_dict.get("expectation_type", "expectation")
    kwargs = exp_dict.get("kwargs", {}) or {}
    column = kwargs.get("column")
    if column:
        return f"{etype}__{column}"
    column_a = kwargs.get("column_A")
    column_b = kwargs.get("column_B")
    if column_a and column_b:
        return f"{etype}__{column_a}__{column_b}"
    return etype


def _build_specs_from_suite(
    ge_context_root_dir: str,
    suite_name: str,
    asset_key,
) -> List:
    """Pre-parse the GE suite JSON to enumerate its expectations.

    Returns a list of AssetCheckSpec, or empty list if the suite can't be
    read at build_defs time (caller falls back to a single aggregate check).
    """
    from dagster import AssetCheckSpec

    # GE v3 default suite path
    suite_path = (
        Path(ge_context_root_dir).expanduser()
        / "expectations"
        / f"{suite_name.replace('.', '/')}.json"
    )
    if not suite_path.exists():
        # Try the dotted-name-as-filename variant
        alt = Path(ge_context_root_dir).expanduser() / "expectations" / f"{suite_name}.json"
        if alt.exists():
            suite_path = alt
        else:
            return []

    try:
        suite_data = json.loads(suite_path.read_text())
    except (OSError, json.JSONDecodeError):
        return []

    expectations = suite_data.get("expectations", [])
    if not expectations:
        return []

    specs = []
    seen = set()
    for exp in expectations:
        base = _name_for_expectation(exp)
        name = base
        i = 1
        while name in seen:
            i += 1
            name = f"{base}__{i}"
        seen.add(name)
        specs.append(
            AssetCheckSpec(
                name=name,
                asset=asset_key,
                description=f"GE: {exp.get('expectation_type', 'expectation')}",
            )
        )
    return specs


def _resolve_severity(*, override: Optional[str], expectation_meta: Dict):
    """Resolve severity for a single expectation.

    Priority:
      1. `severity_override` on the component (forces all checks to one severity).
      2. `meta.severity` on the expectation itself (`WARN` / `ERROR`).
      3. Default: ERROR.
    """
    from dagster import AssetCheckSeverity

    raw = override or (expectation_meta.get("severity") if expectation_meta else None)
    if raw and str(raw).upper() == "WARN":
        return AssetCheckSeverity.WARN
    return AssetCheckSeverity.ERROR
