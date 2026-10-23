"""Repo-wide coverage test: every component carrying the `_build_partitions_def`
date-axis helper supports `partition_type='cron'` (component-feedback/
2026-10-21-lmr-registry-gaps.md -- "weekdays only" cadence for market-data
reconciliation that doesn't exist on weekends).

This isn't a per-component hand test (669 of them share the helper, with ~15
structural variants -- single-line dispatch, a `_pt` local alias, an `or
default` fallback, dict-keyed `parts[...]` call sites instead of `self.*`,
etc.) -- it is a single generic coverage sweep: for every component.py
defining `_build_partitions_def`, load the module directly, introspect the
function's real signature, call it with cron args matching whatever
parameters it actually has, and assert it returns a real
`dg.TimeWindowPartitionsDefinition` -- confirming cron support is wired
all the way through, not just that cron_schedule/cron_fmt parameters exist.
Also confirms the component class itself declares `partition_cron_schedule`
as a real pydantic field (not just accepted by the free function), since a
YAML user configures the component, not the helper directly.
"""
import importlib.util
import inspect
import sys
from pathlib import Path

import dagster as dg
import pytest

ROOT = Path(__file__).resolve().parent.parent


def _component_files():
    for p in sorted(ROOT.glob("**/component.py")):
        parts = p.relative_to(ROOT).parts
        if ".venv" in parts or "site-packages" in parts or parts[0] in ("dist", "quickstarts"):
            continue
        if "def _build_partitions_def(" in p.read_text():
            yield p


FILES = list(_component_files())


def test_covers_the_repo():
    assert len(FILES) >= 600


def _load(path: Path):
    mod_name = f"_cron_coverage_{abs(hash(str(path)))}"
    spec = importlib.util.spec_from_file_location(mod_name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[mod_name] = mod
    try:
        spec.loader.exec_module(mod)
    finally:
        sys.modules.pop(mod_name, None)
    return mod


@pytest.mark.parametrize("path", FILES, ids=[str(p.relative_to(ROOT)) for p in FILES])
def test_build_partitions_def_supports_cron(path):
    mod = _load(path)
    fn = getattr(mod, "_build_partitions_def", None)
    assert fn is not None, f"{path}: no _build_partitions_def in module"

    sig = inspect.signature(fn)
    kwargs = {}
    for p in sig.parameters:
        if p == "partition_type":
            kwargs[p] = "cron"
        elif p == "partition_start":
            kwargs[p] = "2024-01-01"
        elif p in ("partition_values", "dynamic_partition_name", "partition_dimensions"):
            kwargs[p] = None
        elif p == "cron_schedule":
            kwargs[p] = "0 0 * * 1-5"
        elif p == "cron_fmt":
            kwargs[p] = "%Y-%m-%d"
        elif sig.parameters[p].default is inspect.Parameter.empty:
            kwargs[p] = None

    assert "cron_schedule" in sig.parameters, f"{path}: _build_partitions_def has no cron_schedule parameter"
    result = fn(**kwargs)
    assert isinstance(result, dg.TimeWindowPartitionsDefinition), (
        f"{path}: partition_type='cron' did not produce a TimeWindowPartitionsDefinition, got {type(result)}"
    )

    has_field = any(
        isinstance(obj, type) and "partition_cron_schedule" in getattr(obj, "model_fields", {})
        for name in dir(mod)
        for obj in [getattr(mod, name)]
    )
    assert has_field, f"{path}: no Component class declares partition_cron_schedule as a field"


def test_weekday_cron_schedule_skips_weekends_through_a_real_component():
    """Behavioral check (not just type-level): the cron support actually builds a
    Mon-Fri-only axis, through one representative component (sql_transform)."""
    mod = _load(ROOT / "assets/transforms/sql_transform/component.py")
    comp = mod.SqlTransformComponent(
        asset_name="out", connection_url_env_var="X", sql="SELECT 1",
        partition_type="cron", partition_start="2024-01-01", partition_cron_schedule="0 0 * * 1-5",
    )
    pdef = comp.build_defs(None).assets[0].partitions_def
    keys = pdef.get_partition_keys(current_time=__import__("datetime").datetime(2024, 1, 10))
    assert "2024-01-05" in keys and "2024-01-06" not in keys and "2024-01-07" not in keys
