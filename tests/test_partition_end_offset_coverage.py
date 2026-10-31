"""Repo-wide coverage test: every component carrying the `_build_partitions_def`
date-axis helper supports `end_offset`/`timezone` on its time-window
partitions (component-feedback: without them a daily/weekly/monthly/hourly/
cron partition set only ever holds COMPLETED windows -- a daily set never
includes today until tomorrow, so a notebook/asset meant to run intraday
under an eager AutomationCondition would keep re-running yesterday's
partition with yesterday's data instead of processing today).

Both are real, documented kwargs on every Dagster time-window partitions
definition (Daily/Weekly/Monthly/Hourly/TimeWindow), verified via
`inspect.signature` before this rollout (see integrations/fabric_workspace/
component.py, the reference implementation this was generalized from).

Same convention as `test_cron_partition_coverage.py`: not a per-component
hand test (681 of them share the helper) -- a single generic coverage sweep
that loads each module directly, introspects `_build_partitions_def`'s real
signature, calls it with `end_offset=1, timezone="America/New_York"` plus
whatever other params it has, and asserts those two values actually reach
the returned partitions_def object -- confirming the kwargs are wired all
the way through the function body, not just accepted and silently dropped.
Also confirms the owning Component class declares `partition_end_offset`/
`partition_timezone` as real pydantic fields.
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
    mod_name = f"_end_offset_coverage_{abs(hash(str(path)))}"
    spec = importlib.util.spec_from_file_location(mod_name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[mod_name] = mod
    try:
        spec.loader.exec_module(mod)
    finally:
        sys.modules.pop(mod_name, None)
    return mod


@pytest.mark.parametrize("path", FILES, ids=[str(p.relative_to(ROOT)) for p in FILES])
def test_build_partitions_def_supports_end_offset_and_timezone(path):
    mod = _load(path)
    fn = getattr(mod, "_build_partitions_def", None)
    assert fn is not None, f"{path}: no _build_partitions_def in module"

    sig = inspect.signature(fn)
    assert "end_offset" in sig.parameters, f"{path}: _build_partitions_def has no end_offset parameter"
    assert "timezone" in sig.parameters, f"{path}: _build_partitions_def has no timezone parameter"

    kwargs = {}
    for p in sig.parameters:
        if p == "partition_type":
            kwargs[p] = "daily"
        elif p == "partition_start":
            kwargs[p] = "2024-01-01"
        elif p in ("partition_values", "dynamic_partition_name", "partition_dimensions"):
            kwargs[p] = None
        elif p == "cron_schedule":
            kwargs[p] = None
        elif p == "cron_fmt":
            kwargs[p] = "%Y-%m-%d"
        elif p == "end_offset":
            kwargs[p] = 1
        elif p == "timezone":
            kwargs[p] = "America/New_York"
        elif sig.parameters[p].default is inspect.Parameter.empty:
            kwargs[p] = None

    result = fn(**kwargs)
    assert isinstance(result, dg.DailyPartitionsDefinition), (
        f"{path}: partition_type='daily' did not produce a DailyPartitionsDefinition, got {type(result)}"
    )
    assert result.end_offset == 1, f"{path}: end_offset=1 did not reach the returned partitions_def (got {result.end_offset})"
    assert result.timezone == "America/New_York", (
        f"{path}: timezone='America/New_York' did not reach the returned partitions_def (got {result.timezone})"
    )

    has_field = any(
        isinstance(obj, type)
        and "partition_end_offset" in getattr(obj, "model_fields", {})
        and "partition_timezone" in getattr(obj, "model_fields", {})
        for name in dir(mod)
        for obj in [getattr(mod, name)]
    )
    assert has_field, f"{path}: no class declares partition_end_offset/partition_timezone as fields"


def test_end_offset_includes_todays_partition_through_a_real_component():
    """Behavioral check (not just type-level): end_offset=1 actually makes
    the CURRENT day's partition exist, through one representative component
    (the fabric_workspace reference implementation this rollout generalized
    from)."""
    mod = _load(ROOT / "integrations/fabric_workspace/component.py")
    fn = mod._build_partitions_def
    import datetime
    pdef_default = fn("daily", "2024-01-01", None, None, None)
    pdef_offset = fn("daily", "2024-01-01", None, None, None, end_offset=1)
    today = datetime.datetime.now(datetime.timezone.utc)
    keys_default = pdef_default.get_partition_keys(current_time=today)
    keys_offset = pdef_offset.get_partition_keys(current_time=today)
    assert today.strftime("%Y-%m-%d") not in keys_default
    assert today.strftime("%Y-%m-%d") in keys_offset
