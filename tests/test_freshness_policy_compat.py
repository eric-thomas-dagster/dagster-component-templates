"""`freshness_max_lag_minutes` / `freshness_cron` build a FreshnessPolicy through an inlined compat helper.

Dagster replaced `FreshnessPolicy(maximum_lag_minutes=..., cron_schedule=...)` with `FreshnessPolicy.time_window(...)` /
`.cron(...)`, so the old constructor raises on current Dagster. Each component.py is self-contained (no helper files),
so the helper is inlined; this test guards against drift between the copies, against the old constructor creeping back,
and checks the helper's behaviour through one component's copy.
"""
import re
from datetime import timedelta
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
_BEGIN = "# BEGIN freshness-policy compat"
_END = "# END freshness-policy compat"
OLD_CALL = re.compile(r"\bFreshnessPolicy\(\s*maximum_lag_minutes=(?:self|comp)\.")


def _component_files():
    for p in sorted(ROOT.glob("**/component.py")):
        parts = p.relative_to(ROOT).parts
        if ".venv" in parts or "site-packages" in parts or parts[0] in ("dist", "quickstarts"):
            continue
        yield p


def _block(text: str) -> str:
    return text[text.index(_BEGIN): text.index(_END) + len(_END)]


def test_inlined_blocks_are_identical():
    blocks = {}
    for p in _component_files():
        text = p.read_text()
        if _BEGIN in text:
            blocks.setdefault(_block(text), []).append(str(p.relative_to(ROOT)))
    assert len(blocks) == 1, {k[:60]: len(v) for k, v in blocks.items()}
    assert sum(len(v) for v in blocks.values()) > 500  # the whole family carries it


def test_no_component_calls_the_removed_constructor():
    bad = [str(p.relative_to(ROOT)) for p in _component_files() if OLD_CALL.search(p.read_text())]
    assert not bad, bad[:10]


def _helper():
    path = ROOT / "assets/analytics/crm_data_standardizer/component.py"
    text = path.read_text()
    ns: dict = {}
    exec(compile(_block(text), str(path), "exec"), ns)
    return ns["_freshness_policy_compat"]


def test_helper_builds_time_window_and_cron_policies():
    import dagster as dg
    if not hasattr(dg.FreshnessPolicy, "time_window"):
        pytest.skip("older Dagster: helper falls back to the old constructor")
    build = _helper()
    tw = build(90, None)
    assert type(tw).__name__ == "TimeWindowFreshnessPolicy" and tw.fail_window.to_timedelta() == timedelta(minutes=90)
    cron = build(60, "0 6 * * *")
    assert type(cron).__name__ == "CronFreshnessPolicy" and cron.deadline_cron == "0 6 * * *"
    assert cron.lower_bound_delta == timedelta(minutes=60)


def test_a_converted_component_applies_the_policy_to_its_asset():
    import importlib.util

    import yaml
    folder = ROOT / "assets/analytics/crm_data_standardizer"
    doc = next(d for d in yaml.safe_load_all((folder / "example.yaml").read_text()) if d and "attributes" in d)
    spec = importlib.util.spec_from_file_location("crm_for_freshness", folder / "component.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    component = getattr(mod, doc["type"].rsplit(".", 1)[-1])(**{**doc["attributes"], "freshness_max_lag_minutes": 30})
    asset_def = list(component.build_defs(None).assets)[0]
    assert asset_def.get_asset_spec(next(iter(asset_def.keys))).freshness_policy is not None
