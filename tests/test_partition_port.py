"""Components that declared partition fields but never partitioned: now they build a real partitions_def and filter
their input rows to the partition (the same mechanism as assets/transforms/html_parser).

For every ported component, a partitioned materialization of day D over the FULL upstream must equal the unpartitioned
component run over just day D's rows, and the two days must differ (so a missing filter cannot hide)."""
import ast
import importlib.util
import re
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import yaml

import dagster as dg

ROOT = Path(__file__).resolve().parent.parent
BEGIN = "# BEGIN partitions-def builder"
END = "# END partitions-def builder"
GOLD = ROOT / "assets/transforms/html_parser/component.py"
DAYS = ("2026-01-02", "2026-01-03")


def _ported():
    out = []
    for p in sorted(ROOT.glob("**/component.py")):
        parts = p.relative_to(ROOT).parts
        if ".venv" in parts or "site-packages" in parts or parts[0] in ("dist", "quickstarts"):
            continue
        if BEGIN in p.read_text():
            out.append(p)
    return out


PORTED_ALL = _ported()  # everything carrying the inlined builder (row-filtering transforms, SQL, infrastructure...)
FILTER_MARKER = "# Filter to current partition if partitioned"
PORTED = [p for p in PORTED_ALL if FILTER_MARKER in p.read_text()]  # the ones that filter DataFrame rows


def _builder(text):
    return text[text.index(BEGIN): text.index(END) + len(END)]


def test_ported_components_exist_and_share_one_builder_equal_to_the_reference():
    assert len(PORTED_ALL) >= 38 and len(PORTED) >= 26
    reference = re.search(r"^def _build_partitions_def\(.*?(?=^def |^class )", GOLD.read_text(), re.S | re.M).group(0).rstrip()
    for p in PORTED_ALL:
        block = _builder(p.read_text())
        body = block.split("\n", 1)[1].rsplit("\n", 1)[0]
        assert body == reference, f"{p.relative_to(ROOT)} drifted from the reference builder"


def test_every_ported_component_passes_partitions_def_to_its_asset_and_filters():
    for p in PORTED_ALL:
        text = p.read_text()
        tree = ast.parse(text)
        build = next(n for c in tree.body if isinstance(c, ast.ClassDef) for n in c.body if isinstance(n, ast.FunctionDef) and n.name == "build_defs")
        decos = [d for f in ast.walk(build) if isinstance(f, ast.FunctionDef) for d in f.decorator_list
                 if isinstance(d, ast.Call) and getattr(d.func, "attr", getattr(d.func, "id", "")) == "asset"]
        assert decos and all(any(k.arg == "partitions_def" for k in d.keywords) for d in decos), p
        # components that take rows in must filter them (infrastructure and SQL ones parameterize instead; see their tests)
        if p in PORTED:
            assert FILTER_MARKER in text, p


# ── runtime: filtering is real ──────────────────────────────────────────────

def _strings(x):
    if isinstance(x, str):
        yield x
    elif isinstance(x, (list, tuple)):
        for i in x:
            yield from _strings(i)
    elif isinstance(x, dict):
        for v in x.values():
            yield from _strings(v)


def _example(folder):
    return next(d for d in yaml.safe_load_all((folder / "example.yaml").read_text()) if d and "attributes" in d)


def _build(folder, attrs):
    spec = importlib.util.spec_from_file_location(f"ported_{folder.name}", folder / "component.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    doc = _example(folder)
    cls = getattr(mod, doc["type"].rsplit(".", 1)[-1])
    defs = cls(**{**doc["attributes"], **attrs}).build_defs(None)
    return next(a for a in defs.assets if hasattr(a, "node_def"))


def _data(name, base):
    """Input shaped for the components whose semantics need it; day 2 is made genuinely different from day 1."""
    rng = np.random.default_rng(1)
    n = 40
    day = ["2026-01-02"] * 20 + ["2026-01-03"] * 20
    shift = np.array([0.0] * 20 + [5.0] * 20)
    ts = pd.to_datetime(day)
    if name in ("buffer", "smooth", "make_grid"):
        pts = [f"POINT({-100 + i * 0.5} {35 + (i % 7) * 0.5})" for i in range(n)]
        return pd.DataFrame({base.get("geometry_column", "geometry"): pts, "id": range(n), "ts": ts})
    if name == "test_of_means":
        grp = ["control", "treatment"] * 20
        return pd.DataFrame({"signups": rng.normal(10, 1, n) + shift * (np.array(grp) == "treatment"), "variant": grp, "ts": ts})
    if name == "market_basket_rules":
        rows = [(i, it, pd.Timestamp(day[i])) for i in range(n) for it in (["milk", "bread"] if i < 20 else ["beer", "chips", "salsa"])]
        return pd.DataFrame(rows, columns=["order_id", "product_name", "ts"])
    if name == "svm":
        cls = np.array(["a", "b"] * 20)
        df = pd.DataFrame({c: rng.normal(size=n) + (cls == "b") * 2 + shift for c in base["feature_columns"]})
        df[base["target_column"]] = cls
        df["ts"] = ts
        return df
    if name in ("count_regression", "gamma_regression"):
        f = base["feature_columns"]
        df = pd.DataFrame({c: rng.uniform(1, 5, n) for c in f})
        df[base["target_column"]] = np.round(rng.uniform(1, 4, n) * (1 + df[f[0]]) + shift * 2).astype(int) + 1
        df["ts"] = ts
        return df
    if name == "stepwise":
        # A uniform per-day shift cancels out in any model fit with an intercept, so it can't be relied on here:
        # the target must be driven by a DIFFERENT feature each day, or forward selection can coincidentally pick
        # the same subset both days (feature_columns/target_column here are pure noise otherwise).
        f = base["feature_columns"]
        df = pd.DataFrame({c: rng.normal(size=n) for c in f})
        is_day2 = np.array([d == day[-1] for d in day])
        driver = df[f[0]].where(~is_day2, df[f[1]])
        df[base["target_column"]] = driver * 5 + rng.normal(scale=0.05, size=n)
        df["ts"] = ts
        return df
    cols = {s for s in _strings(base) if s.isidentifier() and len(s) < 40} - {"daily", "true", "false"}
    extras = {"target", "label", "y", "outcome", "treatment", "group", "cluster"}
    df = pd.DataFrame({c: (rng.integers(0, 2, n) if c in extras else rng.normal(size=n) + shift) for c in cols | extras})
    df["ts"] = ts
    return df


# Components whose output does not depend on the input rows at all (sample-size calculators and the like): the run
# must still succeed partitioned, but there is nothing to distinguish between days.
DATA_INDEPENDENT = {"ab_controls"}
# Output columns that differ between any two runs (timestamps), so they cannot take part in the comparison.
VOLATILE_COLUMNS = {"moderation_scorer": ["scored_at"]}


def _same(a, b, drop=()):
    a, b = a.drop(columns=list(drop), errors="ignore"), b.drop(columns=list(drop), errors="ignore")
    try:
        pd.testing.assert_frame_equal(a.reset_index(drop=True), b.reset_index(drop=True), check_like=True,
                                      check_dtype=False, rtol=1e-6, atol=1e-9)
        return True
    except AssertionError:
        return False


@pytest.mark.parametrize("path", PORTED, ids=lambda p: p.parent.name)
def test_partitioned_run_equals_unpartitioned_run_over_that_days_rows(path):
    folder = path.parent
    base = dict(_example(folder)["attributes"])
    up_key = base.get("upstream_asset_key") or "upstream_x"
    up_key = up_key[0] if isinstance(up_key, list) else up_key
    try:
        part = _build(folder, dict(upstream_asset_key=up_key, partition_type="daily", partition_start="2026-01-01",
                                   partition_date_column="ts"))
        plain = _build(folder, dict(upstream_asset_key=up_key))
    except ModuleNotFoundError as exc:
        pytest.skip(f"optional dependency missing: {exc}")
    assert isinstance(part.partitions_def, dg.DailyPartitionsDefinition)
    name = next(iter(part.keys)).to_user_string()
    df = _data(folder.name, base)
    outputs = {}
    for day in DAYS:
        mask = df["ts"] == day

        @dg.asset(name=up_key)
        def up_full():
            return df

        @dg.asset(name=up_key)
        def up_day():
            return df[mask].reset_index(drop=True)

        instance = dg.DagsterInstance.ephemeral()
        dg.materialize([up_full], instance=instance)
        try:
            actual = dg.materialize([part, up_full], instance=instance, partition_key=day, selection=[part.key]).output_for_node(name)
            expected = dg.materialize([plain, up_day]).output_for_node(name)
        except ModuleNotFoundError as exc:
            pytest.skip(f"optional dependency missing: {exc}")
        assert _same(actual, expected, VOLATILE_COLUMNS.get(folder.name, ())), f"{folder.name}: partition {day} did not equal the unpartitioned run over that day's rows"
        outputs[day] = actual
    if folder.name not in DATA_INDEPENDENT:
        assert not _same(outputs[DAYS[0]], outputs[DAYS[1]], VOLATILE_COLUMNS.get(folder.name, ())), f"{folder.name}: both days gave identical output"


def test_multi_partition_filters_by_date_and_static_axis():
    folder = ROOT / "assets/transforms/select_records"
    base = dict(_example(folder)["attributes"])
    up_key = base.get("upstream_asset_key") or "upstream_x"
    part = _build(folder, dict(upstream_asset_key=up_key, partition_type="multi", partition_start="2026-01-01",
                               partition_values="acme,globex", partition_static_dim="customer",
                               partition_date_column="ts", partition_static_column="cust"))
    assert isinstance(part.partitions_def, dg.MultiPartitionsDefinition)
    df = _data("select_records", base)
    df["cust"] = ["acme", "globex"] * 20
    name = next(iter(part.keys)).to_user_string()

    @dg.asset(name=up_key)
    def up():
        return df

    instance = dg.DagsterInstance.ephemeral()
    dg.materialize([up], instance=instance)
    key = dg.MultiPartitionKey({"date": "2026-01-02", "static_dim": "globex"})
    out = dg.materialize([part, up], instance=instance, partition_key=key, selection=[part.key]).output_for_node(name)
    expected_rows = int(((df["ts"] == "2026-01-02") & (df["cust"] == "globex")).sum())
    plain = _build(folder, dict(upstream_asset_key=up_key))

    @dg.asset(name=up_key)
    def up_slice():
        return df[(df["ts"] == "2026-01-02") & (df["cust"] == "globex")].reset_index(drop=True)

    assert _same(out, dg.materialize([plain, up_slice]).output_for_node(name))
    assert 0 < expected_rows < len(df)  # the slice is a real subset


def test_unpartitioned_use_is_unchanged():
    folder = ROOT / "assets/transforms/select_records"
    base = dict(_example(folder)["attributes"])
    up_key = base.get("upstream_asset_key") or "upstream_x"
    plain = _build(folder, dict(upstream_asset_key=up_key))
    assert plain.partitions_def is None
    df = _data("select_records", base)

    @dg.asset(name=up_key)
    def up():
        return df

    out = dg.materialize([plain, up]).output_for_node(next(iter(plain.keys)).to_user_string())
    assert len(out) > 0
