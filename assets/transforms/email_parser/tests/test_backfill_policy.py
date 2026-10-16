"""Committed tests for EmailParserComponent's backfill_policy + range-aware partition filtering.

The upstream DataFrame is a real pandas frame (3 daily partitions x 10 rows)
routed through the real asset graph via dg.materialize() with
dg.InMemoryIOManager() (a single_run output spans several partitions, which
the default filesystem IO manager cannot persist). A single_run backfill is
simulated with the same run tags the Dagster backfill daemon sets
(dagster/asset_partition_range_start / _end). Nothing is mocked.

Before the fix, the filter was gated on context.has_partition_key, which is
False for a range, so a single_run backfill silently skipped partition
filtering and processed the ENTIRE upstream frame.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module

CLS = "EmailParserComponent"
KWARGS = {'column': 'raw_email'}
UPSTREAM_ARGS = {'upstream_asset_key': 'src'}
EXTRA_ASSETS = []
NEEDS_NESTED = False
REQUIRES = None


@pytest.fixture()
def mod():
    return load_component_module()


def _src_df():
    days = ["2024-01-01", "2024-01-02", "2024-01-03"]
    rows = []
    for di, day in enumerate(days):
        for h in range(10):
            i = di * 10 + h
            rows.append({
                "id": i,
                "event_date": day,
                "ts": f"{day} {h:02d}:00:00",
                "grp": "a" if h % 2 == 0 else "b",
                "cat": "x" if h % 3 == 0 else "y",
                "value": float(i * 3 % 17 + 1),
                "weight": float(h + 1),
                "text": f"hello{i} world{i}",
                "email": f"user{i}@example.com",
                "html": f"<p>row {i}</p>",
                "md": f"**row {i}**",
                "xml_data": f"<r><name>n{i}</name></r>",
                "payload": '{"a": {"b": %d}}' % i,
                "payload_dict": {"a": {"b": i}},
                "arr": ["p", "q"],
                "raw_email": f"From: a@b.com\nTo: c@d.com\nSubject: s{i}\n\nbody{i}",
                "pdf_path": "/nonexistent.pdf",
            })
    df = pd.DataFrame(rows)
    if not NEEDS_NESTED:
        df = df.drop(columns=["payload_dict", "arr"])
    return df


def _aux_df(name):
    base = _src_df()
    if name == "right_src":
        return base[["id", "event_date", "weight"]].rename(columns={"weight": "w2"})
    if name == "lookup":
        return pd.DataFrame({"code": ["a", "b"], "name": ["A", "B"]})
    if name == "cfg":
        return pd.DataFrame({"tax_rate": [0.1]})
    raise KeyError(name)


def _make_aux(name, frame):
    @dg.asset(name=name)
    def _aux():
        return frame
    return _aux


def _build(mod, partitioned, **overrides):
    cls = getattr(mod, CLS)
    kw = dict(asset_name="out_asset", **UPSTREAM_ARGS, **KWARGS)
    if partitioned:
        kw.update(partition_type="daily", partition_start="2024-01-01",
                  partition_date_column="event_date")
    kw.update(overrides)
    return cls(**kw)


def _materialize(mod, partitioned, partition_key=None, tags=None, **overrides):
    src_df = _src_df()

    @dg.asset(name="src")
    def src():
        return src_df

    assets = [src]
    for n in EXTRA_ASSETS:
        aux = _aux_df(n)

        assets.append(_make_aux(n, aux))
    comp = _build(mod, partitioned, **overrides)
    comp_assets = comp.build_defs(None).assets
    result = dg.materialize(
        assets + list(comp_assets),
        resources={"io_manager": dg.InMemoryIOManager()},
        partition_key=partition_key,
        tags=tags or {},
    )
    assert result.success
    out = result.output_for_node("out_asset")
    return out, comp_assets


RANGE_TAGS = {
    "dagster/asset_partition_range_start": "2024-01-01",
    "dagster/asset_partition_range_end": "2024-01-02",
}


def _as_df(x):
    return x.value if hasattr(x, "value") and hasattr(x, "metadata") else x


def test_backfill_policy_wiring(mod):
    comp_assets = _build(mod, True, backfill_policy="single_run").build_defs(None).assets
    assert comp_assets[0].backfill_policy == dg.BackfillPolicy.single_run()
    comp_assets = _build(mod, True, backfill_policy="multi_run",
                         backfill_policy_max_partitions_per_run=3).build_defs(None).assets
    assert comp_assets[0].backfill_policy == dg.BackfillPolicy.multi_run(max_partitions_per_run=3)
    comp_assets = _build(mod, True).build_defs(None).assets
    assert comp_assets[0].backfill_policy is None
    # Ignored for an unpartitioned asset
    comp_assets = _build(mod, False, backfill_policy="single_run").build_defs(None).assets
    assert comp_assets[0].backfill_policy is None


def test_single_run_range_filters_to_range_not_whole_upstream(mod):
    if REQUIRES:
        pytest.importorskip(REQUIRES)
    """The range run must differ from the unfiltered run (the pre-fix
    behavior: has_partition_key False -> no filtering at all) and from a
    one-day run, proving the per-key filter really ran over BOTH days."""
    full, _ = _materialize(mod, partitioned=False)
    one_day, _ = _materialize(mod, partitioned=True, partition_key="2024-01-01")
    ranged, _ = _materialize(mod, partitioned=True, backfill_policy="single_run", tags=RANGE_TAGS)
    full, one_day, ranged = (_as_df(x) for x in (full, one_day, ranged))
    assert not one_day.equals(full)
    assert not ranged.equals(full), "range run was not filtered (has_partition_key_range ignored)"
    assert not ranged.equals(one_day), "range run only processed one partition"
