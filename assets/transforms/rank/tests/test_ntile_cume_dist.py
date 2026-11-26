"""Tests for RankComponent's ADF Window-transform-parity addition: `method='ntile'`
(SQL NTILE(n)) and `method='cume_dist'` (SQL CUME_DIST()) -- statistical ranking
FUNCTIONS over the same sorted-partition primitive the 5 existing tie-breaking
methods use, not modifications of them.

Expected values below are hand-computed against the standard SQL definitions,
not just asserted against whatever the code happens to produce:

  NTILE(n): split n_rows sequentially-ordered rows into n buckets as evenly as
  possible, with any remainder rows going to the FIRST buckets (confirmed
  against real SQL engines, e.g. Postgres/Snowflake NTILE docs) -- 10 rows / 3
  buckets -> sizes [4, 3, 3], labels [1,1,1,1,2,2,2,3,3,3].

  CUME_DIST(): (# rows with value <= current row's value) / (total # rows).
  For [10, 20, 20, 30, 40]: 10 -> 1/5=0.2, 20 -> 3/5=0.6 (both tied rows), 30 ->
  4/5=0.8, 40 -> 5/5=1.0. Independently confirmed equal to pandas'
  `Series.rank(method='max', pct=True)` on the same data before relying on it.
"""
import pandas as pd
import pytest

import dagster as dg

from .conftest import load_component_module


@pytest.fixture
def mod():
    return load_component_module()


# ---------------------------------------------------------------------------
# _ntile_bucket_labels -- pure function
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("n,n_buckets,expected", [
    (10, 3, [1, 1, 1, 1, 2, 2, 2, 3, 3, 3]),   # remainder (1) goes to the FIRST bucket
    (9, 3, [1, 1, 1, 2, 2, 2, 3, 3, 3]),       # evenly divisible
    (7, 4, [1, 1, 2, 2, 3, 3, 4]),             # remainder (3) -> first 3 buckets get +1
    (4, 4, [1, 2, 3, 4]),                      # one row per bucket
    (3, 5, [1, 2, 3]),                         # fewer rows than buckets: no 4/5 labels emitted
])
def test_ntile_bucket_labels_matches_standard_sql_ntile(mod, n, n_buckets, expected):
    assert mod._ntile_bucket_labels(n, n_buckets) == expected


def test_ntile_bucket_labels_rejects_non_positive(mod):
    with pytest.raises(ValueError, match="positive integer"):
        mod._ntile_bucket_labels(10, 0)


# ---------------------------------------------------------------------------
# _ntile_rank -- pure function
# ---------------------------------------------------------------------------


def test_ntile_rank_7_rows_4_buckets_ascending(mod):
    # Deliberately shuffled relative to sort order, so a row-order bug would
    # actually change the assertion instead of coincidentally matching it.
    df = pd.DataFrame({"v": [70, 10, 40, 20, 60, 30, 50]})
    r = mod._ntile_rank(df, ["v"], [True], 4)
    # sorted ascending: 10,20,30,40,50,60,70 -> sizes [2,2,2,1] -> buckets 1,1,2,2,3,3,4
    expected = {10: 1.0, 20: 1.0, 30: 2.0, 40: 2.0, 50: 3.0, 60: 3.0, 70: 4.0}
    for v, bucket in zip(df["v"], r):
        assert bucket == expected[v]


def test_ntile_rank_descending(mod):
    df = pd.DataFrame({"v": [10, 20, 30, 40]})
    r = mod._ntile_rank(df, ["v"], [False], 2)
    # descending order: 40,30,20,10 -> sizes [2,2] -> 40->1,30->1,20->2,10->2
    expected = {40: 1.0, 30: 1.0, 20: 2.0, 10: 2.0}
    for v, bucket in zip(df["v"], r):
        assert bucket == expected[v]


def test_ntile_rank_respects_group_by(mod):
    df = pd.DataFrame({"v": [1, 2, 3, 4, 5], "grp": ["x", "x", "x", "y", "y"]})
    r = mod._ntile_rank(df, ["v"], [True], 2, group_by=["grp"])
    # group x (3 rows, 2 buckets): sizes [2,1] -> 1->1, 2->1, 3->2
    # group y (2 rows, 2 buckets): sizes [1,1] -> 4->1, 5->2
    assert r.tolist() == [1.0, 1.0, 2.0, 1.0, 2.0]


# ---------------------------------------------------------------------------
# cume_dist -- via _rank_by_multiple_columns(method="max", pct=True)
# ---------------------------------------------------------------------------


def test_cume_dist_matches_hand_computed_values(mod):
    df = pd.DataFrame({"v": [10, 20, 20, 30, 40]})
    r = mod._rank_by_multiple_columns(df, ["v"], [True], method="max", pct=True)
    assert r.tolist() == pytest.approx([0.2, 0.6, 0.6, 0.8, 1.0])


def test_cume_dist_matches_pandas_rank_max_pct(mod):
    # Independent cross-check: pandas' own Series.rank(method='max', pct=True)
    # on the same data, not just the component's internal helper.
    df = pd.DataFrame({"v": [10, 20, 20, 30, 40]})
    direct = df["v"].rank(method="max", pct=True)
    via_helper = mod._rank_by_multiple_columns(df, ["v"], [True], method="max", pct=True)
    assert via_helper.tolist() == pytest.approx(direct.tolist())


def test_cume_dist_respects_group_by(mod):
    df = pd.DataFrame({"v": [10, 20, 20, 30, 40], "grp": ["x", "x", "x", "y", "y"]})
    r = mod._rank_by_multiple_columns(df, ["v"], [True], method="max", pct=True, group_by=["grp"])
    # group x: 10->1/3, 20->3/3 (both); group y: 30->1/2, 40->2/2
    assert r.tolist() == pytest.approx([1 / 3, 1.0, 1.0, 0.5, 1.0])


# ---------------------------------------------------------------------------
# Component: build_defs + materialize
# ---------------------------------------------------------------------------


def _upstream(df):
    @dg.asset(name="upstream")
    def _upstream_asset():
        return df

    return _upstream_asset


def test_ntile_requires_ntile_buckets(mod):
    df = pd.DataFrame({"A": [1, 2, 3, 4]})
    comp = mod.RankComponent(asset_name="out", upstream_asset_key="upstream", column="A", method="ntile")
    defs = comp.build_defs(load_context=None)
    with pytest.raises(ValueError, match="requires `ntile_buckets`"):
        dg.materialize([_upstream(df), *defs.assets])


def test_ntile_via_component(mod):
    df = pd.DataFrame({"A": [40, 10, 30, 20]})
    comp = mod.RankComponent(
        asset_name="out", upstream_asset_key="upstream", column="A", method="ntile", ntile_buckets=2,
    )
    defs = comp.build_defs(load_context=None)
    result = dg.materialize([_upstream(df), *defs.assets])
    assert result.success
    out = result.asset_value("out").set_index("A")
    # ascending (default): 10,20 -> bucket 1; 30,40 -> bucket 2
    assert out.loc[10, "A_rank"] == 1.0
    assert out.loc[20, "A_rank"] == 1.0
    assert out.loc[30, "A_rank"] == 2.0
    assert out.loc[40, "A_rank"] == 2.0


def test_cume_dist_via_component(mod):
    df = pd.DataFrame({"A": [10, 20, 20, 30, 40]})
    comp = mod.RankComponent(
        asset_name="out", upstream_asset_key="upstream", column="A", method="cume_dist",
    )
    defs = comp.build_defs(load_context=None)
    result = dg.materialize([_upstream(df), *defs.assets])
    assert result.success
    out = result.asset_value("out")
    assert out["A_rank"].tolist() == pytest.approx([0.2, 0.6, 0.6, 0.8, 1.0])


def test_ntile_and_cume_dist_do_not_affect_existing_methods(mod):
    # Original 5 tie-breaking methods + normalize must be completely unaffected
    # by the new code paths -- same assertions as the pre-existing parity tests.
    df = pd.DataFrame({"A": [1, 1, 1, 2]})
    comp = mod.RankComponent(asset_name="out", upstream_asset_key="upstream", column="A", method="dense")
    defs = comp.build_defs(load_context=None)
    result = dg.materialize([_upstream(df), *defs.assets])
    assert result.success
    assert result.asset_value("out")["A_rank"].tolist() == [1.0, 1.0, 1.0, 2.0]
