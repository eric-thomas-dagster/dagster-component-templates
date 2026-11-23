"""Tests for WindowCalculationComponent's ADF-parity additions: `ntile` (SQL NTILE(n), an
even row-count bucketing) and `cume_dist` (SQL CUME_DIST(), the ORDER BY tie-aware cumulative
distribution) -- neither has a pandas Series method, unlike rank/lag/lead/cumsum.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module


@pytest.fixture
def mod():
    return load_component_module()


# ---------------------------------------------------------------------------
# _ntile_bucket / _cume_dist -- pure functions
# ---------------------------------------------------------------------------


def test_ntile_bucket_even_split(mod):
    # 6 rows into 3 buckets -> 2 rows each
    buckets = [mod._ntile_bucket(pos, 6, 3) for pos in range(6)]
    assert buckets == [1, 1, 2, 2, 3, 3]


def test_ntile_bucket_uneven_split_front_loads_extra_rows(mod):
    # 7 rows into 3 buckets -> sizes 3,2,2 (first `7 % 3 = 1` bucket gets the extra row)
    buckets = [mod._ntile_bucket(pos, 7, 3) for pos in range(7)]
    assert buckets == [1, 1, 1, 2, 2, 3, 3]


def test_ntile_bucket_more_buckets_than_rows(mod):
    buckets = [mod._ntile_bucket(pos, 2, 5) for pos in range(2)]
    assert buckets == [1, 2]


def test_cume_dist_no_ties(mod):
    df = pd.DataFrame({"v": [10, 20, 30, 40]})
    out = mod._cume_dist(df, None)
    assert out.tolist() == pytest.approx([0.25, 0.5, 0.75, 1.0])


def test_cume_dist_with_ties_shares_max_fraction(mod):
    df = pd.DataFrame({"v": [10, 10, 30, 40]})
    out = mod._cume_dist(df, ["v"])
    # tie group {10,10} occupies positions 1-2 -> both get 2/4; then 30 -> 3/4; 40 -> 4/4
    assert out.tolist() == pytest.approx([0.5, 0.5, 0.75, 1.0])


# ---------------------------------------------------------------------------
# Component: build_defs + materialize
# ---------------------------------------------------------------------------


def _upstream(df):
    @dg.asset(name="upstream")
    def _asset():
        return df

    return _asset


def _materialize(mod, df, operations, **kwargs):
    kwargs.setdefault("asset_name", "out_asset")
    kwargs["upstream_asset_key"] = "upstream"
    kwargs["operations"] = operations
    comp = mod.WindowCalculationComponent(**kwargs)
    defs = comp.build_defs(context=None)
    result = dg.materialize([_upstream(df), *defs.assets])
    assert result.success
    return result.asset_value("out_asset")


def test_ntile_via_component_unpartitioned(mod):
    df = pd.DataFrame({"v": [1, 2, 3, 4, 5, 6]})
    out = _materialize(mod, df, [{"output": "bucket", "func": "ntile", "n": 3}], order_by=["v"])
    assert out["bucket"].tolist() == [1, 1, 2, 2, 3, 3]


def test_ntile_via_component_partitioned(mod):
    df = pd.DataFrame({"grp": ["a", "a", "a", "a", "b", "b"], "v": [1, 2, 3, 4, 1, 2]})
    out = _materialize(
        mod, df, [{"output": "bucket", "func": "ntile", "n": 2}], partition_by=["grp"], order_by=["v"],
    )
    a_rows = out[out["grp"] == "a"].sort_values("v")
    b_rows = out[out["grp"] == "b"].sort_values("v")
    assert a_rows["bucket"].tolist() == [1, 1, 2, 2]
    assert b_rows["bucket"].tolist() == [1, 2]


def test_cume_dist_via_component(mod):
    df = pd.DataFrame({"v": [10, 20, 30, 40]})
    out = _materialize(mod, df, [{"output": "cd", "func": "cume_dist"}], order_by=["v"])
    assert out["cd"].tolist() == pytest.approx([0.25, 0.5, 0.75, 1.0])


def test_ntile_missing_n_raises(mod):
    df = pd.DataFrame({"v": [1, 2, 3]})
    kwargs = dict(asset_name="out_asset", upstream_asset_key="upstream", operations=[{"output": "bucket", "func": "ntile"}])
    comp = mod.WindowCalculationComponent(**kwargs)
    defs = comp.build_defs(context=None)
    with pytest.raises(ValueError, match="requires 'n'"):
        dg.materialize([_upstream(df), *defs.assets])
