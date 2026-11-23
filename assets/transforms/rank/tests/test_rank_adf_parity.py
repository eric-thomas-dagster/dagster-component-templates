"""Tests for RankComponent's ADF-parity addition: a multi-key rank (ADF's Rank transform
supports several sort columns, each with its own direction -- pandas' own Series.rank()
only ever ranks by one column)."""
import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module


@pytest.fixture
def mod():
    return load_component_module()


def _df():
    return pd.DataFrame(
        {
            "A": [1, 1, 1, 2],
            "B": [5, 5, 3, 1],
        }
    )


# ---------------------------------------------------------------------------
# _rank_by_multiple_columns -- pure function
# ---------------------------------------------------------------------------


def test_multi_rank_first(mod):
    rank = mod._rank_by_multiple_columns(_df(), ["A", "B"], [True, False], method="first", pct=False)
    assert rank.tolist() == [1.0, 2.0, 3.0, 4.0]


def test_multi_rank_dense(mod):
    rank = mod._rank_by_multiple_columns(_df(), ["A", "B"], [True, False], method="dense", pct=False)
    assert rank.tolist() == [1.0, 1.0, 2.0, 3.0]


def test_multi_rank_min(mod):
    rank = mod._rank_by_multiple_columns(_df(), ["A", "B"], [True, False], method="min", pct=False)
    assert rank.tolist() == [1.0, 1.0, 3.0, 4.0]


def test_multi_rank_max(mod):
    rank = mod._rank_by_multiple_columns(_df(), ["A", "B"], [True, False], method="max", pct=False)
    assert rank.tolist() == [2.0, 2.0, 3.0, 4.0]


def test_multi_rank_average(mod):
    rank = mod._rank_by_multiple_columns(_df(), ["A", "B"], [True, False], method="average", pct=False)
    assert rank.tolist() == [1.5, 1.5, 3.0, 4.0]


def test_multi_rank_pct_dense_divides_by_group_count(mod):
    rank = mod._rank_by_multiple_columns(_df(), ["A", "B"], [True, False], method="dense", pct=True)
    assert rank.tolist() == pytest.approx([1 / 3, 1 / 3, 2 / 3, 1.0])


def test_multi_rank_pct_min_divides_by_row_count(mod):
    rank = mod._rank_by_multiple_columns(_df(), ["A", "B"], [True, False], method="min", pct=True)
    assert rank.tolist() == pytest.approx([1 / 4, 1 / 4, 3 / 4, 1.0])


def test_multi_rank_respects_group_by(mod):
    df = _df()
    df["grp"] = ["x", "x", "y", "y"]
    rank = mod._rank_by_multiple_columns(df, ["A", "B"], [True, False], method="dense", pct=False, group_by=["grp"])
    # group x: rows 0,1 (A=1,B=5 tie) -> dense rank 1 both. group y: rows 2 (A=1,B=3), 3 (A=2,B=1) -> 1, 2
    assert rank.tolist() == [1.0, 1.0, 1.0, 2.0]


# ---------------------------------------------------------------------------
# Component: build_defs + materialize
# ---------------------------------------------------------------------------


def _upstream(df):
    @dg.asset(name="upstream")
    def _upstream_asset():
        return df

    return _upstream_asset


def test_single_column_rank_unchanged(mod):
    comp = mod.RankComponent(asset_name="out", upstream_asset_key="upstream", column="A")
    defs = comp.build_defs(load_context=None)
    result = dg.materialize([_upstream(_df()), *defs.assets])
    assert result.success
    out = result.asset_value("out")
    assert "A_rank" in out.columns


def test_multi_column_rank_via_component(mod):
    comp = mod.RankComponent(
        asset_name="out", upstream_asset_key="upstream", column=["A", "B"], ascending=[True, False], method="dense",
    )
    defs = comp.build_defs(load_context=None)
    result = dg.materialize([_upstream(_df()), *defs.assets])
    assert result.success
    out = result.asset_value("out")
    assert "A_B_rank" in out.columns
    assert out["A_B_rank"].tolist() == [1.0, 1.0, 2.0, 3.0]


def test_multi_column_rank_ascending_length_mismatch_raises(mod):
    comp = mod.RankComponent(
        asset_name="out", upstream_asset_key="upstream", column=["A", "B"], ascending=[True],
    )
    defs = comp.build_defs(load_context=None)
    with pytest.raises(ValueError, match="must match"):
        dg.materialize([_upstream(_df()), *defs.assets])
