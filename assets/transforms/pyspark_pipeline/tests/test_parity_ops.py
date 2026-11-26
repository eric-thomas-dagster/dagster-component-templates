"""Real, end-to-end tests for the ADF-parity ops added to PySparkPipelineComponent
this session (head, tail, head_per_group, unique, fill_null, cast, pivot, lookup,
window_calculation, unpivot, rank) -- against a real local Spark session, no mocking
of Spark itself. Expected values are hand-computed, matching the rigor of
test_pyspark_pipeline.py / test_control_flow.py."""
import dagster as dg
import pandas as pd
import pytest


@dg.asset
def primary_orders():
    return pd.DataFrame({
        "cust_id": [1, 2, 3, 4],
        "amt": [10, 20, 30, 40],
    })


@dg.asset
def lookup_tiers():
    # cust_id=1 has two candidate rows (tie-break target), cust_id=4 has none.
    return pd.DataFrame({
        "cust_id": [1, 1, 2, 3],
        "tier": ["gold", "silver", "bronze", "gold"],
        "eff_date": ["2024-01-01", "2024-06-01", "2024-01-01", "2024-01-01"],
    })


@dg.asset
def grouped_values():
    return pd.DataFrame({
        "g": ["a", "a", "a", "a", "b", "b", "b", "b"],
        "v": [10, 20, 30, 40, 1, 2, 3, 4],
    })


@dg.asset
def rank_values():
    return pd.DataFrame({"v": [10, 20, 20, 30]})


@dg.asset
def dup_rows():
    return pd.DataFrame({"k": [1, 1, 2, 2, 3], "v": ["a", "b", "c", "d", "e"]})


@dg.asset
def nullable_frame():
    return pd.DataFrame({"a": [1.0, None, 3.0], "b": ["1", "2", "3"]})


@dg.asset
def pivot_source():
    return pd.DataFrame({
        "region": ["east", "east", "west", "west"],
        "q": ["Q1", "Q2", "Q1", "Q2"],
        "rev": [10, 20, 30, 40],
    })


@dg.asset
def unpivot_source():
    return pd.DataFrame({"id": [1, 2], "jan": [100, 200], "feb": [110, 210]})


def _materialize_single(comp, upstream_assets=()):
    defs = dg.Definitions.merge(
        dg.Definitions(assets=list(upstream_assets)) if upstream_assets else dg.Definitions(),
        comp.build_defs(None),
    )
    asset_key = dg.AssetKey(comp.asset_name)
    result = dg.materialize(list(upstream_assets) + [defs.get_assets_def(asset_key)])
    return result.output_for_node(comp.asset_name)


# ── head / tail / head_per_group ──────────────────────────────────────────

def test_head_op(mod, spark):
    comp = mod.PySparkPipelineComponent(
        asset_name="first_two", source={"kind": "upstream"}, upstream_asset_key="primary_orders",
        operations=[{"op": "sort", "by": "cust_id"}, {"op": "head", "n": 2}],
        sink={"kind": "none"},
    )
    df = _materialize_single(comp, [primary_orders])
    assert sorted(df["cust_id"].tolist()) == [1, 2]


def test_tail_op_after_sort_is_globally_ordered(mod, spark):
    comp = mod.PySparkPipelineComponent(
        asset_name="last_two", source={"kind": "upstream"}, upstream_asset_key="primary_orders",
        operations=[{"op": "sort", "by": "cust_id"}, {"op": "tail", "n": 2}],
        sink={"kind": "none"},
    )
    df = _materialize_single(comp, [primary_orders])
    assert sorted(df["cust_id"].tolist()) == [3, 4]


def test_head_per_group(mod, spark):
    comp = mod.PySparkPipelineComponent(
        asset_name="hpg", source={"kind": "upstream"}, upstream_asset_key="grouped_values",
        operations=[{"op": "head_per_group", "group_by": "g", "n": 1}],
        sink={"kind": "none"},
    )
    df = _materialize_single(comp, [grouped_values])
    assert len(df) == 2
    assert sorted(df["g"].tolist()) == ["a", "b"]


# ── unique ─────────────────────────────────────────────────────────────────

def test_unique_subset_keep_first(mod, spark):
    comp = mod.PySparkPipelineComponent(
        asset_name="uniq", source={"kind": "upstream"}, upstream_asset_key="dup_rows",
        operations=[{"op": "unique", "subset": ["k"], "keep": "first"}],
        sink={"kind": "none"},
    )
    df = _materialize_single(comp, [dup_rows])
    assert len(df) == 3
    assert sorted(df["k"].tolist()) == [1, 2, 3]


def test_unique_invalid_keep_raises(mod, spark):
    comp = mod.PySparkPipelineComponent(
        asset_name="uniq_bad", source={"kind": "upstream"}, upstream_asset_key="dup_rows",
        operations=[{"op": "unique", "keep": "middle"}],
        sink={"kind": "none"},
    )
    with pytest.raises(Exception, match="keep must be 'first' or 'last'"):
        _materialize_single(comp, [dup_rows])


# ── fill_null / cast ────────────────────────────────────────────────────────

def test_fill_null(mod, spark):
    comp = mod.PySparkPipelineComponent(
        asset_name="filled", source={"kind": "upstream"}, upstream_asset_key="nullable_frame",
        operations=[{"op": "fill_null", "value": 0}],
        sink={"kind": "none"},
    )
    df = _materialize_single(comp, [nullable_frame])
    assert df["a"].tolist() == [1.0, 0.0, 3.0]


def test_cast(mod, spark):
    comp = mod.PySparkPipelineComponent(
        asset_name="casted", source={"kind": "upstream"}, upstream_asset_key="nullable_frame",
        operations=[{"op": "cast", "mapping": {"b": "int"}}],
        sink={"kind": "none"},
    )
    df = _materialize_single(comp, [nullable_frame])
    assert df["b"].tolist() == [1, 2, 3]
    assert str(df["b"].dtype) in ("int32", "int64")


# ── pivot ───────────────────────────────────────────────────────────────────

def test_pivot_single_value_single_agg(mod, spark):
    comp = mod.PySparkPipelineComponent(
        asset_name="pivoted", source={"kind": "upstream"}, upstream_asset_key="pivot_source",
        operations=[{"op": "pivot", "index": ["region"], "pivot_column": "q",
                     "value_columns": ["rev"], "agg_func": "sum"}],
        sink={"kind": "none"},
    )
    df = _materialize_single(comp, [pivot_source]).set_index("region")
    assert df.loc["east", "rev_Q1"] == 10
    assert df.loc["east", "rev_Q2"] == 20
    assert df.loc["west", "rev_Q1"] == 30
    assert df.loc["west", "rev_Q2"] == 40


def test_pivot_values_restriction_and_fill_value(mod, spark):
    comp = mod.PySparkPipelineComponent(
        asset_name="pivoted_restricted", source={"kind": "upstream"}, upstream_asset_key="pivot_source",
        operations=[{"op": "pivot", "index": ["region"], "pivot_column": "q",
                     "value_columns": ["rev"], "agg_func": "sum",
                     "pivot_values": ["Q1", "Q2", "Q3"], "fill_value": 0}],
        sink={"kind": "none"},
    )
    df = _materialize_single(comp, [pivot_source]).set_index("region")
    assert "rev_Q3" in df.columns
    assert df.loc["east", "rev_Q3"] == 0
    assert df.loc["west", "rev_Q3"] == 0


# ── unpivot ──────────────────────────────────────────────────────────────────

def test_unpivot(mod, spark):
    comp = mod.PySparkPipelineComponent(
        asset_name="unpivoted", source={"kind": "upstream"}, upstream_asset_key="unpivot_source",
        operations=[{"op": "unpivot", "id_columns": ["id"], "value_columns": ["jan", "feb"],
                     "var_name": "month", "value_name": "amount"}],
        sink={"kind": "none"},
    )
    df = _materialize_single(comp, [unpivot_source])
    assert len(df) == 4
    row = df[(df["id"] == 1) & (df["month"] == "jan")].iloc[0]
    assert row["amount"] == 100
    row2 = df[(df["id"] == 2) & (df["month"] == "feb")].iloc[0]
    assert row2["amount"] == 210


def test_unpivot_drop_null_values(mod, spark):
    @dg.asset
    def with_nulls():
        return pd.DataFrame({"id": [1, 2], "jan": [100, None], "feb": [None, 210]})

    comp = mod.PySparkPipelineComponent(
        asset_name="unpivoted_dropna", source={"kind": "upstream"}, upstream_asset_key="with_nulls",
        operations=[{"op": "unpivot", "id_columns": ["id"], "value_columns": ["jan", "feb"],
                     "var_name": "month", "value_name": "amount", "drop_null_values": True}],
        sink={"kind": "none"},
    )
    df = _materialize_single(comp, [with_nulls])
    assert len(df) == 2
    assert df["amount"].notna().all()


# ── window_calculation (ntile / cume_dist) ─────────────────────────────────

def test_window_calculation_ntile_grouped(mod, spark):
    comp = mod.PySparkPipelineComponent(
        asset_name="ntiled", source={"kind": "upstream"}, upstream_asset_key="grouped_values",
        operations=[{"op": "window_calculation", "function": "ntile", "order_by": "v",
                     "group_by": "g", "output_column": "nt", "n": 2}],
        sink={"kind": "none"},
    )
    df = _materialize_single(comp, [grouped_values]).set_index(["g", "v"])
    assert df.loc[("a", 10), "nt"] == 1
    assert df.loc[("a", 20), "nt"] == 1
    assert df.loc[("a", 30), "nt"] == 2
    assert df.loc[("a", 40), "nt"] == 2


def test_window_calculation_cume_dist(mod, spark):
    comp = mod.PySparkPipelineComponent(
        asset_name="cumed", source={"kind": "upstream"}, upstream_asset_key="rank_values",
        operations=[{"op": "window_calculation", "function": "cume_dist", "order_by": "v",
                     "output_column": "cd"}],
        sink={"kind": "none"},
    )
    df = _materialize_single(comp, [rank_values]).sort_values("v")
    # [10, 20, 20, 30] -> cume_dist: 0.25, 0.75, 0.75, 1.0
    assert df["cd"].tolist() == pytest.approx([0.25, 0.75, 0.75, 1.0])


# ── rank ─────────────────────────────────────────────────────────────────

def test_rank_method_rank_and_dense_rank_and_row_number(mod, spark):
    for method, expected in (
        ("rank", [1, 2, 2, 4]),
        ("dense_rank", [1, 2, 2, 3]),
        ("row_number", [1, 2, 3, 4]),
    ):
        comp = mod.PySparkPipelineComponent(
            asset_name=f"ranked_{method}", source={"kind": "upstream"}, upstream_asset_key="rank_values",
            operations=[{"op": "rank", "order_by": "v", "method": method, "output_column": "r"}],
            sink={"kind": "none"},
        )
        df = _materialize_single(comp, [rank_values]).sort_values("v")
        assert df["r"].tolist() == expected, f"method={method}"


def test_rank_ntile(mod, spark):
    comp = mod.PySparkPipelineComponent(
        asset_name="ranked_ntile", source={"kind": "upstream"}, upstream_asset_key="rank_values",
        operations=[{"op": "rank", "order_by": "v", "method": "ntile", "ntile_buckets": 2, "output_column": "r"}],
        sink={"kind": "none"},
    )
    df = _materialize_single(comp, [rank_values]).sort_values("v")
    assert df["r"].tolist() == [1, 1, 2, 2]


def test_rank_invalid_method_raises(mod, spark):
    comp = mod.PySparkPipelineComponent(
        asset_name="ranked_bad", source={"kind": "upstream"}, upstream_asset_key="rank_values",
        operations=[{"op": "rank", "order_by": "v", "method": "average", "output_column": "r"}],
        sink={"kind": "none"},
    )
    with pytest.raises(Exception, match="Spark-native"):
        _materialize_single(comp, [rank_values])


# ── lookup ─────────────────────────────────────────────────────────────────

def _lookup_steps(extra_op_fields):
    op = {"op": "lookup", "right": {"ref": "lk"}, "on_columns": "cust_id"}
    op.update(extra_op_fields)
    return {
        "steps": [
            {"id": "lk", "source": {"kind": "upstream", "upstream_asset_key": "lookup_tiers"}, "operations": []},
            {"id": "merged", "source": {"kind": "upstream", "upstream_asset_key": "primary_orders"},
             "operations": [op]},
        ],
        "sinks": [{"from": "merged", "kind": "none"}],
    }


def test_lookup_multiple_match_first_with_sort(mod, spark):
    shape = _lookup_steps({"multiple_match": "first", "match_sort_by": ["eff_date"], "match_sort_ascending": False})
    comp = mod.PySparkPipelineComponent(asset_name="lk_first", **shape)
    df = _materialize_single(comp, [primary_orders, lookup_tiers]).set_index("cust_id")
    assert df.loc[1, "tier"] == "silver"  # most recent eff_date wins
    assert pd.isna(df.loc[4, "tier"])  # no match -> null


def test_lookup_multiple_match_all_fanout(mod, spark):
    shape = _lookup_steps({"multiple_match": "all", "columns_to_add": ["tier"]})
    comp = mod.PySparkPipelineComponent(asset_name="lk_all", **shape)
    df = _materialize_single(comp, [primary_orders, lookup_tiers])
    assert len(df) == 5  # cust 1 fans out to 2 rows, 2 and 3 to one each, 4 stays with null


def test_lookup_multiple_match_all_as_list(mod, spark):
    shape = _lookup_steps({"multiple_match": "all_as_list", "columns_to_add": ["tier"]})
    comp = mod.PySparkPipelineComponent(asset_name="lk_list", **shape)
    df = _materialize_single(comp, [primary_orders, lookup_tiers]).set_index("cust_id")
    assert sorted(df.loc[1, "tier"]) == ["gold", "silver"]
    assert df.loc[2, "tier"] == ["bronze"]


def test_lookup_is_match_mode(mod, spark):
    shape = _lookup_steps({"output_mode": "is_match", "output_column": "found"})
    comp = mod.PySparkPipelineComponent(asset_name="lk_ismatch", **shape)
    df = _materialize_single(comp, [primary_orders, lookup_tiers]).set_index("cust_id")
    assert bool(df.loc[4, "found"]) is False
    assert bool(df.loc[1, "found"]) is True
    assert "tier" not in df.columns


def test_lookup_match_flag_column_alongside_columns(mod, spark):
    shape = _lookup_steps({
        "multiple_match": "first", "match_sort_by": ["eff_date"], "match_sort_ascending": False,
        "columns_to_add": ["tier"], "match_flag_column": "matched",
    })
    comp = mod.PySparkPipelineComponent(asset_name="lk_flag", **shape)
    df = _materialize_single(comp, [primary_orders, lookup_tiers]).set_index("cust_id")
    assert df.loc[1, "tier"] == "silver"
    assert bool(df.loc[1, "matched"]) is True
    assert bool(df.loc[4, "matched"]) is False
    assert pd.isna(df.loc[4, "tier"])


def test_lookup_on_no_match_error_raises(mod, spark):
    shape = _lookup_steps({"on_no_match": "error"})
    comp = mod.PySparkPipelineComponent(asset_name="lk_err", **shape)
    with pytest.raises(Exception, match="on_no_match='error'"):
        _materialize_single(comp, [primary_orders, lookup_tiers])
