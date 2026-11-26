"""Tests for the ADF Data Flow transform parity gaps closed in this session:

1. `sort` op `na_position` / `na_position_per_column` -- ADF's Sort transform has a
   null-handling flag per sort condition; pandas' own `sort_values(na_position=...)` only
   takes one scalar for the whole sort.
2. `pivot` op -- ADF's Pivot transform (`pd.pivot_table` under the hood), including
   `pivot_values` restricting the output to a declared set of pivot-column values (a value
   absent from the data still produces a fill_value-filled column).
3. `lookup` op -- ADF's Lookup transform: `match_sort_by` guarantees a true 1:1 lookup when
   multiple right-side rows share a key, and `output_mode: is_match` produces a boolean
   match-flag column instead of a full left join.
4. `window_calculation` op -- `ntile` and `cume_dist` only (rank-equivalents are already
   covered by the standalone `rank` component and this file's own `sort`/`group_by`).

Every test actually materializes the asset via dagster.materialize(), same convention as
test_pandas_pipeline.py.
"""
import dagster as dg
import pandas as pd
import pytest


def _materialize_flat(mod, upstream_df, operations, **extra):
    @dg.asset
    def src():
        return upstream_df

    comp = mod.PandasPipelineComponent(
        asset_name="out_asset",
        upstream_asset_key="src",
        operations=operations,
        **extra,
    )
    defs = dg.Definitions.merge(dg.Definitions(assets=[src]), comp.build_defs(None))
    result = dg.materialize([src, defs.get_assets_def(dg.AssetKey("out_asset"))])
    return result.output_for_node("out_asset")


# -- sort: na_position ------------------------------------------------------

def test_sort_na_position_last_default(mod):
    df = pd.DataFrame({"v": [3, None, 1, None, 2]})
    out = _materialize_flat(mod, df, [{"op": "sort", "by": ["v"]}])
    # Default na_position='last' (pandas' own default, matching ADF's Sort default too)
    vals = out["v"].tolist()
    assert vals[:3] == [1, 2, 3]
    assert pd.isna(vals[3]) and pd.isna(vals[4])


def test_sort_na_position_first(mod):
    df = pd.DataFrame({"v": [3, None, 1, None, 2]})
    out = _materialize_flat(mod, df, [{"op": "sort", "by": ["v"], "na_position": "first"}])
    vals = out["v"].tolist()
    assert pd.isna(vals[0]) and pd.isna(vals[1])
    assert vals[2:] == [1, 2, 3]


def test_sort_na_position_invalid_raises(mod):
    df = pd.DataFrame({"v": [1, 2]})
    with pytest.raises(ValueError, match="na_position"):
        _materialize_flat(mod, df, [{"op": "sort", "by": ["v"], "na_position": "middle"}])


def test_sort_na_position_per_column(mod):
    # region ascending (nulls last), amount descending (nulls first) -- independent
    # null-handling per sort key, which a single scalar na_position can't express.
    df = pd.DataFrame({
        "region": ["east", None, "east", "west", None],
        "amount": [10, 5, None, 20, None],
    })
    out = _materialize_flat(mod, df, [{
        "op": "sort", "by": ["region", "amount"], "descending": False,
        "na_position_per_column": ["last", "first"],
    }])
    # All non-null regions first ("east" rows before "west"), null regions last.
    regions = out["region"].tolist()
    assert regions[:3] == ["east", "east", "west"]
    assert pd.isna(regions[3]) and pd.isna(regions[4])
    # Within the two "east" rows, amount is descending with nulls first -> None before 10.
    east_amounts = out[out["region"] == "east"]["amount"].tolist()
    assert pd.isna(east_amounts[0])
    assert east_amounts[1] == 10


def test_sort_na_position_per_column_length_mismatch_raises(mod):
    df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    with pytest.raises(ValueError, match="na_position_per_column"):
        _materialize_flat(mod, df, [{
            "op": "sort", "by": ["a", "b"], "na_position_per_column": ["first"],
        }])


# -- pivot --------------------------------------------------------------------

def test_pivot_basic(mod):
    df = pd.DataFrame({
        "region": ["east", "east", "west", "west"],
        "quarter": ["Q1", "Q2", "Q1", "Q2"],
        "revenue": [100, 150, 200, 50],
    })
    out = _materialize_flat(mod, df, [{
        "op": "pivot", "index": ["region"], "pivot_column": "quarter",
        "value_columns": ["revenue"], "agg_func": "sum",
    }])
    by_region = out.set_index("region")
    assert by_region.loc["east", "revenue_Q1"] == 100
    assert by_region.loc["east", "revenue_Q2"] == 150
    assert by_region.loc["west", "revenue_Q1"] == 200
    assert by_region.loc["west", "revenue_Q2"] == 50


def test_pivot_multiple_index_and_value_columns(mod):
    df = pd.DataFrame({
        "region": ["east", "east", "west", "west"],
        "segment": ["retail", "retail", "wholesale", "wholesale"],
        "quarter": ["Q1", "Q2", "Q1", "Q2"],
        "revenue": [100, 150, 200, 50],
        "units": [10, 15, 20, 5],
    })
    out = _materialize_flat(mod, df, [{
        "op": "pivot", "index": ["region", "segment"], "pivot_column": "quarter",
        "value_columns": ["revenue", "units"], "agg_func": "sum",
    }])
    assert {"region", "segment", "revenue_Q1", "revenue_Q2", "units_Q1", "units_Q2"} <= set(out.columns)
    row = out[out["region"] == "east"].iloc[0]
    assert row["revenue_Q1"] == 100
    assert row["units_Q2"] == 15


def test_pivot_values_restricts_and_fills_missing(mod):
    df = pd.DataFrame({
        "region": ["east", "east", "west"],
        "quarter": ["Q1", "Q2", "Q1"],
        "revenue": [100, 150, 200],
    })
    out = _materialize_flat(mod, df, [{
        "op": "pivot", "index": ["region"], "pivot_column": "quarter",
        "value_columns": ["revenue"], "agg_func": "sum",
        "pivot_values": ["Q1", "Q2", "Q3"], "fill_value": 0,
    }])
    # Q3 never appears in the data but must still produce a fill_value column.
    assert "revenue_Q3" in out.columns
    by_region = out.set_index("region")
    assert by_region.loc["east", "revenue_Q3"] == 0
    assert by_region.loc["west", "revenue_Q2"] == 0  # west has no Q2 row either


# -- lookup ---------------------------------------------------------------

def test_lookup_columns_mode(mod):
    @dg.asset
    def orders():
        return pd.DataFrame({"customer_id": [1, 2, 3], "amount": [10.0, 20.0, 30.0]})

    @dg.asset
    def customers():
        return pd.DataFrame({"customer_id": [1, 2], "tier": ["gold", "silver"]})

    comp = mod.PandasPipelineComponent(
        asset_name="looked_up",
        steps=[
            {"id": "o", "source": {"kind": "upstream", "upstream_asset_key": "orders"}, "operations": []},
            {"id": "c", "source": {"kind": "upstream", "upstream_asset_key": "customers"}, "operations": []},
            {"id": "looked_up", "source": {"kind": "ref", "ref": "o"},
             "operations": [{"op": "lookup", "right": {"ref": "c"}, "on_columns": ["customer_id"]}]},
        ],
    )
    defs = dg.Definitions.merge(dg.Definitions(assets=[orders, customers]), comp.build_defs(None))
    result = dg.materialize([orders, customers, defs.get_assets_def(dg.AssetKey("looked_up"))])
    df = result.output_for_node("looked_up")
    by_customer = dict(zip(df["customer_id"], df["tier"]))
    assert by_customer[1] == "gold"
    assert by_customer[2] == "silver"
    assert pd.isna(by_customer[3])  # no match -> NaN, like a left join


def test_lookup_match_sort_by_guarantees_one_to_one(mod):
    @dg.asset
    def orders():
        return pd.DataFrame({"customer_id": [1, 2]})

    @dg.asset
    def events():
        # customer 1 has TWO events -- match_sort_by must pick exactly one.
        return pd.DataFrame({
            "customer_id": [1, 1, 2],
            "event_time": ["2024-01-02", "2024-01-05", "2024-01-01"],
            "event_id": ["e_early", "e_late", "e_only"],
        })

    comp = mod.PandasPipelineComponent(
        asset_name="latest_event",
        steps=[
            {"id": "o", "source": {"kind": "upstream", "upstream_asset_key": "orders"}, "operations": []},
            {"id": "e", "source": {"kind": "upstream", "upstream_asset_key": "events"}, "operations": []},
            {"id": "latest_event", "source": {"kind": "ref", "ref": "o"},
             "operations": [{
                 "op": "lookup", "right": {"ref": "e"}, "on_columns": ["customer_id"],
                 "match_sort_by": ["event_time"], "match_sort_ascending": False,
             }]},
        ],
    )
    defs = dg.Definitions.merge(dg.Definitions(assets=[orders, events]), comp.build_defs(None))
    result = dg.materialize([orders, events, defs.get_assets_def(dg.AssetKey("latest_event"))])
    df = result.output_for_node("latest_event")
    assert len(df) == 2  # exactly one row per left row -- no fan-out from the duplicate match
    by_customer = dict(zip(df["customer_id"], df["event_id"]))
    assert by_customer[1] == "e_late"  # latest event_time wins (descending sort)
    assert by_customer[2] == "e_only"


def test_lookup_is_match_mode(mod):
    @dg.asset
    def orders():
        return pd.DataFrame({"customer_id": [1, 2, 3]})

    @dg.asset
    def gold_customers():
        return pd.DataFrame({"customer_id": [1, 3], "tier": ["gold", "gold"]})

    comp = mod.PandasPipelineComponent(
        asset_name="flagged",
        steps=[
            {"id": "o", "source": {"kind": "upstream", "upstream_asset_key": "orders"}, "operations": []},
            {"id": "g", "source": {"kind": "upstream", "upstream_asset_key": "gold_customers"}, "operations": []},
            {"id": "flagged", "source": {"kind": "ref", "ref": "o"},
             "operations": [{
                 "op": "lookup", "right": {"ref": "g"}, "on_columns": ["customer_id"],
                 "output_mode": "is_match", "output_column": "is_gold",
             }]},
        ],
    )
    defs = dg.Definitions.merge(dg.Definitions(assets=[orders, gold_customers]), comp.build_defs(None))
    result = dg.materialize([orders, gold_customers, defs.get_assets_def(dg.AssetKey("flagged"))])
    df = result.output_for_node("flagged")
    assert "tier" not in df.columns  # is_match mode drops every right-side column but the flag
    by_customer = dict(zip(df["customer_id"], df["is_gold"]))
    assert by_customer == {1: True, 2: False, 3: True}


# -- window_calculation: ntile + cume_dist ---------------------------------

def test_window_calculation_ntile_matches_sql_bucket_sizes(mod):
    # 10 rows into 4 buckets: SQL NTILE(4) convention is sizes [3, 3, 2, 2] -- the FIRST
    # (10 mod 4 = 2) buckets get the extra row, not the outer ones (verified this is NOT
    # what pd.qcut alone produces before writing the implementation).
    df = pd.DataFrame({"score": list(range(1, 11))})
    out = _materialize_flat(mod, df, [{
        "op": "window_calculation", "function": "ntile", "order_by": ["score"],
        "n": 4, "output_column": "bucket",
    }])
    out = out.sort_values("score")
    assert out["bucket"].tolist() == [1, 1, 1, 2, 2, 2, 3, 3, 4, 4]


def test_window_calculation_ntile_evenly_divisible(mod):
    df = pd.DataFrame({"score": list(range(1, 9))})  # 8 rows, 4 buckets -> 2 each
    out = _materialize_flat(mod, df, [{
        "op": "window_calculation", "function": "ntile", "order_by": ["score"],
        "n": 4, "output_column": "bucket",
    }])
    out = out.sort_values("score")
    assert out["bucket"].tolist() == [1, 1, 2, 2, 3, 3, 4, 4]


def test_window_calculation_ntile_per_group(mod):
    df = pd.DataFrame({
        "region": ["east"] * 4 + ["west"] * 4,
        "score": [1, 2, 3, 4, 10, 20, 30, 40],
    })
    out = _materialize_flat(mod, df, [{
        "op": "window_calculation", "function": "ntile", "order_by": ["score"],
        "group_by": ["region"], "n": 2, "output_column": "bucket",
    }])
    east = out[out["region"] == "east"].sort_values("score")
    west = out[out["region"] == "west"].sort_values("score")
    assert east["bucket"].tolist() == [1, 1, 2, 2]
    assert west["bucket"].tolist() == [1, 1, 2, 2]


def test_window_calculation_cume_dist(mod):
    # Hand-computed: CUME_DIST = (# rows with value <= this row) / total rows.
    # [10, 20, 20, 30] -> [0.25, 0.75, 0.75, 1.0]
    df = pd.DataFrame({"v": [10, 20, 20, 30]})
    out = _materialize_flat(mod, df, [{
        "op": "window_calculation", "function": "cume_dist", "order_by": ["v"],
        "output_column": "cd",
    }])
    out = out.sort_values("v")
    assert out["cd"].tolist() == pytest.approx([0.25, 0.75, 0.75, 1.0])


def test_window_calculation_invalid_function_raises(mod):
    df = pd.DataFrame({"v": [1, 2]})
    with pytest.raises(ValueError, match="ntile.*cume_dist"):
        _materialize_flat(mod, df, [{
            "op": "window_calculation", "function": "rank", "order_by": ["v"],
            "output_column": "x",
        }])


# -- lookup: multiple_match / on_no_match / columns_to_add / rename / match_flag_column ----
# (richer field shape ported from the canonical standalone LookupComponent, for cross-pipeline
# consistency with polars_pipeline/pyspark_pipeline's own lookup ops -- see commit history.)

def _lookup_comp(mod, right_rows, op_extra):
    @dg.asset
    def orders():
        return pd.DataFrame({"customer_id": [1, 2]})

    @dg.asset
    def events():
        return pd.DataFrame(right_rows)

    comp = mod.PandasPipelineComponent(
        asset_name="out",
        steps=[
            {"id": "o", "source": {"kind": "upstream", "upstream_asset_key": "orders"}, "operations": []},
            {"id": "e", "source": {"kind": "upstream", "upstream_asset_key": "events"}, "operations": []},
            {"id": "out", "source": {"kind": "ref", "ref": "o"},
             "operations": [{"op": "lookup", "right": {"ref": "e"}, "on_columns": ["customer_id"], **op_extra}]},
        ],
    )
    defs = dg.Definitions.merge(dg.Definitions(assets=[orders, events]), comp.build_defs(None))
    result = dg.materialize([orders, events, defs.get_assets_def(dg.AssetKey("out"))])
    return result.output_for_node("out")


def test_lookup_multiple_match_last(mod):
    # customer 1 has two events; match_sort_by ascending + multiple_match='last' picks the
    # LATEST event_time (opposite of the 'first' default).
    out = _lookup_comp(mod, {
        "customer_id": [1, 1, 2], "event_time": ["2024-01-02", "2024-01-05", "2024-01-01"],
        "event_id": ["e_early", "e_late", "e_only"],
    }, {"multiple_match": "last", "match_sort_by": ["event_time"]})
    assert len(out) == 2
    by_customer = dict(zip(out["customer_id"], out["event_id"]))
    assert by_customer[1] == "e_late"
    assert by_customer[2] == "e_only"


def test_lookup_multiple_match_all_fans_out(mod):
    out = _lookup_comp(mod, {
        "customer_id": [1, 1, 2], "event_id": ["e1", "e2", "e3"],
    }, {"multiple_match": "all"})
    assert len(out) == 3  # customer 1's two matches both survive -- plain left-join fan-out
    assert sorted(out[out["customer_id"] == 1]["event_id"].tolist()) == ["e1", "e2"]


def test_lookup_multiple_match_all_as_list(mod):
    out = _lookup_comp(mod, {
        "customer_id": [1, 1, 2], "event_id": ["e1", "e2", "e3"],
    }, {"multiple_match": "all_as_list"})
    assert len(out) == 2  # one row per primary row, no fan-out
    by_customer = dict(zip(out["customer_id"], out["event_id"]))
    assert sorted(by_customer[1]) == ["e1", "e2"]
    assert by_customer[2] == ["e3"]


def test_lookup_on_no_match_error_raises(mod):
    with pytest.raises(ValueError, match="on_no_match='error'"):
        _lookup_comp(mod, {"customer_id": [1], "event_id": ["e1"]}, {"on_no_match": "error"})


def test_lookup_on_no_match_pass_is_default(mod):
    # Customer 2 has no match -- default on_no_match='pass' lets it through with nulls.
    out = _lookup_comp(mod, {"customer_id": [1], "event_id": ["e1"]}, {})
    by_customer = dict(zip(out["customer_id"], out["event_id"]))
    assert by_customer[1] == "e1"
    assert pd.isna(by_customer[2])


def test_lookup_columns_to_add_restricts_output(mod):
    out = _lookup_comp(mod, {
        "customer_id": [1, 2], "event_id": ["e1", "e2"], "region": ["east", "west"],
    }, {"columns_to_add": ["event_id"]})
    assert "event_id" in out.columns
    assert "region" not in out.columns


def test_lookup_rename_renames_added_columns(mod):
    out = _lookup_comp(mod, {
        "customer_id": [1, 2], "event_id": ["e1", "e2"],
    }, {"rename": {"event_id": "last_event_id"}})
    assert "last_event_id" in out.columns
    assert "event_id" not in out.columns


def test_lookup_rename_join_key_raises(mod):
    with pytest.raises(ValueError, match="join key"):
        _lookup_comp(mod, {"customer_id": [1, 2], "event_id": ["e1", "e2"]},
                     {"rename": {"customer_id": "cust_id"}})


def test_lookup_match_flag_column_with_columns_mode(mod):
    # match_flag_column attaches BOTH the matched columns AND a boolean flag --
    # distinct from output_mode='is_match', which emits only the boolean.
    out = _lookup_comp(mod, {"customer_id": [1], "event_id": ["e1"]},
                        {"match_flag_column": "matched"})
    assert "event_id" in out.columns
    by_customer = dict(zip(out["customer_id"], out["matched"]))
    assert by_customer[1] is True or by_customer[1] == True  # noqa: E712
    assert by_customer[2] is False or by_customer[2] == False  # noqa: E712
