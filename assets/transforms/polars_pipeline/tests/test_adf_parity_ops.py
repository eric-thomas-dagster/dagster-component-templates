"""Tests closing ADF (Azure Data Factory) Data Flow transform parity gaps on
PolarsPipelineComponent: `join` how=semi/anti (ADF's Exists transform, confirmed
end-to-end rather than assumed from the `_maintain_order_by_how` dict alone),
`sort`'s new `nulls_last`, and the three new ops `pivot` (ADF Pivot), `lookup`
(ADF Lookup), and `window_calculation` (ADF Window's ntile/cume_dist statistical
functions).
"""
import pytest

from .conftest import load_component_module

pytest.importorskip("polars")

import dagster as dg


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize_steps(mod, steps, asset_name="out_asset", primary_step=None):
    comp = mod.PolarsPipelineComponent(
        asset_name=asset_name,
        steps=steps,
        primary_step=primary_step,
    )
    result = dg.materialize(
        comp.build_defs(None).assets,
        resources={"io_manager": dg.InMemoryIOManager()},
    )
    assert result.success
    return result.output_for_node(asset_name)


# ── join how=semi / how=anti (ADF's Exists transform) ──────────────────────


def test_join_semi_keeps_matched_left_rows_no_right_columns(mod):
    out = _materialize_steps(
        mod,
        steps=[
            {"id": "left", "source": {"kind": "inline", "rows": [
                {"customer_id": 1, "name": "a"},
                {"customer_id": 2, "name": "b"},
                {"customer_id": 3, "name": "c"},
            ]}, "operations": []},
            {"id": "right", "source": {"kind": "inline", "rows": [
                {"customer_id": 1, "tier": "gold"},
                {"customer_id": 2, "tier": "silver"},
            ]}, "operations": []},
            {"id": "out", "source": {"kind": "ref", "ref": "left"},
             "operations": [{"op": "join", "right": {"ref": "right"}, "on_columns": ["customer_id"], "how": "semi"}]},
        ],
    )
    assert sorted(out["customer_id"].to_list()) == [1, 2]
    # Exists-semantics: no right-side columns leak into the output.
    assert "tier" not in out.columns


def test_join_anti_keeps_only_unmatched_left_rows(mod):
    out = _materialize_steps(
        mod,
        steps=[
            {"id": "left", "source": {"kind": "inline", "rows": [
                {"customer_id": 1, "name": "a"},
                {"customer_id": 2, "name": "b"},
                {"customer_id": 3, "name": "c"},
            ]}, "operations": []},
            {"id": "right", "source": {"kind": "inline", "rows": [
                {"customer_id": 1, "tier": "gold"},
                {"customer_id": 2, "tier": "silver"},
            ]}, "operations": []},
            {"id": "out", "source": {"kind": "ref", "ref": "left"},
             "operations": [{"op": "join", "right": {"ref": "right"}, "on_columns": ["customer_id"], "how": "anti"}]},
        ],
    )
    assert out["customer_id"].to_list() == [3]
    assert "tier" not in out.columns


# ── sort nulls_last ──────────────────────────────────────────────────────────


def test_sort_default_nulls_first(mod):
    out = _materialize_steps(
        mod,
        steps=[
            {"id": "s", "source": {"kind": "inline", "rows": [
                {"v": 2}, {"v": None}, {"v": 1},
            ]}, "operations": [{"op": "sort", "by": ["v"]}]},
        ],
        asset_name="sorted_default",
    )
    assert out["v"].to_list() == [None, 1, 2]


def test_sort_nulls_last_true(mod):
    out = _materialize_steps(
        mod,
        steps=[
            {"id": "s", "source": {"kind": "inline", "rows": [
                {"v": 2}, {"v": None}, {"v": 1},
            ]}, "operations": [{"op": "sort", "by": ["v"], "nulls_last": True}]},
        ],
        asset_name="sorted_nulls_last",
    )
    assert out["v"].to_list() == [1, 2, None]


# ── pivot (ADF's Pivot transform) ───────────────────────────────────────────


def test_pivot_multi_index_multi_value_and_pivot_values_with_absent_value(mod):
    out = _materialize_steps(
        mod,
        steps=[
            {"id": "s", "source": {"kind": "inline", "rows": [
                {"region": "east", "segment": "retail", "quarter": "Q1", "revenue": 10, "cost": 1},
                {"region": "east", "segment": "retail", "quarter": "Q2", "revenue": 20, "cost": 2},
                {"region": "west", "segment": "retail", "quarter": "Q1", "revenue": 30, "cost": 3},
                {"region": "west", "segment": "retail", "quarter": "Q2", "revenue": 40, "cost": 4},
            ]}, "operations": [{
                "op": "pivot",
                "index": ["region", "segment"],
                "pivot_column": "quarter",
                "value_columns": ["revenue", "cost"],
                "agg_func": "sum",
                # Q3 is requested but absent from the data -- ADF's
                # declared-schema Pivot still produces the column, filled.
                "pivot_values": ["Q1", "Q2", "Q3"],
                "fill_value": 0,
            }]},
        ],
        asset_name="pivoted",
    )
    assert set(out.columns) == {
        "region", "segment", "revenue_Q1", "revenue_Q2", "revenue_Q3",
        "cost_Q1", "cost_Q2", "cost_Q3",
    }
    by_region = {row["region"]: row for row in out.to_dicts()}
    assert by_region["east"]["revenue_Q1"] == 10
    assert by_region["east"]["revenue_Q2"] == 20
    assert by_region["east"]["revenue_Q3"] == 0  # absent value -> filled, not missing
    assert by_region["east"]["cost_Q1"] == 1
    assert by_region["west"]["revenue_Q1"] == 30
    assert by_region["west"]["cost_Q3"] == 0


def test_pivot_single_value_column_bare_column_names(mod):
    out = _materialize_steps(
        mod,
        steps=[
            {"id": "s", "source": {"kind": "inline", "rows": [
                {"region": "east", "quarter": "Q1", "revenue": 10},
                {"region": "east", "quarter": "Q2", "revenue": 20},
            ]}, "operations": [{
                "op": "pivot",
                "index": ["region"],
                "pivot_column": "quarter",
                "value_columns": ["revenue"],
                "agg_func": "sum",
            }]},
        ],
        asset_name="pivoted_single",
    )
    # Single value_column -> bare pivot-value column names, no "revenue_" prefix.
    assert set(out.columns) == {"region", "Q1", "Q2"}


# ── lookup (ADF's Lookup transform) ─────────────────────────────────────────


def test_lookup_columns_mode_multi_match_resolved_by_sort(mod):
    # customer 1 has two right-side rows; match_sort_by + descending picks
    # the row with the highest priority (10, tier=gold) as the single match.
    out = _materialize_steps(
        mod,
        steps=[
            {"id": "left", "source": {"kind": "inline", "rows": [
                {"customer_id": 1, "name": "a"},
                {"customer_id": 2, "name": "b"},
                {"customer_id": 3, "name": "c"},
            ]}, "operations": []},
            {"id": "right", "source": {"kind": "inline", "rows": [
                {"customer_id": 1, "priority": 5, "tier": "bronze"},
                {"customer_id": 1, "priority": 10, "tier": "gold"},
                {"customer_id": 2, "priority": 1, "tier": "silver"},
            ]}, "operations": []},
            {"id": "out", "source": {"kind": "ref", "ref": "left"},
             "operations": [{
                 "op": "lookup", "right": {"ref": "right"}, "on_columns": ["customer_id"],
                 "match_sort_by": ["priority"], "match_sort_ascending": False,
             }]},
        ],
    )
    by_id = {row["customer_id"]: row for row in out.to_dicts()}
    assert by_id[1]["tier"] == "gold"  # highest-priority match wins, exactly one row
    assert by_id[2]["tier"] == "silver"
    assert by_id[3]["tier"] is None  # no match -> left-join null
    assert len(out) == 3  # no fan-out despite customer 1 having two right rows


def test_lookup_is_match_mode_drops_right_columns(mod):
    out = _materialize_steps(
        mod,
        steps=[
            {"id": "left", "source": {"kind": "inline", "rows": [
                {"customer_id": 1, "name": "a"},
                {"customer_id": 2, "name": "b"},
                {"customer_id": 3, "name": "c"},
            ]}, "operations": []},
            {"id": "right", "source": {"kind": "inline", "rows": [
                {"customer_id": 1, "priority": 5, "tier": "bronze"},
                {"customer_id": 1, "priority": 10, "tier": "gold"},
                {"customer_id": 2, "priority": 1, "tier": "silver"},
            ]}, "operations": []},
            {"id": "out", "source": {"kind": "ref", "ref": "left"},
             "operations": [{
                 "op": "lookup", "right": {"ref": "right"}, "on_columns": ["customer_id"],
                 "output_mode": "is_match", "output_column": "found",
             }]},
        ],
    )
    by_id = {row["customer_id"]: row for row in out.to_dicts()}
    assert by_id[1]["found"] is True
    assert by_id[2]["found"] is True
    assert by_id[3]["found"] is False
    # Existence check only -- no right-side columns (tier/priority) leak through.
    assert set(out.columns) == {"customer_id", "name", "found"}
    assert len(out) == 3  # no fan-out from customer 1's two right-side rows


# ── window_calculation (ADF's Window transform: ntile + cume_dist) ─────────


def test_window_calculation_ntile_hand_verified_buckets(mod):
    # 10 rows, n=4 buckets -> standard SQL NTILE bucket sizes [3, 3, 2, 2]
    # (hand-verified against Postgres' own NTILE(4) OVER (ORDER BY v)):
    # ranks 1-3 -> bucket 1, 4-6 -> bucket 2, 7-8 -> bucket 3, 9-10 -> bucket 4.
    rows = [{"v": i} for i in range(1, 11)]
    out = _materialize_steps(
        mod,
        steps=[
            {"id": "s", "source": {"kind": "inline", "rows": rows}, "operations": [{
                "op": "window_calculation", "function": "ntile",
                "order_by": ["v"], "n": 4, "output_column": "bucket",
            }]},
        ],
        asset_name="ntiled",
    )
    out_sorted = out.sort("v")
    assert out_sorted["bucket"].to_list() == [1, 1, 1, 2, 2, 2, 3, 3, 4, 4]


def test_window_calculation_ntile_per_group(mod):
    rows = [{"grp": "a", "v": i} for i in range(1, 11)] + [{"grp": "b", "v": i} for i in range(1, 5)]
    out = _materialize_steps(
        mod,
        steps=[
            {"id": "s", "source": {"kind": "inline", "rows": rows}, "operations": [{
                "op": "window_calculation", "function": "ntile",
                "order_by": ["v"], "group_by": ["grp"], "n": 4, "output_column": "bucket",
            }]},
        ],
        asset_name="ntiled_grouped",
    )
    a_buckets = out.filter(out["grp"] == "a").sort("v")["bucket"].to_list()
    b_buckets = out.filter(out["grp"] == "b").sort("v")["bucket"].to_list()
    assert a_buckets == [1, 1, 1, 2, 2, 2, 3, 3, 4, 4]
    # group b: 4 rows, 4 buckets -> base=1, rem=0 -> each row its own bucket.
    assert b_buckets == [1, 2, 3, 4]


def test_window_calculation_cume_dist_hand_verified(mod):
    # values [10, 20, 20, 30] -> cume_dist [0.25, 0.75, 0.75, 1.0]
    # (hand-verified: count(<=10)/4=0.25, count(<=20)/4=0.75 for both 20s,
    # count(<=30)/4=1.0).
    rows = [{"v": 10}, {"v": 20}, {"v": 20}, {"v": 30}]
    out = _materialize_steps(
        mod,
        steps=[
            {"id": "s", "source": {"kind": "inline", "rows": rows}, "operations": [{
                "op": "window_calculation", "function": "cume_dist",
                "order_by": ["v"], "output_column": "cd",
            }]},
        ],
        asset_name="cume_dist_out",
    )
    out_sorted = out.sort("v")
    assert out_sorted["cd"].to_list() == [0.25, 0.75, 0.75, 1.0]


# ── lookup: multiple_match / on_no_match / columns_to_add / rename / match_flag_column ─────
# (richer field shape ported from the canonical standalone LookupComponent, for cross-pipeline
# consistency with pandas_pipeline/pyspark_pipeline's own lookup ops.)

def _lookup_steps(right_rows, op_extra):
    return [
        {"id": "left", "source": {"kind": "inline", "rows": [
            {"customer_id": 1}, {"customer_id": 2},
        ]}, "operations": []},
        {"id": "right", "source": {"kind": "inline", "rows": right_rows}, "operations": []},
        {"id": "out", "source": {"kind": "ref", "ref": "left"},
         "operations": [{"op": "lookup", "right": {"ref": "right"}, "on_columns": ["customer_id"], **op_extra}]},
    ]


def test_lookup_multiple_match_last(mod):
    out = _materialize_steps(mod, steps=_lookup_steps(
        [
            {"customer_id": 1, "event_time": "2024-01-02", "event_id": "e_early"},
            {"customer_id": 1, "event_time": "2024-01-05", "event_id": "e_late"},
            {"customer_id": 2, "event_time": "2024-01-01", "event_id": "e_only"},
        ],
        {"multiple_match": "last", "match_sort_by": ["event_time"]},
    ), asset_name="mm_last")
    assert len(out) == 2
    by_id = {row["customer_id"]: row["event_id"] for row in out.to_dicts()}
    assert by_id[1] == "e_late"
    assert by_id[2] == "e_only"


def test_lookup_multiple_match_all_fans_out(mod):
    out = _materialize_steps(mod, steps=_lookup_steps(
        [
            {"customer_id": 1, "event_id": "e1"},
            {"customer_id": 1, "event_id": "e2"},
            {"customer_id": 2, "event_id": "e3"},
        ],
        {"multiple_match": "all"},
    ), asset_name="mm_all")
    assert len(out) == 3
    assert sorted(row["event_id"] for row in out.to_dicts() if row["customer_id"] == 1) == ["e1", "e2"]


def test_lookup_multiple_match_all_as_list(mod):
    out = _materialize_steps(mod, steps=_lookup_steps(
        [
            {"customer_id": 1, "event_id": "e1"},
            {"customer_id": 1, "event_id": "e2"},
            {"customer_id": 2, "event_id": "e3"},
        ],
        {"multiple_match": "all_as_list"},
    ), asset_name="mm_all_as_list")
    assert len(out) == 2
    by_id = {row["customer_id"]: row["event_id"] for row in out.to_dicts()}
    assert sorted(by_id[1]) == ["e1", "e2"]
    assert by_id[2] == ["e3"]


def test_lookup_on_no_match_error_raises(mod):
    with pytest.raises(Exception, match="on_no_match='error'"):
        _materialize_steps(mod, steps=_lookup_steps(
            [{"customer_id": 1, "event_id": "e1"}], {"on_no_match": "error"},
        ), asset_name="no_match_error")


def test_lookup_on_no_match_pass_is_default(mod):
    out = _materialize_steps(mod, steps=_lookup_steps(
        [{"customer_id": 1, "event_id": "e1"}], {},
    ), asset_name="no_match_pass")
    by_id = {row["customer_id"]: row["event_id"] for row in out.to_dicts()}
    assert by_id[1] == "e1"
    assert by_id[2] is None


def test_lookup_columns_to_add_restricts_output(mod):
    out = _materialize_steps(mod, steps=_lookup_steps(
        [
            {"customer_id": 1, "event_id": "e1", "region": "east"},
            {"customer_id": 2, "event_id": "e2", "region": "west"},
        ],
        {"columns_to_add": ["event_id"]},
    ), asset_name="cols_restrict")
    assert "event_id" in out.columns
    assert "region" not in out.columns


def test_lookup_rename_renames_added_columns(mod):
    out = _materialize_steps(mod, steps=_lookup_steps(
        [{"customer_id": 1, "event_id": "e1"}, {"customer_id": 2, "event_id": "e2"}],
        {"rename": {"event_id": "last_event_id"}},
    ), asset_name="renamed")
    assert "last_event_id" in out.columns
    assert "event_id" not in out.columns


def test_lookup_rename_join_key_raises(mod):
    with pytest.raises(Exception, match="join key"):
        _materialize_steps(mod, steps=_lookup_steps(
            [{"customer_id": 1, "event_id": "e1"}], {"rename": {"customer_id": "cust_id"}},
        ), asset_name="rename_key_err")


def test_lookup_match_flag_column_with_columns_mode(mod):
    out = _materialize_steps(mod, steps=_lookup_steps(
        [{"customer_id": 1, "event_id": "e1"}], {"match_flag_column": "matched"},
    ), asset_name="match_flag")
    assert "event_id" in out.columns
    by_id = {row["customer_id"]: row["matched"] for row in out.to_dicts()}
    assert by_id[1] is True
    assert by_id[2] is False
