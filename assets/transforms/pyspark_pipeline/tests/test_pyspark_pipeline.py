"""Real, end-to-end tests for PySparkPipelineComponent against a real local Spark
session (no mocking of Spark itself) -- every test actually materializes the asset
via dagster.materialize() and asserts on real computed values."""
import dagster as dg
import pandas as pd
import pytest


@dg.asset
def raw_orders():
    return pd.DataFrame({
        "customer_id": [1, 2, 3, 4, 5],
        "amount": [100.0, 50.0, 200.0, 10.0, 75.0],
        "status": ["paid", "paid", "unpaid", "paid", "paid"],
        "region": ["east", "east", "west", "west", "east"],
    })


@dg.asset
def raw_customers():
    return pd.DataFrame({
        "customer_id": [1, 2, 3, 4],
        "tier": ["gold", "silver", "gold", "gold"],
    })


def _materialize_single(comp, upstream_assets=()):
    defs = dg.Definitions.merge(
        dg.Definitions(assets=list(upstream_assets)) if upstream_assets else dg.Definitions(),
        comp.build_defs(None),
    )
    asset_key = dg.AssetKey(comp.asset_name)
    result = dg.materialize(list(upstream_assets) + [defs.get_assets_def(asset_key)])
    return result.output_for_node(comp.asset_name)


def test_flat_shape_filter(mod, spark):
    comp = mod.PySparkPipelineComponent(
        asset_name="paid_only",
        source={"kind": "upstream"},
        upstream_asset_key="raw_orders",
        operations=[{"op": "filter", "predicate": "status = 'paid'"}],
        sink={"kind": "none"},
    )
    df = _materialize_single(comp, [raw_orders])
    assert sorted(df["customer_id"].tolist()) == [1, 2, 4, 5]


def test_select_drop_rename(mod, spark):
    comp = mod.PySparkPipelineComponent(
        asset_name="renamed",
        source={"kind": "upstream"},
        upstream_asset_key="raw_orders",
        operations=[
            {"op": "select", "columns": ["customer_id", "amount", "status"]},
            {"op": "drop", "columns": ["status"]},
            {"op": "rename", "mapping": {"amount": "total_amount"}},
        ],
        sink={"kind": "none"},
    )
    df = _materialize_single(comp, [raw_orders])
    assert list(df.columns) == ["customer_id", "total_amount"]


def test_with_columns_expr(mod, spark):
    comp = mod.PySparkPipelineComponent(
        asset_name="doubled",
        source={"kind": "upstream"},
        upstream_asset_key="raw_orders",
        operations=[{"op": "with_columns", "expressions": {"doubled_amount": "amount * 2"}}],
        sink={"kind": "none"},
    )
    df = _materialize_single(comp, [raw_orders])
    df = df.sort_values("customer_id").reset_index(drop=True)
    assert df["doubled_amount"].tolist() == [200.0, 100.0, 400.0, 20.0, 150.0]


@pytest.mark.parametrize(
    "func,expected",
    [
        ("sum", 435.0),
        ("mean", 87.0),
        ("avg", 87.0),
        ("min", 10.0),
        ("max", 200.0),
        ("count", 5),
    ],
)
def test_group_by_aggs_overall(mod, spark, func, expected):
    comp = mod.PySparkPipelineComponent(
        asset_name=f"agg_{func}",
        source={"kind": "upstream"},
        upstream_asset_key="raw_orders",
        operations=[
            {"op": "with_columns", "expressions": {"_one": "1"}},
            {"op": "group_by", "group_by": ["_one"], "aggregations": {"result": {"col": "amount", "agg": func}}},
        ],
        sink={"kind": "none"},
    )
    df = _materialize_single(comp, [raw_orders])
    assert df["result"].iloc[0] == pytest.approx(expected)


def test_group_by_count_distinct(mod, spark):
    comp = mod.PySparkPipelineComponent(
        asset_name="agg_count_distinct",
        source={"kind": "upstream"},
        upstream_asset_key="raw_orders",
        operations=[
            {"op": "with_columns", "expressions": {"_one": "1"}},
            {"op": "group_by", "group_by": ["_one"],
             "aggregations": {"n_regions": {"col": "region", "agg": "countDistinct"}}},
        ],
        sink={"kind": "none"},
    )
    df = _materialize_single(comp, [raw_orders])
    assert df["n_regions"].iloc[0] == 2


def test_group_by_per_region(mod, spark):
    comp = mod.PySparkPipelineComponent(
        asset_name="region_totals",
        source={"kind": "upstream"},
        upstream_asset_key="raw_orders",
        operations=[
            {"op": "group_by", "group_by": ["region"], "aggregations": {"total": {"col": "amount", "agg": "sum"}}},
        ],
        sink={"kind": "none"},
    )
    df = _materialize_single(comp, [raw_orders]).set_index("region")
    assert df.loc["east", "total"] == pytest.approx(225.0)
    assert df.loc["west", "total"] == pytest.approx(210.0)


def test_sort_and_limit(mod, spark):
    comp = mod.PySparkPipelineComponent(
        asset_name="top_2_by_amount",
        source={"kind": "upstream"},
        upstream_asset_key="raw_orders",
        operations=[
            {"op": "sort", "by": "amount", "descending": True},
            {"op": "limit", "n": 2},
        ],
        sink={"kind": "none"},
    )
    df = _materialize_single(comp, [raw_orders])
    assert df["customer_id"].tolist() == [3, 1]


def test_distinct_and_drop_nulls(mod, spark):
    @dg.asset
    def raw_with_dupes_and_nulls():
        return pd.DataFrame({
            "a": [1, 1, 2, None],
            "b": ["x", "x", "y", "z"],
        })

    comp = mod.PySparkPipelineComponent(
        asset_name="cleaned",
        source={"kind": "upstream"},
        upstream_asset_key="raw_with_dupes_and_nulls",
        operations=[
            {"op": "distinct"},
            {"op": "drop_nulls", "subset": ["a"]},
        ],
        sink={"kind": "none"},
    )
    df = _materialize_single(comp, [raw_with_dupes_and_nulls])
    assert sorted(df["a"].tolist()) == [1, 2]


def test_multi_step_join_inner(mod, spark):
    comp = mod.PySparkPipelineComponent(
        asset_name="enriched_orders",
        steps=[
            {"id": "orders", "source": {"kind": "upstream", "upstream_asset_key": "raw_orders"},
             "operations": [{"op": "filter", "predicate": "status = 'paid'"}]},
            {"id": "customers", "source": {"kind": "upstream", "upstream_asset_key": "raw_customers"},
             "operations": []},
            {"id": "joined", "source": {"kind": "ref", "ref": "orders"},
             "operations": [
                 {"op": "join", "right": {"ref": "customers"}, "on_columns": ["customer_id"], "how": "inner"},
             ]},
        ],
        sinks=[{"from": "joined", "kind": "none"}],
    )
    df = _materialize_single(comp, [raw_orders, raw_customers])
    # customer_id=5 is paid but has no row in raw_customers (which only has ids 1-4),
    # so the inner join must exclude it even though it passed the upstream filter.
    assert sorted(df["customer_id"].tolist()) == [1, 2, 4]
    assert set(df.columns) >= {"customer_id", "amount", "tier"}


def test_join_left_on_right_on(mod, spark):
    comp = mod.PySparkPipelineComponent(
        asset_name="joined_left_right_on",
        steps=[
            {"id": "orders", "source": {"kind": "upstream", "upstream_asset_key": "raw_orders"}, "operations": []},
            {"id": "customers", "source": {"kind": "upstream", "upstream_asset_key": "raw_customers"},
             "operations": [{"op": "rename", "mapping": {"customer_id": "cust_id"}}]},
            {"id": "joined", "source": {"kind": "ref", "ref": "orders"},
             "operations": [
                 {"op": "join", "right": {"ref": "customers"},
                  "left_on": ["customer_id"], "right_on": ["cust_id"], "how": "inner"},
             ]},
        ],
        sinks=[{"from": "joined", "kind": "none"}],
    )
    df = _materialize_single(comp, [raw_orders, raw_customers])
    assert "tier" in df.columns
    assert len(df) == 4  # 4 of 5 orders have a matching customer row


def test_join_semi_and_anti(mod, spark):
    comp_semi = mod.PySparkPipelineComponent(
        asset_name="semi_join",
        steps=[
            {"id": "orders", "source": {"kind": "upstream", "upstream_asset_key": "raw_orders"}, "operations": []},
            {"id": "customers", "source": {"kind": "upstream", "upstream_asset_key": "raw_customers"}, "operations": []},
            {"id": "joined", "source": {"kind": "ref", "ref": "orders"},
             "operations": [{"op": "join", "right": {"ref": "customers"}, "on_columns": ["customer_id"], "how": "left_semi"}]},
        ],
        sinks=[{"from": "joined", "kind": "none"}],
    )
    semi_df = _materialize_single(comp_semi, [raw_orders, raw_customers])
    assert sorted(semi_df["customer_id"].tolist()) == [1, 2, 3, 4]
    assert "tier" not in semi_df.columns  # semi join keeps only the left side's columns

    comp_anti = mod.PySparkPipelineComponent(
        asset_name="anti_join",
        steps=[
            {"id": "orders", "source": {"kind": "upstream", "upstream_asset_key": "raw_orders"}, "operations": []},
            {"id": "customers", "source": {"kind": "upstream", "upstream_asset_key": "raw_customers"}, "operations": []},
            {"id": "joined", "source": {"kind": "ref", "ref": "orders"},
             "operations": [{"op": "join", "right": {"ref": "customers"}, "on_columns": ["customer_id"], "how": "left_anti"}]},
        ],
        sinks=[{"from": "joined", "kind": "none"}],
    )
    anti_df = _materialize_single(comp_anti, [raw_orders, raw_customers])
    assert anti_df["customer_id"].tolist() == [5]


def test_union_plain_and_distinct(mod, spark):
    comp_plain = mod.PySparkPipelineComponent(
        asset_name="unioned_plain",
        steps=[
            {"id": "a", "source": {"kind": "upstream", "upstream_asset_key": "raw_orders"},
             "operations": [{"op": "select", "columns": ["customer_id"]}, {"op": "filter", "predicate": "customer_id <= 2"}]},
            {"id": "b", "source": {"kind": "ref", "ref": "a"}, "operations": [{"op": "union", "other": {"ref": "a"}}]},
        ],
        sinks=[{"from": "b", "kind": "none"}],
    )
    df_plain = _materialize_single(comp_plain, [raw_orders])
    assert len(df_plain) == 4  # duplicated, no distinct

    comp_distinct = mod.PySparkPipelineComponent(
        asset_name="unioned_distinct",
        steps=[
            {"id": "a", "source": {"kind": "upstream", "upstream_asset_key": "raw_orders"},
             "operations": [{"op": "select", "columns": ["customer_id"]}, {"op": "filter", "predicate": "customer_id <= 2"}]},
            {"id": "b", "source": {"kind": "ref", "ref": "a"}, "operations": [{"op": "union", "other": {"ref": "a"}, "distinct": True}]},
        ],
        sinks=[{"from": "b", "kind": "none"}],
    )
    df_distinct = _materialize_single(comp_distinct, [raw_orders])
    assert len(df_distinct) == 2


def test_sql_op_self_and_other_step(mod, spark):
    comp = mod.PySparkPipelineComponent(
        asset_name="sql_commission",
        steps=[
            {"id": "paid_orders", "source": {"kind": "upstream", "upstream_asset_key": "raw_orders"},
             "operations": [{"op": "filter", "predicate": "status = 'paid'"}]},
            {"id": "gold_customers", "source": {"kind": "upstream", "upstream_asset_key": "raw_customers"},
             "operations": [{"op": "filter", "predicate": "tier = 'gold'"}]},
            {"id": "enriched", "source": {"kind": "ref", "ref": "paid_orders"},
             "operations": [
                 {"op": "sql", "sql": "SELECT self.*, gold_customers.tier FROM self "
                                       "JOIN gold_customers ON self.customer_id = gold_customers.customer_id"},
                 {"op": "with_columns", "expressions": {"commission": "amount * 0.15"}},
             ]},
        ],
        sinks=[{"from": "enriched", "kind": "none"}],
    )
    df = _materialize_single(comp, [raw_orders, raw_customers])
    row = df[df["customer_id"] == 1].iloc[0]
    assert row["commission"] == pytest.approx(15.0)
    assert row["tier"] == "gold"


def test_parquet_sink_roundtrip(mod, spark, tmp_path):
    out_dir = tmp_path / "paid_orders_out"
    comp = mod.PySparkPipelineComponent(
        asset_name="paid_orders_to_parquet",
        source={"kind": "upstream"},
        upstream_asset_key="raw_orders",
        operations=[{"op": "filter", "predicate": "status = 'paid'"}],
        sink={"kind": "parquet", "path": str(out_dir), "mode": "overwrite"},
    )
    defs = dg.Definitions.merge(dg.Definitions(assets=[raw_orders]), comp.build_defs(None))
    result = dg.materialize([raw_orders, defs.get_assets_def(dg.AssetKey(comp.asset_name))])
    assert result.success
    round_tripped = spark.read.parquet(str(out_dir)).toPandas()
    assert sorted(round_tripped["customer_id"].tolist()) == [1, 2, 4, 5]
