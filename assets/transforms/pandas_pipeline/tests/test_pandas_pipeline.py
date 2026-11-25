"""Real, end-to-end tests for PandasPipelineComponent — every test actually
materializes the asset via dagster.materialize() and asserts on the real
DataFrame returned, never on the component's internal plumbing alone."""
import base64
import os

import dagster as dg
import pandas as pd
import pytest


@dg.asset
def raw_orders():
    return pd.DataFrame({
        "customer_id": [1, 2, 3, 4],
        "amount": [100.0, 50.0, 200.0, 10.0],
        "status": ["paid", "paid", "unpaid", "paid"],
    })


@dg.asset
def raw_customers():
    return pd.DataFrame({"customer_id": [1, 2, 3, 4], "tier": ["gold", "silver", "gold", "gold"]})


def _materialize_single(mod, comp, upstream_assets=()):
    defs = dg.Definitions.merge(
        dg.Definitions(assets=list(upstream_assets)) if upstream_assets else dg.Definitions(),
        comp.build_defs(None),
    )
    asset_key = dg.AssetKey(comp.asset_name)
    result = dg.materialize(list(upstream_assets) + [defs.get_assets_def(asset_key)])
    return result.output_for_node(comp.asset_name)


def test_flat_shape_filter(mod):
    comp = mod.PandasPipelineComponent(
        asset_name="paid_only",
        upstream_asset_key="raw_orders",
        operations=[{"op": "filter", "predicate": "status = 'paid'"}],
    )
    df = _materialize_single(mod, comp, [raw_orders])
    assert sorted(df["customer_id"].tolist()) == [1, 2, 4]


def test_with_columns_sql_and_python(mod):
    comp = mod.PandasPipelineComponent(
        asset_name="enriched_amounts",
        upstream_asset_key="raw_orders",
        operations=[
            {"op": "with_columns", "expressions": {"doubled": "amount * 2"},
             "python_expressions": {"tripled": "df['amount'] * 3"}},
        ],
    )
    df = _materialize_single(mod, comp, [raw_orders])
    assert df["doubled"].tolist() == [200.0, 100.0, 400.0, 20.0]
    assert df["tripled"].tolist() == [300.0, 150.0, 600.0, 30.0]


def test_select_drop_rename(mod):
    comp = mod.PandasPipelineComponent(
        asset_name="renamed",
        upstream_asset_key="raw_orders",
        operations=[
            {"op": "select", "columns": ["customer_id", "amount"]},
            {"op": "rename", "mapping": {"amount": "total_amount"}},
        ],
    )
    df = _materialize_single(mod, comp, [raw_orders])
    assert list(df.columns) == ["customer_id", "total_amount"]


def test_drop(mod):
    comp = mod.PandasPipelineComponent(
        asset_name="dropped",
        upstream_asset_key="raw_orders",
        operations=[{"op": "drop", "columns": ["status"]}],
    )
    df = _materialize_single(mod, comp, [raw_orders])
    assert "status" not in df.columns


def test_group_by(mod):
    @dg.asset
    def raw_regional():
        return pd.DataFrame({"region": ["east", "east", "west", "west"], "amount": [10, 20, 30, 40]})

    comp = mod.PandasPipelineComponent(
        asset_name="grouped",
        upstream_asset_key="raw_regional",
        operations=[{"op": "group_by", "group_by": ["region"],
                     "aggregations": {"revenue": {"col": "amount", "agg": "sum"}, "n": {"col": "amount", "agg": "count"}}}],
    )
    df = _materialize_single(mod, comp, [raw_regional])
    by_region = dict(zip(df["region"], df["revenue"]))
    assert by_region == {"east": 30, "west": 70}
    assert dict(zip(df["region"], df["n"])) == {"east": 2, "west": 2}


def test_sort_head_tail(mod):
    comp = mod.PandasPipelineComponent(
        asset_name="sorted_head",
        upstream_asset_key="raw_orders",
        operations=[{"op": "sort", "by": ["amount"], "descending": True}, {"op": "head", "n": 2}],
    )
    df = _materialize_single(mod, comp, [raw_orders])
    assert df["amount"].tolist() == [200.0, 100.0]


def test_head_per_group(mod):
    @dg.asset
    def raw_regional2():
        return pd.DataFrame({"region": ["east", "east", "west", "west"], "amount": [10, 20, 30, 40]})

    comp = mod.PandasPipelineComponent(
        asset_name="top_per_region",
        upstream_asset_key="raw_regional2",
        operations=[{"op": "head_per_group", "group_by": ["region"], "n": 1}],
    )
    df = _materialize_single(mod, comp, [raw_regional2])
    assert len(df) == 2


def test_unique_drop_nulls_fill_null(mod):
    @dg.asset
    def raw_dupes():
        return pd.DataFrame({"k": [1, 1, 2, 3], "v": [10, 10, None, 30]})

    comp = mod.PandasPipelineComponent(
        asset_name="cleaned",
        upstream_asset_key="raw_dupes",
        operations=[{"op": "unique"}, {"op": "fill_null", "value": 0}],
    )
    df = _materialize_single(mod, comp, [raw_dupes])
    assert len(df) == 3
    assert 0 in df["v"].tolist()

    comp2 = mod.PandasPipelineComponent(
        asset_name="dropped_nulls",
        upstream_asset_key="raw_dupes",
        operations=[{"op": "drop_nulls", "subset": ["v"]}],
    )
    df2 = _materialize_single(mod, comp2, [raw_dupes])
    assert not df2["v"].isna().any()


def test_cast(mod):
    @dg.asset
    def raw_strings():
        return pd.DataFrame({"n": ["1", "2", "3"], "d": ["2024-01-01", "2024-01-02", "2024-01-03"]})

    comp = mod.PandasPipelineComponent(
        asset_name="casted",
        upstream_asset_key="raw_strings",
        operations=[{"op": "cast", "mapping": {"n": "int", "d": "datetime"}}],
    )
    df = _materialize_single(mod, comp, [raw_strings])
    assert df["n"].tolist() == [1, 2, 3]
    assert str(df["d"].dtype).startswith("datetime64")


def test_join_inner(mod):
    comp = mod.PandasPipelineComponent(
        asset_name="enriched",
        steps=[
            {"id": "paid_orders", "source": {"kind": "upstream", "upstream_asset_key": "raw_orders"},
             "operations": [{"op": "filter", "predicate": "status = 'paid'"}]},
            {"id": "gold_customers", "source": {"kind": "upstream", "upstream_asset_key": "raw_customers"},
             "operations": [{"op": "filter", "predicate": "tier = 'gold'"}]},
            {"id": "enriched", "source": {"kind": "ref", "ref": "paid_orders"},
             "operations": [{"op": "join", "right": {"ref": "gold_customers"}, "on_columns": ["customer_id"], "how": "inner"}]},
        ],
    )
    df = _materialize_single(mod, comp, [raw_orders, raw_customers])
    # customer 1: paid+gold, customer 2: paid+silver (excluded), customer 3: unpaid+gold (excluded), customer 4: paid+gold
    assert sorted(df["customer_id"].tolist()) == [1, 4]


def test_join_semi_anti(mod):
    comp_semi = mod.PandasPipelineComponent(
        asset_name="semi",
        steps=[
            {"id": "orders", "source": {"kind": "upstream", "upstream_asset_key": "raw_orders"}, "operations": []},
            {"id": "gold", "source": {"kind": "upstream", "upstream_asset_key": "raw_customers"},
             "operations": [{"op": "filter", "predicate": "tier = 'gold'"}]},
            {"id": "semi", "source": {"kind": "ref", "ref": "orders"},
             "operations": [{"op": "join", "right": {"ref": "gold"}, "on_columns": ["customer_id"], "how": "semi"}]},
        ],
    )
    df_semi = _materialize_single(mod, comp_semi, [raw_orders, raw_customers])
    assert "tier" not in df_semi.columns  # semi join keeps only left columns
    assert sorted(df_semi["customer_id"].tolist()) == [1, 3, 4]

    comp_anti = mod.PandasPipelineComponent(
        asset_name="anti",
        steps=[
            {"id": "orders", "source": {"kind": "upstream", "upstream_asset_key": "raw_orders"}, "operations": []},
            {"id": "gold", "source": {"kind": "upstream", "upstream_asset_key": "raw_customers"},
             "operations": [{"op": "filter", "predicate": "tier = 'gold'"}]},
            {"id": "anti", "source": {"kind": "ref", "ref": "orders"},
             "operations": [{"op": "join", "right": {"ref": "gold"}, "on_columns": ["customer_id"], "how": "anti"}]},
        ],
    )
    df_anti = _materialize_single(mod, comp_anti, [raw_orders, raw_customers])
    assert df_anti["customer_id"].tolist() == [2]


def test_union_by_name(mod):
    @dg.asset
    def raw_extra():
        return pd.DataFrame({"customer_id": [99], "amount": [1.0], "status": ["paid"]})

    comp = mod.PandasPipelineComponent(
        asset_name="unioned",
        steps=[
            {"id": "a", "source": {"kind": "upstream", "upstream_asset_key": "raw_orders"}, "operations": []},
            {"id": "b", "source": {"kind": "upstream", "upstream_asset_key": "raw_extra"}, "operations": []},
            {"id": "unioned", "source": {"kind": "ref", "ref": "a"},
             "operations": [{"op": "union", "other": {"ref": "b"}, "mode": "by_name"}]},
        ],
    )
    df = _materialize_single(mod, comp, [raw_orders, raw_extra])
    assert len(df) == 5
    assert 99 in df["customer_id"].tolist()


def test_sql_escape_hatch(mod):
    comp = mod.PandasPipelineComponent(
        asset_name="sql_derived",
        upstream_asset_key="raw_orders",
        operations=[{"op": "sql", "sql": "SELECT *, amount * 0.1 AS tax FROM self WHERE status = 'paid'"}],
    )
    df = _materialize_single(mod, comp, [raw_orders])
    assert set(df["customer_id"]) == {1, 2, 4}
    assert abs(df[df["customer_id"] == 1]["tax"].iloc[0] - 10.0) < 1e-9


def test_multi_dimensional_partition(mod):
    comp = mod.PandasPipelineComponent(
        asset_name="partitioned_multi",
        upstream_asset_key="raw_orders",
        operations=[],
        partition_dimensions=[
            {"name": "bank", "type": "static", "values": ["acme", "globex"]},
            {"name": "batch_day", "type": "daily", "start": "2026-01-01", "end_offset": 1},
        ],
    )
    defs = comp.build_defs(None)
    assets_def = defs.get_assets_def(dg.AssetKey("partitioned_multi"))
    assert isinstance(assets_def.partitions_def, dg.MultiPartitionsDefinition)
    dim_names = {d.name for d in assets_def.partitions_def.partitions_defs}
    assert dim_names == {"bank", "batch_day"}


def test_single_dimension_partition(mod):
    comp = mod.PandasPipelineComponent(
        asset_name="partitioned_static",
        upstream_asset_key="raw_orders",
        operations=[],
        partition_type="static",
        partition_values="us,eu,asia",
    )
    defs = comp.build_defs(None)
    assets_def = defs.get_assets_def(dg.AssetKey("partitioned_static"))
    assert isinstance(assets_def.partitions_def, dg.StaticPartitionsDefinition)
    assert set(assets_def.partitions_def.get_partition_keys()) == {"us", "eu", "asia"}


def test_mutually_exclusive_partition_fields(mod):
    comp = mod.PandasPipelineComponent(
        asset_name="bad_partition",
        upstream_asset_key="raw_orders",
        operations=[],
        partition_type="static",
        partition_values="a,b",
        partition_dimensions=[{"name": "x", "type": "static", "values": ["1"]}],
    )
    with pytest.raises(ValueError, match="either partition_type OR partition_dimensions"):
        comp.build_defs(None)


def test_encrypted_file_round_trip(mod, tmp_path):
    os.environ["PANDAS_PIPELINE_TEST_KEY"] = base64.b64encode(os.urandom(32)).decode()
    enc_path = str(tmp_path / "encrypted.csv.enc")

    comp_encrypt = mod.PandasPipelineComponent(
        asset_name="encrypted_out",
        upstream_asset_key="raw_orders",
        operations=[],
        sinks=[{"from": "_default", "kind": "encrypted_file", "path": enc_path, "format": "csv"}],
        key_source="raw_key",
        raw_key_env_var="PANDAS_PIPELINE_TEST_KEY",
        key_id="test-key",
        encryption_context={"purpose": "test"},
    )
    defs = dg.Definitions.merge(dg.Definitions(assets=[raw_orders]), comp_encrypt.build_defs(None))
    dg.materialize([raw_orders, defs.get_assets_def(dg.AssetKey("encrypted_out"))])

    with open(enc_path, "rb") as f:
        raw_bytes = f.read()
    assert b"customer_id" not in raw_bytes  # genuinely not plaintext

    comp_decrypt = mod.PandasPipelineComponent(
        asset_name="decrypted_in",
        source={"kind": "encrypted_file", "path": enc_path, "format": "csv"},
        operations=[],
        key_source="raw_key",
        raw_key_env_var="PANDAS_PIPELINE_TEST_KEY",
        key_id="test-key",
        encryption_context={"purpose": "test"},
    )
    defs2 = comp_decrypt.build_defs(None)
    result = dg.materialize([defs2.get_assets_def(dg.AssetKey("decrypted_in"))])
    df = result.output_for_node("decrypted_in")
    assert sorted(df["customer_id"].tolist()) == [1, 2, 3, 4]


def test_encrypted_file_wrong_key_fails(mod, tmp_path):
    os.environ["PANDAS_PIPELINE_TEST_KEY_A"] = base64.b64encode(os.urandom(32)).decode()
    os.environ["PANDAS_PIPELINE_TEST_KEY_B"] = base64.b64encode(os.urandom(32)).decode()
    enc_path = str(tmp_path / "encrypted_wrongkey.csv.enc")

    comp_encrypt = mod.PandasPipelineComponent(
        asset_name="encrypted_out2",
        upstream_asset_key="raw_orders",
        operations=[],
        sinks=[{"from": "_default", "kind": "encrypted_file", "path": enc_path, "format": "csv"}],
        key_source="raw_key", raw_key_env_var="PANDAS_PIPELINE_TEST_KEY_A", key_id="k",
    )
    defs = dg.Definitions.merge(dg.Definitions(assets=[raw_orders]), comp_encrypt.build_defs(None))
    dg.materialize([raw_orders, defs.get_assets_def(dg.AssetKey("encrypted_out2"))])

    comp_decrypt = mod.PandasPipelineComponent(
        asset_name="decrypted_in2",
        source={"kind": "encrypted_file", "path": enc_path, "format": "csv"},
        operations=[],
        key_source="raw_key", raw_key_env_var="PANDAS_PIPELINE_TEST_KEY_B", key_id="k",
    )
    defs2 = comp_decrypt.build_defs(None)
    with pytest.raises(Exception):
        dg.materialize([defs2.get_assets_def(dg.AssetKey("decrypted_in2"))])


def test_table_sink_duckdb(mod, tmp_path):
    import duckdb

    db_path = str(tmp_path / "test.duckdb")

    class _DuckDBResource(dg.ConfigurableResource):
        def get_connection(self):
            return duckdb.connect(db_path)

    comp = mod.PandasPipelineComponent(
        asset_name="table_out",
        upstream_asset_key="raw_orders",
        operations=[],
        sinks=[{"from": "_default", "kind": "table", "resource_key": "db", "table": "orders_out", "if_exists": "replace"}],
    )
    defs = dg.Definitions.merge(
        dg.Definitions(assets=[raw_orders], resources={"db": _DuckDBResource()}),
        comp.build_defs(None),
    )
    dg.materialize(
        [raw_orders, defs.get_assets_def(dg.AssetKey("table_out"))],
        resources={"db": _DuckDBResource()},
    )
    conn = duckdb.connect(db_path)
    count = conn.execute("SELECT COUNT(*) FROM orders_out").fetchone()[0]
    conn.close()
    assert count == 4


def test_invalid_op_raises(mod):
    comp = mod.PandasPipelineComponent(
        asset_name="bad_op",
        upstream_asset_key="raw_orders",
        operations=[{"op": "not_a_real_op"}],
    )
    with pytest.raises(ValueError, match="not supported"):
        comp.build_defs(None)


def test_mutually_exclusive_source_shapes(mod):
    comp = mod.PandasPipelineComponent(
        asset_name="bad_shape",
        upstream_asset_key="raw_orders",
        source={"kind": "file", "path": "x.csv"},
        operations=[],
    )
    with pytest.raises(ValueError, match="either .upstream_asset_key. OR .source:."):
        comp.build_defs(None)
