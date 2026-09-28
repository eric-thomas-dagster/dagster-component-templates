"""Committed regression tests for AnomalyDetectionComponent's dual
ingestion (upstream_asset_key / source:warehouse_query) and dual
execution_mode (python / sql) backport.

execution_mode='sql' generates ONE query per detection_method using
portable SQL window functions (AVG/STDDEV/PERCENTILE_CONT-or-QUANTILE_CONT
OVER (...)) -- these are genuinely portable statistics, not vendor ML, so
every test here runs the generated SQL for real against DuckDB (one of
the 6 supported sql_dialect values) and asserts NUMERIC PARITY against
the existing python-mode result, not just that the SQL string looks
right.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module, requires_duckdb


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def txns_df():
    return pd.DataFrame({
        "id": range(1, 21),
        "amount": [10.0, 12.0, 11.0, 13.0, 9.0, 10.0, 12.0, 11.0, 500.0, 10.0,
                   11.0, 12.0, 9.0, 13.0, 10.0, 11.0, 12.0, -300.0, 10.0, 11.0],
    })


def _materialize(component, df, upstream_name="raw_txns", resources=None):
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    assets = [asset_def]
    if df is not None:
        @dg.asset(name=upstream_name)
        def _upstream():
            return df
        assets.append(_upstream)
    return dg.materialize(assets, resources=resources or {})


def test_python_mode_upstream_asset_key_z_score(mod, txns_df):
    component = mod.AnomalyDetectionComponent(
        asset_name="anomalies_out", upstream_asset_key="raw_txns",
        detection_method="z_score", metric_column="amount", threshold=2.0,
    )
    result = _materialize(component, txns_df)
    assert result.success
    df_out = result.output_for_node("anomalies_out")
    assert set(df_out[df_out["is_anomaly"]]["id"]) == {9, 18}


@requires_duckdb
def test_python_mode_source_warehouse_query_against_real_duckdb(mod, tmp_path):
    import duckdb
    from dagster_duckdb import DuckDBResource

    db_path = str(tmp_path / "txns.duckdb")
    conn = duckdb.connect(db_path)
    conn.execute(
        "CREATE TABLE txns AS SELECT * FROM (VALUES "
        + ", ".join(f"({i}, {v})" for i, v in enumerate(
            [10.0, 12.0, 11.0, 13.0, 9.0, 10.0, 12.0, 11.0, 500.0, 10.0,
             11.0, 12.0, 9.0, 13.0, 10.0, 11.0, 12.0, -300.0, 10.0, 11.0], start=1))
        + ") AS t(id, amount)"
    )
    conn.close()

    component = mod.AnomalyDetectionComponent(
        asset_name="anomalies_sql_source",
        source={"kind": "warehouse_query", "resource_key": "duckdb_resource", "sql": "SELECT * FROM txns"},
        detection_method="z_score", metric_column="amount", threshold=2.0,
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], resources={"duckdb_resource": DuckDBResource(database=db_path)})
    assert result.success
    df_out = result.output_for_node("anomalies_sql_source")
    assert set(df_out[df_out["is_anomaly"]]["id"]) == {9, 18}


@requires_duckdb
@pytest.mark.parametrize("method,threshold,expected_ids", [
    ("z_score", 2.0, {9, 18}),
    ("iqr", 1.5, {9, 18}),
    ("threshold", 100.0, {9}),
])
def test_execution_mode_sql_matches_python_mode_numerically(mod, txns_df, tmp_path, method, threshold, expected_ids):
    import duckdb

    db_path = str(tmp_path / "txns.duckdb")
    conn = duckdb.connect(db_path)
    conn.execute(
        "CREATE TABLE txns AS SELECT * FROM (VALUES "
        + ", ".join(f"({row.id}, {row.amount})" for row in txns_df.itertuples())
        + ") AS t(id, amount)"
    )

    # Python-mode reference result.
    py_component = mod.AnomalyDetectionComponent(
        asset_name="py_out", upstream_asset_key="raw_txns",
        detection_method=method, metric_column="amount", threshold=threshold,
    )
    py_result = _materialize(py_component, txns_df)
    py_df = py_result.output_for_node("py_out")
    py_scores = dict(zip(py_df["id"], py_df["anomaly_score"]))

    # SQL-mode: generate and execute the same computation server-side.
    sql = mod._build_sql_mode_query(
        "duckdb", "SELECT * FROM txns", "anomalies_sql", "amount", method, threshold, None, None, 7,
    )
    conn.execute(sql)
    sql_rows = conn.execute("SELECT id, amount, anomaly_score, is_anomaly FROM anomalies_sql ORDER BY id").fetchall()
    conn.close()

    sql_anomaly_ids = {r[0] for r in sql_rows if r[3]}
    assert sql_anomaly_ids == expected_ids == set(py_df[py_df["is_anomaly"]]["id"])

    for row_id, amount, score, is_anomaly in sql_rows:
        assert score == pytest.approx(py_scores[row_id], abs=0.01), (
            f"row {row_id}: sql score {score} != python score {py_scores[row_id]}"
        )


@requires_duckdb
def test_execution_mode_sql_moving_average_matches_python_mode(mod, tmp_path):
    import duckdb

    ts_df = pd.DataFrame({
        "id": range(1, 10),
        "ts": pd.date_range("2024-01-01", periods=9),
        "amount": [10.0, 11.0, 10.5, 12.0, 11.0, 10.0, 200.0, 11.0, 10.5],
    })
    py_component = mod.AnomalyDetectionComponent(
        asset_name="py_ma_out", upstream_asset_key="raw_ts",
        detection_method="moving_average", metric_column="amount",
        timestamp_field="ts", moving_average_window=3, threshold=1.0,
    )
    py_result = _materialize(py_component, ts_df, upstream_name="raw_ts")
    py_df = py_result.output_for_node("py_ma_out")

    db_path = str(tmp_path / "ts.duckdb")
    conn = duckdb.connect(db_path)
    conn.execute(
        "CREATE TABLE ts_txns AS SELECT * FROM (VALUES "
        + ", ".join(f"({row.id}, DATE '{row.ts.date()}', {row.amount})" for row in ts_df.itertuples())
        + ") AS t(id, ts, amount)"
    )
    sql = mod._build_sql_mode_query(
        "duckdb", "SELECT * FROM ts_txns", "anomalies_ma", "amount", "moving_average", 1.0, None, "ts", 3,
    )
    conn.execute(sql)
    sql_rows = conn.execute("SELECT id, is_anomaly FROM anomalies_ma ORDER BY id").fetchall()
    conn.close()

    assert {r[0] for r in sql_rows if r[1]} == set(py_df[py_df["is_anomaly"]]["id"])


def test_mutual_exclusivity_guard(mod):
    with pytest.raises(ValueError, match="set exactly one"):
        mod.AnomalyDetectionComponent(asset_name="x", detection_method="z_score").build_defs(context=None)


def test_sql_mode_requires_dialect_table_metric_column(mod):
    with pytest.raises(ValueError, match="sql_dialect"):
        mod.AnomalyDetectionComponent(
            asset_name="x",
            source={"kind": "warehouse_query", "resource_key": "r", "sql": "SELECT 1"},
            execution_mode="sql", detection_method="z_score", metric_column="amount",
        ).build_defs(context=None)


def test_sql_mode_unsupported_dialect_raises(mod):
    with pytest.raises(ValueError, match="unsupported sql_dialect"):
        mod._build_sql_mode_query("mysql", "SELECT 1", "t", "amount", "z_score", 2.0, None, None, 7)


def test_sql_mode_moving_average_requires_timestamp(mod):
    with pytest.raises(ValueError, match="timestamp_field"):
        mod._build_sql_mode_query("duckdb", "SELECT 1", "t", "amount", "moving_average", 2.0, None, None, 7)
