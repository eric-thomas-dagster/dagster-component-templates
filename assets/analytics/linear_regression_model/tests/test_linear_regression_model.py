"""Committed regression tests for LinearRegressionModelComponent's dual
ingestion (upstream_asset_key / source:warehouse_query) and dual
execution_mode (python / sql) backport.

Only 2 dialects here, not 3 -- unlike the classifier family, Snowflake ML
has no generic regression function at all (confirmed in the broader
audit: SNOWFLAKE.ML.FORECAST is time-series-specific, not a general linear
fit), so sql_dialect='snowflake' is not offered and must raise clearly if
requested.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module, requires_duckdb


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def diabetes_df():
    from sklearn.datasets import load_diabetes
    data = load_diabetes(as_frame=True)
    df = data.data.copy()
    df["target"] = data.target
    return df


@pytest.fixture()
def feature_cols(diabetes_df):
    return [c for c in diabetes_df.columns if c != "target"][:5]


def test_python_mode_upstream_asset_key(mod, diabetes_df, feature_cols):
    component = mod.LinearRegressionModelComponent(
        asset_name="lr_out", upstream_asset_key="raw",
        target_column="target", feature_columns=feature_cols,
    )
    defs = component.build_defs(load_context=None)
    asset_def = list(defs.assets)[0]

    @dg.asset(name="raw")
    def raw():
        return diabetes_df

    result = dg.materialize([asset_def, raw])
    assert result.success
    df_out = result.output_for_node("lr_out")
    assert "predicted" in df_out.columns


@requires_duckdb
def test_python_mode_source_warehouse_query_against_real_duckdb(mod, diabetes_df, feature_cols, tmp_path):
    import duckdb
    from dagster_duckdb import DuckDBResource

    db_path = str(tmp_path / "diabetes.duckdb")
    conn = duckdb.connect(db_path)
    conn.register("df_view", diabetes_df[feature_cols + ["target"]])
    conn.execute("CREATE TABLE diabetes AS SELECT * FROM df_view")
    conn.close()

    component = mod.LinearRegressionModelComponent(
        asset_name="lr_sql_source",
        source={"kind": "warehouse_query", "resource_key": "duckdb_resource", "sql": "SELECT * FROM diabetes"},
        target_column="target", feature_columns=feature_cols,
    )
    defs = component.build_defs(load_context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], resources={"duckdb_resource": DuckDBResource(database=db_path)})
    assert result.success
    df_out = result.output_for_node("lr_sql_source")
    assert "predicted" in df_out.columns


def test_sql_mode_bigquery_generates_create_model_and_predict(mod, feature_cols):
    stmts = mod._build_sql_mode_statements(
        "bigquery", "SELECT * FROM t", "ds.out", "ds.model1", "target", feature_cols, 0.2,
    )
    assert len(stmts) == 2
    assert "CREATE OR REPLACE MODEL `ds.model1`" in stmts[0]
    assert "model_type='LINEAR_REG'" in stmts[0]
    assert "ML.PREDICT(MODEL `ds.model1`" in stmts[1]


def test_sql_mode_databricks_is_predict_only_via_ai_query(mod, feature_cols):
    stmts = mod._build_sql_mode_statements(
        "databricks", "SELECT * FROM t", "ds.out", "my_endpoint", "target", feature_cols, 0.2,
    )
    assert len(stmts) == 1
    assert "ai_query('my_endpoint'" in stmts[0]


def test_sql_mode_snowflake_not_offered(mod, feature_cols):
    with pytest.raises(ValueError, match="no generic regression function"):
        mod._build_sql_mode_statements(
            "snowflake", "SELECT * FROM t", "ds.out", "m", "target", feature_cols, 0.2,
        )


def test_sql_mode_unsupported_dialect_raises(mod, feature_cols):
    with pytest.raises(ValueError, match="unsupported sql_dialect"):
        mod._build_sql_mode_statements("mysql", "SELECT 1", "t", "m", "target", feature_cols, 0.2)


def test_mutual_exclusivity_guard(mod, feature_cols):
    with pytest.raises(ValueError, match="set exactly one"):
        mod.LinearRegressionModelComponent(
            asset_name="x", target_column="target", feature_columns=feature_cols,
        ).build_defs(load_context=None)


def test_sql_mode_requires_dialect_and_rejects_coefficients(mod, feature_cols):
    with pytest.raises(ValueError, match="sql_dialect"):
        mod.LinearRegressionModelComponent(
            asset_name="x",
            source={"kind": "warehouse_query", "resource_key": "r", "sql": "SELECT 1"},
            execution_mode="sql", target_column="target", feature_columns=feature_cols,
        ).build_defs(load_context=None)

    with pytest.raises(ValueError, match="output_mode='predictions' or 'both'"):
        mod.LinearRegressionModelComponent(
            asset_name="x",
            source={"kind": "warehouse_query", "resource_key": "r", "sql": "SELECT 1"},
            execution_mode="sql", sql_dialect="bigquery", output_table="t", model_name="m",
            target_column="target", feature_columns=feature_cols, output_mode="coefficients",
        ).build_defs(load_context=None)
