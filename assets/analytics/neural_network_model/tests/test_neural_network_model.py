"""Committed regression tests for NeuralNetworkModelComponent's dual
ingestion (upstream_asset_key / source:warehouse_query) and dual
execution_mode (python / sql) backport.

Same 3-dialect asymmetry as logistic_regression_model/gradient_boosting_model.
BigQuery's DNN_CLASSIFIER/DNN_REGRESSOR is the cleanest possible match in
this whole audit -- BigQuery's DNN model IS a genuine neural network, not
an approximation, and supports both task types. Snowflake ML.CLASSIFICATION
is classification-only (and not literally a neural net internally), so
task_type='regression' + sql_dialect='snowflake' must raise.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module, requires_duckdb


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def breast_cancer_df():
    from sklearn.datasets import load_breast_cancer
    data = load_breast_cancer(as_frame=True)
    df = data.data.copy()
    df["target"] = data.target
    df.columns = [c.replace(" ", "_") for c in df.columns]
    return df


@pytest.fixture()
def feature_cols(breast_cancer_df):
    return [c for c in breast_cancer_df.columns if c != "target"][:5]


def test_python_mode_upstream_asset_key(mod, breast_cancer_df, feature_cols):
    component = mod.NeuralNetworkModelComponent(
        asset_name="nn_out", upstream_asset_key="raw",
        target_column="target", feature_columns=feature_cols, task_type="classification",
    )
    defs = component.build_defs(load_context=None)
    asset_def = list(defs.assets)[0]

    @dg.asset(name="raw")
    def raw():
        return breast_cancer_df

    result = dg.materialize([asset_def, raw])
    assert result.success
    df_out = result.output_for_node("nn_out")
    assert "predicted" in df_out.columns

    mats = result.asset_materializations_for_node("nn_out")
    assert "accuracy" in mats[0].metadata


@requires_duckdb
def test_python_mode_source_warehouse_query_against_real_duckdb(mod, breast_cancer_df, feature_cols, tmp_path):
    import duckdb
    from dagster_duckdb import DuckDBResource

    db_path = str(tmp_path / "cancer.duckdb")
    conn = duckdb.connect(db_path)
    conn.register("df_view", breast_cancer_df[feature_cols + ["target"]])
    conn.execute("CREATE TABLE cancer AS SELECT * FROM df_view")
    conn.close()

    component = mod.NeuralNetworkModelComponent(
        asset_name="nn_sql_source",
        source={"kind": "warehouse_query", "resource_key": "duckdb_resource", "sql": "SELECT * FROM cancer"},
        target_column="target", feature_columns=feature_cols, task_type="classification",
    )
    defs = component.build_defs(load_context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], resources={"duckdb_resource": DuckDBResource(database=db_path)})
    assert result.success
    df_out = result.output_for_node("nn_sql_source")
    assert "predicted" in df_out.columns


@pytest.mark.parametrize("task_type,expected_model_type", [
    ("classification", "DNN_CLASSIFIER"),
    ("regression", "DNN_REGRESSOR"),
])
def test_sql_mode_bigquery_dispatches_model_type_by_task(mod, feature_cols, task_type, expected_model_type):
    stmts = mod._build_sql_mode_statements(
        "bigquery", "SELECT * FROM t", "ds.out", "ds.model1", "target", feature_cols, task_type, 0.2, 32,
    )
    assert len(stmts) == 2
    assert expected_model_type in stmts[0]
    assert "hidden_units=[32]" in stmts[0]
    assert "ML.PREDICT(MODEL `ds.model1`" in stmts[1]


def test_sql_mode_snowflake_classification_generates_view_model_and_predict(mod, feature_cols):
    stmts = mod._build_sql_mode_statements(
        "snowflake", "SELECT * FROM t", "ds.out", "model1", "target", feature_cols, "classification", 0.2, 32,
    )
    assert len(stmts) == 3
    assert "CREATE OR REPLACE SNOWFLAKE.ML.CLASSIFICATION model1" in stmts[1]


def test_sql_mode_snowflake_regression_raises_clear_error(mod, feature_cols):
    with pytest.raises(ValueError, match="no regression function"):
        mod._build_sql_mode_statements(
            "snowflake", "SELECT * FROM t", "ds.out", "model1", "target", feature_cols, "regression", 0.2, 32,
        )


def test_sql_mode_databricks_is_predict_only_via_ai_query(mod, feature_cols):
    stmts = mod._build_sql_mode_statements(
        "databricks", "SELECT * FROM t", "ds.out", "my_endpoint", "target", feature_cols, "classification", 0.2, 32,
    )
    assert len(stmts) == 1
    assert "ai_query('my_endpoint'" in stmts[0]


def test_sql_mode_unsupported_dialect_raises(mod, feature_cols):
    with pytest.raises(ValueError, match="unsupported sql_dialect"):
        mod._build_sql_mode_statements("mysql", "SELECT 1", "t", "m", "target", feature_cols, "classification", 0.2, 32)


def test_mutual_exclusivity_guard(mod, feature_cols):
    with pytest.raises(ValueError, match="set exactly one"):
        mod.NeuralNetworkModelComponent(
            asset_name="x", target_column="target", feature_columns=feature_cols,
        ).build_defs(load_context=None)


def test_sql_mode_requires_dialect_and_rejects_feature_importance(mod, feature_cols):
    with pytest.raises(ValueError, match="sql_dialect"):
        mod.NeuralNetworkModelComponent(
            asset_name="x",
            source={"kind": "warehouse_query", "resource_key": "r", "sql": "SELECT 1"},
            execution_mode="sql", target_column="target", feature_columns=feature_cols,
        ).build_defs(load_context=None)

    with pytest.raises(ValueError, match="output_mode='predictions'"):
        mod.NeuralNetworkModelComponent(
            asset_name="x",
            source={"kind": "warehouse_query", "resource_key": "r", "sql": "SELECT 1"},
            execution_mode="sql", sql_dialect="bigquery", output_table="t", model_name="m",
            target_column="target", feature_columns=feature_cols, output_mode="feature_importance",
        ).build_defs(load_context=None)
