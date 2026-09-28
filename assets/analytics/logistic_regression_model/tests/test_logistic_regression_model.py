"""Committed regression tests for LogisticRegressionModelComponent's dual
ingestion (upstream_asset_key / source:warehouse_query) and dual
execution_mode (python / sql) backport.

Unlike anomaly_detection's portable-statistics SQL mode, fitting a
classifier genuinely needs a warehouse-native ML surface, and the three
supported dialects differ in a way that's tested explicitly here rather
than papered over:

- bigquery / snowflake: real train-and-predict-in-SQL (`CREATE MODEL`/
  `CREATE SNOWFLAKE.ML.CLASSIFICATION`) -- structural only, no live
  warehouse credentials in this environment.
- databricks: predict-ONLY against an already-served model endpoint via
  `ai_query()` -- also structural only.

The python-mode path (real scikit-learn) IS live-tested for real, same
as before this backport.
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
    component = mod.LogisticRegressionModelComponent(
        asset_name="lr_out", upstream_asset_key="raw",
        target_column="target", feature_columns=feature_cols,
    )
    defs = component.build_defs(load_context=None)
    asset_def = list(defs.assets)[0]

    @dg.asset(name="raw")
    def raw():
        return breast_cancer_df

    result = dg.materialize([asset_def, raw])
    assert result.success
    df_out = result.output_for_node("lr_out")
    assert "predicted_class" in df_out.columns
    assert any(c.startswith("predicted_proba_") for c in df_out.columns)

    mats = result.asset_materializations_for_node("lr_out")
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

    component = mod.LogisticRegressionModelComponent(
        asset_name="lr_sql_source",
        source={"kind": "warehouse_query", "resource_key": "duckdb_resource", "sql": "SELECT * FROM cancer"},
        target_column="target", feature_columns=feature_cols,
    )
    defs = component.build_defs(load_context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], resources={"duckdb_resource": DuckDBResource(database=db_path)})
    assert result.success
    df_out = result.output_for_node("lr_sql_source")
    assert "predicted_class" in df_out.columns


def test_sql_mode_bigquery_generates_create_model_and_predict(mod, feature_cols):
    stmts = mod._build_sql_mode_statements(
        "bigquery", "SELECT * FROM t", "ds.out", "ds.model1", "target", feature_cols, 0.2, 1000,
    )
    assert len(stmts) == 2
    assert "CREATE OR REPLACE MODEL `ds.model1`" in stmts[0]
    assert "model_type='LOGISTIC_REG'" in stmts[0]
    assert "input_label_cols=['target']" in stmts[0]
    assert "ML.PREDICT(MODEL `ds.model1`" in stmts[1]


def test_sql_mode_snowflake_generates_view_model_and_predict(mod, feature_cols):
    stmts = mod._build_sql_mode_statements(
        "snowflake", "SELECT * FROM t", "ds.out", "model1", "target", feature_cols, 0.2, 1000,
    )
    assert len(stmts) == 3
    assert "CREATE OR REPLACE VIEW ds.out_training_view" in stmts[0]
    assert "CREATE OR REPLACE SNOWFLAKE.ML.CLASSIFICATION model1" in stmts[1]
    assert "SYSTEM$REFERENCE('VIEW', 'ds.out_training_view')" in stmts[1]
    assert "TARGET_COLNAME => 'target'" in stmts[1]
    assert "model1!PREDICT(INPUT_DATA => {*})" in stmts[2]


def test_sql_mode_databricks_is_predict_only_via_ai_query(mod, feature_cols):
    stmts = mod._build_sql_mode_statements(
        "databricks", "SELECT * FROM t", "ds.out", "my_endpoint", "target", feature_cols, 0.2, 1000,
    )
    assert len(stmts) == 1
    assert "ai_query('my_endpoint'" in stmts[0]
    assert "named_struct(" in stmts[0]
    # No CREATE MODEL anywhere -- Databricks genuinely can't train via SQL.
    assert "CREATE MODEL" not in stmts[0] and "CREATE OR REPLACE MODEL" not in stmts[0]


def test_sql_mode_unsupported_dialect_raises(mod, feature_cols):
    with pytest.raises(ValueError, match="unsupported sql_dialect"):
        mod._build_sql_mode_statements("mysql", "SELECT 1", "t", "m", "target", feature_cols, 0.2, 1000)


def test_mutual_exclusivity_guard(mod, feature_cols):
    with pytest.raises(ValueError, match="set exactly one"):
        mod.LogisticRegressionModelComponent(
            asset_name="x", target_column="target", feature_columns=feature_cols,
        ).build_defs(load_context=None)


def test_sql_mode_requires_dialect_table_model_name(mod, feature_cols):
    with pytest.raises(ValueError, match="sql_dialect"):
        mod.LogisticRegressionModelComponent(
            asset_name="x",
            source={"kind": "warehouse_query", "resource_key": "r", "sql": "SELECT 1"},
            execution_mode="sql", target_column="target", feature_columns=feature_cols,
        ).build_defs(load_context=None)
