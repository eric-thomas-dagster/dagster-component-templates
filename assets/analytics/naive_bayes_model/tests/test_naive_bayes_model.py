"""Committed regression tests for NaiveBayesModelComponent's dual ingestion
(upstream_asset_key / source:warehouse_query) and dual execution_mode
(python / sql) backport.

Databricks-ONLY sql_dialect, predict-only, unlike every other component in
this family: BigQuery ML has no NAIVE_BAYES model type at all, and
Snowflake's SNOWFLAKE.ML.CLASSIFICATION is AutoML that never guarantees
Naive Bayes specifically -- neither is a genuine per-algorithm match, the
same reasoning decision_tree_model was skipped for entirely from this
whole initiative.

Also removes three fields that never did anything (n_estimators, max_depth,
n_jobs -- copy-paste leftovers from a random_forest_model sibling; GaussianNB
takes none of them) and replaces the always-broken output_mode='feature_importance'
(GaussianNB has no feature_importances_) with real output_predictions/
output_probabilities flags (GaussianNB.predict_proba is a real, always-available
method, unlike feature importances).
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module, requires_duckdb


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def iris_df():
    from sklearn.datasets import load_iris
    data = load_iris(as_frame=True)
    df = data.data.copy()
    df.columns = [c.replace(" ", "_").replace("(cm)", "").strip("_") for c in df.columns]
    df["species"] = data.target
    return df


@pytest.fixture()
def feature_cols(iris_df):
    return [c for c in iris_df.columns if c != "species"]


def test_python_mode_upstream_asset_key(mod, iris_df, feature_cols):
    component = mod.NaiveBayesModelComponent(
        asset_name="nb_out", upstream_asset_key="raw",
        target_column="species", feature_columns=feature_cols,
    )
    defs = component.build_defs(load_context=None)
    asset_def = list(defs.assets)[0]

    @dg.asset(name="raw")
    def raw():
        return iris_df

    result = dg.materialize([asset_def, raw])
    assert result.success
    df_out = result.output_for_node("nb_out")
    assert "predicted" in df_out.columns
    assert any(c.startswith("predicted_proba_") for c in df_out.columns)


def test_model_path_persists_and_metadata_logged(mod, iris_df, feature_cols, tmp_path):
    model_path = str(tmp_path / "nb_model.joblib")
    component = mod.NaiveBayesModelComponent(
        asset_name="nb_out", upstream_asset_key="raw",
        target_column="species", feature_columns=feature_cols, model_path=model_path,
    )
    defs = component.build_defs(load_context=None)
    asset_def = list(defs.assets)[0]

    @dg.asset(name="raw")
    def raw():
        return iris_df

    result = dg.materialize([asset_def, raw])
    assert result.success

    import joblib
    loaded = joblib.load(model_path)
    preds = loaded.predict(iris_df[feature_cols].fillna(0).values)
    assert len(preds) == len(iris_df)

    mats = result.asset_materializations_for_node("nb_out")
    metadata = mats[0].metadata
    assert "accuracy" in metadata
    assert "dagster/row_count" in metadata


def test_output_predictions_and_probabilities_can_be_disabled(mod, iris_df, feature_cols):
    component = mod.NaiveBayesModelComponent(
        asset_name="nb_out", upstream_asset_key="raw",
        target_column="species", feature_columns=feature_cols,
        output_predictions=False, output_probabilities=False,
    )
    defs = component.build_defs(load_context=None)
    asset_def = list(defs.assets)[0]

    @dg.asset(name="raw")
    def raw():
        return iris_df

    result = dg.materialize([asset_def, raw])
    assert result.success
    df_out = result.output_for_node("nb_out")
    assert "predicted" not in df_out.columns
    assert not any(c.startswith("predicted_proba_") for c in df_out.columns)


def test_regression_task_type_raises(mod, feature_cols):
    with pytest.raises(ValueError, match="only supports classification"):
        mod.NaiveBayesModelComponent(
            asset_name="x", upstream_asset_key="raw",
            target_column="species", feature_columns=feature_cols,
            task_type="regression",
        ).build_defs(load_context=None)


@requires_duckdb
def test_python_mode_source_warehouse_query_against_real_duckdb(mod, iris_df, feature_cols, tmp_path):
    import duckdb
    from dagster_duckdb import DuckDBResource

    db_path = str(tmp_path / "iris.duckdb")
    conn = duckdb.connect(db_path)
    conn.register("df_view", iris_df)
    conn.execute("CREATE TABLE iris AS SELECT * FROM df_view")
    conn.close()

    component = mod.NaiveBayesModelComponent(
        asset_name="nb_sql_source",
        source={"kind": "warehouse_query", "resource_key": "duckdb_resource", "sql": "SELECT * FROM iris"},
        target_column="species", feature_columns=feature_cols,
    )
    defs = component.build_defs(load_context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], resources={"duckdb_resource": DuckDBResource(database=db_path)})
    assert result.success
    df_out = result.output_for_node("nb_sql_source")
    assert "predicted" in df_out.columns


def test_sql_mode_databricks_generates_ai_query_predict_only(mod, feature_cols):
    stmts = mod._build_sql_mode_statements(
        "databricks", "SELECT * FROM t", "ds.out", "my_endpoint", feature_cols,
    )
    assert len(stmts) == 1
    assert "ai_query('my_endpoint'" in stmts[0]
    assert "CREATE OR REPLACE TABLE ds.out" in stmts[0]
    assert "CREATE MODEL" not in stmts[0]


def test_sql_mode_bigquery_not_offered(mod, feature_cols):
    with pytest.raises(ValueError, match="unsupported sql_dialect"):
        mod._build_sql_mode_statements("bigquery", "SELECT * FROM t", "ds.out", "m", feature_cols)


def test_sql_mode_snowflake_not_offered(mod, feature_cols):
    with pytest.raises(ValueError, match="unsupported sql_dialect"):
        mod._build_sql_mode_statements("snowflake", "SELECT * FROM t", "ds.out", "m", feature_cols)


def test_mutual_exclusivity_guard(mod, feature_cols):
    with pytest.raises(ValueError, match="set exactly one"):
        mod.NaiveBayesModelComponent(
            asset_name="x", target_column="species", feature_columns=feature_cols,
        ).build_defs(load_context=None)


def test_sql_mode_requires_dialect_table_and_model_name(mod, feature_cols):
    with pytest.raises(ValueError, match="sql_dialect"):
        mod.NaiveBayesModelComponent(
            asset_name="x",
            source={"kind": "warehouse_query", "resource_key": "r", "sql": "SELECT 1"},
            execution_mode="sql", target_column="species", feature_columns=feature_cols,
        ).build_defs(load_context=None)
