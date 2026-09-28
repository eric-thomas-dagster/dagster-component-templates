"""Committed regression tests for PcaComponent's dual ingestion
(upstream_asset_key / source:warehouse_query) and dual execution_mode
(python / sql) backport.

BigQuery-ONLY sql_dialect, same reasoning as k_means_clustering:
Snowflake ML has no PCA function, and Databricks has no natural
predict-against-a-served-endpoint story for PCA either.

Also fixes a real, pre-existing bug found while adding this: `model_path`
is a declared field, referenced in the asset body (`if model_path is not
None: ... joblib.dump(...)`), but was never extracted from `self` in
build_defs -- python mode raised a bare NameError on every single
materialize, even before this session's changes. test_model_path_actually_
persists_the_fitted_model below is the regression test for that fix.
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
    return df


@pytest.fixture()
def feature_cols(iris_df):
    return list(iris_df.columns)


def test_python_mode_upstream_asset_key(mod, iris_df, feature_cols):
    component = mod.PcaComponent(
        asset_name="pca_out", upstream_asset_key="raw",
        feature_columns=feature_cols, n_components=2,
    )
    defs = component.build_defs(load_context=None)
    asset_def = list(defs.assets)[0]

    @dg.asset(name="raw")
    def raw():
        return iris_df

    result = dg.materialize([asset_def, raw])
    assert result.success
    df_out = result.output_for_node("pca_out")
    assert "pc_1" in df_out.columns and "pc_2" in df_out.columns


def test_model_path_actually_persists_the_fitted_model(mod, iris_df, feature_cols, tmp_path):
    """Regression test for a real pre-existing bug: model_path was never
    extracted in build_defs (NameError on every materialize before this fix)."""
    model_path = str(tmp_path / "pca_model.joblib")
    component = mod.PcaComponent(
        asset_name="pca_out", upstream_asset_key="raw",
        feature_columns=feature_cols, n_components=2, model_path=model_path,
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
    transformed = loaded.transform(iris_df[feature_cols].fillna(0).values)
    assert transformed.shape[1] == 2


@requires_duckdb
def test_python_mode_source_warehouse_query_against_real_duckdb(mod, iris_df, feature_cols, tmp_path):
    import duckdb
    from dagster_duckdb import DuckDBResource

    db_path = str(tmp_path / "iris.duckdb")
    conn = duckdb.connect(db_path)
    conn.register("df_view", iris_df)
    conn.execute("CREATE TABLE iris AS SELECT * FROM df_view")
    conn.close()

    component = mod.PcaComponent(
        asset_name="pca_sql_source",
        source={"kind": "warehouse_query", "resource_key": "duckdb_resource", "sql": "SELECT * FROM iris"},
        feature_columns=feature_cols, n_components=2,
    )
    defs = component.build_defs(load_context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], resources={"duckdb_resource": DuckDBResource(database=db_path)})
    assert result.success
    df_out = result.output_for_node("pca_sql_source")
    assert "pc_1" in df_out.columns


def test_sql_mode_bigquery_generates_create_model_and_predict(mod, feature_cols):
    stmts = mod._build_sql_mode_statements(
        "bigquery", "SELECT * FROM t", "ds.out", "ds.model1", feature_cols, 2, "pc_",
    )
    assert len(stmts) == 2
    assert "CREATE OR REPLACE MODEL `ds.model1`" in stmts[0]
    assert "model_type='PCA'" in stmts[0]
    assert "num_principal_components=2" in stmts[0]
    assert "principal_component_1 AS pc_1" in stmts[1]
    assert "principal_component_2 AS pc_2" in stmts[1]


def test_sql_mode_snowflake_not_offered(mod, feature_cols):
    with pytest.raises(ValueError, match="unsupported sql_dialect"):
        mod._build_sql_mode_statements("snowflake", "SELECT * FROM t", "ds.out", "m", feature_cols, 2, "pc_")


def test_sql_mode_databricks_not_offered(mod, feature_cols):
    with pytest.raises(ValueError, match="unsupported sql_dialect"):
        mod._build_sql_mode_statements("databricks", "SELECT * FROM t", "ds.out", "m", feature_cols, 2, "pc_")


def test_mutual_exclusivity_guard(mod, feature_cols):
    with pytest.raises(ValueError, match="set exactly one"):
        mod.PcaComponent(
            asset_name="x", feature_columns=feature_cols,
        ).build_defs(load_context=None)


def test_sql_mode_requires_dialect_table_and_model_name(mod, feature_cols):
    with pytest.raises(ValueError, match="sql_dialect"):
        mod.PcaComponent(
            asset_name="x",
            source={"kind": "warehouse_query", "resource_key": "r", "sql": "SELECT 1"},
            execution_mode="sql", feature_columns=feature_cols,
        ).build_defs(load_context=None)
