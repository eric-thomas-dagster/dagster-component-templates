"""Committed regression tests for KMeansClusteringComponent's dual
ingestion (upstream_asset_key / source:warehouse_query) and dual
execution_mode (python / sql) backport.

BigQuery-ONLY sql_dialect: Snowflake ML has no clustering function at
all (confirmed in the broader audit), and Databricks has no natural
predict-against-a-served-endpoint story for clustering (there's no
stable label space to serve predictions against, unlike a classifier/
regressor endpoint) -- neither is offered here.
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
    component = mod.KMeansClusteringComponent(
        asset_name="km_out", upstream_asset_key="raw",
        feature_columns=feature_cols, n_clusters=3,
    )
    defs = component.build_defs(load_context=None)
    asset_def = list(defs.assets)[0]

    @dg.asset(name="raw")
    def raw():
        return iris_df

    result = dg.materialize([asset_def, raw])
    assert result.success
    df_out = result.output_for_node("km_out")
    assert "cluster" in df_out.columns
    assert df_out["cluster"].nunique() == 3


@requires_duckdb
def test_python_mode_source_warehouse_query_against_real_duckdb(mod, iris_df, feature_cols, tmp_path):
    import duckdb
    from dagster_duckdb import DuckDBResource

    db_path = str(tmp_path / "iris.duckdb")
    conn = duckdb.connect(db_path)
    conn.register("df_view", iris_df)
    conn.execute("CREATE TABLE iris AS SELECT * FROM df_view")
    conn.close()

    component = mod.KMeansClusteringComponent(
        asset_name="km_sql_source",
        source={"kind": "warehouse_query", "resource_key": "duckdb_resource", "sql": "SELECT * FROM iris"},
        feature_columns=feature_cols, n_clusters=3,
    )
    defs = component.build_defs(load_context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], resources={"duckdb_resource": DuckDBResource(database=db_path)})
    assert result.success
    df_out = result.output_for_node("km_sql_source")
    assert "cluster" in df_out.columns


def test_sql_mode_bigquery_generates_create_model_and_predict(mod, feature_cols):
    stmts = mod._build_sql_mode_statements(
        "bigquery", "SELECT * FROM t", "ds.out", "ds.model1", feature_cols, 3, True, "cluster",
    )
    assert len(stmts) == 2
    assert "CREATE OR REPLACE MODEL `ds.model1`" in stmts[0]
    assert "model_type='KMEANS'" in stmts[0]
    assert "num_clusters=3" in stmts[0]
    assert "standardize_features=true" in stmts[0]
    assert "ML.PREDICT(MODEL `ds.model1`" in stmts[1]
    assert "centroid_id AS cluster" in stmts[1]


def test_sql_mode_snowflake_not_offered(mod, feature_cols):
    with pytest.raises(ValueError, match="unsupported sql_dialect"):
        mod._build_sql_mode_statements("snowflake", "SELECT * FROM t", "ds.out", "m", feature_cols, 3, True, "cluster")


def test_sql_mode_databricks_not_offered(mod, feature_cols):
    with pytest.raises(ValueError, match="unsupported sql_dialect"):
        mod._build_sql_mode_statements("databricks", "SELECT * FROM t", "ds.out", "m", feature_cols, 3, True, "cluster")


def test_mutual_exclusivity_guard(mod, feature_cols):
    with pytest.raises(ValueError, match="set exactly one"):
        mod.KMeansClusteringComponent(
            asset_name="x", feature_columns=feature_cols,
        ).build_defs(load_context=None)


def test_sql_mode_requires_dialect_table_and_model_name(mod, feature_cols):
    with pytest.raises(ValueError, match="sql_dialect"):
        mod.KMeansClusteringComponent(
            asset_name="x",
            source={"kind": "warehouse_query", "resource_key": "r", "sql": "SELECT 1"},
            execution_mode="sql", feature_columns=feature_cols,
        ).build_defs(load_context=None)
