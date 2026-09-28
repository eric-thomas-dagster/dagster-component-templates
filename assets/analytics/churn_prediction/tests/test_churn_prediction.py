"""Committed regression tests for ChurnPredictionComponent's dual ingestion
(upstream_asset_key / source:warehouse_query) and new scoring_method split
(heuristic / ml).

`scoring_method='heuristic'` (default) is the ORIGINAL weighted
activity-decline scoring, completely unchanged -- these tests confirm that
default behavior still works exactly as before this change.

`scoring_method='ml'` is new: fits a real scikit-learn LogisticRegression
against a `target_column` the caller supplies. The heuristic has no access
to any historical "did this customer actually churn" label -- there is no
such column anywhere in its input schema -- so 'ml' mode is opt-in and
requires the caller to bring their own label, unlike every SQL-execution-mode
component built earlier in this session (those were already real classifiers;
this one wasn't).
"""
import dagster as dg
import numpy as np
import pandas as pd
import pytest

from .conftest import load_component_module, requires_duckdb


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def customer_df():
    rng = np.random.RandomState(0)
    n = 60
    return pd.DataFrame({
        "customer_id": [f"c{i}" for i in range(n)],
        "last_activity_date": pd.Timestamp.now() - pd.to_timedelta(rng.randint(0, 400, n), unit="D"),
        "total_orders": rng.randint(1, 50, n),
        "total_revenue": rng.uniform(10, 5000, n),
        "lifetime_days": rng.randint(30, 1000, n),
    })


@pytest.fixture()
def labeled_customer_df(customer_df):
    df = customer_df.copy()
    df["churned"] = (df["total_orders"] < df["total_orders"].median()).astype(int)
    return df


def test_heuristic_mode_default_unchanged(mod, customer_df):
    component = mod.ChurnPredictionComponent(asset_name="churn_out", upstream_asset_key="raw")
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]

    @dg.asset(name="raw")
    def raw():
        return customer_df

    result = dg.materialize([asset_def, raw])
    assert result.success
    df_out = result.output_for_node("churn_out")
    assert set(["customer_id", "churn_risk_score", "churn_risk_level", "recommended_action"]).issubset(df_out.columns)
    assert df_out["churn_risk_level"].isin(["Low", "Medium", "High", "Critical"]).all()


def test_ml_mode_upstream_asset_key(mod, labeled_customer_df):
    component = mod.ChurnPredictionComponent(
        asset_name="churn_ml_out", upstream_asset_key="raw",
        scoring_method="ml", target_column="churned",
        feature_columns=["total_orders", "total_revenue", "lifetime_days"],
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]

    @dg.asset(name="raw")
    def raw():
        return labeled_customer_df

    result = dg.materialize([asset_def, raw])
    assert result.success
    df_out = result.output_for_node("churn_ml_out")
    assert "predicted_class" in df_out.columns
    assert any(c.startswith("predicted_proba_") for c in df_out.columns)

    mats = result.asset_materializations_for_node("churn_ml_out")
    assert "accuracy" in mats[0].metadata


def test_ml_mode_requires_target_column(mod, customer_df):
    with pytest.raises(ValueError, match="requires `target_column`"):
        mod.ChurnPredictionComponent(
            asset_name="x", upstream_asset_key="raw",
            scoring_method="ml", feature_columns=["total_orders"],
        ).build_defs(context=None)


def test_ml_mode_requires_feature_columns(mod):
    with pytest.raises(ValueError, match="requires `feature_columns`"):
        mod.ChurnPredictionComponent(
            asset_name="x", upstream_asset_key="raw",
            scoring_method="ml", target_column="churned",
        ).build_defs(context=None)


def test_mutual_exclusivity_guard(mod):
    with pytest.raises(ValueError, match="set exactly one"):
        mod.ChurnPredictionComponent(asset_name="x").build_defs(context=None)


def test_sql_mode_requires_ml_scoring_method(mod):
    with pytest.raises(ValueError, match="requires scoring_method='ml'"):
        mod.ChurnPredictionComponent(
            asset_name="x",
            source={"kind": "warehouse_query", "resource_key": "r", "sql": "SELECT 1"},
            execution_mode="sql",
        ).build_defs(context=None)


def test_sql_mode_bigquery_generates_create_model_and_predict(mod):
    stmts = mod._build_sql_mode_statements(
        "bigquery", "SELECT * FROM t", "ds.out", "ds.model1",
        "churned", ["total_orders", "total_revenue"], 0.2, 1000,
    )
    assert len(stmts) == 2
    assert "CREATE OR REPLACE MODEL `ds.model1`" in stmts[0]
    assert "model_type='LOGISTIC_REG'" in stmts[0]
    assert "ML.PREDICT" in stmts[1]


@requires_duckdb
def test_heuristic_mode_source_warehouse_query_against_real_duckdb(mod, customer_df, tmp_path):
    import duckdb
    from dagster_duckdb import DuckDBResource

    db_path = str(tmp_path / "customers.duckdb")
    conn = duckdb.connect(db_path)
    conn.register("df_view", customer_df)
    conn.execute("CREATE TABLE customers AS SELECT * FROM df_view")
    conn.close()

    component = mod.ChurnPredictionComponent(
        asset_name="churn_sql_source",
        source={"kind": "warehouse_query", "resource_key": "duckdb_resource", "sql": "SELECT * FROM customers"},
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], resources={"duckdb_resource": DuckDBResource(database=db_path)})
    assert result.success
    df_out = result.output_for_node("churn_sql_source")
    assert "churn_risk_score" in df_out.columns
