"""Committed regression tests for CustomerHealthScoreComponent's new
multi-input dual ingestion (each of customer_data/subscription_data/
product_usage/support_ticket independently supports *_asset_key OR *_source)
and new scoring_method split (heuristic / ml).

Also the regression test for a real, pre-existing bug found while adding
this: `_normalize_score` used a scalar `if pd.isna(value): ...` guard, but
every one of its 5 call sites passes a full pandas Series -- this crashed
with `ValueError: The truth value of a Series is ambiguous` on any real
multi-row input (same bug, same original template, as lead_scoring's
`_normalize_score`). This component had no committed tests at all before
this change, so the bug had never been caught.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module, requires_duckdb


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def customer_data_df():
    return pd.DataFrame({
        "customer_id": ["c1", "c2", "c3"],
        "last_login_days": [1, 30, 90],
        "login_frequency": [20, 5, 1],
        "feature_adoption_rate": [0.8, 0.4, 0.1],
    })


@pytest.fixture()
def subscription_data_df():
    return pd.DataFrame({
        "customer_id": ["c1", "c2", "c3"],
        "status": ["active", "active", "past_due"],
        "days_subscribed": [400, 200, 30],
        "mrr": [500, 200, 50],
    })


def test_heuristic_multi_row_customer_and_subscription_data(mod, customer_data_df, subscription_data_df):
    """Regression test for the _normalize_score vectorization fix: this
    used to crash immediately on any real multi-row DataFrame."""
    component = mod.CustomerHealthScoreComponent(
        asset_name="health_out",
        customer_data_asset_key="customers", subscription_data_asset_key="subs",
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]

    @dg.asset(name="customers")
    def customers():
        return customer_data_df

    @dg.asset(name="subs")
    def subs():
        return subscription_data_df

    result = dg.materialize([asset_def, customers, subs])
    assert result.success
    df_out = result.output_for_node("health_out")
    assert set(["customer_id", "health_score", "risk_category", "is_churn_risk"]).issubset(df_out.columns)
    # The healthiest customer (c1: recent login, high frequency, active sub) should
    # score higher than the least healthy (c3: 90 days inactive, past_due).
    scores = df_out.set_index("customer_id")["health_score"]
    assert scores["c1"] > scores["c3"]

    mats = result.asset_materializations_for_node("health_out")
    assert "dagster/row_count" in mats[0].metadata


def test_heuristic_requires_at_least_one_input(mod):
    with pytest.raises(ValueError, match="at least one of customer_data"):
        mod.CustomerHealthScoreComponent(asset_name="x").build_defs(context=None)


def test_heuristic_asset_key_and_source_mutually_exclusive_per_input(mod):
    with pytest.raises(ValueError, match="set at most one"):
        mod.CustomerHealthScoreComponent(
            asset_name="x",
            customer_data_asset_key="customers",
            customer_data_source={"kind": "warehouse_query", "resource_key": "r", "sql": "SELECT 1"},
        ).build_defs(context=None)


@requires_duckdb
def test_heuristic_one_input_via_source_one_via_asset_key(mod, customer_data_df, subscription_data_df, tmp_path):
    import duckdb
    from dagster_duckdb import DuckDBResource

    db_path = str(tmp_path / "subs.duckdb")
    conn = duckdb.connect(db_path)
    conn.register("df_view", subscription_data_df)
    conn.execute("CREATE TABLE subs AS SELECT * FROM df_view")
    conn.close()

    component = mod.CustomerHealthScoreComponent(
        asset_name="health_out",
        customer_data_asset_key="customers",
        subscription_data_source={"kind": "warehouse_query", "resource_key": "duckdb_resource", "sql": "SELECT * FROM subs"},
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]

    @dg.asset(name="customers")
    def customers():
        return customer_data_df

    result = dg.materialize([asset_def, customers], resources={"duckdb_resource": DuckDBResource(database=db_path)})
    assert result.success
    df_out = result.output_for_node("health_out")
    assert "health_score" in df_out.columns


def test_ml_mode_upstream_asset_key(mod):
    df = pd.DataFrame({
        "login_frequency": [20, 5, 1, 15, 3],
        "mrr": [500, 200, 50, 300, 80],
        "churned": [0, 0, 1, 0, 1],
    })
    component = mod.CustomerHealthScoreComponent(
        asset_name="health_ml_out", scoring_method="ml",
        upstream_asset_key="raw", target_column="churned",
        feature_columns=["login_frequency", "mrr"],
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]

    @dg.asset(name="raw")
    def raw():
        return df

    result = dg.materialize([asset_def, raw])
    assert result.success
    df_out = result.output_for_node("health_ml_out")
    assert "predicted_class" in df_out.columns
    assert any(c.startswith("predicted_proba_") for c in df_out.columns)


def test_ml_mode_cannot_combine_with_multi_input_fields(mod):
    with pytest.raises(ValueError, match="cannot be combined"):
        mod.CustomerHealthScoreComponent(
            asset_name="x", scoring_method="ml",
            customer_data_asset_key="customers",
            upstream_asset_key="raw", target_column="churned", feature_columns=["x"],
        ).build_defs(context=None)


def test_ml_mode_requires_target_column(mod):
    with pytest.raises(ValueError, match="requires `target_column`"):
        mod.CustomerHealthScoreComponent(
            asset_name="x", scoring_method="ml",
            upstream_asset_key="raw", feature_columns=["x"],
        ).build_defs(context=None)


def test_sql_mode_requires_ml_scoring_method(mod):
    with pytest.raises(ValueError, match="requires scoring_method='ml'"):
        mod.CustomerHealthScoreComponent(
            asset_name="x", customer_data_asset_key="customers", execution_mode="sql",
        ).build_defs(context=None)


def test_sql_mode_bigquery_generates_create_model_and_predict(mod):
    stmts = mod._build_sql_mode_statements(
        "bigquery", "SELECT * FROM t", "ds.out", "ds.model1",
        "churned", ["login_frequency", "mrr"], 0.2, 1000,
    )
    assert len(stmts) == 2
    assert "CREATE OR REPLACE MODEL `ds.model1`" in stmts[0]
    assert "model_type='LOGISTIC_REG'" in stmts[0]
    assert "ML.PREDICT" in stmts[1]
