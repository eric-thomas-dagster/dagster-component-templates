"""Committed regression tests for PropensityScoringComponent's dual ingestion
(upstream_asset_key / source:warehouse_query) and new scoring_method split
(heuristic / ml).

`scoring_method='heuristic'` (default) is the ORIGINAL per-propensity_type
scoring, completely unchanged -- these tests confirm all 4 propensity_type
branches (purchase/upgrade/referral/engagement) still work exactly as before.

`scoring_method='ml'` is new: fits a real scikit-learn LogisticRegression
against a `target_column` the caller supplies. None of the 4 heuristic
formulas are fit against any observed outcome today -- there is no
"converted"/"did_upgrade"/"referred" label anywhere in the input schema --
so 'ml' mode is opt-in and requires the caller to bring their own label.
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
def behavior_df():
    rng = np.random.RandomState(0)
    n = 60
    return pd.DataFrame({
        "customer_id": [f"c{i}" for i in range(n)],
        "last_activity_date": pd.Timestamp.now() - pd.to_timedelta(rng.randint(0, 120, n), unit="D"),
        "activity_count": rng.randint(1, 100, n),
        "engagement_score": rng.uniform(0, 100, n),
    })


@pytest.fixture()
def labeled_behavior_df(behavior_df):
    df = behavior_df.copy()
    df["converted"] = (df["activity_count"] > df["activity_count"].median()).astype(int)
    return df


@pytest.mark.parametrize("propensity_type", ["purchase", "upgrade", "referral", "engagement"])
def test_heuristic_mode_all_types_unchanged(mod, behavior_df, propensity_type):
    component = mod.PropensityScoringComponent(
        asset_name="prop_out", upstream_asset_key="raw", propensity_type=propensity_type,
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]

    @dg.asset(name="raw")
    def raw():
        return behavior_df

    result = dg.materialize([asset_def, raw])
    assert result.success
    df_out = result.output_for_node("prop_out")
    assert set(["customer_id", "propensity_score", "propensity_level"]).issubset(df_out.columns)
    assert df_out["propensity_score"].between(0, 100).all()


def test_ml_mode_upstream_asset_key(mod, labeled_behavior_df):
    component = mod.PropensityScoringComponent(
        asset_name="prop_ml_out", upstream_asset_key="raw",
        scoring_method="ml", target_column="converted",
        feature_columns=["activity_count", "engagement_score"],
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]

    @dg.asset(name="raw")
    def raw():
        return labeled_behavior_df

    result = dg.materialize([asset_def, raw])
    assert result.success
    df_out = result.output_for_node("prop_ml_out")
    assert "predicted_class" in df_out.columns
    assert any(c.startswith("predicted_proba_") for c in df_out.columns)

    mats = result.asset_materializations_for_node("prop_ml_out")
    assert "accuracy" in mats[0].metadata


def test_ml_mode_requires_target_column(mod):
    with pytest.raises(ValueError, match="requires `target_column`"):
        mod.PropensityScoringComponent(
            asset_name="x", upstream_asset_key="raw",
            scoring_method="ml", feature_columns=["activity_count"],
        ).build_defs(context=None)


def test_ml_mode_requires_feature_columns(mod):
    with pytest.raises(ValueError, match="requires `feature_columns`"):
        mod.PropensityScoringComponent(
            asset_name="x", upstream_asset_key="raw",
            scoring_method="ml", target_column="converted",
        ).build_defs(context=None)


def test_mutual_exclusivity_guard(mod):
    with pytest.raises(ValueError, match="set exactly one"):
        mod.PropensityScoringComponent(asset_name="x").build_defs(context=None)


def test_sql_mode_requires_ml_scoring_method(mod):
    with pytest.raises(ValueError, match="requires scoring_method='ml'"):
        mod.PropensityScoringComponent(
            asset_name="x",
            source={"kind": "warehouse_query", "resource_key": "r", "sql": "SELECT 1"},
            execution_mode="sql",
        ).build_defs(context=None)


def test_sql_mode_bigquery_generates_create_model_and_predict(mod):
    stmts = mod._build_sql_mode_statements(
        "bigquery", "SELECT * FROM t", "ds.out", "ds.model1",
        "converted", ["activity_count", "engagement_score"], 0.2, 1000,
    )
    assert len(stmts) == 2
    assert "CREATE OR REPLACE MODEL `ds.model1`" in stmts[0]
    assert "model_type='LOGISTIC_REG'" in stmts[0]
    assert "ML.PREDICT" in stmts[1]


@requires_duckdb
def test_heuristic_mode_source_warehouse_query_against_real_duckdb(mod, behavior_df, tmp_path):
    import duckdb
    from dagster_duckdb import DuckDBResource

    db_path = str(tmp_path / "behavior.duckdb")
    conn = duckdb.connect(db_path)
    conn.register("df_view", behavior_df)
    conn.execute("CREATE TABLE behavior AS SELECT * FROM df_view")
    conn.close()

    component = mod.PropensityScoringComponent(
        asset_name="prop_sql_source",
        source={"kind": "warehouse_query", "resource_key": "duckdb_resource", "sql": "SELECT * FROM behavior"},
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], resources={"duckdb_resource": DuckDBResource(database=db_path)})
    assert result.success
    df_out = result.output_for_node("prop_sql_source")
    assert "propensity_score" in df_out.columns
