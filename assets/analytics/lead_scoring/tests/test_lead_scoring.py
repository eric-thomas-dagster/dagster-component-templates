"""Committed regression tests for LeadScoringComponent's new multi-input dual
ingestion (each of lead_data/behavioral_data/company_data independently
supports *_asset_key OR *_source) and new scoring_method split (heuristic / ml).

Also the regression test for a real, pre-existing bug found while adding
this: `_calculate_fit_score(self, lead_data, company_data)` accepted
`company_data` as a parameter but its body never referenced it at all --
`company_data_asset_key` was a completely non-functional input, advertised
in fields/schema/README but with zero effect on the output. Fixed by
merging company_data onto lead_data (on a shared company_id/account_id/
organization_id) before scoring.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module, requires_duckdb


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def lead_data_df():
    return pd.DataFrame({
        "lead_id": ["l1", "l2", "l3"],
        "job_title": ["VP of Engineering", "Manager", "Analyst"],
        "company_id": ["c1", "c2", "c3"],
    })


@pytest.fixture()
def company_data_df():
    return pd.DataFrame({
        "company_id": ["c1", "c2", "c3"],
        "company_size": [200, 300, 150],  # all in the 50-500 "ideal" bucket -> size_score 100
        "industry": ["Software", "Software", "Software"],  # target industry -> industry_score 100
    })


@pytest.fixture()
def behavioral_data_df():
    return pd.DataFrame({
        "lead_id": ["l1", "l2", "l3"],
        "email_opens": [5, 2, 0],
        "page_views": [10, 3, 1],
    })


def test_heuristic_lead_and_behavioral_combined(mod, lead_data_df, behavioral_data_df):
    component = mod.LeadScoringComponent(
        asset_name="scored_leads",
        lead_data_asset_key="leads", behavioral_data_asset_key="behavior",
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]

    @dg.asset(name="leads")
    def leads():
        return lead_data_df

    @dg.asset(name="behavior")
    def behavior():
        return behavioral_data_df

    result = dg.materialize([asset_def, leads, behavior])
    assert result.success
    df_out = result.output_for_node("scored_leads")
    assert set(["lead_id", "lead_score", "lead_temperature", "is_mql", "is_sql"]).issubset(df_out.columns)

    mats = result.asset_materializations_for_node("scored_leads")
    assert "dagster/row_count" in mats[0].metadata


def test_company_data_actually_affects_fit_score(mod, lead_data_df, company_data_df):
    """Regression test for the company_data dead-parameter fix: connecting
    company_data must change the fit score, since lead_data alone has no
    company_size/industry columns for the VP/Manager/Analyst leads."""
    without_company = mod.LeadScoringComponent(
        asset_name="scored_leads", lead_data_asset_key="leads",
        scoring_model="fit_only", include_score_breakdown=True,
    )
    with_company = mod.LeadScoringComponent(
        asset_name="scored_leads", lead_data_asset_key="leads",
        company_data_asset_key="company", scoring_model="fit_only",
        include_score_breakdown=True,
    )

    @dg.asset(name="leads")
    def leads():
        return lead_data_df

    @dg.asset(name="company")
    def company():
        return company_data_df

    defs_without = without_company.build_defs(context=None)
    result_without = dg.materialize([list(defs_without.assets)[0], leads])
    assert result_without.success
    df_without = result_without.output_for_node("scored_leads")

    defs_with = with_company.build_defs(context=None)
    result_with = dg.materialize([list(defs_with.assets)[0], leads, company])
    assert result_with.success
    df_with = result_with.output_for_node("scored_leads")

    fit_without = df_without.set_index("lead_id")["fit_score"]
    fit_with = df_with.set_index("lead_id")["fit_score"]
    assert (fit_with > fit_without).all(), (
        "connecting company_data should raise fit scores (adds company_size + "
        "industry components lead_data alone doesn't have) -- if this fails, "
        "company_data is being silently ignored again"
    )


def test_heuristic_requires_at_least_one_input(mod):
    with pytest.raises(ValueError, match="at least one of lead_data"):
        mod.LeadScoringComponent(asset_name="x").build_defs(context=None)


def test_heuristic_asset_key_and_source_mutually_exclusive_per_input(mod):
    with pytest.raises(ValueError, match="set at most one"):
        mod.LeadScoringComponent(
            asset_name="x",
            lead_data_asset_key="leads",
            lead_data_source={"kind": "warehouse_query", "resource_key": "r", "sql": "SELECT 1"},
        ).build_defs(context=None)


@requires_duckdb
def test_heuristic_one_input_via_source_one_via_asset_key(mod, lead_data_df, behavioral_data_df, tmp_path):
    import duckdb
    from dagster_duckdb import DuckDBResource

    db_path = str(tmp_path / "behavior.duckdb")
    conn = duckdb.connect(db_path)
    conn.register("df_view", behavioral_data_df)
    conn.execute("CREATE TABLE behavior AS SELECT * FROM df_view")
    conn.close()

    component = mod.LeadScoringComponent(
        asset_name="scored_leads",
        lead_data_asset_key="leads",
        behavioral_data_source={"kind": "warehouse_query", "resource_key": "duckdb_resource", "sql": "SELECT * FROM behavior"},
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]

    @dg.asset(name="leads")
    def leads():
        return lead_data_df

    result = dg.materialize([asset_def, leads], resources={"duckdb_resource": DuckDBResource(database=db_path)})
    assert result.success
    df_out = result.output_for_node("scored_leads")
    assert "lead_score" in df_out.columns


def test_ml_mode_upstream_asset_key(mod):
    df = pd.DataFrame({
        "company_size": [100, 20, 500, 5, 300],
        "engagement_score": [80, 10, 90, 5, 60],
        "converted": [1, 0, 1, 0, 1],
    })
    component = mod.LeadScoringComponent(
        asset_name="lead_ml_out", scoring_method="ml",
        upstream_asset_key="raw", target_column="converted",
        feature_columns=["company_size", "engagement_score"],
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]

    @dg.asset(name="raw")
    def raw():
        return df

    result = dg.materialize([asset_def, raw])
    assert result.success
    df_out = result.output_for_node("lead_ml_out")
    assert "predicted_class" in df_out.columns
    assert any(c.startswith("predicted_proba_") for c in df_out.columns)


def test_ml_mode_cannot_combine_with_multi_input_fields(mod):
    with pytest.raises(ValueError, match="cannot be combined"):
        mod.LeadScoringComponent(
            asset_name="x", scoring_method="ml",
            lead_data_asset_key="leads",
            upstream_asset_key="raw", target_column="converted", feature_columns=["x"],
        ).build_defs(context=None)


def test_ml_mode_requires_target_column(mod):
    with pytest.raises(ValueError, match="requires `target_column`"):
        mod.LeadScoringComponent(
            asset_name="x", scoring_method="ml",
            upstream_asset_key="raw", feature_columns=["x"],
        ).build_defs(context=None)


def test_sql_mode_requires_ml_scoring_method(mod):
    with pytest.raises(ValueError, match="requires scoring_method='ml'"):
        mod.LeadScoringComponent(
            asset_name="x", lead_data_asset_key="leads", execution_mode="sql",
        ).build_defs(context=None)


def test_sql_mode_bigquery_generates_create_model_and_predict(mod):
    stmts = mod._build_sql_mode_statements(
        "bigquery", "SELECT * FROM t", "ds.out", "ds.model1",
        "converted", ["company_size", "engagement_score"], 0.2, 1000,
    )
    assert len(stmts) == 2
    assert "CREATE OR REPLACE MODEL `ds.model1`" in stmts[0]
    assert "model_type='LOGISTIC_REG'" in stmts[0]
    assert "ML.PREDICT" in stmts[1]
