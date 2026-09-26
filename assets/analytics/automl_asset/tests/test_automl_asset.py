"""Committed regression tests for AutoMLAssetComponent.

No paid API is involved in this component at all -- every test here runs
a REAL FLAML search against real scikit-learn toy datasets. Nothing is
mocked. Time budgets are kept small (3-6s) to keep the suite fast while
still exercising a genuine multi-estimator search.
"""
import json
import os
from datetime import datetime, timedelta, timezone

import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module, requires_flaml_automl

pytestmark = requires_flaml_automl


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def wine_df():
    from sklearn.datasets import load_wine
    data = load_wine(as_frame=True)
    df = data.data.copy()
    df["target"] = data.target
    return df


@pytest.fixture()
def diabetes_df():
    from sklearn.datasets import load_diabetes
    data = load_diabetes(as_frame=True)
    df = data.data.copy()
    df["target"] = data.target
    return df


def _materialize(component, df, upstream_name="data_in"):
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]

    @dg.asset(name=upstream_name)
    def _upstream():
        return df

    return dg.materialize([asset_def, _upstream])


def test_full_search_then_cached_refit(mod, wine_df, tmp_path):
    state_path = str(tmp_path / "state.json")

    component1 = mod.AutoMLAssetComponent(
        asset_name="automl_out",
        upstream_asset_key="data_in",
        target_column="target",
        task_type="classification",
        state_path=state_path,
        time_budget_seconds=5,
        estimator_list=["lgbm", "rf"],
    )
    result1 = _materialize(component1, wine_df)
    assert result1.success
    meta1 = result1.get_asset_materialization_events()[-1].materialization.metadata
    assert meta1["search_mode"].text == "full_search"
    assert os.path.isfile(state_path)
    first_search_time = meta1["search_time_seconds"].value

    component2 = mod.AutoMLAssetComponent(
        asset_name="automl_out2",
        upstream_asset_key="data_in",
        target_column="target",
        task_type="classification",
        state_path=state_path,
        time_budget_seconds=5,
        estimator_list=["lgbm", "rf"],
    )
    result2 = _materialize(component2, wine_df)
    assert result2.success
    meta2 = result2.get_asset_materialization_events()[-1].materialization.metadata
    assert meta2["search_mode"].text == "cached_refit"
    assert meta2["best_estimator"].text == meta1["best_estimator"].text
    assert meta2["search_time_seconds"].value < first_search_time / 3, (
        "cached refit must be substantially cheaper than the original full search"
    )

    df_out = result2.output_for_node("automl_out2")
    assert "predicted" in df_out.columns
    assert len(df_out) == len(wine_df)


def test_refresh_search_forces_full_search_despite_valid_cache(mod, wine_df, tmp_path):
    state_path = str(tmp_path / "state.json")
    base_kwargs = dict(
        upstream_asset_key="data_in",
        target_column="target",
        task_type="classification",
        state_path=state_path,
        time_budget_seconds=3,
        estimator_list=["lgbm", "rf"],
    )

    result1 = _materialize(mod.AutoMLAssetComponent(asset_name="a1", **base_kwargs), wine_df)
    assert result1.get_asset_materialization_events()[-1].materialization.metadata["search_mode"].text == "full_search"

    result2 = _materialize(
        mod.AutoMLAssetComponent(asset_name="a2", refresh_search=True, **base_kwargs), wine_df,
    )
    meta2 = result2.get_asset_materialization_events()[-1].materialization.metadata
    assert meta2["search_mode"].text == "full_search"


def test_stale_cache_triggers_fresh_search(mod, wine_df, tmp_path):
    state_path = str(tmp_path / "state.json")
    base_kwargs = dict(
        upstream_asset_key="data_in",
        target_column="target",
        task_type="classification",
        state_path=state_path,
        time_budget_seconds=3,
        estimator_list=["lgbm", "rf"],
    )
    _materialize(mod.AutoMLAssetComponent(asset_name="a1", **base_kwargs), wine_df)

    # Backdate the cached state past max_state_age_hours.
    with open(state_path) as f:
        state = json.load(f)
    state["created_at"] = (datetime.now(timezone.utc) - timedelta(hours=100)).isoformat()
    with open(state_path, "w") as f:
        json.dump(state, f)

    result2 = _materialize(
        mod.AutoMLAssetComponent(asset_name="a2", max_state_age_hours=24, **base_kwargs), wine_df,
    )
    meta2 = result2.get_asset_materialization_events()[-1].materialization.metadata
    assert meta2["search_mode"].text == "full_search"


def test_regression_task_end_to_end(mod, diabetes_df, tmp_path):
    state_path = str(tmp_path / "state.json")
    component = mod.AutoMLAssetComponent(
        asset_name="automl_out",
        upstream_asset_key="data_in",
        target_column="target",
        task_type="regression",
        state_path=state_path,
        time_budget_seconds=4,
        estimator_list=["lgbm", "rf"],
    )
    result = _materialize(component, diabetes_df)
    assert result.success
    meta = result.get_asset_materialization_events()[-1].materialization.metadata
    assert "holdout_r2" in meta
    assert "holdout_mae" in meta
    df_out = result.output_for_node("automl_out")
    assert "predicted" in df_out.columns


def test_non_numeric_feature_auto_encoded(mod, diabetes_df, tmp_path):
    df = diabetes_df.copy()
    df["region"] = (df.index % 3).map({0: "north", 1: "south", 2: "east"})
    state_path = str(tmp_path / "state.json")

    component = mod.AutoMLAssetComponent(
        asset_name="automl_out",
        upstream_asset_key="data_in",
        target_column="target",
        task_type="regression",
        state_path=state_path,
        time_budget_seconds=4,
        estimator_list=["lgbm", "rf"],
    )
    result = _materialize(component, df)
    assert result.success, "a non-numeric feature column must be auto-encoded, not crash the run"


def test_model_output_path_persists_a_usable_model(mod, wine_df, tmp_path):
    import joblib

    state_path = str(tmp_path / "state.json")
    model_path = str(tmp_path / "model.joblib")
    component = mod.AutoMLAssetComponent(
        asset_name="automl_out",
        upstream_asset_key="data_in",
        target_column="target",
        task_type="classification",
        state_path=state_path,
        time_budget_seconds=4,
        estimator_list=["lgbm", "rf"],
        model_output_path=model_path,
    )
    result = _materialize(component, wine_df)
    assert result.success
    assert os.path.isfile(model_path)

    loaded_model = joblib.load(model_path)
    features = [c for c in wine_df.columns if c != "target"]
    preds = loaded_model.predict(wine_df[features])
    assert len(preds) == len(wine_df)


def test_invalid_task_type_raises(mod):
    component = mod.AutoMLAssetComponent(
        asset_name="automl_out",
        upstream_asset_key="data_in",
        target_column="target",
        task_type="not_a_real_task",
        state_path="/tmp/unused_state.json",
    )
    with pytest.raises(ValueError, match="task_type"):
        component.build_defs(context=None)


def test_missing_target_column_raises_clearly(mod, wine_df, tmp_path):
    component = mod.AutoMLAssetComponent(
        asset_name="automl_out",
        upstream_asset_key="data_in",
        target_column="not_a_real_column",
        task_type="classification",
        state_path=str(tmp_path / "state.json"),
        time_budget_seconds=3,
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]

    @dg.asset(name="data_in")
    def data_in():
        return wine_df

    result = dg.materialize([asset_def, data_in], raise_on_error=False)
    assert not result.success
