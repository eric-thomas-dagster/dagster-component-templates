"""Committed regression tests for real, pre-existing bugs found in
MLPipelineComponent via a full code-reading audit of all 36 ops (this
component previously had exactly ONE test, covering only the
warehouse_query/table_sink path -- not a single one of the 36 ops was
ever exercised by a test before this file).

Each bug here was confirmed by reading the actual code, not assumed from
docs. See the fix comments in component.py (search "FIX (2026-09-28)")
for the full write-up of each one.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module


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


def _upstream_source():
    return {"kind": "upstream_asset", "upstream_asset_key": "raw"}


def test_end_to_end_pipeline_with_leakage_safe_evaluate(mod, iris_df, feature_cols):
    """Sanity end-to-end run of the corrected, non-leaking pattern this
    session's fix applied to README.md's quick example and example.yaml:
    predict on the full scaled frame, but filter to split=='test' rows
    BEFORE evaluate, so accuracy reflects genuine held-out performance."""
    component = mod.MLPipelineComponent(
        asset_name_prefix="iris",
        source=_upstream_source(),
        target_column="species",
        feature_columns=feature_cols,
        steps=[
            {"id": "scaled", "op": "scale", "method": "standard"},
            {"id": "split", "op": "split", "test_size": 0.3, "stratify_column": "species", "random_state": 42},
            {"id": "trained", "op": "train", "model_type": "decision_tree", "task_type": "classification", "params": {"max_depth": 3}},
            # input: split (not scaled) -- 'split' is the step whose OWN
            # output actually carries the split column; 'scaled' predates it.
            {"id": "preds", "op": "predict", "model": "trained", "input": "split"},
            {"id": "test_preds", "op": "filter", "source": "preds", "predicate": "split == 'test'"},
            {"id": "metrics", "op": "evaluate", "model": "trained", "input": "test_preds", "task_type": "classification"},
        ],
        outputs={"assets": ["preds", "metrics"]},
    )
    defs = component.build_defs(context=None)

    @dg.asset(name="raw")
    def raw():
        return iris_df

    result = dg.materialize(list(defs.assets) + [raw])
    assert result.success

    preds_df = result.output_for_node("iris_pipeline", output_name="iris_preds")
    metrics_df = result.output_for_node("iris_pipeline", output_name="iris_metrics")

    # preds covers the FULL frame (both split values); metrics was computed
    # on the smaller, test-only subset -- the whole point of the fix.
    assert set(preds_df["split"].unique()) == {"train", "test"}
    accuracy = float(metrics_df.set_index("metric").loc["accuracy", "value"])
    assert 0.0 <= accuracy <= 1.0


def test_can_subset_closure_includes_upstream_prep_for_sourceless_train_step(mod, iris_df, feature_cols, monkeypatch):
    """Regression test for the can_subset dependency-closure bug: a train
    step with no explicit `source:` (the documented "defaults to most
    recent frame" shape used by this component's own README/example.yaml
    quick examples) used to get ZERO recorded static dependency, so
    selecting just a downstream output (here: `imp`) would skip `scaled`/
    `split` entirely and silently train on the raw, unsplit, unscaled
    ingested frame instead. Verified by spying on `_run_step` (module-level,
    resolved by name at call time, so monkeypatching it here is visible to
    the multi_asset closure) and asserting `scaled`/`split`/`trained` were
    all actually invoked for a subset materialization that only asked for `imp`."""
    component = mod.MLPipelineComponent(
        asset_name_prefix="iris",
        source=_upstream_source(),
        target_column="species",
        feature_columns=feature_cols,
        steps=[
            {"id": "scaled", "op": "scale", "method": "standard"},
            {"id": "split", "op": "split", "test_size": 0.3, "stratify_column": "species", "random_state": 42},
            # No `source:` on purpose -- this is the exact shape that triggered the bug.
            {"id": "trained", "op": "train", "model_type": "decision_tree", "task_type": "classification"},
            {"id": "imp", "op": "importance", "model": "trained"},
        ],
        outputs={"assets": ["scaled", "imp"]},
    )
    defs = component.build_defs(context=None)

    @dg.asset(name="raw")
    def raw():
        return iris_df

    executed_step_ids = []
    _orig_run_step = mod._run_step

    def _spy(step, state, target, features, context):
        executed_step_ids.append(step["id"])
        return _orig_run_step(step, state, target, features, context)

    monkeypatch.setattr(mod, "_run_step", _spy)

    result = dg.materialize(
        list(defs.assets) + [raw],
        # `raw` must stay in the selection too -- it's a separate asset
        # (not one of the multi_asset's own `outs`), and with no prior
        # persisted materialization in this fresh ephemeral run, its
        # output has to be produced in the same run for `source` to load.
        # `iris_scaled` staying OUT of the selection is what makes this a
        # genuine subset (is_subset=True) at the multi_asset level.
        selection=["raw", "iris_imp"],
    )
    assert result.success
    assert set(executed_step_ids) == {"scaled", "split", "trained", "imp"}, (
        f"expected scaled/split/trained/imp all executed for a subset materialization "
        f"of just `imp` (a sourceless train step must pull in its upstream prep steps); "
        f"got: {executed_step_ids!r}"
    )


def test_register_model_sample_frame_tracks_real_training_source_not_last_frame(mod, iris_df, feature_cols):
    """Regression test for the register_model sample-frame bug:
    `state["__model_source_frame__"]` must map a trained model's step id
    to the frame it was ACTUALLY trained on, not whatever ran most
    recently. Exercised directly at the _run_step level (register_model's
    mlflow/snowflake backends aren't installed in this environment, but
    the bug is entirely in this state-tracking mechanism, not in either
    backend's own code)."""
    state = {"source": iris_df, "__tracker__": None, "__step_metadata__": {}}

    scale_step = {"id": "scaled", "op": "scale", "method": "standard"}
    mod._run_step(scale_step, state, "species", feature_cols, _FakeContext())

    train_step = {"id": "trained", "op": "train", "model_type": "decision_tree", "task_type": "classification", "source": "scaled"}
    mod._run_step(train_step, state, "species", feature_cols, _FakeContext())

    # A step that produces an unrelated small DataFrame runs AFTER training
    # (e.g. importance) -- this is exactly the shape that fooled the old
    # "grab whatever ran most recently" logic.
    imp_step = {"id": "imp", "op": "importance", "model": "trained"}
    mod._run_step(imp_step, state, "species", feature_cols, _FakeContext())

    assert state["__model_source_frame__"]["trained"] == "scaled"
    # And the "most recent frame" at this point is `imp` (feature/importance
    # columns only) -- confirming the old fallback would have picked the
    # WRONG frame if register_model ran next.
    assert mod._last_frame_id(state) == "imp"


def test_best_cv_score_surfaces_in_step_metadata(mod, iris_df, feature_cols):
    """Regression test for the dead best_cv_score computation:
    grid_search/random_search/bayesian_search all compute a best CV score
    that used to be logged to console only and discarded."""
    state = {"source": iris_df, "__tracker__": None, "__step_metadata__": {}}
    step = {
        "id": "tuned", "op": "grid_search", "model_type": "decision_tree", "task_type": "classification",
        "param_grid": {"max_depth": [2, 3]}, "cv": 3,
    }
    mod._run_step(step, state, "species", feature_cols, _FakeContext())

    meta = state["__step_metadata__"]["tuned"]
    assert "best_cv_score" in meta
    assert 0.0 <= meta["best_cv_score"] <= 1.0
    assert hasattr(state["tuned"], "_ml_pipeline_best_cv_score")


class _FakeContext:
    class _Log:
        def info(self, *a, **k): pass
        def warning(self, *a, **k): pass
    log = _Log()


class _FakeMlflow:
    def __init__(self):
        self.logged_params = []
        self.logged_metrics = []
        self.logged_tables = []

    def log_params(self, d):
        self.logged_params.append(d)

    def log_metrics(self, d):
        self.logged_metrics.append(d)

    def log_table(self, data, artifact_file):
        self.logged_tables.append((data, artifact_file))


def _tracker_with_fake_mlflow(mod, cfg):
    tracker = mod._ExperimentTracker.__new__(mod._ExperimentTracker)
    tracker.cfg = cfg
    tracker.run_context = {}
    tracker.log = _FakeContext.log
    tracker.mlflow = _FakeMlflow()
    tracker.wandb = None
    tracker._active = True
    return tracker


def test_log_params_respects_log_params_false(mod):
    """Regression test: `experiment_tracking.mlflow.log_params: false` was
    documented but never actually checked -- log_params always fired."""
    tracker = _tracker_with_fake_mlflow(mod, {"mlflow": {"log_params": False}})
    tracker.log_params("step1", {"max_depth": 3})
    assert tracker.mlflow.logged_params == []


def test_log_params_defaults_to_enabled_when_unset(mod):
    tracker = _tracker_with_fake_mlflow(mod, {"mlflow": {}})
    tracker.log_params("step1", {"max_depth": 3})
    assert len(tracker.mlflow.logged_params) == 1


def test_log_metrics_respects_log_metrics_false(mod):
    tracker = _tracker_with_fake_mlflow(mod, {"mlflow": {"log_metrics": False}})
    tracker.log_metrics("step1", {"accuracy": 0.9})
    assert tracker.mlflow.logged_metrics == []


def test_log_artifact_df_only_fires_when_explicitly_enabled(mod):
    """Regression test: `log_artifacts` was documented in this component's
    own Field description as a real config option, but had NO
    implementation anywhere in the file before this fix."""
    df = pd.DataFrame({"a": [1, 2]})

    tracker_off = _tracker_with_fake_mlflow(mod, {"mlflow": {}})
    tracker_off.log_artifact_df("step1", df)
    assert tracker_off.mlflow.logged_tables == []

    tracker_on = _tracker_with_fake_mlflow(mod, {"mlflow": {"log_artifacts": True}})
    tracker_on.log_artifact_df("step1", df)
    assert len(tracker_on.mlflow.logged_tables) == 1
