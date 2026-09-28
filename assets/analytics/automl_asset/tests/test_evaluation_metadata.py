"""Committed regression tests for _confusion_matrix_metadata and
_feature_importance_metadata -- the "R^2 and other values and charts" gap:
automl_asset previously only emitted 4-5 scalar holdout metrics, nothing
rich enough to actually judge a model against (no confusion matrix, no
feature importance).

Neither helper needs flaml -- both only require an object with .predict(),
which is true of any fitted sklearn-compatible estimator (including
FLAML's own AutoML wrapper, but a plain RandomForest exercises the exact
same code path without requiring flaml[automl] + libomp to be installed).
Verified against two real, well-known scikit-learn toy datasets where the
"right answer" for top features is independently documented (wine's
alcohol/flavanoids, diabetes' bmi), not just "does it run".
"""
import pandas as pd
import pytest
from sklearn.datasets import load_wine, load_diabetes
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.model_selection import train_test_split

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def wine_split():
    data = load_wine(as_frame=True)
    df = data.data.copy()
    df["target"] = data.target
    features = list(data.data.columns)
    train_df, test_df = train_test_split(df, test_size=0.3, random_state=42, stratify=df["target"])
    return train_df, test_df, features


@pytest.fixture()
def diabetes_split():
    data = load_diabetes(as_frame=True)
    df = data.data.copy()
    df["target"] = data.target
    features = list(data.data.columns)
    train_df, test_df = train_test_split(df, test_size=0.3, random_state=42)
    return train_df, test_df, features


def test_confusion_matrix_shape_and_row_sums_match_true_class_counts(mod, wine_split):
    train_df, test_df, features = wine_split
    clf = RandomForestClassifier(random_state=42, n_estimators=50).fit(train_df[features], train_df["target"])
    y_pred = clf.predict(test_df[features])

    cm = mod._confusion_matrix_metadata(test_df["target"], y_pred)

    assert cm["labels"] == ["0", "1", "2"]
    assert len(cm["matrix"]) == 3
    assert all(len(row) == 3 for row in cm["matrix"])
    # Each row of a confusion matrix sums to that true class's row count --
    # a real invariant, not just "produces some numbers".
    true_counts = test_df["target"].value_counts().sort_index().tolist()
    assert [sum(row) for row in cm["matrix"]] == true_counts


def test_feature_importance_classification_ranks_known_strong_predictors_high(mod, wine_split):
    train_df, test_df, features = wine_split
    clf = RandomForestClassifier(random_state=42, n_estimators=50).fit(train_df[features], train_df["target"])

    fi = mod._feature_importance_metadata(clf, test_df, "target", features, "classification", 42)

    assert set(fi["feature"]) == set(features)
    assert fi["importance_mean"] == sorted(fi["importance_mean"], reverse=True)
    # alcohol and flavanoids are well-documented as among the strongest
    # class-separating features in the wine dataset -- a real correctness
    # check, not just "the list is sorted".
    assert set(fi["feature"][:3]) & {"alcohol", "flavanoids", "color_intensity"}


def test_feature_importance_regression_ranks_bmi_highest(mod, diabetes_split):
    train_df, test_df, features = diabetes_split
    reg = RandomForestRegressor(random_state=42, n_estimators=50).fit(train_df[features], train_df["target"])

    fi = mod._feature_importance_metadata(reg, test_df, "target", features, "regression", 42)

    assert set(fi["feature"]) == set(features)
    assert fi["importance_mean"] == sorted(fi["importance_mean"], reverse=True)
    # bmi is the best-known single predictor of diabetes progression in
    # this exact toy dataset.
    assert fi["feature"][0] == "bmi"


def test_feature_importance_std_is_nonnegative(mod, wine_split):
    train_df, test_df, features = wine_split
    clf = RandomForestClassifier(random_state=42, n_estimators=50).fit(train_df[features], train_df["target"])
    fi = mod._feature_importance_metadata(clf, test_df, "target", features, "classification", 42)
    assert all(s >= 0 for s in fi["importance_std"])
