"""SQL-mode builders must reject table/model/column names that could change the SQL's shape
(`source_sql` stays free-form by design)."""
import importlib.util
import inspect
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
COMPS = ["churn_prediction", "customer_health_score", "gradient_boosting_model", "k_means_clustering",
         "lead_scoring", "linear_regression_model", "logistic_regression_model", "naive_bayes_model",
         "neural_network_model", "pca", "propensity_scoring"]


def _mod(name):
    spec = importlib.util.spec_from_file_location(f"_sqlg_{name}", ROOT / "assets/analytics" / name / "component.py")
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def _kwargs(fn, dialect="bigquery", **over):
    base = dict(dialect=dialect, source_sql="SELECT * FROM t", output_table="ds.out", model_name="ds.m",
                target_column="y", feature_columns=["a", "b"], output_column="cluster", output_prefix="pc",
                test_size=0.2, max_iter=10, n_estimators=5, hidden_units="8", task_type="classification",
                n_clusters=3, normalize=True, n_components=2)
    base.update(over)
    return {k: v for k, v in base.items() if k in inspect.signature(fn).parameters}


@pytest.mark.parametrize("name", COMPS)
def test_valid_names_pass_and_injection_is_rejected(name):
    fn = _mod(name)._build_sql_mode_statements
    d = "databricks" if name == "naive_bayes_model" else "bigquery"  # predict-only: Databricks is its sole dialect
    assert fn(**_kwargs(fn, dialect=d))  # plain, dot-qualified, hyphenated names are fine
    for field in ("output_table", "model_name", "target_column"):
        if field in inspect.signature(fn).parameters:
            with pytest.raises(ValueError, match="plain SQL identifier"):
                fn(**_kwargs(fn, dialect=d, **{field: "x; DROP TABLE users"}))
    with pytest.raises(ValueError, match="plain SQL identifier"):
        fn(**_kwargs(fn, dialect=d, feature_columns=["a", "b') --"]))


def test_anomaly_detection():
    m = _mod("anomaly_detection")
    ok = dict(dialect="bigquery", source_sql="SELECT * FROM t", output_table="ds.out", metric_col="v",
              detection_method="z_score", threshold=3.0, group_by_field=None, timestamp_col=None, ma_window=5)
    assert m._build_sql_mode_query(**ok)
    for field in ("output_table", "metric_col", "group_by_field"):
        with pytest.raises(ValueError, match="plain SQL identifier"):
            m._build_sql_mode_query(**{**ok, field: "a b"})
