"""These assets/transforms and assets/analytics components had backfill_policy added without their
own dedicated tests/ directory to extend (6 siblings that already had test infra -- warehouse_join,
automl_asset, chromadb_asset, elasticsearch_asset, pgvector_asset, wandb_asset -- got real tests added
to their own suites instead and aren't repeated here). Rather than skip verification, this parametrizes
over all of them -- in the same spirit as test_field_shapes.py's repo-wide consistency checks -- loading
each real component.py, building a real component instance from its own example.yaml, and confirming
build_defs() actually wires backfill_policy into the resulting AssetsDefinition. It is a real check
against real code, not a mock; it just doesn't exercise each component's own business logic end to end.
Most of these components ALSO get real row-filtering coverage from tests/test_partition_port.py, which
already proves a partitioned run equals the unpartitioned run over that day's rows -- this file only
covers the backfill_policy field/wiring itself."""
import importlib.util
from pathlib import Path

import pytest
import yaml

import dagster as dg

ROOT = Path(__file__).resolve().parent.parent

NAMES = [
    "transforms/address_standardize", "transforms/blob_convert", "transforms/drive_time",
    "transforms/dynamic_rename", "transforms/geo_buffer", "transforms/geo_overlay",
    "transforms/geo_simplify", "transforms/jupyter_notebook", "transforms/make_group",
    "transforms/multi_field_binning", "transforms/per_row_http_fetcher", "transforms/points_from_latlon",
    "transforms/poly_build", "transforms/precisely_address_verify",
    "transforms/pyspark_pipeline", "transforms/r_script", "transforms/select_records",
    "transforms/spatial_info", "transforms/spatial_process", "transforms/train_test_splitter",
    "transforms/warehouse_dedup", "transforms/warehouse_filter", "transforms/warehouse_formula",
    "transforms/warehouse_multi_field_formula", "transforms/warehouse_multi_row_formula",
    "transforms/warehouse_summarize", "transforms/warehouse_top_n_per_group", "transforms/warehouse_union",
    "analytics/ab_controls", "analytics/ab_treatments", "analytics/ab_trend",
    "analytics/ad_spend_standardizer", "analytics/append_cluster", "analytics/buffer",
    "analytics/count_regression", "analytics/create_points", "analytics/cube_query_asset",
    "analytics/feast_asset", "analytics/gamma_regression", "analytics/jupyter_notebook_asset",
    "analytics/k_centroids_diagnostics", "analytics/lakefs_asset", "analytics/make_grid",
    "analytics/market_basket_rules", "analytics/model_coefficients", "analytics/model_score",
    "analytics/multidimensional_scaling", "analytics/pearson_correlation", "analytics/pinecone_asset",
    "analytics/smooth", "analytics/spline_model", "analytics/stepwise", "analytics/svm",
    "analytics/tecton_asset", "analytics/test_of_means", "analytics/ts_covariate_forecast",
    "analytics/vif",
]


def _load(name):
    d = ROOT / "assets" / name
    spec = importlib.util.spec_from_file_location(f"_bp_batch2_{name.replace('/', '_')}", d / "component.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    cls_name = next(n for n in dir(mod) if n.endswith("Component") and getattr(mod, n).__module__ == mod.__name__)
    cls = getattr(mod, cls_name)
    doc = next(doc for doc in yaml.safe_load_all((d / "example.yaml").read_text()) if doc and "attributes" in doc)
    return cls, dict(doc["attributes"])


def _build(name, **overrides):
    cls, attrs = _load(name)
    attrs = {**attrs, **overrides}
    attrs.setdefault("partition_type", "daily")
    attrs.setdefault("partition_start", "2024-01-01")
    if "partition_date_column" in getattr(cls, "model_fields", {}):
        attrs.setdefault("partition_date_column", "ts")
    comp = cls(**attrs)
    defs = comp.build_defs(None)
    return next(a for a in defs.assets if hasattr(a, "node_def"))


@pytest.mark.parametrize("name", NAMES)
def test_single_run_sets_a_real_backfill_policy(name):
    asset = _build(name, backfill_policy="single_run")
    assert asset.backfill_policy == dg.BackfillPolicy.single_run()


@pytest.mark.parametrize("name", NAMES)
def test_multi_run_caps_partitions_per_run(name):
    asset = _build(name, backfill_policy="multi_run", backfill_policy_max_partitions_per_run=2)
    assert asset.backfill_policy == dg.BackfillPolicy.multi_run(max_partitions_per_run=2)


@pytest.mark.parametrize("name", NAMES)
def test_unset_backfill_policy_leaves_dagsters_default(name):
    asset = _build(name)
    assert asset.backfill_policy is None
