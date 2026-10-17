"""include_preview_metadata / preview_rows reach the asset's output metadata (and are off when disabled)."""
import importlib.util
import re
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

import dagster as dg

ROOT = Path(__file__).resolve().parent.parent
PREVIEW = [
    "assets/ai/rag_pipeline", "assets/analytics/chromadb_asset", "assets/analytics/customer_segmentation",
    "assets/analytics/funnel_analysis", "assets/analytics/gradient_boosting_model", "assets/analytics/logistic_regression_model",
    "assets/analytics/neural_network_model", "assets/analytics/pgvector_asset", "assets/analytics/pinecone_asset",
    "assets/analytics/product_usage_analytics", "assets/analytics/random_forest_model", "assets/analytics/wandb_asset",
    "assets/transforms/dataframe_join", "assets/transforms/filter",
]


def _load(rel):
    p = ROOT / rel / "component.py"
    spec = importlib.util.spec_from_file_location("prev_" + p.parent.name, p)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_preview_helper_copies_are_identical_and_every_declaring_component_calls_it():
    blocks = {}
    for rel in PREVIEW:
        s = (ROOT / rel / "component.py").read_text()
        m = re.search(r"# BEGIN output-preview helper.*?# END output-preview helper", s, re.S)
        assert m, rel
        blocks.setdefault(m.group(0), []).append(rel)
        assert re.search(r"\b_emit_preview\(context, ", s), rel
    assert len(blocks) == 1, [v for v in blocks.values()]


def _source(name, df):
    @dg.asset(name=name)
    def src():
        return df
    return src


def _meta(result, key):
    return {k: v for k, v in result.get_asset_materialization_events()[-1].materialization.metadata.items()}


def test_filter_and_join_emit_a_preview_only_when_enabled():
    df = pd.DataFrame({"customer_id": range(30), "age": range(30), "status": "active", "country": "US"})
    flt = _load("assets/transforms/filter")
    for enabled in (True, False):
        c = flt.FilterComponent(asset_name="out", upstream_asset_key="raw", condition="age > 5", include_preview_metadata=enabled, preview_rows=4)
        asset = next(a for a in c.build_defs(None).assets)
        r = dg.materialize([_source("raw", df), asset])
        md = r.asset_materializations_for_node("out")[0].metadata
        assert ("preview" in md) is enabled
        if enabled:
            assert md["preview"].value.count("\n") == 5  # header + separator + 4 rows - last has no trailing newline
    join = _load("assets/transforms/dataframe_join")
    right = pd.DataFrame({"customer_id": range(30), "tier": "gold"})
    c = join.DataframeJoin(asset_name="j", left_asset_key="l", right_asset_key="r", how="left", on=["customer_id"], include_preview_metadata=True, preview_rows=3)
    r = dg.materialize([_source("l", df), _source("r", right), next(a for a in c.build_defs(None).assets)])
    assert "tier" in r.asset_materializations_for_node("j")[0].metadata["preview"].value


def test_model_asset_previews_its_predictions():
    rng = np.random.default_rng(0)
    df = pd.DataFrame({"a": rng.normal(size=60), "b": rng.normal(size=60)})
    df["y"] = df["a"] * 2 + df["b"]
    rf = _load("assets/analytics/random_forest_model")
    c = rf.RandomForestModelComponent(asset_name="m", upstream_asset_key="feat", target_column="y", feature_columns=["a", "b"],
                                      task_type="regression", n_estimators=5, output_mode="predictions",
                                      include_preview_metadata=True, preview_rows=5)
    r = dg.materialize([_source("feat", df), next(a for a in c.build_defs(None).assets)])
    assert "predicted" in r.asset_materializations_for_node("m")[0].metadata["preview"].value


def test_preview_coexists_with_materialize_result_metadata():
    mod = _load("assets/analytics/wandb_asset")
    ns = {n: getattr(mod, n) for n in ("_emit_preview",)}

    @dg.asset
    def store(context: dg.AssetExecutionContext):
        ns["_emit_preview"](context, pd.DataFrame({"x": [1, 2]}), True, 5)
        return dg.MaterializeResult(metadata={"n": 2})

    md = dg.materialize([store]).asset_materializations_for_node("store")[0].metadata
    assert set(md) >= {"n", "preview"}
