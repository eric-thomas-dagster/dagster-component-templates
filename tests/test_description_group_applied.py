"""`description` and `group_name` set on a component reach its asset specs (checked across every category)."""
import importlib.util
import re
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parent.parent
SAMPLE = [
    "assets/ai/audio_transcriber", "assets/ai/llm_judge", "assets/ai/zero_shot_classifier", "assets/ai/moderation_scorer",
    "assets/analytics/chromadb_asset", "assets/analytics/feast_asset", "assets/transforms/arima_forecast",
    "assets/transforms/append_fields", "assets/sinks/cassandra_writer", "assets/sinks/dataframe_to_csv",
    "assets/sinks/dataframe_to_excel", "assets/sources/firestore_reader", "assets/ingestion/servicenow_ingestion",
    "assets/ingestion/document_ingestion", "assets/infrastructure/abinitio_run_asset", "assets/infrastructure/rivery_run_asset",
]


def _build(rel, **extra):
    folder = ROOT / rel
    doc = next(d for d in yaml.safe_load_all((folder / "example.yaml").read_text()) if d and "attributes" in d)
    sys.path.insert(0, str(folder))
    try:
        spec = importlib.util.spec_from_file_location("dg_" + folder.name, folder / "component.py")
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        cls = getattr(mod, doc["type"].rsplit(".", 1)[-1])
        return cls(**{**doc["attributes"], **extra}).build_defs(None)
    finally:
        sys.path.remove(str(folder))


@pytest.mark.parametrize("rel", SAMPLE)
def test_description_and_group_reach_the_asset_specs(rel):
    defs = _build(rel, description="SENTINEL description", group_name="sentinel_group")
    specs = [sp for a in defs.assets for sp in (a.specs if hasattr(a, "specs") else [a])]
    assert specs
    assert any("SENTINEL description" in (sp.description or "") for sp in specs)
    assert "sentinel_group" in {sp.group_name for sp in specs}


def test_unset_description_keeps_the_components_own_default():
    specs = [sp for a in _build("assets/sinks/dataframe_to_csv").assets for sp in a.specs]
    assert all(sp.description for sp in specs)
