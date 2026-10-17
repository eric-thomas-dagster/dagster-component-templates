"""Every component's schema.json documents its fields (the Designer renders its form from it)."""
import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent


def _schemas():
    return sorted(ROOT.glob("*/*/schema.json"))


NO_FIELDS = {"integrations/dbt_state_reuse_patch"}  # a patch with nothing to configure


def test_no_schema_has_an_empty_attributes_block():
    empty = []
    for f in _schemas():
        data = json.loads(f.read_text())
        attrs = data.get("attributes", data.get("properties"))
        if attrs is not None and not attrs and str(f.parent.relative_to(ROOT)) not in NO_FIELDS:
            empty.append(str(f.parent.relative_to(ROOT)))
    assert empty == [], empty


def test_mlflow_model_inference_documents_every_field():
    folder = ROOT / "assets/ai/mlflow_model_inference"
    spec = importlib.util.spec_from_file_location("mlf", folder / "component.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    documented = set(json.loads((folder / "schema.json").read_text())["attributes"])
    assert set(mod.MLflowModelInferenceComponent.model_fields) <= documented
