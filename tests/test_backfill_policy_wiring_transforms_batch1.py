"""These 16 assets/transforms components had backfill_policy added without their own dedicated
tests/ directory to extend (older, simpler-shaped components with no existing test harness to build a
real dg.materialize()-over-a-range fixture from). Rather than skip verification, this parametrizes over
all 16 -- in the same spirit as test_field_shapes.py's repo-wide consistency checks -- loading each real
component.py, building a real component instance from its own example.yaml, and confirming build_defs()
actually wires backfill_policy into the resulting AssetsDefinition. It is a real check against real code,
not a mock; it just doesn't exercise each component's own business logic end to end."""
import importlib.util
from pathlib import Path

import pytest
import yaml

import dagster as dg

ROOT = Path(__file__).resolve().parent.parent

NAMES = [
    "acord_xml_parser", "audio_transform_asset", "dataframe_extract_field",
    "dataframe_flatten_nested_columns", "fhir_resource_normalizer", "fix_message_parser",
    "hl7_v2_parser", "hris_normalizer", "image_exif_extractor", "image_transform_asset",
    "iso20022_payment_parser", "text_codec_convert_asset", "video_audio_extract_asset",
    "video_frame_extract_asset", "video_metadata_extractor", "x12_edi_parser",
]


def _load(name):
    d = ROOT / "assets/transforms" / name
    spec = importlib.util.spec_from_file_location(f"_bp_batch1_{name}", d / "component.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    cls_name = next(n for n in dir(mod) if n.endswith("Component") and getattr(mod, n).__module__ == mod.__name__)
    cls = getattr(mod, cls_name)
    doc = next(doc for doc in yaml.safe_load_all((d / "example.yaml").read_text()) if doc and "attributes" in doc)
    return cls, dict(doc["attributes"])


@pytest.mark.parametrize("name", NAMES)
def test_single_run_sets_a_real_backfill_policy(name):
    cls, attrs = _load(name)
    attrs = {**attrs, "backfill_policy": "single_run"}
    attrs.setdefault("partition_type", "daily")
    attrs.setdefault("partition_start", "2024-01-01")
    comp = cls(**attrs)
    defs = comp.build_defs(None)
    asset = next(a for a in defs.assets if hasattr(a, "node_def"))
    assert asset.backfill_policy == dg.BackfillPolicy.single_run()


@pytest.mark.parametrize("name", NAMES)
def test_multi_run_caps_partitions_per_run(name):
    cls, attrs = _load(name)
    attrs = {**attrs, "backfill_policy": "multi_run", "backfill_policy_max_partitions_per_run": 2}
    attrs.setdefault("partition_type", "daily")
    attrs.setdefault("partition_start", "2024-01-01")
    comp = cls(**attrs)
    defs = comp.build_defs(None)
    asset = next(a for a in defs.assets if hasattr(a, "node_def"))
    assert asset.backfill_policy == dg.BackfillPolicy.multi_run(max_partitions_per_run=2)


@pytest.mark.parametrize("name", NAMES)
def test_unset_backfill_policy_leaves_dagsters_default(name):
    cls, attrs = _load(name)
    attrs.setdefault("partition_type", "daily")
    attrs.setdefault("partition_start", "2024-01-01")
    comp = cls(**attrs)
    defs = comp.build_defs(None)
    asset = next(a for a in defs.assets if hasattr(a, "node_def"))
    assert asset.backfill_policy is None
