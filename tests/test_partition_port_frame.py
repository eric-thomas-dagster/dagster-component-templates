"""Frame-consuming AI / transform / sink components filter their upstream rows to the run's partition.

Each component is built from its own example.yaml and materialized with a 40-row upstream (two days of 20 rows).
The step's  local is captured when the step returns (whether it succeeded or failed at its first external
call), so what is asserted is exactly what survived the partition filter. The network is blocked."""
import importlib.util
import socket
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import yaml

import dagster as dg

ROOT = Path(__file__).resolve().parent.parent
DAY = "2026-01-03"
PORTED = [
    "assets/ai/audio_diarized_transcriber",
    "assets/ai/cloud_dlp_inspect_asset",
    "assets/ai/cloud_text_to_speech_asset",
    "assets/ai/document_ai_extractor",
    "assets/ai/gemini_image_generation",
    "assets/ai/gemini_llm",
    "assets/ai/groq_llm",
    "assets/ai/litellm_text_to_speech",
    "assets/ai/mammouth_llm",
    "assets/ai/mistral_llm",
    "assets/ai/mistral_ocr",
    "assets/ai/openrouter_llm",
    "assets/ai/perplexity_llm",
    "assets/ai/rag_grounding_check",
    "assets/ai/speech_to_text_asset",
    "assets/ai/translation_api_asset",
    "assets/ai/vertex_ai_text_embeddings_asset",
    "assets/ai/video_scene_summarizer",
    "assets/ai/vision_api_asset",
    "assets/analytics/automl_asset",
    "assets/sinks/bigtable_writer_asset",
    "assets/sinks/cloud_tasks_enqueue_asset",
    "assets/sinks/firestore_writer_asset",
    "assets/sinks/msteams_message_send",
    "assets/sinks/pubsub_publish_asset",
    "assets/sinks/slack_message_send",
    "assets/sinks/smtp_send_asset",
    "assets/transforms/acord_xml_parser",
    "assets/transforms/audio_transform_asset",
    "assets/transforms/dataframe_extract_field",
    "assets/transforms/dataframe_flatten_nested_columns",
    "assets/transforms/fhir_resource_normalizer",
    "assets/transforms/fix_message_parser",
    "assets/transforms/hl7_v2_parser",
    "assets/transforms/hris_normalizer",
    "assets/transforms/image_exif_extractor",
    "assets/transforms/image_transform_asset",
    "assets/transforms/iso20022_payment_parser",
    "assets/transforms/text_codec_convert_asset",
    "assets/transforms/video_audio_extract_asset",
    "assets/transforms/video_frame_extract_asset",
    "assets/transforms/video_metadata_extractor",
    "assets/transforms/x12_edi_parser",
    "assets/sinks/azure_search_indexer",
    "assets/sinks/dataframe_to_azure_table",
    "assets/sinks/dataframe_to_dynatrace_events",
    "assets/sinks/dataframe_to_eventhub",
    "assets/sinks/dataframe_to_fabric_lakehouse",
    "assets/sinks/dataframe_to_kusto",
    "assets/sinks/dataframe_to_newrelic_logs",
    "assets/sinks/dataframe_to_odata",
    "assets/sinks/dataframe_to_otlp_logs",
    "assets/sinks/dataframe_to_otlp_metrics",
    "assets/sinks/dataframe_to_otlp_traces",
    "assets/sinks/dataframe_to_prometheus",
    "assets/sinks/dataframe_to_servicebus",
    "assets/analytics/pearson_correlation",
    "assets/sinks/dataframe_to_influxdb",
    "assets/sinks/dataframe_to_victoriametrics",
    "assets/transforms/blob_convert",
    "assets/transforms/drive_time",
    "assets/transforms/geo_buffer",
    "assets/transforms/geo_simplify",
    "assets/transforms/per_row_http_fetcher",
    "assets/transforms/points_from_latlon",
    "assets/transforms/poly_build",
    "assets/transforms/precisely_address_verify",
    "assets/transforms/r_script",
    "assets/transforms/spatial_info",
    "assets/transforms/spatial_process",
]


class _Response:
    status_code = 200
    ok = True
    headers = {}
    text = "{}"
    content = b"{}"

    def json(self, *a, **k):
        return {}

    def raise_for_status(self):
        pass


@pytest.fixture(autouse=True)
def _no_network(monkeypatch):
    import signal
    import time

    import requests

    monkeypatch.setattr(time, "sleep", lambda *_: None)  # retry/backoff loops must not slow the suite
    signal.signal(signal.SIGALRM, lambda *_: (_ for _ in ()).throw(TimeoutError("component hung")))
    signal.alarm(30)

    monkeypatch.setattr(requests.sessions.Session, "request", lambda self, *a, **k: _Response())

    def _refuse(*a, **k):
        raise OSError("network blocked in tests")

    monkeypatch.setattr(socket.socket, "connect", _refuse)
    yield
    signal.alarm(0)


def _frame():
    rng = np.random.default_rng(0)
    return pd.DataFrame({"text": [f"row {i}" for i in range(40)], "id": range(40), "v": rng.normal(size=40),
                         "ts": pd.to_datetime(["2026-01-02"] * 20 + ["2026-01-03"] * 20)})


class _AnyResource:
    def __getattr__(self, name):
        return self

    def __call__(self, *a, **k):
        return self


def _expand_env(value):
    """Examples spell secrets as ${ENV_VAR}; resolve them from the environment like a real project would."""
    import os
    import re

    if isinstance(value, str):
        return re.sub(r"\$\{(\w+)\}", lambda m: os.environ.get(m.group(1), m.group(0)), value)
    if isinstance(value, dict):
        return {k: _expand_env(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_expand_env(v) for v in value]
    return value


def _build(rel, extra, monkeypatch, tmp_path=None):
    if tmp_path is not None:  # GCP-backed components insist on credentials when they are built
        creds = tmp_path / "creds.json"
        creds.write_text("{}")
        monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", str(creds))
    folder = ROOT / rel
    doc = next(d for d in yaml.safe_load_all((folder / "example.yaml").read_text()) if d and "attributes" in d)
    sys.path.insert(0, str(folder))
    try:
        spec = importlib.util.spec_from_file_location("fr_" + folder.name, folder / "component.py")
        mod = importlib.util.module_from_spec(spec)
        monkeypatch.setitem(sys.modules, spec.name, mod)
        spec.loader.exec_module(mod)
        cls = getattr(mod, doc["type"].rsplit(".", 1)[-1])
        attrs = _expand_env({k: v for k, v in doc["attributes"].items() if k not in ("upstream_asset_keys",)})
        defs = cls(**{**attrs, **extra, "upstream_asset_key": "up_src"}).build_defs(None)
    finally:
        sys.path.remove(str(folder))
    return next(a for a in defs.assets if hasattr(a, "node_def"))


def _run(rel, partitioned, monkeypatch, tmp_path):
    extra = {"partition_type": "daily", "partition_start": "2026-01-01", "partition_date_column": "ts"} if partitioned else {}
    asset = _build(rel, extra, monkeypatch, tmp_path)
    resources = {k: dg.ResourceDefinition.hardcoded_resource(_AnyResource()) for k in asset.required_resource_keys if k != "io_manager"}
    fn_code = asset.op.compute_fn.decorated_fn.__code__
    seen = {}

    def prof(frame, event, arg):
        if event == "return" and frame.f_code is fn_code:
            seen["upstream"] = frame.f_locals.get("upstream")

    frame_df = _frame()

    @dg.asset(name="up_src")
    def up_src():
        return frame_df

    instance = dg.DagsterInstance.ephemeral()
    dg.materialize([up_src], instance=instance)
    kwargs = {"partition_key": DAY} if partitioned else {}
    sys.setprofile(prof)
    try:
        dg.materialize([asset, up_src], instance=instance, selection=[asset.key], raise_on_error=False, resources=resources, **kwargs)
    finally:
        sys.setprofile(None)
    return seen.get("upstream"), asset


@pytest.mark.parametrize("rel", PORTED, ids=lambda r: r.split("/", 1)[1])
def test_partitioned_run_only_sees_its_days_rows(rel, monkeypatch, tmp_path):
    seen, asset = _run(rel, True, monkeypatch, tmp_path)
    assert isinstance(asset.partitions_def, dg.DailyPartitionsDefinition)
    assert seen is not None and hasattr(seen, "columns"), "the step never held an upstream frame"
    assert len(seen) == 20 and set(pd.to_datetime(seen["ts"]).dt.strftime("%Y-%m-%d")) == {DAY}


@pytest.mark.parametrize("rel", PORTED, ids=lambda r: r.split("/", 1)[1])
def test_unpartitioned_run_sees_every_row(rel, monkeypatch, tmp_path):
    seen, asset = _run(rel, False, monkeypatch, tmp_path)
    assert asset.partitions_def is None and seen is not None and len(seen) == 40


def test_every_ported_component_carries_the_columns_in_component_and_schema():
    import json

    for rel in PORTED:
        s = (ROOT / rel / "component.py").read_text()
        for field in ("partition_date_column", "partition_static_column", "partition_static_dim"):
            assert f"    {field}:" in s, (rel, field)
        schema = json.loads((ROOT / rel / "schema.json").read_text())
        schema = schema.get("attributes") or schema.get("properties")
        assert {"partition_date_column", "partition_static_column", "partition_static_dim"} <= set(schema), rel


def test_scaffolded_components_share_one_partitions_def_builder():
    import re

    blocks = set()
    for rel in PORTED:
        t = (ROOT / rel / "component.py").read_text()
        m = re.search(r"# BEGIN scaffold partitions-def builder.*?# END scaffold partitions-def builder", t, re.S)
        if m:
            blocks.add(m.group(0))
    assert len(blocks) == 1
