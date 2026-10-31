"""bedrock_converse_asset: real end-to-end tests.

demo_mode (the default) exercises the real asset-building/execution code
with the component's own internal `_DemoBedrockClient` swapped in -- no AWS
calls. Real mode is exercised by monkeypatching the module-level
`_build_bedrock_client` factory (this component's documented seam) to
return a fake client -- the same convention `anthropic_llm_batch` uses.
`dg.materialize()` is real throughout.
"""
import pandas as pd
import pytest
import dagster as dg
from dagster._core.errors import DagsterAssetCheckFailedError

from .conftest import load_component_module

mod = load_component_module()
BedrockConverseAssetComponent = mod.BedrockConverseAssetComponent


def _component(**kw):
    attrs = dict(asset_name="resp", upstream_asset_key="notes", aws_region="us-east-2")
    attrs.update(kw)
    return BedrockConverseAssetComponent(**attrs)


def _materialize(comp, upstream_df, **kwargs):
    @dg.asset(name="notes")
    def notes():
        return upstream_df

    defs = comp.build_defs(None)
    return dg.materialize([notes, *defs.assets], **kwargs)


def test_demo_mode_text_no_schema_adds_output_column():
    comp = _component()
    result = _materialize(comp, pd.DataFrame({"text": ["hello", "world"]}), raise_on_error=True)
    assert result.success
    df = result.asset_value("resp")
    assert list(df["text"]) == ["hello", "world"]
    assert df["llm_response"].str.startswith("demo response").all()


def test_demo_mode_custom_output_column():
    comp = _component(output_column="model_says")
    result = _materialize(comp, pd.DataFrame({"text": ["hi"]}), raise_on_error=True)
    df = result.asset_value("resp")
    assert "model_says" in df.columns


def test_prompt_template_input_substitution():
    captured = {}

    class SpyClient:
        def converse(self, *, messages, **_kw):
            captured["text"] = messages[0]["content"][0]["text"]
            return {"output": {"message": {"content": [{"text": "ok"}]}}, "usage": {}, "metrics": {}}

    mod._build_bedrock_client = lambda region: SpyClient()
    comp = _component(demo_mode=False, prompt_template="Summarize: {input}")
    _materialize(comp, pd.DataFrame({"text": ["the input text"]}), raise_on_error=True)
    assert captured["text"] == "Summarize: the input text"


def test_image_input_sends_raw_bytes_not_base64(tmp_path):
    img = tmp_path / "doc.png"
    raw = b"\x89PNG\r\n\x1a\nnotarealpngbutbytes"
    img.write_bytes(raw)
    captured = {}

    class SpyClient:
        def converse(self, *, messages, **_kw):
            captured["content"] = messages[0]["content"]
            return {"output": {"message": {"content": [{"text": "ok"}]}}, "usage": {}, "metrics": {}}

    mod._build_bedrock_client = lambda region: SpyClient()
    comp = _component(demo_mode=False, input_column="path", input_type="image", prompt_template="extract")
    _materialize(comp, pd.DataFrame({"path": [str(img)]}), raise_on_error=True)
    image_block = captured["content"][0]["image"]
    assert image_block["format"] == "png"
    assert image_block["source"]["bytes"] == raw  # raw bytes, not base64-encoded


def test_output_schema_adds_one_column_per_property_and_fills_missing_with_none():
    schema = {"type": "object", "properties": {"carrier_name": {"type": "string"}, "total_amount": {"type": "number"}}}
    comp = _component(output_schema=schema)
    result = _materialize(comp, pd.DataFrame({"text": ["a", "b"]}), raise_on_error=True)
    df = result.asset_value("resp")
    assert set(["carrier_name", "total_amount"]).issubset(df.columns)
    assert df["total_amount"].apply(lambda v: isinstance(v, (int, float))).all()


def test_invalid_json_response_fills_none_and_fails_validation():
    class BadClient:
        def converse(self, **_kw):
            return {"output": {"message": {"content": [{"text": "not json at all"}]}}, "usage": {}, "metrics": {}}

    mod._build_bedrock_client = lambda region: BadClient()
    schema = {"type": "object", "properties": {"x": {"type": "string"}}}
    comp = _component(demo_mode=False, output_schema=schema, min_valid_rate=0.5)
    result = _materialize(comp, pd.DataFrame({"text": ["a"]}), raise_on_error=True)
    df = result.asset_value("resp")
    assert df["x"].isna().all()
    evals = result.get_asset_check_evaluations()
    assert len(evals) == 1 and evals[0].check_name == "output_schema_valid_rate" and not evals[0].passed


def test_min_valid_rate_warn_does_not_block():
    class BadClient:
        def converse(self, **_kw):
            return {"output": {"message": {"content": [{"text": "nope"}]}}, "usage": {}, "metrics": {}}

    mod._build_bedrock_client = lambda region: BadClient()
    schema = {"type": "object", "properties": {"x": {"type": "string"}}}
    comp = _component(demo_mode=False, output_schema=schema, min_valid_rate=0.9, min_valid_rate_severity="WARN")
    result = _materialize(comp, pd.DataFrame({"text": ["a"]}), raise_on_error=True)
    assert result.success


def test_min_valid_rate_error_blocks():
    class BadClient:
        def converse(self, **_kw):
            return {"output": {"message": {"content": [{"text": "nope"}]}}, "usage": {}, "metrics": {}}

    mod._build_bedrock_client = lambda region: BadClient()
    schema = {"type": "object", "properties": {"x": {"type": "string"}}}
    comp = _component(demo_mode=False, output_schema=schema, min_valid_rate=0.9, min_valid_rate_severity="ERROR")
    with pytest.raises(DagsterAssetCheckFailedError):
        _materialize(comp, pd.DataFrame({"text": ["a"]}), raise_on_error=True)


def test_no_min_valid_rate_means_no_check():
    schema = {"type": "object", "properties": {"x": {"type": "string"}}}
    comp = _component(output_schema=schema)
    result = _materialize(comp, pd.DataFrame({"text": ["a"]}), raise_on_error=True)
    assert result.get_asset_check_evaluations() == []


def test_row_level_failure_does_not_abort_the_whole_run():
    class FlakyClient:
        calls = 0

        def converse(self, **_kw):
            FlakyClient.calls += 1
            if FlakyClient.calls == 1:
                raise RuntimeError("transient AWS error")
            return {"output": {"message": {"content": [{"text": "ok"}]}}, "usage": {}, "metrics": {}}

    mod._build_bedrock_client = lambda region: FlakyClient()
    comp = _component(demo_mode=False)
    result = _materialize(comp, pd.DataFrame({"text": ["a", "b"]}), raise_on_error=True)
    df = result.asset_value("resp")
    assert df["llm_response"].tolist() == [None, "ok"]
    md = result.get_asset_materialization_events()[-1].materialization.metadata
    assert md["failures"].value == 1


def test_partition_date_column_filters_rows():
    comp = _component(partition_type="daily", partition_start="2024-01-01", partition_date_column="day")
    df = pd.DataFrame({"text": ["a", "b", "c"], "day": ["2024-01-01", "2024-01-02", "2024-01-02"]})
    result = _materialize(comp, df, partition_key="2024-01-02", raise_on_error=True)
    out = result.asset_value("resp")
    assert sorted(out["text"]) == ["b", "c"]


def test_token_and_latency_metadata_aggregated():
    class UsageClient:
        def converse(self, **_kw):
            return {
                "output": {"message": {"content": [{"text": "ok"}]}},
                "usage": {"inputTokens": 10, "outputTokens": 20},
                "metrics": {"latencyMs": 100},
            }

    mod._build_bedrock_client = lambda region: UsageClient()
    comp = _component(demo_mode=False)
    result = _materialize(comp, pd.DataFrame({"text": ["a", "b"]}), raise_on_error=True)
    md = result.get_asset_materialization_events()[-1].materialization.metadata
    assert md["total_input_tokens"].value == 20
    assert md["total_output_tokens"].value == 40
    assert md["latency_p50_ms"].value == 100.0


def test_real_mode_uses_configured_region():
    seen = {}

    def fake_build(region_name):
        seen["region"] = region_name
        class C:
            def converse(self, **_kw):
                return {"output": {"message": {"content": [{"text": "ok"}]}}, "usage": {}, "metrics": {}}
        return C()

    mod._build_bedrock_client = fake_build
    comp = _component(demo_mode=False, aws_region="eu-west-1")
    _materialize(comp, pd.DataFrame({"text": ["a"]}), raise_on_error=True)
    assert seen["region"] == "eu-west-1"
