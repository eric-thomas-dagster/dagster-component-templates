"""Tests for MistralOcrComponent.

Mocks only the paid network calls (`_call_ocr_process`,
`_upload_and_get_signed_url`) -- everything else (DataFrame handling,
document/image classification, base64 encoding, retries, error paths)
runs for real against real local files.
"""
import sys
import types
from types import SimpleNamespace

import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module, make_upstream_asset

mod = load_component_module()
MistralOcrComponent = mod.MistralOcrComponent


@pytest.fixture(autouse=True)
def fake_mistralai_module(monkeypatch):
    """mistralai isn't installed in this environment; inject a minimal fake
    so `from mistralai import Mistral` succeeds. The real client is never
    exercised -- tests monkeypatch _call_ocr_process / _upload_and_get_signed_url
    directly instead of this fake client's methods."""
    fake_mod = types.ModuleType("mistralai")

    class FakeMistral:
        def __init__(self, api_key):
            self.api_key = api_key

    fake_mod.Mistral = FakeMistral
    monkeypatch.setitem(sys.modules, "mistralai", fake_mod)
    yield


def _page(index, markdown):
    return SimpleNamespace(index=index, markdown=markdown)


def _component(**overrides):
    attrs = dict(asset_name="ocr_results", upstream_asset_key="source_docs", input_column="file_path")
    attrs.update(overrides)
    return MistralOcrComponent(**attrs)


def _materialize(component, df, monkeypatch):
    monkeypatch.setenv("MISTRAL_API_KEY", "test-key-123")
    defs = component.build_defs(context=None)
    upstream = make_upstream_asset("source_docs", df)
    full_defs = dg.Definitions(assets=[*defs.assets, upstream])
    return full_defs.get_implicit_global_asset_job_def().execute_in_process()


def test_remote_document_url(monkeypatch):
    captured = {}

    def fake_ocr(client, **kwargs):
        captured.update(kwargs)
        return SimpleNamespace(pages=[_page(0, "# Hello")], model="mistral-ocr-latest", usage_info={})

    monkeypatch.setattr(mod, "_call_ocr_process", fake_ocr)

    df = pd.DataFrame({"file_path": ["https://example.com/doc.pdf"]})
    result = _materialize(_component(), df, monkeypatch)

    assert result.success
    assert captured["document"] == {"type": "document_url", "document_url": "https://example.com/doc.pdf"}
    out = result.asset_value(dg.AssetKey("ocr_results"))
    assert out["mistral_ocr_markdown"].iloc[0] == "# Hello"


def test_remote_image_url(monkeypatch):
    captured = {}

    def fake_ocr(client, **kwargs):
        captured.update(kwargs)
        return SimpleNamespace(pages=[_page(0, "a photo of a cat")], model="x", usage_info={})

    monkeypatch.setattr(mod, "_call_ocr_process", fake_ocr)

    df = pd.DataFrame({"file_path": ["https://example.com/pic.jpg"]})
    result = _materialize(_component(), df, monkeypatch)

    assert result.success
    assert captured["document"] == {"type": "image_url", "image_url": "https://example.com/pic.jpg"}


def test_local_image_base64_no_upload(monkeypatch, tmp_path):
    captured = {}
    upload_called = []

    def fake_ocr(client, **kwargs):
        captured.update(kwargs)
        return SimpleNamespace(pages=[_page(0, "ocr text")], model="x", usage_info={})

    def fake_upload(*args, **kwargs):
        upload_called.append(True)
        raise AssertionError("local images must not go through the upload path")

    monkeypatch.setattr(mod, "_call_ocr_process", fake_ocr)
    monkeypatch.setattr(mod, "_upload_and_get_signed_url", fake_upload)

    img_path = tmp_path / "receipt.png"
    img_path.write_bytes(b"\x89PNG\r\n\x1a\nfakebytes")

    df = pd.DataFrame({"file_path": [str(img_path)]})
    result = _materialize(_component(), df, monkeypatch)

    assert result.success
    assert not upload_called
    assert captured["document"]["type"] == "image_url"
    assert captured["document"]["image_url"].startswith("data:image/png;base64,")


def test_local_document_goes_through_upload(monkeypatch, tmp_path):
    captured = {}
    upload_calls = []

    def fake_ocr(client, **kwargs):
        captured.update(kwargs)
        return SimpleNamespace(pages=[_page(0, "invoice text")], model="x", usage_info={})

    def fake_upload(client, file_name, content):
        upload_calls.append((file_name, content))
        return "https://signed.example.com/abc123"

    monkeypatch.setattr(mod, "_call_ocr_process", fake_ocr)
    monkeypatch.setattr(mod, "_upload_and_get_signed_url", fake_upload)

    pdf_path = tmp_path / "invoice.pdf"
    pdf_path.write_bytes(b"%PDF-1.4 fake pdf bytes")

    df = pd.DataFrame({"file_path": [str(pdf_path)]})
    result = _materialize(_component(), df, monkeypatch)

    assert result.success
    assert len(upload_calls) == 1
    assert upload_calls[0][0] == "invoice.pdf"
    assert upload_calls[0][1] == b"%PDF-1.4 fake pdf bytes"
    assert captured["document"] == {"type": "document_url", "document_url": "https://signed.example.com/abc123"}


def test_input_kind_override_forces_image(monkeypatch):
    captured = {}

    def fake_ocr(client, **kwargs):
        captured.update(kwargs)
        return SimpleNamespace(pages=[_page(0, "x")], model="x", usage_info={})

    monkeypatch.setattr(mod, "_call_ocr_process", fake_ocr)

    # A .pdf extension would normally classify as a document; force image_url instead.
    df = pd.DataFrame({"file_path": ["https://example.com/scan.pdf"]})
    result = _materialize(_component(input_kind="image"), df, monkeypatch)

    assert result.success
    assert captured["document"]["type"] == "image_url"


def test_output_pages_column_structured(monkeypatch):
    import json

    def fake_ocr(client, **kwargs):
        return SimpleNamespace(pages=[_page(0, "page one"), _page(1, "page two")], model="x", usage_info={})

    monkeypatch.setattr(mod, "_call_ocr_process", fake_ocr)

    df = pd.DataFrame({"file_path": ["https://example.com/doc.pdf"]})
    result = _materialize(_component(output_pages_column="pages_json"), df, monkeypatch)

    assert result.success
    out = result.asset_value(dg.AssetKey("ocr_results"))
    pages = json.loads(out["pages_json"].iloc[0])
    assert pages == [{"index": 0, "markdown": "page one"}, {"index": 1, "markdown": "page two"}]
    assert out["mistral_ocr_markdown"].iloc[0] == "page one\n\n---\n\npage two"


def test_ocr_call_fails_records_error_column(monkeypatch):
    def fake_ocr(client, **kwargs):
        raise RuntimeError("401 unauthorized: invalid_api_key")

    monkeypatch.setattr(mod, "_call_ocr_process", fake_ocr)

    df = pd.DataFrame({"file_path": ["https://example.com/doc.pdf"]})
    result = _materialize(_component(max_retries=0), df, monkeypatch)

    assert result.success
    out = result.asset_value(dg.AssetKey("ocr_results"))
    assert pd.isna(out["mistral_ocr_markdown"].iloc[0])
    assert "401" in out["mistral_ocr_markdown_error"].iloc[0]


def test_empty_upstream_short_circuits(monkeypatch):
    def fake_ocr(client, **kwargs):
        raise AssertionError("should never be called for an empty upstream")

    monkeypatch.setattr(mod, "_call_ocr_process", fake_ocr)

    df = pd.DataFrame({"file_path": pd.Series([], dtype="object")})
    result = _materialize(_component(), df, monkeypatch)

    assert result.success
    out = result.asset_value(dg.AssetKey("ocr_results"))
    assert out.empty
    assert "mistral_ocr_markdown" in out.columns


def test_invalid_input_kind_raises():
    comp = _component(input_kind="bogus")
    with pytest.raises(ValueError, match="input_kind"):
        comp.build_defs(context=None)
