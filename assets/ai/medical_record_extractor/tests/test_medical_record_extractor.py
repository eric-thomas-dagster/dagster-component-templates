"""Committed regression tests for MedicalRecordExtractorComponent's
input_type='file' content-extraction fix.

Before this fix, every file (PDF, image, anything) was opened in TEXT mode
with errors='replace', so a real medical record PDF or scanned image got
decoded as raw-bytes-as-UTF-8 garbage and fed straight into the LLM prompt --
silently not extracting anything real for the component's stated primary use
case. These tests exercise the fix against REAL files on disk (a real PNG
via Pillow, a real PDF via reportlab, a real .txt file) and mock only the
litellm.completion call -- no real API spend, no fake media.
"""
import base64
import sys
import types

import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def fake_litellm(monkeypatch):
    """Captures every completion() call's messages/kwargs; returns a canned
    valid JSON response by default. Tests mutate `calls`/`next_response` to
    steer behavior per row."""
    calls = []
    state = {"response_text": '{"field_a": "value_a"}'}

    def fake_completion(model, messages, api_key=None, **kwargs):
        calls.append({"model": model, "messages": messages, "kwargs": kwargs})
        return types.SimpleNamespace(
            choices=[types.SimpleNamespace(message=types.SimpleNamespace(content=state["response_text"]))]
        )

    fake_module = types.ModuleType("litellm")
    fake_module.completion = fake_completion
    monkeypatch.setitem(sys.modules, "litellm", fake_module)
    return calls, state


def _materialize_one_row(mod, component, file_path):
    defs = component.build_defs(load_context=None)
    asset_def = list(defs.assets)[0]
    upstream_df = pd.DataFrame({"local_path": [file_path]})

    @dg.asset(name=component.upstream_asset_key)
    def _upstream():
        return upstream_df

    return dg.materialize([asset_def, _upstream])


def test_image_file_reaches_prompt_as_real_vision_block(mod, fake_litellm, tmp_path):
    from PIL import Image

    calls, _ = fake_litellm
    img_path = tmp_path / "record.png"
    Image.new("RGB", (20, 20), color=(255, 0, 0)).save(img_path)

    component = mod.MedicalRecordExtractorComponent(
        asset_name="extracted",
        upstream_asset_key="docs_in",
        input_type="file",
        input_column="local_path",
        output_fields=["field_a"],
    )
    result = _materialize_one_row(mod, component, str(img_path))
    assert result.success

    message_content = calls[0]["messages"][0]["content"]
    image_blocks = [c for c in message_content if isinstance(c, dict) and c.get("type") == "image_url"]
    assert len(image_blocks) == 1, "expected exactly one image_url content block"
    data_url = image_blocks[0]["image_url"]["url"]
    assert data_url.startswith("data:image/png;base64,")
    b64_payload = data_url.split(",", 1)[1]
    decoded = base64.b64decode(b64_payload)
    assert decoded == img_path.read_bytes(), "decoded base64 must match the real PNG bytes exactly"


def test_pdf_with_real_text_reaches_prompt(mod, fake_litellm, tmp_path):
    from reportlab.pdfgen import canvas

    calls, _ = fake_litellm
    pdf_path = tmp_path / "record.pdf"
    c = canvas.Canvas(str(pdf_path))
    c.drawString(100, 700, "PATIENT: Jane Doe -- Diagnosis: Type 2 Diabetes")
    c.save()

    component = mod.MedicalRecordExtractorComponent(
        asset_name="extracted",
        upstream_asset_key="docs_in",
        input_type="file",
        input_column="local_path",
        output_fields=["field_a"],
    )
    result = _materialize_one_row(mod, component, str(pdf_path))
    assert result.success

    prompt_text = calls[0]["messages"][0]["content"]
    assert isinstance(prompt_text, str), "a text PDF should produce a plain string prompt, not a vision block"
    assert "PATIENT: Jane Doe" in prompt_text
    assert "Type 2 Diabetes" in prompt_text


def test_scanned_pdf_with_no_text_fails_cleanly_not_silently(mod, fake_litellm, tmp_path):
    """A PDF page written as pure vector graphics with no text layer (the
    reportlab equivalent of a scanned/photographed page) must fail that row
    cleanly -- not crash the whole run, and not silently send empty content
    as if it were real extracted text."""
    from reportlab.pdfgen import canvas

    pdf_path = tmp_path / "scanned.pdf"
    c = canvas.Canvas(str(pdf_path))
    c.rect(100, 100, 50, 50, fill=1)  # a drawn shape, no text at all
    c.save()

    component = mod.MedicalRecordExtractorComponent(
        asset_name="extracted",
        upstream_asset_key="docs_in",
        input_type="file",
        input_column="local_path",
        output_fields=["field_a"],
    )
    result = _materialize_one_row(mod, component, str(pdf_path))
    assert result.success, "one bad row must not fail the whole materialization"

    df = result.output_for_node("extracted")
    assert df.loc[0, "field_a"] is None
    meta = result.get_asset_materialization_events()[-1].materialization.metadata
    assert meta["extraction_failures"].value == 1


def test_max_content_chars_truncates_oversized_text(mod, fake_litellm, tmp_path):
    calls, _ = fake_litellm
    txt_path = tmp_path / "big.txt"
    txt_path.write_text("x" * 5000)

    component = mod.MedicalRecordExtractorComponent(
        asset_name="extracted",
        upstream_asset_key="docs_in",
        input_type="file",
        input_column="local_path",
        output_fields=["field_a"],
        max_content_chars=50,
    )
    result = _materialize_one_row(mod, component, str(txt_path))
    assert result.success

    prompt_text = calls[0]["messages"][0]["content"]
    doc_content = prompt_text.split("Document:\n")[1].split("\n\nReturn ONLY")[0]
    assert len(doc_content) == 50


def test_non_dict_llm_response_rejected_not_crashed(mod, fake_litellm, tmp_path):
    _, state = fake_litellm
    state["response_text"] = "[1, 2, 3]"
    txt_path = tmp_path / "doc.txt"
    txt_path.write_text("some real medical record content")

    component = mod.MedicalRecordExtractorComponent(
        asset_name="extracted",
        upstream_asset_key="docs_in",
        input_type="file",
        input_column="local_path",
        output_fields=["field_a"],
    )
    result = _materialize_one_row(mod, component, str(txt_path))
    assert result.success

    df = result.output_for_node("extracted")
    assert df.loc[0, "field_a"] is None
    meta = result.get_asset_materialization_events()[-1].materialization.metadata
    assert meta["extraction_failures"].value == 1


def test_llm_max_retries_and_response_format_forwarded_to_litellm(mod, fake_litellm, tmp_path):
    calls, _ = fake_litellm
    txt_path = tmp_path / "doc.txt"
    txt_path.write_text("some real medical record content")

    component = mod.MedicalRecordExtractorComponent(
        asset_name="extracted",
        upstream_asset_key="docs_in",
        input_type="file",
        input_column="local_path",
        output_fields=["field_a"],
        llm_max_retries=5,
    )
    result = _materialize_one_row(mod, component, str(txt_path))
    assert result.success
    assert calls[0]["kwargs"]["num_retries"] == 5
    assert calls[0]["kwargs"]["response_format"] == {"type": "json_object"}
