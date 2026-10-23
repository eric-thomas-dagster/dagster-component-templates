"""Committed regression tests for StructuredDocumentExtractorComponent's
input_type='file' content-extraction fix.

Before this fix, every file (PDF, image, anything) was opened in TEXT mode
with errors='replace', so a real invoice/receipt PDF or scanned image got
decoded as raw-bytes-as-UTF-8 garbage and fed straight into the LLM prompt --
silently not extracting anything real for the component's stated primary use
case. These tests exercise the fix against REAL files on disk (a real PNG
via Pillow, a real PDF via reportlab, a real .txt file) and mock only the
litellm.completion call -- no real API spend, no fake media.
"""
import base64
import json
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
    """Captures every completion() call's messages; returns a canned valid
    JSON response by default. Tests mutate `calls`/`next_response` to steer
    behavior per row."""
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
    img_path = tmp_path / "receipt.png"
    Image.new("RGB", (20, 20), color=(255, 0, 0)).save(img_path)

    component = mod.StructuredDocumentExtractorComponent(
        asset_name="extracted",
        upstream_asset_key="docs_in",
        input_type="file",
        input_column="local_path",
        document_type="custom",
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
    pdf_path = tmp_path / "invoice.pdf"
    c = canvas.Canvas(str(pdf_path))
    c.drawString(100, 700, "INVOICE #12345 -- Total Due: $42.00")
    c.save()

    component = mod.StructuredDocumentExtractorComponent(
        asset_name="extracted",
        upstream_asset_key="docs_in",
        input_type="file",
        input_column="local_path",
        document_type="custom",
        output_fields=["invoice_number"],
    )
    result = _materialize_one_row(mod, component, str(pdf_path))
    assert result.success

    prompt_text = calls[0]["messages"][0]["content"]
    assert isinstance(prompt_text, str), "a text PDF should produce a plain string prompt, not a vision block"
    assert "INVOICE #12345" in prompt_text
    assert "Total Due: $42.00" in prompt_text


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

    component = mod.StructuredDocumentExtractorComponent(
        asset_name="extracted",
        upstream_asset_key="docs_in",
        input_type="file",
        input_column="local_path",
        document_type="custom",
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

    component = mod.StructuredDocumentExtractorComponent(
        asset_name="extracted",
        upstream_asset_key="docs_in",
        input_type="file",
        input_column="local_path",
        document_type="custom",
        output_fields=["field_a"],
        max_content_chars=50,
    )
    result = _materialize_one_row(mod, component, str(txt_path))
    assert result.success

    prompt_text = calls[0]["messages"][0]["content"]
    doc_content = prompt_text.split("Document content:\n")[1].split("\n\nReturn only")[0]
    assert len(doc_content) == 50


def test_non_dict_llm_response_rejected_not_crashed(mod, fake_litellm, tmp_path):
    _, state = fake_litellm
    state["response_text"] = "[1, 2, 3]"
    txt_path = tmp_path / "doc.txt"
    txt_path.write_text("some real document content")

    component = mod.StructuredDocumentExtractorComponent(
        asset_name="extracted",
        upstream_asset_key="docs_in",
        input_type="file",
        input_column="local_path",
        document_type="custom",
        output_fields=["field_a"],
    )
    result = _materialize_one_row(mod, component, str(txt_path))
    assert result.success

    df = result.output_for_node("extracted")
    assert df.loc[0, "field_a"] is None
    meta = result.get_asset_materialization_events()[-1].materialization.metadata
    assert meta["extraction_failures"].value == 1


def test_llm_max_retries_forwarded_to_litellm(mod, fake_litellm, tmp_path):
    calls, _ = fake_litellm
    txt_path = tmp_path / "doc.txt"
    txt_path.write_text("some real document content")

    component = mod.StructuredDocumentExtractorComponent(
        asset_name="extracted",
        upstream_asset_key="docs_in",
        input_type="file",
        input_column="local_path",
        document_type="custom",
        output_fields=["field_a"],
        llm_max_retries=5,
    )
    result = _materialize_one_row(mod, component, str(txt_path))
    assert result.success
    assert calls[0]["kwargs"]["num_retries"] == 5


def test_neither_upstream_asset_key_nor_path_raises(mod):
    component = mod.StructuredDocumentExtractorComponent(
        asset_name="extracted",
        document_type="custom",
        output_fields=["field_a"],
    )
    with pytest.raises(ValueError, match="upstream_asset_key"):
        component.build_defs(load_context=None)


def test_custom_document_type_without_output_fields_raises(mod):
    component = mod.StructuredDocumentExtractorComponent(
        asset_name="extracted",
        upstream_asset_key="docs_in",
        document_type="custom",
    )
    with pytest.raises(ValueError, match="output_fields"):
        component.build_defs(load_context=None)


def test_extract_one_returns_fields_and_the_raw_reply(mod, fake_litellm, tmp_path):
    calls, state = fake_litellm
    state["response_text"] = '```json\n{"total": 12.5, "extra": "dropped"}\n```'
    doc = tmp_path / "a.txt"
    doc.write_text("Total due: 12.50")
    fields, raw = mod.extract_one(str(doc), output_fields=["total", "vendor"], model="gpt-4o", api_key="k")
    assert fields == {"total": 12.5, "vendor": None}  # extras dropped, omitted fields None
    assert "```" in raw
    assert calls[0]["kwargs"]["num_retries"] == 2
    assert "api_base" not in calls[0]["kwargs"]


def test_extract_one_raises_on_a_bad_reply_so_callers_can_show_it(mod, fake_litellm, tmp_path):
    _, state = fake_litellm
    state["response_text"] = "not json"
    doc = tmp_path / "a.txt"
    doc.write_text("x")
    with pytest.raises(Exception):
        mod.extract_one(str(doc), output_fields=["a"], model="m")


def test_api_base_reaches_litellm_only_when_set(mod, fake_litellm, tmp_path, monkeypatch):
    calls, _ = fake_litellm
    doc = tmp_path / "a.txt"
    doc.write_text("x")
    mod.extract_one(str(doc), output_fields=["field_a"], model="ollama/llava", api_base="http://localhost:11434")
    assert calls[0]["kwargs"]["api_base"] == "http://localhost:11434"


def test_component_resolves_api_base_from_the_named_env_var(mod, fake_litellm, tmp_path, monkeypatch):
    calls, _ = fake_litellm
    monkeypatch.setenv("MY_BASE", "http://localhost:1234/v1")
    doc = tmp_path / "a.txt"
    doc.write_text("hello")
    component = mod.StructuredDocumentExtractorComponent(
        asset_name="extracted", upstream_asset_key="docs_in", input_column="local_path",
        document_type="custom", output_fields=["field_a"], model="openai/local", api_base_env_var="MY_BASE",
    )
    assert _materialize_one_row(mod, component, str(doc)).success
    assert calls[0]["kwargs"]["api_base"] == "http://localhost:1234/v1"


def test_prompt_preview_matches_what_the_asset_sends(mod, fake_litellm, tmp_path):
    calls, _ = fake_litellm
    doc = tmp_path / "a.txt"
    doc.write_text("hello")
    msgs = mod.build_extraction_messages(str(doc), input_type="file", output_fields=["field_a"], doc_label="invoice")
    component = mod.StructuredDocumentExtractorComponent(
        asset_name="extracted", upstream_asset_key="docs_in", input_column="local_path",
        document_type="invoice", output_fields=["field_a"],
    )
    assert _materialize_one_row(mod, component, str(doc)).success
    assert calls[0]["messages"] == msgs


# --- validation + the extraction_quality check -----------------------------------------------------------------------------

@pytest.fixture()
def docs_litellm(monkeypatch):
    """Answers each document by a marker in its text: {"marker": {fields}} -- or the string "BAD" for unparseable JSON."""
    answers = {}

    def fake_completion(model, messages, api_key=None, **kwargs):
        prompt = messages[0]["content"]
        text = prompt if isinstance(prompt, str) else prompt[0]["text"]
        for marker, reply in answers.items():
            if marker in text:
                content = reply if isinstance(reply, str) else json.dumps(reply)
                return types.SimpleNamespace(choices=[types.SimpleNamespace(message=types.SimpleNamespace(content=content))])
        raise AssertionError(f"no canned answer for: {text[:80]}")

    fake_module = types.ModuleType("litellm")
    fake_module.completion = fake_completion
    monkeypatch.setitem(sys.modules, "litellm", fake_module)
    return answers


INVOICE_FIELDS = ["invoice_number", "vendor", "total_amount", "tax", "line_items"]
VALIDATION = {
    "required_fields": ["invoice_number", "vendor"],
    "reconcile": {"total_field": "total_amount", "items_field": "line_items", "add_fields": ["tax"]},
    "min_pass_rate": 0.6,
}


def _good(n):
    return {"invoice_number": f"INV-{n}", "vendor": "ACME", "total_amount": "$110.00", "tax": 10,
            "line_items": [{"amount": 60}, {"amount": "$40.00"}]}


def _run(mod, tmp_path, docs, validation, **extra):
    paths = []
    for name, text in docs.items():
        p = tmp_path / f"{name}.txt"
        p.write_text(text)
        paths.append(str(p))
    component = mod.StructuredDocumentExtractorComponent(
        asset_name="invoices", upstream_asset_key="docs_in", input_column="local_path", document_type="custom",
        output_fields=INVOICE_FIELDS, validation=validation, **extra,
    )
    defs = component.build_defs(load_context=None)

    @dg.asset(name="docs_in")
    def _upstream():
        return pd.DataFrame({"local_path": paths})

    result = dg.materialize([list(defs.assets)[0], _upstream])
    return result, paths


def _output(result):
    return result.output_for_node("invoices")


def test_documents_are_flagged_with_reasons_and_the_check_reports_the_pass_rate(mod, docs_litellm, tmp_path):
    docs_litellm.update({
        "alpha": _good(1),
        "bravo": {**_good(2), "vendor": None},
        "charlie": {**_good(3), "total_amount": 500},
        "delta": "not json at all",
    })
    result, paths = _run(mod, tmp_path, {"alpha": "alpha", "bravo": "bravo", "charlie": "charlie", "delta": "delta"}, VALIDATION)
    assert result.success
    df = _output(result)
    by_marker = {p.rsplit("/", 1)[-1][:-4]: (s, i) for p, s, i in zip(df["local_path"], df["_validation_status"], df["_validation_issues"])}
    assert by_marker["alpha"] == ("ok", "")
    assert by_marker["bravo"] == ("needs_review", "missing:vendor")
    assert by_marker["charlie"][0] == "needs_review" and by_marker["charlie"][1].startswith("totals_mismatch:items+adjustments=110,total=500")
    assert by_marker["delta"] == ("needs_review", "extraction_failed")

    (check,) = result.get_asset_check_evaluations()
    assert check.check_name == "extraction_quality"
    assert check.passed is False  # 1 of 4 passed; 0.6 needed
    assert check.metadata["documents"].value == 4 and check.metadata["ok"].value == 1 and check.metadata["needs_review"].value == 3
    review = {r["source"].rsplit("/", 1)[-1]: r["issues"] for r in check.metadata["review"].value}
    assert set(review) == {"bravo.txt", "charlie.txt", "delta.txt"} and review["delta.txt"] == "extraction_failed"


def test_the_check_passes_at_or_above_the_minimum_pass_rate(mod, docs_litellm, tmp_path):
    docs_litellm.update({"alpha": _good(1), "bravo": _good(2), "charlie": {**_good(3), "vendor": ""}})
    result, _ = _run(mod, tmp_path, {"alpha": "alpha", "bravo": "bravo", "charlie": "charlie"}, VALIDATION)
    (check,) = result.get_asset_check_evaluations()
    assert check.passed is True  # 2/3 = 0.67 >= 0.6
    assert check.severity.name == "WARN"


def test_without_validation_nothing_changes(mod, docs_litellm, tmp_path):
    docs_litellm["alpha"] = _good(1)
    result, _ = _run(mod, tmp_path, {"alpha": "alpha"}, None)
    df = _output(result)
    assert "_validation_status" not in df.columns and "_validation_issues" not in df.columns
    assert not [c for c in result.get_asset_check_evaluations() if c.check_name == "extraction_quality"]


def test_validation_also_works_in_direct_path_mode(mod, docs_litellm, tmp_path):
    docs_litellm.update({"alpha": _good(1), "bravo": {**_good(2), "invoice_number": None}})
    (tmp_path / "alpha.txt").write_text("alpha")
    (tmp_path / "bravo.txt").write_text("bravo")
    component = mod.StructuredDocumentExtractorComponent(
        asset_name="invoices", path=str(tmp_path / "*.txt"), document_type="custom", output_fields=INVOICE_FIELDS,
        validation={"required_fields": ["invoice_number"]},
    )
    result = dg.materialize(list(component.build_defs(load_context=None).assets))
    df = _output(result)
    assert dict(zip(df["path"].map(lambda p: p.rsplit("/", 1)[-1]), df["_validation_status"])) == {"alpha.txt": "ok", "bravo.txt": "needs_review"}


def test_an_empty_run_passes_the_check(mod, docs_litellm, tmp_path):
    component = mod.StructuredDocumentExtractorComponent(
        asset_name="invoices", path=str(tmp_path / "*.txt"), document_type="custom", output_fields=INVOICE_FIELDS, validation=VALIDATION,
    )
    result = dg.materialize(list(component.build_defs(load_context=None).assets))
    (check,) = result.get_asset_check_evaluations()
    assert check.passed is True and check.metadata["documents"].value == 0


def test_reconcile_reads_line_items_given_as_a_json_string_and_skips_unreadable_ones(mod):
    v = mod.normalize_validation("a", {"reconcile": {"total_field": "total_amount", "items_field": "line_items"}}, INVOICE_FIELDS)
    ok = {"total_amount": "30", "line_items": '[{"amount": 10}, {"amount": 20}]'}
    assert mod.validate_extraction(ok, True, v) == []
    assert mod.validate_extraction({**ok, "total_amount": "31"}, True, v)[0].startswith("totals_mismatch")
    assert mod.validate_extraction({"total_amount": "30", "line_items": "garbage"}, True, v) == []
    assert mod.validate_extraction({"total_amount": "30", "line_items": [{"amount": "n/a"}]}, True, v) == []
    assert mod.validate_extraction({"total_amount": None, "line_items": [{"amount": 5}]}, True, v) == []


def test_tolerance_and_custom_amount_key(mod):
    v = mod.normalize_validation("a", {"reconcile": {"total_field": "total_amount", "items_field": "line_items",
                                                      "item_amount_key": "price", "tolerance": 0.5}}, INVOICE_FIELDS)
    assert mod.validate_extraction({"total_amount": 10.4, "line_items": [{"price": 10}]}, True, v) == []
    assert mod.validate_extraction({"total_amount": 10.6, "line_items": [{"price": 10}]}, True, v)


@pytest.mark.parametrize("raw,message", [
    ("nope", "must be a mapping"),
    ({"required": ["x"]}, "unknown validation key"),
    ({"required_fields": ["ghost"]}, "not in output_fields"),
    ({"min_pass_rate": 1.5}, "between 0 and 1"),
    ({"min_pass_rate": True}, "between 0 and 1"),
    ({"reconcile": "x"}, "must be a mapping"),
    ({"reconcile": {"total_field": "total_amount"}}, "items_field must name one of output_fields"),
    ({"reconcile": {"total_field": "total_amount", "items_field": "line_items", "tolerance": -1}}, "tolerance"),
    ({"reconcile": {"total_field": "total_amount", "items_field": "line_items", "add_fields": ["ghost"]}}, "add_fields"),
    ({"reconcile": {"total_field": "total_amount", "items_field": "line_items", "nope": 1}}, "unknown validation.reconcile key"),
])
def test_bad_validation_is_rejected_with_a_clear_message(mod, raw, message):
    with pytest.raises(ValueError, match=message):
        mod.normalize_validation("invoices", raw, INVOICE_FIELDS)


def test_validation_defaults(mod):
    assert mod.normalize_validation("a", {}, INVOICE_FIELDS) == {"required_fields": [], "reconcile": None, "min_pass_rate": 0.9}
    assert mod.normalize_validation("a", None, INVOICE_FIELDS) is None


def test_bad_validation_fails_at_build_time_not_at_run_time(mod):
    component = mod.StructuredDocumentExtractorComponent(
        asset_name="invoices", upstream_asset_key="docs_in", input_column="local_path", document_type="custom",
        output_fields=INVOICE_FIELDS, validation={"required_fields": ["ghost"]},
    )
    with pytest.raises(ValueError, match="not in output_fields"):
        component.build_defs(load_context=None)


@pytest.mark.parametrize("value,expected", [
    (12, 12.0), (12.5, 12.5), ("$1,250.50", 1250.5), ("€ 7", 7.0), ("-3.25", -3.25), (" 4 ", 4.0),
    (None, None), (True, None), ("n/a", None), ("12abc", None), ("", None), (float("nan"), None),
])
def test_money_parsing(mod, value, expected):
    assert mod._to_number(value) == expected
