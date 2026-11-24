"""`document_intelligence_tools` ports `structured_document_extractor`'s own extraction core
(`build_extraction_messages`/`parse_extraction_response`/`extract_one`/`_extract_file_content`) byte-faithfully,
per this repo's self-contained-component convention (no cross-component imports -- see both files' own module
docstrings). No drift test protected these four functions between the two copies before this test was added.

`parse_extraction_response` is genuinely byte-identical between the two files (verified below via an exact
source-text comparison) -- it has no config surface that differs between the two components. The other three
functions are NOT byte-identical, on purpose and pre-dating this test: `document_intelligence_tools` drops
`field_regions`/`_crop_region_blocks` (a batch-asset, per-field bounding-box-crop feature that component's
config surface supports and this single-document MCP tool's does not), documented in both files' own module
docstrings. For those three, this test instead does a BEHAVIORAL comparison -- calling both copies with the
same inputs (minus the intentionally-dropped `field_regions` surface) and asserting identical outputs -- which
is exactly what actually matters: that the shared logic (additional_prompt steering, detect_selections'
selection-mark prompting/parsing, the resolved-content reuse for a refine pass, and file-content resolution)
behaves identically in both places.
"""
import ast
import importlib.util
import sys
import types
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
STRUCTURED_PATH = ROOT / "assets/ai/structured_document_extractor/component.py"
DOC_INTEL_PATH = ROOT / "assets/ai/document_intelligence_tools/component.py"

FUNCS = ("build_extraction_messages", "parse_extraction_response", "extract_one", "_extract_file_content")
# Byte-identical today: no config surface differs between the two copies for this one function.
SOURCE_IDENTICAL_FUNCS = ("parse_extraction_response",)


def _load(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture()
def structured_mod():
    return _load(STRUCTURED_PATH, "structured_document_extractor_for_drift_test")


@pytest.fixture()
def doc_intel_mod():
    return _load(DOC_INTEL_PATH, "document_intelligence_tools_for_drift_test")


def _func_source(path: Path, name: str) -> str:
    src = path.read_text()
    tree = ast.parse(src)
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == name:
            segment = ast.get_source_segment(src, node)
            assert segment is not None, f"{path}: could not extract source for {name}"
            return segment
    raise AssertionError(f"{path}: function {name!r} not found at module level")


def test_all_four_functions_exist_in_both_files():
    for name in FUNCS:
        _func_source(STRUCTURED_PATH, name)
        _func_source(DOC_INTEL_PATH, name)


@pytest.mark.parametrize("name", SOURCE_IDENTICAL_FUNCS)
def test_source_identical_functions_have_not_drifted(name):
    a = _func_source(STRUCTURED_PATH, name)
    b = _func_source(DOC_INTEL_PATH, name)
    assert a == b, f"{name} has drifted between structured_document_extractor and document_intelligence_tools"


@pytest.fixture()
def fake_litellm(monkeypatch):
    """Same fake-litellm convention both components' own tests use -- captures every completion() call and
    returns a canned response; swapping `state["response_text"]` steers subsequent calls."""
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


def test_extract_file_content_behaves_identically_on_a_real_text_file(structured_mod, doc_intel_mod, tmp_path):
    doc = tmp_path / "a.txt"
    doc.write_text("hello world, some real content")
    assert structured_mod._extract_file_content(str(doc)) == doc_intel_mod._extract_file_content(str(doc))


def test_extract_file_content_behaves_identically_on_a_real_image_file(structured_mod, doc_intel_mod, tmp_path):
    from PIL import Image

    img_path = tmp_path / "pic.png"
    Image.new("RGB", (12, 12), color=(10, 20, 30)).save(img_path)
    assert structured_mod._extract_file_content(str(img_path)) == doc_intel_mod._extract_file_content(str(img_path))


def _shared_build_extraction_messages_kwargs(**overrides):
    kwargs = dict(
        input_type="file",
        output_fields=["invoice_number", "total_amount"],
        doc_label="invoice",
        max_content_chars=20000,
        additional_prompt=None,
        detect_selections=False,
    )
    kwargs.update(overrides)
    return kwargs


def test_build_extraction_messages_identical_for_text_content(structured_mod, doc_intel_mod, tmp_path):
    doc = tmp_path / "doc.txt"
    doc.write_text("Invoice #1, total due $42.00")
    kwargs = _shared_build_extraction_messages_kwargs(additional_prompt="preserve signed amounts")
    a = structured_mod.build_extraction_messages(str(doc), **kwargs)
    b = doc_intel_mod.build_extraction_messages(str(doc), **kwargs)
    assert a == b


def test_build_extraction_messages_identical_for_image_content_with_detect_selections(structured_mod, doc_intel_mod, tmp_path):
    from PIL import Image

    img_path = tmp_path / "form.png"
    Image.new("RGB", (8, 8), color=(1, 2, 3)).save(img_path)
    kwargs = _shared_build_extraction_messages_kwargs(detect_selections=True, additional_prompt="a tax form")
    a = structured_mod.build_extraction_messages(str(img_path), **kwargs)
    b = doc_intel_mod.build_extraction_messages(str(img_path), **kwargs)
    assert a == b


def test_build_extraction_messages_identical_warn_behavior_for_text_plus_detect_selections(structured_mod, doc_intel_mod, tmp_path):
    doc = tmp_path / "doc.txt"
    doc.write_text("just some text, no image here")
    kwargs = _shared_build_extraction_messages_kwargs(detect_selections=True)

    warns_a, warns_b = [], []
    msgs_a = structured_mod.build_extraction_messages(str(doc), warn=warns_a.append, **kwargs)
    msgs_b = doc_intel_mod.build_extraction_messages(str(doc), warn=warns_b.append, **kwargs)
    assert msgs_a == msgs_b
    assert len(warns_a) == 1 and len(warns_b) == 1
    assert warns_a[0] == warns_b[0]


def test_extract_one_identical_first_pass_result(structured_mod, doc_intel_mod, fake_litellm, tmp_path):
    calls, state = fake_litellm
    state["response_text"] = '{"invoice_number": "INV-1", "total_amount": 42.0}'
    doc = tmp_path / "doc.txt"
    doc.write_text("Invoice #INV-1, total due $42.00")

    common = dict(
        input_type="file", output_fields=["invoice_number", "total_amount"], doc_label="invoice",
        model="gpt-4o", api_key="k", additional_prompt="be precise",
    )
    fields_a, raw_a = structured_mod.extract_one(str(doc), **common)
    fields_b, raw_b = doc_intel_mod.extract_one(str(doc), **common)
    assert fields_a == fields_b
    assert raw_a == raw_b
    assert len(calls) == 2  # one completion() call per module


def test_extract_one_identical_refine_result(structured_mod, doc_intel_mod, fake_litellm, tmp_path):
    _, state = fake_litellm
    doc = tmp_path / "doc.txt"
    doc.write_text("Invoice #INV-2, total due $99.00")

    common = dict(
        input_type="file", output_fields=["invoice_number", "total_amount"], doc_label="invoice",
        model="gpt-4o", api_key="k", refine=True,
    )
    state["response_text"] = '{"invoice_number": "INV-2", "total_amount": 99.0}'
    fields_a, raw_a = structured_mod.extract_one(str(doc), **common)
    state["response_text"] = '{"invoice_number": "INV-2", "total_amount": 99.0}'
    fields_b, raw_b = doc_intel_mod.extract_one(str(doc), **common)
    assert fields_a == fields_b
    assert raw_a == raw_b
