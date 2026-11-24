"""Real, committed tests for DocumentIntelligenceToolsComponent.

Mocks ONLY the paid LLM call (`litellm.completion`, via a fake `litellm` module injected into
sys.modules -- same convention as structured_document_extractor's own tests). Every other behavior --
real file reading (a real .txt file on disk), prompt construction, JSON-fence-stripping, field shaping,
the allow-list/error-shaping dispatcher, the Dagster resource wiring, and the MCP server's tool
registration -- runs for real.
"""
import sys
import types

import dagster as dg
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def fake_litellm(monkeypatch):
    """Captures every completion() call's messages; returns a canned response text. Tests mutate
    `state["response_text"]` to steer the reply."""
    calls = []
    state = {"response_text": '{"field_a": "value_a"}'}

    def fake_completion(model, messages, api_key=None, **kwargs):
        calls.append({"model": model, "messages": messages, "api_key": api_key, "kwargs": kwargs})
        return types.SimpleNamespace(
            choices=[types.SimpleNamespace(message=types.SimpleNamespace(content=state["response_text"]))]
        )

    fake_module = types.ModuleType("litellm")
    fake_module.completion = fake_completion
    monkeypatch.setitem(sys.modules, "litellm", fake_module)
    return calls, state


# ── extract ──────────────────────────────────────────────────────────────────────────────────────────────────────

def test_extract_happy_path_returns_correct_fields_dict(mod, fake_litellm, tmp_path):
    calls, state = fake_litellm
    state["response_text"] = '{"invoice_number": "INV-42", "total_amount": 110.0, "extra_field": "dropped"}'
    doc = tmp_path / "invoice.txt"
    doc.write_text("Invoice #INV-42, total due $110.00")

    tools = mod.DocumentIntelligenceTools(default_model="gpt-4o", api_key_env_var="OPENAI_API_KEY")
    result = tools.call("extract", {
        "file_ref": str(doc), "output_fields": ["invoice_number", "total_amount", "vendor"], "doc_label": "invoice",
    })

    assert "error" not in result, result
    assert result["fields"] == {"invoice_number": "INV-42", "total_amount": 110.0, "vendor": None}
    assert result["model"] == "gpt-4o"
    assert len(calls) == 1
    prompt_text = calls[0]["messages"][0]["content"]
    assert "invoice" in prompt_text
    assert "INV-42" in prompt_text


def test_extract_uses_a_per_call_model_override_and_reads_the_api_key_env_var(mod, fake_litellm, tmp_path, monkeypatch):
    calls, _ = fake_litellm
    monkeypatch.setenv("MY_OPENAI_KEY", "sk-test-123")
    doc = tmp_path / "doc.txt"
    doc.write_text("hello world")

    tools = mod.DocumentIntelligenceTools(default_model="gpt-4o", api_key_env_var="MY_OPENAI_KEY")
    result = tools.call("extract", {"file_ref": str(doc), "output_fields": ["field_a"], "model": "gpt-4o-mini"})

    assert "error" not in result, result
    assert result["model"] == "gpt-4o-mini"
    assert calls[0]["model"] == "gpt-4o-mini"
    assert calls[0]["api_key"] == "sk-test-123"


def test_extract_with_non_json_llm_reply_raises_clearly(mod, fake_litellm, tmp_path):
    _, state = fake_litellm
    state["response_text"] = "sorry, I can't do that"
    doc = tmp_path / "doc.txt"
    doc.write_text("some content")

    tools = mod.DocumentIntelligenceTools()
    result = tools.call("extract", {"file_ref": str(doc), "output_fields": ["field_a"]})

    assert "error" in result
    assert "extract failed" in result["error"]


def test_extract_requires_file_ref_and_output_fields(mod):
    tools = mod.DocumentIntelligenceTools()
    assert "file_ref" in tools.call("extract", {"file_ref": "", "output_fields": ["a"]})["error"]
    assert "output_fields" in tools.call("extract", {"file_ref": "x.txt", "output_fields": []})["error"]


def test_extract_one_module_function_matches_structured_document_extractor_shape(mod, fake_litellm, tmp_path):
    """extract_one's call shape (positional file_ref, keyword-only output_fields/model, returning
    (fields, raw)) is ported byte-faithfully from structured_document_extractor's own extract_one."""
    calls, state = fake_litellm
    state["response_text"] = '```json\n{"total": 12.5, "extra": "dropped"}\n```'
    doc = tmp_path / "a.txt"
    doc.write_text("Total due: 12.50")
    fields, raw = mod.extract_one(str(doc), output_fields=["total", "vendor"], model="gpt-4o", api_key="k")
    assert fields == {"total": 12.5, "vendor": None}
    assert "```" in raw
    assert calls[0]["kwargs"]["num_retries"] == 2


# ── suggest_schema ──────────────────────────────────────────────────────────────────────────────────────────────

def test_suggest_schema_happy_path_returns_field_list(mod, fake_litellm, tmp_path):
    _, state = fake_litellm
    state["response_text"] = (
        '[{"field": "invoice_number", "description": "The invoice identifier"}, '
        '{"field": "total_amount", "description": "The total amount due"}]'
    )
    doc = tmp_path / "sample_invoice.txt"
    doc.write_text("Invoice #42, total due $110.00")

    tools = mod.DocumentIntelligenceTools()
    result = tools.call("suggest_schema", {"sample_file_ref": str(doc), "doc_label": "invoice"})

    assert "error" not in result, result
    assert result["fields"] == [
        {"field": "invoice_number", "description": "The invoice identifier"},
        {"field": "total_amount", "description": "The total amount due"},
    ]


def test_suggest_schema_caps_at_max_fields(mod, fake_litellm, tmp_path):
    _, state = fake_litellm
    state["response_text"] = "[" + ", ".join(f'{{"field": "f{i}", "description": "d{i}"}}' for i in range(20)) + "]"
    doc = tmp_path / "sample.txt"
    doc.write_text("a document with lots of fields")

    tools = mod.DocumentIntelligenceTools()
    result = tools.call("suggest_schema", {"sample_file_ref": str(doc), "max_fields": 3})

    assert "error" not in result, result
    assert len(result["fields"]) == 3
    assert [f["field"] for f in result["fields"]] == ["f0", "f1", "f2"]


def test_suggest_schema_with_invalid_json_raises_clearly(mod, fake_litellm, tmp_path):
    _, state = fake_litellm
    state["response_text"] = "here are some fields: invoice_number, total_amount"
    doc = tmp_path / "sample.txt"
    doc.write_text("a document")

    tools = mod.DocumentIntelligenceTools()
    result = tools.call("suggest_schema", {"sample_file_ref": str(doc)})

    assert "error" in result
    assert "suggest_schema failed" in result["error"]


def test_suggest_schema_rejects_a_non_array_json_reply(mod, fake_litellm, tmp_path):
    _, state = fake_litellm
    state["response_text"] = '{"field": "not_an_array"}'
    doc = tmp_path / "sample.txt"
    doc.write_text("a document")

    tools = mod.DocumentIntelligenceTools()
    result = tools.call("suggest_schema", {"sample_file_ref": str(doc)})
    assert "error" in result


def test_suggest_schema_requires_sample_file_ref(mod):
    tools = mod.DocumentIntelligenceTools()
    result = tools.call("suggest_schema", {"sample_file_ref": ""})
    assert "sample_file_ref" in result["error"]


def test_parse_schema_suggestion_response_module_function_directly(mod):
    fields = mod.parse_schema_suggestion_response(
        '```json\n[{"field": "a", "description": "d1"}, {"field": "b"}]\n```', max_fields=12,
    )
    assert fields == [{"field": "a", "description": "d1"}, {"field": "b", "description": ""}]


def test_parse_schema_suggestion_response_raises_on_bad_json(mod):
    with pytest.raises(ValueError, match="did not return valid JSON"):
        mod.parse_schema_suggestion_response("not json at all", max_fields=12)


def test_parse_schema_suggestion_response_raises_on_non_array(mod):
    with pytest.raises(ValueError, match="expected a JSON array"):
        mod.parse_schema_suggestion_response('{"a": 1}', max_fields=12)


def test_parse_schema_suggestion_response_raises_on_malshaped_items(mod):
    with pytest.raises(ValueError, match="expected items shaped"):
        mod.parse_schema_suggestion_response('["just_a_string"]', max_fields=12)


# ── dispatcher: allow-list + unknown tool + size cap (same conventions as dagster_evidence_tools) ────────────────

def test_tool_names_respects_allow_list(mod):
    tools = mod.DocumentIntelligenceTools(allow=["extract"])
    assert tools.tool_names() == ["extract"]


def test_tool_not_in_allow_list_is_refused(mod, tmp_path):
    doc = tmp_path / "doc.txt"
    doc.write_text("x")
    tools = mod.DocumentIntelligenceTools(allow=["extract"])
    result = tools.call("suggest_schema", {"sample_file_ref": str(doc)})
    assert "not enabled" in result["error"]


def test_constructing_with_unknown_allow_entry_raises(mod):
    with pytest.raises(ValueError, match="unknown tool"):
        mod.DocumentIntelligenceTools(allow=["not_a_real_tool"])


def test_unknown_tool_returns_error_not_an_exception(mod):
    tools = mod.DocumentIntelligenceTools()
    result = tools.call("delete_everything")
    assert "unknown tool" in result["error"]


def test_result_over_max_chars_is_truncated_to_a_preview(mod, fake_litellm, tmp_path):
    _, state = fake_litellm
    state["response_text"] = '{"field_a": "' + ("x" * 10000) + '"}'
    doc = tmp_path / "doc.txt"
    doc.write_text("x")

    tools = mod.DocumentIntelligenceTools(max_result_chars=500)
    result = tools.call("extract", {"file_ref": str(doc), "output_fields": ["field_a"]})
    assert result["truncated"] is True
    assert "preview" in result


# ── component build_defs wiring ──────────────────────────────────────────────────────────────────────────────────

def test_build_defs_registers_the_resource_under_the_default_key(mod):
    component = mod.DocumentIntelligenceToolsComponent()
    defs = component.build_defs(context=None)
    assert "document_intelligence_tools" in defs.resources
    resource_def = defs.resources["document_intelligence_tools"]
    assert isinstance(resource_def, dg.ResourceDefinition)


def test_build_defs_registers_under_a_custom_resource_key(mod):
    component = mod.DocumentIntelligenceToolsComponent(resource_key="my_doc_tools")
    defs = component.build_defs(context=None)
    assert "my_doc_tools" in defs.resources
    assert "document_intelligence_tools" not in defs.resources


def test_build_defs_resource_resolves_to_a_working_toolset(mod, fake_litellm, tmp_path):
    doc = tmp_path / "doc.txt"
    doc.write_text("some content")
    component = mod.DocumentIntelligenceToolsComponent(default_model="gpt-4o-mini", api_key_env_var="OPENAI_API_KEY")
    defs = component.build_defs(context=None)

    @dg.asset(required_resource_keys={"document_intelligence_tools"})
    def _consumer(context):
        tools = context.resources.document_intelligence_tools
        return tools.call("extract", {"file_ref": str(doc), "output_fields": ["field_a"]})

    result = dg.materialize([_consumer], resources=dict(defs.resources))
    assert result.success


def test_build_defs_rejects_an_unknown_allow_entry(mod):
    component = mod.DocumentIntelligenceToolsComponent(allow=["not_a_real_tool"])
    with pytest.raises(ValueError, match="unknown tool"):
        component.build_defs(context=None)


def test_build_defs_with_mcp_server_resource_key_also_registers_a_stdio_resource(mod):
    component = mod.DocumentIntelligenceToolsComponent(mcp_server_resource_key="doc_tools_mcp")
    defs = component.build_defs(context=None)
    assert "document_intelligence_tools" in defs.resources
    assert "doc_tools_mcp" in defs.resources
    mcp_resource = defs.resources["doc_tools_mcp"]
    assert isinstance(mcp_resource, mod._McpServerShapeResource)
    assert mcp_resource.type == "stdio"
    dumped = mcp_resource.model_dump()
    assert dumped["env"]["DOCUMENT_INTELLIGENCE_TOOLS_DEFAULT_MODEL"] == "gpt-4o"


def test_get_form_config_is_editable(mod):
    form_config = mod.DocumentIntelligenceToolsComponent.get_form_config()
    assert form_config.editable is True


# ── MCP server tool registration ────────────────────────────────────────────────────────────────────────────────

def test_build_mcp_server_exposes_exactly_the_two_tools(mod):
    pytest.importorskip("mcp")
    tools = mod.DocumentIntelligenceTools()
    server = mod.build_mcp_server(tools)
    import asyncio
    listed = asyncio.run(server.list_tools())
    names = sorted(t.name for t in listed)
    assert names == ["extract", "suggest_schema"]


def test_build_mcp_server_respects_the_allow_list(mod):
    pytest.importorskip("mcp")
    tools = mod.DocumentIntelligenceTools(allow=["extract"])
    server = mod.build_mcp_server(tools)
    import asyncio
    listed = asyncio.run(server.list_tools())
    names = sorted(t.name for t in listed)
    assert names == ["extract"]


def test_this_module_name_raises_a_clear_error_without_a_module_spec(mod, monkeypatch):
    monkeypatch.setattr(mod, "__spec__", None)
    with pytest.raises(RuntimeError, match="python -m"):
        mod._this_module_name()
