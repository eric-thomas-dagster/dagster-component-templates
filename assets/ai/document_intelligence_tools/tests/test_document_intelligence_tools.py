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


# ── review-queue wiring (extract -> agent_review_queue's pending/*.json; opt-in via review_queue_approval_dir) ──

def test_review_queue_off_by_default_never_writes_anything_even_on_empty_fields(mod, fake_litellm, tmp_path):
    _, state = fake_litellm
    state["response_text"] = '{"field_a": null}'  # 100% null_rate -- would queue if the feature were on
    doc = tmp_path / "doc.txt"
    doc.write_text("some content")

    tools = mod.DocumentIntelligenceTools()  # review_queue_approval_dir unset
    result = tools.call("extract", {"file_ref": str(doc), "output_fields": ["field_a"]})

    assert "error" not in result, result
    assert result["queued_for_review"] is False
    assert "review_key" not in result
    assert not (tmp_path / "pending").exists()


def test_low_null_rate_does_not_queue(mod, fake_litellm, tmp_path):
    _, state = fake_litellm
    state["response_text"] = '{"a": "x", "b": "y", "c": "z"}'  # 0% null_rate
    doc = tmp_path / "doc.txt"
    doc.write_text("some content")
    approval_dir = tmp_path / "reviews"

    tools = mod.DocumentIntelligenceTools(review_queue_approval_dir=str(approval_dir))
    result = tools.call("extract", {"file_ref": str(doc), "output_fields": ["a", "b", "c"]})

    assert "error" not in result, result
    assert result["queued_for_review"] is False
    assert "review_key" not in result
    assert not (approval_dir / "pending").exists()


def test_high_null_rate_queues_and_pending_json_matches_schema_field_for_field(mod, fake_litellm, tmp_path):
    _, state = fake_litellm
    state["response_text"] = '{"a": null, "b": null, "c": "z"}'  # null_rate = 2/3 ≈ 0.67 > default 0.3
    doc = tmp_path / "doc.txt"
    doc.write_text("some content")
    approval_dir = tmp_path / "reviews"

    tools = mod.DocumentIntelligenceTools(review_queue_approval_dir=str(approval_dir))
    result = tools.call("extract", {"file_ref": str(doc), "output_fields": ["a", "b", "c"], "doc_label": "invoice"})

    assert "error" not in result, result
    assert result["queued_for_review"] is True
    assert result["review_reason"] == "null_rate"
    key = result["review_key"]
    assert len(key) == 16

    pending_files = list((approval_dir / "pending").glob("*.json"))
    assert len(pending_files) == 1
    assert pending_files[0].name == f"{key}.json"

    import json
    rec = json.loads(pending_files[0].read_text())
    assert set(rec.keys()) == {
        "step", "key", "requested_at", "fingerprint", "state", "reason", "reuse", "max_age_hours",
        "expires_at", "on_timeout", "summary", "preview", "approve_by_writing",
    }
    assert rec["step"] == "document_intelligence_tools.extract"
    assert rec["key"] == key
    assert rec["fingerprint"].startswith(key)
    assert len(rec["fingerprint"]) == 64  # full sha256 hex
    assert rec["state"] == "new"
    assert "null_rate" in rec["reason"]
    assert rec["reuse"] == "once"
    assert rec["max_age_hours"] is None
    assert rec["expires_at"] is None
    assert rec["on_timeout"] is None
    assert isinstance(rec["summary"], str) and rec["summary"]
    assert isinstance(rec["preview"], str) and rec["preview"]
    assert rec["approve_by_writing"]["path"] == f"{str(approval_dir).rstrip('/')}/{key}.json"
    assert rec["approve_by_writing"]["json"] == {
        "approved": True, "approver": "<you>", "reason": "<why>",
        "fingerprint": rec["fingerprint"], "approved_at": "<iso timestamp>",
    }
    # requested_at parses as ISO 8601 UTC
    from datetime import datetime
    datetime.fromisoformat(rec["requested_at"].replace("Z", "+00:00"))


def test_llm_error_queues_regardless_of_threshold(mod, tmp_path, monkeypatch):
    import sys
    import types

    def raising_completion(*args, **kwargs):
        raise RuntimeError("rate limited")

    fake_module = types.ModuleType("litellm")
    fake_module.completion = raising_completion
    monkeypatch.setitem(sys.modules, "litellm", fake_module)

    doc = tmp_path / "doc.txt"
    doc.write_text("some content")
    approval_dir = tmp_path / "reviews"

    # threshold at 1.0 (never trip on null_rate alone) -- the error must still queue
    tools = mod.DocumentIntelligenceTools(review_queue_approval_dir=str(approval_dir), low_confidence_null_rate_threshold=1.0)
    result = tools.call("extract", {"file_ref": str(doc), "output_fields": ["a"]})

    assert result["queued_for_review"] is True
    assert result["review_reason"] == "extraction_error"
    assert "error" in result

    import json
    pending_files = list((approval_dir / "pending").glob("*.json"))
    assert len(pending_files) == 1
    rec = json.loads(pending_files[0].read_text())
    assert "extraction_error" in rec["reason"]
    assert "rate limited" in rec["reason"]


def test_null_rate_module_function(mod):
    assert mod._null_rate({"a": None, "b": "", "c": "x"}, ["a", "b", "c"]) == pytest.approx(2 / 3)
    assert mod._null_rate({"a": "x"}, ["a"]) == 0.0
    assert mod._null_rate({}, []) == 0.0


# ── check_review_decision (read-only; mirrors agent_review_queue's approve-by-writing token shape) ───────────────

def test_check_review_decision_before_token_file_exists(mod, tmp_path):
    tools = mod.DocumentIntelligenceTools()
    result = tools.call("check_review_decision", {"approval_dir": str(tmp_path), "key": "abc123"})
    assert result == {"decided": False, "approved": None, "approver": None, "reason": None, "approved_at": None}


def test_check_review_decision_after_a_human_writes_the_token(mod, tmp_path):
    import json
    token = {"approved": True, "approver": "alice", "reason": "looks right", "fingerprint": "fp", "approved_at": "2026-01-01T00:00:00+00:00"}
    (tmp_path / "abc123.json").write_text(json.dumps(token))

    tools = mod.DocumentIntelligenceTools()
    result = tools.call("check_review_decision", {"approval_dir": str(tmp_path), "key": "abc123"})
    assert result == {
        "decided": True, "approved": True, "approver": "alice", "reason": "looks right",
        "approved_at": "2026-01-01T00:00:00+00:00",
    }


def test_check_review_decision_end_to_end_with_extract_queue_and_approve_pending_shape(mod, fake_litellm, tmp_path):
    """Simulates the real flow: extract() queues a review, then a human writes the approve_by_writing token
    verbatim (as agentic_pipeline's own approve_pending() does), and check_review_decision reports it."""
    _, state = fake_litellm
    state["response_text"] = '{"a": null}'
    doc = tmp_path / "doc.txt"
    doc.write_text("x")
    approval_dir = tmp_path / "reviews"

    tools = mod.DocumentIntelligenceTools(review_queue_approval_dir=str(approval_dir))
    extract_result = tools.call("extract", {"file_ref": str(doc), "output_fields": ["a"]})
    key = extract_result["review_key"]

    before = tools.call("check_review_decision", {"approval_dir": str(approval_dir), "key": key})
    assert before["decided"] is False

    import json
    pending = json.loads((approval_dir / "pending" / f"{key}.json").read_text())
    token = {**pending["approve_by_writing"]["json"], "approved": True, "approver": "bob", "reason": "fine", "approved_at": "2026-02-02T00:00:00+00:00"}
    (approval_dir / f"{key}.json").write_text(json.dumps(token))

    after = tools.call("check_review_decision", {"approval_dir": str(approval_dir), "key": key})
    assert after == {"decided": True, "approved": True, "approver": "bob", "reason": "fine", "approved_at": "2026-02-02T00:00:00+00:00"}


def test_check_review_decision_requires_approval_dir_and_key(mod):
    tools = mod.DocumentIntelligenceTools()
    assert "approval_dir" in tools.call("check_review_decision", {"approval_dir": "", "key": "k"})["error"]
    assert "key" in tools.call("check_review_decision", {"approval_dir": "/tmp/x", "key": ""})["error"]


def test_check_review_decision_with_fsspec_memory_uri(mod, fake_litellm, tmp_path):
    pytest.importorskip("fsspec")
    import uuid
    import json

    doc = tmp_path / "doc.txt"
    doc.write_text("x")
    _, state = fake_litellm
    state["response_text"] = '{"a": null}'
    approval_dir = f"memory://review-test-{uuid.uuid4().hex}"

    tools = mod.DocumentIntelligenceTools(review_queue_approval_dir=approval_dir)
    extract_result = tools.call("extract", {"file_ref": str(doc), "output_fields": ["a"]})
    assert extract_result["queued_for_review"] is True
    key = extract_result["review_key"]

    before = tools.call("check_review_decision", {"approval_dir": approval_dir, "key": key})
    assert before["decided"] is False

    import fsspec
    fs, root = fsspec.core.url_to_fs(approval_dir)
    pending_raw = fs.open(f"{root}/pending/{key}.json").read()
    pending = json.loads(pending_raw)
    token = {**pending["approve_by_writing"]["json"], "approved": False, "approver": "carol", "reason": "nope"}
    with fs.open(f"{root}/{key}.json", "w") as f:
        f.write(json.dumps(token))

    after = tools.call("check_review_decision", {"approval_dir": approval_dir, "key": key})
    assert after["decided"] is True
    assert after["approved"] is False
    assert after["approver"] == "carol"


# ── ALL_TOOLS / tool_names / call() dispatch include the new tool ─────────────────────────────────────────────────

def test_all_tools_includes_check_review_decision(mod):
    assert mod.ALL_TOOLS == ("extract", "suggest_schema", "check_review_decision")


def test_tool_names_includes_check_review_decision_by_default(mod):
    tools = mod.DocumentIntelligenceTools()
    assert tools.tool_names() == ["extract", "suggest_schema", "check_review_decision"]


def test_call_dispatches_check_review_decision(mod, tmp_path):
    tools = mod.DocumentIntelligenceTools()
    result = tools.call("check_review_decision", {"approval_dir": str(tmp_path), "key": "nope"})
    assert result["decided"] is False


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

def test_build_mcp_server_exposes_exactly_the_three_tools(mod):
    pytest.importorskip("mcp")
    tools = mod.DocumentIntelligenceTools()
    server = mod.build_mcp_server(tools)
    import asyncio
    listed = asyncio.run(server.list_tools())
    names = sorted(t.name for t in listed)
    assert names == ["check_review_decision", "extract", "suggest_schema"]


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
