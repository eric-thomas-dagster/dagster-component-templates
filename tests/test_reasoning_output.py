"""Reasoning models (Qwen3, DeepSeek-R1 via Ollama/vLLM/OpenRouter...) inline `<think>...</think>`.

Left in the reply it breaks JSON parsing / classification / routing. The normalization block is inlined in every
LiteLLM-based component (no helper modules -- the CLI copies only component.py), so a drift test compares the copies
against the canonical block in assets/ai/agentic_pipeline.
"""
import ast
import importlib.util
import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
_BEGIN, _END = "# BEGIN reasoning-output normalization", "# END reasoning-output normalization"
REF = ROOT / "assets/ai/litellm_batch_completion/component.py"

# Components that must (also) CALL split_reasoning, not merely carry the block.
CHANGED = [
    "assets/ai/data_enricher", "assets/ai/image_captioner", "assets/ai/inference_provider_ab_test",
    "assets/ai/litellm_agent", "assets/ai/litellm_batch_completion", "assets/ai/litellm_function_calling",
    "assets/ai/litellm_inference_asset", "assets/ai/litellm_structured_output", "assets/ai/llm_evaluator",
    "assets/ai/llm_judge", "assets/ai/provider_ab_evaluator", "assets/ai/rag_pipeline", "assets/ai/sql_generator",
    "assets/source/synthetic_prompt_generator",
    "assets/ai/zero_shot_classifier", "assets/ai/text_classifier", "assets/ai/ticket_classifier",
    "assets/ai/sentiment_analyzer", "assets/ai/entity_extractor", "assets/ai/text_moderator",
]
# Tier-1 components that expose the opt-in include_reasoning field (text_classifier / ticket_classifier /
# sentiment_analyzer keep their own, differently-meant reasoning options; entity_extractor / text_moderator strip only).
WITH_FIELD = [c for c in CHANGED if c not in (
    "assets/ai/text_classifier", "assets/ai/ticket_classifier", "assets/ai/sentiment_analyzer",
    "assets/ai/entity_extractor", "assets/ai/text_moderator")]
TOOL_COMPONENTS = ["assets/ai/litellm_agent", "assets/ai/litellm_function_calling", "assets/ai/provider_ab_evaluator"]


def _block(p: Path) -> str:
    t = p.read_text()
    return t[t.index(_BEGIN): t.index(_END) + len(_END)]


def _files():
    for p in sorted(ROOT.glob("**/component.py")):
        parts = p.relative_to(ROOT).parts
        if ".venv" in parts or "site-packages" in parts or parts[0] == "dist":
            continue
        if _BEGIN in p.read_text():
            yield p


FILES = list(_files())


def _load(rel, name):
    spec = importlib.util.spec_from_file_location(name, ROOT / rel)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def m():
    return _load("assets/ai/litellm_batch_completion/component.py", "_ro_ref")


def test_covers_every_changed_component():
    have = {str(p.parent.relative_to(ROOT)) for p in FILES}
    assert set(CHANGED) <= have, sorted(set(CHANGED) - have)


def test_inlined_blocks_are_identical():
    ref = _block(REF)
    drift = [str(p.relative_to(ROOT)) for p in FILES if _block(p) != ref]
    assert not drift, "reasoning block differs from the reference copy:\n  " + "\n  ".join(drift)


def test_block_matches_agentic_pipeline_helpers_behaviour(m):
    """agentic_pipeline owns its own (structurally different) copy; the behaviour must agree."""
    ap = _load("assets/ai/agentic_pipeline/component.py", "_ro_ap")
    for s in ["<think>a</think>b", "<think>unclosed", "x</think>y", "plain", "", "<think>a</think>b<think>c</think>d"]:
        assert ap._split_reasoning(s) == m.split_reasoning(s), s


# ── split_reasoning ───────────────────────────────────────────────────

def test_closed_block(m):
    assert m.split_reasoning("<think>let me see</think>\n{\"a\": 1}") == ('{"a": 1}', "let me see")


def test_unclosed_block_means_no_answer_yet(m):
    assert m.split_reasoning("<think>still thinking, ran out of tok") == ("", "still thinking, ran out of tok")


def test_bare_closing_tag(m):
    assert m.split_reasoning("opening tag was in the prompt</think>The answer") == ("The answer", "opening tag was in the prompt")


def test_multiple_blocks(m):
    ans, why = m.split_reasoning("<think>one</think>A <think>two</think>B")
    assert ans == "A B"
    assert why == "one\n\ntwo"


def test_case_insensitive_and_multiline(m):
    assert m.split_reasoning("<THINK>x\ny</THINK>ok") == ("ok", "x\ny")


def test_no_tags_is_untouched(m):
    assert m.split_reasoning("  plain answer  ") == ("  plain answer  ", None)


@pytest.mark.parametrize("empty", [None, ""])
def test_empty(m, empty):
    assert m.split_reasoning(empty) == ("", None)


def test_empty_think_block(m):
    assert m.split_reasoning("<think></think>hi") == ("hi", None)


# ── parse_text_tool_calls ─────────────────────────────────────────────

def test_text_tool_call_recovery(m):
    text = 'Sure.\n<tool_call>\n{"name": "search", "arguments": {"q": "x"}}\n</tool_call>'
    rest, calls = m.parse_text_tool_calls(text)
    assert rest == "Sure."
    assert calls == [{"name": "search", "arguments": '{"q": "x"}'}]


def test_text_tool_call_parameters_key_and_string_args(m):
    _, calls = m.parse_text_tool_calls(
        '<tool_call>{"name": "a", "parameters": {"k": 1}}</tool_call><tool_call>{"name": "b", "arguments": "{}"}</tool_call>'
    )
    assert [c["name"] for c in calls] == ["a", "b"]
    assert json.loads(calls[0]["arguments"]) == {"k": 1}
    assert calls[1]["arguments"] == "{}"


def test_text_tool_call_garbage_is_ignored(m):
    text = "<tool_call>{not json}</tool_call> and <tool_call>{\"arguments\": {}}</tool_call>"
    assert m.parse_text_tool_calls(text) == (text, [])
    assert m.parse_text_tool_calls(None) == ("", [])


# ── ollama_chat_model ─────────────────────────────────────────────────

@pytest.mark.parametrize("model,tools,expected", [
    ("ollama/qwen3", True, "ollama_chat/qwen3"),
    ("Ollama/qwen3:8b", True, "ollama_chat/qwen3:8b"),
    ("ollama/qwen3", False, "ollama/qwen3"),
    ("ollama_chat/qwen3", True, "ollama_chat/qwen3"),
    ("gpt-4o", True, "gpt-4o"),
    ("openrouter/deepseek/deepseek-r1", True, "openrouter/deepseek/deepseek-r1"),
])
def test_ollama_chat_model(m, model, tools, expected):
    assert m.ollama_chat_model(model, tools) == expected


# ── every changed component actually calls split_reasoning ────────────

def _calls(rel, fn):
    tree = ast.parse((ROOT / rel / "component.py").read_text())
    for n in ast.walk(tree):
        if isinstance(n, ast.Call) and getattr(n.func, "id", getattr(n.func, "attr", None)) == fn:
            return True
    return False


@pytest.mark.parametrize("rel", CHANGED)
def test_component_calls_split_reasoning(rel):
    assert _calls(rel, "split_reasoning"), f"{rel} carries the block but never calls split_reasoning"


@pytest.mark.parametrize("rel", TOOL_COMPONENTS)
def test_tool_components_route_ollama_and_recover_text_calls(rel):
    assert _calls(rel, "ollama_chat_model")
    assert _calls(rel, "parse_text_tool_calls")


@pytest.mark.parametrize("rel", WITH_FIELD)
def test_include_reasoning_field_is_declared_everywhere(rel):
    src = (ROOT / rel / "component.py").read_text()
    assert "    include_reasoning: bool = Field(default=False" in src
    schema = json.loads((ROOT / rel / "schema.json").read_text())
    assert schema["attributes"]["include_reasoning"]["default"] is False
    assert "include_reasoning" in (ROOT / rel / "README.md").read_text()
