"""The manifest's agentic_pipeline `steps_schemas` must cover the component's ops and accept the shipped examples."""
import json
import re
from pathlib import Path

import pytest

jsonschema = pytest.importorskip("jsonschema")
yaml = pytest.importorskip("yaml")

HERE = Path(__file__).resolve().parent.parent
REPO = HERE.parents[2]
# ops whose schemas this test holds to the shipped examples (the reasoning/governance ops)
CHECKED_OPS = {"plan", "best_of_n", "cascade", "verify", "eval", "decide", "approve", "conditional_route", "map", "extract",
               "classify", "reduce", "self_reflect", "sub_pipeline", "agent_call"}
# the original per-op schema set; remaining component ops are a known documentation gap (see test below)
CORE_OPS = {"llm_call", "synthesize", "mcp_call", "route", "debate", "critique_loop", "tool_use_loop", "handoff",
            "delegate", "invoke_component"} | CHECKED_OPS


def _component_ops():
    src = (HERE / "component.py").read_text()
    block = re.search(r"^_OPS = \{(.*?)^\}", src, re.S | re.M).group(1)
    return set(re.findall(r'^\s*"(\w+)":', block, re.M))


def _schemas():
    manifest = json.loads((REPO / "manifest.json").read_text())
    comps = manifest["components"] if isinstance(manifest, dict) else manifest
    comp = next(c for c in comps if c.get("id") == "agentic_pipeline")
    return {s["title"]: s for s in comp["agent_hints"]["steps_schemas"]["oneOf"]}


def _example_steps():
    """(label, step) for every step found in README yaml blocks and examples/*/ yaml files."""
    docs = []
    for md in [HERE / "README.md", *HERE.glob("examples/*/README.md")]:
        for i, block in enumerate(re.findall(r"```ya?ml\n(.*?)```", md.read_text(), re.S)):
            docs.append((f"{md.relative_to(HERE)}#{i}", block))
    for y in [HERE / "example.yaml", *HERE.glob("examples/*/*.yaml")]:
        docs.append((str(y.relative_to(HERE)), y.read_text()))
    out = []
    for label, text in docs:
        try:
            loaded = list(yaml.safe_load_all(text))
        except yaml.YAMLError:
            continue  # prose-with-ellipsis snippets are not parseable examples

        def walk(node):
            if isinstance(node, dict):
                steps = node.get("steps")
                if isinstance(steps, list):
                    for s in steps:
                        if isinstance(s, dict) and "op" in s:
                            out.append((label, s))
                for v in node.values():
                    walk(v)
            elif isinstance(node, list):
                for v in node:
                    walk(v)
        for d in loaded:
            walk(d)
    return out


def test_core_ops_have_schemas():
    ops, schemas = _component_ops(), _schemas()
    assert CORE_OPS <= ops, CORE_OPS - ops
    assert not (CORE_OPS - set(schemas)), f"missing steps_schemas: {sorted(CORE_OPS - set(schemas))}"


def test_every_component_op_has_schema():
    missing = _component_ops() - set(_schemas())
    assert not missing, sorted(missing)


def test_shipped_examples_validate():
    schemas = _schemas()
    seen = set()
    for label, step in _example_steps():
        if step["op"] not in CHECKED_OPS:
            continue
        seen.add(step["op"])
        jsonschema.validate(step, schemas[step["op"]])
    assert {"cascade", "decide", "verify", "approve"} <= seen  # governed_invoice_matching


@pytest.mark.parametrize("op", sorted(CHECKED_OPS - {"approve"}))
def test_schema_rejects_unknown_field_and_wrong_op(op):
    schema = _schemas()[op]
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate({"id": "x", "op": op, "bogus_field": 1}, schema)
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate({"id": "x", "op": "llm_call"}, schema)


# Step dicts the op tests in test_reasoning_ops.py run for real: the schema must accept what the component accepts.
RUNTIME_STEPS = [
    {"id": "v", "op": "verify", "model": "m", "evidence_source": ["docs"], "on_fail": "fail"},
    {"id": "e", "op": "eval", "model": "m", "criteria": ["a", {"name": "b", "weight": 2}], "fail_below": 4.5},
    {"id": "b", "op": "best_of_n", "model": "m", "n": 2, "parallel": False, "verifier": {"kind": "regex", "pattern": "x"}},
    {"id": "c", "op": "cascade", "models": ["a", "b"], "accept": {"kind": "regex", "pattern": "ANSWER"}, "on_exhausted": "fail"},
    {"id": "p", "op": "plan", "model": "m", "max_replans": 1, "verifier": {"kind": "regex", "pattern": "^OK"},
     "executor": {"mcp_servers": [{"transport": "stdio"}]}},
    {"id": "d", "op": "decide", "model": "m", "outcomes": {"a": "x", "b": "y"}, "default_outcome": "a",
     "when": {"source": "s", "field": "outcome", "equals": "a"}, "policy": {"budget": {"max_llm_calls": 1}}},
]


@pytest.mark.parametrize("step", RUNTIME_STEPS, ids=lambda s: s["op"])
def test_runtime_accepted_steps_validate(step):
    jsonschema.validate(step, _schemas()[step["op"]])


def test_required_fields_enforced():
    s = _schemas()
    for op, bad in (("cascade", {}), ("decide", {})):
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate({"id": "x", "op": op, **bad}, s[op])
    with pytest.raises(jsonschema.ValidationError):  # verify needs evidence
        jsonschema.validate({"id": "x", "op": "verify", "model": "m"}, s["verify"])
