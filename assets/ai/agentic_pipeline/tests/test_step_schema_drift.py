"""`steps` is a real discriminated union (StepUnion, 25 per-op Pydantic models) so
schema.json carries every op's fields/types/required/defaults/descriptions for the UI
and for an agent -- this test fails if the README's own per-op field tables and the
real Pydantic models disagree, in EITHER direction: a field the README documents that
the model doesn't have (a stale doc), or a field the model has that the README doesn't
mention (exactly the kind of drift that let 7 real ops ship fully undocumented before
this test existed)."""
import pathlib
import re

import pytest

README = pathlib.Path(__file__).resolve().parent.parent / "README.md"

# Fields common to every op (declared on _StepBase), never repeated in a per-op table.
_COMMON_FIELDS = {"id", "op", "source", "inputs", "when", "policy", "persona", "version"}

# op name -> the exact README section heading text ("### `op: X`") to find its table under.
_OP_HEADING = {op: f"`op: {op}`" for op in (
    "llm_call", "route", "conditional_route", "debate", "critique_loop", "synthesize",
    "mcp_call", "tool_use_loop", "handoff", "map", "extract", "classify", "reduce",
    "self_reflect", "sub_pipeline", "agent_call", "delegate", "invoke_component", "filter",
    "verify", "eval", "best_of_n", "cascade", "plan", "decide", "approve",
)}


def _readme_text():
    return README.read_text()


def _fields_in_section(text: str, heading: str) -> set:
    """Every backticked field name in the first markdown table found after `heading`,
    up to the next heading line. Handles a combined cell like `` `source` / `inputs` ``
    by splitting on '/'."""
    start = text.index(heading)
    # Up to the next '#'-prefixed heading line (### or ##), or end of file.
    rest = text[start + len(heading):]
    m = re.search(r"\n#{2,3} ", rest)
    section = rest[: m.start()] if m else rest
    fields = set()
    for line in section.splitlines():
        if not line.strip().startswith("|"):
            continue
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if not cells or not cells[0]:
            continue
        first = cells[0]
        if first in ("Field", "---") or set(first) <= {"-"}:
            continue
        parts = [p.strip().strip("`").strip() for p in first.split("/")]
        # A row documenting several sub-keys of ONE nested dict field is written as
        # "parent.sub_a` / `sub_b" (the parent prefix dropped after the first mention) --
        # that whole row is about `parent`, not independent top-level fields `sub_a`/`sub_b`.
        dotted_parents = {p.split(".", 1)[0] for p in parts if "." in p}
        if dotted_parents:
            fields |= dotted_parents
            continue
        for name in parts:
            if name and re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", name):
                fields.add(name)
    return fields


@pytest.mark.parametrize("op", sorted(_OP_HEADING))
def test_readme_table_matches_the_real_step_model(mod, op):
    heading = _OP_HEADING[op]
    text = _readme_text()
    assert heading in text, f"README has no '### {heading}' section for op {op!r}"
    readme_fields = _fields_in_section(text, heading)

    model_cls = mod._STEP_MODELS[op]
    model_fields = set(model_cls.model_fields) - _COMMON_FIELDS

    documented_but_not_on_model = readme_fields - model_fields - _COMMON_FIELDS
    assert not documented_but_not_on_model, (
        f"op {op!r}: README documents field(s) {sorted(documented_but_not_on_model)} "
        f"that aren't real fields on {model_cls.__name__} -- stale documentation."
    )

    on_model_but_undocumented = model_fields - readme_fields
    assert not on_model_but_undocumented, (
        f"op {op!r}: {model_cls.__name__} has field(s) {sorted(on_model_but_undocumented)} "
        f"the README's own '### {heading}' table never mentions -- undocumented drift."
    )


def test_every_op_in__OPS_has_a_step_model_and_a_readme_section(mod):
    assert set(mod._STEP_MODELS) == set(mod._OPS), (
        f"_STEP_MODELS vs _OPS mismatch: {set(mod._OPS) ^ set(mod._STEP_MODELS)}"
    )
    # _OP_HEADING itself is hand-maintained (a new op's name must be added here too, by hand) -- this catches a
    # new op added to _STEP_MODELS/_OPS without a corresponding entry here (which `missing` below, checking only
    # the ops ALREADY in _OP_HEADING, cannot: a key absent from the dict it iterates is invisible to it).
    assert set(_OP_HEADING) == set(mod._STEP_MODELS), (
        f"_OP_HEADING vs _STEP_MODELS mismatch -- a new op needs an entry added to _OP_HEADING in this test file "
        f"too, not just a model + README section: {set(mod._STEP_MODELS) ^ set(_OP_HEADING)}"
    )
    text = _readme_text()
    missing = [op for op, heading in _OP_HEADING.items() if heading not in text]
    assert not missing, f"ops with no README '### op: X' section at all: {missing}"
