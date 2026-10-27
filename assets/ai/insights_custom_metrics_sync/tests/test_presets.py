"""Presets stay honest: every preset key must still be emitted as a numeric MetadataValue by the component it names."""
import ast
import pathlib
import re

import pytest

from .conftest import load_component_module

M = load_component_module()
AI = pathlib.Path(__file__).resolve().parents[2]
SOURCE = {"agentic_pipeline": "agentic_pipeline", "context_engineering_pipeline": "context_engineering_pipeline", "receipts_sync": "context_receipt_bigquery_sync",
          "receipt_outcome_ingestion": "receipt_outcome_ingestion", "rag_pipeline": "rag_pipeline"}


# Keys a component emits through a runtime loop over a dict (the AST cannot see a literal for them); each is asserted at run time by that
# component's own tests (agentic_pipeline: test_insights_metrics_constant_matches_what_is_emitted checks them against a real run).
DYNAMIC = {"agentic_pipeline": {"agentic_spent_today_usd", "agentic_remaining_today_usd"}}


def _is_numeric_mv(node):
    return (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr in ("int", "float")
            and isinstance(node.func.value, (ast.Name, ast.Attribute))
            and getattr(node.func.value, "id", getattr(node.func.value, "attr", "")) == "MetadataValue")


def _key_pattern(node):
    """str constant -> exact regex; f-string -> regex with a capture group per placeholder; else None."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return re.compile(re.escape(node.value))
    if isinstance(node, ast.JoinedStr):
        parts = [re.escape(v.value) if isinstance(v, ast.Constant) else "(.+)" for v in node.values]
        return re.compile("".join(parts))
    return None


def emitted_numeric_keys(path):
    tree = ast.parse(path.read_text())
    consts = {n.value for n in ast.walk(tree) if isinstance(n, ast.Constant) and isinstance(n.value, str)}
    pats = []
    for n in ast.walk(tree):
        if isinstance(n, ast.Dict):
            pats += [_key_pattern(k) for k, v in zip(n.keys, n.values) if k is not None and _is_numeric_mv(v)]
        elif isinstance(n, ast.DictComp) and _is_numeric_mv(n.value):
            pats.append(_key_pattern(n.key))
        elif isinstance(n, ast.Assign) and _is_numeric_mv(n.value):
            pats += [_key_pattern(t.slice) for t in n.targets if isinstance(t, ast.Subscript)]
    return [p for p in pats if p is not None], consts


def _emitted(key, pats, consts):
    for p in pats:
        m = p.fullmatch(key)
        if m and all(g in consts for g in m.groups()):  # a dynamic part must be a literal that really appears in the source
            return True
    return False


@pytest.mark.parametrize("preset", sorted(SOURCE))
def test_every_preset_key_is_emitted_numeric(preset):
    pats, consts = emitted_numeric_keys(AI / SOURCE[preset] / "component.py")
    missing = [m["metadata_key"] for m in M.PRESETS[preset]
               if m["metadata_key"] not in DYNAMIC.get(preset, ()) and not _emitted(m["metadata_key"], pats, consts)]
    assert not missing, f"{preset}: keys not emitted as numeric MetadataValue: {missing}"


def test_checker_rejects_a_key_that_is_not_emitted():
    pats, consts = emitted_numeric_keys(AI / "rag_pipeline" / "component.py")
    assert not _emitted("agentic_cost_usd", pats, consts)
    assert not _emitted("n_steps_typo", pats, consts)
    assert not _emitted("rows_not_a_table", *emitted_numeric_keys(AI / "context_receipt_bigquery_sync" / "component.py"))


def test_unit_types_match_value_kind():
    # float-valued emissions must not be INTEGER (and vice versa for the obvious ones)
    by = {m["metadata_key"]: m["unit_type"] for ms in M.PRESETS.values() for m in ms}
    assert by["total_cost_usd"] == "FLOAT" and by["lag_seconds"] == "TIME_SECONDS" and by["total_latency_ms"] == "TIME_MS"


def test_shared_keys_have_identical_definitions():
    seen = {}
    for ms in M.PRESETS.values():
        for m in ms:
            assert seen.setdefault(m["metadata_key"], m) == m, f"{m['metadata_key']} defined differently in two presets"


def test_all_preset_entries_valid():
    for name, ms in M.PRESETS.items():
        assert M.resolve_metrics([name], [])


def test_agentic_preset_matches_component_literal_when_present():
    tree = ast.parse((AI / "agentic_pipeline" / "component.py").read_text())
    lit = None
    for n in tree.body:
        if isinstance(n, ast.Assign) and any(getattr(t, "id", "") == "INSIGHTS_METRICS" for t in n.targets):
            lit = ast.literal_eval(n.value)
    if lit is None:
        pytest.skip("agentic_pipeline does not define INSIGHTS_METRICS yet")
    ours = {m["metadata_key"]: m["unit_type"] for m in M.PRESETS["agentic_pipeline"]}
    theirs = {(m["metadata_key"] if isinstance(m, dict) else m[0]): (m["unit_type"] if isinstance(m, dict) else m[1]) for m in lit}
    assert ours == theirs
