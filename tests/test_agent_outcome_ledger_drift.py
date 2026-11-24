"""The "agent outcome ledger" block (write_outcome_ledger_entry/read_outcome_ledger_entries/
summarize_outcome_ledger) is inlined byte-identical into every component that touches it: the two
tool servers that WRITE entries (github_pr_tools/linear_ticket_tools) and the four agent components
that READ and summarize them into a companion outcome asset (openai_agent/anthropic_agent/
litellm_agent/gemini_agent). Kept in sync via a real AST comparison (not a text-diff of surrounding
BEGIN/END markers) so a fix to the ledger shape in one component lands everywhere."""
import ast
import pathlib

ROOT = pathlib.Path(__file__).resolve().parent.parent / "assets" / "ai"

COMPONENTS = [
    "github_pr_tools", "linear_ticket_tools",
    "openai_agent", "anthropic_agent", "litellm_agent", "gemini_agent",
]
SHARED_FUNCS = (
    "_outcome_ledger_file", "write_outcome_ledger_entry", "read_outcome_ledger_entries", "summarize_outcome_ledger",
    "_answer_decisions", "_answer_summary",
)


def _module_funcs(rel: str) -> dict:
    path = ROOT / rel / "component.py"
    src = path.read_text()
    tree = ast.parse(src)
    out = {}
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name in SHARED_FUNCS:
            out[node.name] = ast.get_source_segment(src, node)
    return out


def test_every_component_defines_the_full_shared_set():
    for rel in COMPONENTS:
        funcs = _module_funcs(rel)
        missing = set(SHARED_FUNCS) - set(funcs)
        assert not missing, f"{rel} is missing agent-outcome-ledger function(s): {missing}"


def test_components_agree_with_each_other():
    ref_rel = COMPONENTS[0]
    ref_funcs = _module_funcs(ref_rel)
    for rel in COMPONENTS[1:]:
        funcs = _module_funcs(rel)
        for name in SHARED_FUNCS:
            assert funcs[name] == ref_funcs[name], f"{rel}.{name} drifted from {ref_rel}'s copy"
