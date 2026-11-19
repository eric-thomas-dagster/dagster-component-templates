"""report_tools_server's manifest-reading functions (`_report_run_dir`, `_report_manifest_path`,
`read_run_manifest`, `lookup_report_file`) are duplicated -- a read-only subset -- into every component that
reads its generated files back (slack_reply/teams_reply to deliver them, slack_chat_tools to post one
mid-conversation). Kept byte-identical (via a real AST comparison, not a text-diff of surrounding
BEGIN/END markers) so a fix to the manifest shape in the writer lands everywhere that reads it."""
import ast
import pathlib

ROOT = pathlib.Path(__file__).resolve().parent.parent / "assets"

WRITER = "ai/report_tools_server"
READERS = ["sinks/slack_reply", "sinks/teams_reply", "ai/slack_chat_tools"]
SHARED_FUNCS = ("_report_run_dir", "_report_manifest_path", "read_run_manifest", "lookup_report_file")


def _module_funcs(rel: str) -> dict:
    path = ROOT / rel / "component.py"
    src = path.read_text()
    tree = ast.parse(src)
    out = {}
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name in SHARED_FUNCS:
            out[node.name] = ast.get_source_segment(src, node)
    return out


def test_writer_defines_every_shared_function():
    funcs = _module_funcs(WRITER)
    missing = set(SHARED_FUNCS) - set(funcs)
    assert not missing, f"report_tools_server is missing: {missing}"


def test_reader_functions_match_the_writer():
    writer_funcs = _module_funcs(WRITER)
    for rel in READERS:
        reader_funcs = _module_funcs(rel)
        missing = set(SHARED_FUNCS) - set(reader_funcs)
        assert not missing, f"{rel} is missing report-manifest reader function(s): {missing}"
        for name in SHARED_FUNCS:
            assert reader_funcs[name] == writer_funcs[name], f"{rel}.{name} drifted from report_tools_server's own copy"


def test_readers_agree_with_each_other():
    ref_rel = READERS[0]
    ref_funcs = _module_funcs(ref_rel)
    for rel in READERS[1:]:
        funcs = _module_funcs(rel)
        for name in SHARED_FUNCS:
            assert funcs[name] == ref_funcs[name], f"{rel}.{name} drifted from {ref_rel}'s copy"
