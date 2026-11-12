"""This component inlines code copied from another component (no cross-component
imports -- `dagster_community_components.resources...` isn't a real submodule path,
confirmed directly): _embed_query/_query_vector_store, copied verbatim from
resources/context_receipt_store's own KnowledgeSearch helper. That copy must stay
byte-identical to its source, same convention as agentic_pipeline's own drift test
against rag_pipeline.

_McpServerShapeResource is also inlined from dagster_workflow_tools, but is NOT
byte-identical by design: dagster_workflow_tools' copy overrides model_dump() to inject
DAGSTER_HOME at run time (a quirk specific to its own stdio server needing that one env
var refreshed live); this component's settings are all fully known at build time, so it
has no such override. Checked structurally instead -- same field shape, so any
mcp_servers-aware consumer's .model_dump() still sees the exact dict shape it expects."""
import ast
import pathlib

HERE = pathlib.Path(__file__).resolve().parent.parent
CONTEXT_RECEIPT_STORE_PY = HERE.parent.parent.parent / "resources" / "context_receipt_store" / "component.py"
DAGSTER_WORKFLOW_TOOLS_PY = HERE.parent / "dagster_workflow_tools" / "component.py"


def _segments(path, names):
    src = path.read_text()
    out = {}
    for node in ast.parse(src).body:
        if isinstance(node, (ast.FunctionDef, ast.ClassDef)) and node.name in names:
            out[node.name] = ast.get_source_segment(src, node)
    return out


def test_embed_and_query_helpers_identical_to_context_receipt_store():
    names = {"_embed_query", "_query_vector_store"}
    mine = _segments(HERE / "component.py", names)
    theirs = _segments(CONTEXT_RECEIPT_STORE_PY, names)
    assert set(mine) == names and set(theirs) == names
    for n in sorted(names):
        assert mine[n] == theirs[n], f"{n} drifted from resources/context_receipt_store/component.py"


def test_mcp_server_shape_resource_same_field_shape_as_dagster_workflow_tools(mod):
    import importlib.util

    spec = importlib.util.spec_from_file_location("dagster_workflow_tools_for_drift_check", DAGSTER_WORKFLOW_TOOLS_PY)
    theirs_mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(theirs_mod)

    mine_fields = set(mod._McpServerShapeResource.model_fields)
    theirs_fields = set(theirs_mod._McpServerShapeResource.model_fields)
    assert mine_fields == theirs_fields == {"name", "type", "command", "url", "env", "headers", "headers_env"}
