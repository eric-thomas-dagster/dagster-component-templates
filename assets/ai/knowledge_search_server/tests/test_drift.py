"""This component inlines code copied from another component (no cross-component
imports -- `dagster_community_components.resources...` isn't a real submodule path,
confirmed directly): _embed_query/_query_vector_store, copied verbatim from
resources/context_receipt_store's own KnowledgeSearch helper. That copy must stay
byte-identical to its source, same convention as agentic_pipeline's own drift test
against rag_pipeline.

_McpServerShapeResource is also inlined from dagster_workflow_tools -- byte-identical,
including the live_env_passthrough field and its model_dump() override. An earlier version
of this component claimed all of its settings were "fully known at build time" and skipped
that field entirely; a live bug (API key env var values never reaching the spawned stdio
server) showed that claim was wrong -- env var NAMES are build-time config, but VALUES are
not, the exact same problem dagster_workflow_tools already had for DAGSTER_HOME."""
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


def test_mcp_server_shape_resource_passthrough_behaves_identically_to_dagster_workflow_tools(mod, monkeypatch):
    """Behavioral drift check, not textual: same fields, and model_dump() does the exact
    same live-env-passthrough merge given the same inputs -- this is what actually matters
    for a consumer, and would have caught the missing-passthrough bug directly."""
    import importlib.util

    spec = importlib.util.spec_from_file_location("dagster_workflow_tools_for_drift_check", DAGSTER_WORKFLOW_TOOLS_PY)
    theirs_mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(theirs_mod)

    assert set(mod._McpServerShapeResource.model_fields) == set(theirs_mod._McpServerShapeResource.model_fields)

    monkeypatch.setenv("DRIFT_CHECK_VAR", "secret-value")
    kwargs = dict(name="x", type="stdio", command=["python"], env={"A": "1"}, live_env_passthrough=["DRIFT_CHECK_VAR", "UNSET_VAR"])
    mine = mod._McpServerShapeResource(**kwargs)
    theirs = theirs_mod._McpServerShapeResource(**kwargs)
    assert mine.model_dump() == theirs.model_dump() == {
        "name": "x", "type": "stdio", "command": ["python"], "url": None,
        "env": {"A": "1", "DRIFT_CHECK_VAR": "secret-value"}, "headers": None, "headers_env": None,
    }
