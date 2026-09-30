"""Module-level graph_fn targets for LangGraphAgentComponent tests --
referenced via 'module.path:function_name' dotted strings, so they must be
real, importable module-level functions (not closures/lambdas)."""
from typing import Any, Dict

CALLS: Dict[str, Any] = {"build_count": 0}


def reset():
    CALLS["build_count"] = 0


def build_echo_graph(context):
    """A trivial, real LangGraph graph with zero LLM calls -- proves
    graph_fn mode invokes an ALREADY-BUILT graph object (built once per
    materialize by this function, then handed to the component to
    `.invoke()`), exercising real custom Python node logic that the
    `steps` YAML DSL has no way to express."""
    from langgraph.graph import StateGraph, END

    CALLS["build_count"] += 1

    def _shout(state: Dict[str, Any]) -> Dict[str, Any]:
        return {"final": str(state.get("input", "")).upper() + "!", "extra": state.get("extra")}

    graph = StateGraph(dict)
    graph.add_node("shout", _shout)
    graph.set_entry_point("shout")
    graph.add_edge("shout", END)
    return graph.compile()
