"""Committed tests for MCPToolCallComponent partition handling / backfill_policy.

Only the network call (_call_tool, which talks to an MCP server) is replaced
by a fake that records the exact args it was called with; substitution,
looping and the real dg.materialize() path are all real.
"""
import dagster as dg
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def calls(mod, monkeypatch):
    seen = []

    async def fake_call_tool(*, log, server_cfg, tool_name, tool_args, parse_as):
        seen.append(tool_args)
        raw = '{"echo": "%s"}' % tool_args["day"]
        return {"is_error": False, "kind": "json", "value": {"echo": tool_args["day"]}, "raw": raw, "raw_size": len(raw)}

    monkeypatch.setattr(mod, "_call_tool", fake_call_tool)
    return seen


def _component(mod, **kw):
    return mod.MCPToolCallComponent(
        asset_name="mcp_out",
        server={"name": "s", "type": "http", "url": "http://localhost:1/mcp"},
        tool_name="t", tool_args={"day": "{partition_key}"},
        partition_type="daily", partition_start="2024-01-01", **kw,
    )


def _run(component, partition_key=None, tags=None):
    return dg.materialize(
        component.build_defs(None).assets, partition_key=partition_key, tags=tags or {},
        resources={"io_manager": dg.InMemoryIOManager()},
    )


def test_single_partition_substitutes_key(mod, calls):
    r = _run(_component(mod), partition_key="2024-01-03")
    assert r.success and calls == [{"day": "2024-01-03"}]
    assert r.output_for_node("mcp_out") == {"echo": "2024-01-03"}


def test_backfill_policy_wiring(mod):
    assert _component(mod, backfill_policy="single_run").build_defs(None).assets[0].backfill_policy == dg.BackfillPolicy.single_run()
    assert _component(mod).build_defs(None).assets[0].backfill_policy is None


def test_single_run_backfill_calls_tool_once_per_partition(mod, calls):
    """Before the fix has_partition_key was False for a range, so the tool was
    called ONCE with the literal unsubstituted '{partition_key}'."""
    r = _run(_component(mod, backfill_policy="single_run"),
             tags={"dagster/asset_partition_range_start": "2024-01-02",
                   "dagster/asset_partition_range_end": "2024-01-04"})
    assert r.success
    assert calls == [{"day": "2024-01-02"}, {"day": "2024-01-03"}, {"day": "2024-01-04"}]
    assert r.output_for_node("mcp_out") == {
        "2024-01-02": {"echo": "2024-01-02"},
        "2024-01-03": {"echo": "2024-01-03"},
        "2024-01-04": {"echo": "2024-01-04"},
    }
