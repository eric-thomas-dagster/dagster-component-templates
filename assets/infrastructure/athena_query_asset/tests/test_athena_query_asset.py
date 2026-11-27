"""Real, committed tests for AthenaQueryAssetComponent.

Mocks only the external boundary (a fake boto3 athena client's
`.start_query_execution()`/`.get_query_execution()`, matching the real
response shape verified against the botocore reference) -- everything else
(Jinja templating, polling loop, metadata extraction, failure handling,
Dagster asset wiring) is real.
"""
import dagster as dg
import pytest

from .conftest import load_component_module

mod = load_component_module()


class FakeAthenaClient:
    def __init__(self, states, data_scanned_bytes=12345, engine_ms=678, output_location=None, state_change_reason=None):
        self.start_calls = []
        self.poll_calls = 0
        self._states = list(states)  # sequence of states returned on successive polls
        self._data_scanned_bytes = data_scanned_bytes
        self._engine_ms = engine_ms
        self._output_location = output_location
        self._state_change_reason = state_change_reason

    def start_query_execution(self, **kwargs):
        self.start_calls.append(kwargs)
        return {"QueryExecutionId": "q-123"}

    def get_query_execution(self, QueryExecutionId):
        state = self._states[min(self.poll_calls, len(self._states) - 1)]
        self.poll_calls += 1
        return {
            "QueryExecution": {
                "Status": {"State": state, "StateChangeReason": self._state_change_reason},
                "Statistics": {
                    "DataScannedInBytes": self._data_scanned_bytes,
                    "EngineExecutionTimeInMillis": self._engine_ms,
                },
                "ResultConfiguration": {"OutputLocation": self._output_location} if self._output_location else {},
            }
        }


def _patch_client(monkeypatch, client):
    monkeypatch.setattr(mod, "_make_athena_client", lambda region_name, aws_profile: client)
    monkeypatch.setattr(mod.time, "sleep", lambda _s: None)


def _comp(**kw):
    args = dict(asset_name="out", query="SELECT 1", database="mydb", region_name="us-east-1")
    args.update(kw)
    return mod.AthenaQueryAssetComponent(**args)


def test_succeeded_query_materializes_with_metadata(monkeypatch):
    client = FakeAthenaClient(states=["RUNNING", "SUCCEEDED"], output_location="s3://bucket/results/q-123.csv")
    _patch_client(monkeypatch, client)
    comp = _comp()
    defs = comp.build_defs(None)
    result = dg.materialize(list(defs.assets))
    assert result.success

    mat = result.asset_materializations_for_node("out")[0]
    assert mat.metadata["query_execution_id"].text == "q-123"
    assert mat.metadata["state"].text == "SUCCEEDED"
    assert mat.metadata["data_scanned_bytes"].value == 12345
    assert mat.metadata["engine_execution_time_ms"].value == 678
    assert mat.metadata["output_location"].path == "s3://bucket/results/q-123.csv"

    assert client.start_calls[0]["QueryString"] == "SELECT 1"
    assert client.start_calls[0]["QueryExecutionContext"] == {"Database": "mydb"}
    assert client.poll_calls == 2


def test_failed_query_raises_with_state_change_reason(monkeypatch):
    client = FakeAthenaClient(states=["FAILED"], state_change_reason="SYNTAX_ERROR: line 1")
    _patch_client(monkeypatch, client)
    comp = _comp()
    defs = comp.build_defs(None)
    with pytest.raises(Exception, match="SYNTAX_ERROR"):
        dg.materialize(list(defs.assets))


def test_cancelled_query_raises(monkeypatch):
    client = FakeAthenaClient(states=["CANCELLED"], state_change_reason="USER_INITIATED")
    _patch_client(monkeypatch, client)
    comp = _comp()
    defs = comp.build_defs(None)
    with pytest.raises(Exception, match="CANCELLED"):
        dg.materialize(list(defs.assets))


def test_query_is_jinja_rendered_with_partition_date(monkeypatch):
    client = FakeAthenaClient(states=["SUCCEEDED"])
    _patch_client(monkeypatch, client)
    comp = _comp(
        query="SELECT * FROM t WHERE d = '{{ partition_date }}'",
        partition_type="daily",
        partition_start="2024-01-01",
    )
    defs = comp.build_defs(None)
    result = dg.materialize(list(defs.assets), partition_key="2024-01-02")
    assert result.success
    assert client.start_calls[0]["QueryString"] == "SELECT * FROM t WHERE d = '2024-01-02'"


def test_output_location_and_workgroup_passed_through(monkeypatch):
    client = FakeAthenaClient(states=["SUCCEEDED"])
    _patch_client(monkeypatch, client)
    comp = _comp(output_location="s3://bucket/out/", workgroup="my-wg")
    defs = comp.build_defs(None)
    dg.materialize(list(defs.assets))
    assert client.start_calls[0]["ResultConfiguration"] == {"OutputLocation": "s3://bucket/out/"}
    assert client.start_calls[0]["WorkGroup"] == "my-wg"


def test_timeout_raises_before_terminal_state(monkeypatch):
    # Every poll returns RUNNING; the clock advances past the deadline on the 2nd check.
    client = FakeAthenaClient(states=["RUNNING"])
    monkeypatch.setattr(mod, "_make_athena_client", lambda region_name, aws_profile: client)
    monkeypatch.setattr(mod, "time", mod.time)
    clock = {"t": 0.0}
    monkeypatch.setattr(mod.time, "monotonic", lambda: clock["t"])
    monkeypatch.setattr(mod.time, "sleep", lambda s: clock.__setitem__("t", clock["t"] + s + 0.01))
    comp = _comp(timeout_seconds=0.05, poll_interval_seconds=0.03)
    defs = comp.build_defs(None)
    with pytest.raises(Exception, match="did not reach a terminal state"):
        dg.materialize(list(defs.assets))
