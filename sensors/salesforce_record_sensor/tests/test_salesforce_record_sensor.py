"""Salesforce was the one sensor-family gap in this repo: stripe_event_sensor,
zendesk_ticket_sensor, jira_issue_sensor, linear_issue_sensor,
notion_database_sensor, servicenow_sensor, pagerduty_incident_sensor, and
github_event_sensor all exist; salesforce_ingestion and
salesforce_record_upsert both already exist too. This closes that gap.

Covers: credential validation (resource_key XOR full inline creds), the
resource_key path (mocking the resource's own soql() method -- a real
Dagster resource dependency, confirmed end-to-end against a live
build_sensor_context/SensorDefinition call, not just a direct function
call, since several sibling sensors in this repo declare
required_resource_keys but never actually consume the injected resource),
and the standalone OAuth path (mocking only requests.post/requests.get,
per this repo's test convention of mocking just the unavoidable external
call).
"""
import importlib.util
import pathlib

import dagster as dg
import pytest

_HERE = pathlib.Path(__file__).resolve().parent.parent
_COMPONENT_PY = _HERE / "component.py"
_spec = importlib.util.spec_from_file_location("salesforce_record_sensor_component", _COMPONENT_PY)
assert _spec is not None and _spec.loader is not None
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

SalesforceRecordSensorComponent = _mod.SalesforceRecordSensorComponent


def _build_component(**overrides):
    defaults = dict(sensor_name="sf_sensor", asset_keys=["opp_features"], sobject="Opportunity")
    defaults.update(overrides)
    return SalesforceRecordSensorComponent(**defaults)


def test_raises_without_resource_key_or_full_credentials():
    with pytest.raises(ValueError, match="resource_key"):
        _build_component()


def test_raises_with_partial_inline_credentials():
    with pytest.raises(ValueError, match="resource_key"):
        _build_component(username="u", password="p")  # missing consumer_key/secret/token


def test_resource_key_path_runs_the_resources_own_soql(monkeypatch):
    component = _build_component(resource_key="salesforce")
    defs = component.build_defs(None)
    sensor_def = defs.sensors[0]

    calls = []

    class _FakeSalesforceResource:
        def soql(self, query, *, max_records=None):
            calls.append((query, max_records))
            return iter([
                {"Id": "006AAA", "Name": "Big Deal", "LastModifiedDate": "2026-01-02T00:00:00.000Z"},
            ])

    @dg.asset(name="opp_features")
    def opp_features():
        return 1

    full_defs = dg.Definitions(
        assets=[opp_features],
        sensors=[sensor_def],
        resources={"salesforce": _FakeSalesforceResource()},
    )
    resolved = full_defs.resolve_sensor_def(sensor_def.name)
    ctx = dg.build_sensor_context(resources={"salesforce": _FakeSalesforceResource()})
    result = resolved(ctx)

    assert len(calls) == 1
    assert "FROM Opportunity" in calls[0][0]
    assert "LastModifiedDate >" in calls[0][0]
    assert isinstance(result, dg.SensorResult)
    assert len(result.run_requests) == 1
    assert result.run_requests[0].run_key == "006AAA"


def test_resource_key_path_skips_when_no_records():
    component = _build_component(resource_key="salesforce")
    sensor_def = component.build_defs(None).sensors[0]

    class _EmptyResource:
        def soql(self, query, *, max_records=None):
            return iter([])

    @dg.asset(name="opp_features")
    def opp_features():
        return 1

    full_defs = dg.Definitions(assets=[opp_features], sensors=[sensor_def], resources={"salesforce": _EmptyResource()})
    resolved = full_defs.resolve_sensor_def(sensor_def.name)
    ctx = dg.build_sensor_context(resources={"salesforce": _EmptyResource()})
    result = resolved(ctx)
    assert isinstance(result, dg.SkipReason)


def test_resource_key_path_applies_soql_filter_and_cursor():
    component = _build_component(resource_key="salesforce", soql_filter="StageName = 'Closed Won'")
    sensor_def = component.build_defs(None).sensors[0]

    captured = {}

    class _FakeResource:
        def soql(self, query, *, max_records=None):
            captured["query"] = query
            return iter([{"Id": "006BBB", "LastModifiedDate": "2026-03-04T00:00:00.000Z"}])

    @dg.asset(name="opp_features")
    def opp_features():
        return 1

    full_defs = dg.Definitions(assets=[opp_features], sensors=[sensor_def], resources={"salesforce": _FakeResource()})
    resolved = full_defs.resolve_sensor_def(sensor_def.name)
    ctx = dg.build_sensor_context(cursor="2026-01-01T00:00:00.000Z", resources={"salesforce": _FakeResource()})
    result = resolved(ctx)

    assert "StageName = 'Closed Won'" in captured["query"]
    assert "2026-01-01T00:00:00.000Z" in captured["query"]
    assert isinstance(result, dg.SensorResult)


def test_standalone_oauth_path_queries_rest_api(monkeypatch):
    component = _build_component(
        consumer_key="ck", consumer_secret="cs", username="u", password="p", security_token="tok",
    )
    sensor_def = component.build_defs(None).sensors[0]

    import requests

    login_calls = []
    query_calls = []

    class _FakeResp:
        def __init__(self, json_data):
            self._json = json_data

        def raise_for_status(self):
            pass

        def json(self):
            return self._json

    def fake_post(url, data=None, timeout=None):
        login_calls.append((url, data))
        return _FakeResp({"access_token": "tok123", "instance_url": "https://na1.salesforce.com"})

    def fake_get(url, params=None, headers=None, timeout=None):
        query_calls.append((url, params, headers))
        return _FakeResp({"records": [{"Id": "006CCC", "LastModifiedDate": "2026-02-02T00:00:00.000Z"}]})

    monkeypatch.setattr(requests, "post", fake_post)
    monkeypatch.setattr(requests, "get", fake_get)

    @dg.asset(name="opp_features")
    def opp_features():
        return 1

    full_defs = dg.Definitions(assets=[opp_features], sensors=[sensor_def])
    resolved = full_defs.resolve_sensor_def(sensor_def.name)
    ctx = dg.build_sensor_context()
    result = resolved(ctx)

    assert len(login_calls) == 1
    assert login_calls[0][1]["password"] == "ptok"  # password + security_token concatenated
    assert len(query_calls) == 1
    assert query_calls[0][2]["Authorization"] == "Bearer tok123"
    assert isinstance(result, dg.SensorResult)
    assert result.run_requests[0].run_key == "006CCC"
