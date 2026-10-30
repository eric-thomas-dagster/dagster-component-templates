"""Real, committed tests for three gaps found in review feedback:

1. No `metadata` field (asset_tags/owners/kinds already existed) -- added,
   merged into the asset spec's static metadata.
2. No `endpoint_url` field -- the clean seam for DynamoDB Local instead of a
   real AWS account. Added, passed through to boto3.resource.
3. No observation sensor -- changes to a table Dagster doesn't own were never
   observed. Added, opt-in, change-only-on-delta (table.scan(Select='COUNT'),
   not DynamoDB's free but ~6h-stale describe_table ItemCount).
"""
import dagster as dg

from .conftest import load_component_module

mod = load_component_module()
DynamodbReaderComponent = mod.DynamodbReaderComponent


def _component(**overrides):
    attrs = dict(asset_name="dynamo_orders", table_name="orders")
    attrs.update(overrides)
    return DynamodbReaderComponent(**attrs)


def test_metadata_field_merged_into_asset_spec():
    comp = _component(metadata={"owner_team": "payments"})
    defs = comp.build_defs(None)
    spec = next(iter(defs.resolve_all_asset_specs()))
    assert spec.metadata.get("owner_team") == "payments"


def test_no_metadata_field_set_does_not_crash():
    comp = _component()
    defs = comp.build_defs(None)
    spec = next(iter(defs.resolve_all_asset_specs()))
    assert spec.key == dg.AssetKey(["dynamo_orders"])


def test_polling_sensor_off_by_default():
    comp = _component()
    defs = comp.build_defs(None)
    assert not defs.sensors


class _FakeTable:
    def __init__(self, count):
        self.count = count

    def scan(self, **kwargs):
        return {"Count": self.count}


class _FakeDynamoDBResource:
    instances = []

    def __init__(self, count, **kwargs):
        self.count = count
        self.kwargs = kwargs
        _FakeDynamoDBResource.instances.append(self)

    def Table(self, name):
        return _FakeTable(self.count)


def test_endpoint_url_passed_to_boto3_resource(monkeypatch):
    import boto3

    _FakeDynamoDBResource.instances = []
    captured = {}

    def _fake_resource(service, **kwargs):
        captured.update(kwargs)
        return _FakeDynamoDBResource(10, **kwargs)

    monkeypatch.setattr(boto3, "resource", _fake_resource)
    comp = _component(endpoint_url="http://localhost:8000")
    defs = comp.build_defs(None)
    asset_def = next(iter(defs.assets))
    dg.materialize([asset_def], raise_on_error=True)
    assert captured.get("endpoint_url") == "http://localhost:8000"


def test_polling_sensor_emits_only_on_item_count_change(monkeypatch):
    import boto3

    comp = _component(polling_sensor=True)
    defs = comp.build_defs(None)
    sensor_def = next(iter(defs.sensors))

    monkeypatch.setattr(boto3, "resource", lambda service, **kw: _FakeDynamoDBResource(100, **kw))
    result1 = sensor_def(dg.build_sensor_context())
    assert result1.asset_events == []  # first sighting -- baseline only

    monkeypatch.setattr(boto3, "resource", lambda service, **kw: _FakeDynamoDBResource(100, **kw))
    result2 = sensor_def(dg.build_sensor_context(cursor=result1.cursor))
    assert result2.asset_events == []  # unchanged

    monkeypatch.setattr(boto3, "resource", lambda service, **kw: _FakeDynamoDBResource(150, **kw))
    result3 = sensor_def(dg.build_sensor_context(cursor=result2.cursor))
    assert len(result3.asset_events) == 1
    assert result3.asset_events[0].asset_key == dg.AssetKey(["dynamo_orders"])
    assert result3.asset_events[0].metadata["dynamodb/item_count"].value == 150
