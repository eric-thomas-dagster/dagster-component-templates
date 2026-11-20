"""Committed tests for EmailSendAssetComponent.

Mocks ONLY the registered resource's send_email(...) (the smtp_resource/sendgrid_resource
boundary); attachment file reading, dry_run, run_window, and metadata are exercised for real
through dg.materialize().
"""
import dagster as dg
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


class FakeEmailResource:
    def __init__(self):
        self.calls: list = []

    def send_email(self, **kwargs):
        self.calls.append(kwargs)


def _make_component(mod, **overrides):
    kwargs = dict(
        asset_key="notifications/nightly_close_complete",
        provider="smtp",
        resource_key="smtp_resource",
        to=["ops@example.com"],
        subject="Nightly close complete",
        body="The nightly close chain finished successfully.",
    )
    kwargs.update(overrides)
    return mod.EmailSendAssetComponent(**kwargs)


def _materialize(component, resource=None):
    asset_def = component.build_defs(context=None).assets[0]
    resources = {component.resource_key: resource} if resource is not None else {}
    result = dg.materialize([asset_def], resources=resources)
    return result, asset_def


def test_sends_via_the_registered_resource(mod):
    resource = FakeEmailResource()
    component = _make_component(mod)
    result, asset_def = _materialize(component, resource)

    assert result.success
    assert len(resource.calls) == 1
    call = resource.calls[0]
    assert call["to"] == "ops@example.com"
    assert call["subject"] == "Nightly close complete"
    assert call["cc"] is None
    assert call["bcc"] is None
    assert call["attachments"] is None

    md = result.asset_materializations_for_node(asset_def.op.name)[0].metadata
    assert md["dry_run"].value is False
    assert md["to"].value == ["ops@example.com"]
    assert md["attachment_count"].value == 0


def test_multiple_to_cc_bcc_joined_and_passed_through(mod):
    resource = FakeEmailResource()
    component = _make_component(
        mod, to=["a@example.com", "b@example.com"], cc=["cc@example.com"], bcc=["hidden@example.com"],
    )
    result, _ = _materialize(component, resource)
    assert result.success
    call = resource.calls[0]
    assert call["to"] == "a@example.com, b@example.com"
    assert call["cc"] == ["cc@example.com"]
    assert call["bcc"] == ["hidden@example.com"]


def test_attachment_paths_are_read_from_disk(mod, tmp_path):
    report = tmp_path / "report.csv"
    report.write_bytes(b"a,b\n1,2")
    resource = FakeEmailResource()
    component = _make_component(mod, attachment_paths=[str(report)])
    result, asset_def = _materialize(component, resource)

    assert result.success
    attachments = resource.calls[0]["attachments"]
    assert attachments == [("report.csv", b"a,b\n1,2", "text/csv")]
    md = result.asset_materializations_for_node(asset_def.op.name)[0].metadata
    assert md["attachment_count"].value == 1


def test_sendgrid_requires_from_addr_at_config_time(mod):
    with pytest.raises(Exception, match="requires from_addr"):
        _make_component(mod, provider="sendgrid", resource_key="sendgrid")


def test_sendgrid_with_from_addr_is_accepted(mod):
    resource = FakeEmailResource()
    component = _make_component(mod, provider="sendgrid", resource_key="sendgrid", from_addr="me@example.com")
    result, _ = _materialize(component, resource)
    assert result.success
    assert resource.calls[0]["from_addr"] == "me@example.com"


def test_dry_run_never_looks_up_the_resource(mod):
    component = _make_component(mod, dry_run=True)
    result, asset_def = _materialize(component, resource=None)  # no resources at all
    assert result.success
    md = result.asset_materializations_for_node(asset_def.op.name)[0].metadata
    assert md["dry_run"].value is True


def test_run_window_outside_the_window_skips_cleanly(mod):
    component = _make_component(mod, dry_run=True, run_window="00:00-00:01 UTC")
    result, asset_def = _materialize(component, resource=None)
    assert result.success  # skip, not fail
    assert result.asset_materializations_for_node(asset_def.op.name) == []


def test_run_window_covering_all_day_always_runs(mod):
    component = _make_component(mod, dry_run=True, run_window="00:00-23:59 UTC")
    result, asset_def = _materialize(component, resource=None)
    assert result.success
    assert len(result.asset_materializations_for_node(asset_def.op.name)) == 1
