"""Committed tests for SSHAssetComponent's cosmetic-partitioning fix +
backfill_policy support.

Unlike the other 23 cosmetic-partitioning components, this one runs shell
commands over SSH -- there's no DataFrame to filter, so the applicable
fix is real `{partition_key}` templating into `commands`/`working_dir`
(the component's own field docstring already promised "filtering /
templating" -- templating is the half that applies here), looped once per
key in a backfill range over a single reused SSH connection.

Mocks ONLY paramiko (not installed in this repo's own dev environment, and
this component does a module-level, non-lazy `import paramiko` -- see
conftest.py) -- partitions_def construction, {partition_key} templating,
and per-key looping are all exercised for real through dg.materialize().
"""
import dagster as dg
import pytest

from .conftest import FakeSSHClient, install_fake_paramiko, load_component_module


@pytest.fixture()
def mod(monkeypatch):
    install_fake_paramiko(monkeypatch, lambda: FakeSSHClient({}))
    return load_component_module()


def _make_component(mod, monkeypatch, **overrides):
    monkeypatch.setenv("FAKE_SSH_HOST", "ssh.example.com")
    monkeypatch.setenv("FAKE_SSH_USER", "demo")
    kwargs = dict(
        asset_name="ssh_job",
        host_env_var="FAKE_SSH_HOST",
        username_env_var="FAKE_SSH_USER",
        commands=["echo {partition_key}"],
    )
    kwargs.update(overrides)
    return mod.SSHAssetComponent(**kwargs)


def _materialize(mod, component, fake_client, monkeypatch, partition_key=None, tags=None):
    install_fake_paramiko(monkeypatch, lambda: fake_client)
    asset_def = component.build_defs(load_context=None).assets[0]
    return dg.materialize([asset_def], partition_key=partition_key, tags=tags or {})


def test_single_partition_runs_one_templated_command(mod, monkeypatch):
    fake_client = FakeSSHClient({"echo store_a": ([], 0)})
    component = _make_component(
        mod, monkeypatch,
        partition_type="static", partition_values="store_a,store_b,store_c",
    )
    result = _materialize(mod, component, fake_client, monkeypatch, partition_key="store_a")
    assert result.success
    assert fake_client.exec_calls == ["echo store_a"]


def test_backfill_policy_single_run_is_wired_onto_the_asset(mod, monkeypatch):
    component = _make_component(
        mod, monkeypatch,
        partition_type="static", partition_values="store_a,store_b,store_c",
        backfill_policy="single_run",
    )
    asset_def = component.build_defs(load_context=None).assets[0]
    assert asset_def.backfill_policy == dg.BackfillPolicy.single_run()


def test_backfill_policy_unset_by_default(mod, monkeypatch):
    component = _make_component(
        mod, monkeypatch,
        partition_type="static", partition_values="store_a,store_b,store_c",
    )
    asset_def = component.build_defs(load_context=None).assets[0]
    assert asset_def.backfill_policy is None


def test_single_run_backfill_runs_once_per_key_over_one_reused_connection(mod, monkeypatch):
    """Real bug before the fix: {partition_key} was documented as available
    for "filtering / templating" but nothing ever substituted it anywhere
    -- every partition (and every backfill range) ran the IDENTICAL
    unsubstituted command exactly once. Confirmed fixed: a 3-key
    single_run backfill now runs the templated command once per key, all
    over a SINGLE reused SSH connection (connect()/close() each called
    exactly once, not once per key)."""
    fake_client = FakeSSHClient({
        "echo store_a": ([], 0),
        "echo store_b": ([], 0),
        "echo store_c": ([], 0),
    })
    component = _make_component(
        mod, monkeypatch,
        partition_type="static", partition_values="store_a,store_b,store_c",
        backfill_policy="single_run",
    )
    result = _materialize(
        mod, component, fake_client, monkeypatch,
        tags={
            "dagster/asset_partition_range_start": "store_a",
            "dagster/asset_partition_range_end": "store_c",
        },
    )
    assert result.success
    assert fake_client.exec_calls == ["echo store_a", "echo store_b", "echo store_c"]
    assert fake_client.connected
    assert fake_client.closed
