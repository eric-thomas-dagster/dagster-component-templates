"""Committed tests for SnowflakeStoredProcedureCallAssetComponent's retry_policy fields."""
import snowflake.connector
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def sf(monkeypatch):
    def fake_connect(**kwargs):
        raise AssertionError("not exercised in these tests")

    monkeypatch.setattr(snowflake.connector, "connect", fake_connect)
    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "env_acct")
    monkeypatch.setenv("SNOWFLAKE_USER", "env_user")
    monkeypatch.setenv("SNOWFLAKE_PASSWORD", "env_pw")


def _comp(mod, **kw):
    kw.setdefault("asset_key", "snowflake/procs/transform_customers")
    kw.setdefault("procedure_name", "TRANSFORM_CUSTOMERS")
    kw.setdefault("database", "DB")
    kw.setdefault("schema", "SC")
    kw.setdefault("account_env_var", "SNOWFLAKE_ACCOUNT")
    kw.setdefault("user_env_var", "SNOWFLAKE_USER")
    kw.setdefault("password_env_var", "SNOWFLAKE_PASSWORD")
    return mod.SnowflakeStoredProcedureCallAssetComponent(**kw)


def test_no_retry_policy_by_default(mod, sf):
    asset_def = _comp(mod).build_defs(None).assets[0]
    assert asset_def.op.retry_policy is None


def test_retry_policy_wires_through(mod, sf):
    asset_def = _comp(
        mod, retry_policy_max_retries=3, retry_policy_delay_seconds=5, retry_policy_backoff="linear",
    ).build_defs(None).assets[0]
    rp = asset_def.op.retry_policy
    assert rp is not None
    assert rp.max_retries == 3
    assert rp.delay == 5


def test_retry_policy_default_backoff_is_exponential(mod, sf):
    asset_def = _comp(mod, retry_policy_max_retries=2).build_defs(None).assets[0]
    from dagster import Backoff
    assert asset_def.op.retry_policy.backoff == Backoff.EXPONENTIAL
