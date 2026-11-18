"""Real, committed tests for SnowflakeResourceComponent -- builds the real
dagster_snowflake.SnowflakeResource throughout; no network calls (connecting happens lazily,
only when a consuming asset actually uses the resource)."""
import pytest


def _resource(mod, **attrs):
    component = mod.SnowflakeResourceComponent(**attrs)
    defs = component.build_defs(context=None)
    return defs.resources[component.resource_key]


def test_plain_account_and_user_with_password(mod, monkeypatch):
    monkeypatch.setenv("SF_PW", "secret")
    resource = _resource(mod, account="xy12345", user="alice", password_env_var="SF_PW")
    assert resource.account == "xy12345"
    assert resource.user == "alice"
    assert resource.password.get_value() == "secret"


def test_account_env_var_and_user_env_var_resolve(mod, monkeypatch):
    monkeypatch.setenv("SF_ACCOUNT", "xy12345")
    monkeypatch.setenv("SF_USER", "alice")
    monkeypatch.setenv("SF_PW", "secret")
    resource = _resource(mod, account_env_var="SF_ACCOUNT", user_env_var="SF_USER", password_env_var="SF_PW")
    assert resource.account.get_value() == "xy12345"
    assert resource.user.get_value() == "alice"


def test_pat_env_var_sets_password_field(mod, monkeypatch):
    monkeypatch.setenv("SF_PAT", "pat-token-value")
    resource = _resource(mod, account="xy12345", user="alice", pat_env_var="SF_PAT")
    assert resource.password.get_value() == "pat-token-value"


def test_private_key_path_and_passphrase(mod, monkeypatch, tmp_path):
    key_path = str(tmp_path / "key.pem")
    monkeypatch.setenv("SF_KEY_PATH", key_path)
    monkeypatch.setenv("SF_KEY_PASS", "key-passphrase")
    resource = _resource(
        mod, account="xy12345", user="alice",
        private_key_path_env_var="SF_KEY_PATH", private_key_passphrase_env_var="SF_KEY_PASS",
    )
    assert resource.private_key_path.get_value() == key_path
    assert resource.private_key_password.get_value() == "key-passphrase"


def test_private_key_path_without_passphrase(mod, monkeypatch, tmp_path):
    key_path = str(tmp_path / "key.pem")
    monkeypatch.setenv("SF_KEY_PATH", key_path)
    resource = _resource(mod, account="xy12345", user="alice", private_key_path_env_var="SF_KEY_PATH")
    assert resource.private_key_path.get_value() == key_path
    assert resource.private_key_password is None


def test_requires_account_or_account_env_var(mod):
    with pytest.raises(Exception, match="account"):
        mod.SnowflakeResourceComponent(user="alice", password_env_var="SF_PW")


def test_requires_user_or_user_env_var(mod):
    with pytest.raises(Exception, match="user"):
        mod.SnowflakeResourceComponent(account="xy12345", password_env_var="SF_PW")


def test_requires_exactly_one_auth_method_zero_set(mod):
    with pytest.raises(Exception, match="exactly one"):
        mod.SnowflakeResourceComponent(account="xy12345", user="alice")


def test_requires_exactly_one_auth_method_two_set(mod):
    with pytest.raises(Exception, match="exactly one"):
        mod.SnowflakeResourceComponent(account="xy12345", user="alice", password_env_var="SF_PW", pat_env_var="SF_PAT")
