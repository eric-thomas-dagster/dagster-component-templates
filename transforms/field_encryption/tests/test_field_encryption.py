"""Real round-trip tests for FieldEncryptionComponent against the actual
installed aws-encryption-sdk library (never mocked) -- only the DataFrame
input/output boundary is test fixture data."""
import base64
import os

import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module, requires_esdk


@pytest.fixture
def mod():
    return load_component_module()


@pytest.fixture
def raw_key_env(monkeypatch):
    key_b64 = base64.b64encode(os.urandom(32)).decode()
    monkeypatch.setenv("TEST_FIELD_ENCRYPTION_KEY", key_b64)
    return "TEST_FIELD_ENCRYPTION_KEY"


def _source_asset(df: pd.DataFrame, name: str = "source_rows"):
    @dg.asset(name=name)
    def _a() -> pd.DataFrame:
        return df
    return _a


@requires_esdk
def test_encrypt_then_decrypt_round_trips_all_types(mod, raw_key_env):
    df = pd.DataFrame({
        "ssn": ["123-45-6789", "987-65-4321"],
        "age": [34, 61],
        "balance": [1050.75, -12.5],
        "is_active": [True, False],
        "signup_at": [pd.Timestamp("2024-01-15T10:30:00"), pd.Timestamp("2026-06-01T00:00:00")],
        "note": [None, "has a value"],
        "other": ["keep me", "also keep me"],
    })
    src = _source_asset(df)

    enc = mod.FieldEncryptionComponent(
        asset_name="encrypted_rows",
        upstream_asset_key="source_rows",
        mode="encrypt",
        fields=["ssn", "age", "balance", "is_active", "signup_at", "note"],
        key_source="raw_key",
        raw_key_env_var=raw_key_env,
        key_id="test-key",
    )
    enc_defs = enc.build_defs(context=None)
    result = dg.materialize([src, *enc_defs.assets])
    assert result.success
    encrypted_df = result.asset_value(dg.AssetKey("encrypted_rows"))

    # Real encryption happened: ciphertext is not the plaintext, and two
    # different plaintext rows (age=34 vs 61) must not produce identical
    # ciphertext.
    assert encrypted_df["ssn"].iloc[0] != df["ssn"].iloc[0]
    assert encrypted_df["age"].iloc[0] != encrypted_df["age"].iloc[1]
    assert encrypted_df["other"].iloc[0] == "keep me"  # untouched column
    assert pd.isna(encrypted_df["note"].iloc[0])  # null passes through, not encrypted

    enc_src = _source_asset(encrypted_df, name="encrypted_rows")
    dec = mod.FieldEncryptionComponent(
        asset_name="decrypted_rows",
        upstream_asset_key="encrypted_rows",
        mode="decrypt",
        fields=["ssn", "age", "balance", "is_active", "signup_at", "note"],
        key_source="raw_key",
        raw_key_env_var=raw_key_env,
        key_id="test-key",
    )
    dec_defs = dec.build_defs(context=None)
    result2 = dg.materialize([enc_src, *dec_defs.assets])
    assert result2.success
    decrypted_df = result2.asset_value(dg.AssetKey("decrypted_rows"))

    # Value AND type preserved -- pandas upcasts a reassigned column to a
    # numpy dtype (np.int64/np.bool_), so check types via the column dtype
    # rather than isinstance() on an individual scalar.
    assert decrypted_df["ssn"].tolist() == df["ssn"].tolist()
    assert decrypted_df["age"].tolist() == df["age"].tolist()
    assert pd.api.types.is_integer_dtype(decrypted_df["age"])
    assert decrypted_df["balance"].tolist() == df["balance"].tolist()
    assert pd.api.types.is_float_dtype(decrypted_df["balance"])
    assert decrypted_df["is_active"].tolist() == df["is_active"].tolist()
    assert pd.api.types.is_bool_dtype(decrypted_df["is_active"])
    assert pd.Timestamp(decrypted_df["signup_at"].iloc[0]) == df["signup_at"].iloc[0]
    assert pd.isna(decrypted_df["note"].iloc[0])
    assert decrypted_df["note"].iloc[1] == "has a value"


@requires_esdk
def test_wrong_key_fails_to_decrypt_raise_mode(mod, raw_key_env):
    df = pd.DataFrame({"secret": ["classified"]})
    src = _source_asset(df)
    enc = mod.FieldEncryptionComponent(
        asset_name="encrypted_rows2", upstream_asset_key="source_rows", mode="encrypt",
        fields=["secret"], key_source="raw_key", raw_key_env_var=raw_key_env, key_id="k1",
    )
    result = dg.materialize([src, *enc.build_defs(context=None).assets])
    encrypted_df = result.asset_value(dg.AssetKey("encrypted_rows2"))

    other_key_env = "TEST_FIELD_ENCRYPTION_WRONG_KEY"
    os.environ[other_key_env] = base64.b64encode(os.urandom(32)).decode()
    try:
        enc_src = _source_asset(encrypted_df, name="encrypted_rows2")
        dec = mod.FieldEncryptionComponent(
            asset_name="decrypted_rows2", upstream_asset_key="encrypted_rows2", mode="decrypt",
            fields=["secret"], key_source="raw_key", raw_key_env_var=other_key_env, key_id="k1",
            on_decrypt_error="raise",
        )
        with pytest.raises(ValueError, match="failed to decrypt column 'secret'"):
            dg.materialize([enc_src, *dec.build_defs(context=None).assets])
    finally:
        del os.environ[other_key_env]


@requires_esdk
def test_wrong_key_null_mode_counts_failures(mod, raw_key_env):
    df = pd.DataFrame({"secret": ["classified", "also classified"]})
    src = _source_asset(df)
    enc = mod.FieldEncryptionComponent(
        asset_name="encrypted_rows3", upstream_asset_key="source_rows", mode="encrypt",
        fields=["secret"], key_source="raw_key", raw_key_env_var=raw_key_env, key_id="k1",
    )
    result = dg.materialize([src, *enc.build_defs(context=None).assets])
    encrypted_df = result.asset_value(dg.AssetKey("encrypted_rows3"))

    other_key_env = "TEST_FIELD_ENCRYPTION_WRONG_KEY2"
    os.environ[other_key_env] = base64.b64encode(os.urandom(32)).decode()
    try:
        enc_src = _source_asset(encrypted_df, name="encrypted_rows3")
        dec = mod.FieldEncryptionComponent(
            asset_name="decrypted_rows3", upstream_asset_key="encrypted_rows3", mode="decrypt",
            fields=["secret"], key_source="raw_key", raw_key_env_var=other_key_env, key_id="k1",
            on_decrypt_error="null",
        )
        result2 = dg.materialize([enc_src, *dec.build_defs(context=None).assets])
        assert result2.success
        decrypted_df = result2.asset_value(dg.AssetKey("decrypted_rows3"))
        assert decrypted_df["secret"].isna().all()
    finally:
        del os.environ[other_key_env]


@requires_esdk
def test_encryption_context_binds_ciphertext_to_its_column(mod, raw_key_env):
    """Swapping ciphertext between two differently-named columns must fail
    to decrypt -- proves the per-field encryption-context AAD binding is
    actually enforced, not just documented."""
    df = pd.DataFrame({"col_a": ["value-a"], "col_b": ["value-b"]})
    src = _source_asset(df)
    enc = mod.FieldEncryptionComponent(
        asset_name="encrypted_rows4", upstream_asset_key="source_rows", mode="encrypt",
        fields=["col_a", "col_b"], key_source="raw_key", raw_key_env_var=raw_key_env, key_id="k1",
    )
    result = dg.materialize([src, *enc.build_defs(context=None).assets])
    encrypted_df = result.asset_value(dg.AssetKey("encrypted_rows4"))

    swapped = encrypted_df.copy()
    swapped["col_a"], swapped["col_b"] = encrypted_df["col_b"].iloc[0], encrypted_df["col_a"].iloc[0]
    enc_src = _source_asset(swapped, name="encrypted_rows4")
    dec = mod.FieldEncryptionComponent(
        asset_name="decrypted_rows4", upstream_asset_key="encrypted_rows4", mode="decrypt",
        fields=["col_a", "col_b"], key_source="raw_key", raw_key_env_var=raw_key_env, key_id="k1",
        on_decrypt_error="raise",
    )
    with pytest.raises(ValueError, match="encryption_context mismatch"):
        dg.materialize([enc_src, *dec.build_defs(context=None).assets])


def test_upstream_asset_key_and_source_mutually_exclusive(mod):
    kwargs = dict(asset_name="out", mode="encrypt", fields=["x"], key_source="raw_key", raw_key_env_var="X")
    with pytest.raises(ValueError, match="set exactly one"):
        mod.FieldEncryptionComponent(**kwargs).build_defs(context=None)


def test_kms_mode_requires_kms_key_ids(mod):
    kwargs = dict(
        asset_name="out", upstream_asset_key="src", mode="encrypt", fields=["x"], key_source="kms",
    )
    with pytest.raises(ValueError, match="kms_key_ids"):
        mod.FieldEncryptionComponent(**kwargs).build_defs(context=None)


def test_raw_key_mode_requires_raw_key_env_var(mod):
    kwargs = dict(
        asset_name="out", upstream_asset_key="src", mode="encrypt", fields=["x"], key_source="raw_key",
    )
    with pytest.raises(ValueError, match="raw_key_env_var"):
        mod.FieldEncryptionComponent(**kwargs).build_defs(context=None)
