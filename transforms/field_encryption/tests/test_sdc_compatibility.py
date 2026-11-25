"""Tests for `compatibility: streamsets` -- the byte-for-byte-compatible mode with
StreamSets' "Encrypt and Decrypt Fields" processor. Real aws-encryption-sdk calls
throughout (never mocked), including a genuine cross-implementation interop check
against the migrating-streamsets-to-dagster skill's own reference implementation
(streamsets_to_dagster.runtime.encrypt_value/decrypt_value) -- not a live Data
Collector engine (none is available here), but the next best thing: proof that
this component's streamsets-mode ciphertext round-trips through an independently
written, already engine-verified implementation of the same real layout, and
vice versa."""
import base64
import importlib.util
import os
import pathlib

import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module, requires_esdk

SKILL_RUNTIME_PATH = pathlib.Path(
    "/Users/ericthomasii/9-16-demo/dagster-community-components-cli/"
    "skills/migrating-streamsets-to-dagster/src/streamsets_to_dagster/runtime.py"
)


@pytest.fixture
def mod():
    return load_component_module()


@pytest.fixture
def raw_key_env(monkeypatch):
    key_b64 = base64.b64encode(os.urandom(32)).decode()
    monkeypatch.setenv("TEST_SDC_KEY", key_b64)
    return "TEST_SDC_KEY", key_b64


def _source_asset(df: pd.DataFrame, name: str = "source_rows"):
    @dg.asset(name=name)
    def _a() -> pd.DataFrame:
        return df
    return _a


def _load_skill_runtime():
    import sys

    if not SKILL_RUNTIME_PATH.exists():
        pytest.skip(f"skill reference implementation not found at {SKILL_RUNTIME_PATH}")
    spec = importlib.util.spec_from_file_location("sdc_runtime_under_test", SKILL_RUNTIME_PATH)
    rt = importlib.util.module_from_spec(spec)
    # Must be registered in sys.modules BEFORE exec_module -- runtime.py's dataclasses
    # do a sys.modules[cls.__module__] lookup during class definition that fails
    # otherwise (same fix other components' tests needed this session).
    sys.modules["sdc_runtime_under_test"] = rt
    spec.loader.exec_module(rt)
    return rt


@requires_esdk
def test_streamsets_mode_round_trips_all_types(mod, raw_key_env):
    key_env_var, _ = raw_key_env
    df = pd.DataFrame({
        "ssn": ["123-45-6789", "987-65-4321"],
        "age": [34, 61],
        "big": [12345678901, -12345678901],
        "balance": [1050.75, -12.5],
        "is_active": [True, False],
        "note": [None, "has a value"],
        "payload": [b"\x00\x01binary", b"more\xffbytes"],
    })
    src = _source_asset(df)

    enc = mod.FieldEncryptionComponent(
        asset_name="encrypted_rows", upstream_asset_key="source_rows", mode="encrypt",
        fields=["ssn", "age", "big", "balance", "is_active", "note", "payload"],
        key_source="raw_key", raw_key_env_var=key_env_var, key_id="k1",
        compatibility="streamsets", encryption_context={"purpose": "pii"},
    )
    result = dg.materialize([src, *enc.build_defs(context=None).assets])
    assert result.success
    encrypted_df = result.asset_value(dg.AssetKey("encrypted_rows"))

    # Ciphertext is raw bytes, not base64 text -- a real behavioral difference
    # from compatibility='native'.
    assert isinstance(encrypted_df["ssn"].iloc[0], (bytes, bytearray))
    assert encrypted_df["ssn"].iloc[0] != df["ssn"].iloc[0].encode()

    enc_src = _source_asset(encrypted_df, name="encrypted_source")
    dec = mod.FieldEncryptionComponent(
        asset_name="decrypted_rows", upstream_asset_key="encrypted_source", mode="decrypt",
        fields=["ssn", "age", "big", "balance", "is_active", "note", "payload"],
        key_source="raw_key", raw_key_env_var=key_env_var, key_id="k1",
        compatibility="streamsets", encryption_context={"purpose": "pii"},
    )
    result2 = dg.materialize([enc_src, *dec.build_defs(context=None).assets])
    assert result2.success
    decrypted_df = result2.asset_value(dg.AssetKey("decrypted_rows"))

    assert decrypted_df["ssn"].tolist() == df["ssn"].tolist()
    assert decrypted_df["age"].tolist() == df["age"].tolist()
    assert all(isinstance(v, int) for v in decrypted_df["age"])
    assert decrypted_df["big"].tolist() == df["big"].tolist()
    assert decrypted_df["balance"].tolist() == df["balance"].tolist()
    assert all(isinstance(v, float) for v in decrypted_df["balance"])
    assert decrypted_df["is_active"].tolist() == df["is_active"].tolist()
    assert decrypted_df["payload"].tolist() == df["payload"].tolist()
    assert decrypted_df["note"].iloc[0] is None


@requires_esdk
def test_streamsets_mode_does_not_add_field_context_entry(mod, raw_key_env):
    """Unlike compatibility='native', this mode must NOT bind ciphertext to the
    column name -- StreamSets never does, so adding it would break real interop.
    Proven here by successfully decrypting a value under a DIFFERENT column name
    than it was encrypted under."""
    key_env_var, _ = raw_key_env
    df = pd.DataFrame({"a": ["secret-value"]})
    enc = mod.FieldEncryptionComponent(
        asset_name="enc", upstream_asset_key="src", mode="encrypt", fields=["a"],
        key_source="raw_key", raw_key_env_var=key_env_var, key_id="k1", compatibility="streamsets",
    )
    result = dg.materialize([_source_asset(df, "src"), *enc.build_defs(context=None).assets])
    encrypted_df = result.asset_value(dg.AssetKey("enc"))

    renamed = encrypted_df.rename(columns={"a": "b"})
    dec = mod.FieldEncryptionComponent(
        asset_name="dec", upstream_asset_key="renamed_src", mode="decrypt", fields=["b"],
        key_source="raw_key", raw_key_env_var=key_env_var, key_id="k1", compatibility="streamsets",
    )
    result2 = dg.materialize([_source_asset(renamed, "renamed_src"), *dec.build_defs(context=None).assets])
    assert result2.success
    assert result2.asset_value(dg.AssetKey("dec"))["b"].iloc[0] == "secret-value"


@requires_esdk
def test_cross_interop_component_ciphertext_readable_by_skill_runtime(mod, raw_key_env):
    """Real cross-implementation proof: encrypt with THIS component's streamsets
    mode, decrypt with the skill's independently-written, engine-verified
    runtime.decrypt_value. No live Data Collector engine is available here, but
    this proves the two implementations agree on the wire format, which is the
    part under THIS component's control."""
    key_env_var, key_b64 = raw_key_env
    rt = _load_skill_runtime()

    df = pd.DataFrame({"s": ["hello"], "n": [42], "f": [2.5], "flag": [True]})
    enc = mod.FieldEncryptionComponent(
        asset_name="enc", upstream_asset_key="src", mode="encrypt",
        fields=["s", "n", "f", "flag"], key_source="raw_key", raw_key_env_var=key_env_var,
        key_id="k1", compatibility="streamsets", encryption_context={"purpose": "pii"},
    )
    result = dg.materialize([_source_asset(df, "src"), *enc.build_defs(context=None).assets])
    encrypted_df = result.asset_value(dg.AssetKey("enc"))

    for col, expected in [("s", "hello"), ("n", 42), ("f", 2.5), ("flag", True)]:
        got = rt.decrypt_value(
            bytes(encrypted_df[col].iloc[0]), raw_key_b64=key_b64, key_id="k1",
            context={"purpose": "pii"},
        )
        assert got == expected, f"column {col}: skill runtime decrypted {got!r}, expected {expected!r}"
        assert type(got) is type(expected)


@requires_esdk
def test_cross_interop_skill_runtime_ciphertext_readable_by_component(mod, raw_key_env):
    """Inverse direction: encrypt with the skill's runtime.encrypt_value, decrypt
    with this component's streamsets mode."""
    key_env_var, key_b64 = raw_key_env
    rt = _load_skill_runtime()

    blob_s = rt.encrypt_value("world", raw_key_b64=key_b64, key_id="k1", context={"purpose": "pii"})
    blob_n = rt.encrypt_value(7, raw_key_b64=key_b64, key_id="k1", context={"purpose": "pii"})
    blob_f = rt.encrypt_value(3.5, raw_key_b64=key_b64, key_id="k1", context={"purpose": "pii"})

    df = pd.DataFrame({"s": [blob_s], "n": [blob_n], "f": [blob_f]})
    dec = mod.FieldEncryptionComponent(
        asset_name="dec", upstream_asset_key="src", mode="decrypt",
        fields=["s", "n", "f"], key_source="raw_key", raw_key_env_var=key_env_var,
        key_id="k1", compatibility="streamsets", encryption_context={"purpose": "pii"},
    )
    result = dg.materialize([_source_asset(df, "src"), *dec.build_defs(context=None).assets])
    assert result.success
    decrypted_df = result.asset_value(dg.AssetKey("dec"))
    # A DataFrame column of homogeneous ints/floats upcasts to numpy int64/float64
    # on construction -- a pandas dtype-inference detail, not a decryption defect
    # (the skill's own decrypt_value, called directly in the other interop test,
    # does return plain Python int/float). Values, not container dtype, are what's
    # under test here.
    assert decrypted_df["s"].iloc[0] == "world"
    assert decrypted_df["n"].iloc[0] == 7
    assert decrypted_df["f"].iloc[0] == 3.5


@requires_esdk
def test_invalid_compatibility_value_rejected(mod, raw_key_env):
    key_env_var, _ = raw_key_env
    comp = mod.FieldEncryptionComponent(
        asset_name="x", upstream_asset_key="src", mode="encrypt", fields=["a"],
        key_source="raw_key", raw_key_env_var=key_env_var, key_id="k1", compatibility="bogus",
    )
    with pytest.raises(ValueError, match="compatibility"):
        comp.build_defs(context=None)
