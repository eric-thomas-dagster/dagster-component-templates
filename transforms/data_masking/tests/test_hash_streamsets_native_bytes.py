"""Tests for DataMaskingComponent's `encoding: streamsets` hash mode -- StreamSets Field Hasher's REAL
native-per-type-byte serialization (confirmed by reading com.streamsets.pipeline.lib.hashing.HashingUtil.
RecordFunnel.funnel() directly, Guava's PrimitiveSink), as opposed to `encoding` left unset, which hashes
every value's TEXT representation (this component's original, pre-existing behavior).

The int32-little-endian-42 case below is cross-checked against a reported real Data Collector 6.4.1 MD5
output for that exact value (9824a7030ce67cf3f0efe7529f0c6ecc) -- this test locks that specific value in,
not just internal self-consistency.
"""
import hashlib

import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module


@pytest.fixture
def mod():
    return load_component_module()


def _materialize_and_get_df(mod, rules, upstream_df, asset_name="out_asset"):
    comp = mod.DataMaskingComponent(asset_name=asset_name, upstream_asset_key="upstream_data", rules=rules)
    defs = comp.build_defs(context=None)

    @dg.asset(key=dg.AssetKey("upstream_data"))
    def upstream_data() -> pd.DataFrame:
        return upstream_df

    result = dg.materialize([upstream_data, *defs.assets])
    assert result.success
    return result.asset_value(dg.AssetKey(asset_name))


# ── streamsets_field_bytes: unit-level, no asset materialization ────────────────────────────────────────────────
def test_integer_42_matches_the_real_reported_engine_hash(mod):
    """The exact cross-check: a real Data Collector 6.4.1 MD5 of the INTEGER field 42 was reported as
    9824a7030ce67cf3f0efe7529f0c6ecc. int32 little-endian 42 is b'\\x2a\\x00\\x00\\x00' -- hashing that
    reproduces the reported value exactly, independent of this repo's own code."""
    data = mod.streamsets_field_bytes(42)
    assert data == b"\x2a\x00\x00\x00"
    assert hashlib.md5(data).hexdigest() == "9824a7030ce67cf3f0efe7529f0c6ecc"


def test_integer_vs_long_auto_inference_boundary(mod):
    assert mod.streamsets_field_bytes(2**31 - 1) == (2**31 - 1).to_bytes(4, "little", signed=True)
    assert mod.streamsets_field_bytes(2**31) == (2**31).to_bytes(8, "little", signed=True)
    assert mod.streamsets_field_bytes(-(2**31)) == (-(2**31)).to_bytes(4, "little", signed=True)
    assert mod.streamsets_field_bytes(-(2**31) - 1) == (-(2**31) - 1).to_bytes(8, "little", signed=True)


def test_double_is_8_byte_little_endian_ieee754(mod):
    import struct
    assert mod.streamsets_field_bytes(42.5) == struct.pack("<d", 42.5)


def test_boolean_is_one_byte(mod):
    assert mod.streamsets_field_bytes(True) == b"\x01"
    assert mod.streamsets_field_bytes(False) == b"\x00"


def test_string_is_utf8_text_unchanged(mod):
    assert mod.streamsets_field_bytes("hello") == b"hello"
    assert mod.streamsets_field_bytes("héllo") == "héllo".encode("utf-8")


def test_byte_array_passes_through_unchanged(mod):
    assert mod.streamsets_field_bytes(b"\x00\x01\xff") == b"\x00\x01\xff"


def test_null_is_the_guava_sentinel_byte(mod):
    assert mod.streamsets_field_bytes(None) == b"\x01"
    assert mod.streamsets_field_bytes(float("nan")) == b"\x01"


def test_explicit_field_type_overrides_auto_inference(mod):
    # A value that would auto-infer as INTEGER (4 bytes), pinned instead to LONG (8 bytes).
    assert mod.streamsets_field_bytes(42, field_type="long") == (42).to_bytes(8, "little", signed=True)
    assert mod.streamsets_field_bytes(42, field_type="short") == (42).to_bytes(2, "little", signed=True)
    assert mod.streamsets_field_bytes(42, field_type="byte") == (42).to_bytes(1, "little", signed=True)


def test_unknown_field_type_is_a_clear_error(mod):
    with pytest.raises(ValueError, match="field_type"):
        mod.streamsets_field_bytes(1, field_type="nope")


def test_datetime_is_8_byte_little_endian_epoch_millis(mod):
    import datetime as dt
    import struct
    value = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)
    expected_millis = int(value.timestamp() * 1000)
    assert mod.streamsets_field_bytes(value) == struct.pack("<q", expected_millis)
    # pandas Timestamp auto-infers the same way (duck-typed via .timestamp())
    assert mod.streamsets_field_bytes(pd.Timestamp(value)) == struct.pack("<q", expected_millis)


# ── full materialize() round trip through the component ─────────────────────────────────────────────────────────
def test_in_place_streamsets_encoding_on_an_integer_column(mod):
    df = pd.DataFrame({"id": pd.Series([42], dtype="int64")})
    out = _materialize_and_get_df(
        mod, [{"column": "id", "method": "hash", "algorithm": "md5", "encoding": "streamsets"}], df,
    )
    assert out["id"].iloc[0] == "9824a7030ce67cf3f0efe7529f0c6ecc"


def test_in_place_streamsets_encoding_differs_from_text_encoding_for_an_integer(mod):
    """The whole point: encoding unset hashes the TEXT "42", encoding='streamsets' hashes native int32
    bytes -- these must NOT be the same hash for the same input."""
    df = pd.DataFrame({"id": pd.Series([42], dtype="int64")})
    text_hash = _materialize_and_get_df(
        mod, [{"column": "id", "method": "hash", "algorithm": "md5"}], df.copy(),
    )["id"].iloc[0]
    native_hash = _materialize_and_get_df(
        mod, [{"column": "id", "method": "hash", "algorithm": "md5", "encoding": "streamsets"}], df.copy(),
    )["id"].iloc[0]
    assert text_hash == hashlib.md5(b"42").hexdigest()
    assert native_hash == "9824a7030ce67cf3f0efe7529f0c6ecc"
    assert text_hash != native_hash


def test_target_field_streamsets_encoding_on_a_double_column(mod):
    import struct
    df = pd.DataFrame({"amount": pd.Series([42.5], dtype="float64")})
    out = _materialize_and_get_df(
        mod,
        [{"column": "amount", "method": "hash", "algorithm": "sha256", "hash_mode": "target_field",
          "output_field": "amount_hash", "encoding": "streamsets"}],
        df,
    )
    expected = hashlib.sha256(struct.pack("<d", 42.5)).hexdigest()
    assert out["amount_hash"].iloc[0] == expected
    assert out["amount"].iloc[0] == 42.5  # original column untouched in target_field mode


def test_record_mode_streamsets_encoding_hashes_each_field_natively_with_separator(mod):
    df = pd.DataFrame({"id": pd.Series([42], dtype="int64"), "active": pd.Series([True])})
    out = _materialize_and_get_df(
        mod,
        [{"column": "id", "method": "hash", "algorithm": "md5", "hash_mode": "record",
          "columns": ["id", "active"], "output_field": "combined_hash",
          "field_separator": "|", "encoding": "streamsets"}],
        df,
    )
    expected_bytes = b"\x2a\x00\x00\x00" + b"|" + b"\x01" + b"|"
    assert out["combined_hash"].iloc[0] == hashlib.md5(expected_bytes).hexdigest()


def test_record_mode_rejects_an_explicit_field_type(mod):
    """field_type pins a single type; 'record' mode spans several columns of possibly different types, so
    pinning one type for all of them would silently mis-hash whichever columns aren't that type."""
    comp = mod.DataMaskingComponent(
        asset_name="out_asset",
        upstream_asset_key="upstream_data",
        rules=[{"column": "id", "method": "hash", "hash_mode": "record", "columns": ["id"],
                "output_field": "h", "encoding": "streamsets", "field_type": "integer"}],
    )
    defs = comp.build_defs(context=None)

    @dg.asset(key=dg.AssetKey("upstream_data"))
    def upstream_data() -> pd.DataFrame:
        return pd.DataFrame({"id": [1]})

    with pytest.raises(Exception, match="field_type is not supported with hash_mode='record'"):
        dg.materialize([upstream_data, *defs.assets])


def test_unknown_encoding_is_a_clear_error(mod):
    comp = mod.DataMaskingComponent(
        asset_name="out_asset",
        upstream_asset_key="upstream_data",
        rules=[{"column": "id", "method": "hash", "encoding": "bogus"}],
    )
    defs = comp.build_defs(context=None)

    @dg.asset(key=dg.AssetKey("upstream_data"))
    def upstream_data() -> pd.DataFrame:
        return pd.DataFrame({"id": [1]})

    with pytest.raises(Exception, match="unknown encoding"):
        dg.materialize([upstream_data, *defs.assets])


def test_murmur3_128_streamsets_encoding_uses_native_bytes_too(mod):
    pytest.importorskip("mmh3")
    import mmh3
    df = pd.DataFrame({"id": pd.Series([42], dtype="int64")})
    out = _materialize_and_get_df(
        mod, [{"column": "id", "method": "hash", "algorithm": "murmur3_128", "encoding": "streamsets"}], df,
    )
    assert out["id"].iloc[0] == mmh3.mmh3_x64_128_digest(b"\x2a\x00\x00\x00", 0).hex()
