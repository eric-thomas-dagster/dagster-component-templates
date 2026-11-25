"""Tests for DataMaskingComponent's StreamSets-Field-Hasher-parity hash options:
`algorithm` (md5/sha1/sha512/murmur3_128 in addition to the original default
sha256), `hash_mode` (in_place/target_field/record), and `field_separator`
for 'record' mode. Real materialize() calls against an in-memory upstream
asset -- no external boundary to mock here, every algorithm runs for real.
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
    comp = mod.DataMaskingComponent(
        asset_name=asset_name,
        upstream_asset_key="upstream_data",
        rules=rules,
    )
    defs = comp.build_defs(context=None)

    @dg.asset(key=dg.AssetKey("upstream_data"))
    def upstream_data() -> pd.DataFrame:
        return upstream_df

    result = dg.materialize([upstream_data, *defs.assets])
    assert result.success
    return result.asset_value(dg.AssetKey(asset_name))


def test_default_algorithm_unchanged_backward_compat(mod, monkeypatch):
    """No `algorithm`/`hash_mode` set at all -> byte-identical to the
    component's original (pre-this-change) behavior: salted, truncated
    16-char sha256 hex, in place."""
    monkeypatch.setenv("MASKING_SALT", "pepper")
    df = pd.DataFrame({"email": ["a@x.com", "b@x.com"]})
    out = _materialize_and_get_df(mod, [{"column": "email", "method": "hash"}], df)
    expected = hashlib.sha256(("pepper" + "a@x.com").encode()).hexdigest()[:16]
    assert out["email"].iloc[0] == expected
    assert len(out["email"].iloc[0]) == 16


@pytest.mark.parametrize(
    "algorithm,hasher",
    [
        ("md5", hashlib.md5),
        ("sha1", hashlib.sha1),
        ("sha512", hashlib.sha512),
    ],
)
def test_parity_algorithms_are_unsalted_and_untruncated(mod, monkeypatch, algorithm, hasher):
    """md5/sha1/sha512 must NOT apply the configured salt and must NOT
    truncate -- StreamSets' own Field Hasher does neither, and parity
    hashing exists specifically to match its output."""
    monkeypatch.setenv("MASKING_SALT", "pepper")
    df = pd.DataFrame({"email": ["a@x.com"]})
    out = _materialize_and_get_df(
        mod, [{"column": "email", "method": "hash", "algorithm": algorithm}], df
    )
    expected = hasher("a@x.com".encode()).hexdigest()
    assert out["email"].iloc[0] == expected  # unsalted
    assert out["email"].iloc[0] != hasher(("pepper" + "a@x.com").encode()).hexdigest()


def test_murmur3_128_empty_string_is_provably_zero(mod):
    """MurmurHash3_x64_128 of the empty string with seed 0 is mathematically
    guaranteed to be all-zero bytes (no blocks to process; fmix64(0) == 0
    by construction) -- this is ground truth independent of this repo or
    StreamSets, and proves the mmh3 wiring (algorithm selection, hex
    encoding, no accidental salt/prefix on the value) is correct."""
    mmh3 = pytest.importorskip("mmh3")
    df = pd.DataFrame({"val": [""]})
    out = _materialize_and_get_df(
        mod, [{"column": "val", "method": "hash", "algorithm": "murmur3_128"}], df
    )
    assert out["val"].iloc[0] == "0" * 32


def test_murmur3_128_matches_mmh3_digest_directly(mod):
    mmh3 = pytest.importorskip("mmh3")
    df = pd.DataFrame({"val": ["hello"]})
    out = _materialize_and_get_df(
        mod, [{"column": "val", "method": "hash", "algorithm": "murmur3_128"}], df
    )
    assert out["val"].iloc[0] == mmh3.mmh3_x64_128_digest(b"hello", 0).hex()


def test_hash_mode_target_field_leaves_original_untouched(mod):
    df = pd.DataFrame({"email": ["a@x.com"], "other": ["x"]})
    out = _materialize_and_get_df(
        mod,
        [{
            "column": "email", "method": "hash", "algorithm": "sha1",
            "hash_mode": "target_field", "output_field": "email_hash",
        }],
        df,
    )
    assert out["email"].iloc[0] == "a@x.com"  # untouched
    assert out["email_hash"].iloc[0] == hashlib.sha1(b"a@x.com").hexdigest()


def test_hash_mode_target_field_requires_output_field(mod):
    df = pd.DataFrame({"email": ["a@x.com"]})
    with pytest.raises(ValueError, match="output_field"):
        _materialize_and_get_df(
            mod, [{"column": "email", "method": "hash", "hash_mode": "target_field"}], df
        )


def test_hash_mode_record_concatenates_with_trailing_separator(mod):
    """StreamSets' Hash Record mode appends the separator AFTER every
    field's value, including the last -- not a between-fields join."""
    df = pd.DataFrame({"first": ["Jane"], "last": ["Doe"]})
    out = _materialize_and_get_df(
        mod,
        [{
            "method": "hash", "column": "first", "algorithm": "sha256",
            "hash_mode": "record", "columns": ["first", "last"],
            "field_separator": "|", "output_field": "record_hash",
        }],
        df,
    )
    expected = hashlib.sha256(b"Jane|Doe|").hexdigest()  # trailing sep included
    assert out["record_hash"].iloc[0] == expected
    assert out["first"].iloc[0] == "Jane"  # source columns untouched


def test_hash_mode_record_requires_output_field(mod):
    df = pd.DataFrame({"a": ["x"], "b": ["y"]})
    with pytest.raises(ValueError, match="output_field"):
        _materialize_and_get_df(
            mod,
            [{"method": "hash", "column": "a", "hash_mode": "record", "columns": ["a", "b"]}],
            df,
        )


def test_hash_mode_record_missing_column_raises(mod):
    df = pd.DataFrame({"a": ["x"]})
    with pytest.raises(ValueError, match="not found"):
        _materialize_and_get_df(
            mod,
            [{
                "method": "hash", "column": "a", "hash_mode": "record",
                "columns": ["a", "nonexistent"], "output_field": "h",
            }],
            df,
        )


def test_unknown_algorithm_raises(mod):
    df = pd.DataFrame({"a": ["x"]})
    with pytest.raises(ValueError, match="unknown hash algorithm"):
        _materialize_and_get_df(
            mod, [{"method": "hash", "column": "a", "algorithm": "not_a_real_algo"}], df
        )
