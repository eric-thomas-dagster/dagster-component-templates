"""Committed tests for Base64TransformComponent — no external boundary to mock,
everything runs for real (stdlib base64 only)."""
import base64

import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module

mod = load_component_module()
Base64TransformComponent = mod.Base64TransformComponent


def _upstream(rows):
    @dg.asset(name="payloads_raw")
    def _up() -> pd.DataFrame:
        return pd.DataFrame(rows)
    return _up


def _xform(**kw):
    kw.setdefault("asset_name", "payloads_out")
    kw.setdefault("upstream_asset_key", "payloads_raw")
    return Base64TransformComponent(**kw).build_defs(None).assets[0]


def _materialize(x, rows):
    up = _upstream(rows)
    return dg.materialize([up, x])


def _out(result, asset_name="payloads_out"):
    x = result.output_for_node(asset_name)
    return x.value if hasattr(x, "value") and hasattr(x, "metadata") else x


def test_encode_bytes_column():
    x = _xform(mode="encode", fields=[{"origin": "payload"}])
    r = _materialize(x, [{"payload": b"hello world"}])
    assert r.success
    out = _out(r)
    assert out.loc[0, "payload"] == base64.b64encode(b"hello world").decode("ascii")


def test_encode_string_field_errors_by_default():
    x = _xform(mode="encode", fields=[{"origin": "payload"}])
    with pytest.raises(ValueError, match="on_string_field"):
        _materialize(x, [{"payload": "plain text"}])


def test_encode_string_field_with_utf8_first_opt_in():
    x = _xform(
        mode="encode",
        fields=[{"origin": "payload", "result": "payload_b64"}],
        on_string_field="encode_utf8_first",
    )
    r = _materialize(x, [{"payload": "hello"}])
    assert r.success
    out = _out(r)
    assert out.loc[0, "payload_b64"] == base64.b64encode(b"hello").decode("ascii")
    assert out.loc[0, "payload"] == "hello"  # origin untouched, result is a new column


def test_decode_round_trip():
    encoded = base64.b64encode(b"round trip me").decode("ascii")
    x = _xform(mode="decode", fields=[{"origin": "payload"}])
    r = _materialize(x, [{"payload": encoded}])
    assert r.success
    out = _out(r)
    assert out.loc[0, "payload"] == b"round trip me"


def test_url_safe_variant():
    # bytes chosen so standard vs url-safe alphabets actually differ
    raw = bytes([251, 239, 190])
    x = _xform(mode="encode", fields=[{"origin": "payload"}], url_safe=True)
    r = _materialize(x, [{"payload": raw}])
    assert r.success
    out = _out(r)
    assert out.loc[0, "payload"] == base64.urlsafe_b64encode(raw).decode("ascii")
    assert out.loc[0, "payload"] != base64.b64encode(raw).decode("ascii")


def test_multiple_field_pairs_in_one_call():
    x = _xform(
        mode="encode",
        fields=[
            {"origin": "a"},
            {"origin": "b", "result": "b_out"},
        ],
    )
    r = _materialize(x, [{"a": b"aaa", "b": b"bbb"}])
    assert r.success
    out = _out(r)
    assert out.loc[0, "a"] == base64.b64encode(b"aaa").decode("ascii")
    assert out.loc[0, "b_out"] == base64.b64encode(b"bbb").decode("ascii")
    assert out.loc[0, "b"] == b"bbb"  # original untouched when result differs


def test_null_values_pass_through_as_null():
    x = _xform(mode="encode", fields=[{"origin": "payload"}])
    r = _materialize(x, [{"payload": None}])
    assert r.success
    out = _out(r)
    assert out.loc[0, "payload"] is None or out.loc[0, "payload"] != out.loc[0, "payload"]


def test_decode_invalid_base64_on_error_null():
    x = _xform(mode="decode", fields=[{"origin": "payload"}], on_error="null")
    r = _materialize(x, [{"payload": "not valid base64!!!"}])
    assert r.success
    out = _out(r)
    assert out.loc[0, "payload"] is None


def test_decode_invalid_base64_raises_by_default():
    x = _xform(mode="decode", fields=[{"origin": "payload"}])
    with pytest.raises(ValueError, match="base64 decode failed"):
        _materialize(x, [{"payload": "not valid base64!!!"}])


def test_requires_at_least_one_field():
    with pytest.raises(ValueError, match="fields"):
        _xform(mode="encode", fields=[])
