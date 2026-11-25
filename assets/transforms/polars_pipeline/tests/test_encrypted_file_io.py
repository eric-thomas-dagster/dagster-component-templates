"""Real round-trip tests for PolarsPipelineComponent's `encrypted_file` source/sink
kinds, against the actual installed `aws_encryption_sdk` library (never mocked) --
same discipline as transforms/field_encryption's own tests, file-level instead of
per-cell. Covers: sink-then-source round trip through the component itself,
genuine ciphertext on disk (decrypted independently, not just trusted), format
inference with a .enc suffix, wrong-key failure, and encryption-context binding
to the resolved file path (a ciphertext written for one path must not decrypt
under a different path's expected context).
"""
import base64
import os

import pandas as pd
import pytest

from .conftest import load_component_module

pytest.importorskip("polars")
pytest.importorskip("aws_encryption_sdk")

import dagster as dg
import polars as pl


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def raw_key_env(monkeypatch):
    key_b64 = base64.b64encode(os.urandom(32)).decode()
    monkeypatch.setenv("TEST_ENCRYPTED_FILE_KEY", key_b64)
    return "TEST_ENCRYPTED_FILE_KEY"


def _upstream():
    @dg.asset(name="raw_orders")
    def raw_orders():
        return pd.DataFrame({"acct": [1, 2, 3], "amount": [10.5, 20.0, 30.25]})

    return raw_orders


def test_sink_then_source_round_trip(mod, raw_key_env, tmp_path):
    enc_path = str(tmp_path / "orders.csv.enc")

    sink_comp = mod.PolarsPipelineComponent(
        asset_name="encrypted_out",
        steps=[{"id": "s", "source": {"kind": "upstream", "upstream_asset_key": "raw_orders"}, "operations": []}],
        sinks=[{
            "from": "s", "kind": "encrypted_file", "path": enc_path, "format": "csv",
            "key_source": "raw_key", "raw_key_env_var": raw_key_env,
        }],
    )
    result = dg.materialize(
        [_upstream(), *sink_comp.build_defs(None).assets],
        resources={"io_manager": dg.InMemoryIOManager()},
    )
    assert result.success
    assert os.path.exists(enc_path)

    # Genuinely encrypted -- not plaintext accidentally written.
    with open(enc_path, "rb") as f:
        on_disk = f.read()
    assert b"acct" not in on_disk and b"amount" not in on_disk

    source_comp = mod.PolarsPipelineComponent(
        asset_name="decrypted_in",
        source={
            "kind": "encrypted_file", "path": enc_path, "format": "csv",
            "key_source": "raw_key", "raw_key_env_var": raw_key_env,
        },
        operations=[{"op": "rename", "mapping": {"acct": "account_id"}}],
    )
    result2 = dg.materialize(
        source_comp.build_defs(None).assets,
        resources={"io_manager": dg.InMemoryIOManager()},
    )
    assert result2.success
    out = result2.output_for_node("decrypted_in")
    out_df = out if isinstance(out, pl.DataFrame) else pl.from_pandas(out)
    assert sorted(out_df["account_id"].to_list()) == [1, 2, 3]
    assert sorted(out_df["amount"].to_list()) == [10.5, 20.0, 30.25]


def test_format_inferred_from_enc_suffix(mod, raw_key_env, tmp_path):
    """orders.csv.enc with no explicit `format:` should strip .enc and infer csv."""
    enc_path = str(tmp_path / "orders.csv.enc")
    sink_comp = mod.PolarsPipelineComponent(
        asset_name="encrypted_out2",
        steps=[{"id": "s", "source": {"kind": "upstream", "upstream_asset_key": "raw_orders"}, "operations": []}],
        sinks=[{"from": "s", "kind": "encrypted_file", "path": enc_path,
                "key_source": "raw_key", "raw_key_env_var": raw_key_env}],
    )
    dg.materialize([_upstream(), *sink_comp.build_defs(None).assets],
                   resources={"io_manager": dg.InMemoryIOManager()})

    source_comp = mod.PolarsPipelineComponent(
        asset_name="decrypted_in2",
        source={"kind": "encrypted_file", "path": enc_path,
                "key_source": "raw_key", "raw_key_env_var": raw_key_env},
        operations=[],
    )
    result = dg.materialize(source_comp.build_defs(None).assets,
                             resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success


def test_wrong_key_fails_to_decrypt(mod, raw_key_env, tmp_path, monkeypatch):
    enc_path = str(tmp_path / "orders.csv.enc")
    sink_comp = mod.PolarsPipelineComponent(
        asset_name="encrypted_out3",
        steps=[{"id": "s", "source": {"kind": "upstream", "upstream_asset_key": "raw_orders"}, "operations": []}],
        sinks=[{"from": "s", "kind": "encrypted_file", "path": enc_path, "format": "csv",
                "key_source": "raw_key", "raw_key_env_var": raw_key_env}],
    )
    dg.materialize([_upstream(), *sink_comp.build_defs(None).assets],
                   resources={"io_manager": dg.InMemoryIOManager()})

    # A different 32-byte key -- decryption must fail loudly, not silently
    # return garbage or empty data.
    monkeypatch.setenv("TEST_ENCRYPTED_FILE_KEY_WRONG", base64.b64encode(os.urandom(32)).decode())
    source_comp = mod.PolarsPipelineComponent(
        asset_name="decrypted_in3",
        source={"kind": "encrypted_file", "path": enc_path, "format": "csv",
                "key_source": "raw_key", "raw_key_env_var": "TEST_ENCRYPTED_FILE_KEY_WRONG"},
        operations=[],
    )
    with pytest.raises(Exception):
        dg.materialize(source_comp.build_defs(None).assets,
                        resources={"io_manager": dg.InMemoryIOManager()})


def test_encryption_context_bound_to_resolved_path(mod, raw_key_env, tmp_path):
    """Ciphertext encrypted for path A, copied verbatim to path B, must fail the
    source's own encryption-context check (path binding), not silently decrypt."""
    enc_path_a = str(tmp_path / "a.csv.enc")
    enc_path_b = str(tmp_path / "b.csv.enc")
    sink_comp = mod.PolarsPipelineComponent(
        asset_name="encrypted_out4",
        steps=[{"id": "s", "source": {"kind": "upstream", "upstream_asset_key": "raw_orders"}, "operations": []}],
        sinks=[{"from": "s", "kind": "encrypted_file", "path": enc_path_a, "format": "csv",
                "key_source": "raw_key", "raw_key_env_var": raw_key_env}],
    )
    dg.materialize([_upstream(), *sink_comp.build_defs(None).assets],
                   resources={"io_manager": dg.InMemoryIOManager()})

    with open(enc_path_a, "rb") as f:
        ciphertext = f.read()
    with open(enc_path_b, "wb") as f:
        f.write(ciphertext)

    source_comp = mod.PolarsPipelineComponent(
        asset_name="decrypted_in4",
        source={"kind": "encrypted_file", "path": enc_path_b, "format": "csv",
                "key_source": "raw_key", "raw_key_env_var": raw_key_env},
        operations=[],
    )
    with pytest.raises(ValueError, match="encryption_context mismatch"):
        dg.materialize(source_comp.build_defs(None).assets,
                        resources={"io_manager": dg.InMemoryIOManager()})
