"""Real tests for three additions to MLPipelineComponent: multi-dimensional
partition support (previously entirely absent), four general-ETL ops
(rename/cast/with_columns/join), and encrypted_file source/sink (real AWS
Encryption SDK round-trip, not mocked) -- built so the bank-file-batch
ingestion demo in dagster-community-components-cli can use this pandas-based
pipeline component as an alternative to polars_pipeline.
"""
import base64
import os

import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def raw_key_env(monkeypatch):
    key_b64 = base64.b64encode(os.urandom(32)).decode()
    monkeypatch.setenv("TEST_ML_PIPELINE_FILE_KEY", key_b64)
    return "TEST_ML_PIPELINE_FILE_KEY"


# ── Partition builder ──────────────────────────────────────────────────

def test_build_partitions_def_single_static(mod):
    pdef = mod._build_partitions_def(
        "static", None, "us,eu,asia", None, None,
    )
    assert isinstance(pdef, dg.StaticPartitionsDefinition)
    assert set(pdef.get_partition_keys()) == {"us", "eu", "asia"}


def test_build_partitions_def_multi_dimension_bank_batch_day(mod):
    pdef = mod._build_partitions_def(
        None, None, None, None,
        [
            {"name": "bank", "type": "static", "values": ["acme", "globex"]},
            {"name": "batch_day", "type": "daily", "start": "2024-01-01"},
        ],
    )
    assert isinstance(pdef, dg.MultiPartitionsDefinition)
    dims = {d.name: d for d in pdef.partitions_defs}
    assert set(dims.keys()) == {"bank", "batch_day"}
    assert isinstance(dims["bank"].partitions_def, dg.StaticPartitionsDefinition)
    assert isinstance(dims["batch_day"].partitions_def, dg.DailyPartitionsDefinition)


def test_build_partitions_def_rejects_both_type_and_dimensions(mod):
    with pytest.raises(ValueError, match="either partition_type OR partition_dimensions"):
        mod._build_partitions_def(
            "static", None, "a,b", None,
            [{"name": "x", "type": "static", "values": ["a"]}],
        )


def test_build_partitions_def_none_when_unset(mod):
    assert mod._build_partitions_def(None, None, None, None, None) is None


# ── New ETL ops (real pandas behavior) ─────────────────────────────────

def _simple_source(df: pd.DataFrame):
    return {"kind": "dataframe", "dataframe": df}


def test_rename_cast_with_columns_ops(mod):
    df = pd.DataFrame({"old_col": ["1", "2", "3"], "keep": ["a", "b", "c"]})
    component = mod.MLPipelineComponent(
        asset_name_prefix="etl",
        source=_simple_source(df),
        target_column="keep",
        feature_columns=["new_col"],
        steps=[
            {"id": "renamed", "op": "rename", "columns": {"old_col": "new_col"}},
            {"id": "casted", "op": "cast", "source": "renamed", "columns": {"new_col": "int"}},
            {"id": "derived", "op": "with_columns", "source": "casted", "columns": {"doubled": "new_col * 2"}},
        ],
        outputs={"assets": ["derived"]},
    )
    defs = component.build_defs(context=None)
    result = dg.materialize(list(defs.assets))
    assert result.success
    out = result.output_for_node("etl_pipeline", output_name="etl_derived")
    assert list(out["new_col"]) == [1, 2, 3]
    assert list(out["doubled"]) == [2, 4, 6]
    assert "old_col" not in out.columns


def test_cast_coerce_errors_produces_nan_instead_of_raising(mod):
    df = pd.DataFrame({"n": ["1", "not_a_number", "3"]})
    component = mod.MLPipelineComponent(
        asset_name_prefix="castcoerce",
        source=_simple_source(df),
        target_column="n",
        feature_columns=["n"],
        steps=[
            {"id": "casted", "op": "cast", "columns": {"n": "float"}, "errors": "coerce"},
        ],
        outputs={"assets": ["casted"]},
    )
    defs = component.build_defs(context=None)
    result = dg.materialize(list(defs.assets))
    assert result.success
    out = result.output_for_node("castcoerce_pipeline", output_name="castcoerce_casted")
    assert out["n"].isna().sum() == 1


def test_join_op_against_a_second_source(mod):
    left = pd.DataFrame({"customer_id": [1, 2, 3], "amount": [10.0, 20.0, 30.0]})
    right = pd.DataFrame({"customer_id": [1, 2, 3], "tier": ["gold", "silver", "gold"]})
    component = mod.MLPipelineComponent(
        asset_name_prefix="joinop",
        source=_simple_source(left),
        target_column="amount",
        feature_columns=["amount"],
        steps=[
            {"id": "joined", "op": "join", "right": _simple_source(right), "on": ["customer_id"], "how": "inner"},
        ],
        outputs={"assets": ["joined"]},
    )
    defs = component.build_defs(context=None)
    result = dg.materialize(list(defs.assets))
    assert result.success
    out = result.output_for_node("joinop_pipeline", output_name="joinop_joined")
    assert sorted(out.columns) == sorted(["customer_id", "amount", "tier"])
    assert list(out.sort_values("customer_id")["tier"]) == ["gold", "silver", "gold"]


# ── encrypted_file source + sink (real AWS Encryption SDK round-trip) ──

def test_encrypted_file_source_and_sink_round_trip_real_aws_esdk(mod, tmp_path, raw_key_env):
    """Writes a real envelope-encrypted CSV via encrypted_file_sinks, then
    reads it back via source kind=encrypted_file -- through the ACTUAL
    aws_encryption_sdk library both directions, not mocked. Also asserts the
    file on disk is not plaintext (the whole point of encrypting it)."""
    original = pd.DataFrame({"account": ["acc-1", "acc-2"], "balance": [100.5, 200.25]})

    # Stage 1: encrypt `original` straight to disk via a tiny one-step pipeline.
    write_component = mod.MLPipelineComponent(
        asset_name_prefix="enc_write",
        source=_simple_source(original),
        target_column="balance",
        feature_columns=["balance"],
        steps=[{"id": "passthrough", "op": "select", "columns": ["account", "balance"]}],
        outputs={
            "assets": ["passthrough"],
            "encrypted_file_sinks": [{
                "from": "passthrough",
                "path": str(tmp_path / "accounts.enc"),
                "format": "csv",
                "key_source": "raw_key",
                "raw_key_env_var": raw_key_env,
                "key_id": "test-file-key",
            }],
        },
    )
    defs = dg.materialize(list(write_component.build_defs(context=None).assets))
    assert defs.success

    enc_path = tmp_path / "accounts.enc"
    assert enc_path.exists()
    raw_bytes = enc_path.read_bytes()
    assert b"acc-1" not in raw_bytes  # genuinely encrypted, not plaintext CSV

    # Stage 2: a fresh pipeline reads the SAME file back via encrypted_file.
    read_component = mod.MLPipelineComponent(
        asset_name_prefix="enc_read",
        source={
            "kind": "encrypted_file",
            "path": str(enc_path),
            "format": "csv",
            "key_source": "raw_key",
            "raw_key_env_var": raw_key_env,
            "key_id": "test-file-key",
        },
        target_column="balance",
        feature_columns=["balance"],
        steps=[{"id": "noop", "op": "select", "columns": ["account", "balance"]}],
        outputs={"assets": ["noop"]},
    )
    result = dg.materialize(list(read_component.build_defs(context=None).assets))
    assert result.success
    round_tripped = result.output_for_node("enc_read_pipeline", output_name="enc_read_noop")
    pd.testing.assert_frame_equal(
        round_tripped.reset_index(drop=True), original.reset_index(drop=True)
    )


def test_encrypted_file_wrong_key_fails_to_decrypt(mod, tmp_path, raw_key_env, monkeypatch):
    original = pd.DataFrame({"x": [1, 2, 3]})
    write_component = mod.MLPipelineComponent(
        asset_name_prefix="wrongkey_write",
        source=_simple_source(original),
        target_column="x",
        feature_columns=["x"],
        steps=[{"id": "passthrough", "op": "select", "columns": ["x"]}],
        outputs={
            "assets": ["passthrough"],
            "encrypted_file_sinks": [{
                "from": "passthrough",
                "path": str(tmp_path / "x.enc"),
                "key_source": "raw_key",
                "raw_key_env_var": raw_key_env,
                "key_id": "k1",
            }],
        },
    )
    assert dg.materialize(list(write_component.build_defs(context=None).assets)).success

    # A DIFFERENT key — decryption must fail, not silently return garbage.
    monkeypatch.setenv("TEST_ML_PIPELINE_WRONG_KEY", base64.b64encode(os.urandom(32)).decode())
    read_component = mod.MLPipelineComponent(
        asset_name_prefix="wrongkey_read",
        source={
            "kind": "encrypted_file", "path": str(tmp_path / "x.enc"),
            "key_source": "raw_key", "raw_key_env_var": "TEST_ML_PIPELINE_WRONG_KEY", "key_id": "k1",
        },
        target_column="x", feature_columns=["x"],
        steps=[{"id": "noop", "op": "select", "columns": ["x"]}],
        outputs={"assets": ["noop"]},
    )
    with pytest.raises(Exception):
        dg.materialize(list(read_component.build_defs(context=None).assets))
