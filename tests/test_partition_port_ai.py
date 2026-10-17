"""precision_match and synthetic_data: partitions are real in both of their shapes (with and without an upstream/reference
asset). LLM calls are faked; assets, partitions and materialization are real."""
import importlib.util
import json
import re
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import yaml

import dagster as dg

ROOT = Path(__file__).resolve().parent.parent


def _load(folder):
    spec = importlib.util.spec_from_file_location(f"ai_{folder.name}", folder / "component.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    doc = next(d for d in yaml.safe_load_all((folder / "example.yaml").read_text()) if d and "attributes" in d)
    return getattr(mod, doc["type"].rsplit(".", 1)[-1]), dict(doc["attributes"])


def _asset(cls, attrs):
    return next(a for a in cls(**attrs).build_defs(None).assets if hasattr(a, "node_def"))


def _completion(reply):
    def _call(model, messages, api_key=None, **kw):
        resp = MagicMock()
        resp.choices = [MagicMock(message=MagicMock(content=reply(messages[-1]["content"])))]
        return resp
    return _call


# ── precision_match ─────────────────────────────────────────────────────────

def _match_reply(prompt):
    inputs = json.loads(re.search(r"Inputs to match: (\[.*\])", prompt).group(1))
    return json.dumps({v: {"match": v.upper(), "confidence": 0.9} for v in inputs})


def _products():
    return pd.DataFrame({
        "product_category": ["shoes", "hats", "shoes", "coats", "scarves", "coats"],
        "ts": pd.to_datetime(["2026-01-02"] * 3 + ["2026-01-03"] * 3),
    })


@pytest.mark.parametrize("shape", ["reference_asset", "reference_values"])
def test_precision_match_filters_to_the_partition_in_both_shapes(shape):
    cls, base = _load(ROOT / "assets/ai/precision_match")
    base = {**base, "upstream_asset_key": "crm_raw"}
    if shape == "reference_values":
        base = {k: v for k, v in base.items() if k not in ("reference_asset_key", "reference_column")}
        base["reference_values"] = ["SHOES", "HATS", "COATS", "SCARVES"]
    part = _asset(cls, {**base, "partition_type": "daily", "partition_start": "2026-01-01", "partition_date_column": "ts"})
    plain = _asset(cls, base)
    assert isinstance(part.partitions_def, dg.DailyPartitionsDefinition)
    name = next(iter(part.keys)).to_user_string()
    df = _products()
    reference = pd.DataFrame({"category_name": ["SHOES", "HATS", "COATS", "SCARVES"]})

    def assets(frame):
        @dg.asset(name="crm_raw")
        def crm_raw():
            return frame

        out = [crm_raw]
        if shape == "reference_asset":
            @dg.asset(name="canonical_product_taxonomy")
            def canonical_product_taxonomy():
                return reference
            out.append(canonical_product_taxonomy)
        return out

    with patch("litellm.completion", side_effect=_completion(_match_reply)):
        for day in ("2026-01-02", "2026-01-03"):
            instance = dg.DagsterInstance.ephemeral()
            dg.materialize(assets(df), instance=instance)
            actual = dg.materialize([part, *assets(df)], instance=instance, partition_key=day, selection=[part.key]).output_for_node(name)
            expected = dg.materialize([plain, *assets(df[df["ts"] == day].reset_index(drop=True))]).output_for_node(name)
            pd.testing.assert_frame_equal(actual.reset_index(drop=True), expected.reset_index(drop=True), check_dtype=False)
            assert len(actual) == 3 and set(actual["ts"].dt.strftime("%Y-%m-%d")) == {day}


# ── synthetic_data ──────────────────────────────────────────────────────────

def _gen_reply(prompt):
    n = int(re.search(r"Generate (\d+) rows", prompt).group(1))
    return json.dumps([{"id": i, "created": "1999-01-01", "customer": "llm-chose-this"} for i in range(n)])


def _gen_attrs(base, **extra):
    return {**base, "n_rows": 4, "batch_size": 4, "schema": {"id": "int", "created": "date", "customer": "name"}, **extra}


def test_generated_rows_are_stamped_with_the_partition_and_the_model_is_told_which_one():
    cls, base = _load(ROOT / "assets/ai/synthetic_data")
    prompts = []

    def reply(prompt):
        prompts.append(prompt)
        return _gen_reply(prompt)

    part = _asset(cls, _gen_attrs(base, partition_type="multi", partition_start="2026-01-01", partition_values="acme,globex",
                                 partition_static_dim="customer", partition_date_column="created", partition_static_column="customer"))
    assert isinstance(part.partitions_def, dg.MultiPartitionsDefinition)
    name = next(iter(part.keys)).to_user_string()
    with patch("litellm.completion", side_effect=_completion(reply)):
        key = dg.MultiPartitionKey({"date": "2026-01-03", "static_dim": "globex"})
        out = dg.materialize([part], partition_key=key).output_for_node(name)
    assert len(out) == 4
    assert set(out["customer"]) == {"globex"}                                   # static axis stamped on every row
    assert all(str(d).startswith("2026-01-03") for d in out["created"])         # dates within the partition's window
    assert "2026-01-03" in prompts[0] and "globex" in prompts[0]                # the model is told the partition


def test_unpartitioned_synthetic_data_is_untouched():
    cls, base = _load(ROOT / "assets/ai/synthetic_data")
    plain = _asset(cls, _gen_attrs(base))
    assert plain.partitions_def is None
    with patch("litellm.completion", side_effect=_completion(_gen_reply)):
        out = dg.materialize([plain]).output_for_node(next(iter(plain.keys)).to_user_string())
    assert set(out["customer"]) == {"llm-chose-this"} and set(out["created"]) == {"1999-01-01"}


def test_seed_rows_are_filtered_to_the_partition_before_prompting():
    cls, base = _load(ROOT / "assets/ai/synthetic_data")
    seed_field = next(f for f in cls.model_fields if "seed" in f)
    seen = []

    def reply(prompt):
        seen.append(prompt)
        return _gen_reply(prompt)

    part = _asset(cls, _gen_attrs(base, **{seed_field: "seed_rows"}, partition_type="daily", partition_start="2026-01-01",
                                 partition_date_column="ts"))
    seed = pd.DataFrame({"who": ["day2-a", "day2-b", "day3-a", "day3-b"],
                         "ts": pd.to_datetime(["2026-01-02"] * 2 + ["2026-01-03"] * 2)})

    @dg.asset(name="seed_rows")
    def seed_rows():
        return seed

    instance = dg.DagsterInstance.ephemeral()
    dg.materialize([seed_rows], instance=instance)
    with patch("litellm.completion", side_effect=_completion(reply)):
        dg.materialize([part, seed_rows], instance=instance, partition_key="2026-01-03", selection=[part.key])
    assert "day3-a" in seen[0] and "day2-a" not in seen[0]  # only that day's seed rows reach the prompt


# ── snowflake_cortex_asset ──────────────────────────────────────────────────

class _FakeCursor:
    def __init__(self, log):
        self.log = log

    def execute(self, sql):
        self.log.append(sql)

    def fetchone(self):
        return (7,)

    def close(self):
        pass


class _FakeConn:
    def __init__(self, log):
        self.log = log

    def cursor(self):
        return _FakeCursor(self.log)

    def close(self):
        pass


def _cortex(monkeypatch, **extra):
    import sys
    import types
    log = []
    connector = types.SimpleNamespace(connect=lambda **kw: _FakeConn(log))
    pkg = types.ModuleType("snowflake")
    pkg.connector = connector
    monkeypatch.setitem(sys.modules, "snowflake", pkg)
    monkeypatch.setitem(sys.modules, "snowflake.connector", connector)
    cls, base = _load(ROOT / "assets/ai/snowflake_cortex_asset")
    for name in ("snowflake_account_env_var", "snowflake_user_env_var", "snowflake_password_env_var"):
        monkeypatch.setenv(base.get(name) or cls.model_fields[name].default, "x")
    return _asset(cls, {**base, **extra}), log


def test_snowflake_cortex_partitioned_run_only_processes_and_replaces_its_slice(monkeypatch):
    part, log = _cortex(monkeypatch, if_exists="replace", partition_type="multi", partition_start="2026-01-01",
                        partition_values="acme,globex", partition_static_dim="customer",
                        partition_date_column="CREATED_AT", partition_static_column="CUSTOMER")
    key = dg.MultiPartitionKey({"date": "2026-01-03", "static_dim": "globex"})
    assert dg.materialize([part], partition_key=key).success
    slice_where = "CREATED_AT >= '2026-01-03 00:00:00' AND CREATED_AT < '2026-01-04 00:00:00' AND CUSTOMER IN ('globex')"
    assert len(log) == 4  # create-if-missing, delete this slice, insert this slice, count
    assert log[0].startswith("CREATE TABLE IF NOT EXISTS") and "1 = 0" in log[0]      # never CREATE OR REPLACE a partitioned table
    assert log[1].startswith("DELETE FROM") and log[1].endswith(slice_where)
    assert log[2].startswith("INSERT INTO") and f"WHERE {slice_where}" in log[2]
    assert not any("CREATE OR REPLACE" in s for s in log)


def test_snowflake_cortex_unpartitioned_sql_is_unchanged(monkeypatch):
    plain, log = _cortex(monkeypatch, if_exists="replace")
    assert plain.partitions_def is None and dg.materialize([plain]).success
    assert log[0].startswith("CREATE OR REPLACE TABLE") and "WHERE" not in log[0]


def test_snowflake_cortex_rejects_unsafe_partition_column_names(monkeypatch):
    part, log = _cortex(monkeypatch, partition_type="daily", partition_start="2026-01-01",
                        partition_date_column="ts; DROP TABLE x")
    result = dg.materialize([part], partition_key="2026-01-03", raise_on_error=False)
    assert not result.success and log == []  # nothing executed against the warehouse
