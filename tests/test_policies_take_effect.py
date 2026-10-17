"""Partitions, freshness and retry take effect on the assets every component emits.

Each component is built from its own example.yaml with the three settings turned on, then Definitions are assembled
around it (upstream assets stubbed with the same partitions, resources mocked) and a job over everything is constructed,
which is where mismatched partitions or bad policy objects surface. What is asserted is the policy objects themselves:
the partitions definition, the FreshnessPolicy (window and cron variants) and the RetryPolicy's retries, delay and backoff."""
import importlib.util
import json
import os
import re
import sys
import warnings
from datetime import timedelta
from pathlib import Path

import dagster as dg
import pytest
import yaml

ROOT = Path(__file__).resolve().parent.parent
DAILY = dg.DailyPartitionsDefinition("2026-01-01")
FOLDERS = sorted(p.parent for p in ROOT.glob("assets/*/*/example.yaml") if (p.parent / "component.py").exists())

# Components that cannot be built from their example with these settings, and why. Anything else must build.
UNBUILDABLE = {
    "ai/agentic_pipeline": "backfill_policy='single_run' is refused by design (a run over a range cannot carry per-partition state)",
    "ai/llm_multi_path_router": "its example turns on dynamic partitions, which need partition_values",
    "ingestion/google_sheets_ingestion": "its example gives credentials as a string",
    "sinks/dataframe_to_azure_table": "its example omits the required partition_key_column",
    "dbt/enriched_dbt_cloud_workspace": "its example uses {{ env.X }} templates that only Dagster's own resolver evaluates",
}
NO_PARTITIONS = {"infrastructure/rivery_run_asset": "a Rivery river takes no partition-dependent input; it warns that it is not partitioned"}


def _expand_env(value):
    if isinstance(value, str):
        return re.sub(r"\$\{(\w+)\}", lambda m: os.environ.get(m.group(1), m.group(0)), value)
    if isinstance(value, dict):
        return {k: _expand_env(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_expand_env(v) for v in value]
    return value


@pytest.fixture(scope="module")
def service_account(tmp_path_factory):
    """A syntactically valid service-account file around a throwaway key; it never authenticates anywhere."""
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()).decode()
    path = tmp_path_factory.mktemp("sa") / "sa.json"
    path.write_text(json.dumps({"type": "service_account", "project_id": "p", "private_key_id": "k", "private_key": pem,
                                "client_email": "sa@p.iam.gserviceaccount.com", "client_id": "1", "token_uri": "https://oauth2.googleapis.com/token"}))
    return str(path)


def _build(folder, extra, monkeypatch):
    doc = next(d for d in yaml.safe_load_all((folder / "example.yaml").read_text()) if d and "attributes" in d)
    sys.path.insert(0, str(folder))
    try:
        spec = importlib.util.spec_from_file_location("pol_" + folder.name, folder / "component.py")
        mod = importlib.util.module_from_spec(spec)
        monkeypatch.setitem(sys.modules, spec.name, mod)
        spec.loader.exec_module(mod)
    finally:
        sys.path.remove(str(folder))
    cls = getattr(mod, doc["type"].rsplit(".", 1)[-1])
    fields = getattr(cls, "model_fields", None) or {}
    attrs = _expand_env(doc["attributes"])
    settings = {}
    if "partition_type" in fields:
        attrs = {k: v for k, v in attrs.items() if not k.startswith("partition") or k == "partition_date_column"}
        settings.update(partition_type="daily", partition_start="2026-01-01")
        if "partition_date_column" in fields and not attrs.get("partition_date_column"):
            settings["partition_date_column"] = "ts"
    if "backfill_policy" in fields:
        settings["backfill_policy"] = "single_run"
    settings.update({k: v for k, v in extra.items() if k in fields})
    return fields, cls(**{**attrs, **settings}).build_defs(None)


def _assemble(defs, partitioned):
    assets = [a for a in (defs.assets or []) if isinstance(a, dg.AssetsDefinition)]
    produced = {k for a in assets for k in a.keys}
    needed = {k for a in assets for k in a.keys_by_input_name.values()} | {d.asset_key for a in assets for sp in a.specs for d in sp.deps}
    stubs = [dg.AssetSpec(k, partitions_def=DAILY if partitioned else None) for k in needed - produced]
    resources = {k: dg.ResourceDefinition.hardcoded_resource(object()) for k in {k for a in assets for k in a.required_resource_keys} if k != "io_manager"}
    full = dg.Definitions(assets=[*defs.assets, *stubs], asset_checks=list(defs.asset_checks or []), sensors=list(defs.sensors or []),
                          schedules=list(defs.schedules or []), resources=resources)
    full.resolve_asset_graph()
    full.get_implicit_global_asset_job_def()   # job construction is where partition mismatches and bad policies fail
    return assets


VARIANTS = {
    "window freshness, exponential retry": dict(freshness_max_lag_minutes=60, freshness_cron=None, retry_policy_max_retries=2, retry_policy_delay_seconds=3, retry_policy_backoff="exponential"),
    "cron freshness, linear retry": dict(freshness_max_lag_minutes=60, freshness_cron="0 9 * * *", retry_policy_max_retries=5, retry_policy_delay_seconds=7, retry_policy_backoff="linear"),
}


@pytest.mark.parametrize("folder", FOLDERS, ids=lambda p: str(p.relative_to(ROOT / "assets")))
@pytest.mark.parametrize("variant", list(VARIANTS), ids=lambda v: v.split(",")[0].replace(" ", "-"))
def test_partitions_freshness_and_retry_are_on_every_emitted_asset(folder, variant, monkeypatch, service_account):
    name = str(folder.relative_to(ROOT / "assets"))
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", service_account)
    extra = VARIANTS[variant]
    try:
        fields, defs = _build(folder, extra, monkeypatch)
    except Exception as exc:
        if name in UNBUILDABLE:
            pytest.skip(UNBUILDABLE[name])
        if isinstance(exc, ImportError):
            pytest.skip(f"needs an optional library that is not installed here: {exc}")
        if "project_root" in str(exc) or "defs_state_discriminator" in str(exc):
            pytest.skip("state-backed component: needs a real project load context")
        raise
    wants = {"partition": "partition_type" in fields, "freshness": "freshness_max_lag_minutes" in fields, "retry": "retry_policy_max_retries" in fields,
             "backfill": "backfill_policy" in fields}
    if not any(wants.values()):
        pytest.skip("declares no partition, freshness or retry settings")
    assets = _assemble(defs, partitioned=wants["partition"] and name not in NO_PARTITIONS)
    assert assets or not any(wants.values()), "emits no assets"
    lag = timedelta(minutes=60)
    expected_fresh = (dg.FreshnessPolicy.cron(deadline_cron="0 9 * * *", lower_bound_delta=lag) if extra.get("freshness_cron")
                      else dg.FreshnessPolicy.time_window(fail_window=lag))
    for a in assets:
        key = a.key.to_user_string() if len(a.keys) == 1 else sorted(k.to_user_string() for k in a.keys)
        if wants["partition"] and name not in NO_PARTITIONS:
            assert a.partitions_def == DAILY, f"{key} is not partitioned as configured"
        if wants["backfill"] and wants["partition"] and name not in NO_PARTITIONS:
            assert a.backfill_policy == dg.BackfillPolicy.single_run(), f"{key} ignores backfill_policy"
        if wants["freshness"]:
            for spec in a.specs:
                assert spec.freshness_policy == expected_fresh, f"{spec.key.to_user_string()} has freshness {spec.freshness_policy}, expected {expected_fresh}"
        if wants["retry"]:
            rp = a.node_def.retry_policy
            want_backoff = dg.Backoff.LINEAR if extra["retry_policy_backoff"] == "linear" else dg.Backoff.EXPONENTIAL
            assert rp is not None, f"{key} has no retry policy"
            assert (rp.max_retries, rp.delay, rp.backoff) == (extra["retry_policy_max_retries"], extra["retry_policy_delay_seconds"], want_backoff), (key, rp)


def test_the_allowlists_name_real_components():
    names = {str(p.relative_to(ROOT / "assets")) for p in FOLDERS}
    assert set(UNBUILDABLE) <= names and set(NO_PARTITIONS) <= names


# ── end to end: a real run, a flaky write, one partition ─────────────────────────────────────────────────────────

def _csv_component(monkeypatch, tmp_path, **extra):
    import numpy as np
    import pandas as pd

    spec = importlib.util.spec_from_file_location("e2e_csv", ROOT / "assets/sinks/dataframe_to_csv/component.py")
    mod = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, spec.name, mod)
    spec.loader.exec_module(mod)
    frame = pd.DataFrame({"id": range(40), "v": np.arange(40.0),
                          "ts": pd.to_datetime(["2026-01-02"] * 20 + ["2026-01-03"] * 20)})

    @dg.asset(name="src")
    def src():
        return frame

    comp = mod.DataframeToCsvComponent(asset_name="out", upstream_asset_key="src", file_path=str(tmp_path / "out_{partition_key}.csv"),
                                       partition_type="daily", partition_start="2026-01-01", partition_date_column="ts", **extra)
    return src, next(a for a in comp.build_defs(None).assets if hasattr(a, "node_def"))


def _flaky_to_csv(monkeypatch, failures):
    import pandas as pd

    real, attempts = pd.DataFrame.to_csv, []

    def flaky(self, *a, **k):
        attempts.append(1)
        if len(attempts) <= failures:
            raise IOError(f"transient write failure #{len(attempts)}")
        return real(self, *a, **k)

    monkeypatch.setattr(pd.DataFrame, "to_csv", flaky)
    return attempts


def test_a_flaky_partition_run_retries_then_writes_only_its_partitions_rows(monkeypatch, tmp_path):
    import pandas as pd

    src, asset = _csv_component(monkeypatch, tmp_path, retry_policy_max_retries=2, retry_policy_delay_seconds=1, retry_policy_backoff="linear")
    attempts = _flaky_to_csv(monkeypatch, failures=2)
    instance = dg.DagsterInstance.ephemeral()
    dg.materialize([src], instance=instance)
    r = dg.materialize([asset, src], selection=[asset.key], instance=instance, partition_key="2026-01-03", raise_on_error=False)
    assert r.success and len(attempts) == 3                                  # failed twice, succeeded on the third attempt
    written = pd.read_csv(tmp_path / "out_2026-01-03.csv")
    assert len(written) == 20 and set(pd.to_datetime(written["ts"]).dt.strftime("%Y-%m-%d")) == {"2026-01-03"}
    assert not (tmp_path / "out_2026-01-02.csv").exists()


def test_retries_are_bounded_and_the_run_then_fails(monkeypatch, tmp_path):
    src, asset = _csv_component(monkeypatch, tmp_path, retry_policy_max_retries=1, retry_policy_delay_seconds=1)
    attempts = _flaky_to_csv(monkeypatch, failures=99)
    instance = dg.DagsterInstance.ephemeral()
    dg.materialize([src], instance=instance)
    r = dg.materialize([asset, src], selection=[asset.key], instance=instance, partition_key="2026-01-03", raise_on_error=False)
    assert not r.success and len(attempts) == 2                              # one try plus exactly one retry


def test_a_single_run_backfill_writes_one_file_per_partition_with_the_right_rows(monkeypatch, tmp_path):
    import pandas as pd

    src, asset = _csv_component(monkeypatch, tmp_path, backfill_policy="single_run")
    assert asset.backfill_policy == dg.BackfillPolicy.single_run()
    instance = dg.DagsterInstance.ephemeral()
    dg.materialize([src], instance=instance)
    tags = {"dagster/asset_partition_range_start": "2026-01-02", "dagster/asset_partition_range_end": "2026-01-03"}

    class AnyPartitionIO(dg.IOManager):
        def handle_output(self, context, obj): ...
        def load_input(self, context):
            return pd.DataFrame({"id": range(40), "v": range(40), "ts": pd.to_datetime(["2026-01-02"] * 20 + ["2026-01-03"] * 20)})

    r = dg.materialize([asset, src], selection=[asset.key], instance=instance, tags=tags, raise_on_error=False,
                       resources={"io_manager": AnyPartitionIO()})
    assert r.success, r
    for day in ("2026-01-02", "2026-01-03"):
        df = pd.read_csv(tmp_path / f"out_{day}.csv")
        assert len(df) == 20 and set(pd.to_datetime(df["ts"]).dt.strftime("%Y-%m-%d")) == {day}
