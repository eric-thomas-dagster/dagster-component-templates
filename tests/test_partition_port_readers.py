"""Query-style readers substitute {partition_*} tokens into their query/filter/time-range fields.

Each component is built from its own example.yaml and materialized with a 40-row upstream (two days of 20 rows).
The step's  local is captured when the step returns (whether it succeeded or failed at its first external
call), so what is asserted is exactly what survived the partition filter. The network is blocked."""
import importlib.abc
import importlib.machinery
import importlib.util
import json
import types
import socket
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import yaml

import dagster as dg

ROOT = Path(__file__).resolve().parent.parent
DAY = "2026-01-03"
class _Response:
    status_code = 200
    ok = True
    headers = {}
    text = "{}"
    content = b"{}"

    def json(self, *a, **k):
        return {}

    def raise_for_status(self):
        pass


@pytest.fixture(autouse=True)
def _no_network(monkeypatch):
    import signal
    import time

    import requests

    monkeypatch.setattr(time, "sleep", lambda *_: None)  # retry/backoff loops must not slow the suite
    signal.signal(signal.SIGALRM, lambda *_: (_ for _ in ()).throw(TimeoutError("component hung")))
    signal.alarm(30)

    monkeypatch.setattr(requests.sessions.Session, "request", lambda self, *a, **k: _Response())

    def _refuse(*a, **k):
        raise OSError("network blocked in tests")

    monkeypatch.setattr(socket.socket, "connect", _refuse)
    yield
    signal.alarm(0)


def _frame():
    rng = np.random.default_rng(0)
    return pd.DataFrame({"text": [f"row {i}" for i in range(40)], "id": range(40), "v": rng.normal(size=40),
                         "ts": pd.to_datetime(["2026-01-02"] * 20 + ["2026-01-03"] * 20)})


class _AnyResource:
    def __getattr__(self, name):
        return self

    def __call__(self, *a, **k):
        return self

    def __iter__(self):
        return iter(())

    def __or__(self, other):
        return self

    __ror__ = __or__

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def _expand_env(value):
    """Examples spell secrets as ${ENV_VAR}; resolve them from the environment like a real project would."""
    import os
    import re

    if isinstance(value, str):
        return re.sub(r"\$\{(\w+)\}", lambda m: os.environ.get(m.group(1), m.group(0)), value)
    if isinstance(value, dict):
        return {k: _expand_env(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_expand_env(v) for v in value]
    return value


FIELDS = {
    "assets/source/bigtable_reader_asset": ["row_key_prefix", "start_key", "end_key"],
    "assets/source/cloud_logging_query_asset": ["filter_"],
    "assets/source/cloud_monitoring_metrics_asset": ["filter_"],
    "assets/source/dataplex_dq_scan_results_asset": ["since_iso"],
    "assets/source/file_lister": ["path"],
    "assets/source/spanner_query_asset": ["sql"],
    "assets/sources/aws_cloudwatch_logs_insights_query": ["query_string"],
    "assets/sources/azure_log_analytics_query": ["query", "timespan_iso8601"],
    "assets/sources/azure_search_query": ["filter_query"],
    "assets/sources/azure_table_reader": ["filter_query"],
    "assets/sources/dataframe_from_kusto": ["query"],
    "assets/sources/dataframe_from_prometheus": ["query"],
    "assets/sources/dynatrace_metrics_query": ["timeframe"],
    "assets/sources/newrelic_nrql_query": ["query"],
    "assets/ingestion/google_calendar_ingestion": ["time_min", "time_max"],
    "assets/ingestion/google_drive_ingestion": ["query"],
    "assets/ingestion/archive_fetcher": ["url"],
    "assets/source/firestore_reader_asset": ["where", "collection"],
    "assets/ai/bigquery_ml_predict_asset": ["input_query"],
    "assets/ai/bigquery_ml_train_asset": ["select_query", "query_params"],
    "assets/source/synthetic_audio_generator": ["output_dir"],
    "assets/source/synthetic_image_generator": ["output_dir"],
    "assets/source/synthetic_pdf_generator": ["output_dir"],
    "assets/source/synthetic_video_generator": ["output_dir"],
    "assets/sources/doris_query_asset": ["query"],
    "assets/sources/victoriametrics_query_asset": ["query"],
    "assets/sources/polars_read_database": ["query"],
    "assets/sources/polars_scan_parquet": ["path"],
    "assets/sources/dataframe_from_yxdb": ["file_path"],
    "assets/analytics/cube_query_asset": ["query"],
    "assets/ingestion/sentry_issues_ingestion": ["query"],
    "assets/ingestion/metronome_invoices_ingestion": ["starting_on", "ending_before"],
    "assets/ingestion/figment_staking_rewards_ingestion": ["start", "end"],
    "assets/ingestion/tm1_cube_data_ingestion": ["filters"],
}
class _MissingModules(importlib.abc.MetaPathFinder, importlib.abc.Loader):
    """Last-resort finder: any module that is not installed (cloud SDKs imported lazily inside the step) is
    fabricated -- but only for the specific names below, not unconditionally.

    This is appended to the END of sys.meta_path, so every real finder (builtin, frozen, path-based) has
    already had first crack at resolving the name; find_spec here only ever runs for a name genuinely
    missing from the environment. It must stay a named allowlist rather than "fabricate anything
    unresolvable": boto3 (a real, installed dependency) lazily imports `awscrt`, an optional compiled
    extension that's itself genuinely absent here, and botocore's own code already handles that absence
    with a try/except -- fabricating `awscrt` instead of letting that import fail normally broke the
    fallback (botocore read `awscrt.__version__` off the fake module and got a confusing AttributeError
    instead of the ImportError its own except-clause expects). So this only stands in for first-party
    cloud/auth SDKs this repo's components import lazily and guard with their own "pip install ..."
    ImportError, never for a third-party library's own internal optional dependencies.

    google.oauth2/google.auth/google.api_core/google.genai/google.protobuf are siblings of google.cloud.*
    (not children of it) that several components also import for credentials/typing -- real CI failures,
    caught locally only because those happened to already be installed as someone else's transitive
    dependency.
    """

    TOP_LEVEL = {"azure", "boto3", "botocore", "googleapiclient", "pymysql", "polars", "isodate"}
    GOOGLE_PREFIXES = ("google.cloud", "google.oauth2", "google.auth", "google.api_core", "google.genai", "google.protobuf")

    def find_spec(self, name, path=None, target=None):
        if name.split(".")[0] in self.TOP_LEVEL or any(name == p or name.startswith(p + ".") for p in self.GOOGLE_PREFIXES):
            return importlib.machinery.ModuleSpec(name, self, is_package=True)
        return None

    def create_module(self, spec):
        mod = types.ModuleType(spec.name)
        mod.__path__ = []
        mod.__getattr__ = lambda attr: _AnyResource() if not attr.startswith("__") else (_ for _ in ()).throw(AttributeError(attr))
        return mod

    def exec_module(self, module):
        pass


class _EmptyPageClient:
    """A paginating REST client whose every call returns one empty, final page."""

    def get(self, *a, **k):
        return self

    def raise_for_status(self):
        pass

    def json(self):
        return {"data": [], "next_page": None}


class _EmptyPageResource:
    base_url = "https://example.invalid/v1"

    def get_client(self):
        return _EmptyPageClient()


RESOURCES = {"assets/ingestion/metronome_invoices_ingestion": _EmptyPageResource()}
MODE = {"assets/ingestion/metronome_invoices_ingestion": {"customer_ids": ["c1"]},
        "assets/source/dataplex_dq_scan_results_asset": {"mode": "history"}}  # since_iso is only read in history mode
SHAPED = {"filters": {"Year": "X {partition_date} {partition_start}/{partition_end}"},
          "where": [{"field": "d", "op": "==", "value": "X {partition_date} {partition_start}/{partition_end}"}],
          "query_params": {"d": "X {partition_date} {partition_start}/{partition_end}"}}  # fields that are not plain strings
SHAPED_BY_COMPONENT = {("assets/analytics/cube_query_asset", "query"): {"measures": ["X {partition_date} {partition_start}/{partition_end}"]}}
ALIAS = {"filter_": "filter"}  # the YAML name of a field whose Python name avoids the `filter` builtin
TEMPLATE = "X {partition_date} {partition_start}/{partition_end}"
EXPECTED = "X 2026-01-03 2026-01-03T00:00:00/2026-01-04T00:00:00"


def _service_account():
    """A syntactically valid service-account document around a throwaway key; it never authenticates anywhere."""
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()).decode()
    return {"type": "service_account", "project_id": "p", "private_key_id": "k", "private_key": pem,
            "client_email": "sa@p.iam.gserviceaccount.com", "client_id": "1", "token_uri": "https://oauth2.googleapis.com/token"}


def _build(rel, extra, monkeypatch, tmp_path):
    creds = tmp_path / "creds.json"
    creds.write_text(json.dumps(_service_account()))
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", str(creds))
    folder = ROOT / rel
    doc = next(d for d in yaml.safe_load_all((folder / "example.yaml").read_text()) if d and "attributes" in d)
    sys.path.insert(0, str(folder))
    try:
        spec = importlib.util.spec_from_file_location("tk_" + folder.name, folder / "component.py")
        mod = importlib.util.module_from_spec(spec)
        monkeypatch.setitem(sys.modules, spec.name, mod)
        spec.loader.exec_module(mod)
        cls = getattr(mod, doc["type"].rsplit(".", 1)[-1])
        attrs = _expand_env(doc["attributes"])
        for key, value in attrs.items():  # secrets are named by *_env_var fields; give each a value
            if key.endswith(("env_var", "_env")) and isinstance(value, str) and value.isidentifier():
                monkeypatch.setenv(value, "x")
        defs = cls(**{**attrs, **extra}).build_defs(None)
    finally:
        sys.path.remove(str(folder))
    return next(a for a in defs.assets if hasattr(a, "node_def"))


def _substituted(rel, monkeypatch, tmp_path, partitioned=True):
    monkeypatch.chdir(tmp_path)  # components that write files (output_dir, extract_to) must write here, never into the repo
    import signal
    import time

    monkeypatch.setattr(time, "sleep", lambda *_: None)
    signal.signal(signal.SIGALRM, lambda *_: (_ for _ in ()).throw(TimeoutError("component hung")))
    signal.alarm(5)
    monkeypatch.setattr(sys, "meta_path", [*sys.meta_path, _MissingModules()])
    fields = FIELDS[rel]
    extra = {ALIAS.get(f, f): SHAPED_BY_COMPONENT.get((rel, f), SHAPED.get(f, TEMPLATE)) for f in fields} | MODE.get(rel, {})
    if partitioned:
        extra.update(partition_type="daily", partition_start="2026-01-01")
    asset = _build(rel, extra, monkeypatch, tmp_path)
    resources = {k: dg.ResourceDefinition.hardcoded_resource(RESOURCES.get(rel, _AnyResource())) for k in asset.required_resource_keys if k != "io_manager"}
    results = []

    def prof(frame, event, arg):
        if event == "return" and frame.f_code.co_name == "_substitute_partition" and "/assets/" in frame.f_code.co_filename:
            results.append(arg)

    sys.setprofile(prof)
    try:
        dg.materialize([asset], raise_on_error=False, resources=resources, **({"partition_key": "2026-01-03"} if partitioned else {}))
    finally:
        sys.setprofile(None)
        signal.alarm(0)
    return results


@pytest.mark.parametrize("rel", list(FIELDS), ids=lambda r: r.split("/", 1)[1])
def test_partitioned_run_substitutes_the_tokens(rel, monkeypatch, tmp_path):
    results = _substituted(rel, monkeypatch, tmp_path)
    assert EXPECTED in results, results


@pytest.mark.parametrize("rel", list(FIELDS), ids=lambda r: r.split("/", 1)[1])
def test_unpartitioned_run_leaves_the_tokens_alone(rel, monkeypatch, tmp_path):
    results = _substituted(rel, monkeypatch, tmp_path, partitioned=False)
    wanted = [SHAPED_BY_COMPONENT.get((rel, f), SHAPED.get(f, TEMPLATE)) for f in FIELDS[rel]]
    assert EXPECTED not in results and any(w in results for w in wanted), results


def test_helper_copies_are_identical():
    import re

    blocks = {re.search(r"# BEGIN partition-vars helper.*?# END partition-vars helper", (ROOT / rel / "component.py").read_text(), re.S).group(0)
              for rel in FIELDS}
    assert len(blocks) == 1
