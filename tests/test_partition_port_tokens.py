"""Partition tokens/windows reach the notebook, Trino, the document corpus, Feast and Tecton.

Each component runs for real against a recording fake of its one external dependency."""
import importlib.util
import sys
import types
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest

import dagster as dg

ROOT = Path(__file__).resolve().parent.parent
DAILY = {"partition_type": "daily", "partition_start": "2026-01-01"}


def _load(rel):
    p = ROOT / rel / "component.py"
    spec = importlib.util.spec_from_file_location("tok_" + p.parent.name, p)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _asset(comp):
    return next(a for a in comp.build_defs(None).assets if hasattr(a, "node_def"))


def _run(asset, key="2026-01-03", **kw):
    r = dg.materialize([asset], partition_key=key, raise_on_error=False, **kw) if key else dg.materialize([asset], raise_on_error=False)
    return r


def test_trino_substitutes_tokens_into_the_query(monkeypatch):
    executed = []

    class Cur:
        description = [("a",)]
        def execute(self, q): executed.append(q)
        def fetchall(self): return [(1,)]

    class Conn:
        def cursor(self): return Cur()
        def close(self): pass

    trino = types.ModuleType("trino"); dbapi = types.ModuleType("trino.dbapi"); auth = types.ModuleType("trino.auth")
    dbapi.connect = lambda **k: Conn(); auth.BasicAuthentication = lambda *a: None
    trino.dbapi, trino.auth = dbapi, auth
    for n, m in (("trino", trino), ("trino.dbapi", dbapi), ("trino.auth", auth)):
        monkeypatch.setitem(sys.modules, n, m)
    mod = _load("assets/sources/trino_query")
    q = "SELECT * FROM t WHERE d >= DATE '{partition_date}' AND ts < TIMESTAMP '{partition_end}'"
    base = dict(asset_name="tq", host="h", user="u", catalog="c", schema_name="s", query=q)
    assert _run(_asset(mod.TrinoQueryComponent(**base, **DAILY))).success
    assert executed[-1] == "SELECT * FROM t WHERE d >= DATE '2026-01-03' AND ts < TIMESTAMP '2026-01-04T00:00:00'"
    assert _run(_asset(mod.TrinoQueryComponent(**base)), key=None).success
    assert executed[-1] == q  # unpartitioned: untouched


def test_document_corpus_resolves_tokens_in_directory_and_glob(tmp_path):
    for day in ("2026-01-02", "2026-01-03"):
        (tmp_path / day).mkdir()
        (tmp_path / day / "a.md").write_text(f"doc for {day}")
    mod = _load("assets/ai/document_corpus")
    comp = mod.DocumentCorpusComponent(asset_name="corpus", source_dir=str(tmp_path / "{partition_date}"), **DAILY)
    r = _run(_asset(comp))
    assert r.success
    df = r.output_for_node("corpus")
    assert len(df) == 1 and "2026-01-03" in df.iloc[0].to_json()


def test_jupyter_receives_partition_parameters(monkeypatch):
    seen = {}
    pm = types.ModuleType("papermill")
    class Err(Exception): ...
    pm.PapermillExecutionError = Err
    pm.execute_notebook = lambda nb, out, parameters=None, **k: seen.update(parameters=parameters)
    monkeypatch.setitem(sys.modules, "papermill", pm)
    mod = _load("assets/analytics/jupyter_notebook_asset")
    nb = ROOT / "assets/analytics/jupyter_notebook_asset" / "nb.ipynb"
    created = not nb.exists()
    nb.write_text('{"cells": [], "metadata": {}, "nbformat": 4, "nbformat_minor": 5}')
    try:
        comp = mod.JupyterNotebookAssetComponent(asset_name="nbk", notebook_path=str(nb), parameters={"since": "{partition_date}", "n": 3}, **DAILY)
        r = _run(_asset(comp))
    finally:
        if created:
            nb.unlink()
    assert r.success, r
    assert seen["parameters"]["since"] == "2026-01-03" and seen["parameters"]["n"] == 3
    assert seen["parameters"]["partition_start"] == "2026-01-03T00:00:00"


def test_feast_materializes_exactly_the_partition_window(monkeypatch):
    calls = []
    feast = types.ModuleType("feast")
    class FS:
        def __init__(self, repo_path): ...
        def materialize(self, **k): calls.append(("materialize", k))
        def materialize_incremental(self, **k): calls.append(("incremental", k))
    feast.FeatureStore = FS
    monkeypatch.setitem(sys.modules, "feast", feast)
    mod = _load("assets/analytics/feast_asset")
    comp = mod.FeastAssetComponent(asset_name="fe", feature_store_repo_path="/x", **DAILY)
    assert _run(_asset(comp)).success
    kind, kw = calls[-1]
    assert kind == "materialize" and kw["start_date"].replace(tzinfo=None) == datetime(2026, 1, 3) and kw["end_date"].replace(tzinfo=None) == datetime(2026, 1, 4)
    assert _run(_asset(mod.FeastAssetComponent(asset_name="fe", feature_store_repo_path="/x")), key=None).success
    assert calls[-1][0] == "incremental"


def test_tecton_bounds_the_job_to_the_partition_window(monkeypatch):
    jobs = []
    tecton = types.ModuleType("tecton")
    class Job: id = "j1"
    class FV:
        name = "fv"
        def run_materialization_job(self, **k): jobs.append(k); return Job()
    class WS:
        def get_feature_view(self, n): return FV()
        def list_feature_views(self): return [FV()]
    tecton.set_credentials = lambda **k: None
    tecton.get_workspace = lambda n: WS()
    monkeypatch.setitem(sys.modules, "tecton", tecton)
    monkeypatch.setenv("TECTON_API_KEY", "k")
    mod = _load("assets/analytics/tecton_asset")
    base = dict(asset_name="te", workspace="w")
    assert _run(_asset(mod.TectonAssetComponent(**base, **DAILY))).success
    assert jobs[-1]["start_time"].replace(tzinfo=None) == datetime(2026, 1, 3) and jobs[-1]["end_time"].replace(tzinfo=None) == datetime(2026, 1, 4)
    assert _run(_asset(mod.TectonAssetComponent(**base)), key=None).success
    assert "start_time" not in jobs[-1]
