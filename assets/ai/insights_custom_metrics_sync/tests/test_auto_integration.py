"""Integration: REAL definitions from the four opt-in components + the sync component, run through the `presets: auto` code path
(repository scope, as in a Dagster+ code-location run) against the fake Dagster+ GraphQL server."""
import pathlib
import tempfile

import dagster as dg
import pytest

from .conftest import TOKEN, load_component_module

M = load_component_module()
AI = pathlib.Path(__file__).resolve().parents[2]
ROOT = AI.parents[1]
FOUR = ["context_engineering_pipeline", "receipts_sync", "receipt_outcome_ingestion", "rag_pipeline"]


def test_auto_discovers_exactly_the_four_components_presets(fake, monkeypatch, tmp_path):
    pytest.importorskip("pandas")
    from dagster._core.definitions.reconstruct import ReconstructableRepository

    loc = tmp_path / "loc.py"
    loc.write_text(f'''
import importlib.util, sys, dagster as dg

def load(path, name):
    spec = importlib.util.spec_from_file_location(name, path); m = importlib.util.module_from_spec(spec)
    sys.modules[name] = m; spec.loader.exec_module(m); return m

AI = {str(AI)!r}
cep = load(AI + "/context_engineering_pipeline/component.py", "auto_it_cep")
bq = load(AI + "/context_receipt_bigquery_sync/component.py", "auto_it_bq")
roi = load(AI + "/receipt_outcome_ingestion/component.py", "auto_it_roi")
rag = load(AI + "/rag_pipeline/component.py", "auto_it_rag")
ics = load(AI + "/insights_custom_metrics_sync/component.py", "auto_it_ics")

parts = [
    cep.ContextEngineeringPipelineComponent(asset_name="kb", upstream_asset_key="raw", id_column="id", text_column="body",
                                            steps=[{{"id": "chunks", "op": "chunk", "chunk_size": 200}}]),
    bq.ContextReceiptBigQuerySyncComponent(store_resource_key="receipts", bigquery_resource_key="bigquery", dataset="proj.ctx"),
    roi.ReceiptOutcomeIngestionComponent(source={{"kind": "file", "path": "/nonexistent/*.csv"}}, mapping={{"receipt_id": "rid", "value": "score"}}),
    rag.RAGPipelineComponent(asset_name="answers", upstream_asset_key="questions",
                             retrievers={{"kb": {{"kind": "chromadb", "path": "/nonexistent", "collection_name": "c"}}}},
                             steps=[{{"id": "found", "op": "retrieve", "retriever": "kb", "top_k": 2}}]),
    ics.InsightsCustomMetricsSyncComponent(deployment_url={fake.url!r}, presets=["auto"], dry_run=False),
]
assets = [dg.AssetSpec("raw"), dg.AssetSpec("questions"), *[a for p in parts for a in p.build_defs(None).assets]]
stub = dg.ResourceDefinition.hardcoded_resource(object())
defs = dg.Definitions(assets=assets, resources={{"receipts": stub, "bigquery": stub, "context_receipts": stub}}, executor=dg.in_process_executor)
''')
    monkeypatch.setenv("DAGSTER_CLOUD_API_TOKEN", TOKEN)
    inst = dg.DagsterInstance.local_temp(tempfile.mkdtemp())
    job = ReconstructableRepository.for_file(str(loc), "defs", working_directory=str(tmp_path)).get_reconstructable_job("__ASSET_JOB")
    res = dg.execute_job(job, inst, asset_selection=[dg.AssetKey("insights_custom_metrics_sync")])
    assert res.success
    md = res.get_asset_materialization_events()[0].materialization.metadata
    assert md["auto_scope"].value == "repository"
    assert md["auto_discovered"].value == sorted(FOUR)
    assert md["auto_unknown_tag_values"].value == []
    expected = sorted({m["metadata_key"] for p in FOUR for m in M.PRESETS[p]})
    assert sorted(m["metadataKey"] for m in fake.metrics.values()) == expected
    assert "agentic_cost_usd" not in expected  # agentic preset was not opted in here
