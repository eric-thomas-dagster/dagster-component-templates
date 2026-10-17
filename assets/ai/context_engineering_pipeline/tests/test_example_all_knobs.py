"""Runs example_all_knobs.yaml end to end, so the documented example is always a working pipeline.
Real chromadb and Dagster; embedding and completion are faked (offline, deterministic)."""
import hashlib
import importlib.util
import re
from pathlib import Path
from unittest.mock import MagicMock, patch

import dagster as dg
import pandas as pd
import pytest
import yaml

from .conftest import load_component_module, requires_chromadb

pytestmark = requires_chromadb
HERE = Path(__file__).parents[1]

EXPORT_DOC = """# Reports

## Exporting a report
To export a report open the report, choose Export, then pick CSV or PDF. Exports run in the background and the file
appears in your downloads when ready. Large exports can take a few minutes, and you will get an email when it finishes.
If an export seems stuck, contact help@acme.com and include the report name and the time you started the export.

## Scheduling exports
You can schedule a report export to run every day or week from the Schedule tab. Scheduled exports are delivered to your
inbox as attachments, and the schedule can be paused at any time without losing the report definition itself.
"""

BILLING_DOC = """# Billing

## Duplicate charges
If you were charged twice for the same subscription period, check whether a second workspace was created by accident.
Duplicate charges are refunded automatically within five business days once the second workspace is closed down.

## Duplicate charges
If you were charged twice for the same subscription period, check whether a second workspace was created by accident.
Duplicate charges are refunded automatically within five business days once the second workspace is closed down.

## Invoices
Invoices are emailed on the first day of each month and can also be downloaded from the Billing page at any time.

Unsubscribe from this list at any time. All rights reserved.
"""

SHIPPING_DOC = """# Shipping

## Delivery times
Standard shipping takes five to seven business days. Express shipping takes two business days and is available for most
destinations. Tracking numbers are emailed as soon as the parcel leaves the warehouse and update every few hours.
"""


def _embed(model, input, api_key=None):
    """Bag-of-hashed-words: similar texts land near each other, deterministically."""
    resp = MagicMock()
    vectors = []
    for text in input:
        v = [0.0] * 64
        for word in re.findall(r"[a-z]+", text.lower()):
            v[int(hashlib.md5(word.encode()).hexdigest(), 16) % 64] += 1.0
        norm = sum(x * x for x in v) ** 0.5 or 1.0
        vectors.append([x / norm for x in v])
    resp.data = [{"embedding": v} for v in vectors]
    return resp


def _completion(**kwargs):
    messages = kwargs["messages"]
    resp = MagicMock()
    text = "Support documentation chunk." if messages[0]["role"] == "system" else '{"category": "technical"}'
    resp.choices = [MagicMock(message=MagicMock(content=text))]
    return resp


@pytest.fixture()
def mod():
    return load_component_module()


def _config(tmp_path):
    cfg = yaml.safe_load((HERE / "example_all_knobs.yaml").read_text())["attributes"]
    for step in cfg["steps"]:
        if step["op"] == "write_vector_store":
            step["connection_string"] = str(tmp_path / "kb")
        if step["op"] == "embed":
            step["reuse_from"]["connection_string"] = str(tmp_path / "kb")
    return cfg


def _run(component, instance):
    asset_def = list(component.build_defs(None).assets)[0]
    frame = pd.DataFrame([
        ("export_guide", "acme", "Export guide", EXPORT_DOC),
        ("billing_faq", "acme", "Billing FAQ", BILLING_DOC),
        ("shipping_info", "globex", "Shipping", SHIPPING_DOC),
    ], columns=["doc_id", "customer_id", "title", "body"])

    @dg.asset(name="raw_support_docs")
    def raw():
        return frame
    return dg.materialize([asset_def, raw], instance=instance)


def test_the_documented_all_knobs_pipeline_runs_end_to_end(mod, tmp_path):
    import chromadb
    component = mod.ContextEngineeringPipelineComponent(**_config(tmp_path))
    instance = dg.DagsterInstance.ephemeral()
    with patch("litellm.embedding", side_effect=_embed), patch("litellm.completion", side_effect=_completion):
        result = _run(component, instance)
    assert result.success

    # the retrieval gate ran against the index it just wrote
    checks = {e.check_name: e for e in result.get_asset_check_evaluations()}
    assert checks["retrieval_quality"].passed
    assert checks["retrieval_quality"].metadata["recall_at_k"].value == 1.0

    got = chromadb.PersistentClient(path=str(tmp_path / "kb")).get_collection("support_kb").get(include=["metadatas", "documents"])
    docs, metas = got["documents"], got["metadatas"]
    assert docs and all(i.startswith("support_kb:") for i in got["ids"])                        # chunk_id_prefix
    assert all(m["parent_text"] and m["parent_id"] for m in metas)                               # parent-child
    assert all(m["source_asset_key"] == "support_kb" and m["source_code_version"] == "kb-recipe-3" for m in metas)
    assert all(m["valid_until"] and m["scope"] == m["customer_id"] and m["access"] == "support" for m in metas)  # stamp
    assert all(m["category"] == "technical" for m in metas)                                      # classify
    assert all(d.startswith("Support documentation chunk.") for d in docs)                       # enrich + prepend
    assert any("[Export guide]" in d for d in docs)                                              # header_template
    text = " ".join(docs)
    assert "help@acme.com" not in text and "[EMAIL]" in text                                    # sensitivity: redact
    assert "nsubscribe" not in text                                                              # filter: boilerplate gone
    assert sum("charged twice" in d for d in docs) == 1                                         # dedupe: duplicate section once

    # identity: asset code version is what the YAML said, and a no-op rebuild keeps the data version
    def tags():
        t = instance.get_latest_data_version_record(dg.AssetKey("support_kb")).asset_materialization.tags
        return t["dagster/code_version"], t["dagster/data_version"]
    first = tags()
    with patch("litellm.embedding", side_effect=_embed), patch("litellm.completion", side_effect=_completion):
        again = _run(component, instance)
    assert again.success and tags() == first and first[0] == "kb-recipe-3"


def test_the_declared_policy_is_discoverable(mod, tmp_path):
    path = Path(__file__).parents[4] / "resources/context_receipt_store/component.py"
    spec = importlib.util.spec_from_file_location("receipts_for_all_knobs", path)
    receipts = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(receipts)
    component = mod.ContextEngineeringPipelineComponent(**_config(tmp_path))
    merged = dg.Definitions.merge(component.build_defs(None), dg.Definitions(assets=[dg.AssetSpec("raw_support_docs")]))
    policy = receipts.context_policies(merged)["support_kb"]
    assert policy.scope == "customer_id" and policy.sensitivity == "confidential"
    assert policy.token_budget == 4000 and policy.retention_days == 365 and policy.access_tags == ("support",)
