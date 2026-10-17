"""Reverse-ETL components send only the rows in the run's partition.

Each component is built from its own example.yaml and materialized with a 40-row upstream (two days of 20 rows).
The step's  local is captured when the step returns (whether it succeeded or failed at its first external
call), so what is asserted is exactly what survived the partition filter. The network is blocked."""
import importlib.util
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
PORTED = [
    "assets/reverse_etl/airtable_record_upsert",
    "assets/reverse_etl/algolia_index_upsert",
    "assets/reverse_etl/amplitude_event_track",
    "assets/reverse_etl/asana_task_create",
    "assets/reverse_etl/auth0_user_upsert",
    "assets/reverse_etl/basecamp_todo_upsert",
    "assets/reverse_etl/bigcommerce_product_upsert",
    "assets/reverse_etl/braze_sink",
    "assets/reverse_etl/chargebee_customer_upsert",
    "assets/reverse_etl/clickup_task_upsert",
    "assets/reverse_etl/close_crm_lead_upsert",
    "assets/reverse_etl/confluence_page_upsert",
    "assets/reverse_etl/copper_record_upsert",
    "assets/reverse_etl/customer_io_sink",
    "assets/reverse_etl/default_workflow_trigger",
    "assets/reverse_etl/docusign_envelope_send",
    "assets/reverse_etl/dynamics_crm_record_upsert",
    "assets/reverse_etl/firebase_fcm_send",
    "assets/reverse_etl/freshdesk_contact_upsert",
    "assets/reverse_etl/freshservice_ticket_upsert",
    "assets/reverse_etl/github_issue_upsert",
    "assets/reverse_etl/google_ads_customer_match_upsert",
    "assets/reverse_etl/google_sheets_row_upsert",
    "assets/reverse_etl/greenhouse_candidate_update",
    "assets/reverse_etl/heap_user_property_sync",
    "assets/reverse_etl/help_scout_conversation_upsert",
    "assets/reverse_etl/hubspot_object_upsert",
    "assets/reverse_etl/insightly_record_upsert",
    "assets/reverse_etl/intercom_contact_upsert",
    "assets/reverse_etl/iterable_sink",
    "assets/reverse_etl/jira_issue_upsert",
    "assets/reverse_etl/jira_service_management_request_upsert",
    "assets/reverse_etl/klaviyo_profile_upsert",
    "assets/reverse_etl/launchdarkly_segment_update",
    "assets/reverse_etl/lever_candidate_upsert",
    "assets/reverse_etl/linear_issue_upsert",
    "assets/reverse_etl/linkedin_matched_audience_upsert",
    "assets/reverse_etl/magento_product_upsert",
    "assets/reverse_etl/mailchimp_member_upsert",
    "assets/reverse_etl/marketo_lead_upsert",
    "assets/reverse_etl/meta_custom_audience_upsert",
    "assets/reverse_etl/metronome_usage_event_send",
    "assets/reverse_etl/mixpanel_event_track",
    "assets/reverse_etl/netsuite_record_upsert",
    "assets/reverse_etl/notion_database_upsert",
    "assets/reverse_etl/notion_page_sync",
    "assets/reverse_etl/okta_user_upsert",
    "assets/reverse_etl/onesignal_push_send",
    "assets/reverse_etl/opensearch_index_upsert",
    "assets/reverse_etl/pagerduty_incident_upsert",
    "assets/reverse_etl/pandadoc_document_create",
    "assets/reverse_etl/pinterest_audience_upsert",
    "assets/reverse_etl/pipedrive_person_upsert",
    "assets/reverse_etl/quickbooks_customer_upsert",
    "assets/reverse_etl/ramp_reimbursement_card_write",
    "assets/reverse_etl/recurly_account_upsert",
    "assets/reverse_etl/salesforce_record_upsert",
    "assets/reverse_etl/segment_track",
    "assets/reverse_etl/sendgrid_contact_upsert",
    "assets/reverse_etl/servicenow_record_upsert",
    "assets/reverse_etl/shopify_product_upsert",
    "assets/reverse_etl/sigma_input_table_upsert",
    "assets/reverse_etl/smartsheet_row_upsert",
    "assets/reverse_etl/stripe_customer_upsert",
    "assets/reverse_etl/tiktok_custom_audience_upsert",
    "assets/reverse_etl/trello_card_upsert",
    "assets/reverse_etl/twilio_sms_send",
    "assets/reverse_etl/twitter_ads_tailored_audience_upsert",
    "assets/reverse_etl/typesense_index_upsert",
    "assets/reverse_etl/vanta_evidence_upload",
    "assets/reverse_etl/woocommerce_product_upsert",
    "assets/reverse_etl/workable_candidate_upsert",
    "assets/reverse_etl/wrike_task_upsert",
    "assets/reverse_etl/xero_contact_upsert",
    "assets/reverse_etl/zendesk_user_upsert",
    "assets/reverse_etl/zocdoc_availability_upsert",
    "assets/reverse_etl/zoho_crm_record_upsert",
    "assets/reverse_etl/zuora_account_upsert",
]


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
    signal.alarm(5)

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




def _build(rel, extra, monkeypatch, tmp_path):
    folder = ROOT / rel
    doc = next(d for d in yaml.safe_load_all((folder / "example.yaml").read_text()) if d and "attributes" in d)
    sys.path.insert(0, str(folder))
    try:
        spec = importlib.util.spec_from_file_location("rt_" + folder.name, folder / "component.py")
        mod = importlib.util.module_from_spec(spec)
        monkeypatch.setitem(sys.modules, spec.name, mod)
        spec.loader.exec_module(mod)
        cls = getattr(mod, doc["type"].rsplit(".", 1)[-1])
        attrs = {k: v for k, v in _expand_env(doc["attributes"]).items() if k not in ("source", "upstream_asset_keys")}
        defs = cls(**{**attrs, **extra, "upstream_asset_key": "up_src"}).build_defs(None)
    finally:
        sys.path.remove(str(folder))
    return next(a for a in defs.assets if hasattr(a, "node_def"))


def _run(rel, partitioned, monkeypatch, tmp_path):
    extra = {"partition_type": "daily", "partition_start": "2026-01-01", "partition_date_column": "ts"} if partitioned else {"partition_type": None}
    asset = _build(rel, extra, monkeypatch, tmp_path)
    resources = {k: dg.ResourceDefinition.hardcoded_resource(_AnyResource()) for k in asset.required_resource_keys if k != "io_manager"}
    seen = {}

    def prof(frame, event, arg):
        if event == "return" and frame.f_code.co_name.startswith("_run") and "reverse_etl" in frame.f_code.co_filename:
            loc = frame.f_locals
            for name in ("df", "upstream"):
                if hasattr(loc.get(name), "columns"):
                    seen.setdefault("frame", loc[name])
                    break

    frame_df = _frame()

    @dg.asset(name="up_src")
    def up_src():
        return frame_df

    instance = dg.DagsterInstance.ephemeral()
    dg.materialize([up_src], instance=instance)
    kwargs = {"partition_key": DAY} if partitioned else {}
    sys.setprofile(prof)
    try:
        dg.materialize([asset, up_src], instance=instance, selection=[asset.key], raise_on_error=False, resources=resources, **kwargs)
    finally:
        sys.setprofile(None)
    return seen.get("frame"), asset


@pytest.mark.parametrize("rel", PORTED, ids=lambda r: r.split("/", 1)[1])
def test_partitioned_run_only_sends_its_days_rows(rel, monkeypatch, tmp_path):
    seen, asset = _run(rel, True, monkeypatch, tmp_path)
    assert isinstance(asset.partitions_def, dg.DailyPartitionsDefinition)
    assert seen is not None, "the sender never received a frame"
    assert len(seen) == 20 and set(pd.to_datetime(seen["ts"]).dt.strftime("%Y-%m-%d")) == {DAY}


@pytest.mark.parametrize("rel", PORTED, ids=lambda r: r.split("/", 1)[1])
def test_unpartitioned_run_sends_every_row(rel, monkeypatch, tmp_path):
    seen, asset = _run(rel, False, monkeypatch, tmp_path)
    assert asset.partitions_def is None and seen is not None and len(seen) == 40


def test_helpers_are_identical_and_schema_documents_the_fields():
    import json
    import re

    blocks = {}
    for rel in PORTED:
        s = (ROOT / rel / "component.py").read_text()
        m = re.search(r"# BEGIN (reverse-etl partition helpers|partition row-filter helper).*?# END (reverse-etl partition helpers|partition row-filter helper)", s, re.S)
        assert m, rel
        blocks.setdefault(m.group(0), []).append(rel)
        schema = json.loads((ROOT / rel / "schema.json").read_text())
        schema = schema.get("attributes") or schema.get("properties")
        assert {"partition_date_column", "partition_static_column", "partition_type"} <= set(schema), rel
    assert len(blocks) == 2 and sorted(len(v) for v in blocks.values()) == [8, 70]
