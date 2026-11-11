"""Real bugs fixed: a failed/empty-body doc emitted a null-text row into the output DataFrame
(nothing a downstream chunker could index) instead of being dropped; url/updated_at were never
emitted at all (Docs API itself has no modifiedTime -- needs a Drive API call); folder-level
selection required chaining a separate Drive-listing component. Mocks only the Google API
service objects (service_account.Credentials, googleapiclient build) -- the component's own
_extract_doc_text() parsing logic runs for real against a hand-built Docs-API-shaped body.
"""
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import dagster as dg

from .conftest import load_component_module

mod = load_component_module()
GoogleDocsExtractorComponent = mod.GoogleDocsExtractorComponent


def _fake_doc(title, text):
    return {
        "title": title,
        "body": {"content": [{"paragraph": {"elements": [{"textRun": {"content": text}}]}}]},
    }


def _component(**kw):
    attrs = dict(asset_name="docs", credentials={"client_email": "sa@example.com", "type": "service_account"})
    attrs.update(kw)
    return GoogleDocsExtractorComponent(**attrs)


def _materialize(comp, **kwargs):
    defs = comp.build_defs(None)
    return dg.materialize(defs.assets, **kwargs)


def _patched(docs_side_effect=None, drive_list_response=None, drive_get_side_effect=None):
    fake_docs_svc = MagicMock()
    fake_docs_svc.documents.return_value.get.return_value.execute.side_effect = docs_side_effect

    fake_drive_svc = MagicMock()
    if drive_list_response is not None:
        fake_drive_svc.files.return_value.list.return_value.execute.return_value = drive_list_response
    if drive_get_side_effect is not None:
        fake_drive_svc.files.return_value.get.return_value.execute.side_effect = drive_get_side_effect
    else:
        fake_drive_svc.files.return_value.get.return_value.execute.return_value = {"modifiedTime": "2026-01-01T00:00:00Z"}

    def _fake_build(service_name, version, credentials=None, cache_discovery=False):
        return fake_docs_svc if service_name == "docs" else fake_drive_svc

    return patch("google.oauth2.service_account.Credentials.from_service_account_info", return_value=MagicMock()), \
        patch("googleapiclient.discovery.build", side_effect=_fake_build)


def test_doc_ids_mode_emits_url_and_updated_at(tmp_path):
    comp = _component(doc_ids=["doc1"], rate_limit_delay=0)
    p1, p2 = _patched(docs_side_effect=[_fake_doc("Title One", "Hello world")])
    with p1, p2:
        result = _materialize(comp, raise_on_error=True)
    df = result.output_for_node("docs")
    assert len(df) == 1
    assert df.iloc[0]["url"] == "https://docs.google.com/document/d/doc1/edit"
    assert df.iloc[0]["updated_at"] == "2026-01-01T00:00:00Z"
    assert df.iloc[0]["title"] == "Title One"


def test_empty_body_doc_is_dropped_not_emitted_as_null_row(tmp_path):
    comp = _component(doc_ids=["empty_doc", "real_doc"], rate_limit_delay=0)
    p1, p2 = _patched(docs_side_effect=[_fake_doc("", ""), _fake_doc("Real", "content here")])
    with p1, p2:
        result = _materialize(comp, raise_on_error=True)
    df = result.output_for_node("docs")
    assert len(df) == 1
    assert df.iloc[0]["title"] == "Real"
    meta = result.get_asset_materialization_events()[-1].materialization.metadata
    assert meta["failed"].value == 1


def test_fetch_error_drops_the_doc_not_a_null_row(tmp_path):
    comp = _component(doc_ids=["bad_doc"], rate_limit_delay=0)
    p1, p2 = _patched(docs_side_effect=[Exception("404 not found")])
    with p1, p2:
        result = _materialize(comp, raise_on_error=True)
    df = result.output_for_node("docs")
    assert len(df) == 0
    meta = result.get_asset_materialization_events()[-1].materialization.metadata
    assert meta["failed"].value == 1


def test_drive_folder_id_mode_lists_and_extracts(tmp_path):
    comp = _component(drive_folder_id="folder123", rate_limit_delay=0)
    p1, p2 = _patched(
        docs_side_effect=[_fake_doc("Folder Doc", "content")],
        drive_list_response={"files": [{"id": "doc_in_folder", "modifiedTime": "2026-02-02T00:00:00Z"}], "nextPageToken": None},
    )
    with p1, p2:
        result = _materialize(comp, raise_on_error=True)
    df = result.output_for_node("docs")
    assert len(df) == 1
    assert df.iloc[0]["id"] == "doc_in_folder"
    assert df.iloc[0]["updated_at"] == "2026-02-02T00:00:00Z"


def test_exactly_one_input_mode_required_none_set():
    comp = _component()
    with pytest.raises(ValueError, match="exactly one of"):
        comp.build_defs(None)


def test_exactly_one_input_mode_required_two_set():
    comp = _component(doc_ids=["a"], drive_folder_id="f")
    with pytest.raises(ValueError, match="exactly one of"):
        comp.build_defs(None)


def test_upstream_asset_key_mode_still_works():
    comp = _component(upstream_asset_key="source_docs", id_column="doc_id", rate_limit_delay=0)

    @dg.asset(name="source_docs")
    def source_docs():
        return pd.DataFrame({"doc_id": ["doc1"]})

    defs = comp.build_defs(None)
    p1, p2 = _patched(docs_side_effect=[_fake_doc("From Upstream", "content")])
    with p1, p2:
        result = dg.materialize([source_docs, *defs.assets], raise_on_error=True)
    df = result.output_for_node("docs")
    assert len(df) == 1
    assert df.iloc[0]["title"] == "From Upstream"
