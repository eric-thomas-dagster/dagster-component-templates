"""Committed regression tests for NotionBriefPageComponent.

The real Notion API is never hit here -- `NotionResource.create_page` is
monkeypatched on a `FakeNotionResource` instance. Block-building logic,
title templating, the default-sections fallback, markdown-body paragraph
splitting, and the optional receipt-recording path (against a real
temp-file-backed ContextReceiptStore) are all exercised for real.
"""
import dagster as dg
import pytest

from .conftest import (
    FakeNotionResource,
    load_component_module,
    load_context_receipt_store_module,
    make_upstream_asset,
    metadata_for,
)


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize(component, upstream_value, resources):
    upstream_asset = make_upstream_asset("prospect_extract", upstream_value)
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    return dg.materialize([asset_def, upstream_asset], resources=resources)


# --- pure block-building helpers --------------------------------------------

def test_section_blocks_paragraph_scalar(mod):
    extracted = {"summary": "Everything is fine."}
    blocks = mod._section_blocks("Summary", "summary", "paragraph", extracted)
    assert blocks == [
        {"object": "block", "type": "heading_2", "heading_2": {"rich_text": [{"type": "text", "text": {"content": "Summary"}}]}},
        {"object": "block", "type": "paragraph", "paragraph": {"rich_text": [{"type": "text", "text": {"content": "Everything is fine."}}]}},
    ]


def test_section_blocks_paragraph_list_joins_with_newline(mod):
    extracted = {"notes": ["first", "second"]}
    blocks = mod._section_blocks("Notes", "notes", "paragraph", extracted)
    assert blocks[1]["paragraph"]["rich_text"][0]["text"]["content"] == "first\nsecond"


def test_section_blocks_bulleted_list(mod):
    extracted = {"risks": ["Budget cut", "Churn risk"]}
    blocks = mod._section_blocks("Key Risks", "risks", "bulleted_list", extracted)
    assert blocks[0]["type"] == "heading_2"
    assert blocks[1] == {
        "object": "block", "type": "bulleted_list_item",
        "bulleted_list_item": {"rich_text": [{"type": "text", "text": {"content": "Budget cut"}}]},
    }
    assert blocks[2]["bulleted_list_item"]["rich_text"][0]["text"]["content"] == "Churn risk"


def test_section_blocks_bulleted_list_requires_list_value(mod):
    extracted = {"risks": "not a list"}
    with pytest.raises(ValueError, match="bulleted_list"):
        mod._section_blocks("Key Risks", "risks", "bulleted_list", extracted)


def test_section_blocks_skips_missing_field(mod):
    assert mod._section_blocks("Heading", "absent_field", "paragraph", {}) == []


def test_default_sections_builds_one_per_remaining_key(mod):
    extracted = {"company_name": "Acme", "risks": ["r1", "r2"], "markdown_body": "skip me"}
    blocks = mod._default_sections(extracted, skip_field="markdown_body")
    # company_name -> heading + paragraph; risks -> heading + 2 bullets; markdown_body skipped
    headings = [b["heading_2"]["rich_text"][0]["text"]["content"] for b in blocks if b["type"] == "heading_2"]
    assert "Company Name" in headings
    assert "Risks" in headings
    assert all("Markdown Body" != h for h in headings)
    bullet_texts = [b["bulleted_list_item"]["rich_text"][0]["text"]["content"] for b in blocks if b["type"] == "bulleted_list_item"]
    assert bullet_texts == ["r1", "r2"]


def test_markdown_body_blocks_splits_on_blank_lines(mod):
    md = "Paragraph one.\n\nParagraph two.\n\n\nParagraph three."
    blocks = mod._markdown_body_blocks(md)
    texts = [b["paragraph"]["rich_text"][0]["text"]["content"] for b in blocks]
    assert texts == ["Paragraph one.", "Paragraph two.", "Paragraph three."]


def test_substitute_uses_str_replace_not_format(mod):
    # Literal braces unrelated to a known field must survive unresolved (str.replace semantics).
    out = mod._substitute("Brief: {company_name} {unrelated}", {"company_name": "Acme"})
    assert out == "Brief: Acme {unrelated}"


# --- full asset body, against the monkeypatched create_page ----------------

def test_create_page_called_with_expected_args(mod):
    resource = FakeNotionResource(page_id="page-1", url="https://notion.so/page-1")
    component = mod.NotionBriefPageComponent(
        asset_name="brief_out",
        upstream_asset_key="prospect_extract",
        parent_page_id="parent-page-id",
        title_template="Brief: {company_name}",
        sections=[
            {"heading": "Summary", "field": "summary", "format": "paragraph"},
            {"heading": "Risks", "field": "risks", "format": "bulleted_list"},
        ],
        icon_emoji="📄",
    )
    upstream_value = {
        "text": "...",
        "extracted": {"company_name": "Acme", "summary": "Doing fine.", "risks": ["r1", "r2"]},
        "model": "gpt-test",
    }
    result = _materialize(component, upstream_value, {"notion_resource": resource})

    assert len(resource.calls) == 1
    call = resource.calls[0]
    assert call["parent"] == {"page_id": "parent-page-id"}
    assert call["properties"] == {"title": {"title": [{"type": "text", "text": {"content": "Brief: Acme"}}]}}
    assert call["icon"] == {"type": "emoji", "emoji": "📄"}
    assert call["children"][0]["heading_2"]["rich_text"][0]["text"]["content"] == "Summary"
    assert call["children"][1]["paragraph"]["rich_text"][0]["text"]["content"] == "Doing fine."
    assert call["children"][2]["heading_2"]["rich_text"][0]["text"]["content"] == "Risks"
    assert call["children"][3]["bulleted_list_item"]["rich_text"][0]["text"]["content"] == "r1"

    md = metadata_for(result, "brief_out")
    assert md["notion_page_id"] == "page-1"
    assert md["notion_page_url"] == "https://notion.so/page-1"
    assert "receipt_id" not in md


def test_default_sections_used_when_sections_omitted(mod):
    resource = FakeNotionResource()
    component = mod.NotionBriefPageComponent(
        asset_name="brief_out",
        upstream_asset_key="prospect_extract",
        parent_page_id="parent-page-id",
        title_template="Brief: {company_name}",
    )
    upstream_value = {"extracted": {"company_name": "Acme", "summary": "Doing fine."}}
    _materialize(component, upstream_value, {"notion_resource": resource})
    headings = [
        b["heading_2"]["rich_text"][0]["text"]["content"]
        for b in resource.calls[0]["children"] if b["type"] == "heading_2"
    ]
    assert "Company Name" in headings
    assert "Summary" in headings


def test_markdown_body_field_appended_as_final_section(mod):
    resource = FakeNotionResource()
    component = mod.NotionBriefPageComponent(
        asset_name="brief_out",
        upstream_asset_key="prospect_extract",
        parent_page_id="parent-page-id",
        title_template="Brief: {company_name}",
        sections=[{"heading": "Summary", "field": "summary", "format": "paragraph"}],
    )
    upstream_value = {
        "extracted": {
            "company_name": "Acme",
            "summary": "Doing fine.",
            "markdown_body": "Free text one.\n\nFree text two.",
        }
    }
    _materialize(component, upstream_value, {"notion_resource": resource})
    children = resource.calls[0]["children"]
    # Last 3 blocks: the markdown_body heading + 2 paragraph blocks.
    assert children[-3]["heading_2"]["rich_text"][0]["text"]["content"] == "Markdown Body"
    assert children[-2]["paragraph"]["rich_text"][0]["text"]["content"] == "Free text one."
    assert children[-1]["paragraph"]["rich_text"][0]["text"]["content"] == "Free text two."


def test_missing_extracted_field_raises_clear_error(mod):
    resource = FakeNotionResource()
    component = mod.NotionBriefPageComponent(
        asset_name="brief_out",
        upstream_asset_key="prospect_extract",
        parent_page_id="parent-page-id",
        title_template="Brief: {company_name}",
    )
    upstream_value = {"not_extracted": {"company_name": "Acme"}}
    with pytest.raises(Exception, match="extracted_field"):
        _materialize(component, upstream_value, {"notion_resource": resource})


# --- receipt recording -------------------------------------------------------

def test_receipt_recorded_and_surfaced_in_metadata(mod, tmp_path):
    receipt_mod = load_context_receipt_store_module()
    store = receipt_mod.ContextReceiptStore(str(tmp_path / "receipts.sqlite3"))

    resource = FakeNotionResource(page_id="page-42", url="https://notion.so/page-42")
    component = mod.NotionBriefPageComponent(
        asset_name="brief_out",
        upstream_asset_key="prospect_extract",
        parent_page_id="parent-page-id",
        title_template="Brief: {company_name}",
        sections=[{"heading": "Summary", "field": "summary", "format": "paragraph"}],
        receipts={"resource_key": "context_receipt_store", "caller_id": "brief-agent", "policy_version": "v1"},
    )
    upstream_value = {"extracted": {"company_name": "Acme", "summary": "Doing fine."}}
    result = _materialize(
        component, upstream_value,
        {"notion_resource": resource, "context_receipt_store": store},
    )

    md = metadata_for(result, "brief_out")
    assert "receipt_id" in md
    receipt = store.get_receipt(md["receipt_id"])
    assert receipt is not None
    item_ids = [i["item_id"] for i in receipt["items"]]
    assert item_ids == ["notion_page:page-42"]
    assert receipt["caller_id"] == "brief-agent"
    assert receipt["policy_version"] == "v1"
