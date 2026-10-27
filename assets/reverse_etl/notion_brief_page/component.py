"""LLM-extraction dict → brand-new, freeform, multi-section Notion page.

Creates a NEW standalone page under a fixed parent page on every materialization,
built from the structured fields an upstream `agentic_pipeline` `extract` step (or
any step producing a dict with a nested structured-fields sub-dict) emits.

This fills a real gap the other two Notion reverse-ETL components don't cover:
  - `notion_page_sync`        — UPDATE-ONLY against one fixed, already-existing
                                 page_id. Its own docstring says outright: "For
                                 creating brand-new pages on every run, use the
                                 `notion_resource` directly from your own asset --
                                 that's not a materialization pattern." No body/block
                                 content support beyond one markdown blob replace.
  - `notion_database_upsert`  — flat tabular property-upsert into a database. No
                                 block/body content at all.

This component builds the real Notion block JSON (headings, paragraphs, bulleted
lists) for a brand-new page every run, via `NotionResource.create_page` directly.

Pairs with:
  - `notion_resource`       — connection (required)
  - `context_receipt_store` — optional provenance recording after a successful
                               page creation
  - `agentic_pipeline`'s `extract` op — typical upstream: its output asset's value
                               is `{"text": ..., "extracted": {...fields...}, "model": ...}`;
                               this component reads the `extracted` sub-key (configurable
                               via `extracted_field`, default "extracted" -- confirmed
                               against `_do_extract`'s actual return dict).
"""
from typing import Any, Dict, List, Optional, Union

import dagster as dg
from pydantic import Field, model_validator


# BEGIN upstream-asset-key normalization (inlined per component; keep copies identical — see tests/test_upstream_keys.py)
def normalize_upstream_keys(single, plural=None) -> List[str]:
    """Merge ``upstream_asset_key`` / ``upstream_asset_keys`` into one clean list.

    Each argument may be a str, a list/tuple of str, or None. Entries are
    stripped, empties dropped, and duplicates removed (order preserved).
    """
    out: List[str] = []
    for value in (single, plural):
        if value is None:
            continue
        items = [value] if isinstance(value, str) else value
        if not isinstance(items, (list, tuple)):
            raise ValueError(
                f"upstream asset key(s) must be a string or a list of strings, got {value!r}"
            )
        for item in items:
            if not isinstance(item, str):
                raise ValueError(f"upstream asset keys must be strings, got {item!r}")
            item = item.strip()
            if item and item not in out:
                out.append(item)
    return out


def coerce_upstream_input(data, *, component: str, mode: str, optional: bool = True):
    """Rewrite raw component input so both spellings/shapes land on the declared field(s).

    mode "single": declared field is ``upstream_asset_key`` (str); >1 key is an error.
    mode "multi":  declared field is ``upstream_asset_keys`` (list).
    mode "both":   both fields are declared; everything is merged into the plural one.
    """
    if not isinstance(data, dict):
        return data
    if "upstream_asset_key" not in data and "upstream_asset_keys" not in data:
        return data
    keys = normalize_upstream_keys(data.get("upstream_asset_key"), data.get("upstream_asset_keys"))
    out = {k: v for k, v in data.items() if k not in ("upstream_asset_key", "upstream_asset_keys")}
    if mode == "single":
        if len(keys) > 1:
            raise ValueError(f"{component} takes exactly one upstream asset, got {keys}")
        out["upstream_asset_key"] = keys[0] if keys else (None if optional else "")
    else:
        out["upstream_asset_keys"] = keys or (None if optional else [])
    return out
# END upstream-asset-key normalization


# BEGIN field-shape normalization (inlined per component; keep copies identical — see tests/test_field_shapes.py)
def coerce_field_shapes(data, *, lists=(), csv=(), tags=()):
    """Accept the shapes people (and LLMs) naturally write for common boilerplate fields."""
    if not isinstance(data, dict):
        return data
    out = dict(data)
    for k in lists:  # List[str]: "a" / "a, b" -> ["a", "b"]; strip + drop empties
        v = out.get(k)
        if isinstance(v, str):
            v = v.split(",")
        if isinstance(v, (list, tuple)):
            out[k] = [x.strip() if isinstance(x, str) else x for x in v if not (isinstance(x, str) and not x.strip())]
    for k in csv:  # comma-separated str given as a list -> "a,b"
        v = out.get(k)
        if isinstance(v, (list, tuple, set)):
            out[k] = ",".join(str(x).strip() for x in v)
    for k in tags:  # Dict[str, str]: scalars -> str, ["k=v"] / ["k:v"] -> {k: v}
        v = out.get(k)
        if isinstance(v, (list, tuple)) and v and all(isinstance(x, str) and ("=" in x or ":" in x) for x in v):
            v = dict((p.strip() for p in (x.split("=", 1) if "=" in x else x.split(":", 1))) for x in v)
        if isinstance(v, dict):
            out[k] = {str(a).strip(): (str(b).lower() if isinstance(b, bool) else str(b)) for a, b in v.items() if b is not None}
    return out
# END field-shape normalization


def _substitute(template: str, fields: Dict[str, Any]) -> str:
    """Template a string against `fields` via plain `str.replace("{"+k+"}", ...)`.

    NOT `.format` -- this repo's established templating convention elsewhere
    (agentic_pipeline, polars_pipeline, ticket_classifier, ml_pipeline, ...) so
    literal braces unrelated to a known field survive unresolved instead of
    raising a KeyError/IndexError.
    """
    out = template
    for k, v in fields.items():
        out = out.replace("{" + k + "}", str(v))
    return out


def _rich_text(content: str) -> List[dict]:
    return [{"type": "text", "text": {"content": content}}]


def _heading_block(text: str) -> dict:
    return {"object": "block", "type": "heading_2", "heading_2": {"rich_text": _rich_text(text)}}


def _paragraph_block(text: str) -> dict:
    return {"object": "block", "type": "paragraph", "paragraph": {"rich_text": _rich_text(text)}}


def _bulleted_list_item_block(text: str) -> dict:
    return {
        "object": "block",
        "type": "bulleted_list_item",
        "bulleted_list_item": {"rich_text": _rich_text(text)},
    }


def _render_paragraph_value(value: Any) -> str:
    """List -> newline-joined; scalar -> str(value)."""
    if isinstance(value, (list, tuple)):
        return "\n".join(str(v) for v in value)
    return str(value)


def _section_blocks(heading: str, field: str, fmt: str, extracted: Dict[str, Any]) -> List[dict]:
    """Build a heading_2 block plus its content block(s) for one section spec."""
    if field not in extracted:
        return []
    value = extracted[field]
    blocks: List[dict] = [_heading_block(heading)]
    if fmt == "bulleted_list":
        if not isinstance(value, (list, tuple)):
            raise ValueError(
                f"notion_brief_page: section field={field!r} has format='bulleted_list' "
                f"but its value is not a list (got {type(value).__name__})."
            )
        for item in value:
            blocks.append(_bulleted_list_item_block(str(item)))
    else:  # "paragraph" (default)
        blocks.append(_paragraph_block(_render_paragraph_value(value)))
    return blocks


def _default_sections(extracted: Dict[str, Any], skip_field: Optional[str], log=None) -> List[dict]:
    """Build one section per remaining key in `extracted` when `sections` is omitted.

    Lists render as bulleted_list, everything else as paragraph. The upstream
    extraction schema isn't rigid, so this logs which fields it found.
    """
    blocks: List[dict] = []
    found = []
    for key, value in extracted.items():
        if skip_field and key == skip_field:
            continue
        found.append(key)
        heading = key.replace("_", " ").title()
        fmt = "bulleted_list" if isinstance(value, (list, tuple)) else "paragraph"
        blocks.extend(_section_blocks(heading, key, fmt, extracted))
    if log is not None:
        log.info(f"notion_brief_page: no `sections` configured; built default sections from extracted fields: {found}")
    return blocks


def _markdown_body_blocks(markdown: str) -> List[dict]:
    """Crude markdown-to-blocks: split on blank lines into separate paragraph blocks.

    This is NOT a markdown parser -- headings/lists/links inside the text are left
    as literal characters in a paragraph's rich_text. It only splits on `\\n\\n`.
    """
    blocks: List[dict] = []
    for chunk in markdown.split("\n\n"):
        chunk = chunk.strip()
        if chunk:
            blocks.append(_paragraph_block(chunk))
    return blocks


class NotionBriefPageComponent(dg.Component, dg.Model, dg.Resolvable):
    """Create a brand-new, freeform, multi-section Notion page from an LLM-extraction dict.

    Every materialization creates a NEW page under `parent_page_id` -- this is NOT an
    update-in-place pattern (see `notion_page_sync` for that) and NOT a flat database
    row upsert (see `notion_database_upsert` for that).

    Example:
        ```yaml
        type: dagster_community_components.NotionBriefPageComponent
        attributes:
          asset_name: prospect_brief_page
          upstream_asset_key: prospect_extract
          parent_page_id: "abc123def456"
          resource_key: notion_resource
          title_template: "Brief: {company_name}"
          sections:
            - heading: Summary
              field: summary
              format: paragraph
            - heading: Key Risks
              field: risks
              format: bulleted_list
          icon_emoji: "📄"
        ```
    """

    asset_name: str = Field(description="Output Dagster asset name.")
    upstream_asset_key: Union[str, List[str]] = Field(
        description="Upstream Dagster asset producing the LLM-extraction dict (single upstream only)."
    )
    extracted_field: str = Field(
        default="extracted",
        description=(
            "Key of the upstream dict holding the structured fields dict. Matches "
            "agentic_pipeline's `extract` op output shape: {text, extracted, model, ...}."
        ),
    )
    resource_key: str = Field(
        default="notion_resource",
        description="Resource key registered by NotionResourceComponent.",
    )
    parent_page_id: str = Field(
        description="Notion page ID the new page is created under (UUID with or without dashes). Standalone-page target only -- no database/property-schema support."
    )
    title_template: str = Field(
        description=(
            "Template for the new page's title, substituted against the extracted dict's "
            "fields via plain str.replace('{field}', value) -- not .format -- so unrelated "
            "literal braces survive unresolved. Example: 'Brief: {company_name}'."
        )
    )
    sections: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description=(
            "Explicit section list, each {heading: str, field: str, format: 'paragraph'|'bulleted_list'}. "
            "`field` names a key in the extracted dict. paragraph renders the value as one paragraph "
            "block under a heading_2 (lists are newline-joined); bulleted_list requires a list value and "
            "renders one bulleted_list_item block per element. Omit/empty to auto-build sections from "
            "whichever keys are present in the extracted dict."
        ),
    )
    markdown_body_field: Optional[str] = Field(
        default="markdown_body",
        description=(
            "Key in the extracted dict holding free-form markdown to append as a final section, if "
            "present. Rendered via a crude paragraph-per-blank-line split -- not full markdown parsing. "
            "Set to null to disable."
        ),
    )
    icon_emoji: Optional[str] = Field(
        default=None,
        description="Emoji to set as the new page's icon, e.g. '📄'. Passed as icon={type: emoji, emoji: ...}.",
    )
    receipts: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Optional provenance recording: {resource_key: str, caller_id: str|null, policy_version: str|null}. "
            "When set, records a context_receipt_store retrieval receipt after page creation succeeds, and "
            "surfaces the resulting receipt_id in output metadata."
        ),
    )

    description: Optional[str] = Field(default=None, description="Asset description.")
    group_name: Optional[str] = Field(default="notion", description="Dagster asset group name.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds (auto-includes 'notion').")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=("owners", "kinds"), csv=(), tags=("tags",))

    @model_validator(mode="before")
    @classmethod
    def _coerce_upstream_asset_keys(cls, data):
        return coerce_upstream_input(data, component=cls.__name__, mode="single", optional=False)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        if not self.parent_page_id or not str(self.parent_page_id).strip():
            raise ValueError("NotionBriefPageComponent: parent_page_id must be a non-empty string.")

        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("notion")

        required_rks = {self.resource_key}
        receipts_resource_key = None
        if self.receipts:
            receipts_resource_key = self.receipts.get("resource_key")
            if not receipts_resource_key:
                raise ValueError("NotionBriefPageComponent: receipts.resource_key is required when receipts is set.")
            required_rks.add(receipts_resource_key)

        def _build_children(extracted: Dict[str, Any], log=None) -> List[dict]:
            children: List[dict] = []
            if _self.sections:
                for spec in _self.sections:
                    heading = spec.get("heading") or spec.get("field")
                    field = spec.get("field")
                    fmt = spec.get("format", "paragraph")
                    if not field:
                        raise ValueError(f"notion_brief_page: section spec missing 'field': {spec!r}")
                    children.extend(_section_blocks(heading, field, fmt, extracted))
            else:
                children.extend(_default_sections(extracted, _self.markdown_body_field, log=log))

            if _self.markdown_body_field and _self.markdown_body_field in extracted:
                md = extracted[_self.markdown_body_field]
                if md:
                    children.append(_heading_block(_self.markdown_body_field.replace("_", " ").title()))
                    children.extend(_markdown_body_blocks(str(md)))

            return children

        @dg.asset(
            key=dg.AssetKey.from_user_string(_self.asset_name),
            ins={"upstream": dg.AssetIn(key=dg.AssetKey.from_user_string(_self.upstream_asset_key))},
            required_resource_keys=required_rks,
            group_name=_self.group_name,
            kinds=kinds,
            owners=_self.owners,
            tags=_self.tags,
            description=_self.description or (
                f"Create a new Notion page under {_self.parent_page_id} from an LLM-extraction dict."
            ),
        )
        def _asset(context: dg.AssetExecutionContext, upstream):
            if not isinstance(upstream, dict) or _self.extracted_field not in upstream:
                available = list(upstream.keys()) if isinstance(upstream, dict) else type(upstream).__name__
                raise KeyError(
                    f"notion_brief_page: upstream asset value is missing extracted_field={_self.extracted_field!r}. "
                    f"Available: {available}. Expected the upstream to be a dict (e.g. agentic_pipeline's `extract` "
                    "op output: {text, extracted, model, ...}) containing this key."
                )
            extracted = upstream[_self.extracted_field]
            if not isinstance(extracted, dict):
                raise ValueError(
                    f"notion_brief_page: upstream[{_self.extracted_field!r}] must be a dict of structured "
                    f"fields, got {type(extracted).__name__}."
                )

            title = _substitute(_self.title_template, extracted)
            children = _build_children(extracted, log=context.log)

            notion = getattr(context.resources, _self.resource_key)
            icon = {"type": "emoji", "emoji": _self.icon_emoji} if _self.icon_emoji else None

            page = notion.create_page(
                parent={"page_id": _self.parent_page_id},
                properties={"title": {"title": [{"type": "text", "text": {"content": title}}]}},
                children=children,
                icon=icon,
            )
            context.log.info(f"notion_brief_page: created page {page.get('id')} ({len(children)} blocks).")

            receipt_id = None
            if _self.receipts:
                store = getattr(context.resources, receipts_resource_key)
                receipt_id = store.record_retrieval(
                    context_name=f"prospect_recon:{_self.asset_name}",
                    items=[
                        {
                            "item_id": f"notion_page:{page['id']}",
                            "source_asset_key": _self.upstream_asset_key,
                            "rank": 1,
                            "selection": "ranked",
                        }
                    ],
                    caller_id=_self.receipts.get("caller_id"),
                    policy_version=_self.receipts.get("policy_version"),
                    dagster_asset_key=_self.asset_name,
                    dagster_run_id=context.run_id,
                )

            metadata = {
                "notion_page_id": dg.MetadataValue.text(page["id"]),
                "notion_page_url": dg.MetadataValue.url(page.get("url", "")),
                "blocks_written": dg.MetadataValue.int(len(children)),
            }
            if receipt_id is not None:
                metadata["receipt_id"] = dg.MetadataValue.text(receipt_id)

            return dg.MaterializeResult(metadata=metadata)

        return dg.Definitions(assets=[_asset])
