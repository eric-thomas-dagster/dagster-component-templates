"""DataFrame → Notion page property sync.

Keeps a specific Notion page's properties (and optionally its markdown body)
in sync with the first row of an upstream DataFrame.

Property values are serialized based on the page's existing property types,
which Notion returns on `pages.retrieve`. Set a property to `None` to clear it.

Pairs with:
  - ``notion_resource`` — connection (required)
  - ``notion_database_upsert`` — multi-row analogue

For creating brand-new pages on every run, use the ``notion_resource``
directly from your own asset — that's not a materialization pattern.
"""
from typing import Dict, List, Optional

import dagster as dg
from pydantic import Field


def _serialize_property(value, existing_prop: dict) -> dict:
    """Serialize a Python value into a Notion property object.

    Uses the property's existing type (from `pages.retrieve`) to pick the
    right shape. Returns a partial property dict suitable for `pages.update`.
    """
    ptype = existing_prop.get("type")
    if value is None:
        # Explicit clear — different Notion types have different empty forms
        if ptype in ("title", "rich_text"):
            return {ptype: []}
        if ptype == "multi_select":
            return {"multi_select": []}
        return {ptype: None}

    if ptype == "title" or ptype == "rich_text":
        return {ptype: [{"type": "text", "text": {"content": str(value)}}]}
    if ptype == "number":
        try:
            return {"number": float(value)}
        except (TypeError, ValueError):
            return {"number": None}
    if ptype == "select":
        return {"select": {"name": str(value)}}
    if ptype == "multi_select":
        if isinstance(value, (list, tuple)):
            items = [str(v) for v in value]
        else:
            items = [s.strip() for s in str(value).split(",") if s.strip()]
        return {"multi_select": [{"name": v} for v in items]}
    if ptype == "checkbox":
        if isinstance(value, bool):
            return {"checkbox": value}
        return {"checkbox": str(value).lower() in ("true", "1", "yes", "y", "t")}
    if ptype == "date":
        # Accept ISO string, date, or datetime
        try:
            iso = value.isoformat() if hasattr(value, "isoformat") else str(value)
        except Exception:  # noqa: BLE001
            iso = str(value)
        return {"date": {"start": iso}}
    if ptype in ("url", "email", "phone_number"):
        return {ptype: str(value) if value else None}
    if ptype == "status":
        return {"status": {"name": str(value)}}
    # Fallback — write as rich_text stringified
    return {"rich_text": [{"type": "text", "text": {"content": str(value)}}]}


class NotionPageSyncComponent(dg.Component, dg.Model, dg.Resolvable):
    """Upsert a Notion page's properties from an upstream DataFrame.

    Example:
        ```yaml
        type: dagster_community_components.NotionPageSyncComponent
        attributes:
          asset_name: notion_kpi_dashboard
          upstream_asset_key: kpi_snapshot
          page_id: "abc123def456"
          resource_key: notion_resource
          properties_map:
            revenue: Revenue
            active_users: Active Users
            status: Status
          markdown_column: report_markdown
        ```
    """

    asset_name: str = Field(description="Output Dagster asset name.")
    upstream_asset_key: str = Field(description="Upstream asset providing the DataFrame.")
    page_id: str = Field(description="Notion page ID to keep in sync (UUID with or without dashes).")
    resource_key: str = Field(
        default="notion_resource",
        description="Resource key registered by NotionResourceComponent.",
    )

    properties_map: Dict[str, str] = Field(
        default_factory=dict,
        description=(
            "Upstream column → Notion property name. Values are serialized based on "
            "the page's existing property types (title, rich_text, number, select, "
            "multi_select, checkbox, date, url, email, phone_number, status). "
            "Leave empty to do a body-only sync via `markdown_column`."
        ),
    )
    markdown_column: Optional[str] = Field(
        default=None,
        description=(
            "Optional column whose value (from row 0) is written to the page body "
            "as markdown, replacing the current content."
        ),
    )
    row_index: int = Field(
        default=0,
        description=(
            "Which upstream row to sync from. Defaults to 0 (first row). "
            "A page_sync is single-page — pass a filtered upstream if you need a specific row."
        ),
    )

    group_name: Optional[str] = Field(default="notion", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds (auto-includes 'notion').")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("notion")

        @dg.asset(
            key=dg.AssetKey.from_user_string(_self.asset_name),
            ins={"upstream": dg.AssetIn(key=dg.AssetKey.from_user_string(_self.upstream_asset_key))},
            group_name=_self.group_name,
            kinds=kinds,
            owners=_self.owners,
            tags=_self.tags,
            required_resource_keys={_self.resource_key},
            description=_self.description or (
                f"Sync Notion page {_self.page_id} properties from upstream DataFrame."
            ),
        )
        def _asset(context: dg.AssetExecutionContext, upstream):
            notion = getattr(context.resources, _self.resource_key)

            # Coerce to DataFrame if it's not already (dict / list of dicts also ok)
            import pandas as pd
            if not isinstance(upstream, pd.DataFrame):
                if isinstance(upstream, dict):
                    df = pd.DataFrame([upstream])
                else:
                    df = pd.DataFrame(upstream)
            else:
                df = upstream

            if len(df) == 0:
                raise dg.Failure("Upstream DataFrame is empty — nothing to sync.")
            if len(df) > 1:
                context.log.warning(
                    f"Upstream has {len(df)} rows; syncing row_index={_self.row_index}. "
                    "Pass a filtered upstream if you meant to sync a specific row."
                )
            row = df.iloc[_self.row_index]

            # Retrieve page to get property types
            page = notion.get_page(_self.page_id)
            existing_props = page.get("properties") or {}

            # Serialize mapped columns → Notion properties
            new_props: dict = {}
            missing_cols = []
            missing_props = []
            for col, notion_prop in _self.properties_map.items():
                if col not in df.columns:
                    missing_cols.append(col)
                    continue
                if notion_prop not in existing_props:
                    missing_props.append(notion_prop)
                    continue
                value = row[col]
                # pandas NaN → None
                if isinstance(value, float) and pd.isna(value):
                    value = None
                new_props[notion_prop] = _serialize_property(value, existing_props[notion_prop])

            if missing_cols:
                raise dg.Failure(
                    f"Columns not in upstream: {missing_cols}. "
                    f"Available: {list(df.columns)}"
                )
            if missing_props:
                raise dg.Failure(
                    f"Properties not on page {_self.page_id}: {missing_props}. "
                    f"Available: {list(existing_props.keys())}"
                )

            if not new_props and not _self.markdown_column:
                raise dg.Failure(
                    "properties_map is empty and no markdown_column set — nothing to sync."
                )

            if new_props:
                updated = notion.update_page(page_id=_self.page_id, properties=new_props)
                context.log.info(
                    f"Notion page {_self.page_id} updated: {len(new_props)} properties patched."
                )
            else:
                updated = notion.get_page(_self.page_id)

            # Optional markdown body replace
            if _self.markdown_column:
                if _self.markdown_column not in df.columns:
                    raise dg.Failure(
                        f"markdown_column='{_self.markdown_column}' not in upstream. "
                        f"Available: {list(df.columns)}"
                    )
                md = row[_self.markdown_column]
                if md and not (isinstance(md, float) and pd.isna(md)):
                    notion.get_client().pages.update_markdown(
                        page_id=_self.page_id, markdown=str(md)
                    )
                    context.log.info("Notion page body replaced with markdown content.")

            page_url = updated.get("url", f"https://notion.so/{_self.page_id.replace('-', '')}")
            return dg.MaterializeResult(
                metadata={
                    "notion_page_id": dg.MetadataValue.text(_self.page_id),
                    "notion_page_url": dg.MetadataValue.url(page_url),
                    "properties_updated": dg.MetadataValue.int(len(new_props)),
                    "last_edited_time": dg.MetadataValue.text(
                        updated.get("last_edited_time", "")
                    ),
                }
            )

        return dg.Definitions(assets=[_asset])
