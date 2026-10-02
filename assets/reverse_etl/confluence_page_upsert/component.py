"""DataFrame -> Confluence Cloud page upsert (match on space + title).

Mirrors an upstream DataFrame into Confluence pages, one row per page,
matched on `(space_id, title)` -- Confluence has no native upsert endpoint
on pages, so this is a search-then-write pattern: `GET /pages?space-id=..&
title=..` to find an existing page, then `PUT` (update) or `POST` (create).

This is the "Shopify/Salesforce shape" (loop over every DataFrame row,
batch-upsert many pages in one run) rather than the `notion_page_sync`
shape (sync exactly one already-known page_id from row 0) -- Confluence
pages are usually matched by title within a space rather than a
pre-known page id, so a realistic sink needs to handle many rows per run.

Version-number gotcha: Confluence's v2 `PUT /pages/{id}` requires
`version.number` to be exactly the page's current version + 1
(optimistic-concurrency protection). All of that sequencing lives in
`ConfluenceResource.upsert_page_by_title` (re-fetches the page to read its
current version before every update) -- this component just calls it.

Body-format limitation: page bodies are Confluence's "storage format"
(XHTML-based wiki markup), not Markdown and not arbitrary HTML. By default
(`wrap_body_as_html: true`), this component treats `body_column` as plain
text and wraps it in escaped `<p>` paragraphs -- a simple, safe default
with no rich formatting. Set `wrap_body_as_html: false` to pass pre-built
storage-format XHTML through unchanged (tables, macros, layouts, etc.).

Pairs with:
  - ``confluence_resource`` -- connection + auth + workhorse HTTP (required)
"""
import html as _html
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


def _to_storage_format(text: str) -> str:
    """Wrap plain text into Confluence storage-format XHTML.

    Splits on blank lines into `<p>` paragraphs, HTML-escaping content and
    turning single newlines into `<br/>`. This is a deliberately simple,
    safe default -- it does not support rich formatting (tables, macros,
    layouts). For those, set `wrap_body_as_html: false` and pass pre-built
    storage-format XHTML directly in `body_column`.
    """
    if not text or not text.strip():
        return "<p></p>"
    paragraphs = [p for p in text.split("\n\n") if p.strip()] or [text]
    return "".join(
        f"<p>{_html.escape(p).replace(chr(10), '<br/>')}</p>" for p in paragraphs
    )


class ConfluencePageUpsertComponent(dg.Component, dg.Model, dg.Resolvable):
    """Batch-upsert rows from an upstream DataFrame into Confluence Cloud pages.

    Example:
        ```yaml
        type: dagster_component_templates.ConfluencePageUpsertComponent
        attributes:
          asset_name: confluence_release_notes_sync
          upstream_asset_key: dbt_marts_release_notes
          resource_key: confluence
          space_id: "98765"
          fields_map:
            page_title: title
          body_column: notes_body
          batch_size: 500
        ```

    For every upstream row: looks up a page by `(space_id, title)`. If
    found, re-fetches it to read the current version and `PUT`s with
    `version.number = current + 1`. If not found, `POST`s a new page.
    """

    asset_name: str = Field(description="Output Dagster asset name.")

    # Two source shapes -- supply exactly one.
    upstream_asset_key: Optional[str] = Field(
        default=None,
        description=(
            "Upstream Dagster asset providing the DataFrame. Mutually exclusive "
            "with `source:`."
        ),
    )
    source: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Inline source config. Mutually exclusive with `upstream_asset_key`. "
            "Shapes: {kind: sql, resource_key/database_url_env_var, query}, "
            "{kind: csv, path, read_csv_kwargs}, {kind: inline, rows}."
        ),
    )

    resource_key: str = Field(
        default="confluence",
        description="Resource key registered by ConfluenceResourceComponent.",
    )

    space_id: str = Field(
        description="Confluence space id to scope pages to (numeric space id, not the space key).",
    )
    fields_map: Dict[str, str] = Field(
        description=(
            "Upstream column -> Confluence field name. Must include exactly one "
            "mapping to the Confluence field 'title' -- that column is the match "
            "key within the space. (Confluence pages only have a title to match "
            "on; the page body itself comes from `body_column`, not fields_map.)"
        ),
    )
    body_column: str = Field(
        description=(
            "Upstream column holding the page's body content. By default "
            "(`wrap_body_as_html: true`) treated as plain text and wrapped in "
            "escaped <p> paragraphs; set `wrap_body_as_html: false` to pass "
            "pre-built Confluence storage-format XHTML through unchanged."
        ),
    )
    wrap_body_as_html: bool = Field(
        default=True,
        description=(
            "If true (default), `body_column` is treated as plain text and "
            "wrapped into simple escaped <p> paragraphs (storage format). If "
            "false, `body_column` is assumed to already be valid Confluence "
            "storage-format XHTML and is passed through unchanged -- needed for "
            "rich formatting (tables, macros, layouts)."
        ),
    )
    batch_size: int = Field(
        default=5000,
        description="Max upstream rows per run (safety cap).",
    )

    group_name: Optional[str] = Field(default="confluence", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds (auto-includes 'confluence').")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("confluence")

        # Validate: exactly one of upstream_asset_key OR source: must be set.
        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError(
                "ConfluencePageUpsertComponent: supply exactly one of "
                "`upstream_asset_key` OR `source:` (got both or neither)."
            )

        # Validate fields_map maps some column to the Confluence 'title' field.
        if "title" not in self.fields_map.values():
            raise ValueError(
                "ConfluencePageUpsertComponent: fields_map must include a mapping "
                "to the Confluence field 'title' (the match key within the "
                f"space). fields_map values: {sorted(self.fields_map.values())}"
            )

        use_source = self.source is not None
        extra_rks: set = set()
        if use_source and (self.source.get("kind") or "").lower() == "sql":
            _rk = self.source.get("resource_key")
            if _rk:
                extra_rks.add(_rk)

        # ── Source resolver (self-contained per no-shared-code rule) ──────
        def _resolve_source_df(exec_ctx):
            import pandas as pd
            src = _self.source or {}
            kind = (src.get("kind") or "").lower()
            if kind == "sql":
                query = src.get("query")
                if not query:
                    raise ValueError("source kind=sql requires 'query'")
                rk = src.get("resource_key")
                if rk:
                    resource = getattr(exec_ctx.resources, rk)
                    if hasattr(resource, "get_engine"):
                        return pd.read_sql(query, resource.get_engine())
                    if hasattr(resource, "get_connection"):
                        with resource.get_connection() as conn:
                            if hasattr(conn, "execute") and hasattr(conn, "df"):
                                return conn.execute(query).df()
                            return pd.read_sql(query, conn)
                    raise ValueError(f"source kind=sql: resource {rk!r} must expose .get_engine() or .get_connection()")
                env = src.get("database_url_env_var")
                if env:
                    import os
                    from sqlalchemy import create_engine
                    url = os.environ.get(env, "")
                    if not url:
                        raise ValueError(f"database_url_env_var {env!r} is unset")
                    return pd.read_sql(query, create_engine(url))
                raise ValueError("source kind=sql requires 'resource_key' OR 'database_url_env_var'")
            if kind == "csv":
                path = src.get("path")
                if not path:
                    raise ValueError("source kind=csv requires 'path'")
                return pd.read_csv(path, **(src.get("read_csv_kwargs") or {}))
            if kind == "inline":
                return pd.DataFrame(src.get("rows") or [])
            raise ValueError(f"ConfluencePageUpsertComponent source kind={kind!r} not supported (sql / csv / inline)")

        def _run_upsert(context, upstream):
            confluence = getattr(context.resources, _self.resource_key)

            import pandas as pd
            if not isinstance(upstream, pd.DataFrame):
                df = pd.DataFrame([upstream]) if isinstance(upstream, dict) else pd.DataFrame(upstream)
            else:
                df = upstream

            if len(df) == 0:
                context.log.warning("Upstream DataFrame is empty -- nothing to upsert.")
                return dg.MaterializeResult(metadata={"rows_upserted": dg.MetadataValue.int(0)})

            if len(df) > _self.batch_size:
                context.log.warning(
                    f"Upstream has {len(df)} rows; capped at batch_size={_self.batch_size}."
                )
                df = df.head(_self.batch_size)

            required_cols = set(_self.fields_map.keys()) | {_self.body_column}
            missing_cols = [c for c in required_cols if c not in df.columns]
            if missing_cols:
                raise dg.Failure(
                    f"Columns not in upstream: {missing_cols}. Available: {list(df.columns)}"
                )

            title_col = next(
                col for col, cf in _self.fields_map.items() if cf == "title"
            )

            def _row_value(v):
                if v is None or (isinstance(v, float) and pd.isna(v)):
                    return None
                return v

            created = 0
            updated = 0
            skipped_blank_title = 0
            errors: List[str] = []

            for _, row in df.iterrows():
                title_val = _row_value(row[title_col])
                if title_val is None or not str(title_val).strip():
                    skipped_blank_title += 1
                    continue
                title = str(title_val).strip()

                body_val = _row_value(row[_self.body_column])
                body_text = "" if body_val is None else str(body_val)
                body_storage = (
                    _to_storage_format(body_text) if _self.wrap_body_as_html else body_text
                )

                try:
                    result = confluence.upsert_page_by_title(
                        _self.space_id, title, body_storage
                    )
                except Exception as e:  # noqa: BLE001
                    errors.append(f"title={title!r}: {type(e).__name__}: {e}")
                    continue

                if result.get("action") == "created":
                    created += 1
                else:
                    updated += 1

            context.log.info(
                f"Confluence upsert into space {_self.space_id}: {created} created, "
                f"{updated} updated, {len(errors)} errors, {skipped_blank_title} skipped "
                f"(blank title) -- matched on title."
            )
            if errors:
                context.log.error("First few errors:\n" + "\n".join(errors[:5]))

            metadata = {
                "confluence_space_id": dg.MetadataValue.text(str(_self.space_id)),
                "rows_created": dg.MetadataValue.int(created),
                "rows_updated": dg.MetadataValue.int(updated),
                "rows_upserted": dg.MetadataValue.int(created + updated),
                "rows_errored": dg.MetadataValue.int(len(errors)),
                "rows_skipped_blank_title": dg.MetadataValue.int(skipped_blank_title),
            }
            if errors:
                metadata["first_errors"] = dg.MetadataValue.json(errors[:5])

            return dg.MaterializeResult(metadata=metadata)

        common_kwargs = dict(
            key=dg.AssetKey.from_user_string(_self.asset_name),
            group_name=_self.group_name,
            kinds=kinds,
            owners=_self.owners,
            tags=_self.tags,
            description=_self.description or (
                f"Upsert DataFrame rows into Confluence space {_self.space_id} "
                f"pages (match on title)."
            ),
        )

        if use_source:
            required_rks = {_self.resource_key} | extra_rks

            @dg.asset(required_resource_keys=required_rks, **common_kwargs)
            def _asset(context: dg.AssetExecutionContext):
                df = _resolve_source_df(context)
                return _run_upsert(context, df)
        else:
            @dg.asset(
                ins={"upstream": dg.AssetIn(key=dg.AssetKey.from_user_string(_self.upstream_asset_key))},
                required_resource_keys={_self.resource_key},
                **common_kwargs,
            )
            def _asset(context: dg.AssetExecutionContext, upstream):
                return _run_upsert(context, upstream)

        return dg.Definitions(assets=[_asset])
