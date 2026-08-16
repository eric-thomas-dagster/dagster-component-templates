"""Notion Resource component.

Wraps `notion-client` with ergonomic read + write convenience methods so
Dagster assets/ops can call `context.resources.notion.query_database(...)`
without reaching for the raw client.

Drop to `.get_client()` for anything not covered here.
"""
from typing import Iterator, List, Optional

import dagster as dg
from dagster import ConfigurableResource
from pydantic import Field


class NotionResource(ConfigurableResource):
    """Dagster resource wrapping the notion-client SDK with convenience methods.

    Covers the common read + write API surfaces. For anything not covered,
    use `.get_client()` to get the raw `notion_client.Client` instance.
    """

    token: str = Field(description="Notion integration token")

    def get_client(self):
        """Return the raw `notion_client.Client`. Escape hatch for anything the
        convenience methods don't cover."""
        import notion_client
        return notion_client.Client(auth=self.token)

    # ------------------------------------------------------------------ reads

    def search(
        self,
        query: str = "",
        filter: Optional[dict] = None,
        sort: Optional[dict] = None,
        page_size: int = 100,
    ) -> List[dict]:
        """Search across pages and data sources the integration has access to.

        `filter` example: `{"value": "data_source", "property": "object"}` (only DBs).
        Notion's 2025 API accepts `"page"` or `"data_source"`; the old `"database"`
        value no longer works — pass `data_source` to filter on database-shaped items.
        Returns the first page of results — use `iter_search` for full pagination.
        """
        kwargs: dict = {"query": query, "page_size": page_size}
        if filter:
            kwargs["filter"] = filter
        if sort:
            kwargs["sort"] = sort
        return self.get_client().search(**kwargs).get("results", [])

    def iter_search(
        self,
        query: str = "",
        filter: Optional[dict] = None,
        sort: Optional[dict] = None,
        page_size: int = 100,
    ) -> Iterator[dict]:
        """Auto-paginated variant of `search`."""
        client = self.get_client()
        start_cursor: Optional[str] = None
        while True:
            kwargs: dict = {"query": query, "page_size": page_size}
            if filter:
                kwargs["filter"] = filter
            if sort:
                kwargs["sort"] = sort
            if start_cursor:
                kwargs["start_cursor"] = start_cursor
            resp = client.search(**kwargs)
            for r in resp.get("results", []):
                yield r
            if not resp.get("has_more"):
                return
            start_cursor = resp.get("next_cursor")

    def get_page(self, page_id: str) -> dict:
        """Retrieve a single page (properties, not block content)."""
        return self.get_client().pages.retrieve(page_id=page_id)

    def get_page_markdown(self, page_id: str) -> str:
        """Retrieve a page's block content rendered as markdown.

        Uses notion-client's built-in markdown renderer (v3+).
        """
        return self.get_client().pages.retrieve_markdown(page_id=page_id)

    def get_database(self, database_id: str) -> dict:
        """Retrieve a database's schema + metadata."""
        return self.get_client().databases.retrieve(database_id=database_id)

    def query_database(
        self,
        database_id: Optional[str] = None,
        data_source_id: Optional[str] = None,
        filter: Optional[dict] = None,
        sorts: Optional[List[dict]] = None,
        page_size: int = 100,
    ) -> List[dict]:
        """Query a database (or data source directly).

        Notion's 2025 API split databases into `database` (container) and
        `data_source` (queryable). Pass either `database_id` OR `data_source_id` —
        if given `database_id`, we retrieve the DB first to resolve the primary
        data source ID (one extra API call).

        Returns the first page. Use `iter_query_database` for full pagination.
        """
        ds_id = self._resolve_data_source_id(database_id, data_source_id)
        kwargs: dict = {"data_source_id": ds_id, "page_size": page_size}
        if filter:
            kwargs["filter"] = filter
        if sorts:
            kwargs["sorts"] = sorts
        return self.get_client().data_sources.query(**kwargs).get("results", [])

    def iter_query_database(
        self,
        database_id: Optional[str] = None,
        data_source_id: Optional[str] = None,
        filter: Optional[dict] = None,
        sorts: Optional[List[dict]] = None,
        page_size: int = 100,
    ) -> Iterator[dict]:
        """Auto-paginated variant of `query_database`."""
        client = self.get_client()
        ds_id = self._resolve_data_source_id(database_id, data_source_id)
        start_cursor: Optional[str] = None
        while True:
            kwargs: dict = {"data_source_id": ds_id, "page_size": page_size}
            if filter:
                kwargs["filter"] = filter
            if sorts:
                kwargs["sorts"] = sorts
            if start_cursor:
                kwargs["start_cursor"] = start_cursor
            resp = client.data_sources.query(**kwargs)
            for r in resp.get("results", []):
                yield r
            if not resp.get("has_more"):
                return
            start_cursor = resp.get("next_cursor")

    def get_block_children(self, block_id: str, page_size: int = 100) -> List[dict]:
        """List child blocks of a block (or page — pages are blocks too).

        Returns the first page. Use `iter_block_children` for full pagination.
        """
        return (
            self.get_client()
            .blocks.children.list(block_id=block_id, page_size=page_size)
            .get("results", [])
        )

    def iter_block_children(self, block_id: str, page_size: int = 100) -> Iterator[dict]:
        """Auto-paginated variant of `get_block_children`."""
        client = self.get_client()
        start_cursor: Optional[str] = None
        while True:
            kwargs: dict = {"block_id": block_id, "page_size": page_size}
            if start_cursor:
                kwargs["start_cursor"] = start_cursor
            resp = client.blocks.children.list(**kwargs)
            for r in resp.get("results", []):
                yield r
            if not resp.get("has_more"):
                return
            start_cursor = resp.get("next_cursor")

    def get_comments(self, block_id: str, page_size: int = 100) -> List[dict]:
        """List comments on a page or block. Returns first page."""
        return (
            self.get_client()
            .comments.list(block_id=block_id, page_size=page_size)
            .get("results", [])
        )

    def list_users(self, page_size: int = 100) -> List[dict]:
        """List users (bots + humans) the integration can see. Returns first page."""
        return self.get_client().users.list(page_size=page_size).get("results", [])

    def whoami(self) -> dict:
        """Retrieve the bot user that owns this integration token."""
        return self.get_client().users.me()

    # ----------------------------------------------------------------- writes

    def create_page(
        self,
        parent: dict,
        properties: dict,
        children: Optional[List[dict]] = None,
        icon: Optional[dict] = None,
        cover: Optional[dict] = None,
    ) -> dict:
        """Create a page under a database or another page.

        `parent` shape depends on where the page lives:
            - inside a DB:   {"database_id": "..."}
            - inside a page: {"page_id": "..."}
            - inside a data source (2025 API): {"data_source_id": "..."}
        """
        body: dict = {"parent": parent, "properties": properties}
        if children is not None:
            body["children"] = children
        if icon is not None:
            body["icon"] = icon
        if cover is not None:
            body["cover"] = cover
        return self.get_client().pages.create(**body)

    def update_page(
        self,
        page_id: str,
        properties: Optional[dict] = None,
        archived: Optional[bool] = None,
        icon: Optional[dict] = None,
        cover: Optional[dict] = None,
    ) -> dict:
        """Patch a page's properties (or archive/unarchive it)."""
        body: dict = {"page_id": page_id}
        if properties is not None:
            body["properties"] = properties
        if archived is not None:
            body["archived"] = archived
        if icon is not None:
            body["icon"] = icon
        if cover is not None:
            body["cover"] = cover
        return self.get_client().pages.update(**body)

    def append_blocks(self, block_id: str, children: List[dict]) -> dict:
        """Append child blocks to a page or block.

        `children` is a list of Notion block objects, e.g.:
            [{"object": "block", "type": "paragraph",
              "paragraph": {"rich_text": [{"type": "text", "text": {"content": "hi"}}]}}]
        """
        return self.get_client().blocks.children.append(block_id=block_id, children=children)

    def add_comment(
        self,
        text: str,
        page_id: Optional[str] = None,
        discussion_id: Optional[str] = None,
    ) -> dict:
        """Add a comment to a page (new thread) or an existing discussion thread.

        Provide EITHER page_id (new top-level comment) OR discussion_id (reply).
        """
        rich_text = [{"type": "text", "text": {"content": text}}]
        if page_id and discussion_id:
            raise ValueError("Pass page_id OR discussion_id, not both.")
        if page_id:
            body = {"parent": {"page_id": page_id}, "rich_text": rich_text}
        elif discussion_id:
            body = {"discussion_id": discussion_id, "rich_text": rich_text}
        else:
            raise ValueError("Must pass page_id or discussion_id.")
        return self.get_client().comments.create(**body)

    def create_database(
        self,
        parent_page_id: str,
        title: str,
        properties: dict,
    ) -> dict:
        """Create a new database as a child of a page.

        `properties` is a schema dict, e.g.:
            {"Name": {"title": {}}, "Status": {"select": {"options": [...]}}}
        """
        return self.get_client().databases.create(
            parent={"type": "page_id", "page_id": parent_page_id},
            title=[{"type": "text", "text": {"content": title}}],
            properties=properties,
        )

    def upload_file(
        self,
        file_path: str,
        filename: Optional[str] = None,
        content_type: Optional[str] = None,
    ) -> dict:
        """Upload a file to Notion. Returns the file_upload object.

        The returned dict has an `id` field — reference it in a page/block via
        `{"type": "file_upload", "file_upload": {"id": <id>}}` on file/image/PDF blocks
        or file-type properties.

        Notion's file upload is a 3-step protocol (create → send → complete);
        we hide that here.
        """
        import mimetypes
        import os
        client = self.get_client()
        name = filename or os.path.basename(file_path)
        ctype = content_type or (mimetypes.guess_type(name)[0] or "application/octet-stream")

        upload = client.file_uploads.create(filename=name, content_type=ctype)
        upload_id = upload["id"]
        with open(file_path, "rb") as fh:
            client.file_uploads.send(file_upload_id=upload_id, file=fh, content_type=ctype)
        client.file_uploads.complete(file_upload_id=upload_id)
        return client.file_uploads.retrieve(file_upload_id=upload_id)

    # ----------------------------------------------------------------- helpers

    def _resolve_data_source_id(
        self, database_id: Optional[str], data_source_id: Optional[str]
    ) -> str:
        """Notion 2025 API: databases are containers; queries hit data sources.

        Given a database_id, retrieve the DB and return the primary data source ID.
        If given data_source_id directly, return it as-is (skip the extra call).
        """
        if data_source_id and database_id:
            raise ValueError("Pass database_id OR data_source_id, not both.")
        if data_source_id:
            return data_source_id
        if not database_id:
            raise ValueError("Must pass database_id or data_source_id.")
        db = self.get_client().databases.retrieve(database_id=database_id)
        # `data_sources` is a list; the first is the primary
        sources = db.get("data_sources") or []
        if not sources:
            raise ValueError(
                f"Database {database_id} has no data_sources — is the integration "
                "granted access? (Share the DB with your integration in Notion.)"
            )
        return sources[0]["id"]


class NotionResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a NotionResource for use by other components."""

    resource_key: str = Field(
        default="notion_resource",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    token_env_var: str = Field(
        description="Env var holding Notion integration token",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = NotionResource(
            token=dg.EnvVar(self.token_env_var),
        )
        return dg.Definitions(resources={self.resource_key: resource})
