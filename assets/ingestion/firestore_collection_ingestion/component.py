"""Firestore Collection Ingestion component.

Materializable asset that reads a Firestore collection (or subcollection) into
a pandas DataFrame. Uses a `firebase_resource` for authenticated access.
"""

from typing import Any, Dict, List, Optional

import dagster as dg
import pandas as pd
from pydantic import Field


# Firestore FieldFilter operators.
_VALID_OPS = {"==", "!=", "<", "<=", ">", ">=", "in", "not-in", "array-contains", "array-contains-any"}


class FirestoreCollectionIngestionComponent(dg.Component, dg.Model, dg.Resolvable):
    """Read a Firestore collection into a pandas DataFrame.

    `collection_path` accepts a top-level collection (`users`) or a
    subcollection path (`orders/{order_id}/items` — segments alternate
    collection/document). `where_clauses` accepts a list of
    `{field, op, value}` dicts; `order_by` accepts `{field, direction}`
    where direction is `"asc"` (default) or `"desc"`.

    The document id lands in a `_id` column.
    """

    asset_name: str = Field(description="Name of the asset to create.")
    resource_name: str = Field(
        default="firebase_resource",
        description="Name of the FirebaseResource this asset uses.",
    )
    collection_path: str = Field(
        description="Firestore collection path, e.g. 'users' or 'orders/{order_id}/items'.",
    )
    where_clauses: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Optional filters: list of {field, op, value} dicts. "
                    "Ops: ==, !=, <, <=, >, >=, in, not-in, array-contains, array-contains-any.",
    )
    limit: Optional[int] = Field(
        default=None,
        description="Optional maximum number of documents to read.",
    )
    order_by: Optional[Dict[str, str]] = Field(
        default=None,
        description="Optional ordering: {field: <name>, direction: 'asc'|'desc'} (default 'asc').",
    )
    group_name: Optional[str] = Field(
        default="firebase",
        description="Asset group for organization.",
    )
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners (team names or emails).",
    )
    asset_tags: Optional[Dict[str, str]] = Field(
        default=None,
        description="Additional key-value tags for the asset.",
    )
    include_preview_metadata: bool = Field(
        default=True,
        description="Include a sample preview in the run metadata.",
    )
    preview_rows: int = Field(
        default=25, ge=1, le=500,
        description="Rows to include in the preview.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        asset_name = self.asset_name
        resource_name = self.resource_name
        collection_path = self.collection_path
        where_clauses = self.where_clauses or []
        limit = self.limit
        order_by = self.order_by
        description = self.description or f"Firestore collection: {collection_path}"
        group_name = self.group_name
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows

        # Validate where clauses at load time.
        for i, clause in enumerate(where_clauses):
            if not isinstance(clause, dict):
                raise ValueError(f"where_clauses[{i}] must be a dict, got {type(clause).__name__}")
            missing = {"field", "op", "value"} - clause.keys()
            if missing:
                raise ValueError(f"where_clauses[{i}] missing keys: {sorted(missing)}")
            if clause["op"] not in _VALID_OPS:
                raise ValueError(
                    f"where_clauses[{i}].op={clause['op']!r} — must be one of {sorted(_VALID_OPS)}"
                )

        if order_by is not None:
            if "field" not in order_by:
                raise ValueError("order_by requires a 'field' key")
            direction = order_by.get("direction", "asc").lower()
            if direction not in ("asc", "desc"):
                raise ValueError(f"order_by.direction must be 'asc' or 'desc', got {direction!r}")

        tags = dict(self.asset_tags or {})
        tags["dagster/kind/firestore"] = ""

        @dg.asset(
            key=dg.AssetKey.from_user_string(asset_name),
            description=description,
            group_name=group_name,
            owners=self.owners or [],
            tags=tags,
            required_resource_keys={resource_name},
        )
        def firestore_collection_asset(context: dg.AssetExecutionContext) -> dg.Output:
            from google.cloud.firestore_v1.base_query import FieldFilter  # type: ignore

            fb = getattr(context.resources, resource_name)
            client = fb.get_firestore()

            context.log.info(f"Reading Firestore collection: {collection_path}")

            # Firestore treats a path with an odd number of segments as a
            # collection and even as a document — the client's .collection()
            # accepts a slash-delimited path directly.
            query = client.collection(collection_path)
            for clause in where_clauses:
                query = query.where(filter=FieldFilter(clause["field"], clause["op"], clause["value"]))

            if order_by is not None:
                from google.cloud.firestore_v1 import Query  # type: ignore
                direction = order_by.get("direction", "asc").lower()
                query = query.order_by(
                    order_by["field"],
                    direction=Query.DESCENDING if direction == "desc" else Query.ASCENDING,
                )

            if limit is not None:
                query = query.limit(limit)

            docs = list(query.stream())
            rows = []
            for doc in docs:
                data = doc.to_dict() or {}
                data["_id"] = doc.id
                rows.append(data)

            df = pd.DataFrame(rows)
            context.log.info(f"Read {len(df)} documents from {collection_path}")

            metadata = {
                "collection_path": dg.MetadataValue.text(collection_path),
                "row_count": dg.MetadataValue.int(len(df)),
                "where_clauses": dg.MetadataValue.json(where_clauses),
            }
            if limit is not None:
                metadata["limit"] = dg.MetadataValue.int(limit)
            if include_preview and len(df) > 0:
                try:
                    prev = df.head(preview_rows)
                    metadata["preview"] = dg.MetadataValue.md(prev.to_markdown(index=False))
                except Exception as e:
                    context.log.warning(f"preview emission failed: {e}")

            return dg.Output(value=df, metadata=metadata)

        return dg.Definitions(assets=[firestore_collection_asset])
