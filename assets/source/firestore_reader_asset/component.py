"""FirestoreReaderAssetComponent — read documents from a Firestore collection.

Returns a pandas DataFrame, one row per document, with the document id
as a column. Supports basic filters (where_clauses), ordering, limit,
and collection-group queries.
"""

import json
import os
from typing import Any, Dict, List, Literal, Optional

import pandas as pd

from dagster import (
    AssetExecutionContext,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    MetadataValue,
    Model,
    Output,
    Resolvable,
    asset,
)
from pydantic import Field


class FirestoreReaderAssetComponent(Component, Model, Resolvable):
    """Read documents from a Firestore collection (or collection group) into a DataFrame."""

    asset_name: str = Field(description="Output asset name.")
    credentials: Optional[Dict[str, Any]] = Field(default=None)
    credentials_path: Optional[str] = Field(default=None, description="Falls back to GOOGLE_APPLICATION_CREDENTIALS.")

    project_id: Optional[str] = Field(default=None)
    database: str = Field(default="(default)", description="Firestore database id ('(default)' or named).")

    collection: str = Field(description="Collection path (e.g. 'orders' or 'tenants/acme/orders').")
    collection_group: bool = Field(
        default=False,
        description="If True, treat `collection` as a collection-group query (matches every collection of that name across the database).",
    )

    where: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description=(
            "Optional list of {field, op, value} filter dicts. op is one of: "
            "==, !=, <, <=, >, >=, in, not-in, array-contains, array-contains-any."
        ),
    )
    order_by: Optional[List[str]] = Field(
        default=None,
        description="List of `field` (asc) or `-field` (desc) entries.",
    )
    limit: Optional[int] = Field(default=None, description="Optional max documents to return.")

    description: Optional[str] = Field(default=None)
    group_name: Optional[str] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)
    tags: Optional[Dict[str, str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        creds_dict = self.credentials
        if creds_dict is None:
            cred_path = self.credentials_path or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
            if cred_path:
                with open(cred_path, "r") as fh:
                    creds_dict = json.load(fh)
        if creds_dict is None:
            raise ValueError("Provide credentials, credentials_path, or set GOOGLE_APPLICATION_CREDENTIALS.")

        asset_name = self.asset_name
        project_id = self.project_id or creds_dict.get("project_id")
        database = self.database
        collection = self.collection
        collection_group = self.collection_group
        where_clauses = self.where or []
        order_by = self.order_by or []
        limit = self.limit

        @asset(
            name=asset_name,
            description=self.description or f"Firestore read: {collection} in {project_id}/{database}.",
            group_name=self.group_name,
            kinds={"google", "firestore"},
            tags=self.tags or None,
            owners=self.owners or None,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])] or None,
        )
        def _asset(context: AssetExecutionContext) -> Output:
            try:
                from google.cloud import firestore
                from google.oauth2 import service_account
            except ImportError:
                raise ImportError("pip install google-cloud-firestore google-auth")

            sa_creds = service_account.Credentials.from_service_account_info(creds_dict)
            client = firestore.Client(project=project_id, credentials=sa_creds, database=database)

            ref = client.collection_group(collection.split("/")[-1]) if collection_group else client.collection(collection)
            query = ref
            for clause in where_clauses:
                field = clause["field"]; op = clause["op"]; val = clause["value"]
                query = query.where(filter=firestore.FieldFilter(field, op, val))
            for ob in order_by:
                if ob.startswith("-"):
                    query = query.order_by(ob[1:], direction=firestore.Query.DESCENDING)
                else:
                    query = query.order_by(ob)
            if limit is not None:
                query = query.limit(limit)

            docs = list(query.stream())
            context.log.info(f"Firestore returned {len(docs)} document(s) for {collection}")

            rows = []
            for d in docs:
                row = {"_id": d.id}
                row.update(d.to_dict() or {})
                rows.append(row)

            df = pd.DataFrame(rows)
            preview = df.head(10).to_markdown(index=False) if not df.empty else "(no docs)"
            return Output(
                value=df,
                metadata={
                    "collection":    MetadataValue.text(collection),
                    "is_group":      MetadataValue.bool(collection_group),
                    "doc_count":     MetadataValue.int(len(df)),
                    "preview":       MetadataValue.md(preview or ""),
                },
            )

        return Definitions(assets=[_asset])
