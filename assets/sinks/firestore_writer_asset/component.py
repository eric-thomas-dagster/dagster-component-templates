"""FirestoreWriterAssetComponent — write DataFrame rows to a Firestore collection.

Each row of the upstream DataFrame becomes one Firestore document.
The doc id is taken from `id_column` if set, otherwise auto-generated.
Supports merge / overwrite behavior on existing documents.

Useful as a real-time-readable sink behind ML scoring, customer 360
denormalized views, dynamic config tables.
"""

import json
import os
from typing import Any, Dict, List, Literal, Optional

import pandas as pd

from dagster import (
    AssetExecutionContext,
    AssetIn,
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


class FirestoreWriterAssetComponent(Component, Model, Resolvable):
    """Write DataFrame rows to a Firestore collection — one doc per row."""

    asset_name: str = Field(description="Output asset name.")
    upstream_asset_key: str = Field(description="Upstream DataFrame asset key.")

    credentials: Optional[Dict[str, Any]] = Field(default=None)
    credentials_path: Optional[str] = Field(default=None, description="Falls back to GOOGLE_APPLICATION_CREDENTIALS.")

    project_id: Optional[str] = Field(default=None)
    database: str = Field(default="(default)")
    collection: str = Field(description="Collection path (e.g. 'customers' or 'tenants/acme/customers').")

    id_column: Optional[str] = Field(
        default=None,
        description="Column whose value becomes each document's id. If unset, Firestore auto-generates ids.",
    )
    write_mode: Literal["set", "merge", "create"] = Field(
        default="merge",
        description=(
            "set: overwrite the doc; merge: deep-merge keys (preserves untouched fields); "
            "create: fail if doc already exists."
        ),
    )
    drop_id_column_from_body: bool = Field(
        default=True,
        description="If True (default), the id_column is omitted from the doc body when used as id.",
    )
    batch_size: int = Field(default=500, description="Firestore commit batch size (max 500 per batch).")

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
        upstream_key = AssetKey.from_user_string(self.upstream_asset_key)
        project_id = self.project_id or creds_dict.get("project_id")
        database = self.database
        collection = self.collection
        id_column = self.id_column
        write_mode = self.write_mode
        drop_id = self.drop_id_column_from_body
        batch_size = self.batch_size

        @asset(
            name=asset_name,
            description=self.description or f"Firestore write to {collection} in {project_id}/{database}.",
            group_name=self.group_name,
            kinds={"google", "firestore"},
            tags=self.tags or None,
            owners=self.owners or None,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])] or None,
            ins={"upstream": AssetIn(key=upstream_key)},
        )
        def _asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> Output:
            try:
                from google.cloud import firestore
                from google.oauth2 import service_account
                from google.api_core.exceptions import AlreadyExists
            except ImportError:
                raise ImportError("pip install google-cloud-firestore google-auth")

            sa_creds = service_account.Credentials.from_service_account_info(creds_dict)
            client = firestore.Client(project=project_id, credentials=sa_creds, database=database)
            col = client.collection(collection)

            df = upstream.copy().reset_index(drop=True)
            if id_column and id_column not in df.columns:
                raise ValueError(f"id_column={id_column!r} not in upstream: {list(df.columns)}")

            written = 0
            errors: List[Dict[str, Any]] = []
            batch = client.batch()
            ops_in_batch = 0

            for i, row in df.iterrows():
                body = row.to_dict()
                if id_column:
                    doc_id = str(body[id_column])
                    if drop_id:
                        body.pop(id_column, None)
                    doc_ref = col.document(doc_id)
                else:
                    doc_ref = col.document()  # auto-id

                # Convert pandas-y NaN/NaT to None for Firestore.
                body = {k: (None if (isinstance(v, float) and pd.isna(v)) else v) for k, v in body.items()}

                if write_mode == "set":
                    batch.set(doc_ref, body, merge=False)
                elif write_mode == "merge":
                    batch.set(doc_ref, body, merge=True)
                else:  # create
                    batch.create(doc_ref, body)
                ops_in_batch += 1

                if ops_in_batch >= batch_size:
                    try:
                        batch.commit()
                        written += ops_in_batch
                    except AlreadyExists as e:
                        errors.append({"row": int(i), "error": str(e)})
                    except Exception as e:
                        errors.append({"row": int(i), "error": str(e)})
                    batch = client.batch()
                    ops_in_batch = 0

            if ops_in_batch > 0:
                try:
                    batch.commit()
                    written += ops_in_batch
                except Exception as e:
                    errors.append({"error": str(e)})

            df_out = df.copy()
            df_out["_firestore_written"] = True
            md = {
                "collection":   MetadataValue.text(collection),
                "rows_written": MetadataValue.int(written),
                "errors":       MetadataValue.int(len(errors)),
                "preview":      MetadataValue.md(df_out.head(10).to_markdown(index=False) or ""),
            }
            if errors:
                md["error_sample"] = MetadataValue.json(errors[:5])
            return Output(value=df_out, metadata=md)

        return Definitions(assets=[_asset])
