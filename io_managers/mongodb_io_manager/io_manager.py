"""MongoDBIOManager.

ConfigurableIOManager that writes pandas DataFrames to MongoDB. Each row becomes a document; the asset key becomes the collection name. On replace mode, drops the collection before insert.
"""
from typing import Any, Optional

import dagster as dg
import pandas as pd
from pydantic import Field

from pymongo import MongoClient


def _sanitize(s: str) -> str:
    return s.replace("[", "--").replace("]", "--").replace(" ", "_").replace("/", "__")


class MongoDBIOManager(dg.ConfigurableIOManager):
    """Persist DataFrames to MongoDB collections (one per asset, document-store)."""

    connection_uri: str = Field(description="MongoDB connection URI, e.g. mongodb://user:pwd@host:27017/")
    database: str = Field(description="MongoDB database name.")
    if_exists: str = Field(default="replace", description="'replace', 'append', or 'fail'.")

    def _table_name(self, context) -> str:
        parts = list(context.asset_key.path)
        if context.has_asset_partitions:
            parts.append(context.asset_partition_key)
        return "_".join(_sanitize(str(p)) for p in parts)

    def handle_output(self, context, obj) -> None:
        if obj is None:
            return
        if not isinstance(obj, pd.DataFrame):
            raise TypeError(f"MongoDBIOManager only handles DataFrames; got {type(obj)}")
        client = MongoClient(self.connection_uri)
        try:
            db = client[self.database]
            coll_name = self._table_name(context)
            coll = db[coll_name]
            if self.if_exists == "replace":
                coll.drop()
            elif self.if_exists == "fail" and coll.estimated_document_count() > 0:
                raise ValueError(f"Collection {coll_name} already exists.")
            if len(obj) > 0:
                # Coerce pandas NaT / NaN to None so pymongo (which uses BSON)
                # doesn't choke on `NaTType does not support utcoffset` etc.
                # `astype(object).where(notna, None)` survives mixed-tz columns.
                clean = obj.astype(object).where(obj.notna(), None)
                # Drop any inbound `_id` — let MongoDB auto-generate ObjectIds
                # so upstream string-coerced ids don't collide with the
                # destination collection's unique-id index.
                if "_id" in clean.columns:
                    clean = clean.drop(columns=["_id"])
                coll.insert_many(clean.to_dict(orient="records"))
            context.add_output_metadata({"collection": dg.MetadataValue.text(f"{self.database}.{coll_name}"), "row_count": dg.MetadataValue.int(len(obj))})
        finally:
            client.close()

    def load_input(self, context):
        client = MongoClient(self.connection_uri)
        try:
            coll = client[self.database][self._table_name(context.upstream_output)]
            return pd.DataFrame(list(coll.find({}, {"_id": 0})))
        finally:
            client.close()
