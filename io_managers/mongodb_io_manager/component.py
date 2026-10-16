"""MongoDBIOManagerComponent.

YAML/Component wrapper around `MongoDBIOManager`.
"""
from typing import Optional, Any, Mapping
import dagster as dg
from pydantic import Field
import pandas as pd


def MongoClient(uri: str):
    """Create a pymongo client (imported lazily so the module loads without the driver)."""
    from pymongo import MongoClient as _MongoClient

    return _MongoClient(uri)


def _sanitize(s: str) -> str:
    return s.replace("[", "--").replace("]", "--").replace(" ", "_").replace("/", "__")


def _output_range_keys(context) -> Optional[list]:
    """Partition keys covered by an OutputContext that spans a partition RANGE.

    For a single_run backfill, ``has_partition_key`` is False while
    ``has_asset_partitions`` is True and ``asset_partition_keys`` lists the range.
    Returns None for single-partition and unpartitioned outputs.
    """
    if context.has_partition_key:
        return None
    if not context.has_asset_partitions:
        return None
    return [str(k) for k in context.asset_partition_keys]


def _input_range_keys(context) -> Optional[list]:
    """Upstream partition keys when an InputContext loads a partition RANGE, else None."""
    if context.upstream_output is None or context.upstream_output.has_partition_key:
        return None
    if not context.has_asset_partitions:
        return None
    return [str(k) for k in context.asset_partition_keys]


def _partition_label(context) -> str:
    """Human-readable partition for metadata: the key, 'start...end' for a range, or '(unpartitioned)'."""
    if context.has_partition_key:
        return str(context.partition_key)
    if _output_range_keys(context) is not None:
        rng = context.asset_partition_key_range
        return f"{rng.start}...{rng.end}"
    return "(unpartitioned)"


def _check_range_mapping(manager_name: str, context, obj, keys) -> None:
    """Validate that a multi-partition output is a Mapping {partition_key: DataFrame} covering exactly `keys`."""
    if not isinstance(obj, Mapping):
        raise TypeError(
            f"{manager_name}: this output covers a partition range ({len(keys)} partitions, a "
            f"BackfillPolicy.single_run backfill), so the asset must return a dict "
            f"{{partition_key: DataFrame}} with one entry per partition; got {type(obj).__name__}."
        )
    got, want = {str(k) for k in obj.keys()}, set(keys)
    if got != want:
        raise ValueError(
            f"{manager_name}: dict keys must equal the partition range's keys. "
            f"Missing: {sorted(want - got)}; unexpected: {sorted(got - want)}."
        )


class MongoDBIOManager(dg.ConfigurableIOManager):
    """Persist DataFrames to MongoDB collections (one per asset, document-store).

    Partition ranges (single_run backfills): when one run covers a range of
    partitions (``BackfillPolicy.single_run()``), the asset must return a dict
    ``{partition_key: DataFrame}`` whose keys equal the range's partition keys; each
    key is written exactly like a single partition. Downstream assets loading the
    range receive a dict ``{partition_key: DataFrame}``.
    """

    connection_uri: str = Field(description="MongoDB connection URI, e.g. mongodb://user:pwd@host:27017/")
    database: str = Field(description="MongoDB database name.")
    if_exists: str = Field(default="replace", description="'replace', 'append', or 'fail'.")

    def _table_name(self, context, partition_key: Optional[str] = None) -> str:
        parts = list(context.asset_key.path)
        if partition_key is not None:
            parts.append(partition_key)
        elif context.has_asset_partitions:
            parts.append(context.asset_partition_key)
        return "_".join(_sanitize(str(p)) for p in parts)

    def _write_collection(self, db, coll_name: str, obj: pd.DataFrame) -> None:
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

    def handle_output(self, context, obj) -> None:
        if obj is None:
            return
        keys = _output_range_keys(context)
        if keys is not None:
            _check_range_mapping("MongoDBIOManager", context, obj, keys)
            by_key = {str(k): v for k, v in obj.items()}
            for k in keys:  # validate all before writing any
                if not isinstance(by_key[k], pd.DataFrame):
                    raise TypeError(f"MongoDBIOManager only handles DataFrames; got {type(by_key[k])} for partition {k!r}")
            frames = [(self._table_name(context, partition_key=k), by_key[k]) for k in keys]
        else:
            if not isinstance(obj, pd.DataFrame):
                raise TypeError(f"MongoDBIOManager only handles DataFrames; got {type(obj)}")
            frames = [(self._table_name(context), obj)]
        client = MongoClient(self.connection_uri)
        try:
            db = client[self.database]
            for coll_name, frame in frames:
                self._write_collection(db, coll_name, frame)
            metadata = {
                "collection": dg.MetadataValue.text(", ".join(f"{self.database}.{n}" for n, _ in frames)),
                "row_count": dg.MetadataValue.int(sum(len(f) for _, f in frames)),
            }
            if context.has_asset_partitions:
                metadata["partition_key"] = dg.MetadataValue.text(_partition_label(context))
            context.add_output_metadata(metadata)
        finally:
            client.close()

    def _read_collection(self, client, coll_name: str) -> pd.DataFrame:
        coll = client[self.database][coll_name]
        return pd.DataFrame(list(coll.find({}, {"_id": 0})))

    def load_input(self, context):
        client = MongoClient(self.connection_uri)
        try:
            keys = _input_range_keys(context)
            if keys is not None:
                return {
                    k: self._read_collection(client, self._table_name(context.upstream_output, partition_key=k))
                    for k in keys
                }
            return self._read_collection(client, self._table_name(context.upstream_output))
        finally:
            client.close()



class MongoDBIOManagerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Persist DataFrames to MongoDB collections (one per asset, document-store)."""

    resource_key: str = Field(
        default="io_manager",
        description="Dagster resource key. Use 'io_manager' to make this the default.",
    )
    connection_uri: Optional[str] = Field(default=None, description="MongoDB URI (literal). Set this OR connection_uri_env_var.")
    connection_uri_env_var: Optional[str] = Field(default="MONGODB_URI", description="Env var holding the MongoDB URI. Set this OR connection_uri.")
    database: str = Field(description="MongoDB database name.")
    if_exists: str = Field(default="replace", description="'replace', 'append', or 'fail'.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        if self.connection_uri:
            resolved_uri = self.connection_uri
        elif self.connection_uri_env_var:
            resolved_uri = dg.EnvVar(self.connection_uri_env_var).get_value() or ""
        else:
            resolved_uri = ""
        io_manager = MongoDBIOManager(
            connection_uri=resolved_uri,
            database=self.database,
            if_exists=self.if_exists,
        )
        return dg.Definitions(resources={self.resource_key: io_manager})
