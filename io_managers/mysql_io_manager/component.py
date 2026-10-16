"""MySQLIOManagerComponent.

YAML/Component wrapper around `MySQLIOManager`.
"""
from typing import Optional, Any, Mapping
import dagster as dg
from pydantic import Field
import pandas as pd
from sqlalchemy import create_engine


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


class MySQLIOManager(dg.ConfigurableIOManager):
    """Persist DataFrames to a MySQL/MariaDB schema, one table per asset.

    Partition ranges (single_run backfills): when one run covers a range of
    partitions (``BackfillPolicy.single_run()``), the asset must return a dict
    ``{partition_key: DataFrame}`` whose keys equal the range's partition keys; each
    key is written exactly like a single partition (one table per key). Downstream
    assets loading the range receive a dict ``{partition_key: DataFrame}``.
    """

    connection_url: str = Field(description="SQLAlchemy URL, e.g. mysql+pymysql://user:pwd@host/db")
    schema_name: Optional[str] = Field(default=None, description="MySQL schema (uses connection default if None).")
    if_exists: str = Field(default="replace", description="'replace', 'append', or 'fail' (pandas.to_sql arg).")

    def _table_name(self, context, partition_key: Optional[str] = None) -> str:
        parts = list(context.asset_key.path)
        if partition_key is not None:
            parts.append(partition_key)
        elif context.has_asset_partitions:
            parts.append(context.asset_partition_key)
        return "_".join(_sanitize(str(p)) for p in parts)

    def handle_output(self, context, obj) -> None:
        keys = _output_range_keys(context)
        if keys is not None:
            _check_range_mapping("MySQLIOManager", context, obj, keys)
            by_key = {str(k): v for k, v in obj.items()}
            for k in keys:  # validate all before writing any
                if not isinstance(by_key[k], pd.DataFrame):
                    raise TypeError(f"MySQLIOManager only handles DataFrames; got {type(by_key[k])} for partition {k!r}")
            frames = [(self._table_name(context, partition_key=k), by_key[k]) for k in keys]
        else:
            if not isinstance(obj, pd.DataFrame):
                raise TypeError(f"MySQLIOManager only handles DataFrames; got {type(obj)}")
            frames = [(self._table_name(context), obj)]
        engine = create_engine(self.connection_url)
        try:
            for table, frame in frames:
                frame.to_sql(table, engine, schema=self.schema_name, if_exists=self.if_exists, index=False)
        finally:
            engine.dispose()
        metadata = {
            "table": dg.MetadataValue.text(", ".join(t for t, _ in frames)),
            "row_count": dg.MetadataValue.int(sum(len(f) for _, f in frames)),
        }
        if context.has_asset_partitions:
            metadata["partition_key"] = dg.MetadataValue.text(_partition_label(context))
        context.add_output_metadata(metadata)

    def load_input(self, context):
        engine = create_engine(self.connection_url)
        try:
            keys = _input_range_keys(context)
            if keys is not None:
                return {
                    k: pd.read_sql_table(
                        self._table_name(context.upstream_output, partition_key=k), engine, schema=self.schema_name
                    )
                    for k in keys
                }
            return pd.read_sql_table(self._table_name(context.upstream_output), engine, schema=self.schema_name)
        finally:
            engine.dispose()



class MySQLIOManagerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Persist DataFrames to a MySQL/MariaDB schema, one table per asset."""

    resource_key: str = Field(
        default="io_manager",
        description="Dagster resource key. Use 'io_manager' to make this the default.",
    )
    connection_url_env_var: str = Field(default="MYSQL_URL", description="Env var holding the SQLAlchemy URL.")
    schema_name: Optional[str] = Field(default=None, description="MySQL schema name.")
    if_exists: str = Field(default="replace", description="'replace', 'append', or 'fail'.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        io_manager = MySQLIOManager(
            connection_url=dg.EnvVar(self.connection_url_env_var).get_value() or "",
            schema_name=self.schema_name,
            if_exists=self.if_exists,
        )
        return dg.Definitions(resources={self.resource_key: io_manager})
