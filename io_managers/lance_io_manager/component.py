"""Lance IO Manager component.

YAML/Component wrapper around `LanceIOManager`. Use `resource_key:
io_manager` to make this the default IO manager for the project.
"""
from typing import Optional, Any, Mapping
import dagster as dg
from pydantic import Field
import pandas as pd


def _sanitize(component: str) -> str:
    """Replace characters that produce confusing or invalid table names / paths."""
    return component.replace("[", "--").replace("]", "--").replace(" ", "_")


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


class LanceIOManager(dg.ConfigurableIOManager):
    """ConfigurableIOManager that reads/writes pandas DataFrames (or pyarrow Tables) as LanceDB tables.

    Partition ranges (single_run backfills): when one run covers a range of
    partitions (``BackfillPolicy.single_run()``), the asset must return a dict
    ``{partition_key: DataFrame}`` whose keys equal the range's partition keys; each
    key is written exactly like a single partition. Downstream assets loading the
    range receive a dict ``{partition_key: DataFrame}``.
    """

    base_path: str = Field(description="Directory or object-storage URI where LanceDB tables live")
    table_name_prefix: Optional[str] = Field(default=None, description="Optional prefix prepended to every table name")
    s3_access_key: Optional[str] = Field(default=None, description="S3 access key (resolved from env via dg.EnvVar)")
    s3_secret_key: Optional[str] = Field(default=None, description="S3 secret key (resolved from env via dg.EnvVar)")
    s3_endpoint_url: Optional[str] = Field(default=None, description="Custom S3 endpoint URL (e.g. MinIO)")

    def _get_db(self):
        import lancedb
        storage_options: dict[str, str] = {}
        if self.s3_access_key:
            storage_options["aws_access_key_id"] = self.s3_access_key
        if self.s3_secret_key:
            storage_options["aws_secret_access_key"] = self.s3_secret_key
        if self.s3_endpoint_url:
            storage_options["aws_endpoint"] = self.s3_endpoint_url
            storage_options["allow_http"] = "true"
        return lancedb.connect(self.base_path, storage_options=storage_options or None)

    def _table_name(self, context, partition_key: Optional[str] = None) -> str:
        """Build the LanceDB table name for an asset (or output).

        Layout: ``<prefix><asset_path_joined>[__<partition_key>]``
        Multi-component asset keys are flattened with ``__`` so the result is a
        valid single-segment table name. Partition key (when present) is
        appended after a ``__`` separator.
        """
        prefix = self.table_name_prefix or ""
        if context.has_asset_key:
            base = "__".join(_sanitize(p) for p in context.asset_key.path)
        else:
            base = "__".join(_sanitize(p) for p in (context.run_id, context.step_key, context.name))
        if partition_key is None and context.has_partition_key:
            partition_key = context.partition_key
        if partition_key is not None:
            return f"{prefix}{base}__{_sanitize(str(partition_key))}"
        return f"{prefix}{base}"

    @staticmethod
    def _validate(obj: Any) -> int:
        """Lance accepts pandas DataFrames and pyarrow Tables; reject everything else. Returns row count."""
        try:
            import pyarrow as pa
            is_arrow = isinstance(obj, pa.Table)
        except Exception:
            is_arrow = False
        if not isinstance(obj, pd.DataFrame) and not is_arrow:
            raise TypeError(
                f"LanceIOManager only handles pandas DataFrames or pyarrow Tables; got {type(obj).__name__}. "
                f"Use a different IO manager for non-tabular outputs."
            )
        return len(obj) if isinstance(obj, pd.DataFrame) else obj.num_rows

    def handle_output(self, context: dg.OutputContext, obj: Any) -> None:
        if obj is None:
            return
        keys = _output_range_keys(context)
        if keys is not None:
            _check_range_mapping("LanceIOManager", context, obj, keys)
            by_key = {str(k): v for k, v in obj.items()}
            counts = {k: self._validate(by_key[k]) for k in keys}  # validate all before writing any
            db = self._get_db()
            names = {}
            for k in keys:
                names[k] = self._table_name(context, partition_key=k)
                db.create_table(names[k], by_key[k], mode="overwrite")
            context.add_output_metadata(
                {
                    "object_path": dg.MetadataValue.path(
                        ", ".join(f"{self.base_path.rstrip('/')}/{names[k]}.lance" for k in keys)
                    ),
                    "row_count": dg.MetadataValue.int(sum(counts.values())),
                    "partition_key": dg.MetadataValue.text(_partition_label(context)),
                }
            )
            return
        row_count = self._validate(obj)
        db = self._get_db()
        name = self._table_name(context)
        db.create_table(name, obj, mode="overwrite")
        context.add_output_metadata(
            {
                "object_path": dg.MetadataValue.path(f"{self.base_path.rstrip('/')}/{name}.lance"),
                "row_count": dg.MetadataValue.int(row_count),
                "partition_key": dg.MetadataValue.text(_partition_label(context)),
            }
        )

    def load_input(self, context: dg.InputContext):
        db = self._get_db()
        keys = _input_range_keys(context)
        if keys is not None:
            return {
                k: db.open_table(self._table_name(context.upstream_output, partition_key=k)).to_pandas()
                for k in keys
            }
        name = self._table_name(context.upstream_output)
        return db.open_table(name).to_pandas()



class LanceIOManagerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a LanceDB IO manager for ML-optimised columnar storage.

    Features:
      - Partitioned assets land in tables named ``<asset_key>__<partition_key>``
      - Multi-component asset keys (e.g. ``["features","embeddings","text"]``) are
        flattened with ``__`` so each combination becomes its own table
      - Output metadata records the table path, row count, and partition key
      - Backed by a local directory or any object-storage URI (S3, GCS,
        S3-compatible MinIO via ``s3_endpoint_url``)

    To make this the default IO manager for the project, leave
    ``resource_key`` as ``io_manager``.
    """

    resource_key: str = Field(
        default="io_manager",
        description="Dagster resource key for this IO manager. Use 'io_manager' to make it the default.",
    )
    base_path: str = Field(
        default="./lance_data",
        description="Directory (or S3/GCS URI) where LanceDB tables are stored",
    )
    table_name_prefix: Optional[str] = Field(
        default=None,
        description="Optional prefix prepended to every table name, e.g. 'prod_'",
    )
    s3_access_key_env_var: Optional[str] = Field(
        default=None,
        description="Environment variable for AWS access key when using S3-based storage",
    )
    s3_secret_key_env_var: Optional[str] = Field(
        default=None,
        description="Environment variable for AWS secret key when using S3-based storage",
    )
    s3_endpoint_url: Optional[str] = Field(
        default=None,
        description="Custom S3 endpoint URL, e.g. 'http://localhost:9000' for MinIO",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        io_manager = LanceIOManager(
            base_path=self.base_path,
            table_name_prefix=self.table_name_prefix,
            s3_access_key=dg.EnvVar(self.s3_access_key_env_var) if self.s3_access_key_env_var else None,
            s3_secret_key=dg.EnvVar(self.s3_secret_key_env_var) if self.s3_secret_key_env_var else None,
            s3_endpoint_url=self.s3_endpoint_url,
        )
        return dg.Definitions(resources={self.resource_key: io_manager})
