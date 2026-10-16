"""LakeFS IO Manager component.

YAML/Component wrapper around `LakeFSIOManager`. Use `resource_key:
io_manager` to make this the default IO manager for the project.
"""
import dagster as dg
from pydantic import Field
from datetime import datetime, timezone
from typing import Any, Mapping, Optional
import pandas as pd


def _sanitize(component: str) -> str:
    """Replace characters that produce confusing or invalid lakeFS object keys."""
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


class LakeFSIOManager(dg.ConfigurableIOManager):
    """ConfigurableIOManager that reads/writes pandas DataFrames as Parquet on lakeFS.

    Partition ranges (single_run backfills): when one run covers a range of
    partitions (``BackfillPolicy.single_run()``), the asset must return a dict
    ``{partition_key: DataFrame}`` whose keys equal the range's partition keys; each
    key is written exactly like a single partition. Downstream assets loading the
    range receive a dict ``{partition_key: DataFrame}``.
    """

    repository: str = Field(description="lakeFS repository name")
    branch: str = Field(default="main", description="Branch within the repository")
    endpoint_url: str = Field(description="lakeFS server endpoint URL, e.g. 'http://lakefs:8000'")
    prefix: str = Field(default="dagster/assets", description="Key prefix within the branch")
    access_key: Optional[str] = Field(default=None, description="lakeFS access key (resolved from env via dg.EnvVar)")
    secret_key: Optional[str] = Field(default=None, description="lakeFS secret key (resolved from env via dg.EnvVar)")
    commit_per_materialization: bool = Field(
        default=False,
        description="If True, call the lakeFS API after each handle_output to commit the change to the branch",
    )
    commit_message_template: str = Field(
        default="Materialized {asset_key} at {timestamp}",
        description="Format string for commit messages. Substitutions: {asset_key}, {partition_key}, {timestamp}, {row_count}",
    )

    def _get_fs(self):
        """S3-compatible filesystem talking to the lakeFS S3 gateway."""
        import s3fs
        kwargs: dict[str, Any] = {
            "client_kwargs": {"endpoint_url": self.endpoint_url},
        }
        if self.access_key:
            kwargs["key"] = self.access_key
        if self.secret_key:
            kwargs["secret"] = self.secret_key
        return s3fs.S3FileSystem(**kwargs)

    def _object_path(self, context, partition_key: Optional[str] = None) -> str:
        """Build the lakeFS object path for an asset (or output).

        Layout: ``<repository>/<branch>/<prefix>/<asset_path>/<partition_key>.parquet``
        For non-partitioned assets the partition segment is omitted:
        ``<repository>/<branch>/<prefix>/<asset_path>.parquet``
        """
        if context.has_asset_key:
            key_parts = [_sanitize(p) for p in context.asset_key.path]
            base = "/".join(key_parts)
        else:
            base = "/".join(_sanitize(p) for p in (context.run_id, context.step_key, context.name))
        if partition_key is None and context.has_partition_key:
            partition_key = context.partition_key
        if partition_key is not None:
            return f"{self.repository}/{self.branch}/{self.prefix}/{base}/{_sanitize(str(partition_key))}.parquet"
        return f"{self.repository}/{self.branch}/{self.prefix}/{base}.parquet"

    def _format_commit_message(self, context: dg.OutputContext, row_count: int) -> str:
        asset_key = "/".join(context.asset_key.path) if context.has_asset_key else context.step_key
        partition_key = _partition_label(context)
        timestamp = datetime.now(timezone.utc).isoformat()
        return self.commit_message_template.format(
            asset_key=asset_key,
            partition_key=partition_key,
            timestamp=timestamp,
            row_count=row_count,
        )

    def _commit(self, context: dg.OutputContext, row_count: int) -> Optional[str]:
        """Commit the current branch state to lakeFS via the official `lakefs` SDK."""
        try:
            import lakefs
            from lakefs.client import Client
        except ImportError:
            raise ImportError(
                "commit_per_materialization=True requires the `lakefs` package. "
                "Install it with `pip install lakefs`."
            )
        client = Client(
            host=self.endpoint_url,
            username=self.access_key or "",
            password=self.secret_key or "",
        )
        repo = lakefs.Repository(self.repository, client=client)
        branch = repo.branch(self.branch)
        message = self._format_commit_message(context, row_count)
        commit = branch.commit(message=message)
        commit_id = getattr(commit, "id", None) or getattr(commit, "get_commit", lambda: None)()
        return str(commit_id) if commit_id else None

    @staticmethod
    def _validate(obj: Any) -> None:
        if not isinstance(obj, pd.DataFrame):
            raise TypeError(
                f"LakeFSIOManager only handles pandas DataFrames; got {type(obj).__name__}. "
                f"Use a different IO manager for non-DataFrame outputs."
            )

    def handle_output(self, context: dg.OutputContext, obj: pd.DataFrame) -> None:
        if obj is None:
            return
        keys = _output_range_keys(context)
        if keys is not None:
            _check_range_mapping("LakeFSIOManager", context, obj, keys)
            by_key = {str(k): v for k, v in obj.items()}
            for k in keys:  # validate all before writing any
                self._validate(by_key[k])
            fs = self._get_fs()
            paths = []
            for k in keys:
                path = self._object_path(context, partition_key=k)
                with fs.open(path, "wb") as f:
                    by_key[k].to_parquet(f, index=False)
                paths.append(path)
            total_rows = sum(len(by_key[k]) for k in keys)
            path_meta = ", ".join(f"s3://{p}" for p in paths)
        else:
            self._validate(obj)
            path = self._object_path(context)
            with self._get_fs().open(path, "wb") as f:
                obj.to_parquet(f, index=False)
            total_rows = len(obj)
            path_meta = f"s3://{path}"

        metadata: dict[str, Any] = {
            "object_path": dg.MetadataValue.path(path_meta),
            "lakefs_uri": dg.MetadataValue.path(
                f"lakefs://{self.repository}/{self.branch}/{self.prefix}/"
                + "/".join(_sanitize(p) for p in (context.asset_key.path if context.has_asset_key else []))
            ),
            "row_count": dg.MetadataValue.int(total_rows),
            "branch": dg.MetadataValue.text(self.branch),
            "partition_key": dg.MetadataValue.text(_partition_label(context)),
        }

        if self.commit_per_materialization:
            commit_id = self._commit(context, total_rows)
            if commit_id:
                metadata["commit_id"] = dg.MetadataValue.text(commit_id)

        context.add_output_metadata(metadata)

    def load_input(self, context: dg.InputContext):
        fs = self._get_fs()
        keys = _input_range_keys(context)
        if keys is not None:
            out = {}
            for k in keys:
                with fs.open(self._object_path(context.upstream_output, partition_key=k), "rb") as f:
                    out[k] = pd.read_parquet(f)
            return out
        path = self._object_path(context.upstream_output)
        with fs.open(path, "rb") as f:
            return pd.read_parquet(f)



class LakeFSIOManagerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a lakeFS IO manager that stores assets as Parquet on a versioned data lake.

    LakeFS (https://lakefs.io) is a Git-for-data layer over S3-compatible storage:
    every write goes to a *branch*, and *commits* snapshot the branch state. This
    manager writes Parquet via the lakeFS S3 gateway and (optionally) commits
    after each materialization so every asset version is reproducible.

    Features:
      - Partitioned assets land at ``<prefix>/<asset_key>/<partition_key>.parquet`` on the branch
      - Multi-component asset keys (e.g. ``["raw","stripe","customers"]``) become
        nested object paths, preventing collisions between same-named assets
      - Output metadata records the lakeFS URI, branch, row count, partition key,
        and (when commit_per_materialization is True) the commit ID
      - Optional auto-commit per materialization via ``commit_per_materialization=True``
        with a configurable ``commit_message_template``

    To make this the default IO manager for the project, leave
    ``resource_key`` as ``io_manager``.
    """

    resource_key: str = Field(
        default="io_manager",
        description="Dagster resource key for this IO manager. Use 'io_manager' to make it the default.",
    )
    repository: str = Field(description="lakeFS repository name")
    branch: str = Field(default="main", description="Branch within the repository")
    endpoint_url: str = Field(
        description="lakeFS server endpoint URL, e.g. 'http://lakefs:8000'",
    )
    prefix: str = Field(
        default="dagster/assets",
        description="Key prefix within the branch, e.g. 'dagster/assets'",
    )
    lakefs_access_key_env_var: str = Field(
        description="Environment variable holding the lakeFS access key",
    )
    lakefs_secret_key_env_var: str = Field(
        description="Environment variable holding the lakeFS secret key",
    )
    commit_per_materialization: bool = Field(
        default=False,
        description="If True, commit the branch after every successful materialization",
    )
    commit_message_template: str = Field(
        default="Materialized {asset_key} at {timestamp}",
        description="Format string. Available substitutions: {asset_key}, {partition_key}, {timestamp}, {row_count}",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        io_manager = LakeFSIOManager(
            repository=self.repository,
            branch=self.branch,
            endpoint_url=self.endpoint_url,
            prefix=self.prefix,
            access_key=dg.EnvVar(self.lakefs_access_key_env_var) if self.lakefs_access_key_env_var else None,
            secret_key=dg.EnvVar(self.lakefs_secret_key_env_var) if self.lakefs_secret_key_env_var else None,
            commit_per_materialization=self.commit_per_materialization,
            commit_message_template=self.commit_message_template,
        )
        return dg.Definitions(resources={self.resource_key: io_manager})
