"""FabricLakehouseIOManagerComponent.

Wires a `FabricLakehouseIOManager` into Dagster as a configurable IO
manager — assets that don't override `io_manager_key` will land as Delta
tables in the configured Fabric Lakehouse on OneLake.
"""
from typing import Optional
from collections.abc import Mapping
import dagster as dg
from pydantic import Field
import os
import pandas as pd


def _sanitize(s: str) -> str:
    return "".join(c if c.isalnum() else "_" for c in s).strip("_").lower() or "t"


def _is_partition_range(context) -> bool:
    """True when the context covers a partition RANGE (BackfillPolicy.single_run):
    it has asset partitions but no single partition_key."""
    return bool(context.has_asset_partitions) and not context.has_partition_key


def _range_label(context) -> str:
    keys = list(context.asset_partition_keys)
    return f"{keys[0]}...{keys[-1]}" if len(keys) > 1 else str(keys[0])


def _range_items(context, obj, who: str):
    """Validate a range write: obj must be {partition_key: DataFrame} for exactly the range's keys."""
    keys = list(context.asset_partition_keys)
    if not isinstance(obj, Mapping):
        raise TypeError(
            f"{who}: this output covers a partition range ({_range_label(context)}), e.g. a "
            f"BackfillPolicy.single_run() backfill. A multi-partition output must be a dict "
            f"{{partition_key: DataFrame}} with one entry per partition key {keys}; got {type(obj).__name__}."
        )
    missing = [k for k in keys if k not in obj]
    extra = [k for k in obj if k not in keys]
    if missing or extra:
        raise ValueError(
            f"{who}: dict keys must equal the partition keys of the range. "
            f"Missing: {missing}; unexpected: {extra}."
        )
    for k in keys:
        if not isinstance(obj[k], pd.DataFrame):
            raise TypeError(f"{who}: value for partition {k!r} must be a DataFrame; got {type(obj[k]).__name__}.")
    return [(k, obj[k]) for k in keys]


class FabricLakehouseIOManager(dg.ConfigurableIOManager):
    """ConfigurableIOManager that writes pandas DataFrames as Delta tables in OneLake.

    Partition ranges (single_run backfills): for an asset with
    ``BackfillPolicy.single_run()`` a run covers a range of partitions. The
    asset must then return ``{partition_key: DataFrame}`` covering exactly the
    range's keys; each is written to its own per-partition Delta table (one table per partition key). Loading a
    range returns ``{partition_key: DataFrame}``. Single-partition and
    unpartitioned behaviour is unchanged.
    """

    workspace_id: str = Field(description="Fabric workspace ID (GUID)")
    lakehouse_name: str = Field(description="Lakehouse item display name")
    if_exists: str = Field(default="overwrite", description="'overwrite' | 'append'")
    table_prefix: str = Field(default="", description="Optional prefix prepended to all table names")

    tenant_id_env_var: Optional[str] = Field(default=None)
    client_id_env_var: Optional[str] = Field(default=None)
    client_secret_env_var: Optional[str] = Field(default=None)
    use_azure_cli: bool = Field(
        default=False,
        description="Force Azure CLI auth when no service principal is set (local dev only -- needs an interactive `az login` session). Leave false in production/CI to let delta-rs/object_store's own default credential resolution run instead (can include managed identity).",
    )

    def _table_name(self, context, partition_key=None) -> str:
        parts = [_sanitize(p) for p in context.asset_key.path]
        if partition_key is not None:
            parts.append(_sanitize(str(partition_key)))
        elif context.has_asset_partitions:
            parts.append(_sanitize(str(context.asset_partition_key)))
        name = "__".join(parts)
        return f"{self.table_prefix}{name}" if self.table_prefix else name

    def _table_path(self, context, partition_key=None) -> str:
        return (
            f"abfss://{self.workspace_id}@onelake.dfs.fabric.microsoft.com/"
            f"{self.lakehouse_name}.Lakehouse/Tables/{self._table_name(context, partition_key)}"
        )

    def _storage_options(self) -> dict:
        tenant = os.environ.get(self.tenant_id_env_var) if self.tenant_id_env_var else None
        client = os.environ.get(self.client_id_env_var) if self.client_id_env_var else None
        secret = os.environ.get(self.client_secret_env_var) if self.client_secret_env_var else None
        if tenant and client and secret:
            return {
                "azure_tenant_id": tenant,
                "azure_client_id": client,
                "azure_client_secret": secret,
            }
        if self.use_azure_cli:
            return {"use_azure_cli": "true"}
        # No explicit credential: let delta-rs/object_store's own default
        # credential resolution run instead of forcing CLI-only auth, which
        # fails outright in any non-interactive deployment (CI, a Dagster+
        # agent, an Azure VM without `az login`).
        return {}

    def handle_output(self, context, obj) -> None:
        if obj is None:
            return
        from deltalake import write_deltalake
        if _is_partition_range(context):
            items = _range_items(context, obj, "FabricLakehouseIOManager")
            paths, tables = [], []
            for pkey, df in items:
                path = self._table_path(context, pkey)
                write_deltalake(path, df, mode=self.if_exists, storage_options=self._storage_options())
                paths.append(path)
                tables.append(self._table_name(context, pkey))
            context.add_output_metadata({
                "onelake_paths": dg.MetadataValue.json(paths),
                "tables": dg.MetadataValue.json(tables),
                "partition_key": dg.MetadataValue.text(_range_label(context)),
                "row_count": dg.MetadataValue.int(sum(len(df) for _, df in items)),
                "if_exists": dg.MetadataValue.text(self.if_exists),
            })
            return
        if not isinstance(obj, pd.DataFrame):
            raise TypeError(
                f"FabricLakehouseIOManager only handles pandas DataFrames; got {type(obj).__name__}."
            )
        path = self._table_path(context)
        write_deltalake(
            path,
            obj,
            mode=self.if_exists,
            storage_options=self._storage_options(),
        )
        context.add_output_metadata({
            "onelake_path": dg.MetadataValue.text(path),
            "table": dg.MetadataValue.text(self._table_name(context)),
            "row_count": dg.MetadataValue.int(len(obj)),
            "if_exists": dg.MetadataValue.text(self.if_exists),
        })

    def load_input(self, context):
        from deltalake import DeltaTable
        upstream = context.upstream_output
        if _is_partition_range(upstream):
            return {
                pkey: DeltaTable(self._table_path(upstream, pkey), storage_options=self._storage_options()).to_pandas()
                for pkey in upstream.asset_partition_keys
            }
        path = self._table_path(upstream)
        return DeltaTable(path, storage_options=self._storage_options()).to_pandas()


class FabricLakehouseIOManagerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a FabricLakehouseIOManager as a Dagster IO manager."""

    resource_key: str = Field(
        default="io_manager",
        description="IO manager key. 'io_manager' makes this the workspace default.",
    )
    workspace_id: str = Field(description="Fabric workspace ID (GUID)")
    lakehouse_name: str = Field(description="Lakehouse item display name")
    if_exists: str = Field(default="overwrite", description="'overwrite' | 'append'")
    table_prefix: str = Field(default="", description="Optional prefix prepended to all table names")
    tenant_id_env_var: Optional[str] = Field(default=None)
    client_id_env_var: Optional[str] = Field(default=None)
    client_secret_env_var: Optional[str] = Field(default=None)
    use_azure_cli: bool = Field(
        default=False,
        description="Force Azure CLI auth when no service principal is set (local dev only -- needs an interactive `az login` session). Leave false in production/CI to let delta-rs/object_store's own default credential resolution run instead.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        io_manager = FabricLakehouseIOManager(
            workspace_id=self.workspace_id,
            lakehouse_name=self.lakehouse_name,
            if_exists=self.if_exists,
            table_prefix=self.table_prefix,
            tenant_id_env_var=self.tenant_id_env_var,
            client_id_env_var=self.client_id_env_var,
            client_secret_env_var=self.client_secret_env_var,
            use_azure_cli=self.use_azure_cli,
        )
        return dg.Definitions(resources={self.resource_key: io_manager})
