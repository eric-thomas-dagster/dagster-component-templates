"""IcebergIOManager.

ConfigurableIOManager that writes pandas DataFrames to Iceberg tables. Supports REST, Hive, and Glue catalogs. Each asset becomes a namespaced Iceberg table; partitioned assets get one Iceberg-partition per Dagster-partition.
"""
from typing import Any, Optional

import dagster as dg
import pandas as pd
from pydantic import Field

from pyiceberg.catalog import load_catalog
import pyarrow as pa


def _sanitize(s: str) -> str:
    return s.replace("[", "--").replace("]", "--").replace(" ", "_").replace("/", "__")


class IcebergIOManager(dg.ConfigurableIOManager):
    """Persist DataFrames as Apache Iceberg tables via PyIceberg."""

    catalog_name: str = Field(default="default", description="Catalog name to load.")
    namespace: str = Field(description="Iceberg namespace (e.g. 'analytics').")
    catalog_properties: dict = Field(default_factory=dict, description="Properties passed to load_catalog (uri, warehouse, etc.).")

    def _table_name(self, context) -> str:
        parts = list(context.asset_key.path)
        if context.has_asset_partitions:
            parts.append(context.asset_partition_key)
        return "_".join(_sanitize(str(p)) for p in parts)

    def handle_output(self, context, obj) -> None:
        if not isinstance(obj, pd.DataFrame):
            raise TypeError(f"IcebergIOManager only handles DataFrames; got {type(obj)}")
        catalog = load_catalog(self.catalog_name, **self.catalog_properties)
        table_name = f"{self.namespace}.{self._table_name(context)}"
        arrow_table = pa.Table.from_pandas(obj)
        try:
            iceberg_table = catalog.load_table(table_name)
        except Exception:
            iceberg_table = catalog.create_table(table_name, schema=arrow_table.schema)
        iceberg_table.overwrite(arrow_table)
        context.add_output_metadata({"table": dg.MetadataValue.text(table_name), "row_count": dg.MetadataValue.int(len(obj))})

    def load_input(self, context):
        catalog = load_catalog(self.catalog_name, **self.catalog_properties)
        table_name = f"{self.namespace}.{self._table_name(context.upstream_output)}"
        return catalog.load_table(table_name).scan().to_pandas()
