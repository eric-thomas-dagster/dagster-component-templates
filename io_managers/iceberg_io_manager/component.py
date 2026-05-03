"""IcebergIOManagerComponent.

YAML/Component wrapper around `IcebergIOManager`.
"""
from typing import Optional

import dagster as dg
from pydantic import Field

from .io_manager import IcebergIOManager


class IcebergIOManagerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Persist DataFrames as Apache Iceberg tables via PyIceberg."""

    resource_key: str = Field(
        default="io_manager",
        description="Dagster resource key. Use 'io_manager' to make this the default.",
    )
    catalog_name: str = Field(default="default", description="Catalog name to load.")
    namespace: str = Field(description="Iceberg namespace (e.g. 'analytics').")
    catalog_uri: Optional[str] = Field(default=None, description="REST/Hive catalog URI.")
    warehouse: Optional[str] = Field(default=None, description="Warehouse path (s3://, gs://, file://).")
    catalog_type: str = Field(default="rest", description="'rest', 'hive', or 'glue'.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        io_manager = IcebergIOManager(
            catalog_name=self.catalog_name,
            namespace=self.namespace,
            catalog_properties={
                k: v for k, v in {
                    "type": self.catalog_type,
                    "uri": self.catalog_uri,
                    "warehouse": self.warehouse,
                }.items() if v is not None
            },
        )
        return dg.Definitions(resources={self.resource_key: io_manager})
