"""IcebergIOManagerComponent.

ConfigurableIOManager that wraps the official `dagster-iceberg` package's IcebergPyarrowIOManager. Supports REST, Hive, and Glue catalogs via the standard Iceberg catalog config. Each asset becomes a namespaced Iceberg table; partitioned assets get one Iceberg-partition per Dagster-partition.
"""
from typing import Any, Dict, Optional

import dagster as dg
from pydantic import Field

from dagster_iceberg.io_manager.arrow import PyArrowIcebergIOManager
from dagster_iceberg.config import IcebergCatalogConfig


class IcebergIOManagerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Wrap the official `dagster-iceberg` PyArrowIcebergIOManager so assets persist as Iceberg tables."""

    resource_key: str = Field(default="io_manager", description="Dagster resource key. Use 'io_manager' to make this the default.")
    catalog_name: str = Field(default="default", description="Catalog name to load.")
    namespace: str = Field(description="Iceberg namespace (e.g. 'analytics').")
    catalog_uri: Optional[str] = Field(default=None, description="REST/Hive catalog URI.")
    warehouse: Optional[str] = Field(default=None, description="Warehouse path (s3://, gs://, file://).")
    bootstrap_namespace: bool = Field(
        default=True,
        description=(
            "Create `namespace` in the catalog if it doesn't already exist, before the IO "
            "manager's first write. Without this, a brand-new catalog/namespace (a fresh "
            "warehouse, or a real deploy's first run) fails every asset's first write with "
            "pyiceberg.exceptions.NoSuchNamespaceError -- PyArrowIcebergIOManager checks the "
            "namespace exists but never creates it. Set False if the namespace is already "
            "guaranteed to exist (e.g. provisioned out of band) and you'd rather not pay for "
            "one extra catalog round trip at defs-load time."
        ),
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        properties: Dict[str, Any] = {
            k: v for k, v in {"uri": self.catalog_uri, "warehouse": self.warehouse}.items() if v is not None
        }
        if self.bootstrap_namespace:
            from pyiceberg.catalog import load_catalog

            load_catalog(name=self.catalog_name, **properties).create_namespace_if_not_exists(self.namespace)
        io_manager = PyArrowIcebergIOManager(
            name=self.catalog_name,
            config=IcebergCatalogConfig(properties=properties),
            namespace=self.namespace,
        )
        return dg.Definitions(resources={self.resource_key: io_manager})
