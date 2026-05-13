"""Iceberg Catalog Resource.

Register a PyIceberg catalog configuration once and reuse it across many
ingestion / sink / observation components. Mirrors `sap_hana_resource` —
holds the connection config, exposes a `catalog()` method that returns a
ready-to-use PyIceberg Catalog instance.

Headless-friendly: all credentials come from env vars (with `${ENV_VAR}`
expansion supported in catalog_properties).
"""

import os
from typing import Dict, Optional

import dagster as dg
from pydantic import Field


def _resolve_env_vars(d: Optional[Dict[str, str]]) -> Dict[str, str]:
    if not d:
        return {}
    out = {}
    for k, v in d.items():
        if isinstance(v, str) and v.startswith("${") and v.endswith("}"):
            out[k] = os.environ.get(v[2:-1], "")
        else:
            out[k] = v
    return out


class IcebergCatalogResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a PyIceberg catalog as a Dagster resource.

    Example — REST catalog (Nessie, Polaris, Lakekeeper, Snowflake, S3 Tables):

        ```yaml
        type: dagster_component_templates.IcebergCatalogResourceComponent
        attributes:
          resource_key: iceberg
          catalog_type: rest
          catalog_properties:
            uri: https://catalog.example.com
            credential: "${CATALOG_CREDENTIAL}"
            warehouse: my_warehouse
        ```

    Custom assets read the catalog via:

        ```python
        @asset(required_resource_keys={"iceberg"})
        def my_asset(context):
            catalog = context.resources.iceberg.catalog()
            table = catalog.load_table(("sales", "orders"))
            return table.scan().to_pandas()
        ```
    """

    resource_key: str = Field(default="iceberg", description="Dagster resource key")

    catalog_type: str = Field(
        default="rest",
        description="'rest' | 'glue' | 'hive' | 'hadoop' | 'sql'",
    )
    catalog_name: str = Field(
        default="default",
        description="PyIceberg local catalog identifier",
    )
    catalog_properties: Optional[Dict[str, str]] = Field(
        default=None,
        description="Catalog-specific config. ${ENV_VAR} expansion supported.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        cfg = self

        class IcebergCatalogResource(dg.ConfigurableResource):
            """Lazy-loaded PyIceberg catalog."""

            def catalog(self):
                """Return a loaded PyIceberg Catalog instance."""
                from pyiceberg.catalog import load_catalog

                props = _resolve_env_vars(cfg.catalog_properties)
                props["type"] = cfg.catalog_type
                return load_catalog(cfg.catalog_name, **props)

            def load_table(self, namespace: str, table_name: str):
                """Convenience: load (namespace, table_name) in one call."""
                return self.catalog().load_table((namespace, table_name))

        return dg.Definitions(resources={cfg.resource_key: IcebergCatalogResource()})
