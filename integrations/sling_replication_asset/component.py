"""SlingReplicationAssetComponent.

Wraps `dagster-sling` to run Sling replication YAMLs as Dagster assets — one asset per stream in the replication. Each materialization runs the configured stream from source to target.
"""
from typing import Dict, List, Optional

import dagster as dg
from pydantic import Field


class SlingReplicationAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Run a Sling YAML replication as Dagster assets via dagster-sling."""

    replication_config_path: str = Field(description="Path to a Sling replication YAML file.")
    group_name: str = Field(default="sling", description="Asset group.")
    name: str = Field(default="sling_replication", description="Asset name prefix.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        from dagster_sling import SlingResource, sling_assets, DagsterSlingTranslator
        import yaml
        with open(self.replication_config_path) as f:
            replication_config = yaml.safe_load(f)

        @sling_assets(
            replication_config=replication_config,
            name=self.name,
            group_name=self.group_name,
        )
        def _sling_assets(context, sling: SlingResource):
            yield from sling.replicate(context=context)

        return dg.Definitions(assets=[_sling_assets], resources={"sling": SlingResource()})

