"""SlingReplicationAssetComponent.

Wraps `dagster-sling` to run Sling replication YAMLs as Dagster assets — one asset per stream in the replication. Each materialization runs the configured stream from source to target.
"""
from pathlib import Path
from typing import Optional

import dagster as dg
from pydantic import Field


class SlingReplicationAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Run a Sling YAML replication as Dagster assets via dagster-sling."""

    replication_config_path: str = Field(description="Path to a Sling replication YAML file (relative to project root, or absolute).")
    group_name: Optional[str] = Field(default="sling", description="Asset group applied to every stream in the replication.")
    name: Optional[str] = Field(default="sling_replication", description="Op name for the replication run.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        from dagster_sling import SlingResource, sling_assets, DagsterSlingTranslator

        # Resolve path relative to project root when not absolute.
        cfg_path = Path(self.replication_config_path)
        if not cfg_path.is_absolute():
            root = getattr(context, "path", None) or Path.cwd()
            cfg_path = Path(root) / cfg_path

        # Bail out cleanly when the replication YAML isn't present yet —
        # e.g. running `dg check` before the user has authored it. The
        # component still validates; the assets show up once the file lands.
        if not cfg_path.exists():
            return dg.Definitions(resources={"sling": SlingResource()})

        # Group_name is applied via a subclassed translator so every stream
        # in the replication picks up the same group.
        _group = self.group_name

        class _GroupTranslator(DagsterSlingTranslator):
            def get_group_name(self, stream_definition):  # type: ignore[override]
                return _group

        @sling_assets(
            replication_config=cfg_path,
            name=self.name,
            dagster_sling_translator=_GroupTranslator() if _group else None,
        )
        def _sling_assets(context, sling: SlingResource):
            yield from sling.replicate(context=context)

        return dg.Definitions(assets=[_sling_assets], resources={"sling": SlingResource()})
