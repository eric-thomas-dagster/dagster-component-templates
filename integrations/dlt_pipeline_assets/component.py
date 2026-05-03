"""DltPipelineAssetsComponent.

Wraps `dagster-dlt`'s `build_dlt_asset_specs` to surface dlt pipeline resources as Dagster assets. Each dlt resource becomes a Dagster asset; materialization runs the dlt pipeline. The official path for dlt + Dagster integration.
"""
from typing import Dict, List, Optional

import dagster as dg
from pydantic import Field


class DltPipelineAssetsComponent(dg.Component, dg.Model, dg.Resolvable):
    """Run a dlt (data-load-tool) pipeline as Dagster assets via dagster-dlt."""

    pipeline_module: str = Field(description="Importable module containing the dlt pipeline definition, e.g. 'my_dlt_pipelines.stripe_pipeline'.")
    pipeline_name: str = Field(description="dlt pipeline variable name in that module.")
    source_module: str = Field(description="Importable module with the dlt source, e.g. 'my_dlt_pipelines.stripe_source'.")
    source_name: str = Field(description="dlt source variable name (or factory function).")
    group_name: str = Field(default="dlt", description="Asset group.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        from dagster_dlt import DagsterDltResource, dlt_assets
        import importlib
        pipeline = getattr(importlib.import_module(self.pipeline_module), self.pipeline_name)
        source = getattr(importlib.import_module(self.source_module), self.source_name)
        if callable(source):
            source = source()

        @dlt_assets(
            dlt_source=source,
            dlt_pipeline=pipeline,
            group_name=self.group_name,
            name=f"dlt_{self.pipeline_name}",
        )
        def _dlt_assets(context, dlt: DagsterDltResource):
            yield from dlt.run(context=context)

        return dg.Definitions(assets=[_dlt_assets], resources={"dlt": DagsterDltResource()})

