"""PapertrailResourceComponent.

Wraps `dagster-papertrail`'s log handler so Dagster's run logs are forwarded to a Papertrail destination. Useful for centralized log aggregation when you don't have a heavier observability stack.
"""
from typing import Dict, List, Optional

import dagster as dg
from pydantic import Field


class PapertrailResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Stream Dagster logs to Papertrail via dagster-papertrail."""

    papertrail_hostname: str = Field(description="Papertrail hostname (e.g. 'logs1.papertrailapp.com').")
    papertrail_port: int = Field(description="Papertrail port.")
    resource_key: str = Field(default="papertrail", description="Dagster resource key.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        from dagster_papertrail import papertrail_logger
        # The Papertrail integration registers a logger; we expose the
        # hostname/port via a thin resource that asset code can use.
        @dg.resource(
            config_schema={"hostname": str, "port": int},
            description="Papertrail logging endpoint",
        )
        def _papertrail_endpoint(context):
            return {"hostname": context.resource_config["hostname"], "port": context.resource_config["port"]}
        return dg.Definitions(
            resources={self.resource_key: _papertrail_endpoint.configured({"hostname": self.papertrail_hostname, "port": self.papertrail_port})},
            loggers={"papertrail": papertrail_logger},
        )

