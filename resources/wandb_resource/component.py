"""Weights & Biases Resource component."""
from dataclasses import dataclass
from typing import Optional
import os
import dagster as dg
from pydantic import Field


class WandbResource(dg.ConfigurableResource):
    api_key: str
    entity: str = ""
    project: str = ""

    def login(self) -> None:
        import wandb
        wandb.login(key=self.api_key)

    def init_run(self, run_name: str = "", **kwargs):
        import wandb
        wandb.login(key=self.api_key)
        return wandb.init(
            project=self.project or None,
            entity=self.entity or None,
            name=run_name or None,
            **kwargs,
        )


@dataclass
class WandbResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a Weights & Biases resource for experiment tracking."""

    resource_key: str = Field(default="wandb_resource", description="Key used to register this resource. Other components reference it via resource_key.")
    api_key_env_var: str = Field(description="Environment variable holding the W&B API key")
    entity: Optional[str] = Field(default=None, description="W&B entity/team name")
    project: Optional[str] = Field(default=None, description="Default W&B project name")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = WandbResource(
            api_key=os.environ.get(self.api_key_env_var, ""),
            entity=self.entity or "",
            project=self.project or "",
        )
        return dg.Definitions(resources={self.resource_key: resource})
