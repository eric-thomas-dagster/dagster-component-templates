"""MLflow Resource component."""
from dataclasses import dataclass
from typing import Optional
import os
import dagster as dg
from pydantic import Field


class MLflowResource(dg.ConfigurableResource):
    tracking_uri: str
    experiment_name: str = ""
    username: str = ""
    password: str = ""

    def configure(self) -> None:
        import mlflow
        mlflow.set_tracking_uri(self.tracking_uri)
        if self.username:
            os.environ["MLFLOW_TRACKING_USERNAME"] = self.username
        if self.password:
            os.environ["MLFLOW_TRACKING_PASSWORD"] = self.password
        if self.experiment_name:
            mlflow.set_experiment(self.experiment_name)

    def get_client(self):
        import mlflow
        self.configure()
        return mlflow.MlflowClient()


@dataclass
class MLflowResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register an MLflow resource for experiment tracking and model registry."""

    resource_key: str = Field(default="mlflow_resource", description="Key used to register this resource. Other components reference it via resource_key.")
    tracking_uri: str = Field(description="MLflow tracking server URI, e.g. 'http://localhost:5000', 'databricks', or an S3/GCS path")
    experiment_name: Optional[str] = Field(default=None, description="Default experiment name")
    username_env_var: Optional[str] = Field(default=None, description="Environment variable holding the MLflow server username")
    password_env_var: Optional[str] = Field(default=None, description="Environment variable holding the MLflow server password")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = MLflowResource(
            tracking_uri=self.tracking_uri,
            experiment_name=self.experiment_name or "",
            username=os.environ.get(self.username_env_var, "") if self.username_env_var else "",
            password=os.environ.get(self.password_env_var, "") if self.password_env_var else "",
        )
        return dg.Definitions(resources={self.resource_key: resource})
