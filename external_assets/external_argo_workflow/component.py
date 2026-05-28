"""External Argo Workflow — declare-only AssetSpec.

Surfaces an Argo Workflow (Kubernetes-native workflow engine) as an
external asset in the Dagster catalog. Argo runs the workflow on its
own schedule; Dagster observes via the paired ``argo_workflow_sensor``.

Pattern mirrors ``external_precisely_job`` / ``external_snowflake_task``:
declare-only (no execution), marks ``dagster.observability_type=external``,
downstream assets can ``deps: [...]`` against it.

Pair with ``argo_workflow_sensor`` (Case A — Argo owns the schedule) or
``argo_workflow_trigger`` (Case B — Dagster fires the workflow). Don't
mix both on the same ``asset_key``.

Docs: https://argo-workflows.readthedocs.io/en/latest/rest-api/
"""
from typing import Dict, List, Optional

import dagster as dg
from pydantic import Field


class ExternalArgoWorkflowAsset(dg.Component, dg.Model, dg.Resolvable):
    """Declare an Argo Workflow as an external Dagster asset.

    Example:
        ```yaml
        type: dagster_community_components.ExternalArgoWorkflowAsset
        attributes:
          asset_key: argo/etl/nightly_aggregation
          workflow_template: nightly-aggregation
          namespace: argo
          argo_ui_url: https://argo.mycompany.com
          group_name: argo
        ```
    """

    asset_key: str = Field(description="Dagster asset key (e.g. 'argo/etl/nightly_aggregation').")
    workflow_template: Optional[str] = Field(
        default=None,
        description="Argo WorkflowTemplate name (the reusable template; templates produce Workflows when triggered).",
    )
    workflow_name: Optional[str] = Field(
        default=None,
        description="Specific Argo Workflow name this asset represents (one-off runs, not template-derived).",
    )
    namespace: str = Field(
        default="argo",
        description="Kubernetes namespace the workflow runs in.",
    )
    argo_ui_url: Optional[str] = Field(
        default=None,
        description="Argo Server UI base URL — surfaced as a clickable link in the catalog.",
    )
    group_name: Optional[str] = Field(default="argo", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds (auto-includes 'argo', 'workflow').")
    deps: Optional[List[str]] = Field(default=None, description="Upstream Dagster asset keys.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("argo")
        kinds.add("workflow")

        metadata: Dict[str, object] = {"dagster.observability_type": "external"}
        if self.workflow_template:
            metadata["argo_workflow_template"] = self.workflow_template
        if self.workflow_name:
            metadata["argo_workflow_name"] = self.workflow_name
        metadata["argo_namespace"] = self.namespace
        if self.argo_ui_url:
            metadata["argo_ui_url"] = dg.MetadataValue.url(self.argo_ui_url)

        spec = dg.AssetSpec(
            key=dg.AssetKey(self.asset_key.split("/")),
            group_name=self.group_name,
            description=self.description or (
                f"Argo Workflow "
                f"{self.workflow_template or self.workflow_name or '(unspecified)'} "
                f"— declared external; updated via argo_workflow_sensor."
            ),
            kinds=kinds,
            owners=self.owners,
            tags=self.tags,
            metadata=metadata,
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        return dg.Definitions(assets=[spec])
