"""External Temporal Workflow — declare-only AssetSpec.

Surfaces a Temporal Workflow (or Temporal Schedule) as an external asset in
the Dagster catalog. Temporal owns the execution + schedule; Dagster observes
via the paired ``temporal_workflow_sensor``.

Pattern mirrors ``external_argo_workflow`` / ``external_precisely_job``:
declare-only (no execution), marks ``dagster.observability_type=external``,
downstream assets can ``deps: [...]`` against it.

Pair with ``temporal_workflow_sensor`` (Case A — Temporal owns the schedule)
or ``temporal_workflow_trigger`` (Case B — Dagster fires the workflow).
Don't mix both on the same ``asset_key``.

Docs: https://docs.temporal.io/develop/python
"""
from typing import Dict, List, Optional

import dagster as dg
from pydantic import Field


class ExternalTemporalWorkflowAsset(dg.Component, dg.Model, dg.Resolvable):
    """Declare a Temporal Workflow as an external Dagster asset.

    Example:
        ```yaml
        type: dagster_community_components.ExternalTemporalWorkflowAsset
        attributes:
          asset_key: temporal/etl/nightly_aggregation
          workflow_type: NightlyAggregationWorkflow
          namespace: default
          task_queue: etl-tq
          temporal_ui_url: http://localhost:8233
          group_name: temporal
        ```
    """

    asset_key: str = Field(description="Dagster asset key, '/'-separated (e.g. 'temporal/etl/nightly').")
    workflow_type: Optional[str] = Field(
        default=None,
        description="Temporal Workflow class name (registered on the worker).",
    )
    workflow_id: Optional[str] = Field(
        default=None,
        description="Specific Workflow ID this asset represents (for a fixed workflow_id, not template-derived).",
    )
    schedule_id: Optional[str] = Field(
        default=None,
        description="Temporal Schedule ID (when the workflow is fired by a Temporal Schedule).",
    )
    task_queue: Optional[str] = Field(
        default=None,
        description="Task queue the workflow's worker polls (for context / docs).",
    )
    namespace: str = Field(default="default", description="Temporal namespace.")
    temporal_ui_url: Optional[str] = Field(
        default=None,
        description="Temporal Web UI base URL — surfaces as a clickable link in the catalog.",
    )
    group_name: Optional[str] = Field(default="temporal", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds (auto-includes 'temporal', 'workflow').",
    )
    deps: Optional[List[str]] = Field(default=None, description="Upstream Dagster asset keys.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _kinds = set(self.kinds or [])
        _kinds.update({"temporal", "workflow"})

        metadata: Dict[str, object] = {"dagster.observability_type": "external"}
        if self.workflow_type:
            metadata["temporal_workflow_type"] = self.workflow_type
        if self.workflow_id:
            metadata["temporal_workflow_id"] = self.workflow_id
        if self.schedule_id:
            metadata["temporal_schedule_id"] = self.schedule_id
        if self.task_queue:
            metadata["temporal_task_queue"] = self.task_queue
        metadata["temporal_namespace"] = self.namespace
        if self.temporal_ui_url:
            metadata["temporal_ui_url"] = dg.MetadataValue.url(self.temporal_ui_url)

        spec = dg.AssetSpec(
            key=dg.AssetKey(self.asset_key.split("/")),
            group_name=self.group_name,
            description=self.description or (
                f"Temporal Workflow "
                f"{self.workflow_type or self.workflow_id or '(unspecified)'} "
                f"— declared external; updated via temporal_workflow_sensor."
            ),
            kinds=_kinds,
            owners=self.owners,
            tags=self.tags,
            metadata=metadata,
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        return dg.Definitions(assets=[spec])
