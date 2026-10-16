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



# BEGIN field-shape normalization (inlined per component; keep copies identical — see tests/test_field_shapes.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def coerce_field_shapes(data, *, lists=(), csv=(), tags=()):
    """Accept the shapes people (and LLMs) naturally write for common boilerplate fields."""
    if not isinstance(data, dict):
        return data
    out = dict(data)
    for k in lists:  # List[str]: "a" / "a, b" -> ["a", "b"]; strip + drop empties
        v = out.get(k)
        if isinstance(v, str):
            v = v.split(",")
        if isinstance(v, (list, tuple)):
            out[k] = [x.strip() if isinstance(x, str) else x for x in v if not (isinstance(x, str) and not x.strip())]
    for k in csv:  # comma-separated str given as a list -> "a,b"
        v = out.get(k)
        if isinstance(v, (list, tuple, set)):
            out[k] = ",".join(str(x).strip() for x in v)
    for k in tags:  # Dict[str, str]: scalars -> str, ["k=v"] / ["k:v"] -> {k: v}
        v = out.get(k)
        if isinstance(v, (list, tuple)) and v and all(isinstance(x, str) and ("=" in x or ":" in x) for x in v):
            v = dict((p.strip() for p in (x.split("=", 1) if "=" in x else x.split(":", 1))) for x in v)
        if isinstance(v, dict):
            out[k] = {str(a).strip(): (str(b).lower() if isinstance(b, bool) else str(b)) for a, b in v.items() if b is not None}
    return out
# END field-shape normalization


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
    tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds (auto-includes 'argo', 'workflow').")
    deps: Optional[List[str]] = Field(default=None, description="Upstream Dagster asset keys.")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('owners', 'kinds', 'deps'), csv=(), tags=('tags',))

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
            key=dg.AssetKey.from_user_string(self.asset_key),
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
