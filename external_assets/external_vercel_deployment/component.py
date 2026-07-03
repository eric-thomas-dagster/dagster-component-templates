"""External Vercel Deployment — declare-only AssetSpec.

Surfaces a Vercel project's deployment stream as an external asset in the
Dagster catalog. Vercel owns the build lifecycle (git push, deploy hook,
etc.); Dagster observes via the paired ``vercel_deployment_sensor``.

Pattern mirrors ``external_temporal_workflow`` / ``external_argo_workflow``:
declare-only (no execution), marks ``dagster.observability_type=external``,
downstream assets can ``deps: [...]`` against it.

Docs: https://vercel.com/docs/rest-api/endpoints/deployments
"""
from typing import Dict, List, Optional

import dagster as dg
from pydantic import Field


class ExternalVercelDeploymentAsset(dg.Component, dg.Model, dg.Resolvable):
    """Declare a Vercel deployment stream as an external Dagster asset.

    Example:
        ```yaml
        type: dagster_community_components.ExternalVercelDeploymentAsset
        attributes:
          asset_key: vercel/website/production
          project_name: my-marketing-site
          target: production
          vercel_dashboard_url: https://vercel.com/my-team/my-marketing-site
          group_name: vercel
        ```
    """

    asset_key: str = Field(description="Dagster asset key, '/'-separated (e.g. 'vercel/website/production').")
    project_id: Optional[str] = Field(
        default=None,
        description="Vercel project ID (prj_...). Preferred — stable across renames.",
    )
    project_name: Optional[str] = Field(
        default=None,
        description="Vercel project slug (URL name). Set this OR project_id.",
    )
    target: str = Field(
        default="production",
        description="Deployment target: 'production' (default), 'preview', 'development'.",
    )
    team_id: Optional[str] = Field(
        default=None,
        description="Vercel team ID (team_...). For team-scoped projects.",
    )
    vercel_dashboard_url: Optional[str] = Field(
        default=None,
        description="Full Vercel dashboard URL for the project (surfaces as a clickable link).",
    )
    production_url: Optional[str] = Field(
        default=None,
        description="Canonical production URL (e.g. https://my-site.com). Surfaces as a clickable link.",
    )

    group_name: Optional[str] = Field(default="vercel", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds (auto-includes 'vercel', 'deployment').",
    )
    deps: Optional[List[str]] = Field(default=None, description="Upstream Dagster asset keys.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _kinds = set(self.kinds or [])
        _kinds.update({"vercel", "deployment"})

        metadata: Dict[str, object] = {"dagster.observability_type": "external"}
        if self.project_id:
            metadata["vercel_project_id"] = self.project_id
        if self.project_name:
            metadata["vercel_project_name"] = self.project_name
        if self.team_id:
            metadata["vercel_team_id"] = self.team_id
        metadata["vercel_target"] = self.target
        if self.vercel_dashboard_url:
            metadata["vercel_dashboard_url"] = dg.MetadataValue.url(self.vercel_dashboard_url)
        if self.production_url:
            metadata["vercel_production_url"] = dg.MetadataValue.url(self.production_url)

        spec = dg.AssetSpec(
            key=dg.AssetKey(self.asset_key.split("/")),
            group_name=self.group_name,
            description=self.description or (
                f"Vercel deployments for {self.project_name or self.project_id or '(unspecified)'} "
                f"({self.target}) — declared external; updated via vercel_deployment_sensor."
            ),
            kinds=_kinds,
            owners=self.owners,
            tags=self.tags,
            metadata=metadata,
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        return dg.Definitions(assets=[spec])
