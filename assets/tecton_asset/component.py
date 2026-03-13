import os
from typing import Optional

import dagster as dg
from pydantic import Field


@dg.definitions
class TectonAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Trigger Tecton feature view materialization jobs via the Tecton SDK.

    Authenticates against a Tecton workspace using an API key, then triggers
    online and/or offline materialization jobs for the specified feature views
    (or all views in the workspace when none are listed).  Job IDs are logged
    and the total count of triggered jobs is surfaced as asset metadata.
    """

    # --- Tecton connection config ---------------------------------------------
    workspace: str = Field(
        description="Tecton workspace name (e.g. 'production', 'staging')."
    )
    tecton_api_key_env_var: str = Field(
        default="TECTON_API_KEY",
        description="Name of the environment variable holding the Tecton API key.",
    )
    tecton_url_env_var: Optional[str] = Field(
        default=None,
        description=(
            "Name of the environment variable holding the Tecton cluster URL. "
            "Defaults to https://app.tecton.ai when not set."
        ),
    )

    # --- Feature view selection -----------------------------------------------
    feature_views: Optional[list[str]] = Field(
        default=None,
        description=(
            "Specific feature view names to trigger materialization for. "
            "When None all feature views in the workspace are triggered."
        ),
    )

    # --- Materialization targets -----------------------------------------------
    online: bool = Field(
        default=True,
        description="Materialize to the online store.",
    )
    offline: bool = Field(
        default=True,
        description="Materialize to the offline store.",
    )

    # --- Asset metadata -------------------------------------------------------
    group_name: Optional[str] = Field(
        default="feature_store",
        description="Dagster asset group name.",
    )
    asset_name: str = Field(
        default="tecton_features",
        description="Dagster asset key for this component.",
    )
    deps: Optional[list[str]] = Field(
        default=None,
        description="Upstream asset keys for lineage.",
    )

    # -------------------------------------------------------------------------
    # build_defs
    # -------------------------------------------------------------------------

    def build_defs(self, load_context: dg.ComponentLoadContext) -> dg.Definitions:
        component = self

        dep_keys = [dg.AssetKey.from_user_string(k) for k in (component.deps or [])]

        @dg.asset(
            name=component.asset_name,
            group_name=component.group_name,
            deps=dep_keys,
            kinds={"tecton", "feature_store"},
        )
        def _tecton_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            import tecton

            api_key = os.environ.get(component.tecton_api_key_env_var)
            if not api_key:
                raise ValueError(
                    f"Environment variable '{component.tecton_api_key_env_var}' "
                    "is not set or is empty."
                )

            tecton_url = "https://app.tecton.ai"
            if component.tecton_url_env_var:
                url_from_env = os.environ.get(component.tecton_url_env_var)
                if url_from_env:
                    tecton_url = url_from_env

            context.log.info(
                f"Authenticating with Tecton at {tecton_url} "
                f"(workspace: {component.workspace})"
            )
            tecton.set_credentials(tecton_url=tecton_url, tecton_api_key=api_key)

            ws = tecton.get_workspace(component.workspace)

            if component.feature_views is not None:
                fv_names = component.feature_views
            else:
                context.log.info(
                    "No feature_views specified — fetching all feature views "
                    f"from workspace '{component.workspace}'."
                )
                fv_names = [fv.name for fv in ws.list_feature_views()]

            context.log.info(
                f"Triggering materialization for {len(fv_names)} feature view(s): "
                f"{fv_names}"
            )

            triggered_jobs = []
            for fv_name in fv_names:
                fv = ws.get_feature_view(fv_name)
                job = fv.run_materialization_job(
                    online=component.online,
                    offline=component.offline,
                )
                context.log.info(
                    f"Triggered materialization job {job.id} for feature view '{fv_name}'"
                )
                triggered_jobs.append({"feature_view": fv_name, "job_id": job.id})

            return dg.MaterializeResult(
                metadata={
                    "workspace": component.workspace,
                    "feature_views_count": len(fv_names),
                    "feature_views": str(fv_names),
                    "online": component.online,
                    "offline": component.offline,
                    "triggered_jobs": str(triggered_jobs),
                }
            )

        return dg.Definitions(assets=[_tecton_asset])
