"""External Precisely Connect ETL job — declare-only AssetSpec.

Surfaces a Precisely Connect ETL job-run as an external asset in the
Dagster catalog. Precisely runs the job; Dagster observes / materializes
via the paired ``precisely_job_sensor``.

Pattern:

  1. Declare the external asset with this component:
        external_precisely_job_asset.asset_key = "precisely/etl/load_customers"
  2. Pair with ``precisely_job_sensor`` (sets the same ``asset_key`` field).
     When the sensor sees a terminal SUCCESS on the configured job-run-id,
     it emits ``AssetMaterialization(asset_key=...)`` — which lights up this
     external asset in the Dagster catalog.

No execution function — Dagster never "runs" a Precisely job because
Precisely does not publish a submit endpoint. The asset's purpose is
*catalog presence*: it shows up in the asset graph, downstream assets
can ``deps: [precisely/etl/load_customers]``, and the materialization
history reflects sensor-detected Precisely runs.
"""
from typing import Dict, List, Optional

import dagster as dg
from pydantic import Field


class ExternalPreciselyJobAsset(dg.Component, dg.Model, dg.Resolvable):
    """Declare a Precisely Connect ETL job as an external Dagster asset.

    Example:
        ```yaml
        type: dagster_community_components.ExternalPreciselyJobAsset
        attributes:
          asset_key: precisely/etl/load_customers
          job_id: "abc-123-xyz"
          host: https://precisely.mycompany.com
          group_name: precisely
        ```
    """

    asset_key: str = Field(description="Dagster asset key (e.g. 'precisely/etl/load_customers').")
    job_id: Optional[str] = Field(
        default=None,
        description="Precisely Connect ETL job ID (the stable id, not run-id).",
    )
    job_run_id: Optional[str] = Field(
        default=None,
        description="Specific job-run id this asset represents (optional metadata).",
    )
    host: Optional[str] = Field(
        default=None,
        description="Precisely Connect ETL host URL — surfaced as a clickable link in the catalog.",
    )
    group_name: Optional[str] = Field(default="precisely", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds (auto-includes 'precisely').")
    deps: Optional[List[str]] = Field(default=None, description="Upstream Dagster asset keys.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("precisely")
        kinds.add("etl")

        metadata: Dict[str, object] = {
            "dagster.observability_type": "external",
        }
        if self.job_id:
            metadata["precisely_job_id"] = self.job_id
        if self.job_run_id:
            metadata["precisely_job_run_id"] = self.job_run_id
        if self.host:
            metadata["precisely_host"] = dg.MetadataValue.url(self.host)

        spec = dg.AssetSpec(
            key=dg.AssetKey(self.asset_key.split("/")),
            group_name=self.group_name,
            description=self.description or (
                f"Precisely Connect ETL run "
                f"{self.job_id or self.job_run_id or '(unspecified id)'} — "
                f"declared external; updated via precisely_job_sensor."
            ),
            kinds=kinds,
            owners=self.owners,
            tags=self.tags,
            metadata=metadata,
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        return dg.Definitions(assets=[spec])
