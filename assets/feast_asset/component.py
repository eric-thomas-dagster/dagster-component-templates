import os
from datetime import datetime, timedelta
from typing import Optional

import dagster as dg
from pydantic import Field


@dg.definitions
class FeastAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Materialize features from a source table into a Feast feature store.

    Wraps ``store.materialize_incremental`` (default) or ``store.materialize``
    for a full date-range materialization, depending on the ``incremental``
    flag.  The resulting Dagster asset surfaces feature-view names, end date,
    and materialization mode as asset metadata for observability.
    """

    # --- Feature store config -------------------------------------------------
    feature_store_repo_path: str = Field(
        default=".",
        description=(
            "Path to the Feast repository directory that contains "
            "``feature_store.yaml``."
        ),
    )
    feature_views: Optional[list[str]] = Field(
        default=None,
        description=(
            "Specific feature view names to materialize. "
            "When None all feature views in the store are materialized."
        ),
    )
    incremental: bool = Field(
        default=True,
        description=(
            "When True (default) use ``store.materialize_incremental``, which "
            "materializes from the last materialization timestamp to ``end_date``. "
            "When False use ``store.materialize`` with an explicit start date."
        ),
    )
    start_date_env_var: Optional[str] = Field(
        default=None,
        description=(
            "Name of an environment variable whose value is an ISO-8601 date/datetime "
            "string used as ``start_date`` for non-incremental materialization. "
            "Required when ``incremental`` is False."
        ),
    )
    end_date_offset_days: int = Field(
        default=0,
        description=(
            "Number of days before UTC now to use as ``end_date``. "
            "0 means use the current UTC time."
        ),
    )

    # --- Asset metadata -------------------------------------------------------
    group_name: Optional[str] = Field(
        default="feature_store",
        description="Dagster asset group name.",
    )
    asset_name: str = Field(
        default="feast_features",
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
            kinds={"feast", "feature_store"},
        )
        def _feast_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            from feast import FeatureStore

            store = FeatureStore(repo_path=component.feature_store_repo_path)

            end_date = datetime.utcnow() - timedelta(days=component.end_date_offset_days)

            if component.incremental:
                context.log.info(
                    f"Running incremental materialization up to {end_date.isoformat()}"
                )
                store.materialize_incremental(
                    end_date=end_date,
                    feature_views=component.feature_views,
                )
                mode = "incremental"
            else:
                if not component.start_date_env_var:
                    raise ValueError(
                        "start_date_env_var must be set when incremental=False."
                    )
                start_date_str = os.environ.get(component.start_date_env_var)
                if not start_date_str:
                    raise ValueError(
                        f"Environment variable '{component.start_date_env_var}' "
                        "is not set or is empty."
                    )
                start_date = datetime.fromisoformat(start_date_str)
                context.log.info(
                    f"Running full materialization from {start_date.isoformat()} "
                    f"to {end_date.isoformat()}"
                )
                store.materialize(
                    start_date=start_date,
                    end_date=end_date,
                    feature_views=component.feature_views,
                )
                mode = "full"

            feature_views_materialized = component.feature_views or ["<all>"]
            context.log.info(
                f"Feast materialization complete. mode={mode}, "
                f"feature_views={feature_views_materialized}"
            )

            return dg.MaterializeResult(
                metadata={
                    "feature_views": str(feature_views_materialized),
                    "end_date": end_date.isoformat(),
                    "mode": mode,
                    "feature_store_repo_path": component.feature_store_repo_path,
                }
            )

        return dg.Definitions(assets=[_feast_asset])
