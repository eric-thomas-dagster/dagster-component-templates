"""MakeGridComponent.

Tile the bounding box of the input geometries (or an explicit bbox) into a regular grid of squares with a configurable cell size. Outputs one row per cell with cell_id, row, col, and a polygon geometry — useful for heatmap-style aggregation by spatial bucket.
"""
from typing import Dict, List, Optional

import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    MetadataValue,
    Model,
    Resolvable,
    asset,
)
from pydantic import Field


class MakeGridComponent(Component, Model, Resolvable):
    """Generate a regular grid of square polygons covering the bounding box of input geometries."""

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame")
    geometry_column: str = Field(default="geometry", description="WKT geometry column to derive bbox from.")
    cell_size_meters: float = Field(description="Grid cell size in meters.")
    metric_crs: str = Field(default="EPSG:3857", description="Metric projection used for cell generation.")
    src_crs: str = Field(default="EPSG:4326", description="Source CRS of input geometries.")
    bbox: Optional[List[float]] = Field(default=None, description="Optional explicit bbox [minx, miny, maxx, maxy] in src_crs. If None, derived from input.")

    include_preview_metadata: bool = Field(
        default=False,
        description="Include a preview of the output DataFrame in metadata (for builder UIs).",
    )
    preview_rows: int = Field(
        default=25,
        ge=1,
        le=500,
        description="Rows in the preview when include_preview_metadata=True.",
    )
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    asset_tags: Optional[Dict[str, str]] = Field(default=None, description="Asset tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds.")
    deps: Optional[List[str]] = Field(default=None, description="Lineage-only upstream asset keys.")


    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale. Builds a FreshnessPolicy when set.",
    )

    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5' (weekdays 9am).",
    )

    column_lineage: Optional[Dict[str, List[str]]] = Field(
        default=None,
        description="Column-level lineage: output column → list of upstream columns it derives from, e.g. {'revenue': ['price', 'quantity']}.",
    )

    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', or None for unpartitioned.",
    )

    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format (e.g. '2024-01-01'). Required for time-based partition types.",
    )

    partition_date_column: Optional[str] = Field(
        default=None,
        description="Column used to filter the upstream DataFrame to the current date partition key.",
    )

    partition_values: Optional[str] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning, e.g. 'acme,globex,initech'.",
    )

    partition_static_dim: Optional[str] = Field(
        default=None,
        description="Dimension name for the static axis in multi-partitioning, e.g. 'customer'.",
    )

    partition_static_column: Optional[str] = Field(
        default=None,
        description="Column used to filter the upstream DataFrame to the current static partition value.",
    )

    retry_policy_max_retries: Optional[int] = Field(
        default=None,
        description="Max retries on failure. Defines a RetryPolicy when set.",
    )

    retry_policy_delay_seconds: Optional[int] = Field(
        default=None,
        description="Seconds between retries (default 1).",
    )

    retry_policy_backoff: str = Field(
        default="exponential",
        description="Backoff strategy: 'linear' or 'exponential'.",
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        # Standard catalog fields — phase 2 wiring
        _retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            _retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )
        _freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy
            _freshness_policy = FreshnessPolicy(
                maximum_lag_minutes=self.freshness_max_lag_minutes,
                cron_schedule=self.freshness_cron,
            )
        _all_tags = dict(self.asset_tags or {})
        for _k in (self.kinds or []):
            _all_tags[f"dagster/kind/{_k}"] = ""
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows
        group_name = self.group_name
        _self = self

        ins = {"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))}
        tags_dict = dict(self.asset_tags or {})
        for k in (self.kinds or ["python"]):
            tags_dict[f"dagster/kind/{k}"] = ""

        @asset(
            key=AssetKey.from_user_string(asset_name),
            ins=ins,
            group_name=group_name,
            description=self.description or "Generate a regular grid of square polygons covering the bounding box of input geometries.",
            tags=tags_dict,
            owners=self.owners or [],
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
            retry_policy=_retry_policy,
            freshness_policy=_freshness_policy,
        )
        def _asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
            df = upstream
            import geopandas as gpd
            import numpy as np
            from shapely import wkt
            from shapely.geometry import box
            if _self.bbox:
                minx, miny, maxx, maxy = _self.bbox
                bbox_geom = gpd.GeoSeries([box(minx, miny, maxx, maxy)], crs=_self.src_crs)
            else:
                df_g = df.copy()
                df_g[_self.geometry_column] = df_g[_self.geometry_column].apply(wkt.loads)
                gdf = gpd.GeoDataFrame(df_g, geometry=_self.geometry_column, crs=_self.src_crs)
                tb = gdf.total_bounds  # numpy array (minx, miny, maxx, maxy)
                bbox_geom = gpd.GeoSeries([box(tb[0], tb[1], tb[2], tb[3])], crs=_self.src_crs)
            bbox_m = bbox_geom.to_crs(_self.metric_crs)
            minx, miny, maxx, maxy = bbox_m.total_bounds
            xs = np.arange(minx, maxx + _self.cell_size_meters, _self.cell_size_meters)
            ys = np.arange(miny, maxy + _self.cell_size_meters, _self.cell_size_meters)
            cells = []
            cell_id = 0
            for ri, y in enumerate(ys[:-1]):
                for ci, x in enumerate(xs[:-1]):
                    cells.append({"cell_id": cell_id, "row": ri, "col": ci, "geometry_m": box(x, y, x + _self.cell_size_meters, y + _self.cell_size_meters)})
                    cell_id += 1
            cells_gdf = gpd.GeoDataFrame(cells, geometry="geometry_m", crs=_self.metric_crs).to_crs(_self.src_crs)
            out_df = pd.DataFrame({
                "cell_id": cells_gdf["cell_id"].values,
                "row": cells_gdf["row"].values,
                "col": cells_gdf["col"].values,
                _self.geometry_column: cells_gdf.geometry.apply(lambda g: g.wkt).values,
            })

            if include_preview and len(out_df) > 0:
                try:
                    _prev = out_df.sample(min(preview_rows, len(out_df))) if len(out_df) > preview_rows * 10 else out_df.head(preview_rows)
                    context.add_output_metadata({"preview": MetadataValue.md(_prev.to_markdown(index=False))})
                except Exception as _e:
                    context.log.warning(f"preview emission failed: {_e}")
            return out_df

        return Definitions(assets=[_asset])
