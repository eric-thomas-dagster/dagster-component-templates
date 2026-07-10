"""TM1 Cube Data Ingestion Component.

Materialize a slice of a TM1 cube via MDX as a Dagster DataFrame asset.
Turn TM1's multidimensional cube data into flat rows for downstream Dagster
transforms, dbt models, or warehouse loads.
"""
from typing import List, Optional

import dagster as dg
from pydantic import Field


class TM1CubeDataIngestionComponent(dg.Component, dg.Model, dg.Resolvable):
    """Materialize a TM1 cube slice via MDX as a DataFrame asset.

    Two query modes:

    1. **MDX** — pass a full MDX statement; the component executes it via
       `/api/v1/ExecuteMDX` and flattens the resulting cellset.
    2. **Cube + Row/Column dims** (simplified) — specify a cube name plus
       row-axis and column-axis dimensions; the component builds an
       MDX statement that projects all leaf members. Right for "give me
       every combination of these dimensions" — the common warehouse-load
       pattern.

    Example (MDX):

        ```yaml
        type: dagster_community_components.TM1CubeDataIngestionComponent
        attributes:
          asset_key: actuals_by_month
          cube: Finance
          mdx: |
            SELECT
              NON EMPTY [Period].Members ON ROWS,
              NON EMPTY [Account].Members ON COLUMNS
            FROM [Finance]
            WHERE ([Version].[Actual], [Year].[2026])
          group_name: tm1_actuals
          resource_key: tm1_resource
        ```

    Example (simplified cube + dims):

        ```yaml
        attributes:
          asset_key: sales_by_product_period
          cube: Sales
          row_dimensions: [Period]
          column_dimensions: [Product]
          filters: {Version: Actual, Year: "2026"}
          group_name: tm1_sales
          resource_key: tm1_resource
        ```
    """

    asset_key: str = Field(description="Asset key for the emitted DataFrame.")
    cube: str = Field(description="TM1 cube name.")
    mdx: Optional[str] = Field(default=None, description="Explicit MDX statement. If set, row/column/filter fields are ignored.")
    row_dimensions: Optional[List[str]] = Field(default=None, description="Simplified mode: dimensions to project on rows.")
    column_dimensions: Optional[List[str]] = Field(default=None, description="Simplified mode: dimensions to project on columns.")
    filters: Optional[dict] = Field(default=None, description="Simplified mode: {dimension: member} WHERE clause.")

    group_name: Optional[str] = Field(default=None, description="Dagster asset group name.")
    resource_key: str = Field(default="tm1_resource", description="Resource key to look up.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        required_resource_keys = {self.resource_key}

        @dg.asset(
            name=_self.asset_key,
            group_name=_self.group_name,
            required_resource_keys=required_resource_keys,
            compute_kind="tm1",
        )
        def _the_asset(context: dg.AssetExecutionContext):
            try:
                import pandas as pd
                import requests
            except ImportError as e:
                raise Exception("pandas or requests library not installed") from e

            resource = getattr(context.resources, _self.resource_key)
            session = requests.Session()
            session.verify = resource.verify_ssl
            headers = resource.get_auth_headers()

            # Build MDX if not explicitly provided.
            if _self.mdx:
                mdx = _self.mdx
            else:
                if not _self.row_dimensions or not _self.column_dimensions:
                    raise Exception("Either `mdx` OR (`row_dimensions` + `column_dimensions`) must be provided")
                rows_axis = ", ".join(f"[{d}].Members" for d in _self.row_dimensions)
                cols_axis = ", ".join(f"[{d}].Members" for d in _self.column_dimensions)
                where = ""
                if _self.filters:
                    where = " WHERE (" + ", ".join(f"[{d}].[{m}]" for d, m in _self.filters.items()) + ")"
                mdx = (
                    f"SELECT NON EMPTY {{{rows_axis}}} ON ROWS, "
                    f"NON EMPTY {{{cols_axis}}} ON COLUMNS "
                    f"FROM [{_self.cube}]{where}"
                )

            # Execute MDX via POST /api/v1/ExecuteMDX
            execute_url = f"{resource.api_base}/ExecuteMDX"
            body = {"MDX": mdx}
            resp = session.post(execute_url, json=body, headers=headers, timeout=180)
            resp.raise_for_status()
            cellset_id = resp.json().get("ID") or resp.json().get("@odata.id")

            # Fetch the cellset content — flat cells with row/col tuple refs.
            cellset_url = f"{resource.api_base}/Cellsets('{cellset_id}')?$expand=Cells($expand=Members),Axes($expand=Tuples($expand=Members))"
            cr = session.get(cellset_url, headers=headers, timeout=180)
            cr.raise_for_status()
            cellset = cr.json() or {}

            # Flatten: one row per cell with its axis-member tuple identifiers.
            rows = []
            axes = cellset.get("Axes") or []
            for cell in cellset.get("Cells", []):
                row = {"value": cell.get("Value"), "ordinal": cell.get("Ordinal")}
                ordinal = cell.get("Ordinal", 0)
                for axis_idx, axis in enumerate(axes):
                    tuples = axis.get("Tuples") or []
                    if not tuples:
                        continue
                    dim_stride = 1
                    for prev_axis in axes[:axis_idx]:
                        dim_stride *= len(prev_axis.get("Tuples") or [])
                    tuple_idx = (ordinal // dim_stride) % len(tuples)
                    for member in tuples[tuple_idx].get("Members", []):
                        dim_name = member.get("Hierarchy", {}).get("Dimension", {}).get("Name")
                        member_name = member.get("Name")
                        if dim_name:
                            row[dim_name] = member_name
                rows.append(row)

            df = pd.DataFrame(rows)
            context.add_output_metadata({
                "row_count": len(df),
                "cube": _self.cube,
                "mdx_preview": dg.MetadataValue.md(f"```sql\n{mdx[:800]}\n```"),
            })
            return df

        return dg.Definitions(assets=[_the_asset])
