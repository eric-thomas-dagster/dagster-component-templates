"""TM1 Cube Data Ingestion Component.

Materialize a slice of a TM1 cube via MDX as a Dagster DataFrame asset.
Turn TM1's multidimensional cube data into flat rows for downstream Dagster
transforms, dbt models, or warehouse loads.
"""
from typing import Any, Dict, List, Optional, Union

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
    filters: Optional[dict] = Field(default=None, description="Simplified mode: {dimension: member} WHERE clause. On a partitioned run {partition_key}, {partition_date}, {partition_start} and {partition_end} (and {partition_<dimension>} for multi-partitions) are substituted.")

    group_name: Optional[str] = Field(default=None, description="Dagster asset group name.")

    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', 'dynamic', or None for unpartitioned. A partitioned run sends only the rows in its partition.",
    )

    partition_cron_schedule: Optional[str] = Field(
        default=None,
        description=(
            "Required when partition_type='cron'. A cron expression restricting the date axis to a "
            "custom cadence, e.g. '0 0 * * 1-5' for weekdays only (business-day cadence). Built on "
            "Dagster's TimeWindowPartitionsDefinition(cron_schedule=...)."
        ),
    )
    partition_cron_fmt: Optional[str] = Field(
        default="%Y-%m-%d",
        description="strftime format for partition keys produced by partition_type='cron'. Only change this for a sub-daily cron_schedule.",
    )
    partition_end_offset: int = Field(
        default=0,
        description=(
            "Passed through to the underlying Daily/Weekly/Monthly/Hourly/cron "
            "partitions definition. 0 (the Dagster default) means a time-based "
            "partition set only ever holds COMPLETED windows -- e.g. a daily set "
            "never includes today until tomorrow. Set to 1 to include the "
            "current, still-in-progress window."
        ),
    )
    partition_timezone: Optional[str] = Field(
        default=None,
        description="IANA timezone for the underlying Daily/Weekly/Monthly/Hourly/cron partitions definition (defaults to UTC when unset, matching Dagster's own default).",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.",
    )
    partition_values: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning, e.g. 'customer_a,customer_b'.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition (when partition_type='dynamic'), e.g. 'tenants'.",
    )
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Multi-axis partition spec: list of {name, type, start, values, dynamic_partition_name} dicts. Overrides the flat partition fields.",
    )
    resource_key: str = Field(default="tm1_resource", description="Resource key to look up.")

    include_preview_metadata: bool = Field(
        default=False,
        description="Include a markdown preview of the fetched rows in the materialization metadata.",
    )
    preview_rows: int = Field(
        default=25,
        description="Max rows to include in the preview when include_preview_metadata is True.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values, self.dynamic_partition_name, self.partition_dimensions
        ,
    cron_schedule=self.partition_cron_schedule,
    cron_fmt=self.partition_cron_fmt or "%Y-%m-%d", end_offset=self.partition_end_offset, timezone=self.partition_timezone,
)

        _self = self
        required_resource_keys = {self.resource_key}

        @dg.asset(partitions_def=partitions_def, 
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
                if _substitute_partition(_self.filters, _partition_vars(context)):
                    where = " WHERE (" + ", ".join(f"[{d}].[{m}]" for d, m in _substitute_partition(_self.filters, _partition_vars(context)).items()) + ")"
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
            _preview_metadata = {}
            if _self.include_preview_metadata:
                _prev_df = df.sample(min(_self.preview_rows, len(df))) if len(df) > _self.preview_rows * 10 else df.head(_self.preview_rows)
                _cols = list(_prev_df.columns)
                _preview_metadata["preview"] = dg.MetadataValue.md(
                    "| " + " | ".join(_cols) + " |\n"
                    "| " + " | ".join(["---"] * len(_cols)) + " |\n" +
                    "\n".join("| " + " | ".join(str(v) for v in row) + " |" for row in _prev_df.itertuples(index=False))
                )

            context.add_output_metadata({**_preview_metadata, 
                "row_count": len(df),
                "cube": _self.cube,
                "mdx_preview": dg.MetadataValue.md(f"```sql\n{mdx[:800]}\n```"),
            })
            return df

        return dg.Definitions(assets=[_the_asset])


# BEGIN scaffold partitions-def builder (inlined per component; keep copies identical, see tests/test_partition_port_scaffold.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from dagster import (  # noqa: E402,F401
    DailyPartitionsDefinition,
    DynamicPartitionsDefinition,
    HourlyPartitionsDefinition,
    MonthlyPartitionsDefinition,
    MultiPartitionsDefinition,
    StaticPartitionsDefinition,
    WeeklyPartitionsDefinition,
)


def _build_partitions_def(
    partition_type,
    partition_start,
    partition_values,
    dynamic_partition_name,
    partition_dimensions,
    cron_schedule=None,
    cron_fmt="%Y-%m-%d",
    end_offset=0,
    timezone=None,
):
    """Construct a Dagster partitions_def from the canonical partition fields.

    Strict: raises ValueError on misconfigured combinations rather than
    silently picking a default. Specifically:
      - time-based partition_type without partition_start
      - partition_type=multi without partition_values
      - partition_type=dynamic without dynamic_partition_name
      - both partition_dimensions AND flat fields set (ambiguous intent)
    """
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )

    if partition_dimensions and partition_type:
        raise ValueError(
            "Set either partition_type (flat-fields shape) or "
            "partition_dimensions (multi-axis shape), not both."
        )

    def _build_axis(spec):
        t = spec.get("type")
        _end_offset = spec.get("end_offset", 0)
        _timezone = spec.get("timezone")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily":
            return DailyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "weekly":
            return WeeklyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "monthly":
            return MonthlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "hourly":
            return HourlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "cron":
            from dagster import TimeWindowPartitionsDefinition
            if not spec.get("start"):
                raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
            if not spec.get("cron_schedule"):
                raise ValueError("partition dimension type='cron' requires 'cron_schedule'")
            return TimeWindowPartitionsDefinition(start=spec["start"], fmt=spec.get("fmt") or "%Y-%m-%d", cron_schedule=spec["cron_schedule"], end_offset=_end_offset, timezone=_timezone)
        if t == "static":
            vals = spec.get("values") or []
            if isinstance(vals, str):
                vals = [v.strip() for v in vals.split(",") if v.strip()]
            if not vals:
                raise ValueError("partition dimension type='static' requires non-empty 'values'")
            return StaticPartitionsDefinition(list(vals))
        if t == "dynamic":
            name = spec.get("dynamic_partition_name") or spec.get("name")
            if not name:
                raise ValueError("partition dimension type='dynamic' requires a name")
            return DynamicPartitionsDefinition(name=name)
        raise ValueError(f"unknown partition type: {t!r}")

    if partition_dimensions:
        if len(partition_dimensions) == 1:
            return _build_axis(partition_dimensions[0])
        axes = {d["name"]: _build_axis(d) for d in partition_dimensions}
        return MultiPartitionsDefinition(axes)

    if not partition_type:
        return None
    if isinstance(partition_values, (list, tuple)):
        _values = [str(v).strip() for v in partition_values if str(v).strip()]
    else:
        _values = [v.strip() for v in (str(partition_values) if partition_values else "").split(",") if v.strip()]
    if partition_type in ("daily", "weekly", "monthly", "hourly", "cron") and not partition_start:
        raise ValueError(
            f"partition_type={partition_type!r} requires partition_start (ISO date, e.g. '2024-01-01')."
        )
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "cron":
        from dagster import TimeWindowPartitionsDefinition
        if not cron_schedule:
            raise ValueError(
                "partition_type='cron' requires cron_schedule, e.g. '0 0 * * 1-5' for weekdays only."
            )
        return TimeWindowPartitionsDefinition(start=partition_start, fmt=cron_fmt, cron_schedule=cron_schedule, end_offset=end_offset, timezone=timezone)
    if partition_type == "static":
        if not _values:
            raise ValueError("partition_type='static' requires partition_values (comma-separated).")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError(
                "partition_type='dynamic' requires dynamic_partition_name."
            )
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    if partition_type == "multi":
        if not _values:
            raise ValueError("partition_type='multi' requires partition_values (comma-separated).")
        if not partition_start:
            raise ValueError("partition_type='multi' requires partition_start (the date axis start).")
        return MultiPartitionsDefinition({
            "date": DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone),
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")
# END scaffold partitions-def builder

# BEGIN partition-vars helper (inlined per component; keep copies identical, see tests/test_partition_vars.py)
def _partition_vars(context):
    """Tokens a partitioned run can use in its variables: {partition_key}, and for time partitions {partition_date}
    (YYYY-MM-DD), {partition_start} and {partition_end} (ISO datetimes); for a multi-partition also
    {partition_<dimension>}. Empty for an unpartitioned run."""
    import re as _re

    if not (context.has_partition_key or context.has_partition_key_range):
        return {}
    keys = list(context.partition_keys)
    tokens = {"partition_key": ",".join(str(k) for k in keys)}
    try:
        window = context.partition_time_window
        tokens["partition_start"] = window.start.strftime("%Y-%m-%dT%H:%M:%S")
        tokens["partition_end"] = window.end.strftime("%Y-%m-%dT%H:%M:%S")
        tokens["partition_date"] = window.start.strftime("%Y-%m-%d")
    except Exception:
        pass  # static / dynamic partitions have no time window
    if len(keys) == 1 and hasattr(keys[0], "keys_by_dimension"):
        for dimension, value in keys[0].keys_by_dimension.items():
            tokens["partition_" + _re.sub(r"\W", "_", str(dimension))] = str(value)
    return tokens


def _partition_env(tokens):
    """DAGSTER_PARTITION_* environment variables for a partitioned run (empty when unpartitioned)."""
    if not tokens:
        return {}
    env = {"DAGSTER_PARTITION_KEY": tokens["partition_key"]}
    for token, name in (("partition_date", "DAGSTER_PARTITION_DATE"), ("partition_start", "DAGSTER_PARTITION_START"),
                        ("partition_end", "DAGSTER_PARTITION_END")):
        if token in tokens:
            env[name] = tokens[token]
    return env


def _substitute_partition(value, tokens):
    """Replace {partition_*} tokens inside strings, recursively through dicts and lists. Only known tokens are
    replaced, so other braces (JSON, shell, templates) are left alone. A no-op for an unpartitioned run."""
    import re as _re

    if not tokens:
        return value
    if isinstance(value, str):
        return _re.sub(r"\{(partition_\w+)\}", lambda m: tokens.get(m.group(1), m.group(0)), value)
    if isinstance(value, dict):
        return {k: _substitute_partition(v, tokens) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return type(value)(_substitute_partition(v, tokens) for v in value)
    return value
# END partition-vars helper
