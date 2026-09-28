"""DataFrame Transformer Asset Component.

Transform DataFrames from upstream assets using IO managers for automatic data flow.
Works with visual dependency drawing - just connect DataFrame-producing assets!
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Union
import json
import string as _string_module

import numpy as np
import pandas as pd
from dagster import (
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    AssetExecutionContext,
    AssetKey,
    asset,
    Resolvable,
    Model,
    Output,
    MetadataValue,
)
from pydantic import Field, field_validator


# ─── Asset overrides (inline; kept per-component to preserve self-containment) ─
#
# Per-asset override applied after enumeration. Today supports `depends_on` —
# a list of upstream Dagster asset keys (strings; slash-delimited becomes a
# hierarchical AssetKey). Extend with more fields as needed (group, tags,
# description). Matches the pattern used by the official Databricks workspace
# component's `attributes.asset_overrides.<key>.depends_on`.


@dataclass
class AssetOverride(Resolvable):
    depends_on: Optional[List[str]] = None


def _resolve_override_deps(
    asset_overrides: Optional[Dict[str, "AssetOverride"]],
    lookup_key: str,
) -> List[AssetKey]:
    if not asset_overrides:
        return []
    ov = asset_overrides.get(lookup_key)
    if not ov or not ov.depends_on:
        return []
    return [AssetKey(d.split("/")) if "/" in d else AssetKey(d) for d in ov.depends_on]


def _build_partitions_def(
    partition_type,
    partition_start,
    partition_values,
    dynamic_partition_name,
    partition_dimensions,
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

    # Both shapes set: ambiguous. Pick one.
    if partition_dimensions and partition_type:
        raise ValueError(
            "Set either partition_type (flat-fields shape) or "
            "partition_dimensions (multi-axis shape), not both."
        )

    def _build_axis(spec):
        t = spec.get("type")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily":
            return DailyPartitionsDefinition(start_date=spec["start"])
        if t == "weekly":
            return WeeklyPartitionsDefinition(start_date=spec["start"])
        if t == "monthly":
            return MonthlyPartitionsDefinition(start_date=spec["start"])
        if t == "hourly":
            return HourlyPartitionsDefinition(start_date=spec["start"])
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
    if partition_type in ("daily", "weekly", "monthly", "hourly") and not partition_start:
        raise ValueError(
            f"partition_type={partition_type!r} requires partition_start (ISO date, e.g. '2024-01-01')."
        )
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start)
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
            "date": DailyPartitionsDefinition(start_date=partition_start),
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")


class DataFrameTransformerComponent(Component, Model, Resolvable):
    """Component for transforming DataFrames from upstream assets.

    This component works with visual dependency drawing in Dagster Designer.
    Simply draw a connection from any DataFrame-producing asset (REST API, Database Query,
    CSV Ingestion) to this transformer, and it will automatically receive the DataFrame.

    **Compatible upstream assets:**
    - REST API Fetcher (with output_format: dataframe)
    - Database Query
    - CSV File Ingestion
    - Other DataFrame Transformers

    Example:
        ```yaml
        # Just configure the transformation - dependencies set by drawing connections!
        type: dagster_component_templates.DataFrameTransformerComponent
        attributes:
          asset_name: cleaned_data
          drop_duplicates: true
          filter_columns: "id,name,amount,date"
          fill_na_value: "0"
        ```
    """

    asset_name: str = Field(
        description="Name of this asset"
    )

    # Column operations
    filter_columns: Optional[Union[str, int]] = Field(
        default=None,
        description="Comma-separated list of columns to keep"
    )

    drop_columns: Optional[Union[str, int]] = Field(
        default=None,
        description="Comma-separated list of columns to drop"
    )

    rename_columns: Optional[Union[str, int]] = Field(
        default=None,
        description="JSON mapping of column renames: '{\"old_name\": \"new_name\"}'"
    )

    # Row operations
    drop_duplicates: bool = Field(
        default=False,
        description="Whether to drop duplicate rows"
    )

    drop_na: bool = Field(
        default=False,
        description="Whether to drop rows with NA values"
    )

    fill_na_value: Optional[str] = Field(
        default=None,
        description="Value to fill NA values with"
    )

    # Filtering
    filter_expression: Optional[str] = Field(
        default=None,
        description="Pandas query expression (e.g., 'amount > 100 and status == \"active\"')"
    )

    # Sorting
    sort_by: Optional[str] = Field(
        default=None,
        description="Comma-separated columns to sort by"
    )

    sort_ascending: bool = Field(
        default=True,
        description="Sort direction"
    )

    # Aggregation
    group_by: Optional[str] = Field(
        default=None,
        description="Comma-separated columns to group by"
    )

    agg_functions: Optional[str] = Field(
        default=None,
        description="JSON mapping of aggregations: '{\"amount\": \"sum\", \"id\": \"count\"}'"
    )

    # Multiple DataFrame handling
    combine_method: str = Field(
        default="concat",
        description="How to combine multiple DataFrames: 'concat', 'merge', or 'first'"
    )

    description: Optional[str] = Field(
        default=None,
        description="Asset description"
    )

    group_name: Optional[str] = Field(
        default=None,
        description="Asset group for organization"
    )
    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', or None for unpartitioned",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.",
    )
    partition_date_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter upstream DataFrame to the current date partition key.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition (when partition_type='dynamic'), e.g. 'tenants'.",
    )

    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Multi-axis partition spec: list of {name, type, start, values, dynamic_partition_name} dicts. Overrides flat fields when set.",
    )

    partition_values: Optional[str] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning, e.g. 'customer_a,customer_b,customer_c'.",
    )
    partition_static_dim: Optional[str] = Field(
        default=None,
        description="Dimension name for the static axis in multi-partitioning, e.g. 'customer' or 'region'.",
    )
    partition_static_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter upstream DataFrame to the current static partition dimension (e.g. 'customer_id').",
    )
    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners — list of team names or email addresses, e.g. ['team:analytics', 'user@company.com']",
    )
    asset_tags: Optional[Dict[str, str]] = Field(
        default=None,
        description="Additional key-value tags to apply to the asset, e.g. {'domain': 'finance', 'tier': 'gold'}",
    )
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds for the Dagster catalog, e.g. ['snowflake', 'python']. Auto-inferred from component name if not set.",
    )
    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale. Defines a FreshnessPolicy.",
    )
    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5' (weekdays at 9am).",
    )
    column_lineage: Optional[Dict[str, List[str]]] = Field(
        default=None,
        description="Column-level lineage mapping: output column name → list of upstream column names it was derived from, e.g. {'revenue': ['price', 'quantity']}",
    )

    # String operations
    string_operations: Optional[str] = Field(
        default=None,
        description='JSON list of string operations: [{"column": "name", "operation": "upper"}, {"column": "email", "operation": "trim"}]. Operations: upper, lower, trim, strip, title'
    )

    string_replace: Optional[str] = Field(
        default=None,
        description='JSON mapping of string replacements: {"column_name": {"old": "new", "pattern": "replacement"}}'
    )

    # JSON list: [{"column", "find", "replace"}] -- REPLACE(col, find, replace),
    # chainable (multiple ops on the same column apply in order). Distinct
    # from string_replace's per-column dict-of-dicts shape -- this is the
    # ordered-list shape the SQL backend (SqlTransformerComponent) already
    # uses, kept parallel so the frontend can send either backend the same
    # payload shape for this op.
    replace_ops: Optional[str] = Field(
        default=None,
        description='JSON list of REPLACE ops: [{"column": "name", "find": "Mr.", "replace": ""}]. Multiple ops on the same column chain in order.',
    )
    # JSON list: [{"column", "delimiter", "into"}] -- into is a comma-separated
    # list of new column names, filled left-to-right from the split parts.
    split_ops: Optional[str] = Field(
        default=None,
        description='JSON list of split ops: [{"column": "full_name", "delimiter": " ", "into": "first_name,last_name"}].',
    )
    # JSON list: [{"kind", "orderBy", "partitionBy", "orderAsc", "into"}].
    # kind in {rank, dense_rank, row_number}.
    window_ops: Optional[str] = Field(
        default=None,
        description='JSON list of window ops: [{"kind": "rank", "orderBy": "amount", "partitionBy": "customer_id", "orderAsc": false, "into": "amount_rank"}]. kind: rank, dense_rank, row_number.',
    )
    # JSON list: [{"column", "operator", "value", "into", "partitionBy"}] --
    # count of rows matching the condition, optionally within a partition.
    count_match_ops: Optional[str] = Field(
        default=None,
        description='JSON list of count-matching ops: [{"column": "status", "operator": "equals", "value": "completed", "into": "completed_count", "partitionBy": "customer_id"}].',
    )
    # JSON list: [{"branches": [{"column","operator","value","then"}], "else", "into"}].
    case_when_ops: Optional[str] = Field(
        default=None,
        description='JSON list of case-when ops: [{"branches": [{"column": "amount", "operator": "greater_than", "value": "100", "then": "large"}], "else": "small", "into": "size_bucket"}].',
    )
    # JSON list: [{"columns" (csv), "separator", "into"}].
    concat_ops: Optional[str] = Field(
        default=None,
        description='JSON list of concat ops: [{"columns": "first_name,last_name", "separator": " ", "into": "full_name"}].',
    )
    # JSON list: [{"column", "part", "into"}]. part in {year, month, day, dayofweek, hour}.
    date_extract_ops: Optional[str] = Field(
        default=None,
        description='JSON list of date-extract ops: [{"column": "created_at", "part": "year", "into": "created_year"}]. part: year, month, day, dayofweek, hour.',
    )
    # JSON list: [{"column", "start" (1-based), "length"|null, "into"}].
    substring_ops: Optional[str] = Field(
        default=None,
        description='JSON list of substring ops: [{"column": "sku", "start": 1, "length": 3, "into": "sku_prefix"}]. start is 1-based; omit length to take the rest of the string.',
    )
    # JSON list: [{"column", "op": round|floor|ceil|abs, "digits", "into"}].
    numeric_ops: Optional[str] = Field(
        default=None,
        description='JSON list of numeric ops: [{"column": "price", "op": "round", "digits": 2, "into": "price_rounded"}]. op: round, floor, ceil, abs.',
    )
    # JSON: {"n" | "fraction", "random"}. n takes precedence over fraction.
    sample_config: Optional[str] = Field(
        default=None,
        description='JSON sample config: {"n": 1000, "random": true} or {"fraction": 0.1, "random": true}. n takes precedence when both are set.',
    )
    # JSON list: [{"column", "boundaries" (csv), "labels" (csv), "into"}].
    bin_ops: Optional[str] = Field(
        default=None,
        description='JSON list of binning ops: [{"column": "age", "boundaries": "18,35,50", "labels": "young,mid,senior,elder", "into": "age_bucket"}].',
    )
    # JSON: {"subsetCols" (csv), "keep": first|last}. An alternative to the
    # blanket drop_duplicates flag above -- dedupes on a specific column
    # subset instead of every column.
    dedupe_subset: Optional[str] = Field(
        default=None,
        description='JSON dedupe config: {"subsetCols": "customer_id,order_date", "keep": "first"}.',
    )
    # JSON list: [{"column", "partitionBy", "orderBy", "orderAsc", "into"}].
    cumsum_ops: Optional[str] = Field(
        default=None,
        description='JSON list of cumulative-sum ops: [{"column": "amount", "partitionBy": "customer_id", "orderBy": "order_date", "into": "running_total"}].',
    )
    # JSON list: [{"column", "direction": ffill|bfill, "partitionBy", "orderBy"}].
    fill_direction_ops: Optional[str] = Field(
        default=None,
        description='JSON list of fill-direction ops: [{"column": "price", "direction": "ffill", "partitionBy": "sku", "orderBy": "date"}]. direction: ffill (forward-fill) or bfill (backward-fill), applied in place.',
    )

    # Calculated columns
    calculated_columns: Optional[Union[str, int]] = Field(
        default=None,
        description='JSON mapping of calculated columns: {"new_col": "price * quantity", "full_name": "first_name + \' \' + last_name"}'
    )

    # Pivot/Unpivot operations
    pivot_config: Optional[str] = Field(
        default=None,
        description='JSON config for pivot: {"index": "date", "columns": "category", "values": "amount", "aggfunc": "sum"}'
    )

    unpivot_config: Optional[str] = Field(
        default=None,
        description='JSON config for unpivot/melt: {"id_vars": ["id", "name"], "value_vars": ["q1", "q2", "q3"], "var_name": "quarter", "value_name": "sales"}'
    )

    # Upstream asset wiring — per FIELD_CONVENTIONS: singular for one upstream,
    # plural list for multi-source. If both are set, singular is prepended.
    upstream_asset_key: Optional[str] = Field(
        default=None,
        description="Single upstream asset key. Convenience field for the common single-source case.",
    )
    upstream_asset_keys: Optional[List[str]] = Field(
        default=None,
        description="Multiple upstream asset keys for multi-source transforms.",
    )

    asset_overrides: Optional[Dict[str, AssetOverride]] = Field(
        default=None,
        description=(
            "Per-asset overrides keyed by the emitted asset's name (typically "
            "`asset_name`). Today supports `depends_on: [upstream_key, ...]` to add "
            "Dagster asset dependencies (merged with `upstream_asset_key(s)`). "
            "Matches the pattern used by the official Databricks workspace component."
        ),
    )

    # Sample metadata
    include_preview_metadata: bool = Field(
        default=False,
        description="Include a preview of the output data in metadata (first 5 rows as markdown table). Used by builder UIs to render asset shape without warehouse access."
    )

    preview_rows: int = Field(
        default=25,
        ge=1,
        le=500,
        description=(
            "Rows to include in the preview metadata when "
            "`include_preview_metadata` is True. For long DataFrames "
            "(>10x preview_rows), a random sample is used so the preview "
            "reflects the data distribution; otherwise head() is used."
        ),
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

    # Field validators to handle Dagster Components auto-deserializing JSON strings
    @field_validator('rename_columns', 'agg_functions', 'string_operations', 'string_replace',
                     'calculated_columns', 'pivot_config', 'unpivot_config',
                     'replace_ops', 'split_ops', 'window_ops', 'count_match_ops',
                     'case_when_ops', 'concat_ops', 'date_extract_ops', 'substring_ops',
                     'numeric_ops', 'sample_config', 'bin_ops', 'dedupe_subset',
                     'cumsum_ops', 'fill_direction_ops', mode='before')
    @classmethod
    def convert_dict_to_json_string(cls, v):
        """Convert dict to JSON string if needed.

        Dagster Components may auto-deserialize JSON strings in YAML to dicts,
        so we accept both and ensure they're converted to JSON strings.
        """
        if v is None:
            return None
        if isinstance(v, dict) or isinstance(v, list):
            return json.dumps(v)
        return v

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
        filter_columns = self.filter_columns
        drop_columns = self.drop_columns
        rename_columns_str = self.rename_columns
        drop_duplicates_flag = self.drop_duplicates
        drop_na_flag = self.drop_na
        fill_na_value = self.fill_na_value
        filter_expression = self.filter_expression
        sort_by = self.sort_by
        sort_ascending = self.sort_ascending
        group_by = self.group_by
        agg_functions_str = self.agg_functions
        combine_method = self.combine_method
        string_operations_str = self.string_operations
        string_replace_str = self.string_replace
        calculated_columns_str = self.calculated_columns
        pivot_config_str = self.pivot_config
        unpivot_config_str = self.unpivot_config
        replace_ops_str = self.replace_ops
        split_ops_str = self.split_ops
        window_ops_str = self.window_ops
        count_match_ops_str = self.count_match_ops
        case_when_ops_str = self.case_when_ops
        concat_ops_str = self.concat_ops
        date_extract_ops_str = self.date_extract_ops
        substring_ops_str = self.substring_ops
        numeric_ops_str = self.numeric_ops
        sample_config_str = self.sample_config
        bin_ops_str = self.bin_ops
        dedupe_subset_str = self.dedupe_subset
        cumsum_ops_str = self.cumsum_ops
        fill_direction_ops_str = self.fill_direction_ops
        # Bare `self.` attribute -- the closure below previously referenced
        # this as a bare local name with no assignment anywhere in scope,
        # which raises NameError the moment _effective_lineage's auto-infer
        # branch produces anything truthy (i.e. almost every real run with
        # at least one passthrough column) -- confirmed by reading the
        # closure body, not run live (no test harness for this component
        # exists in this checkout yet).
        upstream_asset_key = self.upstream_asset_key
        description = self.description or "Transform DataFrames from upstream assets"
        group_name = self.group_name
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows

        # Combine single + list upstreams. Singular field is for the common
        # one-source case; list is for multi-source. Both may be set.
        upstream_keys: List[str] = []
        if self.upstream_asset_key:
            upstream_keys.append(self.upstream_asset_key)
        if self.upstream_asset_keys:
            upstream_keys.extend(self.upstream_asset_keys)

        # asset_overrides.depends_on merges into the deps list alongside upstream_keys.
        override_deps = _resolve_override_deps(self.asset_overrides, asset_name)
        # upstream_keys are raw user-typed strings (e.g. "marts/fct_orders")
        # -- convert with the same "/" split _resolve_override_deps already
        # uses for asset_overrides.depends_on, so a bare string dep isn't
        # misread as one single-segment name containing a literal "/"
        # (which Dagster rejects: names must match ^[A-Za-z0-9_]+$). Only
        # `deps=` needs AssetKey objects; `upstream_keys` itself stays a
        # plain string list below for context.load_asset_value().
        upstream_dep_keys: List[AssetKey] = [
            AssetKey(k.split("/")) if "/" in k else AssetKey(k) for k in upstream_keys
        ]
        combined_deps: List[Any] = list(upstream_dep_keys) + list(override_deps)

        partitions_def = _build_partitions_def(
            self.partition_type,
            self.partition_start,
            self.partition_values,
            self.dynamic_partition_name,
            self.partition_dimensions,
        )
        partition_type = self.partition_type
        partition_date_column = self.partition_date_column
        partition_static_column = self.partition_static_column
        partition_static_dim = self.partition_static_dim

        # Infer kinds from component name if not explicitly set
        _comp_name = "dataframe_transformer"  # component directory name
        _kind_map = {
            "snowflake": "snowflake", "bigquery": "bigquery", "redshift": "redshift",
            "postgres": "postgres", "postgresql": "postgres", "mysql": "mysql",
            "s3": "s3", "adls": "azure", "azure": "azure", "gcs": "gcp",
            "google": "gcp", "databricks": "databricks", "dbt": "dbt",
            "kafka": "kafka", "mongodb": "mongodb", "redis": "redis",
            "neo4j": "neo4j", "elasticsearch": "elasticsearch", "pinecone": "pinecone",
            "chromadb": "chromadb", "pgvector": "postgres",
        }
        _inferred_kinds = self.kinds or []
        if not _inferred_kinds:
            _comp_lower = asset_name.lower()
            for keyword, kind in _kind_map.items():
                if keyword in _comp_lower:
                    _inferred_kinds.append(kind)
            if not _inferred_kinds:
                _inferred_kinds = ["python"]

        # Build combined tags: user tags + kind tags
        _all_tags = dict(self.asset_tags or {})
        for _kind in _inferred_kinds:
            _all_tags[f"dagster/kind/{_kind}"] = ""

        # Build freshness policy
        _freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy
            _freshness_policy = FreshnessPolicy(
                maximum_lag_minutes=self.freshness_max_lag_minutes,
                cron_schedule=self.freshness_cron,
            )

        owners = self.owners or []
        column_lineage = self.column_lineage if hasattr(self, 'column_lineage') else None


        @asset(
            key=AssetKey.from_user_string(asset_name),
            description=description,
            partitions_def=partitions_def,
                        owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
group_name=group_name,
            deps=combined_deps if combined_deps else None,
            retry_policy=_retry_policy,
        )
        def dataframe_transformer_asset(context: AssetExecutionContext, **kwargs) -> pd.DataFrame:
            """Asset that transforms DataFrames from upstream assets.

            Upstream DataFrames are automatically loaded by the IO manager
            and passed as keyword arguments.
            """

            # Load upstream assets based on configuration
            upstream_assets = {}

            # If upstream_asset_keys is configured, try to load assets explicitly
            if upstream_keys and hasattr(context, 'load_asset_value'):
                # Real execution context - load assets explicitly
                context.log.info(f"Loading {len(upstream_keys)} upstream asset(s) via context.load_asset_value()")
                for key in upstream_keys:
                    try:
                        # Convert string key to AssetKey object
                        asset_key = AssetKey(key)
                        value = context.load_asset_value(asset_key)
                        upstream_assets[key] = value
                        context.log.info(f"  - Loaded '{key}': {type(value).__name__}")
                    except Exception as e:
                        context.log.error(f"  - Failed to load '{key}': {e}")
                        raise
            else:
                # Preview/mock context or no upstream_keys - fall back to kwargs
                upstream_assets = {k: v for k, v in kwargs.items()}

            # Validate we have at least one upstream asset
            if not upstream_assets:
                raise ValueError(
                    f"DataFrame Transformer '{asset_name}' requires at least one upstream asset "
                    "that produces a DataFrame. Please connect a DataFrame-producing asset "
                    "in the visual editor, such as:\n"
                    "  - REST API Fetcher (with output_format: dataframe)\n"
                    "  - Database Query\n"
                    "  - CSV File Ingestion\n"
                    "  - Another DataFrame Transformer"
                )

            context.log.info(f"Received {len(upstream_assets)} upstream asset(s)")

            # Validate all inputs are DataFrames
            non_dataframes = []
            dataframes = {}

            for key, value in upstream_assets.items():
                if isinstance(value, pd.DataFrame):
                    dataframes[key] = value
                    context.log.info(f"  - '{key}': DataFrame with {len(value)} rows, {len(value.columns)} columns")
                else:
                    non_dataframes.append((key, type(value).__name__))

            if non_dataframes:
                error_msg = (
                    f"DataFrame Transformer '{asset_name}' received non-DataFrame inputs:\n"
                )
                for key, type_name in non_dataframes:
                    error_msg += f"  - '{key}': {type_name}\n"
                error_msg += "\nThis component only accepts DataFrame inputs. Compatible assets:\n"
                error_msg += "  - REST API Fetcher (set output_format: dataframe)\n"
                error_msg += "  - Database Query (returns DataFrames by default)\n"
                error_msg += "  - CSV File Ingestion (returns DataFrames by default)\n"
                error_msg += "  - Other DataFrame Transformers\n"
                raise TypeError(error_msg)

            # Filter each connected input to current partition if partitioned
            if context.has_partition_key:
                _pk = context.partition_key
                _is_multi = hasattr(_pk, "keys_by_dimension")
                _date_key = _pk.keys_by_dimension.get("date", "") if _is_multi else str(_pk)
                _static_key = _pk.keys_by_dimension.get(partition_static_dim or "segment", "") if _is_multi else None
                for _key, _frame in dataframes.items():
                    if partition_date_column and partition_date_column in _frame.columns and _date_key:
                        _frame = _frame[_frame[partition_date_column].astype(str) == _date_key]
                    if partition_static_column and partition_static_column in _frame.columns and _static_key:
                        _frame = _frame[_frame[partition_static_column].astype(str) == _static_key]
                    elif partition_static_column and partition_static_column in _frame.columns and not _is_multi:
                        _frame = _frame[_frame[partition_static_column].astype(str) == str(_pk)]
                    dataframes[_key] = _frame

            # Handle multiple DataFrames
            if len(dataframes) == 1:
                df = list(dataframes.values())[0]
                source_name = list(dataframes.keys())[0]
                context.log.info(f"Processing DataFrame from '{source_name}'")
            else:
                context.log.info(f"Combining {len(dataframes)} DataFrames using method: {combine_method}")

                if combine_method == "first":
                    # Just use the first DataFrame
                    df = list(dataframes.values())[0]
                    source_name = list(dataframes.keys())[0]
                    context.log.info(f"Using first DataFrame from '{source_name}'")

                elif combine_method == "concat":
                    # Concatenate all DataFrames vertically
                    df = pd.concat(dataframes.values(), ignore_index=True)
                    context.log.info(f"Concatenated into {len(df)} rows")

                elif combine_method == "merge":
                    # Merge DataFrames (assumes they have common columns)
                    df_list = list(dataframes.values())
                    df = df_list[0]
                    for next_df in df_list[1:]:
                        df = pd.merge(df, next_df, how='outer')
                    context.log.info(f"Merged into {len(df)} rows")

                else:
                    raise ValueError(f"Unknown combine_method: {combine_method}")

            original_rows = len(df)
            original_cols = len(df.columns)

            # Column dropping
            if drop_columns:
                cols = [c.strip() for c in drop_columns.split(',')]
                cols_to_drop = [c for c in cols if c in df.columns]
                df = df.drop(columns=cols_to_drop)
                context.log.info(f"Dropped {len(cols_to_drop)} columns")

            # Column renaming
            if rename_columns_str:
                try:
                    rename_map = json.loads(rename_columns_str)
                    df = df.rename(columns=rename_map)
                    context.log.info(f"Renamed {len(rename_map)} columns")
                except json.JSONDecodeError as e:
                    context.log.error(f"Invalid rename_columns JSON: {e}")

            # Drop duplicates
            if drop_duplicates_flag:
                before = len(df)
                df = df.drop_duplicates()
                context.log.info(f"Dropped {before - len(df)} duplicate rows")

            # Drop NA
            if drop_na_flag:
                before = len(df)
                df = df.dropna()
                context.log.info(f"Dropped {before - len(df)} rows with NA values")

            # Fill NA
            if fill_na_value is not None:
                df = df.fillna(fill_na_value)
                context.log.info(f"Filled NA values with: {fill_na_value}")

            # Filter expression
            if filter_expression:
                try:
                    before = len(df)
                    df = df.query(filter_expression)
                    context.log.info(f"Filter '{filter_expression}' kept {len(df)}/{before} rows")
                except Exception as e:
                    context.log.error(f"Filter expression failed: {e}")
                    raise

            # Sorting
            if sort_by:
                cols = [c.strip() for c in sort_by.split(',')]
                existing_cols = [c for c in cols if c in df.columns]
                if existing_cols:
                    df = df.sort_values(by=existing_cols, ascending=sort_ascending)
                    context.log.info(f"Sorted by: {existing_cols} ({'ascending' if sort_ascending else 'descending'})")

            # Aggregation
            if group_by and agg_functions_str:
                try:
                    group_cols = [c.strip() for c in group_by.split(',')]
                    agg_map = json.loads(agg_functions_str)

                    # Filter to existing columns
                    group_cols = [c for c in group_cols if c in df.columns]
                    agg_map = {k: v for k, v in agg_map.items() if k in df.columns}

                    if group_cols and agg_map:
                        before = len(df)
                        df = df.groupby(group_cols).agg(agg_map).reset_index()
                        context.log.info(f"Grouped by {group_cols}, aggregated {len(agg_map)} columns ({before} → {len(df)} rows)")
                except Exception as e:
                    context.log.error(f"Aggregation failed: {e}")
                    raise

            # String operations. `column` of "*" or empty/missing -- applies
            # to every object(string)-dtype column instead of requiring each
            # one named individually, matching data_cleansing's own
            # auto-detect behavior (df.select_dtypes(include="object")).
            if string_operations_str:
                try:
                    operations = json.loads(string_operations_str)
                    for op in operations:
                        col = op.get('column')
                        operation = op.get('operation')
                        target_cols = (
                            list(df.select_dtypes(include=['object', 'string']).columns)
                            if not col or col == '*'
                            else [col] if col in df.columns else []
                        )
                        for c in target_cols:
                            if operation == 'upper':
                                df[c] = df[c].astype(str).str.upper()
                            elif operation == 'lower':
                                df[c] = df[c].astype(str).str.lower()
                            elif operation in ['trim', 'strip']:
                                df[c] = df[c].astype(str).str.strip()
                            elif operation == 'title':
                                df[c] = df[c].astype(str).str.title()
                            elif operation == 'remove_punctuation':
                                _punct_table = str.maketrans('', '', _string_module.punctuation)
                                df[c] = df[c].astype(str).str.translate(_punct_table)
                        if target_cols:
                            context.log.info(f"Applied '{operation}' to column(s) {target_cols}")
                except Exception as e:
                    context.log.error(f"String operations failed: {e}")
                    raise

            # String replace
            if string_replace_str:
                try:
                    replace_map = json.loads(string_replace_str)
                    for col, replacements in replace_map.items():
                        if col in df.columns:
                            for old_val, new_val in replacements.items():
                                df[col] = df[col].astype(str).str.replace(old_val, new_val, regex=False)
                                context.log.info(f"Replaced '{old_val}' with '{new_val}' in column '{col}'")
                except Exception as e:
                    context.log.error(f"String replace failed: {e}")
                    raise

            # Replace ops -- REPLACE(col, find, replace), chainable (same
            # ordered-list shape as SqlTransformerComponent's replace_ops,
            # distinct from string_replace's per-column dict-of-dicts above).
            if replace_ops_str:
                try:
                    replace_ops = json.loads(replace_ops_str)
                    by_col: Dict[str, List[dict]] = {}
                    for op in replace_ops:
                        col = op.get('column')
                        if col and op.get('find') is not None:
                            by_col.setdefault(col, []).append(op)
                    for col, ops in by_col.items():
                        if col in df.columns:
                            for op in ops:
                                df[col] = df[col].astype(str).str.replace(
                                    str(op.get('find', '')), str(op.get('replace', '')), regex=False,
                                )
                            context.log.info(f"Applied {len(ops)} replace op(s) to column '{col}'")
                except Exception as e:
                    context.log.error(f"replace_ops failed: {e}")
                    raise

            # Split ops -- splits a column on a delimiter into `into`'s
            # comma-separated target column names, left-to-right.
            if split_ops_str:
                try:
                    split_ops = json.loads(split_ops_str)
                    for op in split_ops:
                        col = op.get('column')
                        delim = op.get('delimiter')
                        into = op.get('into')
                        if col in df.columns and delim and into:
                            targets = [t.strip() for t in str(into).split(',') if t.strip()]
                            parts = df[col].astype(str).str.split(delim, expand=True)
                            for idx, t in enumerate(targets):
                                df[t] = parts[idx] if idx in parts.columns else None
                            context.log.info(f"Split '{col}' by '{delim}' into {targets}")
                except Exception as e:
                    context.log.error(f"split_ops failed: {e}")
                    raise

            # Numeric ops -- round / floor / ceil / abs into a new column.
            if numeric_ops_str:
                try:
                    numeric_ops = json.loads(numeric_ops_str)
                    for op in numeric_ops:
                        col = op.get('column')
                        into = op.get('into')
                        kind = str(op.get('op', 'round')).lower()
                        digits = int(op.get('digits', 0))
                        if col in df.columns and into:
                            numeric_col = pd.to_numeric(df[col], errors='coerce')
                            if kind == 'floor':
                                df[into] = np.floor(numeric_col)
                            elif kind == 'ceil':
                                df[into] = np.ceil(numeric_col)
                            elif kind == 'abs':
                                df[into] = numeric_col.abs()
                            else:
                                df[into] = numeric_col.round(digits)
                    context.log.info(f"Applied {len(numeric_ops)} numeric op(s)")
                except Exception as e:
                    context.log.error(f"numeric_ops failed: {e}")
                    raise

            # Date extract ops -- EXTRACT(part FROM col) equivalent.
            if date_extract_ops_str:
                try:
                    date_extract_ops = json.loads(date_extract_ops_str)
                    for op in date_extract_ops:
                        col = op.get('column')
                        part = str(op.get('part', 'year')).lower()
                        into = op.get('into')
                        if col in df.columns and into:
                            dt = pd.to_datetime(df[col], errors='coerce')
                            if part == 'month':
                                df[into] = dt.dt.month
                            elif part == 'day':
                                df[into] = dt.dt.day
                            elif part == 'dayofweek':
                                df[into] = dt.dt.dayofweek
                            elif part == 'hour':
                                df[into] = dt.dt.hour
                            else:
                                df[into] = dt.dt.year
                    context.log.info(f"Applied {len(date_extract_ops)} date-extract op(s)")
                except Exception as e:
                    context.log.error(f"date_extract_ops failed: {e}")
                    raise

            # Substring ops -- 1-based start (matches SQL SUBSTRING semantics),
            # omit length to take the rest of the string.
            if substring_ops_str:
                try:
                    substring_ops = json.loads(substring_ops_str)
                    for op in substring_ops:
                        col = op.get('column')
                        into = op.get('into')
                        start = int(op.get('start', 1))
                        length = op.get('length')
                        if col in df.columns and into:
                            s = df[col].astype(str)
                            start0 = max(start - 1, 0)
                            if length is None or length == '':
                                df[into] = s.str.slice(start0)
                            else:
                                df[into] = s.str.slice(start0, start0 + int(length))
                    context.log.info(f"Applied {len(substring_ops)} substring op(s)")
                except Exception as e:
                    context.log.error(f"substring_ops failed: {e}")
                    raise

            # Concat ops -- col1 || sep || col2 || sep || col3 ..., row-wise.
            if concat_ops_str:
                try:
                    concat_ops = json.loads(concat_ops_str)
                    for op in concat_ops:
                        cols = [c.strip() for c in str(op.get('columns', '')).split(',') if c.strip()]
                        cols = [c for c in cols if c in df.columns]
                        sep = str(op.get('separator', ''))
                        into = op.get('into')
                        if cols and into:
                            df[into] = df[cols].astype(str).apply(lambda row: sep.join(row), axis=1)
                    context.log.info(f"Applied {len(concat_ops)} concat op(s)")
                except Exception as e:
                    context.log.error(f"concat_ops failed: {e}")
                    raise

            # Case-when ops -- CASE WHEN cond1 THEN t1 WHEN cond2 THEN t2 ELSE e END.
            if case_when_ops_str:
                try:
                    case_when_ops = json.loads(case_when_ops_str)
                    for op in case_when_ops:
                        into = op.get('into')
                        branches = op.get('branches') or []
                        else_val = op.get('else')
                        if not into or not branches:
                            continue
                        conditions = []
                        choices = []
                        for b in branches:
                            col = b.get('column')
                            operator = str(b.get('operator', 'equals'))
                            val = b.get('value')
                            then = b.get('then')
                            if col not in df.columns or val is None:
                                continue
                            if operator == 'not_equals':
                                cond = df[col].astype(str) != str(val)
                            elif operator == 'greater_than':
                                cond = pd.to_numeric(df[col], errors='coerce') > float(val)
                            elif operator == 'less_than':
                                cond = pd.to_numeric(df[col], errors='coerce') < float(val)
                            elif operator == 'contains':
                                cond = df[col].astype(str).str.contains(str(val), na=False)
                            else:
                                cond = df[col].astype(str) == str(val)
                            conditions.append(cond)
                            choices.append(then)
                        if conditions:
                            df[into] = np.select(conditions, choices, default=else_val)
                    context.log.info(f"Applied {len(case_when_ops)} case-when op(s)")
                except Exception as e:
                    context.log.error(f"case_when_ops failed: {e}")
                    raise

            # Bin/bucket ops -- CASE WHEN col <= b1 THEN L0 WHEN col <= b2 THEN L1 ... END.
            if bin_ops_str:
                try:
                    bin_ops = json.loads(bin_ops_str)
                    for op in bin_ops:
                        col = op.get('column')
                        into = op.get('into')
                        bounds_str = op.get('boundaries', '')
                        labels_str = op.get('labels', '')
                        if col not in df.columns or not into or not bounds_str:
                            continue
                        bounds = [float(x.strip()) for x in bounds_str.split(',') if x.strip()]
                        labels = [x.strip() for x in labels_str.split(',') if x.strip()] or None
                        edges = [-np.inf] + bounds + [np.inf]
                        if labels and len(labels) != len(edges) - 1:
                            labels = None
                        df[into] = pd.cut(pd.to_numeric(df[col], errors='coerce'), bins=edges, labels=labels)
                    context.log.info(f"Applied {len(bin_ops)} binning op(s)")
                except Exception as e:
                    context.log.error(f"bin_ops failed: {e}")
                    raise

            # Dedupe on a specific column subset -- alternative to the
            # blanket drop_duplicates flag above (dedupes on ALL columns).
            if dedupe_subset_str:
                try:
                    dedupe_cfg = json.loads(dedupe_subset_str)
                    subset_cols = [c.strip() for c in str(dedupe_cfg.get('subsetCols', '')).split(',') if c.strip()]
                    subset_cols = [c for c in subset_cols if c in df.columns]
                    keep = dedupe_cfg.get('keep', 'first')
                    keep = keep if keep in ('first', 'last') else 'first'
                    if subset_cols:
                        before = len(df)
                        df = df.drop_duplicates(subset=subset_cols, keep=keep)
                        context.log.info(f"Deduped on {subset_cols} (keep={keep}): {before} → {len(df)} rows")
                except Exception as e:
                    context.log.error(f"dedupe_subset failed: {e}")
                    raise

            # Cumulative sum -- SUM(col) OVER (PARTITION BY ... ORDER BY ...).
            if cumsum_ops_str:
                try:
                    cumsum_ops = json.loads(cumsum_ops_str)
                    for op in cumsum_ops:
                        col = op.get('column')
                        into = op.get('into')
                        order_by = op.get('orderBy') or op.get('order_by')
                        partition_by = op.get('partitionBy') or op.get('partition_by') or ''
                        order_asc = bool(op.get('orderAsc', op.get('order_asc', True)))
                        if col not in df.columns or not into or not order_by or order_by not in df.columns:
                            continue
                        sorted_df = df.sort_values(by=order_by, ascending=order_asc)
                        parts = [p.strip() for p in partition_by.split(',') if p.strip() and p.strip() in df.columns]
                        cum = sorted_df.groupby(parts)[col].cumsum() if parts else sorted_df[col].cumsum()
                        df.loc[sorted_df.index, into] = cum
                    context.log.info(f"Applied {len(cumsum_ops)} cumulative-sum op(s)")
                except Exception as e:
                    context.log.error(f"cumsum_ops failed: {e}")
                    raise

            # Fill-direction ops -- forward/backward fill within a partition,
            # in row order, applied in place (same column name).
            if fill_direction_ops_str:
                try:
                    fill_direction_ops = json.loads(fill_direction_ops_str)
                    for op in fill_direction_ops:
                        col = op.get('column')
                        direction = str(op.get('direction', 'ffill')).lower()
                        order_by = op.get('orderBy') or op.get('order_by')
                        partition_by = op.get('partitionBy') or op.get('partition_by') or ''
                        if col not in df.columns:
                            continue
                        backward = direction in ('bfill', 'backward')
                        work = df.sort_values(by=order_by) if order_by and order_by in df.columns else df
                        parts = [p.strip() for p in partition_by.split(',') if p.strip() and p.strip() in df.columns]
                        # .fillna(method=...) was removed in pandas 3.x -- use
                        # the direct .ffill()/.bfill() accessors instead,
                        # which also work directly on a SeriesGroupBy (no
                        # equivalent groupby(...).fillna(method=...) exists).
                        if parts:
                            grouped = work.groupby(parts)[col]
                            filled = grouped.bfill() if backward else grouped.ffill()
                        else:
                            filled = work[col].bfill() if backward else work[col].ffill()
                        df.loc[work.index, col] = filled
                    context.log.info(f"Applied {len(fill_direction_ops)} fill-direction op(s)")
                except Exception as e:
                    context.log.error(f"fill_direction_ops failed: {e}")
                    raise

            # Window ops -- RANK/DENSE_RANK/ROW_NUMBER OVER (PARTITION BY... ORDER BY...).
            if window_ops_str:
                try:
                    window_ops = json.loads(window_ops_str)
                    for op in window_ops:
                        kind = str(op.get('kind', 'rank')).lower()
                        order_by = op.get('orderBy') or op.get('order_by')
                        into = op.get('into')
                        partition_by = op.get('partitionBy') or op.get('partition_by') or ''
                        order_asc = bool(op.get('orderAsc', op.get('order_asc', True)))
                        if not order_by or not into or order_by not in df.columns:
                            continue
                        parts = [p.strip() for p in partition_by.split(',') if p.strip() and p.strip() in df.columns]
                        sorted_df = df.sort_values(by=order_by, ascending=order_asc)
                        if kind == 'row_number':
                            result = (sorted_df.groupby(parts).cumcount() + 1) if parts else pd.Series(
                                range(1, len(sorted_df) + 1), index=sorted_df.index,
                            )
                        else:
                            method = 'dense' if kind == 'dense_rank' else 'min'
                            result = (
                                sorted_df.groupby(parts)[order_by].rank(method=method, ascending=order_asc)
                                if parts else sorted_df[order_by].rank(method=method, ascending=order_asc)
                            )
                        df.loc[sorted_df.index, into] = result
                    context.log.info(f"Applied {len(window_ops)} window op(s)")
                except Exception as e:
                    context.log.error(f"window_ops failed: {e}")
                    raise

            # Count-matching ops -- COUNT(CASE WHEN cond THEN 1 END) OVER (PARTITION BY ...).
            if count_match_ops_str:
                try:
                    count_match_ops = json.loads(count_match_ops_str)
                    for op in count_match_ops:
                        col = op.get('column')
                        operator = str(op.get('operator', 'equals'))
                        val = op.get('value')
                        into = op.get('into')
                        if col not in df.columns or not into or val is None or str(val).strip() == '':
                            continue
                        if operator == 'not_equals':
                            cond = df[col].astype(str) != str(val)
                        elif operator == 'greater_than':
                            cond = pd.to_numeric(df[col], errors='coerce') > float(val)
                        elif operator == 'less_than':
                            cond = pd.to_numeric(df[col], errors='coerce') < float(val)
                        elif operator == 'contains':
                            cond = df[col].astype(str).str.contains(str(val), na=False)
                        else:
                            cond = df[col].astype(str) == str(val)
                        partition_by = op.get('partitionBy') or op.get('partition_by') or ''
                        parts = [p.strip() for p in partition_by.split(',') if p.strip() and p.strip() in df.columns]
                        if parts:
                            df[into] = cond.groupby([df[p] for p in parts]).transform('sum')
                        else:
                            df[into] = int(cond.sum())
                    context.log.info(f"Applied {len(count_match_ops)} count-matching op(s)")
                except Exception as e:
                    context.log.error(f"count_match_ops failed: {e}")
                    raise

            # Sample -- applied after all other row-shaping ops, before the
            # final column selection (mirrors SqlTransformerComponent's own
            # "sample before LIMIT" ordering).
            if sample_config_str:
                try:
                    sample_cfg = json.loads(sample_config_str)
                    n = sample_cfg.get('n')
                    fraction = sample_cfg.get('fraction')
                    random_flag = bool(sample_cfg.get('random', True))
                    before = len(df)
                    if n:
                        n = min(int(n), len(df))
                        df = df.sample(n=n) if random_flag else df.head(n)
                    elif fraction:
                        df = df.sample(frac=float(fraction)) if random_flag else df.head(int(len(df) * float(fraction)))
                    context.log.info(f"Sampled {before} → {len(df)} rows")
                except Exception as e:
                    context.log.error(f"sample_config failed: {e}")
                    raise

            # Calculated columns
            if calculated_columns_str:
                try:
                    calc_cols = json.loads(calculated_columns_str)
                    for new_col, expression in calc_cols.items():
                        # Evaluate expression in context of DataFrame
                        df[new_col] = df.eval(expression)
                        context.log.info(f"Created calculated column '{new_col}' = '{expression}'")
                except Exception as e:
                    context.log.error(f"Calculated columns failed: {e}")
                    raise

            # Pivot
            if pivot_config_str:
                try:
                    pivot_cfg = json.loads(pivot_config_str)
                    df = df.pivot_table(
                        index=pivot_cfg.get('index'),
                        columns=pivot_cfg.get('columns'),
                        values=pivot_cfg.get('values'),
                        aggfunc=pivot_cfg.get('aggfunc', 'sum')
                    ).reset_index()
                    context.log.info(f"Pivoted DataFrame: {pivot_cfg}")
                except Exception as e:
                    context.log.error(f"Pivot failed: {e}")
                    raise

            # Unpivot (melt)
            if unpivot_config_str:
                try:
                    unpivot_cfg = json.loads(unpivot_config_str)
                    df = pd.melt(
                        df,
                        id_vars=unpivot_cfg.get('id_vars', []),
                        value_vars=unpivot_cfg.get('value_vars'),
                        var_name=unpivot_cfg.get('var_name', 'variable'),
                        value_name=unpivot_cfg.get('value_name', 'value')
                    )
                    context.log.info(f"Unpivoted DataFrame: {unpivot_cfg}")
                except Exception as e:
                    context.log.error(f"Unpivot failed: {e}")
                    raise

            # Column filtering (select final output columns) - applied LAST
            if filter_columns:
                cols = [c.strip() for c in filter_columns.split(',')]

                # Expand filter list to include renamed columns and calculated columns
                # Build the final list of columns to keep
                final_cols_to_keep = []

                # Add renamed versions of filter columns
                if rename_columns_str:
                    try:
                        rename_map = json.loads(rename_columns_str)
                        for col in cols:
                            # If this column was renamed, use the new name
                            if col in rename_map:
                                final_cols_to_keep.append(rename_map[col])
                            else:
                                final_cols_to_keep.append(col)
                    except json.JSONDecodeError:
                        final_cols_to_keep = cols
                else:
                    final_cols_to_keep = cols

                # Add calculated columns (they should always be kept)
                if calculated_columns_str:
                    try:
                        calc_cols = json.loads(calculated_columns_str)
                        for new_col in calc_cols.keys():
                            if new_col not in final_cols_to_keep:
                                final_cols_to_keep.append(new_col)
                    except json.JSONDecodeError:
                        pass

                missing = set(final_cols_to_keep) - set(df.columns)
                if missing:
                    context.log.warning(f"Columns not found: {missing}")
                existing = [c for c in final_cols_to_keep if c in df.columns]
                df = df[existing]
                context.log.info(f"Selected {len(existing)} output columns: {existing}")

            # Add metadata

            # Build column schema metadata
            from dagster import TableSchema, TableColumn, TableColumnLineage, TableColumnDep
            _col_schema = TableSchema(columns=[
                TableColumn(name=str(col), type=str(df.dtypes[col]))
                for col in df.columns
            ])
            _metadata = {
                "dagster/row_count": MetadataValue.int(len(df)),
                "dagster/column_schema": MetadataValue.table_schema(_col_schema),
            }
            # Use explicit lineage, or auto-infer passthrough columns at runtime
            _effective_lineage = column_lineage
            if not _effective_lineage:
                try:
                    _upstream_cols = set()
                    for _df in dataframes.values():
                        _upstream_cols |= set(_df.columns)
                    _effective_lineage = {
                        col.name: [col.name] for col in _col_schema.columns
                        if col.name in _upstream_cols
                    }
                except Exception:
                    pass
            if _effective_lineage:
                _upstream_key = AssetKey.from_user_string(upstream_asset_key) if upstream_asset_key else None
                if _upstream_key:
                    _lineage_deps = {}
                    for out_col, in_cols in _effective_lineage.items():
                        _lineage_deps[str(out_col)] = [
                            TableColumnDep(asset_key=_upstream_key, column_name=str(ic))
                            for ic in in_cols
                        ]
                    _metadata["dagster/column_lineage"] = MetadataValue.column_lineage(
                        TableColumnLineage(_lineage_deps)
                    )
            context.add_output_metadata(_metadata)

            context.log.info(
                f"Transformation complete: {original_rows} → {len(df)} rows, "
                f"{original_cols} → {len(df.columns)} columns"
            )

            # Return DataFrame - IO manager will handle persistence
            if include_preview and len(df) > 0:
                # Return with sample metadata
                context.add_output_metadata({
                        "row_count": len(df),
                        "columns": df.columns.tolist(),
                        "preview": MetadataValue.md(df.head().to_markdown())
                    })
                return df
            else:
                return df

        from dagster import build_column_schema_change_checks


        _schema_checks = build_column_schema_change_checks(assets=[dataframe_transformer_asset])


        return Definitions(assets=[dataframe_transformer_asset], asset_checks=list(_schema_checks))
