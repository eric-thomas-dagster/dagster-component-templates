"""LLM Output Parser Asset Component.

Parse and validate LLM outputs in a DataFrame column into structured formats.
"""

import json
from typing import Dict, List, Optional
import re
import pandas as pd

from dagster import (
    AssetIn,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    AssetExecutionContext,
    asset,
    Resolvable,
    Model,
)
from pydantic import Field


class LLMOutputParserComponent(Component, Model, Resolvable):
    """Component for parsing LLM output column in an upstream DataFrame.

    Accepts a DataFrame via ins= and parses the values in input_column using
    the selected parser_type, adding parsed results as new column(s).

    Example:
        ```yaml
        type: dagster_component_templates.LLMOutputParserComponent
        attributes:
          asset_name: parsed_output
          upstream_asset_key: llm_enriched_data
          input_column: llm_response
          parser_type: json
          output_column: parsed_json
          validation_schema: '{"type": "object", "properties": {"name": {"type": "string"}}}'
        ```
    """

    asset_name: str = Field(description="Name of the asset")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame with LLM output to parse")
    input_column: str = Field(default="llm_response", description="Column name containing LLM output text to parse")
    output_column: str = Field(default="parsed_output", description="Column name for parsed results")
    parser_type: str = Field(description="Parser: 'json', 'csv', 'list', 'key_value', 'markdown_table', 'custom'")
    validation_schema: Optional[str] = Field(default=None, description="JSON schema for validation")
    custom_regex: Optional[str] = Field(default=None, description="Custom regex pattern")
    extract_code_blocks: bool = Field(default=False, description="Extract code blocks from markdown")
    strip_markdown: bool = Field(default=True, description="Strip markdown formatting")
    strict_validation: bool = Field(default=False, description="Raise error on validation failure")
    description: Optional[str] = Field(default=None, description="Asset description")
    group_name: Optional[str] = Field(default=None, description="Asset group")
    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', or None for unpartitioned",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.",
    )
    partition_date_column: Optional[str] = Field(
        default=None,
        description="Column used to filter upstream DataFrame to the current date partition key.",
    )
    partition_values: Optional[str] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning, e.g. 'customer_a,customer_b,customer_c'.",
    )
    partition_static_dim: Optional[str] = Field(
        default=None,
        description="Dimension name for the static axis in multi-partitioning, e.g. 'customer' or 'region'.",
    )
    partition_static_column: Optional[str] = Field(
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

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        input_column = self.input_column
        output_column = self.output_column
        parser_type = self.parser_type
        validation_schema_str = self.validation_schema
        custom_regex = self.custom_regex
        extract_code_blocks = self.extract_code_blocks
        strip_markdown = self.strip_markdown
        strict_validation = self.strict_validation
        description = self.description or "Parse LLM output column"
        group_name = self.group_name

        # Build partition definition
        partitions_def = None
        if self.partition_type:
            from dagster import (
                DailyPartitionsDefinition, WeeklyPartitionsDefinition,
                MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
                StaticPartitionsDefinition, MultiPartitionsDefinition,
            )
            _start = self.partition_start or "2020-01-01"
            _values = [v.strip() for v in (self.partition_values or "").split(",") if v.strip()]
            if self.partition_type == "daily":
                partitions_def = DailyPartitionsDefinition(start_date=_start)
            elif self.partition_type == "weekly":
                partitions_def = WeeklyPartitionsDefinition(start_date=_start)
            elif self.partition_type == "monthly":
                partitions_def = MonthlyPartitionsDefinition(start_date=_start)
            elif self.partition_type == "hourly":
                partitions_def = HourlyPartitionsDefinition(start_date=_start)
            elif self.partition_type == "static":
                partitions_def = StaticPartitionsDefinition(_values)
            elif self.partition_type == "multi":
                _dim = self.partition_static_dim or "segment"
                partitions_def = MultiPartitionsDefinition({
                    "date": DailyPartitionsDefinition(start_date=_start),
                    _dim: StaticPartitionsDefinition(_values),
                })
        partition_type = self.partition_type
        partition_date_column = self.partition_date_column
        partition_static_column = self.partition_static_column
        partition_static_dim = self.partition_static_dim

        # Infer kinds from component name if not explicitly set
        _comp_name = "llm_output_parser"  # component directory name
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
            name=asset_name,
            description=description,
            partitions_def=partitions_def,
                        owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
group_name=group_name,
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
        )
        def llm_output_parser_asset(ctx: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
            # Filter to current partition if partitioned
            if context.has_partition_key:
                _pk = context.partition_key
                _is_multi = hasattr(_pk, "keys_by_dimension")
                _date_key = _pk.keys_by_dimension.get("date", "") if _is_multi else str(_pk)
                _static_key = _pk.keys_by_dimension.get(partition_static_dim or "segment", "") if _is_multi else None
                if partition_date_column and partition_date_column in upstream.columns and _date_key:
                    upstream = upstream[upstream[partition_date_column].astype(str) == _date_key]
                if partition_static_column and partition_static_column in upstream.columns and _static_key:
                    upstream = upstream[upstream[partition_static_column].astype(str) == _static_key]
                elif partition_static_column and partition_static_column in upstream.columns and not _is_multi:
                    upstream = upstream[upstream[partition_static_column].astype(str) == str(_pk)]
            """Parse LLM output column in an upstream DataFrame."""

            df = upstream.copy()

            if input_column not in df.columns:
                raise ValueError(f"Input column '{input_column}' not found. Available: {list(df.columns)}")

            ctx.log.info(f"Parsing {len(df)} rows with {parser_type} parser")

            def parse_single(raw_output: str) -> str:
                """Parse a single LLM output string."""
                raw = str(raw_output)

                # Extract code blocks if requested
                if extract_code_blocks:
                    code_blocks = re.findall(r'```(?:\w+)?\n(.*?)\n```', raw, re.DOTALL)
                    if code_blocks:
                        raw = code_blocks[0]  # Use first code block

                # Strip markdown
                if strip_markdown:
                    raw = re.sub(r'[*_`#]', '', raw)

                # Parse based on type
                parsed_result = None

                if parser_type == "json":
                    json_match = re.search(r'(\{.*\}|\[.*\])', raw, re.DOTALL)
                    if json_match:
                        try:
                            parsed_result = json.dumps(json.loads(json_match.group(1)))
                        except json.JSONDecodeError as e:
                            if strict_validation:
                                raise
                            parsed_result = json.dumps({"raw": raw, "error": str(e)})

                elif parser_type == "csv":
                    lines = raw.strip().split('\n')
                    if lines:
                        import csv
                        from io import StringIO
                        reader = csv.DictReader(StringIO('\n'.join(lines)))
                        parsed_result = json.dumps(list(reader))

                elif parser_type == "list":
                    items = re.findall(r'(?:^|\n)(?:[-*•]|\d+\.)\s+(.+)', raw)
                    parsed_result = json.dumps(items if items else raw.strip().split('\n'))

                elif parser_type == "key_value":
                    pairs = re.findall(r'(\w+):\s*(.+?)(?:\n|$)', raw)
                    parsed_result = json.dumps(dict(pairs))

                elif parser_type == "markdown_table":
                    lines = [l.strip() for l in raw.split('\n') if '|' in l]
                    if len(lines) >= 2:
                        headers = [h.strip() for h in lines[0].split('|')[1:-1]]
                        rows = []
                        for line in lines[2:]:  # Skip separator
                            values = [v.strip() for v in line.split('|')[1:-1]]
                            rows.append(dict(zip(headers, values)))
                        parsed_result = json.dumps(rows)

                elif parser_type == "custom" and custom_regex:
                    matches = re.findall(custom_regex, raw, re.DOTALL)
                    parsed_result = json.dumps(matches)

                if parsed_result is None:
                    parsed_result = json.dumps({"raw": raw})

                # Validate schema if provided
                if validation_schema_str and parser_type == "json":
                    try:
                        import jsonschema
                        schema = json.loads(validation_schema_str)
                        jsonschema.validate(json.loads(parsed_result), schema)
                    except ImportError:
                        ctx.log.warning("jsonschema not installed, skipping validation")
                    except Exception as e:
                        if strict_validation:
                            raise
                        ctx.log.warning(f"Schema validation failed: {e}")

                return parsed_result

            df[output_column] = df[input_column].apply(parse_single)

            ctx.log.info(f"Parsed {len(df)} rows successfully")
            ctx.add_output_metadata({
                "parser_type": parser_type,
                "rows_processed": len(df),
                "input_column": input_column,
                "output_column": output_column,
            })

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
            if column_lineage:
                _upstream_key = AssetKey.from_user_string(upstream_asset_key) if upstream_asset_key else None
                if _upstream_key:
                    _lineage_deps = {}
                    for out_col, in_cols in column_lineage.items():
                        _lineage_deps[out_col] = [
                            TableColumnDep(asset_key=_upstream_key, column_name=ic)
                            for ic in in_cols
                        ]
                    _metadata["dagster/column_lineage"] = MetadataValue.table_column_lineage(
                        TableColumnLineage(_lineage_deps)
                    )
            context.add_output_metadata(_metadata)

            return df

        from dagster import build_column_schema_change_checks


        _schema_checks = build_column_schema_change_checks(assets=[llm_output_parser_asset])


        return Definitions(assets=[llm_output_parser_asset], asset_checks=list(_schema_checks))
