"""Schema Fit Component.

Use an LLM to automatically map a source DataFrame's columns to a target schema,
applying transformations as needed.
"""

from dataclasses import dataclass
from typing import Optional, List, Dict
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


@dataclass
class SchemaFitComponent(Component, Model, Resolvable):
    """Component for LLM-driven schema mapping and transformation.

    Sends sample rows and source column names to an LLM along with the target
    schema definition. The LLM returns a mapping plan (direct copy, concat,
    split, format, or compute), which is then applied to the full DataFrame.

    Features:
    - Automatic column mapping via LLM
    - Supports direct copy, concat, split, format, and compute transformations
    - Configurable sample size for LLM context
    - Any litellm-compatible model
    - Graceful fallback to None for failed mappings

    Use Cases:
    - Data integration between systems with different schemas
    - ETL normalization
    - Legacy data migration
    - API response normalization
    """

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(
        description="Upstream asset key providing the source DataFrame to transform"
    )
    target_schema: Dict[str, str] = Field(
        description="Target schema definition: {column_name: description} e.g. {'customer_id': 'unique customer identifier'}"
    )
    model: str = Field(default="gpt-4o-mini", description="LLM model name (litellm format)")
    api_key_env_var: str = Field(
        default="OPENAI_API_KEY",
        description="Environment variable name holding the API key",
    )
    sample_rows: int = Field(
        default=3, description="Number of sample rows to send as LLM context"
    )
    batch_size: int = Field(
        default=10, description="Row batch size for apply step (unused in mapping, kept for consistency)"
    )
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name")
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

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        target_schema = self.target_schema
        model = self.model
        api_key_env_var = self.api_key_env_var
        sample_rows = self.sample_rows

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

        @asset(
            name=asset_name,
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
            partitions_def=partitions_def,
            group_name=self.group_name,
        )
        def _asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
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
            import os
            import json

            try:
                from litellm import completion
            except ImportError:
                raise ImportError("litellm required: pip install litellm")

            source_cols = list(upstream.columns)
            source_sample = upstream.head(sample_rows).to_dict(orient="records")

            context.log.info(
                f"Requesting LLM schema mapping: {len(source_cols)} source cols → {len(target_schema)} target cols"
            )

            mapping_prompt = (
                f"Given source columns {source_cols} with sample data:\n"
                f"{json.dumps(source_sample, default=str)}\n\n"
                f"Map to target schema: {json.dumps(target_schema)}\n\n"
                'Return JSON: {"mappings": [{"target_col": "...", "source_col": "...", '
                '"transformation": "direct|concat|split|format|compute", "expression": "..."}]}\n'
                "For transformations: direct=copy, concat=\"col1 + ' ' + col2\", "
                "split=\"col.str.split()[0]\", format=\"pd.to_datetime(col)\", compute=\"col*100\""
            )

            resp = completion(
                model=model,
                messages=[{"role": "user", "content": mapping_prompt}],
                api_key=os.environ.get(api_key_env_var),
            )

            raw = resp.choices[0].message.content.strip()
            if raw.startswith("```"):
                raw = raw.split("```")[1]
                if raw.startswith("json"):
                    raw = raw[4:]

            try:
                mapping_plan = json.loads(raw)
                mappings = mapping_plan["mappings"]
            except Exception as e:
                raise ValueError(f"Failed to parse LLM mapping response: {e}\nRaw: {raw}")

            context.log.info(f"LLM returned {len(mappings)} mappings")

            result = pd.DataFrame(index=upstream.index)

            for mapping in mappings:
                target = mapping.get("target_col")
                src = mapping.get("source_col")
                expr = mapping.get("expression", "")
                transform = mapping.get("transformation", "direct")

                if not target:
                    continue

                try:
                    if transform == "direct" and src and src in upstream.columns:
                        result[target] = upstream[src]
                    elif expr:
                        safe_expr = expr
                        if src and src in upstream.columns:
                            safe_expr = safe_expr.replace(src, f"upstream['{src}']")
                        result[target] = eval(  # noqa: S307
                            safe_expr, {"upstream": upstream, "pd": pd}
                        )
                    else:
                        context.log.warning(
                            f"No valid mapping for target '{target}' (transform={transform}, src={src})"
                        )
                        result[target] = None
                except Exception as e:
                    context.log.warning(f"Failed to map '{target}': {e}")
                    result[target] = None

            # Ensure all target schema columns exist (fill missing with None)
            for col in target_schema:
                if col not in result.columns:
                    context.log.warning(f"Target column '{col}' not produced by LLM mapping; filling with None")
                    result[col] = None

            context.add_output_metadata(
                {
                    "source_columns": source_cols,
                    "target_columns": list(target_schema.keys()),
                    "mappings_applied": len(mappings),
                    "output_rows": len(result),
                    "preview": MetadataValue.md(result.head(5).to_markdown()),
                }
            )
            return result

        return Definitions(assets=[_asset])
