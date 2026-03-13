from dataclasses import dataclass
from typing import Optional
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
class SqlGeneratorComponent(Component, Model, Resolvable):
    """Generate SQL queries from natural language questions using an LLM, with optional schema context."""

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame")
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
    question_column: str = Field(description="Column containing natural language questions")
    output_column: str = Field(default="generated_sql", description="Column to write generated SQL queries")
    schema_context: Optional[str] = Field(
        default=None,
        description="DDL or table description to include in the system prompt",
    )
    dialect: str = Field(
        default="generic",
        description="SQL dialect to generate for",
    )
    model: str = Field(default="gpt-4o-mini", description="LLM model to use for SQL generation")
    validate_sql: bool = Field(
        default=False,
        description="Run basic syntax check using sqlparse and add a validation column",
    )
    api_key_env_var: Optional[str] = Field(
        default=None,
        description="Environment variable name containing the API key",
    )

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        group_name = self.group_name
        question_column = self.question_column
        output_column = self.output_column
        schema_context = self.schema_context
        dialect = self.dialect
        model = self.model
        validate_sql = self.validate_sql
        api_key_env_var = self.api_key_env_var

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
            group_name=group_name,
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
            try:
                import litellm
            except ImportError:
                raise ImportError("pip install litellm>=1.0.0")

            import os

            if question_column not in upstream.columns:
                raise ValueError(f"Column '{question_column}' not found in DataFrame.")

            api_key = os.environ.get(api_key_env_var) if api_key_env_var else None
            dialect_note = f" Use {dialect} SQL dialect." if dialect != "generic" else ""
            schema_note = f"\n\nDatabase schema:\n{schema_context}" if schema_context else ""

            system_prompt = (
                f"You are an expert SQL generator.{dialect_note} "
                f"Generate only the SQL query with no explanation, no markdown code blocks, no extra text.{schema_note}"
            )

            def generate_sql(question: object) -> str:
                if question is None or (isinstance(question, float) and pd.isna(question)):
                    return None
                try:
                    response = litellm.completion(
                        model=model,
                        messages=[
                            {"role": "system", "content": system_prompt},
                            {"role": "user", "content": str(question                api_key=api_key,
            )},
                        ],
                        max_tokens=500,
                    )
                    sql = response.choices[0].message.content.strip()
                    # Strip markdown code blocks if present
                    if sql.startswith("```"):
                        lines = sql.split("\n")
                        sql = "\n".join(lines[1:-1]) if len(lines) > 2 else sql
                    return sql
                except Exception as e:
                    context.log.warning(f"SQL generation failed: {e}")
                    return None

            df = upstream.copy()
            df[output_column] = df[question_column].apply(generate_sql)

            if validate_sql:
                try:
                    import sqlparse

                    def is_valid_sql(sql: object) -> bool:
                        if sql is None or (isinstance(sql, float) and pd.isna(sql)):
                            return False
                        try:
                            parsed = sqlparse.parse(str(sql))
                            return len(parsed) > 0 and bool(parsed[0].tokens)
                        except Exception:
                            return False

                    df[f"{output_column}_valid"] = df[output_column].apply(is_valid_sql)
                except ImportError:
                    context.log.warning("sqlparse not installed, skipping SQL validation.")

            successful = df[output_column].notna().sum()
            context.add_output_metadata({
                "row_count": MetadataValue.int(len(df)),
                "successful_generations": MetadataValue.int(int(successful)),
                "model": MetadataValue.text(model),
                "dialect": MetadataValue.text(dialect),
            })
            return df

        return Definitions(assets=[_asset])
