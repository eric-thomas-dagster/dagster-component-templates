from dataclasses import dataclass, field
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
class DataEnricherComponent(Component, Model, Resolvable):
    """Enrich DataFrame rows with LLM-generated fields based on context columns."""

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
    context_columns: List[str] = Field(description="Columns to include as context for each row")
    enrichment_fields: Dict[str, str] = Field(
        description="Mapping of new_column_name -> instruction e.g. {'sentiment': 'positive/negative/neutral'}"
    )
    model: str = Field(default="gpt-4o-mini", description="LLM model to use for enrichment")
    output_prefix: str = Field(default="", description="Prefix to add to all new enrichment column names")
    max_workers: int = Field(default=4, description="Number of parallel threads for API calls")
    api_key_env_var: Optional[str] = Field(
        default=None,
        description="Environment variable name containing the API key",
    )

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        group_name = self.group_name
        context_columns = self.context_columns
        enrichment_fields = self.enrichment_fields
        model = self.model
        output_prefix = self.output_prefix
        max_workers = self.max_workers
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
            import json
            from concurrent.futures import ThreadPoolExecutor, as_completed

            for col in context_columns:
                if col not in upstream.columns:
                    raise ValueError(f"Context column '{col}' not found in DataFrame.")

            api_key = os.environ.get(api_key_env_var) if api_key_env_var else None
            fields_description = "\n".join(
                f'- "{k}": {v}' for k, v in enrichment_fields.items()
            )
            expected_keys = list(enrichment_fields.keys())

            system_prompt = (
                "You are a data enrichment assistant. Given row context, return a JSON object "
                "with exactly these fields:\n"
                f"{fields_description}\n\n"
                "Return ONLY valid JSON with no explanation or markdown."
            )

            def enrich_row(row_data: dict) -> dict:
                context_str = "\n".join(f"{k}: {v}" for k, v in row_data.items())
                try:
                    response = litellm.completion(
                        model=model,
                        messages=[
                            {"role": "system", "content": system_prompt},
                            {"role": "user", "content": context_str},
                        ],
                        max_tokens=500,
                                    api_key=api_key,
            )
                    content = response.choices[0].message.content.strip()
                    # Strip markdown if present
                    if content.startswith("```"):
                        lines = content.split("\n")
                        content = "\n".join(lines[1:-1])
                    parsed = json.loads(content)
                    return {k: parsed.get(k) for k in expected_keys}
                except Exception as e:
                    return {k: None for k in expected_keys}

            df = upstream.copy()
            rows_data = [
                {col: row[col] for col in context_columns if col in row}
                for _, row in df.iterrows()
            ]

            results = [None] * len(rows_data)
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_idx = {executor.submit(enrich_row, rd): i for i, rd in enumerate(rows_data)}
                for future in as_completed(future_to_idx):
                    idx = future_to_idx[future]
                    try:
                        results[idx] = future.result()
                    except Exception:
                        results[idx] = {k: None for k in expected_keys}

            for key in expected_keys:
                out_col = f"{output_prefix}{key}"
                df[out_col] = [r.get(key) if r else None for r in results]

            successful = sum(1 for r in results if r and any(v is not None for v in r.values()))
            context.add_output_metadata({
                "row_count": MetadataValue.int(len(df)),
                "successful_enrichments": MetadataValue.int(successful),
                "enrichment_fields": MetadataValue.int(len(expected_keys)),
            })
            return df

        return Definitions(assets=[_asset])
