"""Precision Match.

LLM-assisted fuzzy matching to standardize varied string representations to canonical forms.
"""
from dataclasses import dataclass
from typing import List, Optional

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
class PrecisionMatchComponent(Component, Model, Resolvable):
    """LLM-assisted fuzzy matching to standardize varied string representations to canonical forms."""

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame with values to standardize")
    column: str = Field(description="Column with varied strings to standardize")
    reference_asset_key: Optional[str] = Field(default=None, description="Optional upstream asset key for a reference DataFrame containing canonical values")
    reference_column: Optional[str] = Field(default=None, description="Column in the reference DataFrame containing canonical values")
    reference_values: Optional[List[str]] = Field(default=None, description="Explicit list of canonical values (alternative to reference_asset_key)")
    model: str = Field(default="gpt-4o-mini", description="LLM model identifier (passed to litellm)")
    api_key_env_var: str = Field(default="OPENAI_API_KEY", description="Environment variable name holding the API key")
    output_column: str = Field(default="matched_value", description="Column name for the matched canonical value")
    confidence_column: Optional[str] = Field(default="match_confidence", description="Column name for match confidence score (None to skip)")
    batch_size: int = Field(default=20, description="Number of unique values to match per LLM call")
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

    @classmethod
    def get_description(cls) -> str:
        return "LLM-assisted fuzzy matching to standardize varied string representations to canonical forms."

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        column = self.column
        reference_asset_key = self.reference_asset_key
        reference_column = self.reference_column
        reference_values = self.reference_values
        llm_model = self.model
        api_key_env_var = self.api_key_env_var
        output_column = self.output_column
        confidence_column = self.confidence_column
        batch_size = self.batch_size
        group_name = self.group_name

        if reference_asset_key:
            ins = {
                "upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key)),
                "reference": AssetIn(key=AssetKey.from_user_string(reference_asset_key)),
            }

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

        partitions_def=partitions_def,
        @asset(name=asset_name, ins=ins, group_name=group_name)
            def _asset(
                context: AssetExecutionContext,
                upstream: pd.DataFrame,
                reference: pd.DataFrame,
            ) -> pd.DataFrame:
                return _run_matching(
                    context=context,
                    df=upstream,
                    column=column,
                    canonical=reference[reference_column].dropna().unique().tolist(),
                    llm_model=llm_model,
                    api_key_env_var=api_key_env_var,
                    output_column=output_column,
                    confidence_column=confidence_column,
                    batch_size=batch_size,
                )

        else:
            ins = {"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))}

            @asset(name=asset_name, ins=ins, group_name=group_name)
            def _asset(  # type: ignore[no-redef]
                context: AssetExecutionContext,
                upstream: pd.DataFrame,
            ) -> pd.DataFrame:
                canonical = reference_values or []
                return _run_matching(
                    context=context,
                    df=upstream,
                    column=column,
                    canonical=canonical,
                    llm_model=llm_model,
                    api_key_env_var=api_key_env_var,
                    output_column=output_column,
                    confidence_column=confidence_column,
                    batch_size=batch_size,
                )

        return Definitions(assets=[_asset])


def _run_matching(
    context: AssetExecutionContext,
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
    df: pd.DataFrame,
    column: str,
    canonical: list,
    llm_model: str,
    api_key_env_var: str,
    output_column: str,
    confidence_column: Optional[str],
    batch_size: int,
) -> pd.DataFrame:
    import json
    import os

    try:
        from litellm import completion
    except ImportError as e:
        raise ImportError("litellm is required: pip install litellm") from e

    df = df.copy()
    unique_vals = df[column].dropna().unique().tolist()
    api_key = os.environ.get(api_key_env_var)

    mapping: dict = {}
    confidence_map: dict = {}

    for i in range(0, len(unique_vals), batch_size):
        batch = unique_vals[i : i + batch_size]
        prompt = (
            f"Match each input string to the closest canonical value. "
            f"Return a JSON object mapping each input to its matched canonical value "
            f"and a confidence score between 0 and 1.\n"
            f"Format: {{\"input_value\": {{\"match\": \"canonical_value\", \"confidence\": 0.95}}, ...}}\n"
            f"Canonical values: {json.dumps(canonical)}\n"
            f"Inputs to match: {json.dumps(batch)}"
        )
        resp = completion(
            model=llm_model,
            messages=[{"role": "user", "content": prompt}],
            api_key=api_key,
        )
        raw = resp.choices[0].message.content
        # Strip markdown code fences if present
        raw = raw.strip()
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[1].rsplit("```", 1)[0]
        result = json.loads(raw)
        for val, info in result.items():
            if isinstance(info, dict):
                mapping[val] = info.get("match", val)
                confidence_map[val] = info.get("confidence", None)
            else:
                mapping[val] = info

    df[output_column] = df[column].map(mapping)
    if confidence_column:
        df[confidence_column] = df[column].map(confidence_map)

    matched = df[output_column].notna().sum()
    context.add_output_metadata({
        "unique_input_values": MetadataValue.int(len(unique_vals)),
        "canonical_values": MetadataValue.int(len(canonical)),
        "matched_rows": MetadataValue.int(int(matched)),
        "match_rate": MetadataValue.float(float(matched) / max(len(df), 1)),
        "llm_model": MetadataValue.text(llm_model),
    })

    return df
