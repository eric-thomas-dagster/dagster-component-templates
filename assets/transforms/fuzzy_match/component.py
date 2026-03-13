"""FuzzyMatch.

Find and optionally deduplicate rows with similar string values using fuzzy matching.
"""
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
    Model,
    Resolvable,
    asset,
)
from pydantic import Field


@dataclass
class FuzzyMatch(Component, Model, Resolvable):
    """Find and optionally deduplicate rows with similar string values using fuzzy matching."""

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame")
    column: str = Field(description="Column to fuzzy match on")
    threshold: float = Field(default=0.8, description="Similarity threshold 0-1")
    mode: str = Field(default="deduplicate", description="'deduplicate' (remove near-duplicates), 'score' (add similarity scores), 'group' (assign cluster IDs to similar values)")
    output_column: str = Field(default="fuzzy_group", description="For mode='group', column name for group IDs")
    keep: str = Field(default="first", description="For mode='deduplicate': 'first' or 'last'")
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
        return "Find and optionally deduplicate rows with similar string values using fuzzy matching."

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        column = self.column
        threshold = self.threshold
        mode = self.mode
        output_column = self.output_column
        keep = self.keep
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
            from difflib import SequenceMatcher

            def similarity(a, b):
                return SequenceMatcher(None, str(a).lower(), str(b).lower()).ratio()

            df = upstream.copy().reset_index(drop=True)
            values = df[column].tolist()

            if mode == "deduplicate":
                kept_indices = []
                kept_values = []
                drop_indices = set()

                iter_indices = range(len(values)) if keep == "first" else range(len(values) - 1, -1, -1)

                for i in iter_indices:
                    if i in drop_indices:
                        continue
                    is_dup = False
                    for kv in kept_values:
                        if similarity(values[i], kv) >= threshold:
                            is_dup = True
                            break
                    if not is_dup:
                        kept_indices.append(i)
                        kept_values.append(values[i])
                    else:
                        drop_indices.add(i)

                kept_indices_sorted = sorted(kept_indices)
                df = df.iloc[kept_indices_sorted].reset_index(drop=True)

            elif mode == "score":
                scores = []
                for i, val in enumerate(values):
                    max_score = 0.0
                    for j, other in enumerate(values):
                        if i == j:
                            continue
                        s = similarity(val, other)
                        if s > max_score:
                            max_score = s
                    scores.append(max_score)
                df["fuzzy_score"] = scores

            elif mode == "group":
                group_ids = [-1] * len(values)
                current_group = 0

                for i in range(len(values)):
                    if group_ids[i] != -1:
                        continue
                    group_ids[i] = current_group
                    for j in range(i + 1, len(values)):
                        if group_ids[j] == -1 and similarity(values[i], values[j]) >= threshold:
                            group_ids[j] = current_group
                    current_group += 1

                df[output_column] = group_ids

            return df

        return Definitions(assets=[_asset])
