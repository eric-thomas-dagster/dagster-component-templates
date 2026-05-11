"""DataframeFlattenNestedColumnsComponent — JSON-stringify dict/list columns.

Many DataFrame sinks (BigQuery `load_table_from_dataframe`, Snowflake's pandas
writer, plain CSV writers) can't infer a column type for object-dtype columns
holding nested dicts/lists. This component is the standard fix: walk each row
of an upstream DataFrame, find columns whose values are dicts or lists, and
JSON-encode those values to strings.

Common chain:
  cloud_logging_query_asset → dataframe_flatten_nested_columns → dataframe_to_bigquery

Configurable scopes:
  - `columns`: explicit allowlist. Default: every object-dtype column with at
    least one dict/list value.
  - `exclude_columns`: skip these even if they contain dicts/lists.
"""

from typing import Any, Dict, List, Optional

import json
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
    Output,
    Resolvable,
    asset,
)
from pydantic import Field


class DataframeFlattenNestedColumnsComponent(Component, Model, Resolvable):
    """JSON-stringify dict / list values in selected DataFrame columns."""

    asset_name: str = Field(description="Output asset name.")
    upstream_asset_key: str = Field(description="Upstream DataFrame asset key.")

    columns: Optional[List[str]] = Field(
        default=None,
        description="Explicit columns to flatten. Default: every column with at least one dict/list value.",
    )
    exclude_columns: Optional[List[str]] = Field(
        default=None,
        description="Skip these columns even if they contain dicts/lists.",
    )

    description: Optional[str] = Field(default=None)
    group_name: Optional[str] = Field(default=None)
    tags: Optional[Dict[str, str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_key = AssetKey.from_user_string(self.upstream_asset_key)
        explicit_cols = self.columns
        exclude = set(self.exclude_columns or [])

        @asset(
            name=asset_name,
            description=self.description or "Flatten nested dict/list columns to JSON strings.",
            group_name=self.group_name,
            kinds={"pandas"},
            tags=self.tags or None,
            owners=self.owners or None,
            ins={"upstream": AssetIn(key=upstream_key)},
        )
        def _asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> Output:
            df = upstream.copy()

            if explicit_cols is not None:
                target_cols = [c for c in explicit_cols if c in df.columns]
            else:
                target_cols = [
                    c for c in df.columns
                    if c not in exclude
                    and df[c].dtype == object
                    and bool(df[c].apply(lambda v: isinstance(v, (dict, list))).any())
                ]

            flattened: List[str] = []
            for col in target_cols:
                if col in exclude:
                    continue
                df[col] = df[col].apply(
                    lambda v: json.dumps(v, default=str) if isinstance(v, (dict, list)) else v
                )
                flattened.append(col)

            return Output(
                value=df,
                metadata={
                    "rows":              MetadataValue.int(len(df)),
                    "columns_flattened": MetadataValue.json(flattened),
                    "columns_untouched": MetadataValue.json([c for c in df.columns if c not in flattened]),
                    "preview":           MetadataValue.md(df.head(5).to_markdown(index=False) or ""),
                },
            )

        return Definitions(assets=[_asset])
