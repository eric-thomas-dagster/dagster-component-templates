"""DataframeExtractFieldComponent — pull a field out of a column of dicts/lists.

API responses + Vision/DLP/etc. components often emit columns whose values are
LISTS of DICTS (`[{"description": "apple", "score": 0.95}, ...]`). Downstream
components frequently want just one field from the first (or N-th) element —
e.g. the top label, the most likely score, the first email.

Configuration:
  - `source_column`     — the nested column to read
  - `target_column`     — the new flat column to write
  - `index`             — list index to pluck (default 0)
  - `field`             — dict field to pluck after indexing (optional —
                          set when the list contains dicts, omit when it's
                          a list of scalars)

Examples:
  Vision labels — pick the top description:
    source_column: vision_labels
    target_column: top_label
    index: 0
    field: description

  DLP findings — pick the highest-confidence infotype name:
    source_column: dlp_findings
    target_column: top_pii_type
    index: 0
    field: info_type
"""

from typing import Any, Dict, List, Optional

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


class DataframeExtractFieldComponent(Component, Model, Resolvable):
    """Pluck one element + optional field out of a list/dict column."""

    asset_name: str = Field(description="Output asset name.")
    upstream_asset_key: str = Field(description="Upstream DataFrame asset key.")

    source_column: str = Field(description="Column to read from (values are lists or dicts).")
    target_column: str = Field(description="New column to write the extracted scalar into.")

    index: int = Field(
        default=0,
        description="List index to pluck. Negative indices work (-1 = last). Ignored if source is a dict.",
    )
    field: Optional[str] = Field(
        default=None,
        description=(
            "If the indexed item is a dict, take this field. Omit when items are scalars."
        ),
    )

    drop_source: bool = Field(
        default=False,
        description="If True, drop the source nested column from the output.",
    )

    description: Optional[str] = Field(default=None)
    group_name: Optional[str] = Field(default=None)
    tags: Optional[Dict[str, str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_key = AssetKey.from_user_string(self.upstream_asset_key)
        source_column = self.source_column
        target_column = self.target_column
        idx = self.index
        field = self.field
        drop_source = self.drop_source

        @asset(
            name=asset_name,
            description=self.description or f"Extract {source_column}[{idx}].{field or '_value'} → {target_column}.",
            group_name=self.group_name,
            kinds={"pandas"},
            tags=self.tags or None,
            owners=self.owners or None,
            ins={"upstream": AssetIn(key=upstream_key)},
        )
        def _asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> Output:
            if source_column not in upstream.columns:
                raise ValueError(f"source_column={source_column!r} not in upstream: {list(upstream.columns)}")

            def _extract(v: Any) -> Any:
                if isinstance(v, list):
                    if not v:
                        return None
                    try:
                        item = v[idx]
                    except (IndexError, TypeError):
                        return None
                elif isinstance(v, dict):
                    item = v
                else:
                    return None
                if field is None:
                    return item
                if isinstance(item, dict):
                    return item.get(field)
                return None

            df = upstream.copy()
            df[target_column] = df[source_column].apply(_extract)
            if drop_source:
                df = df.drop(columns=[source_column])

            non_null = int(df[target_column].notna().sum())
            return Output(
                value=df,
                metadata={
                    "rows":             MetadataValue.int(len(df)),
                    "target_column":    MetadataValue.text(target_column),
                    "non_null_count":   MetadataValue.int(non_null),
                    "preview":          MetadataValue.md(df.head(5).to_markdown(index=False) or ""),
                },
            )

        return Definitions(assets=[_asset])
