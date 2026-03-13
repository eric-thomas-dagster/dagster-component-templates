"""Part of Speech Tagger Component.

Tag parts of speech in text using spaCy. Supports tags_column, expanded, and counts output modes.
"""

from dataclasses import dataclass
from typing import Optional, List
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
class PartOfSpeechTaggerComponent(Component, Model, Resolvable):
    """Component for tagging parts of speech in text using spaCy.

    Processes a text column in an upstream DataFrame and returns POS annotations.
    Supports three output modes: adding a tags column, expanding to one row per token,
    or counting POS types per row.

    Features:
    - Three output modes: tags_column, expanded, counts
    - Token text alongside POS tags
    - Dependency relation tagging (expanded mode)
    - Language model selection
    - Handles null/missing text gracefully

    Use Cases:
    - NLP preprocessing pipelines
    - Linguistic feature extraction
    - Grammar analysis
    - Text structure analysis
    """

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame")
    text_column: str = Field(description="Column containing text to tag")
    output_mode: str = Field(
        default="tags_column",
        description="Output mode: 'tags_column' (add column with POS tag list), 'expanded' (one row per token with tag), 'counts' (count of each POS type per row)",
    )
    output_column: str = Field(
        default="pos_tags",
        description="Output column name for mode='tags_column'",
    )
    language: str = Field(
        default="en",
        description="spaCy language model e.g. 'en', 'en_core_web_sm'",
    )
    include_token_text: bool = Field(
        default=True,
        description="Include token text alongside POS tag",
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
        text_column = self.text_column
        output_mode = self.output_mode
        output_column = self.output_column
        language = self.language
        include_token_text = self.include_token_text

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
            try:
                import spacy
                try:
                    nlp = spacy.load(language if "_" in language else f"{language}_core_web_sm")
                except OSError:
                    raise OSError(
                        f"spaCy model not found. Run: python -m spacy download {language}_core_web_sm"
                    )
            except ImportError:
                raise ImportError(
                    "spacy required: pip install spacy && python -m spacy download en_core_web_sm"
                )

            context.log.info(
                f"Running POS tagger on {len(upstream)} rows, mode={output_mode}"
            )

            df = upstream.copy()

            if output_mode == "tags_column":
                def get_tags(text):
                    if pd.isna(text):
                        return []
                    doc = nlp(str(text))
                    if include_token_text:
                        return [(token.text, token.pos_) for token in doc]
                    return [token.pos_ for token in doc]

                df[output_column] = df[text_column].apply(get_tags)
                context.log.info(f"Added POS tags column '{output_column}'")

            elif output_mode == "expanded":
                rows = []
                for idx, row in df.iterrows():
                    text = row[text_column]
                    doc = nlp(str(text) if not pd.isna(text) else "")
                    for token in doc:
                        rows.append(
                            {**row.to_dict(), "token": token.text, "pos_tag": token.pos_, "dep": token.dep_}
                        )
                df = pd.DataFrame(rows)
                context.log.info(f"Expanded to {len(df)} token rows")

            elif output_mode == "counts":
                import collections

                def count_pos(text):
                    if pd.isna(text):
                        return {}
                    doc = nlp(str(text))
                    return dict(collections.Counter(t.pos_ for t in doc))

                pos_df = df[text_column].apply(count_pos).apply(pd.Series).fillna(0)
                df = pd.concat([df, pos_df.add_prefix("pos_")], axis=1)
                context.log.info(f"Added POS count columns: {list(pos_df.columns)}")

            else:
                raise ValueError(
                    f"Unknown output_mode '{output_mode}'. Choose: tags_column, expanded, counts"
                )

            context.add_output_metadata(
                {
                    "num_rows": len(df),
                    "output_mode": output_mode,
                    "language": language,
                    "preview": MetadataValue.md(df.head(5).to_markdown()),
                }
            )
            return df

        return Definitions(assets=[_asset])
