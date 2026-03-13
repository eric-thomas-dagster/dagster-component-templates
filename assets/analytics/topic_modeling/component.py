"""Topic Modeling.

Discover topics in a text column using Latent Dirichlet Allocation (LDA).
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
    MetadataValue,
    Model,
    Resolvable,
    asset,
)
from pydantic import Field


@dataclass
class TopicModelingComponent(Component, Model, Resolvable):
    """Discover topics in a text column using Latent Dirichlet Allocation (LDA)."""

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame")
    text_column: str = Field(description="Column name containing text to model")
    n_topics: int = Field(default=10, description="Number of topics to discover")
    n_top_words: int = Field(default=10, description="Number of top words per topic to log to metadata")
    max_features: int = Field(default=1000, description="Maximum vocabulary size for CountVectorizer")
    max_iter: int = Field(default=10, description="Maximum LDA iterations")
    random_state: int = Field(default=42, description="Random seed for reproducibility")
    output_mode: str = Field(
        default="dominant_topic",
        description="Output mode: 'dominant_topic' (add dominant_topic column), 'all_topics' (add score per topic), 'topic_table' (return topic-word table)",
    )
    language: str = Field(default="english", description="Language for CountVectorizer stop words")
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
        return "Discover topics in a text column using Latent Dirichlet Allocation (LDA)."

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        text_column = self.text_column
        n_topics = self.n_topics
        n_top_words = self.n_top_words
        max_features = self.max_features
        max_iter = self.max_iter
        random_state = self.random_state
        output_mode = self.output_mode
        language = self.language
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
            try:
                from sklearn.decomposition import LatentDirichletAllocation
                from sklearn.feature_extraction.text import CountVectorizer
            except ImportError as e:
                raise ImportError("scikit-learn is required: pip install scikit-learn") from e

            df = upstream.copy()
            texts = df[text_column].fillna("").astype(str)

            vectorizer = CountVectorizer(max_features=max_features, stop_words=language)
            X = vectorizer.fit_transform(texts)

            lda = LatentDirichletAllocation(
                n_components=n_topics, max_iter=max_iter, random_state=random_state
            )
            topic_matrix = lda.fit_transform(X)

            feature_names = vectorizer.get_feature_names_out()

            # Build topic descriptions for metadata
            topic_descriptions = {}
            for topic_idx, topic in enumerate(lda.components_):
                top_words = [feature_names[i] for i in topic.argsort()[: -n_top_words - 1 : -1]]
                topic_descriptions[f"topic_{topic_idx}"] = ", ".join(top_words)

            context.add_output_metadata({
                "n_topics": MetadataValue.int(n_topics),
                "vocabulary_size": MetadataValue.int(len(feature_names)),
                "documents": MetadataValue.int(len(texts)),
                "topics": MetadataValue.json(topic_descriptions),
            })

            if output_mode == "topic_table":
                rows = []
                for topic_idx, top_words_str in topic_descriptions.items():
                    rows.append({"topic": int(topic_idx.split("_")[1]), "top_words": top_words_str})
                return pd.DataFrame(rows)
            elif output_mode == "dominant_topic":
                df["dominant_topic"] = topic_matrix.argmax(axis=1)
                return df
            elif output_mode == "all_topics":
                for i in range(n_topics):
                    df[f"topic_{i}_score"] = topic_matrix[:, i]
                return df
            else:
                raise ValueError(f"Unknown output_mode: {output_mode}. Use 'dominant_topic', 'all_topics', or 'topic_table'.")

        return Definitions(assets=[_asset])
