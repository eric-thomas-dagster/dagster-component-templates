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
class TextSimilarityComponent(Component, Model, Resolvable):
    """Compute semantic or lexical similarity between two text columns or against a fixed query."""

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
    text_column_a: str = Field(description="First text column for comparison")
    text_column_b: Optional[str] = Field(
        default=None,
        description="Second text column. If omitted, compare each row against `query`.",
    )
    query: Optional[str] = Field(
        default=None,
        description="Fixed string to compare each row against (used when text_column_b is omitted)",
    )
    output_column: str = Field(default="similarity_score", description="Column to write float similarity score (0-1)")
    method: str = Field(
        default="cosine_tfidf",
        description="Similarity method: cosine_tfidf, jaccard, sequence_matcher, or semantic",
    )
    model_name: str = Field(
        default="all-MiniLM-L6-v2",
        description="Sentence-transformers model name (used for semantic method only)",
    )

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        group_name = self.group_name
        text_column_a = self.text_column_a
        text_column_b = self.text_column_b
        query = self.query
        output_column = self.output_column
        method = self.method
        model_name = self.model_name

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
            if text_column_a not in upstream.columns:
                raise ValueError(f"Column '{text_column_a}' not found in DataFrame.")
            if text_column_b and text_column_b not in upstream.columns:
                raise ValueError(f"Column '{text_column_b}' not found in DataFrame.")
            if not text_column_b and not query:
                raise ValueError("Either text_column_b or query must be provided.")

            df = upstream.copy()
            texts_a = df[text_column_a].fillna("").astype(str).tolist()
            texts_b = df[text_column_b].fillna("").astype(str).tolist() if text_column_b else [query] * len(df)

            if method == "cosine_tfidf":
                try:
                    from sklearn.feature_extraction.text import TfidfVectorizer
                    from sklearn.metrics.pairwise import cosine_similarity
                    import numpy as np
                except ImportError:
                    raise ImportError("pip install scikit-learn>=1.0.0")

                all_texts = texts_a + texts_b
                vectorizer = TfidfVectorizer()
                tfidf = vectorizer.fit_transform(all_texts)
                n = len(texts_a)
                scores = []
                for i in range(n):
                    sim = cosine_similarity(tfidf[i], tfidf[n + i])[0][0]
                    scores.append(round(float(sim), 6))

            elif method == "jaccard":
                def jaccard(a: str, b: str) -> float:
                    set_a = set(a.lower().split())
                    set_b = set(b.lower().split())
                    if not set_a and not set_b:
                        return 1.0
                    intersection = len(set_a & set_b)
                    union = len(set_a | set_b)
                    return round(intersection / union, 6) if union > 0 else 0.0

                scores = [jaccard(a, b) for a, b in zip(texts_a, texts_b)]

            elif method == "sequence_matcher":
                from difflib import SequenceMatcher

                scores = [
                    round(SequenceMatcher(None, a, b).ratio(), 6)
                    for a, b in zip(texts_a, texts_b)
                ]

            elif method == "semantic":
                try:
                    from sentence_transformers import SentenceTransformer
                    from sklearn.metrics.pairwise import cosine_similarity
                    import numpy as np
                except ImportError:
                    raise ImportError("pip install sentence-transformers>=2.2.0 scikit-learn>=1.0.0")

                model = SentenceTransformer(model_name)
                emb_a = model.encode(texts_a, convert_to_numpy=True)
                emb_b = model.encode(texts_b, convert_to_numpy=True)
                scores = [
                    round(float(cosine_similarity([emb_a[i]], [emb_b[i]])[0][0]), 6)
                    for i in range(len(texts_a))
                ]
            else:
                raise ValueError(f"Unknown method: {method}")

            df[output_column] = scores

            context.add_output_metadata({
                "row_count": MetadataValue.int(len(df)),
                "mean_similarity": MetadataValue.float(round(float(pd.Series(scores).mean()), 4)),
            })
            return df

        return Definitions(assets=[_asset])
