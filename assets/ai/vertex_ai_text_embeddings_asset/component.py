"""VertexAITextEmbeddingsAssetComponent — text → embedding vectors via Vertex AI.

Drop-in shape parallel to `embeddings_generator` and the openai/anthropic
embedding components. Reads a column of text from an upstream DataFrame,
calls Vertex AI's text-embedding model (text-embedding-004 by default,
with multilingual / gecko / textembedding-gecko-multilingual variants),
returns a DataFrame with an embeddings column.

Useful for RAG pipelines, semantic search, vector store loaders.
"""

import json
import os
import time
from typing import Any, Dict, List, Optional, Union

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


class VertexAITextEmbeddingsAssetComponent(Component, Model, Resolvable):
    """Generate text embeddings via Vertex AI's text-embedding models."""

    asset_name: str = Field(description="Output asset name.")
    upstream_asset_key: str = Field(description="Upstream DataFrame asset key.")

    credentials: Optional[Dict[str, Any]] = Field(default=None)
    credentials_path: Optional[str] = Field(default=None, description="Falls back to GOOGLE_APPLICATION_CREDENTIALS.")

    project_id: Optional[str] = Field(default=None, description="GCP project. Defaults to the SA's project.")
    location: str = Field(default="us-central1", description="Vertex AI region.")

    text_column: Union[str, int] = Field(description="Column with the text to embed.")
    output_column: Union[str, int] = Field(default="embedding", description="Column name for the resulting embedding vector.")

    model_name: str = Field(
        default="text-embedding-004",
        description=(
            "Vertex text-embedding model. Common options: text-embedding-004 (default, "
            "768-dim, English-strong), text-multilingual-embedding-002 (multilingual, "
            "768-dim), gemini-embedding-001 (768-dim, latest), text-embedding-005."
        ),
    )
    task_type: Optional[str] = Field(
        default="RETRIEVAL_DOCUMENT",
        description=(
            "Vertex task hint that tunes the embedding for downstream use. Options: "
            "RETRIEVAL_QUERY, RETRIEVAL_DOCUMENT, SEMANTIC_SIMILARITY, "
            "CLASSIFICATION, CLUSTERING, QUESTION_ANSWERING, FACT_VERIFICATION, "
            "CODE_RETRIEVAL_QUERY. Set to null to omit (some older models don't accept it)."
        ),
    )
    output_dimensionality: Optional[int] = Field(
        default=None,
        description="If supported by the model, truncate embeddings to this many dims (e.g. 256 / 512 to save vector-store space).",
    )

    batch_size: int = Field(default=20, description="Texts per Vertex request (max 250 for most models).")
    rate_limit_delay: float = Field(default=0.2)
    max_retries: int = Field(default=3)

    description: Optional[str] = Field(default=None)
    group_name: Optional[str] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)
    tags: Optional[Dict[str, str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)

    retry_policy_max_retries: Optional[int] = Field(
        default=None,
        description="Max retries on asset failure. Useful for transient errors like network glitches or rate limits.",
    )
    retry_policy_delay_seconds: Optional[int] = Field(
        default=None,
        description="Seconds between retries (default 1).",
    )
    retry_policy_backoff: str = Field(
        default="exponential",
        description="Backoff strategy: 'linear' or 'exponential'.",
    )

    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale.",
    )
    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5'.",
    )

    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily' / 'weekly' / 'monthly' / 'hourly' / 'static' / 'dynamic' / None for unpartitioned.",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.",
    )
    partition_values: Optional[str] = Field(
        default=None,
        description="Comma-separated values for static partitioning, e.g. 'us,eu,asia'.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition when partition_type='dynamic'.",
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        partitions_def = None
        if self.partition_type:
            from dagster import (
                DailyPartitionsDefinition, WeeklyPartitionsDefinition,
                MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
                StaticPartitionsDefinition, DynamicPartitionsDefinition,
            )
            _pt = self.partition_type
            _values = [v.strip() for v in (self.partition_values or "").split(",") if v.strip()]
            if _pt in ("daily", "weekly", "monthly", "hourly") and not self.partition_start:
                raise ValueError(f"partition_type={_pt!r} requires partition_start (ISO date).")
            if _pt == "daily":
                partitions_def = DailyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "weekly":
                partitions_def = WeeklyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "monthly":
                partitions_def = MonthlyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "hourly":
                partitions_def = HourlyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "static":
                if not _values:
                    raise ValueError("partition_type='static' requires partition_values.")
                partitions_def = StaticPartitionsDefinition(_values)
            elif _pt == "dynamic":
                if not self.dynamic_partition_name:
                    raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
                partitions_def = DynamicPartitionsDefinition(name=self.dynamic_partition_name)

        freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy

            freshness_policy = FreshnessPolicy(
                maximum_lag_minutes=self.freshness_max_lag_minutes,
                cron_schedule=self.freshness_cron,
            )

        retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy

            retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        creds_dict = self.credentials
        if creds_dict is None:
            cred_path = self.credentials_path or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
            if cred_path:
                with open(cred_path, "r") as fh:
                    creds_dict = json.load(fh)
        if creds_dict is None:
            raise ValueError("Provide credentials, credentials_path, or set GOOGLE_APPLICATION_CREDENTIALS.")

        asset_name = self.asset_name
        upstream_key = AssetKey.from_user_string(self.upstream_asset_key)
        project_id = self.project_id or creds_dict.get("project_id")
        location = self.location
        text_column = self.text_column
        output_column = self.output_column
        model_name = self.model_name
        task_type = self.task_type
        output_dim = self.output_dimensionality
        batch_size = self.batch_size
        rate_limit_delay = self.rate_limit_delay
        max_retries = self.max_retries

        @asset(
            key=AssetKey.from_user_string(asset_name),
            description=self.description or f"Vertex AI text embeddings ({model_name}).",
            group_name=self.group_name,
            kinds={"vertex-ai", "embeddings"},
            tags=self.tags or None,
            owners=self.owners or None,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])] or None,
            ins={"upstream": AssetIn(key=upstream_key)},
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
            partitions_def=partitions_def,
        )
        def _asset(context: AssetExecutionContext, upstream: Any):
            # partition bridge dict-concat: when an unpartitioned
            # asset consumes a partitioned upstream, Dagster's IO
            # manager loads ALL partitions as a dict; concat to
            # a single DataFrame before any DataFrame ops.
            if isinstance(upstream, dict):
                _frames = [v for v in upstream.values() if isinstance(v, pd.DataFrame)]
                upstream = pd.concat(_frames, ignore_index=True) if _frames else pd.DataFrame()
            try:
                import vertexai
                from vertexai.language_models import TextEmbeddingInput, TextEmbeddingModel
                from google.oauth2 import service_account
            except ImportError:
                raise ImportError("pip install google-cloud-aiplatform google-auth")

            if text_column not in upstream.columns:
                raise ValueError(f"text_column={text_column!r} not in upstream columns: {list(upstream.columns)}")

            sa_creds = service_account.Credentials.from_service_account_info(creds_dict)
            vertexai.init(project=project_id, location=location, credentials=sa_creds)
            model = TextEmbeddingModel.from_pretrained(model_name)

            df = upstream.copy().reset_index(drop=True)
            embeddings: List[Optional[List[float]]] = [None] * len(df)
            errors: List[Optional[str]] = [None] * len(df)

            indices = list(range(len(df)))
            for batch_start in range(0, len(indices), batch_size):
                batch_idx = indices[batch_start:batch_start + batch_size]
                batch_texts = df.iloc[batch_idx][text_column].astype(str).tolist()

                inputs = [
                    TextEmbeddingInput(text=t, task_type=task_type) if task_type else TextEmbeddingInput(text=t)
                    for t in batch_texts
                ]

                attempt = 0
                last_err = None
                while attempt <= max_retries:
                    try:
                        kwargs: Dict[str, Any] = {}
                        if output_dim is not None:
                            kwargs["output_dimensionality"] = output_dim
                        resp = model.get_embeddings(inputs, **kwargs)
                        for j, emb in enumerate(resp):
                            embeddings[batch_idx[j]] = list(emb.values)
                        last_err = None
                        break
                    except Exception as e:
                        err_str = str(e)
                        if "404" in err_str or "NOT_FOUND" in err_str or "model" in err_str.lower():
                            last_err = e
                            break  # don't retry — bad model id
                        last_err = e
                        attempt += 1
                        if attempt > max_retries:
                            break
                        time.sleep((2 ** attempt) * 0.5)

                if last_err is not None:
                    msg = str(last_err)
                    for i in batch_idx:
                        errors[i] = msg
                    if "404" in msg or "NOT_FOUND" in msg:
                        context.log.error(
                            f"Vertex returned 404 for model {model_name!r}. Set "
                            f"`model_name:` to a current id (e.g. text-embedding-004, "
                            f"text-multilingual-embedding-002, gemini-embedding-001)."
                        )
                    elif "PERMISSION_DENIED" in msg or "403" in msg:
                        context.log.error(
                            f"403 PERMISSION_DENIED. Service account needs "
                            f"roles/aiplatform.user on the project. Enable the Vertex AI API "
                            f"if not already on."
                        )
                    else:
                        context.log.warning(f"batch starting at row {batch_idx[0]}: {msg}")
                time.sleep(rate_limit_delay)

            df[output_column] = embeddings
            success_count = sum(1 for e in embeddings if e is not None)
            if any(errors):
                df[f"{output_column}_error"] = errors

            sample = df[[text_column, output_column]].head(3).copy()
            sample[output_column] = sample[output_column].apply(
                lambda v: f"[{len(v)} dims]" if isinstance(v, list) else None
            )
            preview_md = sample.to_markdown(index=False) or ""

            return Output(
                value=df,
                metadata={
                    "row_count":      MetadataValue.int(len(df)),
                    "embeddings_ok":  MetadataValue.int(success_count),
                    "model":          MetadataValue.text(model_name),
                    "location":       MetadataValue.text(location),
                    "task_type":      MetadataValue.text(task_type or "(none)"),
                    "embedding_dim":  MetadataValue.int(len(next((e for e in embeddings if e is not None), [])) if success_count > 0 else 0),
                    "preview":        MetadataValue.md(preview_md),
                },
            )

        return Definitions(assets=[_asset])
