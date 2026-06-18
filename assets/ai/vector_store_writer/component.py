"""Vector Store Writer Asset Component.

Write embeddings to vector databases (Pinecone, Weaviate, Chroma, FAISS, Qdrant).
Supports batch writing and metadata attachment.
"""

import os
from typing import Any, Dict, List, Optional, Union
import pandas as pd
import numpy as np

from dagster import (
    AssetIn,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    AssetExecutionContext,
    asset,
    Resolvable,
    Model,
    MetadataValue,
)
from pydantic import Field


def _build_partitions_def(
    partition_type,
    partition_start,
    partition_values,
    dynamic_partition_name,
    partition_dimensions,
):
    """Construct a Dagster partitions_def from the canonical partition fields.

    Strict: raises ValueError on misconfigured combinations rather than
    silently picking a default. Specifically:
      - time-based partition_type without partition_start
      - partition_type=multi without partition_values
      - partition_type=dynamic without dynamic_partition_name
      - both partition_dimensions AND flat fields set (ambiguous intent)
    """
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )

    # Both shapes set: ambiguous. Pick one.
    if partition_dimensions and partition_type:
        raise ValueError(
            "Set either partition_type (flat-fields shape) or "
            "partition_dimensions (multi-axis shape), not both."
        )

    def _build_axis(spec):
        t = spec.get("type")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily":
            return DailyPartitionsDefinition(start_date=spec["start"])
        if t == "weekly":
            return WeeklyPartitionsDefinition(start_date=spec["start"])
        if t == "monthly":
            return MonthlyPartitionsDefinition(start_date=spec["start"])
        if t == "hourly":
            return HourlyPartitionsDefinition(start_date=spec["start"])
        if t == "static":
            vals = spec.get("values") or []
            if isinstance(vals, str):
                vals = [v.strip() for v in vals.split(",") if v.strip()]
            if not vals:
                raise ValueError("partition dimension type='static' requires non-empty 'values'")
            return StaticPartitionsDefinition(list(vals))
        if t == "dynamic":
            name = spec.get("dynamic_partition_name") or spec.get("name")
            if not name:
                raise ValueError("partition dimension type='dynamic' requires a name")
            return DynamicPartitionsDefinition(name=name)
        raise ValueError(f"unknown partition type: {t!r}")

    if partition_dimensions:
        if len(partition_dimensions) == 1:
            return _build_axis(partition_dimensions[0])
        axes = {d["name"]: _build_axis(d) for d in partition_dimensions}
        return MultiPartitionsDefinition(axes)

    if not partition_type:
        return None
    if isinstance(partition_values, (list, tuple)):
        _values = [str(v).strip() for v in partition_values if str(v).strip()]
    else:
        _values = [v.strip() for v in (str(partition_values) if partition_values else "").split(",") if v.strip()]
    if partition_type in ("daily", "weekly", "monthly", "hourly") and not partition_start:
        raise ValueError(
            f"partition_type={partition_type!r} requires partition_start (ISO date, e.g. '2024-01-01')."
        )
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "static":
        if not _values:
            raise ValueError("partition_type='static' requires partition_values (comma-separated).")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError(
                "partition_type='dynamic' requires dynamic_partition_name."
            )
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    if partition_type == "multi":
        if not _values:
            raise ValueError("partition_type='multi' requires partition_values (comma-separated).")
        if not partition_start:
            raise ValueError("partition_type='multi' requires partition_start (the date axis start).")
        return MultiPartitionsDefinition({
            "date": DailyPartitionsDefinition(start_date=partition_start),
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")


class VectorStoreWriterComponent(Component, Model, Resolvable):
    """Component for writing embeddings to vector stores.

    Accepts embeddings from upstream assets via IO manager.

    Example:
        ```yaml
        type: dagster_component_templates.VectorStoreWriterComponent
        attributes:
          asset_name: vector_store_index
          provider: chromadb
          collection_name: documents
          api_key: ${PINECONE_API_KEY}
        ```
    """

    asset_name: str = Field(description="Name of the asset")
    provider: str = Field(description="Provider: 'pinecone', 'weaviate', 'chromadb', 'faiss', 'qdrant'")
    collection_name: str = Field(description="Collection/index name")
    embedding_column: Union[str, int] = Field(default="embedding", description="Column with embeddings")
    text_column: Union[str, int] = Field(default="text", description="Column with text")
    metadata_columns: Optional[List[str]] = Field(default=None, description="Additional metadata columns")
    connection_string: Optional[str] = Field(default=None, description="Connection string or path")
    api_key: Optional[str] = Field(default=None, description="API key (use ${VAR_NAME} for environment variable)")
    batch_size: int = Field(default=100, description="Batch size")
    upsert: bool = Field(default=True, description="Upsert if exists")
    description: Optional[str] = Field(default=None, description="Asset description")
    group_name: Optional[str] = Field(default=None, description="Asset group")
    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', or None for unpartitioned",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.",
    )
    partition_date_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter upstream DataFrame to the current date partition key.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition (when partition_type='dynamic'), e.g. 'tenants'.",
    )

    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Multi-axis partition spec: list of {name, type, start, values, dynamic_partition_name} dicts. Overrides flat fields when set.",
    )

    partition_values: Optional[str] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning, e.g. 'customer_a,customer_b,customer_c'.",
    )
    partition_static_dim: Optional[str] = Field(
        default=None,
        description="Dimension name for the static axis in multi-partitioning, e.g. 'customer' or 'region'.",
    )
    partition_static_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter upstream DataFrame to the current static partition dimension (e.g. 'customer_id').",
    )
    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners — list of team names or email addresses, e.g. ['team:analytics', 'user@company.com']",
    )
    asset_tags: Optional[Dict[str, str]] = Field(
        default=None,
        description="Additional key-value tags to apply to the asset, e.g. {'domain': 'finance', 'tier': 'gold'}",
    )
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds for the Dagster catalog, e.g. ['snowflake', 'python']. Auto-inferred from component name if not set.",
    )
    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale. Defines a FreshnessPolicy.",
    )
    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5' (weekdays at 9am).",
    )
    column_lineage: Optional[Dict[str, List[str]]] = Field(
        default=None,
        description="Column-level lineage mapping: output column name → list of upstream column names it was derived from, e.g. {'revenue': ['price', 'quantity']}",
    )
    upstream_asset_key: str = Field(
        description="Upstream asset key providing a DataFrame with embeddings to write"
    )

    retry_policy_max_retries: Optional[int] = Field(

        default=None,

        description="Max retries on asset failure. Defines a RetryPolicy. Useful for transient network failures, rate limits, etc.",

    )

    retry_policy_delay_seconds: Optional[int] = Field(

        default=None,

        description="Seconds between retries (default 1).",

    )

    retry_policy_backoff: str = Field(

        default="exponential",

        description="Backoff strategy: 'linear' or 'exponential'.",

    )



    deps: Optional[List[str]] = Field(
        default=None,
        description="Lineage-only upstream asset keys (no data passed at runtime).",
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        provider = self.provider
        collection_name = self.collection_name
        embedding_column = self.embedding_column
        text_column = self.text_column
        metadata_columns = self.metadata_columns or []
        connection_string = self.connection_string
        api_key = self.api_key
        batch_size = self.batch_size
        upsert = self.upsert
        description = self.description or f"Write embeddings to {provider}/{collection_name}"
        group_name = self.group_name
        upstream_asset_key = self.upstream_asset_key

        partitions_def = _build_partitions_def(
            self.partition_type,
            self.partition_start,
            self.partition_values,
            self.dynamic_partition_name,
            self.partition_dimensions,
        )
        partition_type = self.partition_type
        partition_date_column = self.partition_date_column
        partition_static_column = self.partition_static_column
        partition_static_dim = self.partition_static_dim

        # Infer kinds from component name if not explicitly set
        _comp_name = "vector_store_writer"  # component directory name
        _kind_map = {
            "snowflake": "snowflake", "bigquery": "bigquery", "redshift": "redshift",
            "postgres": "postgres", "postgresql": "postgres", "mysql": "mysql",
            "s3": "s3", "adls": "azure", "azure": "azure", "gcs": "gcp",
            "google": "gcp", "databricks": "databricks", "dbt": "dbt",
            "kafka": "kafka", "mongodb": "mongodb", "redis": "redis",
            "neo4j": "neo4j", "elasticsearch": "elasticsearch", "pinecone": "pinecone",
            "chromadb": "chromadb", "pgvector": "postgres",
        }
        _inferred_kinds = self.kinds or []
        if not _inferred_kinds:
            _comp_lower = asset_name.lower()
            for keyword, kind in _kind_map.items():
                if keyword in _comp_lower:
                    _inferred_kinds.append(kind)
            if not _inferred_kinds:
                _inferred_kinds = ["python"]

        # Build combined tags: user tags + kind tags
        _all_tags = dict(self.asset_tags or {})
        for _kind in _inferred_kinds:
            _all_tags[f"dagster/kind/{_kind}"] = ""

        # Build freshness policy
        _freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy
            _freshness_policy = FreshnessPolicy(
                maximum_lag_minutes=self.freshness_max_lag_minutes,
                cron_schedule=self.freshness_cron,
            )

        owners = self.owners or []
        column_lineage = self.column_lineage if hasattr(self, 'column_lineage') else None


        # Build retry policy (auto-generated; opt-in via retry_policy_max_retries).


        _retry_policy = None


        if self.retry_policy_max_retries is not None:


            from dagster import Backoff, RetryPolicy


            _retry_policy = RetryPolicy(


                max_retries=self.retry_policy_max_retries,


                delay=self.retry_policy_delay_seconds or 1,


                backoff=Backoff[self.retry_policy_backoff.upper()],


            )



        @asset(retry_policy=_retry_policy,
            key=AssetKey.from_user_string(asset_name),
            description=description,
            partitions_def=partitions_def,
                        owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
group_name=group_name,
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
        )
        def vector_store_writer_asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> Dict[str, Any]:
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
            """Write embeddings to vector store from upstream DataFrame."""

            df = upstream

            if embedding_column not in df.columns:
                raise ValueError(
                    f"Embedding column '{embedding_column}' not found in upstream DataFrame. "
                    f"Available: {list(df.columns)}"
                )

            context.log.info(f"Received DataFrame with {len(df)} rows")

            # Expand environment variables in API key (supports ${VAR_NAME} pattern)
            expanded_api_key = None
            if api_key:
                expanded_api_key = os.path.expandvars(api_key)
                if expanded_api_key == api_key and api_key.startswith('${'):
                    var_name = api_key.strip('${}')
                    context.log.warning(f"Environment variable not set: {var_name}")
                    # Don't fail - some providers don't need API keys

            context.log.info(f"Writing {len(df)} embeddings to {provider}")

            if provider == "chromadb":
                import chromadb
                client = chromadb.PersistentClient(path=connection_string or "./chroma_db")
                collection = client.get_or_create_collection(name=collection_name)

                embeddings = df[embedding_column].tolist()
                texts = df[text_column].tolist() if text_column in df.columns else None
                ids = [f"doc_{i}" for i in range(len(df))]
                metadatas = df[metadata_columns].to_dict('records') if metadata_columns else None

                for i in range(0, len(df), batch_size):
                    batch_end = min(i + batch_size, len(df))
                    collection.upsert(
                        embeddings=embeddings[i:batch_end],
                        documents=texts[i:batch_end] if texts else None,
                        ids=ids[i:batch_end],
                        metadatas=metadatas[i:batch_end] if metadatas else None
                    )

            elif provider == "pinecone":
                from pinecone import Pinecone
                pc = Pinecone(api_key=expanded_api_key)
                index = pc.Index(collection_name)

                vectors = []
                for idx, row in df.iterrows():
                    metadata = {text_column: row[text_column]} if text_column in df.columns else {}
                    for col in metadata_columns:
                        if col in df.columns:
                            metadata[col] = row[col]
                    vectors.append({
                        "id": f"doc_{idx}",
                        "values": row[embedding_column],
                        "metadata": metadata
                    })

                for i in range(0, len(vectors), batch_size):
                    index.upsert(vectors=vectors[i:i + batch_size])

            elif provider == "qdrant":
                from qdrant_client import QdrantClient
                from qdrant_client.models import Distance, VectorParams, PointStruct

                client = QdrantClient(url=connection_string or "localhost", api_key=expanded_api_key)

                try:
                    client.get_collection(collection_name)
                except:
                    embedding_dim = len(df[embedding_column].iloc[0])
                    client.create_collection(
                        collection_name=collection_name,
                        vectors_config=VectorParams(size=embedding_dim, distance=Distance.COSINE)
                    )

                points = []
                for idx, row in df.iterrows():
                    payload = {text_column: row[text_column]} if text_column in df.columns else {}
                    for col in metadata_columns:
                        if col in df.columns:
                            payload[col] = row[col]
                    points.append(PointStruct(id=idx, vector=row[embedding_column], payload=payload))

                for i in range(0, len(points), batch_size):
                    client.upsert(collection_name=collection_name, points=points[i:i + batch_size])

            elif provider == "faiss":
                import faiss
                import pickle

                embeddings = np.array(df[embedding_column].tolist()).astype('float32')
                dimension = embeddings.shape[1]

                # Create or load index
                index_path = connection_string or f"./{collection_name}.faiss"
                metadata_path = index_path.replace(".faiss", "_metadata.pkl")

                if os.path.exists(index_path) and not upsert:
                    index = faiss.read_index(index_path)
                else:
                    index = faiss.IndexFlatL2(dimension)

                index.add(embeddings)

                # Save metadata
                metadata = df[[text_column] + metadata_columns].to_dict('records') if text_column in df.columns else []
                with open(metadata_path, 'wb') as f:
                    pickle.dump(metadata, f)

                faiss.write_index(index, index_path)

            else:
                raise ValueError(f"Unsupported provider: {provider}")

            context.log.info(f"Successfully wrote {len(df)} vectors")

            # Build column schema metadata
            from dagster import TableSchema, TableColumn, TableColumnLineage, TableColumnDep
            _col_schema = TableSchema(columns=[
                TableColumn(name=str(col), type=str(df.dtypes[col]))
                for col in df.columns
            ])
            _metadata = {
                "dagster/row_count": MetadataValue.int(len(df)),
                "dagster/column_schema": MetadataValue.table_schema(_col_schema),
            }
            # Use explicit lineage, or auto-infer passthrough columns at runtime
            _effective_lineage = column_lineage
            if not _effective_lineage:
                try:
                    _upstream_cols = set(upstream.columns)
                    _effective_lineage = {
                        col: [col] for col in _col_schema.columns_by_name
                        if col in _upstream_cols
                    }
                except Exception:
                    pass
            if _effective_lineage:
                _upstream_key = AssetKey.from_user_string(upstream_asset_key) if upstream_asset_key else None
                if _upstream_key:
                    _lineage_deps = {}
                    for out_col, in_cols in _effective_lineage.items():
                        _lineage_deps[out_col] = [
                            TableColumnDep(asset_key=_upstream_key, column_name=ic)
                            for ic in in_cols
                        ]
                    _metadata["dagster/column_lineage"] = MetadataValue.column_lineage(
                        TableColumnLineage(_lineage_deps)
                    )
            context.add_output_metadata(_metadata)

            return {"status": "success", "num_vectors": len(df)}

        from dagster import build_column_schema_change_checks


        _schema_checks = build_column_schema_change_checks(assets=[vector_store_writer_asset])


        return Definitions(assets=[vector_store_writer_asset], asset_checks=list(_schema_checks))
