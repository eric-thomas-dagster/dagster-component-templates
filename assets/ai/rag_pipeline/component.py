"""RAG Pipeline Asset Component.

Complete Retrieval-Augmented Generation pipeline combining query, retrieval, and generation.
"""

import os
from typing import Dict, List, Optional

import pandas as pd

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
)
from pydantic import Field


class RAGPipelineComponent(Component, Model, Resolvable):
    """Component for RAG pipeline.

    Accepts query from upstream assets via IO manager.

    Example:
        ```yaml
        type: dagster_component_templates.RAGPipelineComponent
        attributes:
          asset_name: rag_response
          vector_store_provider: chromadb
          collection_name: documents
          llm_provider: openai
          llm_model: gpt-4
          llm_api_key: ${OPENAI_API_KEY}
          embedding_api_key: ${OPENAI_API_KEY}
        ```
    """

    asset_name: str = Field(description="Name of the asset")
    vector_store_provider: str = Field(description="Vector store provider")
    collection_name: str = Field(description="Collection name")
    llm_provider: str = Field(description="LLM provider")
    llm_model: str = Field(description="LLM model")
    embedding_provider: str = Field(default="openai", description="Embedding provider")
    embedding_model: str = Field(default="text-embedding-3-small", description="Embedding model")
    top_k: int = Field(default=5, description="Number of documents to retrieve")
    vector_store_connection: Optional[str] = Field(default=None, description="Vector store connection")
    llm_api_key: Optional[str] = Field(default=None, description="LLM API key with ${VAR_NAME} syntax")
    embedding_api_key: Optional[str] = Field(default=None, description="Embedding API key with ${VAR_NAME} syntax")
    include_sources: bool = Field(default=True, description="Include source documents")
    temperature: float = Field(default=0.7, description="LLM temperature")
    query_column: str = Field(default="query", description="Column name containing query text")
    answer_column: str = Field(default="answer", description="Column name for generated answers")
    sources_column: str = Field(default="sources", description="Column name for retrieved source documents")
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
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame with query text")

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        vector_store_provider = self.vector_store_provider
        collection_name = self.collection_name
        llm_provider = self.llm_provider
        llm_model = self.llm_model
        embedding_provider = self.embedding_provider
        embedding_model = self.embedding_model
        top_k = self.top_k
        vector_store_connection = self.vector_store_connection
        llm_api_key = self.llm_api_key
        embedding_api_key = self.embedding_api_key
        include_sources = self.include_sources
        temperature = self.temperature
        query_column = self.query_column
        answer_column = self.answer_column
        sources_column = self.sources_column
        description = self.description or "RAG pipeline"
        group_name = self.group_name
        upstream_asset_key = self.upstream_asset_key

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

        # Infer kinds from component name if not explicitly set
        _comp_name = "rag_pipeline"  # component directory name
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


        @asset(
            name=asset_name,
            description=description,
            partitions_def=partitions_def,
                        owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
group_name=group_name,
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
        )
        def rag_pipeline_asset(ctx: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
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
            """Execute RAG pipeline for each query row in upstream DataFrame.

            This component orchestrates a complete RAG pipeline per row:
            1. Generate query embeddings
            2. Retrieve relevant documents from vector store
            3. Generate response using LLM with retrieved context
            """

            df = upstream.copy()

            if query_column not in df.columns:
                raise ValueError(f"Query column '{query_column}' not found. Available: {list(df.columns)}")

            ctx.log.info(f"Running RAG pipeline on {len(df)} queries")

            # Expand environment variables in API keys
            expanded_llm_api_key = None
            if llm_api_key:
                expanded_llm_api_key = os.path.expandvars(llm_api_key)
                if expanded_llm_api_key == llm_api_key and '${' in llm_api_key:
                    raise ValueError(f"Environment variable in llm_api_key '{llm_api_key}' is not set")

            expanded_embedding_api_key = None
            if embedding_api_key:
                expanded_embedding_api_key = os.path.expandvars(embedding_api_key)
                if expanded_embedding_api_key == embedding_api_key and '${' in embedding_api_key:
                    raise ValueError(f"Environment variable in embedding_api_key '{embedding_api_key}' is not set")

            def get_query_embedding(query: str) -> list:
                if embedding_provider == "openai":
                    import openai
                    client = openai.OpenAI(api_key=expanded_embedding_api_key)
                    response = client.embeddings.create(model=embedding_model, input=[query])
                    return response.data[0].embedding
                elif embedding_provider == "sentence_transformers":
                    from sentence_transformers import SentenceTransformer
                    st_model = SentenceTransformer(embedding_model)
                    return st_model.encode([query])[0].tolist()
                else:
                    raise ValueError(f"Unsupported embedding provider: {embedding_provider}")

            def retrieve_docs(query_embedding: list) -> list:
                retrieved = []
                if vector_store_provider == "chromadb":
                    import chromadb
                    client = chromadb.PersistentClient(path=vector_store_connection or "./chroma_db")
                    collection = client.get_collection(name=collection_name)
                    results = collection.query(query_embeddings=[query_embedding], n_results=top_k)
                    for i in range(len(results['ids'][0])):
                        retrieved.append({
                            "text": results['documents'][0][i] if 'documents' in results else "",
                            "metadata": results['metadatas'][0][i] if 'metadatas' in results else {}
                        })
                elif vector_store_provider == "pinecone":
                    from pinecone import Pinecone
                    pc = Pinecone(api_key=expanded_embedding_api_key)
                    index = pc.Index(collection_name)
                    results = index.query(vector=query_embedding, top_k=top_k, include_metadata=True)
                    for match in results['matches']:
                        retrieved.append({
                            "text": match.get('metadata', {}).get('text', ''),
                            "metadata": match.get('metadata', {})
                        })
                else:
                    raise ValueError(f"Unsupported vector store provider: {vector_store_provider}")
                return retrieved

            def generate_answer(query: str, retrieved_docs: list) -> str:
                context_text = "\n\n".join([doc["text"] for doc in retrieved_docs])
                prompt = f"""Answer the following question based on the provided context.

Context:
{context_text}

Question: {query}

Answer:"""
                if llm_provider == "openai":
                    import openai
                    client = openai.OpenAI(api_key=expanded_llm_api_key)
                    response = client.chat.completions.create(
                        model=llm_model,
                        messages=[{"role": "user", "content": prompt}],
                        temperature=temperature
                    )
                    return response.choices[0].message.content
                elif llm_provider == "anthropic":
                    import anthropic
                    client = anthropic.Anthropic(api_key=expanded_llm_api_key)
                    message = client.messages.create(
                        model=llm_model,
                        max_tokens=4096,
                        temperature=temperature,
                        messages=[{"role": "user", "content": prompt}]
                    )
                    return message.content[0].text
                else:
                    raise ValueError(f"Unsupported LLM provider: {llm_provider}")

            answers = []
            sources_list = []

            for idx, row in df.iterrows():
                query = str(row[query_column])
                ctx.log.info(f"Processing query {idx + 1}/{len(df)}: {query[:80]}...")

                query_embedding = get_query_embedding(query)
                retrieved_docs = retrieve_docs(query_embedding)
                answer = generate_answer(query, retrieved_docs)

                answers.append(answer)
                sources_list.append(retrieved_docs if include_sources else [])

            df[answer_column] = answers
            if include_sources:
                df[sources_column] = sources_list

            ctx.log.info(f"RAG pipeline complete: {len(df)} queries processed")
            ctx.add_output_metadata({
                "rows_processed": len(df),
                "vector_store_provider": vector_store_provider,
                "llm_provider": llm_provider,
                "top_k": top_k,
            })

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
            if column_lineage:
                _upstream_key = AssetKey.from_user_string(upstream_asset_key) if upstream_asset_key else None
                if _upstream_key:
                    _lineage_deps = {}
                    for out_col, in_cols in column_lineage.items():
                        _lineage_deps[out_col] = [
                            TableColumnDep(asset_key=_upstream_key, column_name=ic)
                            for ic in in_cols
                        ]
                    _metadata["dagster/column_lineage"] = MetadataValue.table_column_lineage(
                        TableColumnLineage(_lineage_deps)
                    )
            context.add_output_metadata(_metadata)

            return df

        from dagster import build_column_schema_change_checks


        _schema_checks = build_column_schema_change_checks(assets=[rag_pipeline_asset])


        return Definitions(assets=[rag_pipeline_asset], asset_checks=list(_schema_checks))
