"""ChromaDB Reader Component.

Query a ChromaDB collection for similar documents using text queries or
pre-computed embeddings, returning results as a DataFrame.
"""

from dataclasses import dataclass
from typing import Optional, List, Dict, Any
import pandas as pd
from dagster import (
    AssetExecutionContext,
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


class ChromadbReaderComponent(Component, Model, Resolvable):
    """Component for querying a ChromaDB collection.

    Queries a local or persistent ChromaDB collection using text queries or
    pre-computed embeddings. Returns results as a flattened DataFrame.

    Example:
        ```yaml
        type: dagster_component_templates.ChromadbReaderComponent
        attributes:
          asset_name: chroma_results
          collection_name: documents
          persist_directory: ./chroma_db
          query_texts:
            - "machine learning tutorials"
          n_results: 10
          group_name: sources
        ```
    """

    asset_name: str = Field(description="Name of the asset to create")

    collection_name: str = Field(description="ChromaDB collection to query")

    persist_directory: Optional[str] = Field(
        default=None,
        description="Local path to persistent ChromaDB directory (None = in-memory)"
    )

    query_texts: Optional[List[str]] = Field(
        default=None,
        description="Text queries for similarity search"
    )

    query_embeddings: Optional[List[List[float]]] = Field(
        default=None,
        description="Pre-computed embedding vectors for search"
    )

    n_results: int = Field(
        default=10,
        description="Number of results to return per query"
    )

    where: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Metadata filter (e.g., {\"source\": \"web\"})"
    )

    include: Optional[List[str]] = Field(
        default=None,
        description="What to include in results: ['documents', 'metadatas', 'distances', 'embeddings']"
    )

    deps: Optional[List[str]] = Field(
        default=None,
        description="Upstream asset keys for lineage"
    )

    group_name: Optional[str] = Field(
        default=None,
        description="Asset group for organization"
    )
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

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        collection_name = self.collection_name
        persist_directory = self.persist_directory
        query_texts = self.query_texts
        query_embeddings = self.query_embeddings
        n_results = self.n_results
        where = self.where
        include = self.include
        deps = self.deps
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
            description=f"ChromaDB query on collection {collection_name}",
            partitions_def=partitions_def,
            group_name=group_name,
            deps=[AssetKey.from_user_string(d) for d in (deps or [])],
        )
        def chromadb_reader_asset(context: AssetExecutionContext) -> pd.DataFrame:
            # Log partition key for source components
            if context.has_partition_key:
                context.log.info(f"Running for partition: {context.partition_key}")
            """Query ChromaDB collection for similar documents."""
            try:
                import chromadb
            except ImportError:
                raise ImportError("chromadb is required: pip install chromadb")

            if persist_directory:
                client = chromadb.PersistentClient(path=persist_directory)
            else:
                client = chromadb.Client()

            collection = client.get_collection(collection_name)

            include_fields = include or ["documents", "metadatas", "distances"]

            context.log.info(f"Querying ChromaDB collection '{collection_name}' for top {n_results} results per query")

            results = collection.query(
                query_texts=query_texts,
                query_embeddings=query_embeddings,
                n_results=n_results,
                where=where,
                include=include_fields,
            )

            rows = []
            ids_list = results["ids"]
            docs_list = results.get("documents") or [[]] * len(ids_list)
            metas_list = results.get("metadatas") or [[]] * len(ids_list)
            dists_list = results.get("distances") or [[]] * len(ids_list)

            for i, (ids, docs, metas, dists) in enumerate(zip(ids_list, docs_list, metas_list, dists_list)):
                for id_, doc, meta, dist in zip(
                    ids,
                    docs or [None] * len(ids),
                    metas or [{}] * len(ids),
                    dists or [None] * len(ids),
                ):
                    rows.append({
                        "query_idx": i,
                        "id": id_,
                        "document": doc,
                        "distance": dist,
                        **(meta or {}),
                    })

            df = pd.DataFrame(rows)

            context.log.info(f"Retrieved {len(df)} total results across {len(ids_list)} queries")
            context.add_output_metadata({
                "n_results": MetadataValue.int(len(df)),
                "collection_name": MetadataValue.text(collection_name),
                "n_queries": MetadataValue.int(len(ids_list)),
            })
            return df

        return Definitions(assets=[chromadb_reader_asset])
