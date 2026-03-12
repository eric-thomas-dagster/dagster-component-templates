import os
from typing import Optional

import dagster as dg
from pydantic import Field


@dg.definitions
class PineconeAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Upsert, query, or manage a Pinecone vector index as a Dagster asset.

    Loads vector embeddings from an upstream Dagster asset or a relational
    database, then performs the requested operation against a Pinecone
    serverless index.  Designed for RAG (Retrieval-Augmented Generation)
    pipelines where embedding generation and vector storage are separate,
    observable pipeline steps.

    Supported operations:

    - ``upsert`` — batch-upsert vectors from a DataFrame into the index.
    - ``delete`` — delete vectors by ID from a DataFrame column.
    - ``query``  — run a nearest-neighbour query and surface results as metadata.
    - ``create_index`` — provision a new Pinecone serverless index.
    """

    # --- Identity -------------------------------------------------------------
    asset_name: str = Field(description="Dagster asset key for this component.")

    # --- Pinecone connection --------------------------------------------------
    api_key_env_var: str = Field(
        description="Name of the environment variable containing the Pinecone API key."
    )
    index_name: str = Field(description="Name of the Pinecone index to operate on.")

    # --- Operation ------------------------------------------------------------
    operation: str = Field(
        default="upsert",
        description=(
            "Operation to perform. One of: ``upsert``, ``delete``, ``query``, "
            "``create_index``."
        ),
    )

    # --- Vector source — upstream asset ---------------------------------------
    upstream_asset_key: Optional[str] = Field(
        default=None,
        description=(
            "Asset key whose materialized output provides a pandas DataFrame with "
            "vector data. The DataFrame must contain at least the columns referenced "
            "by ``id_column`` and ``vector_column``."
        ),
    )

    # --- Vector source — relational database ----------------------------------
    source_database_url_env_var: Optional[str] = Field(
        default=None,
        description=(
            "Name of the environment variable containing a SQLAlchemy-compatible "
            "database URL (e.g. ``postgresql://user:pass@host/db``). When set, "
            "vectors are read from the database instead of an upstream asset."
        ),
    )
    source_table: Optional[str] = Field(
        default=None,
        description=(
            "Table name to read from when ``source_database_url_env_var`` is set. "
            "Ignored when ``source_query`` is also provided."
        ),
    )
    source_query: Optional[str] = Field(
        default=None,
        description=(
            "SQL SELECT statement used to fetch vectors from the database. "
            "Takes precedence over ``source_table``."
        ),
    )

    # --- Column mapping -------------------------------------------------------
    id_column: str = Field(
        default="id",
        description="DataFrame column whose values become Pinecone vector IDs.",
    )
    vector_column: str = Field(
        default="embedding",
        description=(
            "DataFrame column containing the embedding arrays. Each value must be "
            "a list or numpy array of floats."
        ),
    )
    metadata_columns: Optional[list[str]] = Field(
        default=None,
        description=(
            "Additional DataFrame columns to store as Pinecone vector metadata. "
            "Useful for metadata-filtered similarity searches."
        ),
    )

    # --- Upsert / namespace ---------------------------------------------------
    namespace: Optional[str] = Field(
        default=None,
        description=(
            "Pinecone namespace to upsert into or query within. "
            "When None, the default namespace is used."
        ),
    )
    batch_size: int = Field(
        default=100,
        description=(
            "Number of vectors per upsert batch. Tune this to stay within "
            "Pinecone's per-request size limits (recommended: 100–1 000)."
        ),
    )

    # --- create_index parameters ----------------------------------------------
    dimension: Optional[int] = Field(
        default=None,
        description=(
            "Vector dimensionality for ``create_index``. Must match the output "
            "dimension of your embedding model (e.g. 1 536 for text-embedding-ada-002)."
        ),
    )
    metric: str = Field(
        default="cosine",
        description=(
            "Distance metric for ``create_index``. One of: ``cosine``, "
            "``euclidean``, ``dotproduct``."
        ),
    )
    cloud: str = Field(
        default="aws",
        description="Cloud provider for the serverless index: ``aws``, ``gcp``, ``azure``.",
    )
    region: str = Field(
        default="us-east-1",
        description="Cloud region for the serverless index (e.g. ``us-east-1``).",
    )

    # --- Asset metadata -------------------------------------------------------
    group_name: str = Field(
        default="vector_store",
        description="Dagster asset group name.",
    )
    description: Optional[str] = Field(
        default=None,
        description="Human-readable description surfaced in the Dagster UI.",
    )
    deps: Optional[list[str]] = Field(
        default=None,
        description="Upstream asset keys this asset depends on.",
    )

    # -------------------------------------------------------------------------
    # Helpers
    # -------------------------------------------------------------------------

    def _load_dataframe(self, context: dg.AssetExecutionContext):
        """Return a pandas DataFrame of vectors from the configured source."""
        import pandas as pd  # type: ignore[import]

        if self.source_database_url_env_var:
            db_url = os.environ.get(self.source_database_url_env_var)
            if not db_url:
                raise ValueError(
                    f"Environment variable '{self.source_database_url_env_var}' "
                    "is not set or is empty."
                )
            from sqlalchemy import create_engine, text  # type: ignore[import]

            engine = create_engine(db_url)
            with engine.connect() as conn:
                if self.source_query:
                    context.log.info(f"[Pinecone] Executing query: {self.source_query}")
                    df = pd.read_sql(text(self.source_query), conn)
                elif self.source_table:
                    context.log.info(f"[Pinecone] Reading table: {self.source_table}")
                    df = pd.read_sql_table(self.source_table, conn)
                else:
                    raise ValueError(
                        "Either 'source_query' or 'source_table' must be set when "
                        "'source_database_url_env_var' is provided."
                    )
            context.log.info(f"[Pinecone] Loaded {len(df):,} rows from database.")
            return df

        raise ValueError(
            "No vector source configured. Set 'upstream_asset_key' (pass the "
            "DataFrame as input) or configure 'source_database_url_env_var'."
        )

    @staticmethod
    def _build_vectors(df, id_col: str, vec_col: str, meta_cols: Optional[list[str]]):
        """Convert a DataFrame into a list of Pinecone upsert tuples."""
        vectors = []
        for _, row in df.iterrows():
            vec_id = str(row[id_col])
            values = list(map(float, row[vec_col]))
            metadata = {}
            if meta_cols:
                for col in meta_cols:
                    val = row.get(col)
                    # Pinecone metadata values must be str, int, float, or bool
                    if val is not None:
                        metadata[col] = val if isinstance(val, (str, int, float, bool)) else str(val)
            vectors.append({"id": vec_id, "values": values, "metadata": metadata})
        return vectors

    # -------------------------------------------------------------------------
    # build_defs
    # -------------------------------------------------------------------------

    def build_defs(self, load_context: dg.ComponentLoadContext) -> dg.Definitions:
        component = self  # capture for closure

        # Build explicit asset deps; also include upstream_asset_key if present
        explicit_deps = [dg.AssetKey(d) for d in (component.deps or [])]
        if component.upstream_asset_key:
            upstream_key = dg.AssetKey(component.upstream_asset_key)
            if upstream_key not in explicit_deps:
                explicit_deps.append(upstream_key)

        @dg.asset(
            name=component.asset_name,
            group_name=component.group_name,
            description=component.description,
            deps=explicit_deps,
        )
        def _pinecone_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            from pinecone import Pinecone  # type: ignore[import]

            api_key = os.environ.get(component.api_key_env_var)
            if not api_key:
                raise ValueError(
                    f"Environment variable '{component.api_key_env_var}' "
                    "is not set or is empty."
                )

            pc = Pinecone(api_key=api_key)
            operation = component.operation.lower()

            # ------------------------------------------------------------------
            # create_index
            # ------------------------------------------------------------------
            if operation == "create_index":
                if component.dimension is None:
                    raise ValueError(
                        "'dimension' must be set for the 'create_index' operation."
                    )
                from pinecone import ServerlessSpec  # type: ignore[import]

                context.log.info(
                    f"[Pinecone] Creating index '{component.index_name}' "
                    f"(dim={component.dimension}, metric={component.metric}, "
                    f"cloud={component.cloud}, region={component.region}) ..."
                )
                pc.create_index(
                    name=component.index_name,
                    dimension=component.dimension,
                    metric=component.metric,
                    spec=ServerlessSpec(
                        cloud=component.cloud,
                        region=component.region,
                    ),
                )
                context.log.info(
                    f"[Pinecone] Index '{component.index_name}' created."
                )
                return dg.MaterializeResult(
                    metadata={
                        "index": component.index_name,
                        "operation": "create_index",
                        "dimension": component.dimension,
                        "metric": component.metric,
                        "cloud": component.cloud,
                        "region": component.region,
                    }
                )

            # ------------------------------------------------------------------
            # Operations that require the index to exist
            # ------------------------------------------------------------------
            index = pc.Index(component.index_name)

            # ------------------------------------------------------------------
            # upsert
            # ------------------------------------------------------------------
            if operation == "upsert":
                df = component._load_dataframe(context)
                vectors = component._build_vectors(
                    df,
                    component.id_column,
                    component.vector_column,
                    component.metadata_columns,
                )
                total = len(vectors)
                upserted = 0
                batch_size = component.batch_size

                context.log.info(
                    f"[Pinecone] Upserting {total:,} vectors into index "
                    f"'{component.index_name}'"
                    + (f" (namespace='{component.namespace}')" if component.namespace else "")
                    + f" in batches of {batch_size} ..."
                )

                for i in range(0, total, batch_size):
                    batch = vectors[i : i + batch_size]
                    upsert_kwargs: dict = {"vectors": batch}
                    if component.namespace:
                        upsert_kwargs["namespace"] = component.namespace
                    index.upsert(**upsert_kwargs)
                    upserted += len(batch)
                    context.log.info(
                        f"[Pinecone] Upserted {upserted:,}/{total:,} vectors ..."
                    )

                context.log.info(
                    f"[Pinecone] Upsert complete: {upserted:,} vectors."
                )
                return dg.MaterializeResult(
                    metadata={
                        "index": component.index_name,
                        "operation": "upsert",
                        "vectors_upserted": upserted,
                        "namespace": component.namespace or "(default)",
                        "batch_size": batch_size,
                    }
                )

            # ------------------------------------------------------------------
            # delete
            # ------------------------------------------------------------------
            if operation == "delete":
                df = component._load_dataframe(context)
                ids = [str(v) for v in df[component.id_column].tolist()]
                context.log.info(
                    f"[Pinecone] Deleting {len(ids):,} vectors from index "
                    f"'{component.index_name}' ..."
                )
                delete_kwargs: dict = {"ids": ids}
                if component.namespace:
                    delete_kwargs["namespace"] = component.namespace
                index.delete(**delete_kwargs)
                context.log.info(
                    f"[Pinecone] Deleted {len(ids):,} vectors."
                )
                return dg.MaterializeResult(
                    metadata={
                        "index": component.index_name,
                        "operation": "delete",
                        "vectors_deleted": len(ids),
                        "namespace": component.namespace or "(default)",
                    }
                )

            # ------------------------------------------------------------------
            # query
            # ------------------------------------------------------------------
            if operation == "query":
                df = component._load_dataframe(context)
                # Use first row as query vector
                query_vector = list(map(float, df.iloc[0][component.vector_column]))
                context.log.info(
                    f"[Pinecone] Querying index '{component.index_name}' "
                    f"with vector of dimension {len(query_vector)} ..."
                )
                query_kwargs: dict = {
                    "vector": query_vector,
                    "top_k": 10,
                    "include_metadata": True,
                }
                if component.namespace:
                    query_kwargs["namespace"] = component.namespace
                results = index.query(**query_kwargs)
                matches = results.get("matches", [])
                context.log.info(f"[Pinecone] Query returned {len(matches)} matches.")
                return dg.MaterializeResult(
                    metadata={
                        "index": component.index_name,
                        "operation": "query",
                        "matches_returned": len(matches),
                        "namespace": component.namespace or "(default)",
                        "top_match_id": matches[0]["id"] if matches else None,
                        "top_match_score": matches[0]["score"] if matches else None,
                    }
                )

            raise ValueError(
                f"Unsupported operation '{component.operation}'. "
                "Must be one of: upsert, delete, query, create_index."
            )

        return dg.Definitions(assets=[_pinecone_asset])
