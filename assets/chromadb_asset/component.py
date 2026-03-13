import os
from typing import Optional

import dagster as dg
from pydantic import Field


@dg.definitions
class ChromadbAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Read documents from a relational database, generate embeddings via OpenAI,
    and upsert them into a ChromaDB collection as a Dagster asset.

    Connects to ChromaDB over HTTP (for a remote or Chroma Cloud instance) or
    in-process (when ``chroma_host`` is omitted / set to ``localhost`` with no
    cloud API key).  Uses the OpenAI Embeddings API to vectorise a text column
    from the source table and stores the resulting embeddings — plus optional
    metadata columns — in the named collection.
    """

    # --- Identity -------------------------------------------------------------
    asset_name: str = Field(description="Dagster asset key for this component.")

    # --- ChromaDB connection --------------------------------------------------
    chroma_host: str = Field(
        default="localhost",
        description=(
            "Hostname of the ChromaDB server. "
            "Set to the Chroma Cloud endpoint when using Chroma Cloud."
        ),
    )
    chroma_port: int = Field(
        default=8000,
        description="Port of the ChromaDB HTTP server.",
    )
    chroma_api_key_env_var: Optional[str] = Field(
        default=None,
        description=(
            "Env var containing the Chroma Cloud API key. "
            "When set, an authenticated ``chromadb.HttpClient`` is used; "
            "otherwise the client connects unauthenticated."
        ),
    )
    collection_name: str = Field(
        description="ChromaDB collection to get-or-create and upsert documents into."
    )

    # --- Source database ------------------------------------------------------
    database_url_env_var: str = Field(
        description=(
            "Env var containing a SQLAlchemy-compatible database URL used to read "
            "the source documents (e.g. ``postgresql://user:pass@host/db``)."
        )
    )
    source_table: str = Field(
        description="Source table to read documents from. Accepts ``schema.table`` or ``table``."
    )
    id_column: str = Field(
        description="Primary key column. Values are cast to strings and used as ChromaDB document IDs."
    )
    text_column: str = Field(
        description="Column containing the document text to embed."
    )
    metadata_columns: Optional[list[str]] = Field(
        default=None,
        description=(
            "Additional columns to store as ChromaDB document metadata. "
            "Useful for metadata-filtered similarity searches."
        ),
    )

    # --- Embedding model ------------------------------------------------------
    embedding_model: str = Field(
        default="text-embedding-3-small",
        description="OpenAI embedding model name.",
    )
    openai_api_key_env_var: str = Field(
        default="OPENAI_API_KEY",
        description="Env var containing the OpenAI API key.",
    )
    batch_size: int = Field(
        default=100,
        description="Number of documents sent to the OpenAI API per request.",
    )

    # --- Asset metadata -------------------------------------------------------
    group_name: str = Field(
        default="vector_store",
        description="Dagster asset group name.",
    )
    deps: Optional[list[str]] = Field(
        default=None,
        description="Upstream asset keys for lineage.",
    )

    # -------------------------------------------------------------------------
    # build_defs
    # -------------------------------------------------------------------------

    def build_defs(self, load_context: dg.ComponentLoadContext) -> dg.Definitions:
        component = self

        dep_keys = [dg.AssetKey.from_user_string(k) for k in (component.deps or [])]

        @dg.asset(
            name=component.asset_name,
            group_name=component.group_name,
            kinds={"chromadb", "vector"},
            deps=dep_keys,
        )
        def _chromadb_asset(
            context: dg.AssetExecutionContext,
        ) -> dg.MaterializeResult:
            import chromadb  # type: ignore[import]
            import openai  # type: ignore[import]
            import pandas as pd  # type: ignore[import]
            from sqlalchemy import create_engine  # type: ignore[import]

            # ------------------------------------------------------------------
            # Validate environment
            # ------------------------------------------------------------------
            db_url = os.environ.get(component.database_url_env_var)
            if not db_url:
                raise ValueError(
                    f"Environment variable '{component.database_url_env_var}' "
                    "is not set or is empty."
                )
            openai_api_key = os.environ.get(component.openai_api_key_env_var)
            if not openai_api_key:
                raise ValueError(
                    f"Environment variable '{component.openai_api_key_env_var}' "
                    "is not set or is empty."
                )

            # ------------------------------------------------------------------
            # Connect to ChromaDB
            # ------------------------------------------------------------------
            if component.chroma_api_key_env_var:
                chroma_api_key = os.environ.get(component.chroma_api_key_env_var)
                if not chroma_api_key:
                    raise ValueError(
                        f"Environment variable '{component.chroma_api_key_env_var}' "
                        "is not set or is empty."
                    )
                context.log.info(
                    f"[ChromaDB] Connecting to {component.chroma_host}:{component.chroma_port} "
                    "(authenticated) ..."
                )
                chroma_client = chromadb.HttpClient(
                    host=component.chroma_host,
                    port=component.chroma_port,
                    headers={"Authorization": f"Bearer {chroma_api_key}"},
                )
            else:
                context.log.info(
                    f"[ChromaDB] Connecting to {component.chroma_host}:{component.chroma_port} ..."
                )
                chroma_client = chromadb.HttpClient(
                    host=component.chroma_host,
                    port=component.chroma_port,
                )

            collection = chroma_client.get_or_create_collection(
                name=component.collection_name
            )
            context.log.info(
                f"[ChromaDB] Using collection '{component.collection_name}'."
            )

            # ------------------------------------------------------------------
            # Read source rows
            # ------------------------------------------------------------------
            context.log.info(
                f"[ChromaDB] Reading from '{component.source_table}' ..."
            )
            engine = create_engine(db_url)
            with engine.connect() as conn:
                table_name = component.source_table.split(".")[-1]
                schema = (
                    component.source_table.split(".")[0]
                    if "." in component.source_table
                    else None
                )
                df = pd.read_sql_table(table_name, conn, schema=schema)
            context.log.info(f"[ChromaDB] Loaded {len(df):,} rows.")

            if df.empty:
                context.log.warning("[ChromaDB] Source table is empty — nothing to embed.")
                return dg.MaterializeResult(
                    metadata={
                        "num_documents": 0,
                        "collection": component.collection_name,
                    }
                )

            ids = [str(v) for v in df[component.id_column].tolist()]
            texts = df[component.text_column].fillna("").tolist()

            # Build metadata list (one dict per row)
            meta_cols = component.metadata_columns or []
            metadatas: list[dict] = []
            for _, row in df.iterrows():
                meta: dict = {}
                for col in meta_cols:
                    val = row.get(col)
                    if val is not None:
                        # ChromaDB metadata values must be str, int, float, or bool
                        meta[col] = (
                            val
                            if isinstance(val, (str, int, float, bool))
                            else str(val)
                        )
                metadatas.append(meta)

            # ------------------------------------------------------------------
            # Generate embeddings in batches
            # ------------------------------------------------------------------
            openai_client = openai.OpenAI(api_key=openai_api_key)
            context.log.info(
                f"[ChromaDB] Generating embeddings with model "
                f"'{component.embedding_model}' in batches of {component.batch_size} ..."
            )
            all_embeddings: list[list[float]] = []
            for i in range(0, len(texts), component.batch_size):
                batch_texts = texts[i : i + component.batch_size]
                response = openai_client.embeddings.create(
                    input=batch_texts,
                    model=component.embedding_model,
                )
                batch_embeddings = [item.embedding for item in response.data]
                all_embeddings.extend(batch_embeddings)
                context.log.info(
                    f"[ChromaDB] Embedded {min(i + component.batch_size, len(texts)):,}"
                    f"/{len(texts):,} documents ..."
                )

            # ------------------------------------------------------------------
            # Upsert into ChromaDB collection
            # ------------------------------------------------------------------
            context.log.info(
                f"[ChromaDB] Upserting {len(all_embeddings):,} documents into "
                f"collection '{component.collection_name}' in batches of "
                f"{component.batch_size} ..."
            )
            for i in range(0, len(ids), component.batch_size):
                batch_ids = ids[i : i + component.batch_size]
                batch_embeddings = all_embeddings[i : i + component.batch_size]
                batch_documents = texts[i : i + component.batch_size]
                batch_metadatas = metadatas[i : i + component.batch_size]

                upsert_kwargs: dict = {
                    "ids": batch_ids,
                    "embeddings": batch_embeddings,
                    "documents": batch_documents,
                }
                if any(batch_metadatas):
                    upsert_kwargs["metadatas"] = batch_metadatas

                collection.upsert(**upsert_kwargs)
                context.log.info(
                    f"[ChromaDB] Upserted batch {i // component.batch_size + 1} "
                    f"({len(batch_ids)} docs) ..."
                )

            total = len(ids)
            context.log.info(
                f"[ChromaDB] Upsert complete: {total:,} documents in "
                f"'{component.collection_name}'."
            )
            return dg.MaterializeResult(
                metadata={
                    "num_documents": total,
                    "collection": component.collection_name,
                    "embedding_model": component.embedding_model,
                    "source_table": component.source_table,
                    "metadata_columns": meta_cols or [],
                }
            )

        return dg.Definitions(assets=[_chromadb_asset])
