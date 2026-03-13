import json
import os
from typing import Optional

import dagster as dg
from pydantic import Field


@dg.definitions
class ElasticsearchAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Index data into Elasticsearch or query ES and write results to a database table.

    Supports two modes:

    - ``index`` — reads rows from a relational database source table (via SQLAlchemy)
      and bulk-indexes them as documents into an Elasticsearch index.
    - ``query`` — runs a DSL query against an Elasticsearch index and writes the
      matching hits to a relational database table via SQLAlchemy.
    """

    # --- Identity -------------------------------------------------------------
    asset_name: str = Field(description="Dagster asset key for this component.")

    # --- Elasticsearch connection ---------------------------------------------
    elasticsearch_url_env_var: str = Field(
        description=(
            "Name of the environment variable containing the Elasticsearch URL "
            "(e.g. ``http://localhost:9200`` or an Elastic Cloud endpoint)."
        )
    )
    api_key_env_var: Optional[str] = Field(
        default=None,
        description=(
            "Name of the environment variable containing the Elastic Cloud API key. "
            "Leave unset for unauthenticated or username/password-based clusters."
        ),
    )

    # --- Index ----------------------------------------------------------------
    index_name: str = Field(description="Elasticsearch index to operate on.")

    # --- Mode -----------------------------------------------------------------
    mode: str = Field(
        default="index",
        description=(
            "Operation mode. ``index`` bulk-indexes documents from a database source; "
            "``query`` runs a DSL query and writes hits to a database table."
        ),
    )

    # --- Index-mode source (relational DB) ------------------------------------
    source_database_url_env_var: Optional[str] = Field(
        default=None,
        description=(
            "Env var containing a SQLAlchemy-compatible database URL. "
            "Used as the document source in ``index`` mode."
        ),
    )
    source_table_name: Optional[str] = Field(
        default=None,
        description="Source table to read from in ``index`` mode.",
    )
    id_field: Optional[str] = Field(
        default=None,
        description=(
            "Column/field to use as the Elasticsearch document ``_id``. "
            "If omitted, Elasticsearch auto-generates IDs."
        ),
    )
    chunk_size: int = Field(
        default=500,
        description="Number of documents per bulk-index request in ``index`` mode.",
    )

    # --- Query-mode -----------------------------------------------------------
    query: str = Field(
        default="{}",
        description=(
            "JSON string representing the Elasticsearch query DSL body sent in "
            "``query`` mode. Defaults to ``{}`` which Elasticsearch treats as "
            "``match_all``."
        ),
    )
    database_url_env_var: Optional[str] = Field(
        default=None,
        description=(
            "Env var containing a SQLAlchemy-compatible database URL. "
            "Used as the destination in ``query`` mode."
        ),
    )
    table_name: Optional[str] = Field(
        default=None,
        description="Destination table name for ``query`` mode results.",
    )
    if_exists: str = Field(
        default="replace",
        description=(
            "Behaviour when the destination table already exists in ``query`` mode. "
            "One of: ``replace``, ``append``, ``fail``."
        ),
    )

    # --- Asset metadata -------------------------------------------------------
    group_name: str = Field(
        default="elasticsearch",
        description="Dagster asset group name.",
    )
    upstream_asset_key: Optional[str] = Field(
        default=None,
        description=(
            "Asset key of an upstream asset this component depends on. "
            "Automatically added to the asset's ``deps`` for lineage tracking."
        ),
    )
    deps: Optional[list[str]] = Field(
        default=None,
        description="Additional upstream asset keys for lineage.",
    )

    # -------------------------------------------------------------------------
    # build_defs
    # -------------------------------------------------------------------------

    def build_defs(self, load_context: dg.ComponentLoadContext) -> dg.Definitions:
        component = self

        # Collect all explicit dep keys
        dep_keys = [dg.AssetKey.from_user_string(k) for k in (component.deps or [])]
        if component.upstream_asset_key:
            upstream_key = dg.AssetKey.from_user_string(component.upstream_asset_key)
            if upstream_key not in dep_keys:
                dep_keys.append(upstream_key)

        @dg.asset(
            name=component.asset_name,
            group_name=component.group_name,
            kinds={"elasticsearch"},
            deps=dep_keys,
        )
        def _elasticsearch_asset(
            context: dg.AssetExecutionContext,
        ) -> dg.MaterializeResult:
            from elasticsearch import Elasticsearch  # type: ignore[import]
            from elasticsearch.helpers import bulk  # type: ignore[import]

            es_url = os.environ.get(component.elasticsearch_url_env_var)
            if not es_url:
                raise ValueError(
                    f"Environment variable '{component.elasticsearch_url_env_var}' "
                    "is not set or is empty."
                )

            es_kwargs: dict = {"hosts": [es_url]}
            if component.api_key_env_var:
                api_key = os.environ.get(component.api_key_env_var)
                if not api_key:
                    raise ValueError(
                        f"Environment variable '{component.api_key_env_var}' "
                        "is not set or is empty."
                    )
                es_kwargs["api_key"] = api_key

            es = Elasticsearch(**es_kwargs)
            context.log.info(
                f"[Elasticsearch] Connected to {es_url}. Mode: {component.mode}."
            )

            # ------------------------------------------------------------------
            # INDEX mode
            # ------------------------------------------------------------------
            if component.mode == "index":
                import pandas as pd  # type: ignore[import]
                from sqlalchemy import create_engine  # type: ignore[import]

                if not component.source_database_url_env_var:
                    raise ValueError(
                        "'source_database_url_env_var' must be set in 'index' mode."
                    )
                if not component.source_table_name:
                    raise ValueError(
                        "'source_table_name' must be set in 'index' mode."
                    )

                db_url = os.environ.get(component.source_database_url_env_var)
                if not db_url:
                    raise ValueError(
                        f"Environment variable '{component.source_database_url_env_var}' "
                        "is not set or is empty."
                    )

                context.log.info(
                    f"[Elasticsearch] Reading table '{component.source_table_name}' "
                    "from source database ..."
                )
                engine = create_engine(db_url)
                with engine.connect() as conn:
                    df = pd.read_sql_table(component.source_table_name, conn)
                context.log.info(
                    f"[Elasticsearch] Loaded {len(df):,} rows. "
                    f"Indexing into '{component.index_name}' "
                    f"in chunks of {component.chunk_size} ..."
                )

                def _generate_actions(dataframe):
                    for _, row in dataframe.iterrows():
                        doc = row.to_dict()
                        action: dict = {
                            "_index": component.index_name,
                            "_source": doc,
                        }
                        if component.id_field and component.id_field in doc:
                            action["_id"] = str(doc[component.id_field])
                        yield action

                success_count, errors = bulk(
                    es,
                    _generate_actions(df),
                    chunk_size=component.chunk_size,
                    raise_on_error=True,
                )
                context.log.info(
                    f"[Elasticsearch] Indexed {success_count:,} documents into "
                    f"'{component.index_name}'."
                )
                return dg.MaterializeResult(
                    metadata={
                        "index": component.index_name,
                        "mode": "index",
                        "num_docs": success_count,
                        "source_table": component.source_table_name,
                        "chunk_size": component.chunk_size,
                        "id_field": component.id_field or "(auto)",
                    }
                )

            # ------------------------------------------------------------------
            # QUERY mode
            # ------------------------------------------------------------------
            if component.mode == "query":
                import pandas as pd  # type: ignore[import]
                from sqlalchemy import create_engine  # type: ignore[import]

                if not component.database_url_env_var:
                    raise ValueError(
                        "'database_url_env_var' must be set in 'query' mode."
                    )
                if not component.table_name:
                    raise ValueError(
                        "'table_name' must be set in 'query' mode."
                    )

                query_body = json.loads(component.query) if component.query else {}
                if not query_body:
                    query_body = {"query": {"match_all": {}}}
                elif "query" not in query_body:
                    query_body = {"query": query_body}

                context.log.info(
                    f"[Elasticsearch] Querying index '{component.index_name}' ..."
                )
                response = es.search(index=component.index_name, body=query_body, size=10_000)
                hits = response["hits"]["hits"]
                context.log.info(
                    f"[Elasticsearch] Query returned {len(hits):,} hits."
                )

                rows = [hit["_source"] for hit in hits]
                df = pd.DataFrame(rows)

                dest_url = os.environ.get(component.database_url_env_var)
                if not dest_url:
                    raise ValueError(
                        f"Environment variable '{component.database_url_env_var}' "
                        "is not set or is empty."
                    )

                engine = create_engine(dest_url)
                df.to_sql(
                    component.table_name,
                    engine,
                    if_exists=component.if_exists,
                    index=False,
                )
                context.log.info(
                    f"[Elasticsearch] Wrote {len(df):,} rows to table "
                    f"'{component.table_name}'."
                )
                return dg.MaterializeResult(
                    metadata={
                        "index": component.index_name,
                        "mode": "query",
                        "num_docs": len(df),
                        "destination_table": component.table_name,
                        "if_exists": component.if_exists,
                    }
                )

            raise ValueError(
                f"Unsupported mode '{component.mode}'. Must be 'index' or 'query'."
            )

        return dg.Definitions(assets=[_elasticsearch_asset])
