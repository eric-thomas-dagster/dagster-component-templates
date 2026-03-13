import json
import os
from typing import Optional

import dagster as dg
from pydantic import Field


@dg.definitions
class ElasticsearchAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Index data into Elasticsearch or query ES and write results to a database table.

    Supports two modes:

    - ``index`` — accepts a DataFrame from an upstream Dagster asset and bulk-indexes
      the rows as documents into an Elasticsearch index.
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
            "Operation mode. ``index`` bulk-indexes documents from an upstream "
            "Dagster asset DataFrame; ``query`` runs a DSL query and writes hits "
            "to a database table."
        ),
    )

    # --- Index-mode source (upstream asset) -----------------------------------
    upstream_asset_key: str = Field(
        description=(
            "Asset key of the upstream asset providing a pandas DataFrame. "
            "In ``index`` mode the DataFrame rows are indexed as ES documents. "
            "In ``query`` mode this field is still required for lineage but the "
            "upstream value is not used."
        ),
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

    # -------------------------------------------------------------------------
    # build_defs
    # -------------------------------------------------------------------------

    def build_defs(self, load_context: dg.ComponentLoadContext) -> dg.Definitions:
        component = self

        @dg.asset(
            name=component.asset_name,
            group_name=component.group_name,
            kinds={"elasticsearch"},
            ins={"upstream": dg.AssetIn(key=dg.AssetKey.from_user_string(component.upstream_asset_key))},
        )
        def _elasticsearch_asset(
            context: dg.AssetExecutionContext,
            upstream,
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

                df = upstream
                if not isinstance(df, pd.DataFrame):
                    raise ValueError("Upstream asset did not provide a pandas DataFrame.")

                context.log.info(
                    f"[Elasticsearch] Received {len(df):,} rows from upstream asset. "
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
