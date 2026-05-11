"""BigqueryVectorSearchAssetComponent — k-NN similarity search against a BQ vector column.

Wraps BigQuery's [`VECTOR_SEARCH`](https://cloud.google.com/bigquery/docs/vector-search-intro)
function for similarity queries over an `ARRAY<FLOAT64>` column. Given one or
more query embeddings (passed inline or from an upstream DataFrame), returns
the top-K nearest neighbors per query plus their distance scores.

Common use:
  - **RAG retrieval**: query embedding → top-K relevant docs from BQ vector store
  - **Recommendation**: user-embedding → top-K similar items
  - **Dedup / nearest-match**: incoming row → top-1 existing row in BQ

Two modes:
  - **Static**: pass `query_vectors` directly in YAML (one or more vectors).
  - **From upstream**: pass `upstream_asset_key` + `query_vector_column`,
    one BQ search per row.

Requires an indexed (`CREATE VECTOR INDEX`) column for any non-trivial table
size; queries against unindexed columns work but scan the full table.
"""

import json
import os
from typing import Any, Dict, List, Literal, Optional

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


class BigqueryVectorSearchAssetComponent(Component, Model, Resolvable):
    """Run BigQuery VECTOR_SEARCH against an indexed embedding column."""

    asset_name: str = Field(description="Output asset name.")

    credentials: Optional[Dict[str, Any]] = Field(default=None)
    credentials_path: Optional[str] = Field(default=None)
    project_id: Optional[str] = Field(default=None)
    location: Optional[str] = Field(default=None)

    base_table: str = Field(
        description="Fully-qualified BQ table holding embeddings (`project.dataset.table`).",
    )
    base_column: str = Field(
        description="Column in base_table containing the embedding (ARRAY<FLOAT64>).",
    )
    select_columns: List[str] = Field(
        description="Columns to return alongside each match (id, text, metadata, etc.).",
    )

    top_k: int = Field(default=5, description="Number of neighbors to return per query vector.")
    distance_type: Literal["COSINE", "EUCLIDEAN", "DOT_PRODUCT"] = Field(default="COSINE")

    # Two input modes — exactly one must be set
    query_vectors: Optional[List[List[float]]] = Field(
        default=None,
        description="Static mode: inline list of query vectors. One row of results per vector.",
    )
    upstream_asset_key: Optional[str] = Field(
        default=None,
        description="From-upstream mode: asset key of an upstream DataFrame holding query vectors.",
    )
    query_vector_column: Optional[str] = Field(
        default=None,
        description="From-upstream mode: column in upstream containing the embedding vector.",
    )
    query_id_column: Optional[str] = Field(
        default=None,
        description="Optional column in upstream to carry forward as `query_id` in results.",
    )

    use_brute_force: bool = Field(
        default=False,
        description="If True, force exhaustive scan (no index). Use only for small tables or correctness validation.",
    )
    fraction_lists_to_search: Optional[float] = Field(
        default=None,
        description="IVF index tuning: 0–1, higher = more accurate / slower. Default uses BQ's choice.",
    )

    description: Optional[str] = Field(default=None)
    group_name: Optional[str] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)
    tags: Optional[Dict[str, str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        creds_dict = self.credentials
        if creds_dict is None:
            cred_path = self.credentials_path or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
            if cred_path:
                with open(cred_path, "r") as fh:
                    creds_dict = json.load(fh)
        if creds_dict is None:
            raise ValueError("Provide credentials, credentials_path, or set GOOGLE_APPLICATION_CREDENTIALS.")

        if not self.query_vectors and not self.upstream_asset_key:
            raise ValueError("Set either `query_vectors` (static) or `upstream_asset_key` (from-upstream).")
        if self.query_vectors and self.upstream_asset_key:
            raise ValueError("Set exactly one of `query_vectors` or `upstream_asset_key`, not both.")
        if self.upstream_asset_key and not self.query_vector_column:
            raise ValueError("`query_vector_column` is required when using `upstream_asset_key`.")

        asset_name = self.asset_name
        project_id = self.project_id or creds_dict.get("project_id")
        location = self.location
        base_table = self.base_table
        base_column = self.base_column
        select_columns = self.select_columns
        top_k = self.top_k
        distance_type = self.distance_type
        static_vectors = self.query_vectors
        upstream_key_str = self.upstream_asset_key
        query_vector_column = self.query_vector_column
        query_id_column = self.query_id_column
        use_brute_force = self.use_brute_force
        fraction_lists = self.fraction_lists_to_search

        # Build BQ's `options` JSON-string arg (use_brute_force + fraction_lists_to_search).
        # distance_type and top_k are TOP-LEVEL named args, not part of `options`.
        opt_json_parts = []
        if use_brute_force:
            opt_json_parts.append('"use_brute_force": true')
        if fraction_lists is not None:
            opt_json_parts.append(f'"fraction_lists_to_search": {fraction_lists}')
        options_json = "{" + ", ".join(opt_json_parts) + "}" if opt_json_parts else None

        ins: Dict[str, AssetIn] = {}
        if upstream_key_str:
            ins["upstream"] = AssetIn(key=AssetKey.from_user_string(upstream_key_str))

        @asset(
            name=asset_name,
            description=self.description or f"BQ VECTOR_SEARCH against {base_table}.{base_column} (top-{top_k}, {distance_type}).",
            group_name=self.group_name,
            kinds={"google", "bigquery", "vector-search"},
            tags=self.tags or None,
            owners=self.owners or None,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])] or None,
            ins=ins or None,
        )
        def _asset(context: AssetExecutionContext, **kwargs) -> Output:
            try:
                from google.cloud import bigquery
                from google.oauth2 import service_account
            except ImportError:
                raise ImportError("pip install google-cloud-bigquery google-auth")

            sa_creds = service_account.Credentials.from_service_account_info(creds_dict)
            client = bigquery.Client(project=project_id, credentials=sa_creds, location=location)

            # Assemble (query_id, vector) tuples to run
            queries: List[tuple] = []
            if static_vectors:
                for i, v in enumerate(static_vectors):
                    queries.append((f"q{i}", list(v)))
            else:
                upstream = kwargs["upstream"]
                if query_vector_column not in upstream.columns:
                    raise ValueError(
                        f"query_vector_column={query_vector_column!r} not in upstream: {list(upstream.columns)}"
                    )
                for i, row in upstream.iterrows():
                    qid = str(row[query_id_column]) if query_id_column and query_id_column in upstream.columns else f"q{i}"
                    queries.append((qid, list(row[query_vector_column])))

            context.log.info(f"BQ VECTOR_SEARCH against {base_table}.{base_column} for {len(queries)} query vector(s)")

            select_list = ", ".join(f"base.{c}" for c in select_columns)
            all_results: List[Dict[str, Any]] = []

            for qid, vec in queries:
                vec_literal = "[" + ",".join(f"{x:.6f}" for x in vec) + "]"
                options_arg = f", options => '{options_json}'" if options_json else ""
                sql = f"""
                    SELECT
                      '{qid}' AS query_id,
                      {select_list},
                      distance
                    FROM VECTOR_SEARCH(
                      TABLE `{base_table}`,
                      '{base_column}',
                      (SELECT {vec_literal} AS query_vec),
                      query_column_to_search => 'query_vec',
                      top_k => {top_k},
                      distance_type => '{distance_type}'
                      {options_arg}
                    )
                """
                rows = list(client.query(sql).result())
                for r in rows:
                    all_results.append(dict(r))

            df = pd.DataFrame(all_results)
            preview = df.head(10).to_markdown(index=False) if not df.empty else "(no matches)"
            return Output(
                value=df,
                metadata={
                    "base_table":     MetadataValue.text(base_table),
                    "base_column":    MetadataValue.text(base_column),
                    "distance_type":  MetadataValue.text(distance_type),
                    "top_k":          MetadataValue.int(top_k),
                    "query_count":    MetadataValue.int(len(queries)),
                    "row_count":      MetadataValue.int(len(df)),
                    "preview":        MetadataValue.md(preview or ""),
                },
            )

        return Definitions(assets=[_asset])
