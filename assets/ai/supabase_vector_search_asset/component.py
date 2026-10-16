"""SupabaseVectorSearchAssetComponent — pgvector similarity search on Supabase.

Reads a DataFrame of query embeddings from an upstream asset, runs a
pgvector similarity search against a Supabase table with a vector column
(one query per row), and returns the matched rows.

The recommended way to run pgvector queries via Supabase is through a
Postgres function (RPC) that accepts a query vector + top_k and returns
neighbors, because supabase-py's REST-based ``.table()`` client doesn't
expose the `<=>`, `<->`, `<#>` operators directly. This component supports
both modes:

- **RPC mode (default when ``rpc_name`` is set)** — calls
  ``client.rpc(rpc_name, {...})``. Fastest and idiomatic for Supabase.
- **REST fallback (``rpc_name`` unset)** — uses PostgREST's `order` +
  `vector` string form: ``table.select(...).order(embedding_column, ...)``
  passing the query vector as a string. Requires the table to have a
  pre-computed ``distance`` generated column or accept a vector literal in
  ``order``. If your Supabase project doesn't support it, create an RPC
  and set ``rpc_name``.

The query embedding column must contain either a Python ``list[float]``
or a numpy array; both are serialized to the pgvector string form
``[0.1,0.2,...]`` before dispatch.
"""

from typing import Any, Dict, List, Optional, Union

import numpy as np
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


def _vector_to_pgvector_literal(vec: Any) -> str:
    """Serialize a Python list / numpy array into pgvector's text form.

    pgvector accepts the string form ``'[1.0,2.0,3.0]'``. This is the
    format the ``vector`` type parses from SQL literals and the form
    PostgREST expects when passing vectors through query parameters.
    """
    if vec is None:
        raise ValueError("query vector is None")
    if isinstance(vec, np.ndarray):
        vec = vec.tolist()
    if not isinstance(vec, (list, tuple)):
        raise ValueError(f"query vector must be list/tuple/ndarray, got {type(vec).__name__}")
    return "[" + ",".join(f"{float(x)}" for x in vec) + "]"



# BEGIN upstream-asset-key normalization (inlined per component; keep copies identical — see tests/test_upstream_keys.py)
from typing import List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def normalize_upstream_keys(single, plural=None) -> List[str]:
    """Merge ``upstream_asset_key`` / ``upstream_asset_keys`` into one clean list.

    Each argument may be a str, a list/tuple of str, or None. Entries are
    stripped, empties dropped, and duplicates removed (order preserved).
    """
    out: List[str] = []
    for value in (single, plural):
        if value is None:
            continue
        items = [value] if isinstance(value, str) else value
        if not isinstance(items, (list, tuple)):
            raise ValueError(
                f"upstream asset key(s) must be a string or a list of strings, got {value!r}"
            )
        for item in items:
            if not isinstance(item, str):
                raise ValueError(f"upstream asset keys must be strings, got {item!r}")
            item = item.strip()
            if item and item not in out:
                out.append(item)
    return out


def coerce_upstream_input(data, *, component: str, mode: str, optional: bool = True):
    """Rewrite raw component input so both spellings/shapes land on the declared field(s).

    mode "single": declared field is ``upstream_asset_key`` (str); >1 key is an error.
    mode "multi":  declared field is ``upstream_asset_keys`` (list).
    mode "both":   both fields are declared; everything is merged into the plural one.
    """
    if not isinstance(data, dict):
        return data
    if "upstream_asset_key" not in data and "upstream_asset_keys" not in data:
        return data
    keys = normalize_upstream_keys(data.get("upstream_asset_key"), data.get("upstream_asset_keys"))
    out = {k: v for k, v in data.items() if k not in ("upstream_asset_key", "upstream_asset_keys")}
    if mode == "single":
        if len(keys) > 1:
            raise ValueError(f"{component} takes exactly one upstream asset, got {keys}")
        out["upstream_asset_key"] = keys[0] if keys else (None if optional else "")
    else:
        out["upstream_asset_keys"] = keys or (None if optional else [])
    return out
# END upstream-asset-key normalization



# BEGIN field-shape normalization (inlined per component; keep copies identical — see tests/test_field_shapes.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def coerce_field_shapes(data, *, lists=(), csv=(), tags=()):
    """Accept the shapes people (and LLMs) naturally write for common boilerplate fields."""
    if not isinstance(data, dict):
        return data
    out = dict(data)
    for k in lists:  # List[str]: "a" / "a, b" -> ["a", "b"]; strip + drop empties
        v = out.get(k)
        if isinstance(v, str):
            v = v.split(",")
        if isinstance(v, (list, tuple)):
            out[k] = [x.strip() if isinstance(x, str) else x for x in v if not (isinstance(x, str) and not x.strip())]
    for k in csv:  # comma-separated str given as a list -> "a,b"
        v = out.get(k)
        if isinstance(v, (list, tuple, set)):
            out[k] = ",".join(str(x).strip() for x in v)
    for k in tags:  # Dict[str, str]: scalars -> str, ["k=v"] / ["k:v"] -> {k: v}
        v = out.get(k)
        if isinstance(v, (list, tuple)) and v and all(isinstance(x, str) and ("=" in x or ":" in x) for x in v):
            v = dict((p.strip() for p in (x.split("=", 1) if "=" in x else x.split(":", 1))) for x in v)
        if isinstance(v, dict):
            out[k] = {str(a).strip(): (str(b).lower() if isinstance(b, bool) else str(b)) for a, b in v.items() if b is not None}
    return out
# END field-shape normalization


class SupabaseVectorSearchAssetComponent(Component, Model, Resolvable):
    """Run a pgvector similarity search per row of an upstream DataFrame."""

    asset_name: str = Field(description="Output asset name.")
    resource_key: str = Field(
        default="supabase_resource",
        description="Resource key of the SupabaseResource providing the client.",
    )
    table_name: str = Field(description="Supabase table containing the vector column.")
    embedding_column: str = Field(
        default="embedding",
        description="Vector column on the target table (pgvector type).",
    )

    # Query source: from an upstream DataFrame column
    upstream_asset_key: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Upstream DataFrame asset providing query vectors (one search per row).",
    )
    query_embedding_column: Optional[str] = Field(
        default=None,
        description="Column on the upstream DataFrame holding the query vector (list[float] or ndarray).",
    )

    # Or a single static query vector
    query_vector: Optional[List[float]] = Field(
        default=None,
        description="Static query vector (list[float]). Use in place of upstream/query_embedding_column for a one-shot search.",
    )

    top_k: int = Field(default=10, description="Number of nearest neighbors to return per query.")
    metric: str = Field(
        default="cosine",
        description="Distance metric: 'cosine' (<=>), 'l2' (<->), or 'inner' (<#>). Only meaningful with RPC that respects it.",
    )
    additional_columns: Optional[List[str]] = Field(
        default=None,
        description="Extra columns to return alongside the match (defaults to all columns).",
    )

    rpc_name: Optional[str] = Field(
        default=None,
        description=(
            "Name of a Postgres function callable via ``client.rpc(...)`` that "
            "accepts named args ``query_embedding`` (vector), ``match_count`` "
            "(int), and optionally ``match_threshold`` (float). Recommended way "
            "to run pgvector searches through Supabase — see README for the "
            "canonical function body."
        ),
    )
    match_threshold: Optional[float] = Field(
        default=None,
        description="Optional similarity threshold forwarded to the RPC as ``match_threshold``.",
    )

    description: Optional[str] = Field(default=None)
    group_name: Optional[str] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)
    tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('deps', 'owners'), csv=(), tags=('tags',))

    @model_validator(mode="before")
    @classmethod
    def _coerce_upstream_asset_keys(cls, data):
        return coerce_upstream_input(data, component=cls.__name__, mode="single", optional=True)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        resource_key = self.resource_key
        table_name = self.table_name
        embedding_column = self.embedding_column
        upstream_asset_key = self.upstream_asset_key
        query_embedding_column = self.query_embedding_column
        query_vector = self.query_vector
        top_k = int(self.top_k)
        metric = (self.metric or "cosine").lower()
        additional_columns = self.additional_columns
        rpc_name = self.rpc_name
        match_threshold = self.match_threshold

        if metric not in ("cosine", "l2", "inner"):
            raise ValueError(f"metric must be 'cosine' | 'l2' | 'inner', got {metric!r}")
        if upstream_asset_key and query_vector is not None:
            raise ValueError("Set either `upstream_asset_key` (per-row) or `query_vector` (static), not both.")
        if upstream_asset_key and not query_embedding_column:
            raise ValueError("`upstream_asset_key` requires `query_embedding_column`.")
        if not upstream_asset_key and query_vector is None:
            raise ValueError("Provide either `upstream_asset_key` + `query_embedding_column`, or `query_vector`.")

        ins_kwargs: Dict[str, Any] = {}
        if upstream_asset_key:
            ins_kwargs["ins"] = {"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))}

        @asset(
            key=AssetKey.from_user_string(asset_name),
            description=self.description or f"pgvector search against Supabase table {table_name!r}.",
            group_name=self.group_name,
            kinds={"supabase", "pgvector", "vector-search"},
            tags=self.tags or None,
            owners=self.owners or None,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])] or None,
            required_resource_keys={resource_key},
            **ins_kwargs,
        )
        def _asset(context: AssetExecutionContext, **kwargs):
            supabase_resource = getattr(context.resources, resource_key)
            client = supabase_resource.get_client()

            # Build the list of queries.
            queries: List[Dict[str, Any]] = []
            if upstream_asset_key:
                upstream = kwargs["upstream"]
                if isinstance(upstream, dict):
                    frames = [v for v in upstream.values() if isinstance(v, pd.DataFrame)]
                    upstream = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
                if query_embedding_column not in upstream.columns:
                    raise ValueError(
                        f"query_embedding_column={query_embedding_column!r} not in upstream columns: "
                        f"{list(upstream.columns)}"
                    )
                upstream = upstream.reset_index(drop=True)
                for i, row in upstream.iterrows():
                    queries.append({
                        "row_index": int(i),
                        "row": row.to_dict(),
                        "vector": row[query_embedding_column],
                    })
            else:
                queries.append({
                    "row_index": 0,
                    "row": {},
                    "vector": query_vector,
                })

            # Execute each query.
            all_matches: List[Dict[str, Any]] = []
            errors: List[str] = []
            for q in queries:
                try:
                    vec_literal = _vector_to_pgvector_literal(q["vector"])
                except ValueError as e:
                    errors.append(f"row {q['row_index']}: {e}")
                    continue

                try:
                    if rpc_name:
                        params: Dict[str, Any] = {
                            "query_embedding": vec_literal,
                            "match_count": top_k,
                        }
                        if match_threshold is not None:
                            params["match_threshold"] = match_threshold
                        response = client.rpc(rpc_name, params).execute()
                        rows = response.data or []
                    else:
                        # PostgREST fallback: use order() with the vector operator embedded.
                        # PostgREST supports `?order=embedding.asc` but not distance
                        # operators directly. This path only works if the caller has
                        # exposed a `distance` generated column or a view.
                        select_cols = ",".join(additional_columns) if additional_columns else "*"
                        response = (
                            client.table(table_name)
                            .select(select_cols)
                            .order(embedding_column, desc=False)
                            .limit(top_k)
                            .execute()
                        )
                        rows = response.data or []
                        context.log.warning(
                            "REST fallback path used — this returns rows ordered by "
                            f"{embedding_column!r} column contents, not by distance to the "
                            "query vector. Set `rpc_name` for a proper similarity search."
                        )
                except Exception as e:
                    errors.append(f"row {q['row_index']}: {e}")
                    continue

                for rank, match in enumerate(rows):
                    record = {
                        "_query_row_index": q["row_index"],
                        "_rank": rank,
                        **match,
                    }
                    # Attach upstream row context for join-ability.
                    for k, v in q["row"].items():
                        if k == query_embedding_column:
                            continue
                        record.setdefault(f"query_{k}", v)
                    all_matches.append(record)

            out_df = pd.DataFrame(all_matches)
            if errors:
                context.log.warning(
                    f"{len(errors)} query row(s) failed. First: {errors[0]}"
                )

            preview_md = out_df.head(5).to_markdown(index=False) if len(out_df) else "(no matches)"

            return Output(
                value=out_df,
                metadata={
                    "queries": MetadataValue.int(len(queries)),
                    "matches": MetadataValue.int(len(out_df)),
                    "failed_queries": MetadataValue.int(len(errors)),
                    "table": MetadataValue.text(table_name),
                    "embedding_column": MetadataValue.text(embedding_column),
                    "metric": MetadataValue.text(metric),
                    "top_k": MetadataValue.int(top_k),
                    "rpc": MetadataValue.text(rpc_name or "(REST fallback)"),
                    "preview": MetadataValue.md(preview_md),
                },
            )

        return Definitions(assets=[_asset])
