"""Cube Query Asset Component.

Runs a query against a Cube semantic-layer server (https://cube.dev/) and
materializes the result as a Dagster asset (pandas DataFrame). Perfect for
"put my Cube metrics into the warehouse" workflows, or as the first step of
a downstream AI/reporting pipeline that consumes governed metrics.

API: POST {api_url}/cubejs-api/v1/load
Body: {"query": {"measures": [...], "dimensions": [...], "timeDimensions": [...],
                 "filters": [...], "limit": ..., "order": {...}}}

Auth:
  - Cube Core dev mode: no auth
  - Cube production / Cube Cloud: JWT via `Authorization: Bearer <token>`
    (generated via a shared secret). Pass the JWT env var name in
    `api_token_env_var`.

Docs: https://cube.dev/docs/product/apis-integrations/rest-api
"""
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


class CubeQueryAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Run a Cube semantic-layer query and materialize as a Dagster asset.

    Example — daily orders count by status:

        ```yaml
        type: dagster_community_components.CubeQueryAssetComponent
        attributes:
          asset_name: cube_orders_by_status
          api_url_env_var: CUBE_URL
          query:
            measures:
              - Orders.count
              - Orders.totalAmount
            dimensions:
              - Orders.status
            timeDimensions:
              - dimension: Orders.createdAt
                granularity: day
                dateRange: ["2024-01-01", "2024-12-31"]
            order:
              Orders.createdAt: asc
          group_name: cube
        ```

    Cube Cloud (JWT auth):

        ```yaml
        attributes:
          # ...
          api_url_env_var: CUBE_API_URL
          api_token_env_var: CUBE_API_TOKEN
        ```
    """

    asset_name: str = Field(description="Output Dagster asset name.")
    query: Dict[str, Any] = Field(
        description=(
            "Cube query JSON. Standard Cube query shape: measures (list), "
            "dimensions (list), timeDimensions (list of {dimension, granularity, "
            "dateRange}), filters (list of {member, operator, values}), "
            "order (dict), limit (int), offset (int)."
        ),
    )

    api_url_env_var: str = Field(
        default="CUBE_URL",
        description="Env var with Cube API base URL (e.g. http://localhost:4000 for local dev).",
    )
    api_token_env_var: Optional[str] = Field(
        default=None,
        description="Optional env var with JWT for Cube Cloud / production auth. Omit for local dev.",
    )
    request_timeout_seconds: int = Field(default=60, ge=1, description="Timeout per Cube API call.")
    strip_cube_prefix: bool = Field(
        default=True,
        description=(
            "When true (default), strip the '<CubeName>.' prefix from column names — "
            "'Orders.customerName' becomes 'customerName'. Makes downstream consumers "
            "(dbt sources, LangChain templates that use {var} syntax, pandas transforms) "
            "friendlier. Set to false to preserve the fully-qualified Cube identifiers."
        ),
    )

    group_name: Optional[str] = Field(default="cube", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds (auto-includes 'cube', 'semantic-layer').",
    )
    deps: Optional[List[str]] = Field(default=None, description="Upstream Dagster asset keys.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        _kinds = set(self.kinds or [])
        _kinds.update({"cube", "semantic-layer"})

        @dg.asset(
            key=dg.AssetKey.from_user_string(_self.asset_name),
            group_name=_self.group_name,
            kinds=_kinds,
            owners=_self.owners,
            tags=_self.tags,
            description=_self.description or (
                f"Cube query: measures={_self.query.get('measures') or []}, "
                f"dimensions={_self.query.get('dimensions') or []}."
            ),
            deps=[dg.AssetKey.from_user_string(k) for k in (_self.deps or [])],
        )
        def _asset(context: dg.AssetExecutionContext):
            import os
            import pandas as pd
            try:
                import requests
            except ImportError:
                raise ImportError("cube_query_asset requires: pip install requests")

            base_url = os.environ.get(_self.api_url_env_var, "").rstrip("/")
            if not base_url:
                raise RuntimeError(f"{_self.api_url_env_var!r} env var not set.")

            headers = {"Content-Type": "application/json"}
            if _self.api_token_env_var:
                token = os.environ.get(_self.api_token_env_var)
                if not token:
                    raise RuntimeError(f"{_self.api_token_env_var!r} env var not set (Cube JWT).")
                headers["Authorization"] = token if token.startswith("Bearer ") else f"Bearer {token}"

            url = f"{base_url}/cubejs-api/v1/load"
            context.log.info(
                f"[cube] POST {url}  measures={_self.query.get('measures')} "
                f"dimensions={_self.query.get('dimensions')}"
            )
            resp = requests.post(
                url,
                headers=headers,
                json={"query": _self.query},
                timeout=_self.request_timeout_seconds,
            )
            if resp.status_code == 400:
                # Cube returns useful error detail on 400 — surface it.
                raise RuntimeError(f"Cube API 400: {resp.text[:1000]}")
            resp.raise_for_status()
            body = resp.json()

            data = body.get("data") or []
            df = pd.DataFrame(data)
            if _self.strip_cube_prefix and len(df) > 0:
                # Strip the '<CubeName>.' prefix from every column that has one.
                # LangChain / dbt / pandas all prefer identifiers without dots.
                df = df.rename(columns={c: (c.split(".", 1)[1] if "." in c else c) for c in df.columns})
            context.log.info(f"[cube] returned {len(df)} rows, columns={list(df.columns)}")

            md: Dict[str, Any] = {
                "cube_url": dg.MetadataValue.url(base_url),
                "cube_measures": dg.MetadataValue.json(_self.query.get("measures") or []),
                "cube_dimensions": dg.MetadataValue.json(_self.query.get("dimensions") or []),
                "cube_row_count": dg.MetadataValue.int(len(df)),
            }
            if len(df) > 0:
                md["preview"] = dg.MetadataValue.md(df.head(10).to_markdown(index=False))
                md["columns"] = dg.MetadataValue.json(list(df.columns))
            # Cube annotates each query with per-measure/dimension metadata —
            # surface it for lineage docs.
            annotation = body.get("annotation") or {}
            if annotation:
                md["cube_annotation"] = dg.MetadataValue.json(annotation)
            context.add_output_metadata(md)
            return df

        return dg.Definitions(assets=[_asset])
