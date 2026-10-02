"""DataFrame -> BigCommerce Product upsert (search-by-sku).

Mirrors an upstream DataFrame into the BigCommerce Catalog API (v3)
Products via raw HTTP. Since BigCommerce has no native upsert endpoint,
this sink does search-then-write per row: `GET /catalog/products?sku=<sku>`
-> PUT the match if found, else POST a new product.

SKU is BigCommerce's unique (case-insensitive) key across the WHOLE
catalog -- it's the natural merge key for reverse ETL.

Gotcha -- required fields on create: BigCommerce's Create Product
endpoint requires at minimum `name`, `type` ('physical' or 'digital'),
`weight`, and `price` (plus `categories` if your store has the V2
product experience enabled). Every row that results in a CREATE must
supply these via `fields_map`, or BigCommerce will reject the POST with
a 422. PUT (update) is a partial update and does not require them again.

Limitation -- flat body only: unlike Shopify (which nests variant
price/sku/inventory under `variants[0]`), this sink writes a FLAT
BigCommerce product body straight from `fields_map` -- no nesting
convention. BigCommerce variants (size/color combinations with their
own SKU/price/inventory) are a separate, more complex sub-resource
(`/catalog/products/{id}/variants`); this sink covers the common
single-SKU-per-product case. Multi-variant products need custom code
using the resource's methods directly.

Pairs with:
  - ``bigcommerce_resource`` -- store hash + access token + workhorse HTTP (required)
"""
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


class BigCommerceProductUpsertComponent(dg.Component, dg.Model, dg.Resolvable):
    """Batch-upsert products from an upstream DataFrame into BigCommerce Products.

    Example:
        ```yaml
        type: dagster_component_templates.BigCommerceProductUpsertComponent
        attributes:
          asset_name: bigcommerce_products_mirror
          upstream_asset_key: catalog_products
          resource_key: bigcommerce

          fields_map:
            product_sku: sku               # upstream SKU -> BigCommerce sku (match key)
            product_name: name
            product_type: type             # 'physical' or 'digital'
            list_price: price
            weight_lbs: weight
          batch_size: 200
          group_name: reverse_etl
        ```

    For every upstream row:
      1. GET /catalog/products?sku=<row.sku>&limit=1 -- look up existing.
      2. If found -> PUT /catalog/products/{id} (partial update).
      3. If not -> POST /catalog/products (new product -- requires at
         minimum name, type, weight, price; see module docstring).
    """

    asset_name: str = Field(description="Output Dagster asset name.")

    # Two source shapes -- supply exactly one.
    upstream_asset_key: Optional[str] = Field(
        default=None,
        description="Upstream Dagster asset providing the DataFrame. Mutually exclusive with `source:`.",
    )
    source: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Inline source config. Mutually exclusive with `upstream_asset_key`. "
            "Shapes: {kind: sql, resource_key/database_url_env_var, query}, "
            "{kind: csv, path, read_csv_kwargs}, {kind: inline, rows}."
        ),
    )

    resource_key: str = Field(
        default="bigcommerce",
        description="Resource key registered by BigCommerceResourceComponent.",
    )

    fields_map: Dict[str, str] = Field(
        description=(
            "Upstream column -> BigCommerce product field name. MUST include "
            "a mapping to `sku` (the search-by key). BigCommerce requires "
            "`name`, `type` ('physical'/'digital'), `weight`, and `price` on "
            "CREATE -- map those too, or new-product POSTs will fail."
        ),
    )
    batch_size: int = Field(
        default=200,
        description="Max upstream rows per run (safety cap). Each row triggers 1-2 HTTP calls.",
    )

    group_name: Optional[str] = Field(
        default="bigcommerce", description="Dagster asset group name."
    )
    description: Optional[str] = Field(
        default=None, description="Asset description."
    )
    owners: Optional[List[str]] = Field(
        default=None, description="Asset owners."
    )
    tags: Optional[Dict[str, str]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds (auto-includes 'bigcommerce').",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("bigcommerce")

        # Validate: exactly one of upstream_asset_key OR source: must be set.
        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError(
                "BigCommerceProductUpsertComponent: supply exactly one of "
                "`upstream_asset_key` OR `source:` (got both or neither)."
            )

        # Validate sku is in fields_map values (required search key).
        mapped_fields = set(self.fields_map.values())
        if "sku" not in mapped_fields:
            raise ValueError(
                f"BigCommerceProductUpsertComponent: fields_map must include a "
                f"mapping to BigCommerce field `sku` (used as the search-by "
                f"upsert match key). fields_map values: {sorted(mapped_fields)}"
            )

        use_source = self.source is not None
        extra_rks: set = set()
        if use_source and (self.source.get("kind") or "").lower() == "sql":
            _rk = self.source.get("resource_key")
            if _rk:
                extra_rks.add(_rk)

        # ── Source resolver (self-contained per no-shared-code rule) ──────
        def _resolve_source_df(exec_ctx):
            import pandas as pd
            src = _self.source or {}
            kind = (src.get("kind") or "").lower()
            if kind == "sql":
                query = src.get("query")
                if not query:
                    raise ValueError("source kind=sql requires 'query'")
                rk = src.get("resource_key")
                if rk:
                    resource = getattr(exec_ctx.resources, rk)
                    if hasattr(resource, "get_engine"):
                        return pd.read_sql(query, resource.get_engine())
                    if hasattr(resource, "get_connection"):
                        with resource.get_connection() as conn:
                            if hasattr(conn, "execute") and hasattr(conn, "df"):
                                return conn.execute(query).df()
                            return pd.read_sql(query, conn)
                    raise ValueError(f"source kind=sql: resource {rk!r} must expose .get_engine() or .get_connection()")
                env = src.get("database_url_env_var")
                if env:
                    import os
                    from sqlalchemy import create_engine
                    url = os.environ.get(env, "")
                    if not url:
                        raise ValueError(f"database_url_env_var {env!r} is unset")
                    return pd.read_sql(query, create_engine(url))
                raise ValueError("source kind=sql requires 'resource_key' OR 'database_url_env_var'")
            if kind == "csv":
                path = src.get("path")
                if not path:
                    raise ValueError("source kind=csv requires 'path'")
                return pd.read_csv(path, **(src.get("read_csv_kwargs") or {}))
            if kind == "inline":
                return pd.DataFrame(src.get("rows") or [])
            raise ValueError(f"BigCommerceProductUpsertComponent source kind={kind!r} not supported (sql / csv / inline)")

        def _run_upsert(context, upstream):
            bc = getattr(context.resources, _self.resource_key)

            import pandas as pd
            if not isinstance(upstream, pd.DataFrame):
                df = pd.DataFrame([upstream]) if isinstance(upstream, dict) else pd.DataFrame(upstream)
            else:
                df = upstream

            if len(df) == 0:
                context.log.warning("Upstream DataFrame is empty — nothing to upsert.")
                return dg.MaterializeResult(metadata={"rows_upserted": dg.MetadataValue.int(0)})

            if len(df) > _self.batch_size:
                context.log.warning(
                    f"Upstream has {len(df)} rows; capped at batch_size={_self.batch_size}."
                )
                df = df.head(_self.batch_size)

            required_cols = set(_self.fields_map.keys())
            missing_cols = [c for c in required_cols if c not in df.columns]
            if missing_cols:
                raise dg.Failure(
                    f"Columns not in upstream: {missing_cols}. Available: {list(df.columns)}"
                )

            # Upstream column mapped to `sku`.
            sku_col = next(
                (col for col, bc_field in _self.fields_map.items()
                 if bc_field == "sku"),
                None,
            )
            if sku_col is None:
                raise dg.Failure(
                    "fields_map has no column mapping to `sku`. "
                    f"fields_map: {_self.fields_map}"
                )

            def _row_value(v):
                if v is None or (isinstance(v, float) and pd.isna(v)):
                    return None
                return v

            # In-run cache: id of records we created during THIS run so a
            # duplicate sku in the batch doesn't double-post.
            just_created: Dict[str, int] = {}

            created = 0
            updated = 0
            errors: List[str] = []
            skipped_no_sku = 0

            for i, row in df.iterrows():
                sku = _row_value(row[sku_col])
                if sku is None or sku == "":
                    skipped_no_sku += 1
                    continue

                # Flat body -- no variant nesting (see module docstring).
                product_body: dict = {}
                for col, bc_field in _self.fields_map.items():
                    v = _row_value(row[col])
                    if v is None:
                        continue
                    product_body[bc_field] = v

                if not product_body:
                    continue

                try:
                    cached_id = just_created.get(str(sku))
                    if cached_id is not None:
                        bc.update_product(cached_id, product_body)
                        updated += 1
                        continue
                    result = bc.upsert_product_by_sku(str(sku), product_body)
                    if result.get("action") == "created":
                        created += 1
                        product = result.get("product") or {}
                        if product.get("id"):
                            just_created[str(sku)] = product["id"]
                    else:
                        updated += 1
                except Exception as e:  # noqa: BLE001
                    errors.append(f"row {i} (sku={sku}): {type(e).__name__}: {e}")

            context.log.info(
                f"BigCommerce products upsert: {created} created, {updated} updated, "
                f"{len(errors)} errors, {skipped_no_sku} skipped (missing sku)."
            )
            if errors:
                context.log.error(
                    "First few errors:\n" + "\n".join(errors[:5])
                )

            metadata = {
                "bigcommerce_object_type": dg.MetadataValue.text("Product"),
                "match_key": dg.MetadataValue.text("sku"),
                "rows_created": dg.MetadataValue.int(created),
                "rows_updated": dg.MetadataValue.int(updated),
                "rows_upserted": dg.MetadataValue.int(created + updated),
                "rows_errored": dg.MetadataValue.int(len(errors)),
                "rows_skipped_no_sku": dg.MetadataValue.int(skipped_no_sku),
            }
            if errors:
                metadata["first_errors"] = dg.MetadataValue.json(errors[:5])

            return dg.MaterializeResult(metadata=metadata)

        common_kwargs = dict(
            key=dg.AssetKey.from_user_string(_self.asset_name),
            group_name=_self.group_name,
            kinds=kinds,
            owners=_self.owners,
            tags=_self.tags,
            description=_self.description or (
                "Upsert DataFrame rows into BigCommerce Products (match on sku)."
            ),
        )

        if use_source:
            required_rks = {_self.resource_key} | extra_rks

            @dg.asset(required_resource_keys=required_rks, **common_kwargs)
            def _asset(context: dg.AssetExecutionContext):
                df = _resolve_source_df(context)
                return _run_upsert(context, df)
        else:
            @dg.asset(
                ins={"upstream": dg.AssetIn(key=dg.AssetKey.from_user_string(_self.upstream_asset_key))},
                required_resource_keys={_self.resource_key},
                **common_kwargs,
            )
            def _asset(context: dg.AssetExecutionContext, upstream):
                return _run_upsert(context, upstream)

        return dg.Definitions(assets=[_asset])
