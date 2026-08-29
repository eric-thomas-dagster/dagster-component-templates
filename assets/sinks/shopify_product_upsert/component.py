"""DataFrame → Shopify Product upsert (search-by-handle).

Mirrors an upstream DataFrame into the Shopify Products catalog via the
Admin REST API. Since Shopify has no native upsert endpoint, this sink
does search-then-write per row: `GET /products.json?handle=<handle>` →
PUT the match if found, else POST a new product.

Handles are Shopify's per-shop unique URL slugs — they're the natural
merge key for reverse ETL. Add a shop-level uniqueness on handle by
convention; Shopify enforces this at the store level.

Pairs with:
  - ``shopify_resource`` — Admin API access token + workhorse HTTP (required)
"""
from typing import Dict, List, Optional

import dagster as dg
from pydantic import Field


class ShopifyProductUpsertComponent(dg.Component, dg.Model, dg.Resolvable):
    """Batch-upsert products from an upstream DataFrame into Shopify Products.

    Example:
        ```yaml
        type: dagster_component_templates.ShopifyProductUpsertComponent
        attributes:
          asset_name: shopify_products_mirror
          upstream_asset_key: catalog_products
          resource_key: shopify

          fields_map:
            slug: handle                  # upstream slug → Shopify handle (match key)
            product_name: title
            body: body_html
            vendor: vendor
            product_type: product_type
            price: variant_price          # nested — maps to variants[0].price
          batch_size: 200
          group_name: reverse_etl
        ```

    For every upstream row:
      1. GET /products.json?handle=<row.handle>&limit=1 — look up existing.
      2. If found → PUT /products/{id}.json (partial update).
      3. If not → POST /products.json (new product).

    Nested field convention: fields prefixed `variant_` set the first
    variant's property on create/update (e.g. `variant_price`, `variant_sku`,
    `variant_inventory_quantity`). All others go on the top-level product body.
    """

    asset_name: str = Field(description="Output Dagster asset name.")
    upstream_asset_key: str = Field(description="Upstream asset providing the DataFrame.")
    resource_key: str = Field(
        default="shopify",
        description="Resource key registered by ShopifyResourceComponent.",
    )

    fields_map: Dict[str, str] = Field(
        description=(
            "Upstream column → Shopify field name. MUST include a mapping to "
            "`handle` (the search-by key). Fields prefixed `variant_` set the "
            "first variant's property (e.g. `variant_price` → variants[0].price)."
        ),
    )
    batch_size: int = Field(
        default=200,
        description="Max upstream rows per run (safety cap). Each row triggers 1-2 HTTP calls.",
    )

    group_name: Optional[str] = Field(
        default="shopify", description="Dagster asset group name."
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
        description="Asset kinds (auto-includes 'shopify').",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("shopify")

        # Validate handle is in fields_map values (required search key).
        mapped_fields = set(self.fields_map.values())
        if "handle" not in mapped_fields:
            raise ValueError(
                f"ShopifyProductUpsertComponent: fields_map must include a "
                f"mapping to Shopify field `handle` (used as the search-by "
                f"upsert match key). fields_map values: {sorted(mapped_fields)}"
            )

        @dg.asset(
            key=dg.AssetKey.from_user_string(_self.asset_name),
            ins={"upstream": dg.AssetIn(key=dg.AssetKey.from_user_string(_self.upstream_asset_key))},
            group_name=_self.group_name,
            kinds=kinds,
            owners=_self.owners,
            tags=_self.tags,
            required_resource_keys={_self.resource_key},
            description=_self.description or (
                "Upsert DataFrame rows into Shopify Products (match on handle)."
            ),
        )
        def _asset(context: dg.AssetExecutionContext, upstream):
            sh = getattr(context.resources, _self.resource_key)

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

            # Upstream column mapped to `handle`.
            handle_col = next(
                (col for col, shopify_field in _self.fields_map.items()
                 if shopify_field == "handle"),
                None,
            )
            if handle_col is None:
                raise dg.Failure(
                    "fields_map has no column mapping to `handle`. "
                    f"fields_map: {_self.fields_map}"
                )

            def _row_value(v):
                if v is None or (isinstance(v, float) and pd.isna(v)):
                    return None
                return v

            # In-run cache: id of records we created during THIS run so a
            # duplicate handle in the batch doesn't double-post.
            just_created: Dict[str, int] = {}

            created = 0
            updated = 0
            errors: List[str] = []
            skipped_no_handle = 0

            for i, row in df.iterrows():
                handle = _row_value(row[handle_col])
                if handle is None or handle == "":
                    skipped_no_handle += 1
                    continue

                # Split fields into product-level vs variant-level.
                product_body: dict = {}
                variant_props: dict = {}
                for col, sf_field in _self.fields_map.items():
                    v = _row_value(row[col])
                    if v is None:
                        continue
                    if sf_field.startswith("variant_"):
                        variant_props[sf_field[len("variant_"):]] = v
                    else:
                        product_body[sf_field] = v
                if variant_props:
                    product_body["variants"] = [variant_props]

                if not product_body:
                    continue

                try:
                    cached_id = just_created.get(str(handle))
                    if cached_id is not None:
                        sh.update_product(cached_id, product_body)
                        updated += 1
                        continue
                    result = sh.upsert_product_by_handle(str(handle), product_body)
                    if result.get("action") == "created":
                        created += 1
                        product = result.get("product") or {}
                        if product.get("id"):
                            just_created[str(handle)] = product["id"]
                    else:
                        updated += 1
                except Exception as e:  # noqa: BLE001
                    errors.append(f"row {i} (handle={handle}): {type(e).__name__}: {e}")

            context.log.info(
                f"Shopify products upsert: {created} created, {updated} updated, "
                f"{len(errors)} errors, {skipped_no_handle} skipped (missing handle)."
            )
            if errors:
                context.log.error(
                    "First few errors:\n" + "\n".join(errors[:5])
                )

            metadata = {
                "shopify_object_type": dg.MetadataValue.text("Product"),
                "match_key": dg.MetadataValue.text("handle"),
                "rows_created": dg.MetadataValue.int(created),
                "rows_updated": dg.MetadataValue.int(updated),
                "rows_upserted": dg.MetadataValue.int(created + updated),
                "rows_errored": dg.MetadataValue.int(len(errors)),
                "rows_skipped_no_handle": dg.MetadataValue.int(skipped_no_handle),
            }
            if errors:
                metadata["first_errors"] = dg.MetadataValue.json(errors[:5])

            return dg.MaterializeResult(metadata=metadata)

        return dg.Definitions(assets=[_asset])
