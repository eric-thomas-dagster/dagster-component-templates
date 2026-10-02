"""DataFrame -> Magento/Adobe Commerce Product upsert (search-by-sku).

Mirrors an upstream DataFrame into the Magento/Adobe Commerce catalog via
the REST API. Magento's catalog product endpoint has NO native upsert --
`PUT /V1/products/:sku` is **update-ONLY** (it will not create a product
if the SKU in the path doesn't already exist). So this sink does
search-then-write per row: `GET /V1/products/:sku` (None on 404) -> if
found, `PUT /V1/products/:sku` (the body's `sku` is forced to match the
path `sku` -- the most common Magento REST footgun); if not found,
`POST /V1/products` with `{"product": {...}}` to create.

SKU is Magento's natural per-store unique merge key for reverse ETL --
every product has one and it's immutable-ish (changing it is a distinct,
disruptive operation in Magento), unlike Shopify's URL-slug `handle`.

Pairs with:
  - ``magento_resource`` -- bearer-token auth + workhorse HTTP (required)
"""
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


class MagentoProductUpsertComponent(dg.Component, dg.Model, dg.Resolvable):
    """Batch-upsert products from an upstream DataFrame into Magento/Adobe Commerce.

    Example:
        ```yaml
        type: dagster_component_templates.MagentoProductUpsertComponent
        attributes:
          asset_name: magento_products_mirror
          upstream_asset_key: catalog_products
          resource_key: magento

          fields_map:
            product_sku: sku              # upstream sku -> Magento sku (match key)
            product_name: name
            list_price: price
            is_active: status
          batch_size: 200
          group_name: reverse_etl
        ```

    For every upstream row:
      1. GET /V1/products/{row.sku} -- look up existing (None on 404).
      2. If found -> PUT /V1/products/{row.sku} (update-only; body sku forced
         to match path sku).
      3. If not found -> POST /V1/products with `{"product": {...}}` (create;
         requires at minimum sku/name/price/attribute_set_id -- the resource
         fills attribute_set_id from its own default if the row didn't map one).

    GOTCHA: Magento's `PUT /V1/products/:sku` is update-ONLY. It will never
    create a product for you -- if the SKU doesn't already exist, Magento
    errors rather than creating it. This sink's upsert therefore always
    checks existence first (via the resource's `upsert_product_by_sku`,
    which itself does GET-then-PUT-or-POST) rather than blindly PUTing.
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
        default="magento",
        description="Resource key registered by MagentoResourceComponent.",
    )

    fields_map: Dict[str, str] = Field(
        description=(
            "Upstream column -> Magento product field name. MUST include a "
            "mapping to `sku` (the search-by / match key). Other common "
            "Magento fields: name, price, status (1=enabled/2=disabled), "
            "visibility (1/2/3/4), type_id, weight, attribute_set_id."
        ),
    )
    batch_size: int = Field(
        default=200,
        description="Max upstream rows per run (safety cap). Each row triggers 1-2 HTTP calls.",
    )

    group_name: Optional[str] = Field(
        default="magento", description="Dagster asset group name."
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
        description="Asset kinds (auto-includes 'magento').",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("magento")

        # Validate: exactly one of upstream_asset_key OR source: must be set.
        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError(
                "MagentoProductUpsertComponent: supply exactly one of "
                "`upstream_asset_key` OR `source:` (got both or neither)."
            )

        # Validate sku is in fields_map values (required search key).
        mapped_fields = set(self.fields_map.values())
        if "sku" not in mapped_fields:
            raise ValueError(
                f"MagentoProductUpsertComponent: fields_map must include a "
                f"mapping to Magento field `sku` (used as the search-by "
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
            raise ValueError(f"MagentoProductUpsertComponent source kind={kind!r} not supported (sql / csv / inline)")

        def _run_upsert(context, upstream):
            mg = getattr(context.resources, _self.resource_key)

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
                (col for col, magento_field in _self.fields_map.items()
                 if magento_field == "sku"),
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

            created = 0
            updated = 0
            errors: List[str] = []
            skipped_no_sku = 0

            for i, row in df.iterrows():
                sku = _row_value(row[sku_col])
                if sku is None or sku == "":
                    skipped_no_sku += 1
                    continue

                product_body: dict = {}
                for col, mg_field in _self.fields_map.items():
                    v = _row_value(row[col])
                    if v is None:
                        continue
                    product_body[mg_field] = v

                if not product_body:
                    continue

                try:
                    result = mg.upsert_product_by_sku(str(sku), product_body)
                    if result.get("action") == "created":
                        created += 1
                    else:
                        updated += 1
                except Exception as e:  # noqa: BLE001
                    errors.append(f"row {i} (sku={sku}): {type(e).__name__}: {e}")

            context.log.info(
                f"Magento products upsert: {created} created, {updated} updated, "
                f"{len(errors)} errors, {skipped_no_sku} skipped (missing sku)."
            )
            if errors:
                context.log.error(
                    "First few errors:\n" + "\n".join(errors[:5])
                )

            metadata = {
                "magento_object_type": dg.MetadataValue.text("Product"),
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
                "Upsert DataFrame rows into Magento/Adobe Commerce Products (match on sku)."
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
