"""Data Vault 2.0 Hub / Link / Satellite Component.

Take a raw source DataFrame and generate the Data Vault 2.0 modeling
tables — Hubs, Links, and Satellites — with all the required system
columns (hash keys, load date, record source, hash diff). Emits three
Dagster assets per config: `{prefix}_hub`, `{prefix}_link` (if link
business keys are provided), and `{prefix}_sat`.

Right for teams migrating raw source ingestion into a Data Vault 2.0
warehouse layer inside Dagster — SDA (Software-Defined Assets) lets
you materialize hubs / links / satellites independently and see their
lineage back to the raw source.
"""
from typing import List, Optional

import dagster as dg
from pydantic import Field


class DataVaultHubLinkSatelliteComponent(dg.Component, dg.Model, dg.Resolvable):
    """Generate DV2.0 Hub, Link, and Satellite assets from a source DataFrame.

    DV2.0 modeling primitives:
      - **Hub** = unique business keys + hash key + load-date + record-source
      - **Link** = combines hash keys of two-or-more hubs to represent a
        relationship (many-to-many)
      - **Satellite** = descriptive attributes hanging off a hub or link
        + hash diff for change detection + effective-from date

    Every table gets the standard system columns:
      - `hash_key` (SHA-256 of business-key columns)
      - `load_date` (materialization timestamp)
      - `record_source` (e.g. "orders_source")
      - Satellite only: `hash_diff` (SHA-256 of descriptive columns)

    Example (Customer hub + descriptive satellite):

        ```yaml
        type: dagster_community_components.DataVaultHubLinkSatelliteComponent
        attributes:
          entity: customer
          upstream_asset_key: raw_customers
          business_keys: [customer_id]
          satellite_columns: [name, email, phone, address, updated_at]
          record_source: "erp_customers"
          group_name: data_vault_raw
        ```

    Example (Customer-to-Order link with sat on the link):

        ```yaml
        attributes:
          entity: customer_order
          upstream_asset_key: raw_customer_orders
          business_keys: [customer_id, order_id]
          link_business_keys: [customer_id, order_id]        # keys that identify the link
          satellite_columns: [order_status, order_amount, updated_at]
          record_source: "erp_orders"
        ```
    """

    entity: str = Field(description="Base name for emitted assets: <entity>_hub / _link / _sat.")
    upstream_asset_key: str = Field(description="Upstream DataFrame asset key to consume.")
    business_keys: List[str] = Field(description="Business key column(s) that identify the entity in the source.")
    satellite_columns: List[str] = Field(description="Descriptive columns that go into the satellite.")
    link_business_keys: Optional[List[str]] = Field(
        default=None,
        description=(
            "If provided, also emit a `<entity>_link` asset. These are the "
            "columns that jointly identify the LINK (usually the business "
            "keys of both endpoints)."
        ),
    )

    record_source: str = Field(description="Value populated in the record_source column.")
    load_date_column: str = Field(default="load_date", description="Name of the load-date system column.")
    hash_algo: str = Field(default="sha256", description="Hash algorithm: sha256 | md5")

    group_name: Optional[str] = Field(default=None)
    asset_key_prefix: Optional[List[str]] = Field(default=None, description="Prefix for emitted asset keys.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        prefix = self.asset_key_prefix or []

        def _hash_col(df, cols):
            import hashlib
            def _row(r):
                joined = "||".join(str(r[c]) if c in r.index else "" for c in cols)
                h = hashlib.sha256(joined.encode()) if _self.hash_algo == "sha256" else hashlib.md5(joined.encode())
                return h.hexdigest()
            return df.apply(_row, axis=1)

        hub_key = dg.AssetKey([*prefix, f"{self.entity}_hub"])
        sat_key = dg.AssetKey([*prefix, f"{self.entity}_sat"])
        link_key = dg.AssetKey([*prefix, f"{self.entity}_link"]) if self.link_business_keys else None
        upstream = dg.AssetKey(self.upstream_asset_key)

        @dg.asset(key=hub_key, group_name=self.group_name, compute_kind="data_vault",
                  ins={"raw": dg.AssetIn(key=upstream)})
        def _hub(context: dg.AssetExecutionContext, raw):
            import pandas as pd
            df = raw.copy() if hasattr(raw, "copy") else pd.DataFrame(raw)
            hub = df[_self.business_keys].drop_duplicates().reset_index(drop=True)
            hub["hash_key"] = _hash_col(hub, _self.business_keys)
            hub[_self.load_date_column] = pd.Timestamp.utcnow()
            hub["record_source"] = _self.record_source
            context.add_output_metadata({"row_count": len(hub), "entity": _self.entity})
            return hub

        @dg.asset(key=sat_key, group_name=self.group_name, compute_kind="data_vault",
                  ins={"raw": dg.AssetIn(key=upstream)})
        def _sat(context: dg.AssetExecutionContext, raw):
            import pandas as pd
            df = raw.copy() if hasattr(raw, "copy") else pd.DataFrame(raw)
            keep_cols = _self.business_keys + [c for c in _self.satellite_columns if c in df.columns]
            sat = df[keep_cols].copy()
            sat["hash_key"] = _hash_col(df, _self.business_keys)
            sat["hash_diff"] = _hash_col(df, [c for c in _self.satellite_columns if c in df.columns])
            sat[_self.load_date_column] = pd.Timestamp.utcnow()
            sat["record_source"] = _self.record_source
            context.add_output_metadata({"row_count": len(sat), "entity": _self.entity})
            return sat

        assets = [_hub, _sat]

        if _self.link_business_keys:
            @dg.asset(key=link_key, group_name=self.group_name, compute_kind="data_vault",
                      ins={"raw": dg.AssetIn(key=upstream)})
            def _link(context: dg.AssetExecutionContext, raw):
                import pandas as pd
                df = raw.copy() if hasattr(raw, "copy") else pd.DataFrame(raw)
                link = df[_self.link_business_keys].drop_duplicates().reset_index(drop=True)
                link["hash_key"] = _hash_col(link, _self.link_business_keys)
                # Also compute the referenced hub hash keys for lineage.
                for bk in _self.link_business_keys:
                    link[f"{bk}_hash_key"] = _hash_col(link, [bk])
                link[_self.load_date_column] = pd.Timestamp.utcnow()
                link["record_source"] = _self.record_source
                context.add_output_metadata({"row_count": len(link), "entity": _self.entity})
                return link

            assets.append(_link)

        return dg.Definitions(assets=assets)
