"""MarketBasketRulesComponent.

Group rows into baskets (e.g. by transaction_id), one-hot encode item presence per basket, run apriori to find frequent itemsets, then derive association rules with support, confidence, lift. Output is one row per rule.
"""
from typing import Dict, List, Optional

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
    Resolvable,
    asset,
)
from pydantic import Field


class MarketBasketRulesComponent(Component, Model, Resolvable):
    """Mine association rules from transaction data via apriori (mlxtend)."""

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame")
    basket_column: str = Field(description="Column that identifies a basket (e.g. transaction_id).")
    item_column: str = Field(description="Column with item names.")
    min_support: float = Field(default=0.01, description="Minimum support threshold.")
    min_confidence: float = Field(default=0.3, description="Minimum confidence for rules.")
    metric: str = Field(default="confidence", description="Sort metric: confidence, lift, support, etc.")

    include_preview_metadata: bool = Field(
        default=False,
        description="Include a preview of the output DataFrame in metadata (for builder UIs).",
    )
    preview_rows: int = Field(
        default=25,
        ge=1,
        le=500,
        description="Rows in the preview when include_preview_metadata=True.",
    )
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    asset_tags: Optional[Dict[str, str]] = Field(default=None, description="Asset tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds.")
    deps: Optional[List[str]] = Field(default=None, description="Lineage-only upstream asset keys.")

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows
        group_name = self.group_name
        _self = self

        ins = {"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))}
        tags_dict = dict(self.asset_tags or {})
        for k in (self.kinds or ["python"]):
            tags_dict[f"dagster/kind/{k}"] = ""

        @asset(
            name=asset_name,
            ins=ins,
            group_name=group_name,
            description=self.description or "Mine association rules from transaction data via apriori (mlxtend).",
            tags=tags_dict,
            owners=self.owners or [],
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def _asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
            df = upstream
            from mlxtend.frequent_patterns import apriori, association_rules
            baskets = df.groupby(_self.basket_column)[_self.item_column].apply(list).tolist()
            from mlxtend.preprocessing import TransactionEncoder
            te = TransactionEncoder()
            te_ary = te.fit_transform(baskets)
            te_df = pd.DataFrame(te_ary, columns=te.columns_)
            freq = apriori(te_df, min_support=_self.min_support, use_colnames=True)
            rules = association_rules(freq, metric=_self.metric, min_threshold=_self.min_confidence)
            for col in ("antecedents", "consequents"):
                rules[col] = rules[col].apply(lambda s: ", ".join(sorted(s)))
            out_df = rules.sort_values(_self.metric, ascending=False).reset_index(drop=True)

            if include_preview and len(out_df) > 0:
                try:
                    _prev = out_df.sample(min(preview_rows, len(out_df))) if len(out_df) > preview_rows * 10 else out_df.head(preview_rows)
                    context.add_output_metadata({"preview": MetadataValue.md(_prev.to_markdown(index=False))})
                except Exception as _e:
                    context.log.warning(f"preview emission failed: {_e}")
            return out_df

        return Definitions(assets=[_asset])
