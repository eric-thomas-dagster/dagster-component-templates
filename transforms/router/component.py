"""RouterComponent.

Multi-output conditional split — emit each output asset with rows matching its predicate. Equivalent to ADF Conditional Split / Informatica Router.
"""

from typing import Optional

import dagster as dg
import pandas as pd
from pydantic import Field


class RouterComponent(dg.Component, dg.Model, dg.Resolvable):
    """Multi-output conditional split — emit each output asset with rows matching its predicate. Equivalent to ADF Conditional Split / Informatica Router."""


    upstream_asset_key: str = Field(description="Upstream DataFrame")

    routes: list = Field(description="List of {asset_name, condition} dicts. Conditions evaluated in order; non-matching rows go to default route if set.")
    default_asset_name: Optional[str] = Field(default=None, description="Name for the catch-all asset (rows that didn't match any condition)")
    exclusive: bool = Field(default=True, description="If True, each row goes to exactly one route (first match). If False, a row may appear in multiple routes (overlapping conditions).")

    group_name: str = Field(default="transforms")
    kinds: Optional[list[str]] = Field(default=None)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        upstream_key = self.upstream_asset_key

        def _make_route(name, condition):
            @dg.asset(
                name=name,
                description=f"router branch: {condition}",
                group_name=_self.group_name,
                kinds=set(_self.kinds or ["router"]),
                deps=[dg.AssetKey.from_user_string(upstream_key)],
                ins={"df": dg.AssetIn(key=dg.AssetKey.from_user_string(upstream_key))},
            )
            def _route(context: dg.AssetExecutionContext, df: pd.DataFrame) -> pd.DataFrame:
                out = df.query(condition) if condition else df
                context.add_output_metadata({
                    "dagster/row_count": dg.MetadataValue.int(len(out)),
                    "condition": dg.MetadataValue.text(condition),
                })
                return out
            return _route

        assets = []
        if _self.exclusive:
            # Each row goes to exactly one — use a chained-mask helper.
            def _make_exclusive(name, condition, prior_conditions):
                @dg.asset(
                    name=name,
                    description=f"router branch (exclusive): {condition}",
                    group_name=_self.group_name,
                    kinds=set(_self.kinds or ["router"]),
                    deps=[dg.AssetKey.from_user_string(upstream_key)],
                    ins={"df": dg.AssetIn(key=dg.AssetKey.from_user_string(upstream_key))},
                )
                def _route(context: dg.AssetExecutionContext, df: pd.DataFrame) -> pd.DataFrame:
                    mask = pd.Series([True] * len(df), index=df.index)
                    for prior in prior_conditions:
                        mask = mask & ~df.eval(prior).fillna(False)
                    if condition:
                        mask = mask & df.eval(condition).fillna(False)
                    out = df[mask]
                    context.add_output_metadata({
                        "dagster/row_count": dg.MetadataValue.int(len(out)),
                        "condition": dg.MetadataValue.text(condition or "(default)"),
                    })
                    return out
                return _route
            prior = []
            for r in _self.routes:
                assets.append(_make_exclusive(r["asset_name"], r["condition"], list(prior)))
                prior.append(r["condition"])
            if _self.default_asset_name:
                assets.append(_make_exclusive(_self.default_asset_name, None, list(prior)))
        else:
            for r in _self.routes:
                assets.append(_make_route(r["asset_name"], r["condition"]))

        return dg.Definitions(assets=assets)
