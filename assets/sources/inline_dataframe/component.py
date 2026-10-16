"""Materialize a literal DataFrame from YAML.

For small reference tables (region codes, fixture data, an inline-table source
ports), where carrying a CSV file alongside the project is overkill.
Rows and column dtypes live entirely in the defs.yaml — the asset has no
external dependency at runtime.

This component exists primarily so the `alteryx-to-dagster` migrator can
emit an inline-table source tools as a `defs.yaml` instead of a custom .py
asset, but it's useful on its own for any inline-data scenario.
"""
from typing import Any, Dict, List, Optional, Union

import dagster as dg
from dagster import (
    AssetExecutionContext,
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


class InlineDataframeComponent(Component, Model, Resolvable):
    """Materialize a small literal DataFrame from inline YAML rows.

    Example:
        ```yaml
        type: dagster_component_templates.InlineDataframeComponent
        attributes:
          asset_name: region_codes
          columns: [region_code, region_name]
          rows:
            - [N, North]
            - [S, South]
            - [E, East]
            - [W, West]
          group_name: reference
        ```

    With explicit dtypes (handy when row values arrive as strings — e.g.
    from an an ETL tool's inline-table XML — but downstream filters expect
    numeric comparisons):

        ```yaml
        attributes:
          asset_name: raw_sales
          columns: [region, product, quantity, unit_price]
          dtypes: {quantity: Int64, unit_price: float64}
          rows:
            - [North, Widget, "10", "5.50"]
            - [North, Gadget, "3", "12.00"]
        ```
    """

    asset_name: str = Field(description="Output Dagster asset name.")
    # `List[Union[str, int]]` rather than `List[str]` because Alteryx
    # workflows sometimes use literal numeric column names ('1', '2', '3',
    # ...). YAML parses these as int and Dagster's Resolvable templating
    # coerces quoted '1' back to int 1 at resolve time, so a stricter
    # `List[str]` would reject them. We `str()` the values at use sites
    # so the DataFrame columns are always strings regardless of input type.
    columns: List[Union[str, int]] = Field(description="Column names, in order.")
    rows: List[List[Any]] = Field(
        default_factory=list,
        description="Each row is a list of values matching `columns` length.",
    )
    dtypes: Optional[Dict[str, str]] = Field(
        default=None,
        description=(
            "Per-column dtype overrides (e.g. {quantity: Int64, unit_price: float64, "
            "is_active: boolean}). Numeric dtypes route through pd.to_numeric so "
            "string-form values from YAML coerce cleanly. Defaults to pandas inference."
        ),
    )

    group_name: Optional[str] = Field(default=None, description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Extra asset tags.")
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds. Defaults to ['inline', 'reference'].",
    )
    deps: Optional[List[str]] = Field(default=None, description="Lineage-only upstream asset keys.")
    freshness_max_lag_minutes: Optional[int] = Field(default=None)
    freshness_cron: Optional[str] = Field(default=None)

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('owners', 'kinds', 'deps'), csv=(), tags=('asset_tags',))

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        # Stringify column names — accept int (template-coerced from YAML
        # numeric-looking strings) but ensure pandas always sees str labels.
        columns = [str(c) for c in self.columns]
        rows = [list(r) for r in self.rows]
        dtypes = dict(self.dtypes or {})
        group_name = self.group_name
        description = self.description
        owners = self.owners or []

        _inferred_kinds = list(self.kinds or ["inline", "reference"])
        _all_tags = dict(self.asset_tags or {})
        for _kind in _inferred_kinds:
            _all_tags[f"dagster/kind/{_kind}"] = ""

        _freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from datetime import timedelta
            from dagster import FreshnessPolicy
            _lag = timedelta(minutes=int(self.freshness_max_lag_minutes))
            _freshness_policy = (
                FreshnessPolicy.cron(deadline_cron=self.freshness_cron, lower_bound_delta=_lag)
                if self.freshness_cron
                else FreshnessPolicy.time_window(fail_window=_lag)
            )

        @asset(
            key=AssetKey.from_user_string(asset_name),
            group_name=group_name,
            description=description,
            owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def _inline_asset(context: AssetExecutionContext):
            import pandas as pd

            # Preserve the column schema as declared. an inline-table source often
            # stores a CSV-shaped blob in a single `Field1` column on purpose
            # (downstream TextToColumns does the splitting). Auto-splitting
            # here breaks workflows that reference the single source column.
            df = pd.DataFrame(rows, columns=columns)
            # Coerce dtypes — numeric columns via to_numeric (handles "" / None
            # gracefully); everything else via astype. Catch overflow/cast
            # failures and fall back to string so a single bad value doesn't
            # take down the whole asset.
            for col, dtype in dtypes.items():
                if col not in df.columns:
                    context.log.warning(f"dtype set for unknown column {col!r}; ignoring.")
                    continue
                try:
                    if dtype in ("Int64", "Int32", "Int16", "Int8", "float64", "float32"):
                        df[col] = pd.to_numeric(df[col], errors="coerce").astype(dtype)
                    else:
                        df[col] = df[col].astype(dtype)
                except (ValueError, TypeError, OverflowError) as _cast_err:
                    context.log.warning(
                        f"inline_dataframe: failed to cast {col!r} to {dtype!r} "
                        f"({_cast_err}); leaving as object/string."
                    )
                    df[col] = df[col].astype(str)

            context.add_output_metadata({
                "dagster/row_count": MetadataValue.int(len(df)),
                "dagster/column_schema": MetadataValue.table_schema(
                    dg.TableSchema(columns=[
                        dg.TableColumn(name=str(c), type=str(df.dtypes[c]))
                        for c in df.columns
                    ])
                ),
            })
            return df

        return Definitions(assets=[_inline_asset])
