"""DynamicRename.

Pattern-based column renaming.

Supported `mode:` values:

- `first_row` — use the values in the first data row as the new column
  names; the first row is dropped from the output. Common pattern
  when reading a CSV/Excel whose true header is row 2+.
- `add_prefix` — prefix every selected column name with `prefix`.
- `add_suffix` — suffix every selected column name with `suffix`.
- `replace` — `pattern` (regex) replaced with `replacement` in column names.
- `mapping_from_column` — values of column `mapping_key_column` are the
  OLD names and values of `mapping_value_column` are the NEW names, read
  from a secondary `mapping_asset_key` upstream.
- `mapping` — explicit dict in `mapping:` field (same as select_columns'
  rename, but provided here for one-stop convenience).

`columns:` (optional) restricts the renaming to the listed columns.
"""
import re as _re
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


_MODES = {"first_row", "add_prefix", "add_suffix", "replace", "mapping_from_column", "mapping"}


class DynamicRenameComponent(Component, Model, Resolvable):
    """Pattern-based column renaming with several modes."""

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame")
    mode: str = Field(
        description=(
            "Renaming mode: first_row / add_prefix / add_suffix / replace / "
            "mapping / mapping_from_column"
        ),
    )
    columns: Optional[List[str]] = Field(
        default=None,
        description="Restrict renaming to these columns. None = apply to all columns.",
    )
    prefix: Optional[str] = Field(default=None, description="For mode=add_prefix")
    suffix: Optional[str] = Field(default=None, description="For mode=add_suffix")
    pattern: Optional[str] = Field(default=None, description="For mode=replace (regex)")
    replacement: Optional[str] = Field(
        default="",
        description="For mode=replace (replacement string; backrefs supported)",
    )
    mapping: Optional[Dict[str, str]] = Field(
        default=None,
        description="For mode=mapping: explicit {old: new} dict",
    )
    mapping_asset_key: Optional[str] = Field(
        default=None,
        description="For mode=mapping_from_column: secondary asset providing the rename pairs",
    )
    mapping_key_column: Optional[str] = Field(
        default=None,
        description="For mode=mapping_from_column: column in mapping_asset_key with the OLD names",
    )
    mapping_value_column: Optional[str] = Field(
        default=None,
        description="For mode=mapping_from_column: column in mapping_asset_key with the NEW names",
    )

    group_name: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    asset_tags: Optional[Dict[str, str]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        _self = self
        asset_name = self.asset_name

        if self.mode not in _MODES:
            raise ValueError(f"DynamicRename: unknown mode {self.mode!r}. Valid: {sorted(_MODES)}")

        tags = dict(self.asset_tags or {})
        for k in (self.kinds or ["python"]):
            tags[f"dagster/kind/{k}"] = ""

        # Build ins= dict: always have main upstream, add `mapping` slot
        # only when mode requires it (mapping_from_column).
        ins = {"upstream": AssetIn(key=AssetKey.from_user_string(self.upstream_asset_key))}
        if self.mode == "mapping_from_column":
            if not self.mapping_asset_key:
                raise ValueError(
                    "DynamicRename mode=mapping_from_column requires mapping_asset_key."
                )
            ins["mapping"] = AssetIn(key=AssetKey.from_user_string(self.mapping_asset_key))

        @asset(
            name=asset_name,
            ins=ins,
            group_name=self.group_name,
            description=self.description or f"DynamicRename ({self.mode}) on {self.upstream_asset_key}",
            tags=tags,
            owners=self.owners or [],
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def _asset(context: AssetExecutionContext, **kwargs) -> pd.DataFrame:
            upstream: pd.DataFrame = kwargs["upstream"]
            df = upstream.copy()
            scope = _self.columns if _self.columns else list(df.columns)
            rename_map: Dict[str, str] = {}

            if _self.mode == "first_row":
                if len(df) == 0:
                    context.log.warning("DynamicRename first_row: upstream is empty; nothing to rename.")
                else:
                    first = df.iloc[0]
                    for c in scope:
                        if c in df.columns:
                            new_name = first[c]
                            if pd.notna(new_name) and str(new_name).strip():
                                rename_map[c] = str(new_name).strip()
                    df = df.iloc[1:].reset_index(drop=True)
            elif _self.mode == "add_prefix":
                for c in scope:
                    if c in df.columns:
                        rename_map[c] = (_self.prefix or "") + c
            elif _self.mode == "add_suffix":
                for c in scope:
                    if c in df.columns:
                        rename_map[c] = c + (_self.suffix or "")
            elif _self.mode == "replace":
                if not _self.pattern:
                    raise ValueError("DynamicRename mode=replace requires `pattern`.")
                rx = _re.compile(_self.pattern)
                for c in scope:
                    if c in df.columns:
                        rename_map[c] = rx.sub(_self.replacement or "", c)
            elif _self.mode == "mapping":
                if not _self.mapping:
                    raise ValueError("DynamicRename mode=mapping requires `mapping`.")
                rename_map = {k: v for k, v in (_self.mapping or {}).items() if k in df.columns}
            elif _self.mode == "mapping_from_column":
                mapping_df: pd.DataFrame = kwargs["mapping"]
                if not _self.mapping_key_column or not _self.mapping_value_column:
                    raise ValueError(
                        "DynamicRename mode=mapping_from_column requires "
                        "mapping_key_column + mapping_value_column."
                    )
                pairs = dict(zip(
                    mapping_df[_self.mapping_key_column].astype(str),
                    mapping_df[_self.mapping_value_column].astype(str),
                ))
                rename_map = {k: v for k, v in pairs.items() if k in df.columns}

            df = df.rename(columns=rename_map)
            context.log.info(
                f"dynamic_rename ({_self.mode}): renamed {len(rename_map)} column(s) → "
                f"{list(rename_map.values())[:8]}{'...' if len(rename_map) > 8 else ''}"
            )
            context.add_output_metadata({
                "dagster/row_count": MetadataValue.int(len(df)),
                "columns_renamed": MetadataValue.int(len(rename_map)),
                "rename_map_sample": MetadataValue.json(dict(list(rename_map.items())[:20])),
                "mode": MetadataValue.text(_self.mode),
            })
            return df

        return Definitions(assets=[_asset])

    @classmethod
    def get_description(cls) -> str:
        return cls.__doc__ or ""
