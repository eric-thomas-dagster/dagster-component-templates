"""BlobConvert.

Convert a column of binary blobs (bytes) between common encodings:
  - `to_base64` — bytes → base64-encoded string
  - `from_base64` — base64 string → bytes
  - `to_hex` — bytes → hex string
  - `from_hex` — hex string → bytes
  - `to_text` — bytes → decoded string (utf-8 by default)
  - `to_bytes` — text → bytes (utf-8 by default)
"""
import base64
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


_OPS = {"to_base64", "from_base64", "to_hex", "from_hex", "to_text", "to_bytes"}


class BlobConvertComponent(Component, Model, Resolvable):
    """Convert a blob/text column between common binary encodings."""

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame")
    input_column: str = Field(description="Column containing the input bytes / text")
    operation: str = Field(
        description="One of: to_base64, from_base64, to_hex, from_hex, to_text, to_bytes",
    )
    output_column: Optional[str] = Field(
        default=None,
        description="Output column name (defaults to overwriting input_column)",
    )
    encoding: str = Field(
        default="utf-8",
        description="Text codec for to_text / to_bytes operations (default utf-8)",
    )
    error_handling: str = Field(
        default="coerce",
        description=(
            "How to handle conversion errors per row: "
            "'coerce' (set None and log; default), 'raise' (fail asset)."
        ),
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

        if self.operation not in _OPS:
            raise ValueError(
                f"BlobConvert: unknown operation {self.operation!r}. Valid: {sorted(_OPS)}"
            )

        tags = dict(self.asset_tags or {})
        for k in (self.kinds or ["python", "binary"]):
            tags[f"dagster/kind/{k}"] = ""

        @asset(
            name=asset_name,
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(self.upstream_asset_key))},
            group_name=self.group_name,
            description=self.description or f"BlobConvert {self.operation} on column {self.input_column}",
            tags=tags,
            owners=self.owners or [],
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def _asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
            df = upstream.copy()
            if _self.input_column not in df.columns:
                context.log.warning(
                    f"BlobConvert: input_column {_self.input_column!r} not in upstream. "
                    f"Available: {list(df.columns)[:10]}. Returning upstream unchanged."
                )
                return df
            out_col = _self.output_column or _self.input_column

            def _convert(v):
                try:
                    if v is None or (isinstance(v, float) and pd.isna(v)):
                        return None
                    if _self.operation == "to_base64":
                        if isinstance(v, str):
                            v = v.encode(_self.encoding)
                        return base64.b64encode(v).decode("ascii")
                    if _self.operation == "from_base64":
                        return base64.b64decode(v)
                    if _self.operation == "to_hex":
                        if isinstance(v, str):
                            v = v.encode(_self.encoding)
                        return v.hex()
                    if _self.operation == "from_hex":
                        return bytes.fromhex(v)
                    if _self.operation == "to_text":
                        if isinstance(v, str):
                            return v
                        return v.decode(_self.encoding, errors="strict")
                    if _self.operation == "to_bytes":
                        if isinstance(v, (bytes, bytearray)):
                            return bytes(v)
                        return str(v).encode(_self.encoding)
                except Exception:
                    if _self.error_handling == "raise":
                        raise
                    return None
                return None

            df[out_col] = df[_self.input_column].apply(_convert)
            null_count = int(df[out_col].isna().sum())
            context.log.info(
                f"blob_convert: {_self.operation} on {_self.input_column!r} → {out_col!r}; "
                f"{len(df)} rows, {null_count} null/failed."
            )
            context.add_output_metadata({
                "dagster/row_count": MetadataValue.int(len(df)),
                "operation": MetadataValue.text(_self.operation),
                "null_or_failed": MetadataValue.int(null_count),
            })
            return df

        return Definitions(assets=[_asset])

    @classmethod
    def get_description(cls) -> str:
        return cls.__doc__ or ""
