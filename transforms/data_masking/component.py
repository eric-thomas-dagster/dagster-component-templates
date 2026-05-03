"""DataMaskingComponent.

Rule-based PII masking — hash, partial-mask (last 4 only), full-redact, or character-substitute. Per-column policies.
"""

import os
from typing import Optional

import dagster as dg
import pandas as pd
from pydantic import Field


class DataMaskingComponent(dg.Component, dg.Model, dg.Resolvable):
    """Rule-based PII masking — hash, partial-mask (last 4 only), full-redact, or character-substitute. Per-column policies."""

    asset_name: str = Field(description="Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream DataFrame asset key")

    rules: list = Field(description="List of {column, method, ...} dicts. method: 'hash' | 'partial' | 'redact' | 'substitute' | 'pseudonymize'")
    salt_env: Optional[str] = Field(default="MASKING_SALT", description="Env var with hash salt (improves resistance to rainbow attacks)")
    redacted_value: str = Field(default="***", description="Replacement value for 'redact' method")

    description: Optional[str] = Field(default=None)
    group_name: str = Field(default="transforms")
    deps: Optional[list[str]] = Field(default=None)
    owners: Optional[list[str]] = Field(default=None)
    asset_tags: Optional[dict] = Field(default=None)
    kinds: Optional[list[str]] = Field(default=None)
    include_preview_metadata: bool = Field(default=True)
    preview_rows: int = Field(default=20)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        @dg.asset(
            name=self.asset_name,
            description=self.description or "Rule-based PII masking — hash, partial-mask (last 4 only), full-redact, or character-substitute. Per-column policies.",
            group_name=self.group_name,
            kinds=set(self.kinds or ['pii', 'masking', 'anonymize']),
            deps=[dg.AssetKey.from_user_string(self.upstream_asset_key)] + [dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
            ins={"df": dg.AssetIn(key=dg.AssetKey.from_user_string(self.upstream_asset_key))},
            owners=self.owners or None,
            tags=self.asset_tags or None,
        )
        def _asset(context: dg.AssetExecutionContext, df: pd.DataFrame) -> pd.DataFrame:
            self = _self
            import hashlib, os, re
            salt = os.environ.get(self.salt_env, "") if self.salt_env else ""
            df = df.copy()
            for rule in self.rules:
                col = rule["column"]
                method = rule["method"]
                if col not in df.columns:
                    continue
                if method == "hash":
                    df[col] = df[col].astype(str).apply(lambda v: hashlib.sha256((salt + v).encode()).hexdigest()[:16])
                elif method == "partial":
                    # keep last N chars
                    keep = rule.get("keep_last", 4)
                    df[col] = df[col].astype(str).apply(lambda v: ("*" * max(0, len(v) - keep)) + v[-keep:] if len(v) > keep else "*" * len(v))
                elif method == "redact":
                    df[col] = self.redacted_value
                elif method == "substitute":
                    pattern = rule.get("pattern", r".")
                    replacement = rule.get("replacement", "*")
                    df[col] = df[col].astype(str).apply(lambda v: re.sub(pattern, replacement, v))
                elif method == "pseudonymize":
                    # consistent random-looking value per input — useful for analytics where uniqueness matters but identity doesn't
                    df[col] = df[col].astype(str).apply(lambda v: f"p_{hashlib.md5((salt + v).encode()).hexdigest()[:12]}")
                else:
                    raise ValueError(f"unknown masking method: {method}")
            context.add_output_metadata({
                "dagster/row_count": dg.MetadataValue.int(len(df)),
            })
            if _self.include_preview_metadata and len(df) > 0:
                try:
                    sample = df.sample(min(_self.preview_rows, len(df))) if len(df) > _self.preview_rows * 10 else df.head(_self.preview_rows)
                    context.add_output_metadata({"preview": dg.MetadataValue.md(sample.to_markdown(index=False))})
                except Exception:
                    pass
            return df

        return dg.Definitions(assets=[_asset])
