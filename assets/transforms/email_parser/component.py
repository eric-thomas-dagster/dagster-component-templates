from dataclasses import dataclass
from typing import Optional, List
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


DEFAULT_FIELDS = ["from", "to", "subject", "date", "body"]


@dataclass
class EmailParserComponent(Component, Model, Resolvable):
    """Parse raw email content (RFC 2822 format) from a column into structured fields."""

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame")
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name")
    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', or None for unpartitioned",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.",
    )
    partition_date_column: Optional[str] = Field(
        default=None,
        description="Column used to filter upstream DataFrame to the current date partition key.",
    )
    partition_values: Optional[str] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning, e.g. 'customer_a,customer_b,customer_c'.",
    )
    partition_static_dim: Optional[str] = Field(
        default=None,
        description="Dimension name for the static axis in multi-partitioning, e.g. 'customer' or 'region'.",
    )
    partition_static_column: Optional[str] = Field(
        default=None,
        description="Column used to filter upstream DataFrame to the current static partition dimension (e.g. 'customer_id').",
    )
    column: str = Field(description="Column containing raw email strings (RFC 2822 format)")
    extract_fields: List[str] = Field(
        default_factory=lambda: list(DEFAULT_FIELDS),
        description="Which fields to extract: from, to, subject, date, body.",
    )
    output_prefix: str = Field(
        default="",
        description="Prefix for output column names, e.g. 'email_' → 'email_from', 'email_subject'.",
    )
    drop_source: bool = Field(
        default=False,
        description="Drop the source column after parsing.",
    )

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        group_name = self.group_name
        column = self.column
        extract_fields = self.extract_fields
        output_prefix = self.output_prefix
        drop_source = self.drop_source

        # Build partition definition
        partitions_def = None
        if self.partition_type:
            from dagster import (
                DailyPartitionsDefinition, WeeklyPartitionsDefinition,
                MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
                StaticPartitionsDefinition, MultiPartitionsDefinition,
            )
            _start = self.partition_start or "2020-01-01"
            _values = [v.strip() for v in (self.partition_values or "").split(",") if v.strip()]
            if self.partition_type == "daily":
                partitions_def = DailyPartitionsDefinition(start_date=_start)
            elif self.partition_type == "weekly":
                partitions_def = WeeklyPartitionsDefinition(start_date=_start)
            elif self.partition_type == "monthly":
                partitions_def = MonthlyPartitionsDefinition(start_date=_start)
            elif self.partition_type == "hourly":
                partitions_def = HourlyPartitionsDefinition(start_date=_start)
            elif self.partition_type == "static":
                partitions_def = StaticPartitionsDefinition(_values)
            elif self.partition_type == "multi":
                _dim = self.partition_static_dim or "segment"
                partitions_def = MultiPartitionsDefinition({
                    "date": DailyPartitionsDefinition(start_date=_start),
                    _dim: StaticPartitionsDefinition(_values),
                })
        partition_type = self.partition_type
        partition_date_column = self.partition_date_column
        partition_static_column = self.partition_static_column
        partition_static_dim = self.partition_static_dim

        @asset(
            name=asset_name,
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
            partitions_def=partitions_def,
            group_name=group_name,
        )
        def _asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
            # Filter to current partition if partitioned
            if context.has_partition_key:
                _pk = context.partition_key
                _is_multi = hasattr(_pk, "keys_by_dimension")
                _date_key = _pk.keys_by_dimension.get("date", "") if _is_multi else str(_pk)
                _static_key = _pk.keys_by_dimension.get(partition_static_dim or "segment", "") if _is_multi else None
                if partition_date_column and partition_date_column in upstream.columns and _date_key:
                    upstream = upstream[upstream[partition_date_column].astype(str) == _date_key]
                if partition_static_column and partition_static_column in upstream.columns and _static_key:
                    upstream = upstream[upstream[partition_static_column].astype(str) == _static_key]
                elif partition_static_column and partition_static_column in upstream.columns and not _is_multi:
                    upstream = upstream[upstream[partition_static_column].astype(str) == str(_pk)]
            import email as _email
            from email import policy as _policy

            if column not in upstream.columns:
                raise ValueError(f"Column '{column}' not found in DataFrame.")

            def get_body(msg) -> str:
                if msg.is_multipart():
                    parts = []
                    for part in msg.walk():
                        ct = part.get_content_type()
                        if ct == "text/plain":
                            try:
                                parts.append(part.get_payload(decode=True).decode(
                                    part.get_content_charset() or "utf-8", errors="replace"
                                ))
                            except Exception:
                                parts.append(str(part.get_payload()))
                    return "\n".join(parts)
                else:
                    try:
                        payload = msg.get_payload(decode=True)
                        if payload:
                            return payload.decode(msg.get_content_charset() or "utf-8", errors="replace")
                    except Exception:
                        pass
                    return str(msg.get_payload())

            def parse_email(raw: object) -> dict:
                if raw is None or (isinstance(raw, float) and pd.isna(raw)):
                    return {f: None for f in extract_fields}
                try:
                    msg = _email.message_from_string(str(raw), policy=_policy.compat32)
                    result = {}
                    for field in extract_fields:
                        if field == "body":
                            result[field] = get_body(msg)
                        else:
                            result[field] = msg.get(field)
                    return result
                except Exception as e:
                    context.log.warning(f"Failed to parse email: {e}")
                    return {f: None for f in extract_fields}

            df = upstream.copy()
            parsed = df[column].apply(parse_email)

            for field in extract_fields:
                out_col = f"{output_prefix}{field}"
                df[out_col] = parsed.apply(lambda p: p.get(field))

            if drop_source:
                df = df.drop(columns=[column])

            context.add_output_metadata({
                "row_count": MetadataValue.int(len(df)),
                "columns": MetadataValue.int(len(df.columns)),
            })
            return df

        return Definitions(assets=[_asset])
