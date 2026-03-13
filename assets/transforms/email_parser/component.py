from dataclasses import dataclass
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
    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners — list of team names or email addresses, e.g. ['team:analytics', 'user@company.com']",
    )
    asset_tags: Optional[Dict[str, str]] = Field(
        default=None,
        description="Additional key-value tags to apply to the asset, e.g. {'domain': 'finance', 'tier': 'gold'}",
    )
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds for the Dagster catalog, e.g. ['snowflake', 'python']. Auto-inferred from component name if not set.",
    )
    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale. Defines a FreshnessPolicy.",
    )
    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5' (weekdays at 9am).",
    )
    column_lineage: Optional[Dict[str, List[str]]] = Field(
        default=None,
        description="Column-level lineage mapping: output column name → list of upstream column names it was derived from, e.g. {'revenue': ['price', 'quantity']}",
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

        # Infer kinds from component name if not explicitly set
        _comp_name = "email_parser"  # component directory name
        _kind_map = {
            "snowflake": "snowflake", "bigquery": "bigquery", "redshift": "redshift",
            "postgres": "postgres", "postgresql": "postgres", "mysql": "mysql",
            "s3": "s3", "adls": "azure", "azure": "azure", "gcs": "gcp",
            "google": "gcp", "databricks": "databricks", "dbt": "dbt",
            "kafka": "kafka", "mongodb": "mongodb", "redis": "redis",
            "neo4j": "neo4j", "elasticsearch": "elasticsearch", "pinecone": "pinecone",
            "chromadb": "chromadb", "pgvector": "postgres",
        }
        _inferred_kinds = self.kinds or []
        if not _inferred_kinds:
            _comp_lower = asset_name.lower()
            for keyword, kind in _kind_map.items():
                if keyword in _comp_lower:
                    _inferred_kinds.append(kind)
            if not _inferred_kinds:
                _inferred_kinds = ["python"]

        # Build combined tags: user tags + kind tags
        _all_tags = dict(self.asset_tags or {})
        for _kind in _inferred_kinds:
            _all_tags[f"dagster/kind/{_kind}"] = ""

        # Build freshness policy
        _freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy
            _freshness_policy = FreshnessPolicy(
                maximum_lag_minutes=self.freshness_max_lag_minutes,
                cron_schedule=self.freshness_cron,
            )

        owners = self.owners or []
        column_lineage = self.column_lineage if hasattr(self, 'column_lineage') else None


        @asset(
            name=asset_name,
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
            partitions_def=partitions_def,
                        owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
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


                # Build column schema metadata
                from dagster import TableSchema, TableColumn, TableColumnLineage, TableColumnDep
                _col_schema = TableSchema(columns=[
                    TableColumn(name=str(col), type=str(df.dtypes[col]))
                    for col in df.columns
                ])
                _metadata = {
                    "dagster/row_count": MetadataValue.int(len(df)),
                    "dagster/column_schema": MetadataValue.table_schema(_col_schema),
                }
                if column_lineage:
                    _upstream_key = AssetKey.from_user_string(upstream_asset_key) if upstream_asset_key else None
                    if _upstream_key:
                        _lineage_deps = {}
                        for out_col, in_cols in column_lineage.items():
                            _lineage_deps[out_col] = [
                                TableColumnDep(asset_key=_upstream_key, column_name=ic)
                                for ic in in_cols
                            ]
                        _metadata["dagster/column_lineage"] = MetadataValue.table_column_lineage(
                            TableColumnLineage(_lineage_deps)
                        )
                context.add_output_metadata(_metadata)
            return df

        from dagster import build_column_schema_change_checks


        _schema_checks = build_column_schema_change_checks(assets=[_asset])


        return Definitions(assets=[_asset], asset_checks=list(_schema_checks))
