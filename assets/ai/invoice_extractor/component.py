"""Invoice Extractor Component.

Extract structured fields from invoice text using an LLM via litellm.
"""

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


@dataclass
class InvoiceExtractorComponent(Component, Model, Resolvable):
    """Component for extracting structured data from invoice text using LLMs.

    Each row in the upstream DataFrame is treated as one invoice. The LLM
    extracts the requested fields and returns them as additional columns.

    Features:
    - Configurable output fields (invoice_number, date, vendor, total, etc.)
    - Works with any litellm-compatible model (OpenAI, Anthropic, etc.)
    - Batch processing with configurable batch size
    - Null-safe: missing fields become None rather than raising errors
    - Supports raw text or file path inputs

    Use Cases:
    - Accounts payable automation
    - Invoice processing pipelines
    - Vendor data extraction
    - Spend analytics
    """

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(
        description="Upstream asset key providing a DataFrame with invoice content"
    )
    input_column: str = Field(
        default="invoice_text",
        description="Column with invoice content (text or file path)",
    )
    input_type: str = Field(
        default="text",
        description="Input type: 'text' (raw invoice text) or 'file' (PDF/image path)",
    )
    model: str = Field(default="gpt-4o", description="LLM model name (litellm format)")
    api_key_env_var: str = Field(
        default="OPENAI_API_KEY",
        description="Environment variable name holding the API key",
    )
    output_fields: List[str] = Field(
        default=[
            "invoice_number",
            "date",
            "vendor",
            "total_amount",
            "line_items",
            "tax",
            "currency",
        ],
        description="Fields to extract from each invoice",
    )
    batch_size: int = Field(default=5, description="Number of invoices per LLM batch")
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

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        input_column = self.input_column
        input_type = self.input_type
        model = self.model
        api_key_env_var = self.api_key_env_var
        output_fields = self.output_fields
        batch_size = self.batch_size

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
            group_name=self.group_name,
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
            import os
            import json

            try:
                from litellm import completion
            except ImportError:
                raise ImportError("litellm required: pip install litellm")

            df = upstream.copy()
            results = []
            total = len(df)
            context.log.info(
                f"Extracting invoice fields from {total} rows using {model}"
            )

            for i in range(0, total, batch_size):
                batch = df.iloc[i : i + batch_size]
                context.log.info(
                    f"Processing batch {i // batch_size + 1}/{(total - 1) // batch_size + 1}"
                )
                for _, row in batch.iterrows():
                    content = str(row[input_column])

                    if input_type == "file":
                        try:
                            with open(content, "r", encoding="utf-8", errors="replace") as fh:
                                content = fh.read()
                        except Exception as e:
                            context.log.warning(f"Could not read file {content}: {e}")

                    prompt = (
                        f"Extract the following fields from this invoice as JSON: {output_fields}\n\n"
                        f"Invoice content:\n{content}\n\n"
                        "Return only a JSON object with the requested fields. Use null for missing fields."
                    )

                    try:
                        resp = completion(
                            model=model,
                            messages=[{"role": "user", "content": prompt}],
                            api_key=os.environ.get(api_key_env_var),
                        )
                        raw = resp.choices[0].message.content
                        # Strip markdown code fences if present
                        raw = raw.strip()
                        if raw.startswith("```"):
                            raw = raw.split("```")[1]
                            if raw.startswith("json"):
                                raw = raw[4:]
                        extracted = json.loads(raw)
                    except Exception as e:
                        context.log.warning(f"Extraction failed for row: {e}")
                        extracted = {f: None for f in output_fields}

                    results.append(extracted)

            extracted_df = pd.DataFrame(results)
            for col in extracted_df.columns:
                df[col] = extracted_df[col].values

            context.add_output_metadata(
                {
                    "num_invoices": total,
                    "extracted_fields": output_fields,
                    "model": model,
                    "preview": MetadataValue.md(df.head(5).to_markdown()),
                }
            )
            return df

        return Definitions(assets=[_asset])
