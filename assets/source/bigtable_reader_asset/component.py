"""BigtableReaderAssetComponent — read rows from a Cloud Bigtable table into a DataFrame.

Bigtable is GCP's wide-column NoSQL. This component pulls a row range (or
prefix scan) into a pandas DataFrame, one Bigtable row per output row,
each column-family/qualifier as a column (`<family>:<qualifier>`).

Supports:
  - row-key prefix scan (`row_key_prefix`)
  - row-key range scan (`start_key` / `end_key`)
  - column-family filter (`column_families`)
  - row limit (`limit`)

For richer filters (cell-version, regex, timestamp range), build a custom
asset on top of `google-cloud-bigtable` — this component covers the common
batch-read patterns.
"""

import json
import os
from typing import Any, Dict, List, Optional

import pandas as pd

from dagster import (
    AssetExecutionContext,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    MetadataValue,
    Model,
    Output,
    Resolvable,
    asset,
)
from pydantic import Field


class BigtableReaderAssetComponent(Component, Model, Resolvable):
    """Read rows from a Bigtable table into a DataFrame."""

    asset_name: str = Field(description="Output asset name.")

    credentials: Optional[Dict[str, Any]] = Field(default=None)
    credentials_path: Optional[str] = Field(default=None)

    project_id: Optional[str] = Field(default=None)
    instance_id: str = Field(description="Bigtable instance id.")
    table_id: str = Field(description="Bigtable table id.")

    row_key_prefix: Optional[str] = Field(
        default=None,
        description="Row-key prefix scan. Mutually exclusive with start_key/end_key.",
    )
    start_key: Optional[str] = Field(default=None, description="Start of row-key range (inclusive).")
    end_key: Optional[str] = Field(default=None, description="End of row-key range (exclusive).")

    column_families: Optional[List[str]] = Field(
        default=None,
        description="Optional list of column families to read. Default: all.",
    )

    limit: Optional[int] = Field(default=None, description="Max rows to return.")

    decode_values_as: str = Field(
        default="utf-8",
        description="How to decode cell values. 'utf-8' (default), 'bytes' (keep raw), or 'json' (parse JSON cells).",
    )

    description: Optional[str] = Field(default=None)
    group_name: Optional[str] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)
    tags: Optional[Dict[str, str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        creds_dict = self.credentials
        if creds_dict is None:
            cred_path = self.credentials_path or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
            if cred_path:
                with open(cred_path, "r") as fh:
                    creds_dict = json.load(fh)
        if creds_dict is None:
            raise ValueError("Provide credentials, credentials_path, or set GOOGLE_APPLICATION_CREDENTIALS.")

        asset_name = self.asset_name
        project_id = self.project_id or creds_dict.get("project_id")
        instance_id = self.instance_id
        table_id = self.table_id
        row_key_prefix = self.row_key_prefix
        start_key = self.start_key
        end_key = self.end_key
        column_families = self.column_families
        limit = self.limit
        decode_as = self.decode_values_as

        @asset(
            name=asset_name,
            description=self.description or f"Bigtable read: {project_id}/{instance_id}/{table_id}.",
            group_name=self.group_name,
            kinds={"google", "bigtable"},
            tags=self.tags or None,
            owners=self.owners or None,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])] or None,
        )
        def _asset(context: AssetExecutionContext) -> Output:
            try:
                from google.cloud import bigtable
                from google.cloud.bigtable.row_set import RowSet, RowRange
                from google.cloud.bigtable.row_filters import (
                    FamilyNameRegexFilter, RowFilterUnion,
                )
                from google.oauth2 import service_account
            except ImportError:
                raise ImportError("pip install google-cloud-bigtable google-auth")

            sa_creds = service_account.Credentials.from_service_account_info(creds_dict)
            client = bigtable.Client(project=project_id, credentials=sa_creds, admin=False)
            instance = client.instance(instance_id)
            table = instance.table(table_id)

            row_set = RowSet()
            if row_key_prefix:
                row_set.add_row_range_with_prefix(row_key_prefix)
            elif start_key or end_key:
                row_set.add_row_range(RowRange(
                    start_key=start_key.encode() if start_key else None,
                    end_key=end_key.encode() if end_key else None,
                ))

            row_filter = None
            if column_families:
                filters = [FamilyNameRegexFilter(f"^{cf}$") for cf in column_families]
                row_filter = filters[0] if len(filters) == 1 else RowFilterUnion(filters)

            context.log.info(f"Bigtable scan → {project_id}/{instance_id}/{table_id}")

            rows = table.read_rows(row_set=row_set, filter_=row_filter, limit=limit)
            out_rows: List[Dict[str, Any]] = []
            for r in rows:
                out: Dict[str, Any] = {"_row_key": r.row_key.decode("utf-8", errors="replace")}
                for family_name, cols in (r.cells or {}).items():
                    for col_qual, cells in cols.items():
                        if not cells:
                            continue
                        cell = cells[0]  # latest version only
                        col_name = f"{family_name}:{col_qual.decode('utf-8', errors='replace')}"
                        raw = cell.value
                        if decode_as == "bytes":
                            out[col_name] = raw
                        elif decode_as == "json":
                            try:
                                out[col_name] = json.loads(raw.decode("utf-8"))
                            except Exception:
                                out[col_name] = raw.decode("utf-8", errors="replace")
                        else:
                            out[col_name] = raw.decode(decode_as, errors="replace")
                out_rows.append(out)

            df = pd.DataFrame(out_rows)
            preview = df.head(10).to_markdown(index=False) if not df.empty else "(no rows)"
            return Output(
                value=df,
                metadata={
                    "instance":  MetadataValue.text(instance_id),
                    "table":     MetadataValue.text(table_id),
                    "row_count": MetadataValue.int(len(df)),
                    "columns":   MetadataValue.json(list(df.columns) if not df.empty else []),
                    "preview":   MetadataValue.md(preview or ""),
                },
            )

        return Definitions(assets=[_asset])
