"""BigQueryExportToGcsAssetComponent — export a BQ table or query to GCS.

Wraps BigQuery's EXTRACT job + ExportData / EXPORT DATA SQL to push a
table or query result out to GCS as parquet, CSV, JSONL, or AVRO.
The classic warehouse → lake pattern at the end of an ELT pipeline.

Two source modes:
  - source_table_id: an existing BQ table — uses the BQ EXTRACT job
    (faster, supports compression / wildcards / sharding).
  - source_query: a SELECT — uses EXPORT DATA OPTIONS(...) AS <query>
    (more flexible, runs the query then exports).

Returns a small DataFrame describing what was exported (URIs, sizes,
row count) for downstream lineage / dependent assets.
"""

import json
import os
from typing import Any, Dict, List, Literal, Optional

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


class BigQueryExportToGcsAssetComponent(Component, Model, Resolvable):
    """Export a BQ table or query result to GCS — the warehouse → lake step."""

    asset_name: str = Field(description="Output asset name.")
    credentials: Optional[Dict[str, Any]] = Field(default=None)
    credentials_path: Optional[str] = Field(default=None, description="Falls back to GOOGLE_APPLICATION_CREDENTIALS.")

    project_id: Optional[str] = Field(default=None, description="Billing project. Defaults to the SA's project.")
    location: Optional[str] = Field(default=None, description="BQ location. Default: auto-detect.")

    # Source: table or query
    source_table_id: Optional[str] = Field(
        default=None,
        description="Fully-qualified BQ table id to extract. Mutually exclusive with source_query.",
    )
    source_query: Optional[str] = Field(
        default=None,
        description="SELECT to export via EXPORT DATA. Supports {placeholder} substitution.",
    )
    query_params: Dict[str, Any] = Field(default_factory=dict)

    # Destination
    destination_uri: str = Field(
        description=(
            "GCS URI prefix, e.g. `gs://bucket/path/output_*.parquet`. The `*` wildcard "
            "is required when the export will produce multiple shards (BQ shards on "
            "extracts > ~1 GB)."
        ),
    )
    format: Literal["parquet", "csv", "json", "newline_delimited_json", "avro"] = Field(
        default="parquet",
        description="Output format (json == newline_delimited_json — JSONL).",
    )
    compression: Optional[Literal["gzip", "snappy", "deflate", "zstd", "none"]] = Field(
        default=None,
        description="Compression codec. parquet supports snappy/gzip/zstd; csv+jsonl support gzip; avro supports deflate/snappy.",
    )
    csv_field_delimiter: Optional[str] = Field(default=None, description="CSV-only: field delimiter (default ',').")
    csv_print_header: bool = Field(default=True, description="CSV-only: include the header row.")

    overwrite: bool = Field(
        default=True,
        description="Allow overwriting existing GCS objects at the destination URI.",
    )

    deps: Optional[List[str]] = Field(default=None)
    description: Optional[str] = Field(default=None)
    group_name: Optional[str] = Field(default=None)
    tags: Optional[Dict[str, str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        if not self.source_table_id and not self.source_query:
            raise ValueError("BigQueryExportToGcsAssetComponent: set source_table_id or source_query.")
        if self.source_table_id and self.source_query:
            raise ValueError("BigQueryExportToGcsAssetComponent: set source_table_id OR source_query, not both.")

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
        location = self.location
        source_table_id = self.source_table_id
        source_query = self.source_query
        query_params = dict(self.query_params or {})
        destination_uri = self.destination_uri
        out_format = self.format
        compression = self.compression
        csv_field_delimiter = self.csv_field_delimiter
        csv_print_header = self.csv_print_header
        overwrite = self.overwrite

        @asset(
            name=asset_name,
            description=self.description or f"BQ → GCS export ({out_format}) → {destination_uri}",
            group_name=self.group_name,
            kinds={"bigquery", "gcs", "export"},
            tags=self.tags or None,
            owners=self.owners or None,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])] or None,
        )
        def _asset(context: AssetExecutionContext) -> Output:
            try:
                from google.cloud import bigquery, storage
                from google.oauth2 import service_account
            except ImportError:
                raise ImportError("pip install google-cloud-bigquery google-cloud-storage google-auth")

            sa_creds = service_account.Credentials.from_service_account_info(creds_dict)
            bq = bigquery.Client(credentials=sa_creds, project=project_id, location=location)

            # If overwrite=False, fail-fast if any objects exist at the destination prefix.
            if not overwrite:
                gcs = storage.Client(credentials=sa_creds, project=project_id)
                # Strip protocol + wildcard.
                if destination_uri.startswith("gs://"):
                    no_scheme = destination_uri[len("gs://"):]
                    bkt_name, _, prefix = no_scheme.partition("/")
                    prefix_for_check = prefix.split("*")[0] if "*" in prefix else prefix
                    existing = list(gcs.list_blobs(bkt_name, prefix=prefix_for_check, max_results=1))
                    if existing:
                        raise FileExistsError(
                            f"GCS objects already exist at prefix {prefix_for_check!r} in {bkt_name!r} "
                            f"and overwrite=False. Set overwrite=true or pick a different destination_uri."
                        )

            bq_format_map = {
                "parquet":                 "PARQUET",
                "csv":                     "CSV",
                "json":                    "NEWLINE_DELIMITED_JSON",
                "newline_delimited_json":  "NEWLINE_DELIMITED_JSON",
                "avro":                    "AVRO",
            }
            bq_format = bq_format_map[out_format]
            comp_codec = compression.upper() if compression and compression != "none" else None

            if source_table_id:
                # EXTRACT job — fastest path for whole tables.
                job_config = bigquery.ExtractJobConfig()
                job_config.destination_format = bq_format
                if comp_codec:
                    job_config.compression = comp_codec
                if out_format == "csv":
                    if csv_field_delimiter:
                        job_config.field_delimiter = csv_field_delimiter
                    job_config.print_header = csv_print_header

                context.log.info(f"BQ EXTRACT {source_table_id} → {destination_uri} ({bq_format})")
                job = bq.extract_table(
                    source_table_id, destination_uris=destination_uri, job_config=job_config,
                )
                job.result()
                source_descr = source_table_id
                # ExtractJob does not expose total_bytes_processed/billed —
                # extracts are free for the BQ side. Surface input table size
                # instead so the metadata shows what got exported.
                try:
                    src_t = bq.get_table(source_table_id)
                    bytes_processed = int(src_t.num_bytes or 0)
                except Exception:
                    bytes_processed = 0
                bytes_billed = 0  # extract jobs are free
                slot_millis = 0
            else:
                # EXPORT DATA SQL — runs the query, exports results.
                try:
                    rendered_query = source_query.format(**query_params) if query_params else source_query
                except KeyError as e:
                    raise ValueError(f"source_query references missing placeholder {e}; available: {list(query_params.keys())}")

                opts = [
                    f"uri={json.dumps(destination_uri).replace(chr(34), chr(39))}",
                    f"format={json.dumps(bq_format).replace(chr(34), chr(39))}",
                    f"overwrite={'true' if overwrite else 'false'}",
                ]
                if comp_codec:
                    opts.append(f"compression={json.dumps(comp_codec).replace(chr(34), chr(39))}")
                if out_format == "csv":
                    if csv_field_delimiter:
                        opts.append(f"field_delimiter={json.dumps(csv_field_delimiter).replace(chr(34), chr(39))}")
                    opts.append(f"header={'true' if csv_print_header else 'false'}")
                ddl = f"EXPORT DATA OPTIONS({', '.join(opts)}) AS\n{rendered_query}"
                context.log.info(f"BQ EXPORT DATA → {destination_uri} ({bq_format})")
                job = bq.query(ddl)
                job.result()
                source_descr = "<query>"
                bytes_processed = int(job.total_bytes_processed or 0)
                bytes_billed = int(job.total_bytes_billed or 0)
                slot_millis = int(job.slot_millis or 0)

            # Inspect resulting GCS objects.
            object_rows = []
            try:
                gcs = storage.Client(credentials=sa_creds, project=project_id)
                no_scheme = destination_uri[len("gs://"):]
                bkt_name, _, prefix = no_scheme.partition("/")
                prefix_for_list = prefix.split("*")[0] if "*" in prefix else prefix
                for blob in gcs.list_blobs(bkt_name, prefix=prefix_for_list, max_results=100):
                    object_rows.append({
                        "uri":           f"gs://{blob.bucket.name}/{blob.name}",
                        "size_bytes":    int(blob.size or 0),
                        "content_type":  blob.content_type,
                        "updated":       blob.updated.isoformat() if blob.updated else None,
                    })
            except Exception as e:
                context.log.warning(f"could not list resulting GCS objects: {e}")

            df = pd.DataFrame(object_rows)
            md = {
                "source":              MetadataValue.text(source_descr),
                "destination_uri":     MetadataValue.text(destination_uri),
                "format":              MetadataValue.text(out_format),
                "compression":         MetadataValue.text(compression or "none"),
                "object_count":        MetadataValue.int(len(df)),
                "total_size_bytes":    MetadataValue.int(int(df.get("size_bytes", pd.Series(dtype=int)).sum())),
                "bytes_processed":     MetadataValue.int(bytes_processed),
                "bytes_billed":        MetadataValue.int(bytes_billed),
                "slot_millis":         MetadataValue.int(slot_millis),
                "preview":             MetadataValue.md(df.head(20).to_markdown(index=False) if not df.empty else "(no objects)"),
            }
            return Output(value=df, metadata=md)

        return Definitions(assets=[_asset])
