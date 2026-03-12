"""GCS to Database Asset Component.

Reads a file from Google Cloud Storage and writes it to a database table
via SQLAlchemy. Designed to be triggered by gcs_monitor.

Mirrors s3_to_database_asset — same format support, same destination config.
"""
from typing import Optional
import dagster as dg
from dagster import AssetExecutionContext, Config
from pydantic import Field


class GCSToDatabaseAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Read a file from GCS and write it to a database table.

    Triggered by gcs_monitor passing bucket/object_name via run_config.

    Example:
        ```yaml
        type: dagster_component_templates.GCSToDatabaseAssetComponent
        attributes:
          asset_name: gcs_events_ingest
          credentials_env_var: GOOGLE_APPLICATION_CREDENTIALS
          database_url_env_var: DATABASE_URL
          table_name: raw_events
          file_format: parquet
        ```
    """

    asset_name: str = Field(description="Dagster asset name")
    credentials_env_var: Optional[str] = Field(
        default=None,
        description="Env var pointing to GCP service account JSON path. If unset, uses Application Default Credentials."
    )
    database_url_env_var: str = Field(description="Env var with SQLAlchemy database URL")
    table_name: str = Field(description="Destination table name")
    schema_name: Optional[str] = Field(default=None, description="Destination schema name")
    if_exists: str = Field(default="append", description="fail, replace, or append")
    file_format: str = Field(default="auto", description="csv, json, parquet, or auto")
    csv_delimiter: str = Field(default=",", description="CSV delimiter")
    column_mapping: Optional[dict] = Field(default=None, description="Rename columns: {old: new}")
    group_name: Optional[str] = Field(default="ingestion", description="Asset group name")
    description: Optional[str] = Field(default=None)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        class GCSObjectConfig(Config):
            bucket_name: str
            object_name: str
            object_size: Optional[int] = None

        @dg.asset(
            name=_self.asset_name,
            description=_self.description or f"GCS → {_self.table_name}",
            group_name=_self.group_name,
            kinds={"gcs", "sql"},
        )
        def gcs_to_database_asset(context: AssetExecutionContext, config: GCSObjectConfig):
            import os
            from io import BytesIO
            import pandas as pd
            from google.cloud import storage
            from sqlalchemy import create_engine

            if _self.credentials_env_var:
                os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", os.environ[_self.credentials_env_var])

            db_url = os.environ[_self.database_url_env_var]

            context.log.info(f"Downloading gs://{config.bucket_name}/{config.object_name}")
            gcs_client = storage.Client()
            blob = gcs_client.bucket(config.bucket_name).blob(config.object_name)
            content = blob.download_as_bytes()

            fmt = _self.file_format
            if fmt == "auto":
                name = config.object_name.lower()
                fmt = "parquet" if name.endswith(".parquet") else "json" if name.endswith(".json") else "csv"

            if fmt == "parquet":
                df = pd.read_parquet(BytesIO(content))
            elif fmt == "json":
                df = pd.read_json(BytesIO(content), orient="records")
            else:
                df = pd.read_csv(BytesIO(content), delimiter=_self.csv_delimiter)

            context.log.info(f"Loaded {len(df)} rows, {len(df.columns)} columns")

            if _self.column_mapping:
                df = df.rename(columns=_self.column_mapping)

            engine = create_engine(db_url)
            df.to_sql(_self.table_name, con=engine, schema=_self.schema_name,
                      if_exists=_self.if_exists, index=False, method="multi", chunksize=1000)

            context.log.info(f"Wrote {len(df)} rows to {_self.schema_name + '.' if _self.schema_name else ''}{_self.table_name}")
            return dg.MaterializeResult(metadata={
                "num_rows": len(df),
                "num_columns": len(df.columns),
                "columns": list(df.columns),
                "bucket": config.bucket_name,
                "object": config.object_name,
                "table": f"{_self.schema_name + '.' if _self.schema_name else ''}{_self.table_name}",
                "file_format": fmt,
            })

        return dg.Definitions(assets=[gcs_to_database_asset])
