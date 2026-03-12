"""ADLS to Database Asset Component.

Reads a file from Azure Data Lake Storage (ADLS Gen2) and writes it to a
database table via SQLAlchemy. Designed to be triggered by adls_monitor.

Mirrors s3_to_database_asset — same format support, same destination config.
"""
from typing import Optional
import dagster as dg
from dagster import AssetExecutionContext, Config
from pydantic import Field


class ADLSToDatabaseAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Read a file from ADLS Gen2 and write it to a database table.

    Triggered by adls_monitor passing container/blob_name via run_config.

    Example:
        ```yaml
        type: dagster_component_templates.ADLSToDatabaseAssetComponent
        attributes:
          asset_name: adls_orders_ingest
          connection_string_env_var: AZURE_STORAGE_CONNECTION_STRING
          database_url_env_var: DATABASE_URL
          table_name: raw_orders
          file_format: parquet
        ```
    """

    asset_name: str = Field(description="Dagster asset name")
    connection_string_env_var: str = Field(description="Env var with ADLS connection string")
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

        class ADLSFileConfig(Config):
            container_name: str
            blob_name: str
            blob_size: Optional[int] = None

        @dg.asset(
            name=_self.asset_name,
            description=_self.description or f"ADLS → {_self.table_name}",
            group_name=_self.group_name,
            kinds={"adls", "sql"},
        )
        def adls_to_database_asset(context: AssetExecutionContext, config: ADLSFileConfig):
            import os
            from io import BytesIO
            import pandas as pd
            from azure.storage.blob import BlobServiceClient
            from sqlalchemy import create_engine

            conn_str = os.environ[_self.connection_string_env_var]
            db_url = os.environ[_self.database_url_env_var]

            context.log.info(f"Downloading {config.container_name}/{config.blob_name}")
            client = BlobServiceClient.from_connection_string(conn_str)
            blob = client.get_blob_client(container=config.container_name, blob=config.blob_name)
            content = blob.download_blob().readall()

            fmt = _self.file_format
            if fmt == "auto":
                name = config.blob_name.lower()
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
                "container": config.container_name,
                "blob": config.blob_name,
                "table": f"{_self.schema_name + '.' if _self.schema_name else ''}{_self.table_name}",
                "file_format": fmt,
            })

        return dg.Definitions(assets=[adls_to_database_asset])
