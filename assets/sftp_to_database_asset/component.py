"""SFTP to Database Asset Component.

Downloads a file from an SFTP server and writes it to a database table
via SQLAlchemy. Designed to be triggered by sftp_monitor.

Mirrors s3_to_database_asset — same format support, same destination config.
"""
from typing import Optional
import dagster as dg
from dagster import AssetExecutionContext, Config
from pydantic import Field


class SFTPToDatabaseAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Download a file from SFTP and write it to a database table.

    Triggered by sftp_monitor passing remote_path/filename via run_config.

    Example:
        ```yaml
        type: dagster_component_templates.SFTPToDatabaseAssetComponent
        attributes:
          asset_name: sftp_orders_ingest
          host_env_var: SFTP_HOST
          username_env_var: SFTP_USERNAME
          password_env_var: SFTP_PASSWORD
          database_url_env_var: DATABASE_URL
          table_name: raw_orders
          file_format: csv
        ```
    """

    asset_name: str = Field(description="Dagster asset name")
    host_env_var: str = Field(description="Env var with SFTP host")
    username_env_var: str = Field(description="Env var with SFTP username")
    password_env_var: Optional[str] = Field(default=None, description="Env var with SFTP password")
    private_key_env_var: Optional[str] = Field(default=None, description="Env var with path to SSH private key file")
    port: int = Field(default=22, description="SFTP port")
    database_url_env_var: str = Field(description="Env var with SQLAlchemy database URL")
    table_name: str = Field(description="Destination table name")
    schema_name: Optional[str] = Field(default=None, description="Destination schema name")
    if_exists: str = Field(default="append", description="fail, replace, or append")
    file_format: str = Field(default="auto", description="csv, json, parquet, or auto")
    csv_delimiter: str = Field(default=",", description="CSV delimiter")
    column_mapping: Optional[dict] = Field(default=None, description="Rename columns: {old: new}")
    group_name: Optional[str] = Field(default="ingestion", description="Asset group name")
    description: Optional[str] = Field(default=None)
    partition_type: str = Field(default="none", description="none, daily, weekly, or monthly")
    partition_start_date: Optional[str] = Field(default=None, description="Partition start date YYYY-MM-DD (required if partition_type != none)")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        partitions_def = None
        if _self.partition_type == "daily":
            partitions_def = dg.DailyPartitionsDefinition(start_date=_self.partition_start_date or "2020-01-01")
        elif _self.partition_type == "weekly":
            partitions_def = dg.WeeklyPartitionsDefinition(start_date=_self.partition_start_date or "2020-01-01")
        elif _self.partition_type == "monthly":
            partitions_def = dg.MonthlyPartitionsDefinition(start_date=_self.partition_start_date or "2020-01-01")

        class SFTPFileConfig(Config):
            remote_path: str      # full remote file path
            filename: str
            file_size: Optional[int] = None
            modified_time: Optional[int] = None

        @dg.asset(
            name=_self.asset_name,
            description=_self.description or f"SFTP → {_self.table_name}",
            group_name=_self.group_name,
            kinds={"sftp", "sql"},
            partitions_def=partitions_def,
        )
        def sftp_to_database_asset(context: AssetExecutionContext, config: SFTPFileConfig):
            import os
            from io import BytesIO
            import paramiko
            import pandas as pd
            from sqlalchemy import create_engine

            host = os.environ[_self.host_env_var]
            username = os.environ[_self.username_env_var]
            db_url = os.environ[_self.database_url_env_var]

            remote_path = config.remote_path
            if context.has_partition_key:
                remote_path = remote_path.replace("{partition_key}", context.partition_key)

            context.log.info(f"Downloading {remote_path} from {host}")

            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            connect_kwargs: dict = {"username": username, "port": _self.port}
            if _self.private_key_env_var:
                connect_kwargs["key_filename"] = os.environ[_self.private_key_env_var]
            elif _self.password_env_var:
                connect_kwargs["password"] = os.environ[_self.password_env_var]

            ssh.connect(host, **connect_kwargs)
            sftp = ssh.open_sftp()
            buf = BytesIO()
            sftp.getfo(remote_path, buf)
            sftp.close()
            ssh.close()
            content = buf.getvalue()

            fmt = _self.file_format
            if fmt == "auto":
                name = config.filename.lower()
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

            table_name = _self.table_name
            if context.has_partition_key:
                table_name = table_name.replace("{partition_key}", context.partition_key)

            engine = create_engine(db_url)
            df.to_sql(table_name, con=engine, schema=_self.schema_name,
                      if_exists=_self.if_exists, index=False, method="multi", chunksize=1000)

            context.log.info(f"Wrote {len(df)} rows to {_self.schema_name + '.' if _self.schema_name else ''}{table_name}")
            return dg.MaterializeResult(metadata={
                "num_rows": len(df),
                "num_columns": len(df.columns),
                "columns": list(df.columns),
                "remote_path": remote_path,
                "table": f"{_self.schema_name + '.' if _self.schema_name else ''}{table_name}",
                "file_format": fmt,
            })

        return dg.Definitions(assets=[sftp_to_database_asset])
