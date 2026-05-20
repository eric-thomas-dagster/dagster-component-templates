"""Snowflake Table Observation Sensor Component."""
from typing import Optional
import dagster as dg
from dagster import AssetKey, AssetObservation, MetadataValue, SensorEvaluationContext, SensorResult, sensor
from pydantic import Field

class SnowflakeTableObservationSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Emit health observations for an external Snowflake table."""
    sensor_name: str = Field(description="Unique sensor name")
    asset_key: str = Field(description="Asset key of the ExternalSnowflakeTableAsset to observe")
    account: str = Field(description="Snowflake account identifier")
    database: str = Field(description="Snowflake database")
    schema_name: str = Field(description="Snowflake schema")
    table_name: str = Field(description="Snowflake table")
    username_env_var: str = Field(description="Env var with Snowflake username")
    password_env_var: Optional[str] = Field(default=None, description="Env var with password")
    # SSO / keypair / PAT alternatives — for accounts where password auth is
    # disabled. Leave password_env_var unset and use one of these.
    authenticator: Optional[str] = Field(
        default=None,
        description="Snowflake authenticator: 'SNOWFLAKE_JWT' (keypair), 'externalbrowser' (SSO), 'oauth', etc.",
    )
    private_key_file_env_var: Optional[str] = Field(
        default=None,
        description="Env var holding the path to a PEM RSA private key file (for authenticator='SNOWFLAKE_JWT').",
    )
    private_key_file_pwd_env_var: Optional[str] = Field(
        default=None,
        description="Env var holding the passphrase for an encrypted private key file (optional).",
    )
    token_env_var: Optional[str] = Field(
        default=None,
        description="Env var holding an OAuth / PAT token (with authenticator='oauth' or PAT).",
    )
    warehouse: Optional[str] = Field(default=None, description="Snowflake warehouse to use")
    check_interval_seconds: int = Field(default=300, description="Seconds between health checks")
    resource_key: Optional[str] = Field(default=None, description="Optional Dagster resource key.")
    include_preview_metadata: bool = Field(
        default=False,
        description=(
            "Run an extra `SELECT * LIMIT preview_rows` against the table and "
            "include the result as a markdown preview on the AssetObservation."
        ),
    )
    preview_rows: int = Field(
        default=25,
        ge=1,
        le=500,
        description="Rows in the preview SELECT when include_preview_metadata=True.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        resource_key = self.resource_key
        required_resource_keys = {resource_key} if resource_key else set()

        @sensor(
            name=_self.sensor_name,
            minimum_interval_seconds=_self.check_interval_seconds,
            required_resource_keys=required_resource_keys,
            asset_selection=dg.AssetSelection.keys(
                dg.AssetKey(_self.asset_key.split("/"))
            ),
        )
        def _sf_obs(context: SensorEvaluationContext):
            import os
            try:
                import snowflake.connector
            except ImportError:
                return SensorResult(skip_reason="snowflake-connector-python not installed")

            username = os.environ.get(_self.username_env_var, "")
            password = os.environ.get(_self.password_env_var, "") if _self.password_env_var else ""
            try:
                if resource_key:
                    conn = getattr(context.resources, resource_key).get_connection()
                else:
                    conn_kwargs = {
                        "account": _self.account,
                        "user": username,
                        "database": _self.database,
                        "schema": _self.schema_name,
                    }
                    if _self.warehouse:
                        conn_kwargs["warehouse"] = _self.warehouse
                    if _self.authenticator:
                        conn_kwargs["authenticator"] = _self.authenticator
                        if _self.private_key_file_env_var:
                            pk_path = os.environ.get(_self.private_key_file_env_var)
                            if pk_path:
                                conn_kwargs["private_key_file"] = pk_path
                            if _self.private_key_file_pwd_env_var:
                                pk_pwd = os.environ.get(_self.private_key_file_pwd_env_var)
                                if pk_pwd:
                                    conn_kwargs["private_key_file_pwd"] = pk_pwd
                        elif _self.token_env_var:
                            tok = os.environ.get(_self.token_env_var)
                            if tok:
                                conn_kwargs["token"] = tok
                    elif password:
                        conn_kwargs["password"] = password
                    conn = snowflake.connector.connect(**conn_kwargs)
                cursor = conn.cursor()
            except Exception as e:
                return SensorResult(skip_reason=f"Connect failed: {e}")

            try:
                cursor.execute(f"SELECT COUNT(*), MAX(LAST_DDL_TIME) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{_self.table_name.upper()}' AND TABLE_SCHEMA = '{_self.schema_name.upper()}'")
                row = cursor.fetchone()
                # Row count
                cursor.execute(f"SELECT COUNT(*) FROM {_self.database}.{_self.schema_name}.{_self.table_name}")
                row_count = cursor.fetchone()[0]
                # Table info
                cursor.execute(f"SHOW TABLES LIKE '{_self.table_name}' IN SCHEMA {_self.database}.{_self.schema_name}")
                info = cursor.fetchone()
            except Exception as e:
                conn.close()
                return SensorResult(skip_reason=f"Query failed: {e}")

            metadata = {
                "row_count": row_count,
                "database": _self.database,
                "schema": _self.schema_name,
                "table": _self.table_name,
            }
            if _self.include_preview_metadata and row_count > 0:
                try:
                    fqn = f"{_self.database}.{_self.schema_name}.{_self.table_name}"
                    cursor.execute(f"SELECT * FROM {fqn} LIMIT {_self.preview_rows}")
                    cols = [d[0] for d in cursor.description]
                    rows = cursor.fetchall()
                    if rows:
                        import pandas as pd
                        df = pd.DataFrame(rows, columns=cols)
                        metadata["preview"] = MetadataValue.md(df.to_markdown(index=False))
                except Exception as e:
                    context.log.warning(f"Preview query failed: {e}")
            conn.close()
            return SensorResult(asset_events=[AssetObservation(
                asset_key=AssetKey(_self.asset_key.split("/")), metadata=metadata)])

        return dg.Definitions(sensors=[_sf_obs])
