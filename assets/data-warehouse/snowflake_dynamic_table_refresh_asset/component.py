"""Snowflake Dynamic Table — refresh asset.

Materializes by calling ``ALTER DYNAMIC TABLE <db>.<schema>.<name> REFRESH``
on a named, existing Snowflake dynamic table. Pulls scheduling_state +
last_refresh_status into materialization metadata.

Single-entity counterpart to the auto-discovered dynamic-table assets in
``snowflake_workspace``.
"""
from typing import Any, Dict, List, Optional

import dagster as dg
from dagster import (
    AssetExecutionContext,
    Component,
    ComponentLoadContext,
    Definitions,
    MaterializeResult,
    Model,
    Resolvable,
    asset,
)
from pydantic import Field


class SnowflakeDynamicTableRefreshAssetComponent(Component, Model, Resolvable):
    """Materialize by ALTER DYNAMIC TABLE ... REFRESH on a named dynamic table.

    Example:
        ```yaml
        type: dagster_community_components.SnowflakeDynamicTableRefreshAssetComponent
        attributes:
          asset_key: snowflake/dt/customer_summary
          dynamic_table_name: CUSTOMER_SUMMARY
          database: DAGSTER_DEMO
          schema: ANALYTICS
          account_env_var: SNOWFLAKE_ACCOUNT
          user_env_var: SNOWFLAKE_USER
          password_env_var: SNOWFLAKE_PASSWORD
        ```
    """

    asset_key: str = Field(description="Dagster asset key (e.g. 'snowflake/dt/customer_summary').")
    dynamic_table_name: str = Field(description="Name of the dynamic table.")
    database: str = Field(description="Snowflake database holding the DT.")
    schema_name: str = Field(description="Snowflake schema holding the DT.", alias="schema")

    account_env_var: str = Field(description="Env var with Snowflake account.")
    user_env_var: str = Field(description="Env var with Snowflake username.")
    password_env_var: Optional[str] = Field(default=None, description="Env var with Snowflake password.")
    pat_env_var: Optional[str] = Field(default=None, description="Env var with Snowflake PAT.")
    private_key_path_env_var: Optional[str] = Field(default=None, description="Env var with RSA private key path.")
    private_key_passphrase_env_var: Optional[str] = Field(default=None, description="Env var with private-key passphrase.")
    warehouse_env_var: Optional[str] = Field(default=None, description="Env var with Snowflake warehouse.")
    role_env_var: Optional[str] = Field(default=None, description="Env var with Snowflake role.")

    group_name: Optional[str] = Field(default="snowflake", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    asset_tags: Optional[Dict[str, str]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds (auto-includes 'snowflake').")
    deps: Optional[List[str]] = Field(default=None, description="Upstream Dagster asset keys.")

    model_config = {"populate_by_name": True}

    def _connection_kwargs(self) -> Dict[str, Any]:
        import os
        kwargs: Dict[str, Any] = {
            "account": os.environ[self.account_env_var],
            "user": os.environ[self.user_env_var],
            "database": self.database,
            "schema": self.schema_name,
        }
        if self.warehouse_env_var:
            kwargs["warehouse"] = os.environ.get(self.warehouse_env_var, "")
        if self.role_env_var:
            kwargs["role"] = os.environ.get(self.role_env_var, "")
        if self.pat_env_var and os.environ.get(self.pat_env_var):
            kwargs["password"] = os.environ[self.pat_env_var]
        elif self.private_key_path_env_var and os.environ.get(self.private_key_path_env_var):
            kwargs["private_key_file"] = os.environ[self.private_key_path_env_var]
            if self.private_key_passphrase_env_var:
                kwargs["private_key_file_pwd"] = os.environ.get(self.private_key_passphrase_env_var, "")
            kwargs["authenticator"] = "snowflake_jwt"
        elif self.password_env_var:
            kwargs["password"] = os.environ.get(self.password_env_var, "")
        return kwargs

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        _self = self
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("snowflake")

        @asset(
            key=dg.AssetKey(self.asset_key.split("/")),
            description=self.description or (
                f"Snowflake dynamic table: "
                f"{self.database}.{self.schema_name}.{self.dynamic_table_name}"
            ),
            group_name=self.group_name,
            kinds=kinds,
            owners=self.owners,
            tags=self.asset_tags,
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def snowflake_dynamic_table_refresh_asset(context: AssetExecutionContext) -> MaterializeResult:
            try:
                import snowflake.connector
            except ImportError:
                raise ImportError(
                    "snowflake_dynamic_table_refresh_asset requires "
                    "'snowflake-connector-python'. Install with: "
                    "pip install snowflake-connector-python"
                )

            fqn = f"{_self.database}.{_self.schema_name}.{_self.dynamic_table_name}"
            conn = snowflake.connector.connect(**_self._connection_kwargs())
            cursor = conn.cursor()
            try:
                context.log.info(f"ALTER DYNAMIC TABLE {fqn} REFRESH")
                cursor.execute(f"ALTER DYNAMIC TABLE {fqn} REFRESH")

                metadata: Dict[str, Any] = {
                    "dynamic_table_fqn": fqn,
                    "dynamic_table_name": _self.dynamic_table_name,
                    "database": _self.database,
                    "schema": _self.schema_name,
                }

                # SHOW DYNAMIC TABLES (not INFORMATION_SCHEMA.DYNAMIC_TABLES) —
                # INFORMATION_SCHEMA can be invisible to least-privilege roles
                # (e.g. DAGSTER_RUNNER) even with USAGE on the database. SHOW
                # only requires USAGE on the schema + any privilege on the DT.
                # Wrap in try/except so refresh still wins even if SHOW fails.
                try:
                    cursor.execute(
                        f"SHOW DYNAMIC TABLES LIKE '{_self.dynamic_table_name}' "
                        f"IN SCHEMA {_self.database}.{_self.schema_name}"
                    )
                    row = cursor.fetchone()
                    if row:
                        columns = [c[0].lower() for c in cursor.description]
                        rd = dict(zip(columns, row))
                        metadata.update({
                            "scheduling_state": rd.get("scheduling_state"),
                            "last_refresh_status": rd.get("last_refresh_state"),
                            "target_lag": rd.get("target_lag"),
                            "refresh_mode": rd.get("refresh_mode"),
                            "rows": rd.get("rows"),
                            "bytes": rd.get("bytes"),
                            "owner": rd.get("owner"),
                        })
                except Exception as exc:
                    context.log.warning(
                        f"Could not read DT metadata for {_self.dynamic_table_name}: {exc}. "
                        f"Refresh succeeded; emitting asset without enriched metadata."
                    )
                return MaterializeResult(metadata=metadata)
            finally:
                cursor.close()
                conn.close()

        return Definitions(assets=[snowflake_dynamic_table_refresh_asset])
