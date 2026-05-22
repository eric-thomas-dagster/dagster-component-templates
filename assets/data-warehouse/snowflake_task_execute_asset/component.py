"""Snowflake Task — execute asset.

Materializes by calling ``EXECUTE TASK <db>.<schema>.<task_name>`` on a
named, existing Snowflake task. Pairs with ``snowflake_task_completion_sensor``
when you want to also surface autonomous task runs (Snowflake's scheduler
firing the task on its own cadence).

This is the single-entity counterpart to the multi-entity
``snowflake_workspace`` component — use this when you want to wire one
specific task into a Dagster project without scanning the whole account.
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


class SnowflakeTaskExecuteAssetComponent(Component, Model, Resolvable):
    """Materialize by EXECUTE TASK on a named Snowflake task.

    Example:
        ```yaml
        type: dagster_community_components.SnowflakeTaskExecuteAssetComponent
        attributes:
          asset_key: snowflake/tasks/refresh_orders
          task_name: DAILY_REFRESH_ORDERS
          database: DAGSTER_DEMO
          schema_name: RAW
          account_env_var: SNOWFLAKE_ACCOUNT
          user_env_var: SNOWFLAKE_USER
          password_env_var: SNOWFLAKE_PASSWORD
          warehouse_env_var: SNOWFLAKE_WAREHOUSE
          role_env_var: SNOWFLAKE_ROLE
        ```
    """

    asset_key: str = Field(description="Dagster asset key (e.g. 'snowflake/tasks/refresh_orders').")
    task_name: str = Field(description="Name of the existing Snowflake task to execute.")
    database: str = Field(description="Snowflake database holding the task.")
    schema_name: str = Field(
        description="Snowflake schema holding the task.",
        alias="schema",
    )

    account_env_var: str = Field(description="Env var with Snowflake account identifier.")
    user_env_var: str = Field(description="Env var with Snowflake username.")
    password_env_var: Optional[str] = Field(default=None, description="Env var with Snowflake password (or use PAT / key auth).")
    pat_env_var: Optional[str] = Field(default=None, description="Env var with Snowflake Programmatic Access Token.")
    private_key_path_env_var: Optional[str] = Field(default=None, description="Env var with path to RSA private key for keypair auth.")
    private_key_passphrase_env_var: Optional[str] = Field(default=None, description="Env var with private-key passphrase (optional).")
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
            description=self.description or f"Snowflake task: {self.database}.{self.schema_name}.{self.task_name}",
            group_name=self.group_name,
            kinds=kinds,
            owners=self.owners,
            tags=self.asset_tags,
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def snowflake_task_execute_asset(context: AssetExecutionContext) -> MaterializeResult:
            try:
                import snowflake.connector
            except ImportError:
                raise ImportError(
                    "snowflake_task_execute_asset requires 'snowflake-connector-python'. "
                    "Install with: pip install snowflake-connector-python"
                )

            conn = snowflake.connector.connect(**_self._connection_kwargs())
            cursor = conn.cursor()
            try:
                fqn = f"{_self.database}.{_self.schema_name}.{_self.task_name}"
                context.log.info(f"EXECUTE TASK {fqn}")
                cursor.execute(f"EXECUTE TASK {fqn}")

                # Pull the most recent history entry so the materialization
                # has the run-id / state / scheduled_time on it.
                cursor.execute(f"""
                    SELECT query_id, state, scheduled_time, query_start_time,
                           completed_time, error_code, error_message
                    FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
                        TASK_NAME => '{_self.task_name}',
                        SCHEDULED_TIME_RANGE_START =>
                            DATEADD('minute', -5, CURRENT_TIMESTAMP())
                    ))
                    ORDER BY scheduled_time DESC
                    LIMIT 1
                """)
                row = cursor.fetchone()

                metadata: Dict[str, Any] = {
                    "task_fqn": fqn,
                    "task_name": _self.task_name,
                    "database": _self.database,
                    "schema": _self.schema_name,
                }
                if row:
                    columns = [c[0] for c in cursor.description]
                    rd = dict(zip(columns, row))
                    metadata.update({
                        "query_id": rd.get("QUERY_ID"),
                        "state": rd.get("STATE"),
                        "scheduled_time": str(rd.get("SCHEDULED_TIME")) if rd.get("SCHEDULED_TIME") else None,
                        "completed_time": str(rd.get("COMPLETED_TIME")) if rd.get("COMPLETED_TIME") else None,
                        "error_code": rd.get("ERROR_CODE"),
                        "error_message": rd.get("ERROR_MESSAGE"),
                    })
                return MaterializeResult(metadata=metadata)
            finally:
                cursor.close()
                conn.close()

        return Definitions(assets=[snowflake_task_execute_asset])
