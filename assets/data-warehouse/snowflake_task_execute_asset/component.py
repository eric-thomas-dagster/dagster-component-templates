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
    MetadataValue,
    Model,
    Resolvable,
    asset,
)
from pydantic import Field


def _emit_query_perf(cursor, query_id) -> dict:
    """Pull plottable per-query perf metrics from QUERY_HISTORY for ``query_id``.

    Dagster auto-plots ``MetadataValue.int`` / ``MetadataValue.float`` on
    each asset's Plots tab — so calling this after ``cursor.execute()``
    turns every materialization into a time-series of duration / rows /
    bytes / credits / partition pruning. ``cursor.sfqid`` exposed by
    snowflake-connector returns the last query id. Returns ``{}`` on any
    failure so callers can blindly ``metadata.update(...)``.
    """
    if not query_id:
        return {}
    try:
        cursor.execute(
            "SELECT total_elapsed_time, rows_produced, bytes_scanned, "
            "       bytes_spilled_to_local_storage, "
            "       credits_used_cloud_services, "
            "       partitions_scanned, partitions_total "
            f"FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY_BY_QUERY_ID('{query_id}'))"
        )
        row = cursor.fetchone()
        if not row:
            return {}
        return {
            "snowflake/query_duration_ms":   MetadataValue.int(int(row[0] or 0)),
            "snowflake/rows_produced":       MetadataValue.int(int(row[1] or 0)),
            "snowflake/bytes_scanned":       MetadataValue.int(int(row[2] or 0)),
            "snowflake/bytes_spilled_local": MetadataValue.int(int(row[3] or 0)),
            "snowflake/credits_used":        MetadataValue.float(float(row[4] or 0.0)),
            "snowflake/partitions_scanned":  MetadataValue.int(int(row[5] or 0)),
            "snowflake/partitions_total":    MetadataValue.int(int(row[6] or 0)),
        }
    except Exception:
        return {}


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
                execute_query = f"EXECUTE TASK {fqn}"
                exec_sfqid = None
                try:
                    cursor.execute(execute_query)
                    exec_sfqid = cursor.sfqid
                    context.log.info(f"Executed Snowflake task: {_self.task_name}")
                except Exception as exc:
                    if "non-root task" in str(exc).lower():
                        context.log.info(
                            f"{_self.task_name} is a child task — skipping direct EXECUTE TASK "
                            f"(parent triggers it). Run the parent task asset (or wait for "
                            f"its schedule) to actually exercise this asset."
                        )
                    else:
                        raise

                metadata: Dict[str, Any] = {
                    "task_fqn": fqn,
                    "task_name": _self.task_name,
                    "database": _self.database,
                    "schema": _self.schema_name,
                }
                # Per-run numeric perf trace (auto-plots on Plots tab).
                metadata.update(_emit_query_perf(cursor, exec_sfqid))

                # SHOW TASKS — works for least-privilege roles where
                # INFORMATION_SCHEMA may be invisible. Surfaces schedule + state.
                try:
                    cursor.execute(
                        f"SHOW TASKS LIKE '{_self.task_name}' "
                        f"IN SCHEMA {_self.database}.{_self.schema_name}"
                    )
                    info = cursor.fetchone()
                    if info:
                        columns = [c[0].lower() for c in cursor.description]
                        rd = dict(zip(columns, info))
                        metadata.update({
                            "task_state": rd.get("state"),
                            "task_schedule": rd.get("schedule"),
                            "warehouse": rd.get("warehouse"),
                            "owner": rd.get("owner"),
                        })
                except Exception as exc:
                    context.log.warning(
                        f"Could not read task metadata for {_self.task_name}: {exc}. "
                        f"Execute succeeded; emitting asset without enriched metadata."
                    )

                # TASK_HISTORY is a table function — wrap in try/except so
                # EXECUTE TASK still wins even if the role can't read history.
                try:
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
                except Exception as exc:
                    context.log.warning(
                        f"Could not read TASK_HISTORY for {_self.task_name}: {exc}. "
                        f"Execute succeeded; emitting asset without history metadata."
                    )
                return MaterializeResult(metadata=metadata)
            finally:
                cursor.close()
                conn.close()

        return Definitions(assets=[snowflake_task_execute_asset])
