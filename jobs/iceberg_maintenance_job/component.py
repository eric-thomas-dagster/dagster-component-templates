"""IcebergMaintenanceJobComponent.

Recurring maintenance for Apache Iceberg tables: OPTIMIZE (compaction),
expire old snapshots, remove orphan files, rewrite manifests. Configurable
per-engine — works against Spark, Trino, Athena via SQL.

Each maintenance op runs sequentially within one Dagster job; a schedule
(optional) drives recurring execution. Op-level retry policy is honored.
"""

import importlib
import os
from typing import Optional

import dagster as dg
from pydantic import Field


def _resolve_engine_sql(engine: str, op: str, table: str, params: dict) -> str:
    """Return the engine-specific SQL for a maintenance op."""
    if engine == "spark":
        if op == "optimize":
            target_size = params.get("target_file_size_bytes", 134217728)  # 128 MiB
            return (
                f"CALL system.rewrite_data_files("
                f"table => '{table}', "
                f"options => map('target-file-size-bytes', '{target_size}'))"
            )
        if op == "expire_snapshots":
            older_than_days = params.get("older_than_days", 7)
            retain_last = params.get("retain_last", 5)
            return (
                f"CALL system.expire_snapshots("
                f"table => '{table}', "
                f"older_than => TIMESTAMP '{params.get('older_than_iso', '')}', "
                f"retain_last => {retain_last})"
            ) if params.get("older_than_iso") else (
                f"CALL system.expire_snapshots("
                f"table => '{table}', "
                f"retain_last => {retain_last})"
            )
        if op == "remove_orphan_files":
            return f"CALL system.remove_orphan_files(table => '{table}')"
        if op == "rewrite_manifests":
            return f"CALL system.rewrite_manifests(table => '{table}')"
    if engine == "trino":
        if op == "optimize":
            return f"ALTER TABLE {table} EXECUTE optimize"
        if op == "expire_snapshots":
            retention = params.get("retention", "7d")
            return f"ALTER TABLE {table} EXECUTE expire_snapshots(retention_threshold => '{retention}')"
        if op == "remove_orphan_files":
            retention = params.get("retention", "7d")
            return f"ALTER TABLE {table} EXECUTE remove_orphan_files(retention_threshold => '{retention}')"
        if op == "rewrite_manifests":
            return f"ALTER TABLE {table} EXECUTE rewrite_manifests"
    if engine == "athena":
        # Athena Iceberg supports OPTIMIZE + VACUUM
        if op == "optimize":
            return f"OPTIMIZE {table} REWRITE DATA USING BIN_PACK"
        if op == "expire_snapshots" or op == "remove_orphan_files":
            return f"VACUUM {table}"
    raise ValueError(f"unsupported engine={engine!r} or op={op!r}")


class IcebergMaintenanceJobComponent(dg.Component, dg.Model, dg.Resolvable):
    """Cron-driven Iceberg maintenance: optimize / expire snapshots / remove orphans / rewrite manifests."""

    job_name: str = Field(description="Dagster job name")
    schedule: Optional[str] = Field(default=None, description="Cron schedule (None = no schedule)")
    default_status: str = Field(default="STOPPED")

    engine: str = Field(description="spark | trino | athena")
    connection_string_env: str = Field(
        default="ICEBERG_DB_URL",
        description="Env var with SQLAlchemy URL for the engine (e.g. 'trino://user@host:443/iceberg')",
    )

    tables: list = Field(description="List of fully-qualified table names (e.g. ['catalog.db.orders'])")
    operations: list = Field(
        default_factory=lambda: ["optimize", "expire_snapshots", "remove_orphan_files"],
        description="Subset of: optimize | expire_snapshots | remove_orphan_files | rewrite_manifests",
    )
    operation_params: Optional[dict] = Field(
        default=None,
        description="Per-op params: {optimize: {target_file_size_bytes: ...}, expire_snapshots: {older_than_days: 7, retain_last: 5}}",
    )

    fail_fast: bool = Field(default=True, description="Stop on first error (False = collect errors and continue)")
    retry_max_retries: Optional[int] = Field(default=None)
    retry_delay_seconds: Optional[int] = Field(default=None)
    job_tags: Optional[dict] = Field(default=None)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        retry = None
        if self.retry_max_retries:
            retry = dg.RetryPolicy(
                max_retries=self.retry_max_retries,
                delay=self.retry_delay_seconds or 1,
                backoff=dg.Backoff.EXPONENTIAL,
            )

        @dg.op(retry_policy=retry)
        def _maintenance(_ctx):
            from sqlalchemy import create_engine, text
            url = os.environ[_self.connection_string_env]
            engine = create_engine(url, isolation_level="AUTOCOMMIT")
            errors = []
            results = []
            params = _self.operation_params or {}
            with engine.connect() as conn:
                for table in _self.tables:
                    for op_name in _self.operations:
                        op_params = params.get(op_name, {})
                        try:
                            sql = _resolve_engine_sql(_self.engine, op_name, table, op_params)
                            _ctx.log.info(f"[{table}] {op_name}: {sql[:200]}")
                            conn.execute(text(sql))
                            results.append({"table": table, "op": op_name, "ok": True})
                        except Exception as exc:
                            msg = f"[{table}] {op_name} failed: {exc}"
                            _ctx.log.error(msg)
                            errors.append(msg)
                            results.append({"table": table, "op": op_name, "ok": False, "error": str(exc)})
                            if _self.fail_fast:
                                raise
            if errors and not _self.fail_fast:
                raise Exception(f"{len(errors)}/{len(results)} ops failed")
            _ctx.log.info(f"completed {sum(1 for r in results if r['ok'])}/{len(results)} ops")
            return {"results": results}

        @dg.job(name=self.job_name, tags=self.job_tags or None)
        def _the_job():
            _maintenance()

        defs_kwargs = {"jobs": [_the_job]}
        if self.schedule:
            sched = dg.ScheduleDefinition(
                name=f"{self.job_name}_schedule",
                cron_schedule=self.schedule, job=_the_job,
                default_status=dg.DefaultScheduleStatus.STOPPED if self.default_status.upper() == "STOPPED" else dg.DefaultScheduleStatus.RUNNING,
            )
            defs_kwargs["schedules"] = [sched]
        return dg.Definitions(**defs_kwargs)
