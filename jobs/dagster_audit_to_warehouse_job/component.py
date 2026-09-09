"""DagsterAuditToWarehouseJobComponent.

Op-shaped job that pulls Dagster+ audit log entries (who did what, when)
via the Dagster+ GraphQL API and MERGEs them into a warehouse table for
compliance / SOC2 / retention queries.

Dagster+ only. For OSS deployments, audit-log equivalent doesn't exist
at the framework level — use application-level audit logging instead.
"""

import json
import os
import time
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


CURSOR_TAG = "dagster_audit_to_warehouse/last_ts"


class DagsterAuditToWarehouseJobComponent(dg.Component, dg.Model, dg.Resolvable):
    """Op-shaped job that MERGEs Dagster+ audit-log entries into a
    warehouse table (Snowflake | BigQuery | Postgres).

    Target table DDL:
        CREATE TABLE dagster_audit_log (
          timestamp NUMBER/FLOAT,
          timestamp_ts TIMESTAMP,
          user_email STRING,
          event_type STRING,
          target_type STRING,
          target_identifier STRING,
          raw_json STRING
        );
    """

    job_name: str = Field(description="Dagster job name")
    schedule: Optional[str] = Field(default="0 * * * *", description="Cron schedule; default hourly.")
    default_status: str = Field(default="STOPPED", description="STOPPED | RUNNING")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Dagster job tags")

    # Dagster+ GraphQL connection
    dagster_plus_url: str = Field(
        description="Dagster+ deployment URL (e.g. https://acme.dagster.cloud/prod). GraphQL endpoint is appended.",
    )
    dagster_plus_token_env: str = Field(
        default="DAGSTER_CLOUD_API_TOKEN",
        description="Env var with Dagster+ user token.",
    )

    # Warehouse (mirrors run_history_to_warehouse_job)
    warehouse: str = Field(description="Target warehouse type: snowflake | bigquery | postgres")

    snowflake_account: Optional[str] = Field(default=None)
    snowflake_user: Optional[str] = Field(default=None)
    snowflake_password_env: str = Field(default="SNOWFLAKE_PASSWORD")
    snowflake_warehouse: Optional[str] = Field(default=None)
    snowflake_database: Optional[str] = Field(default=None)
    snowflake_schema: str = Field(default="PUBLIC")

    bq_project: Optional[str] = Field(default=None)
    bq_dataset: Optional[str] = Field(default=None)

    postgres_dsn_env: str = Field(default="POSTGRES_DSN")
    postgres_schema: str = Field(default="public")

    table: str = Field(default="dagster_audit_log")
    initial_lookback_hours: int = Field(default=24, ge=1)
    batch_limit: int = Field(default=1000, ge=1)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        @dg.op(name=f"{self.job_name}_op")
        def _export(context: dg.OpExecutionContext):
            import requests

            token = os.environ.get(_self.dagster_plus_token_env)
            if not token:
                raise RuntimeError(f"Missing {_self.dagster_plus_token_env}")

            # Cursor
            last_ts = None
            try:
                prior = context.instance.get_runs(
                    filters=dg.RunsFilter(job_name=_self.job_name, statuses=[dg.DagsterRunStatus.SUCCESS]),
                    limit=2,
                )
                for r in prior:
                    if r.run_id == context.run_id:
                        continue
                    v = (r.tags or {}).get(CURSOR_TAG)
                    if v:
                        last_ts = float(v)
                        break
            except Exception:
                pass
            since = last_ts or (time.time() - _self.initial_lookback_hours * 3600.0)

            # Query Dagster+ audit log via GraphQL.
            query = """
              query AuditLogs($limit: Int, $sinceTs: Float) {
                auditLogs(limit: $limit, filters: {sinceTimestamp: $sinceTs}) {
                  entries {
                    timestamp
                    userEmail
                    eventType
                    targetType
                    targetIdentifier
                  }
                }
              }
            """
            gql_url = _self.dagster_plus_url.rstrip("/") + "/graphql"
            resp = requests.post(
                gql_url,
                json={"query": query, "variables": {"limit": _self.batch_limit, "sinceTs": since}},
                headers={"Dagster-Cloud-Api-Token": token, "Content-Type": "application/json"},
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
            entries = ((data.get("data") or {}).get("auditLogs") or {}).get("entries") or []

            if not entries:
                context.log.info("No new audit entries since last run.")
                context.instance.add_run_tags(context.run_id, {CURSOR_TAG: str(time.time())})
                return {"merged": 0}

            rows = [{
                "timestamp": e.get("timestamp"),
                "timestamp_ts": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(e.get("timestamp") or 0)),
                "user_email": e.get("userEmail"),
                "event_type": e.get("eventType"),
                "target_type": e.get("targetType"),
                "target_identifier": e.get("targetIdentifier"),
                "raw_json": json.dumps(e)[:4000],
            } for e in entries]

            merged = _merge_audit(_self, rows)

            context.instance.add_run_tags(context.run_id, {CURSOR_TAG: str(time.time())})
            context.log.info(f"MERGEd {merged} audit entries into {_self.warehouse} table {_self.table}")
            return {"merged": merged, "warehouse": _self.warehouse}

        @dg.job(name=self.job_name, tags=self.tags or None)
        def _the_job():
            _export()

        defs_kwargs: Dict[str, Any] = {"jobs": [_the_job]}
        if self.schedule:
            defs_kwargs["schedules"] = [dg.ScheduleDefinition(
                name=f"{self.job_name}_schedule",
                cron_schedule=self.schedule,
                job=_the_job,
                default_status=(
                    dg.DefaultScheduleStatus.STOPPED
                    if self.default_status.upper() == "STOPPED"
                    else dg.DefaultScheduleStatus.RUNNING
                ),
            )]
        return dg.Definitions(**defs_kwargs)


def _merge_audit(cfg, rows) -> int:
    w = cfg.warehouse.lower()
    if w == "snowflake":
        import snowflake.connector as sc
        password = os.environ.get(cfg.snowflake_password_env)
        conn = sc.connect(
            account=cfg.snowflake_account, user=cfg.snowflake_user, password=password,
            warehouse=cfg.snowflake_warehouse, database=cfg.snowflake_database, schema=cfg.snowflake_schema,
        )
        try:
            cur = conn.cursor()
            cur.executemany(
                f"INSERT INTO {cfg.table} (timestamp, timestamp_ts, user_email, event_type, "
                "target_type, target_identifier, raw_json) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                [tuple(r.values()) for r in rows],
            )
            conn.commit()
            return len(rows)
        finally:
            conn.close()
    if w == "bigquery":
        from google.cloud import bigquery
        client = bigquery.Client(project=cfg.bq_project)
        table_ref = f"{cfg.bq_project}.{cfg.bq_dataset}.{cfg.table}"
        errs = client.insert_rows_json(table_ref, rows)
        if errs:
            raise RuntimeError(f"BQ errors: {errs[:3]}")
        return len(rows)
    if w == "postgres":
        import psycopg2
        conn = psycopg2.connect(os.environ.get(cfg.postgres_dsn_env))
        try:
            cur = conn.cursor()
            for r in rows:
                cur.execute(
                    f"INSERT INTO {cfg.postgres_schema}.{cfg.table} "
                    "(timestamp, timestamp_ts, user_email, event_type, target_type, target_identifier, raw_json) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    tuple(r.values()),
                )
            conn.commit()
            return len(rows)
        finally:
            conn.close()
    raise ValueError(f"Unknown warehouse: {cfg.warehouse!r}")
