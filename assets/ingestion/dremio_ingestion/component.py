"""Dremio Ingestion Component.

Run a SQL query against a Dremio cluster (OSS or Cloud) via the REST API and
materialize the result as a Dagster asset (pandas DataFrame).

Dremio is SAP's lakehouse query engine (acquired 2024-25). It speaks both ANSI
SQL and Apache Arrow over Flight; this component uses the simpler REST API
which is sufficient for ingestion-sized result sets.

Auth:
- **PAT (recommended, Dremio 24.0+)**: set `auth_token_env_var` → sends
  `Authorization: Bearer <token>`.
- **Username + password (legacy)**: set `auth_username_env_var` +
  `auth_password_env_var` — the component POSTs to `/apiv2/login` to mint a
  session token, then uses `Authorization: _dremio<token>`.

For larger result sets, Apache Arrow Flight SQL (port 32010) is dramatically
faster but adds a `pyarrow>=10` runtime dep. Set `transport: flight` to use
it; default is `rest`.
"""

import os
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
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


def _build_partitions_def(
    partition_type, partition_start, partition_values, dynamic_partition_name
):
    from dagster import (
        DailyPartitionsDefinition,
        WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition,
        HourlyPartitionsDefinition,
        StaticPartitionsDefinition,
        DynamicPartitionsDefinition,
    )

    if not partition_type:
        return None
    _values = (
        [str(v).strip() for v in partition_values if str(v).strip()]
        if isinstance(partition_values, (list, tuple))
        else [v.strip() for v in (str(partition_values) if partition_values else "").split(",") if v.strip()]
    )
    if partition_type in ("daily", "weekly", "monthly", "hourly") and not partition_start:
        raise ValueError(f"partition_type={partition_type!r} requires partition_start.")
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "static":
        if not _values:
            raise ValueError("partition_type='static' requires partition_values.")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    raise ValueError(f"unknown partition_type: {partition_type!r}")


class DremioIngestionComponent(Component, Model, Resolvable):
    """Query Dremio and materialize the result as a Dagster asset.

    Example — PAT auth (Dremio 24.0+):

        ```yaml
        type: dagster_component_templates.DremioIngestionComponent
        attributes:
          asset_name: customer_revenue
          host: https://dremio.example.com
          auth_type: pat
          auth_token_env_var: DREMIO_PAT
          query: |
            SELECT customer_id, SUM(revenue) AS total
            FROM "@my-space"."customers"
            WHERE business_date = DATE '{partition_key}'
            GROUP BY customer_id
          partition_type: daily
          partition_start: '2024-01-01'
        ```
    """

    asset_name: str = Field(description="Dagster asset name")

    host: str = Field(
        description=(
            "Dremio coordinator URL incl. scheme and port if needed, "
            "e.g. 'http://localhost:9047' or 'https://dremio.example.com'."
        )
    )

    query: str = Field(
        description="SQL to execute. Supports `{partition_key}` substitution.",
    )

    # --- Auth ---------------------------------------------------------------

    auth_type: str = Field(
        default="pat",
        description="'pat' (Bearer token, Dremio 24.0+) or 'password' (legacy login flow).",
    )
    auth_token_env_var: Optional[str] = Field(
        default=None, description="Env var with the PAT (auth_type=pat)."
    )
    auth_username_env_var: Optional[str] = Field(
        default=None, description="Env var with username (auth_type=password)."
    )
    auth_password_env_var: Optional[str] = Field(
        default=None, description="Env var with password (auth_type=password)."
    )

    # --- Transport ----------------------------------------------------------

    transport: str = Field(
        default="rest",
        description=(
            "'rest' (default — uses /api/v3/sql) or 'flight' (Apache Arrow Flight SQL, port 32010 by "
            "default; needs pyarrow>=10 + adbc-driver-flightsql or pyarrow.flight). Flight is much "
            "faster for large result sets."
        ),
    )
    flight_port: int = Field(
        default=32010,
        description="Flight SQL port (only used when transport='flight').",
    )

    # --- Result fetching ----------------------------------------------------

    page_size: int = Field(
        default=500,
        description="Rows per page when paging REST `/job/<id>/results` (Dremio max 500).",
    )
    poll_interval_seconds: float = Field(
        default=1.0, description="Seconds between job-status polls."
    )
    poll_timeout_seconds: int = Field(
        default=600, description="Hard cap on job-completion wait."
    )
    verify_ssl: bool = Field(default=True)

    # --- Standard fields ----------------------------------------------------

    description: Optional[str] = Field(default=None)
    group_name: Optional[str] = Field(default="dremio")
    owners: Optional[List[str]] = Field(default=None)
    asset_tags: Optional[Dict[str, str]] = Field(default=None)
    kinds: Optional[List[str]] = Field(
        default=None, description="Defaults to ['dremio', 'sql']."
    )
    freshness_max_lag_minutes: Optional[int] = Field(default=None)
    freshness_cron: Optional[str] = Field(default=None)
    include_preview_metadata: bool = Field(default=True)
    preview_rows: int = Field(default=25, ge=1, le=500)
    deps: Optional[List[str]] = Field(default=None)

    partition_type: Optional[str] = Field(default=None)
    partition_start: Optional[str] = Field(default=None)
    partition_values: Optional[str] = Field(default=None)
    dynamic_partition_name: Optional[str] = Field(default=None)

    retry_policy_max_retries: Optional[int] = Field(default=None)
    retry_policy_delay_seconds: Optional[int] = Field(default=None)
    retry_policy_backoff: str = Field(default="exponential")

    # -----------------------------------------------------------------------

    def _resolve_token(self) -> str:
        if self.auth_type == "pat":
            if not self.auth_token_env_var:
                raise ValueError("auth_type='pat' requires auth_token_env_var.")
            tok = os.environ.get(self.auth_token_env_var)
            if not tok:
                raise ValueError(f"{self.auth_token_env_var!r} is set but empty.")
            return f"Bearer {tok}"
        if self.auth_type == "password":
            if not (self.auth_username_env_var and self.auth_password_env_var):
                raise ValueError(
                    "auth_type='password' requires auth_username_env_var + auth_password_env_var."
                )
            user = os.environ.get(self.auth_username_env_var)
            pwd = os.environ.get(self.auth_password_env_var)
            if not (user and pwd):
                raise ValueError("password env vars are set but empty.")
            r = requests.post(
                f"{self.host.rstrip('/')}/apiv2/login",
                json={"userName": user, "password": pwd},
                timeout=30,
                verify=self.verify_ssl,
            )
            r.raise_for_status()
            return f"_dremio{r.json()['token']}"
        raise ValueError(f"unknown auth_type: {self.auth_type!r}")

    def _resolve_query(self, partition_key: Optional[str]) -> str:
        q = self.query
        if partition_key is not None and "{partition_key}" in q:
            q = q.replace("{partition_key}", partition_key)
        return q

    def _run_rest(self, context: AssetExecutionContext, sql: str) -> pd.DataFrame:
        import time

        host = self.host.rstrip("/")
        headers = {"Authorization": self._resolve_token(), "Content-Type": "application/json"}

        r = requests.post(
            f"{host}/api/v3/sql",
            json={"sql": sql},
            headers=headers,
            timeout=60,
            verify=self.verify_ssl,
        )
        r.raise_for_status()
        job_id = r.json()["id"]
        context.log.info(f"Dremio job submitted: {job_id}")

        # Poll for completion
        deadline = time.time() + self.poll_timeout_seconds
        state = "RUNNING"
        while time.time() < deadline:
            r = requests.get(
                f"{host}/api/v3/job/{job_id}",
                headers=headers,
                timeout=30,
                verify=self.verify_ssl,
            )
            r.raise_for_status()
            state = r.json().get("jobState")
            if state in ("COMPLETED", "CANCELED", "FAILED"):
                break
            time.sleep(self.poll_interval_seconds)
        if state != "COMPLETED":
            raise RuntimeError(f"Dremio job ended in state={state!r} (job_id={job_id})")

        rows: list = []
        columns: Optional[list] = None
        offset = 0
        while True:
            r = requests.get(
                f"{host}/api/v3/job/{job_id}/results",
                params={"offset": offset, "limit": self.page_size},
                headers=headers,
                timeout=60,
                verify=self.verify_ssl,
            )
            r.raise_for_status()
            body = r.json()
            if columns is None:
                columns = [s["name"] for s in body.get("schema", [])]
            batch = body.get("rows", [])
            rows.extend(batch)
            row_count = body.get("rowCount", 0)
            if offset + len(batch) >= row_count or not batch:
                break
            offset += len(batch)

        return pd.DataFrame(rows, columns=columns)

    def _run_flight(self, context: AssetExecutionContext, sql: str) -> pd.DataFrame:
        from urllib.parse import urlparse

        try:
            from pyarrow import flight
        except ImportError as e:
            raise ImportError(
                "transport='flight' requires `pyarrow>=10`. Install with: `pip install pyarrow`."
            ) from e

        parsed = urlparse(self.host)
        flight_host = parsed.hostname or self.host
        scheme = "grpc+tls" if parsed.scheme == "https" else "grpc+tcp"
        client = flight.FlightClient(f"{scheme}://{flight_host}:{self.flight_port}")

        token = self._resolve_token()
        if token.startswith("Bearer "):
            options = flight.FlightCallOptions(
                headers=[(b"authorization", token.encode())]
            )
        else:
            options = flight.FlightCallOptions(
                headers=[(b"authorization", token.encode())]
            )

        info = client.get_flight_info(flight.FlightDescriptor.for_command(sql), options)
        reader = client.do_get(info.endpoints[0].ticket, options)
        table = reader.read_all()
        context.log.info(f"Dremio Flight returned {table.num_rows} rows")
        return table.to_pandas()

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        description = self.description or f"Dremio ingestion ({asset_name})"
        component = self

        kinds = list(self.kinds or []) or ["dremio", "sql"]
        all_tags = dict(self.asset_tags or {})
        for k in kinds:
            all_tags[f"dagster/kind/{k}"] = ""

        freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy

            freshness_policy = FreshnessPolicy(
                maximum_lag_minutes=self.freshness_max_lag_minutes,
                cron_schedule=self.freshness_cron,
            )

        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values, self.dynamic_partition_name
        )

        retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy

            retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        @asset(
            key=AssetKey.from_user_string(asset_name),
            description=description,
            owners=self.owners or [],
            tags=all_tags,
            freshness_policy=freshness_policy,
            group_name=self.group_name,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
            partitions_def=partitions_def,
            retry_policy=retry_policy,
        )
        def dremio_ingestion_asset(context: AssetExecutionContext):
            partition_key = context.partition_key if context.has_partition_key else None
            sql = component._resolve_query(partition_key)
            context.log.info(f"Dremio SQL: {sql[:200]}{'...' if len(sql) > 200 else ''}")

            if component.transport == "flight":
                df = component._run_flight(context, sql)
            else:
                df = component._run_rest(context, sql)

            context.log.info(f"Loaded {len(df)} rows, {len(df.columns)} columns")

            metadata: Dict[str, Any] = {
                "row_count": MetadataValue.int(len(df)),
                "column_count": MetadataValue.int(len(df.columns)),
                "columns": MetadataValue.json(list(df.columns)),
                "query": MetadataValue.md(f"```sql\n{sql}\n```"),
                "transport": MetadataValue.text(component.transport),
            }
            if partition_key:
                metadata["partition_key"] = MetadataValue.text(partition_key)
            if component.include_preview_metadata and len(df) > 0:
                try:
                    sample = (
                        df.sample(min(component.preview_rows, len(df)))
                        if len(df) > component.preview_rows * 10
                        else df.head(component.preview_rows)
                    )
                    metadata["preview"] = MetadataValue.md(sample.to_markdown(index=False))
                except Exception as e:
                    context.log.warning(f"preview emission failed: {e}")
            return Output(value=df, metadata=metadata)

        return Definitions(assets=[dremio_ingestion_asset])
