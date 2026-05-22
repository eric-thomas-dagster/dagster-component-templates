"""SnowflakeSnowpipeLoadSensorComponent.

Fires a Dagster RunRequest each time a Snowpipe ingests one or more new
files. Reads file-level events from Snowflake's COPY_HISTORY table
function — same underlying source the Snowsight Pipe Activity UI uses.

Works with BOTH pipe modes:
  - **AUTO_INGEST = TRUE** (S3/GCS/Azure notification → SNS/SQS → Snowflake auto-COPY):
    sensor detects each auto-ingested file and triggers downstream
    Dagster assets without polling the destination table directly.
  - **AUTO_INGEST = FALSE** (manual PUT-then-COPY): sensor detects loads
    triggered by `ALTER PIPE ... REFRESH` (which Dagster can do
    server-side by materializing the snowpipe asset).

The differentiator vs. `snowflake_table_observation_sensor` (which polls
row counts on a table) is per-file granularity + rich load metadata
(file_name, file_size, rows ingested, errors, error counts).
"""
from typing import List, Optional

import dagster as dg
from dagster import RunRequest, SensorEvaluationContext, SensorResult, sensor
from dagster._core.definitions.sensor_definition import DefaultSensorStatus
from pydantic import ConfigDict, Field


class SnowflakeSnowpipeLoadSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Watch a Snowflake PIPE for new file-level loads and trigger downstream Dagster runs."""

    # Internal field is `schema_name`; YAML accepts the Snowflake-native
    # `schema:` alias too.
    model_config = ConfigDict(populate_by_name=True)

    sensor_name: str = Field(description="Unique sensor name.")
    job_name: str = Field(
        description="Dagster job to trigger on each new pipe ingestion."
    )

    # ── Pipe target ────────────────────────────────────────────────────
    pipe_name: str = Field(
        description="Pipe name (unqualified; resolved against database.schema_name).",
    )
    destination_table: str = Field(
        description=(
            "Destination table the pipe is configured to COPY INTO. "
            "Required because COPY_HISTORY is queried per-table, not per-pipe."
        ),
    )

    # ── Connection (same shape as the other snowflake_* components) ────
    account: str = Field(description="Snowflake account identifier.")
    user: str = Field(description="Snowflake user (NAME for keypair; LOGIN_NAME for password/SSO/PAT).")
    warehouse: Optional[str] = Field(default=None)
    database: str = Field(description="Database containing the pipe + destination table.")
    schema_name: str = Field(
        description="Schema containing the pipe + destination table.",
        alias="schema",
    )
    role: Optional[str] = Field(default=None)

    # ── Auth (pick ONE: password / authenticator+key/token) ─────────────
    password: Optional[str] = Field(default=None, description="Snowflake password. Leave unset for SSO/keypair/PAT.")
    authenticator: Optional[str] = Field(
        default=None,
        description="'SNOWFLAKE_JWT' (keypair), 'externalbrowser' (SSO), 'PROGRAMMATIC_ACCESS_TOKEN' (PAT), 'oauth'.",
    )
    private_key_file: Optional[str] = Field(default=None)
    private_key_file_pwd: Optional[str] = Field(default=None)
    token: Optional[str] = Field(default=None)

    # ── Sensor tuning ──────────────────────────────────────────────────
    minimum_interval_seconds: int = Field(
        default=60,
        description="Seconds between polls. COPY_HISTORY has ~minute-level latency.",
    )
    default_status: str = Field(default="running", description="'running' or 'stopped'.")
    lookback_minutes: int = Field(
        default=60,
        description=(
            "How far back to query COPY_HISTORY on each tick. The sensor "
            "cursor (the LAST_LOAD_TIME of the most recent processed file) "
            "prevents re-firing for the same file; lookback just bounds "
            "the query window."
        ),
    )
    pass_file_metadata: bool = Field(
        default=True,
        description=(
            "Include file_name + file_size + row_count in the run_config "
            "passed to the triggered job. Disable if you have many "
            "RunRequests per tick and want to keep the config small."
        ),
    )

    # ── Asset-style metadata (optional) ────────────────────────────────
    description: Optional[str] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)

    def _connect(self):
        import snowflake.connector
        ck = dict(
            account=self.account, user=self.user,
            database=self.database, schema=self.schema_name,
        )
        if self.warehouse: ck["warehouse"] = self.warehouse
        if self.role:      ck["role"] = self.role
        if self.authenticator:
            ck["authenticator"] = self.authenticator
            if self.private_key_file:
                ck["private_key_file"] = self.private_key_file
                if self.private_key_file_pwd:
                    ck["private_key_file_pwd"] = self.private_key_file_pwd
            elif self.token:
                ck["token"] = self.token
        elif self.password:
            ck["password"] = self.password
        else:
            raise ValueError(
                f"{type(self).__name__}: must set password OR authenticator+key/token."
            )
        return snowflake.connector.connect(**ck)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        default_status = (
            DefaultSensorStatus.RUNNING if self.default_status == "running"
            else DefaultSensorStatus.STOPPED
        )

        @sensor(
            name=_self.sensor_name,
            job_name=_self.job_name,
            minimum_interval_seconds=_self.minimum_interval_seconds,
            default_status=default_status,
            description=_self.description or
                f"Watches Snowpipe {_self.pipe_name} for new file ingestions.",
        )
        def snowflake_snowpipe_load_sensor(context: SensorEvaluationContext):
            conn = _self._connect()
            try:
                cur = conn.cursor()
                # COPY_HISTORY is a table function; we filter by destination
                # table + recent time window. PIPE_NAME column tells us
                # which pipe triggered each load.
                fq_dest = f"{_self.database}.{_self.schema_name}.{_self.destination_table}"
                fq_pipe = f"{_self.database}.{_self.schema_name}.{_self.pipe_name}"
                cur.execute(f"""
                    SELECT
                      FILE_NAME, LAST_LOAD_TIME, ROW_COUNT, FILE_SIZE,
                      STATUS, ERROR_COUNT, FIRST_ERROR_MESSAGE
                    FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
                      TABLE_NAME => '{fq_dest}',
                      START_TIME => DATEADD('minute', -{_self.lookback_minutes},
                                            CURRENT_TIMESTAMP())
                    ))
                    WHERE PIPE_NAME = '{fq_pipe}'
                    ORDER BY LAST_LOAD_TIME ASC
                """)
                rows = cur.fetchall()
                cur.close()
            except Exception as e:
                return SensorResult(skip_reason=f"COPY_HISTORY query failed: {e}")
            finally:
                conn.close()

            if not rows:
                return SensorResult(skip_reason="No loads in lookback window.")

            # Cursor is the LAST_LOAD_TIME of the last processed file (as
            # ISO string). New loads have LAST_LOAD_TIME > cursor.
            prev_cursor = context.cursor or ""
            new_loads = []
            for r in rows:
                file_name, last_load_time, row_count, file_size, \
                  status, error_count, first_error = r
                tstr = last_load_time.isoformat() if last_load_time else ""
                if tstr <= prev_cursor:
                    continue
                new_loads.append({
                    "file_name": file_name,
                    "last_load_time": tstr,
                    "row_count": int(row_count or 0),
                    "file_size": int(file_size or 0),
                    "status": status,
                    "error_count": int(error_count or 0),
                    "first_error": first_error,
                })

            if not new_loads:
                return SensorResult(skip_reason=f"No new loads since cursor {prev_cursor!r}.")

            # One RunRequest per file; run_key=file_name dedups in Dagster.
            run_requests = []
            for ld in new_loads:
                cfg = {}
                if _self.pass_file_metadata:
                    cfg = {"ops": {"config": {
                        "snowpipe_file_name":      ld["file_name"],
                        "snowpipe_last_load_time": ld["last_load_time"],
                        "snowpipe_row_count":      ld["row_count"],
                        "snowpipe_file_size":      ld["file_size"],
                        "snowpipe_status":         ld["status"],
                    }}}
                run_requests.append(RunRequest(
                    run_key=f"snowpipe:{_self.pipe_name}:{ld['file_name']}",
                    run_config=cfg,
                ))

            # Cursor advances to the latest seen timestamp.
            new_cursor = new_loads[-1]["last_load_time"]
            return SensorResult(run_requests=run_requests, cursor=new_cursor)

        return dg.Definitions(sensors=[snowflake_snowpipe_load_sensor])

    @classmethod
    def get_description(cls) -> str:
        return (
            "Watch a Snowflake PIPE for new file-level loads (COPY_HISTORY) and "
            "trigger downstream Dagster runs per ingested file. Works for both "
            "AUTO_INGEST and manual PUT-COPY pipes."
        )
