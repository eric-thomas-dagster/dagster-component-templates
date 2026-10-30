"""Fivetran Usage Observation Sensor Component.

Fivetran's REST API has no endpoint for MAR (Monthly Active Rows) or usage
data at all -- confirmed against the official API reference, which lists 22
resource categories (Connection, Destination, Group, Transformation, ...)
and none of them relate to usage, billing, or metering. The only way to get
this data is the Fivetran Platform Connector: a regular connection Fivetran
auto-adds to each destination, which syncs on a schedule like any other
connector and writes usage (among other things) into a destination table,
INCREMENTAL_MAR -- there is no API to read the numbers directly, only SQL
against that table.

That table is keyed by (schema_name, table_name), not just the connector --
which happens to be exactly how `dagster_fivetran`'s default translator
builds a Dagster asset key for a synced table (`AssetKey([schema_name,
table_name])`, i.e. `f"{schema_name}.{table_name}"`). So under default
translation settings, reconstructing the matching asset key from a query
result row is exact, not fuzzy.

This sensor queries INCREMENTAL_MAR on an interval, sums incremental_rows
per (schema_name, table_name) over a short lookback window (Fivetran can
restate prior days), and emits an AssetObservation per asset ONLY when that
sum has changed since the last tick it saw for that asset -- a daily-synced
usage table polled hourly should produce zero events on most ticks, not a
flood of no-op observations. The summed value is attached as a plain numeric
metadata field (`fivetran/incremental_mar`, MetadataValue.int) specifically
so it can be registered as a Dagster+ Insights custom metric (Insights
custom metrics read numeric int/float metadata from either materializations
or observations -- confirmed against the current Dagster+ docs).
"""
import json
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


def _fetch_rows(connection_resource, sql: str) -> List[Dict[str, Any]]:
    """Run `sql` against a DB-API-ish resource and return every row as a
    lower-cased-key dict. Supports the two connection shapes already used
    elsewhere in this registry: `conn.cursor()` (Snowflake-style) and
    `conn.execute(sql)` returning a result with `.fetchall()` (DuckDB-style).
    Column names are lower-cased on the way out since drivers disagree on
    default casing (Snowflake upper-cases unquoted identifiers, DuckDB
    lower-cases them) -- this component's own SELECT list controls the
    aliases, so lower-casing here makes dict access predictable regardless
    of the underlying warehouse.
    """
    with connection_resource.get_connection() as conn:
        if hasattr(conn, "cursor"):
            cursor = conn.cursor()
            cursor.execute(sql)
            cols = [c[0] for c in cursor.description]
            rows = cursor.fetchall()
            cursor.close()
        else:
            result = conn.execute(sql)
            cols = [c[0] for c in result.description]
            rows = result.fetchall()
    return [dict(zip((c.lower() for c in cols), row)) for row in rows]


def _demo_rows(tick: int) -> List[Dict[str, Any]]:
    """Deterministic synthetic rows -- two tables whose MAR changes on
    alternating ticks, one that never changes, so a demo run visibly
    exercises both the "emit" and "skip, unchanged" paths."""
    return [
        {"schema_name": "postgres_orders", "table_name": "orders", "incremental_mar": 1000 + (tick % 3) * 250, "measured_through": date.today().isoformat()},
        {"schema_name": "postgres_orders", "table_name": "customers", "incremental_mar": 4200, "measured_through": date.today().isoformat()},
        {"schema_name": "salesforce", "table_name": "opportunity", "incremental_mar": 300 + tick * 15, "measured_through": date.today().isoformat()},
    ]


class FivetranUsageObservationSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Observe Fivetran Platform Connector usage (MAR) per synced table, for Dagster+ Insights custom metrics."""

    sensor_name: str = Field(description="Unique sensor name.")
    resource_key: str = Field(
        description=(
            "Dagster resource exposing `.get_connection()` (a context manager yielding a "
            "DB-API-ish connection -- `.cursor()` or `.execute()`) against the warehouse "
            "the Fivetran Platform Connector writes to. Not used when demo_mode=True."
        ),
    )
    mar_table: str = Field(
        default="incremental_mar",
        description=(
            "Fully-qualified INCREMENTAL_MAR table name, exactly as your warehouse expects "
            "it, e.g. 'MYDB.FIVETRAN_LOG.INCREMENTAL_MAR' (Snowflake) or "
            "'my_project.fivetran_log.incremental_mar' (BigQuery)."
        ),
    )
    lookback_days: int = Field(
        default=2,
        ge=1,
        description=(
            "How many days back to sum incremental_rows each tick. >1 so a late Fivetran "
            "restatement of a prior day still gets picked up as a changed value."
        ),
    )
    free_type_filter: Optional[str] = Field(
        default="PAID",
        description=(
            "Filter on INCREMENTAL_MAR's free_type column -- 'PAID' (default) matches "
            "Fivetran's own billing-focused sample queries. Set to null/empty to include free usage too."
        ),
    )
    asset_key_prefix: Optional[List[str]] = Field(
        default=None,
        description=(
            "Prefix segments to prepend to the reconstructed asset key (schema_name, "
            "table_name). Leave unset to match dagster_fivetran's DEFAULT translator "
            "exactly (AssetKey([schema_name, table_name])) -- only set this if your "
            "FivetranAccountComponent's own `translation:` adds a prefix."
        ),
    )
    check_interval_seconds: int = Field(
        default=3600,
        description="Seconds between checks. The Platform Connector itself typically syncs once a day, so hourly is already generous.",
    )
    emit_materialization: bool = Field(
        default=False,
        description=(
            "When False (default), emits AssetObservation -- free of Dagster+ credit "
            "charges, and a MAR tick won't trigger downstream AutomationCondition.eager() "
            "runs (usually not what you want from a usage number changing). Set True only "
            "if you specifically want usage changes to drive downstream automation."
        ),
    )
    demo_mode: bool = Field(
        default=False,
        description="Skip the real warehouse query entirely -- returns synthetic, deterministic MAR rows that change on alternating ticks. No resource_key or real warehouse required.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        resource_key = self.resource_key
        required_resource_keys = set() if self.demo_mode else {resource_key}

        @dg.sensor(
            name=_self.sensor_name,
            minimum_interval_seconds=_self.check_interval_seconds,
            required_resource_keys=required_resource_keys,
            asset_selection=dg.AssetSelection.all(),
        )
        def _fivetran_usage_obs(context: dg.SensorEvaluationContext, **_resources):
            cursor_state = json.loads(context.cursor) if context.cursor else {"tick": 0, "last_mar": {}}
            tick = cursor_state.get("tick", 0)
            last_mar: Dict[str, float] = cursor_state.get("last_mar", {})

            if _self.demo_mode:
                rows = _demo_rows(tick)
            else:
                client = getattr(context.resources, resource_key, None)
                if client is None:
                    return dg.SensorResult(skip_reason=f"resource '{resource_key}' not found on context")
                lookback_start = (date.today() - timedelta(days=_self.lookback_days)).isoformat()
                where = [f"measured_date >= '{lookback_start}'"]
                if _self.free_type_filter:
                    where.append(f"free_type = '{_self.free_type_filter}'")
                sql = (
                    f"SELECT schema_name, table_name, SUM(incremental_rows) AS incremental_mar, "
                    f"MAX(measured_date) AS measured_through "
                    f"FROM {_self.mar_table} "
                    f"WHERE {' AND '.join(where)} "
                    f"GROUP BY schema_name, table_name"
                )
                try:
                    rows = _fetch_rows(client, sql)
                except Exception as e:
                    return dg.SensorResult(skip_reason=f"query failed: {e}")

            events = []
            new_last_mar = dict(last_mar)
            prefix = list(_self.asset_key_prefix or [])
            for row in rows:
                schema_name = row.get("schema_name")
                table_name = row.get("table_name")
                mar = row.get("incremental_mar")
                if schema_name is None or table_name is None or mar is None:
                    continue
                asset_key = dg.AssetKey([*prefix, str(schema_name), str(table_name)])
                asset_key_str = asset_key.to_user_string()
                mar = float(mar)

                # Only emit when the summed value actually changed since the last tick
                # that saw this asset -- a daily-synced usage table polled hourly
                # should produce zero events on most ticks, not a no-op flood.
                if new_last_mar.get(asset_key_str) == mar:
                    continue
                new_last_mar[asset_key_str] = mar

                _event_cls = dg.AssetMaterialization if _self.emit_materialization else dg.AssetObservation
                events.append(_event_cls(
                    asset_key=asset_key,
                    metadata={
                        # Numeric, stable key name -- register this exact key as a
                        # Dagster+ Insights custom metric to chart it.
                        "fivetran/incremental_mar": dg.MetadataValue.int(int(mar)),
                        "fivetran/measured_through": dg.MetadataValue.text(str(row.get("measured_through", ""))),
                        "fivetran/schema_name": dg.MetadataValue.text(str(schema_name)),
                        "fivetran/table_name": dg.MetadataValue.text(str(table_name)),
                    },
                ))

            new_cursor = json.dumps({"tick": tick + 1, "last_mar": new_last_mar})
            if not events:
                return dg.SensorResult(skip_reason="no changed MAR values since last check", cursor=new_cursor)
            return dg.SensorResult(asset_events=events, cursor=new_cursor)

        return dg.Definitions(sensors=[_fivetran_usage_obs])
