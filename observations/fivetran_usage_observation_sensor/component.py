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

This sensor queries INCREMENTAL_MAR on an interval, recomputing the full
CUMULATIVE month-to-date MAR per (schema_name, table_name) every tick --
summed from the start of the current calendar month through today, not a
short rolling window. That's deliberate: Fivetran can restate a prior day's
row after the fact, and a short rolling window (e.g. "just sum the last 2
days") would permanently miss a restatement to any day older than the
window, with no mechanism to ever pick that correction up. Recomputing the
full month every tick means any restatement this month gets picked up on
the very next tick, no matter how old.

Dagster+ Insights custom metrics have no configurable aggregation -- you
register a metadata key, not a sum/average/last-value choice, and (matching
every built-in Insights metric: credits, compute duration, etc., all
inherently additive) the values reported under that key get SUMMED to build
each period's chart point. That means emitting the CUMULATIVE total itself
on every tick would be wrong: Insights would sum together every reading we
ever reported, turning a real total of 1,000 into a chart showing many times
that, growing worse the more often this sensor ticks. So this sensor reports
the DELTA -- the net change in the cumulative total since the last tick we
saw a change for that asset -- not the level. Summing deltas over any period
telescopes back to the real net change for that period, regardless of how
many ticks happened or how Insights buckets them by day. The one structural
limitation this can't paper over: Insights attaches a value to the
OBSERVATION's own timestamp, not any date we put in metadata, so a late
restatement's delta lands on the day it was DETECTED, not the Fivetran day
it technically belongs to -- the running total still converges to correct,
but day-level attribution of a late correction won't be perfectly placed.

The cumulative baseline resets at the start of each calendar month (tracked
in the sensor's own cursor, alongside the per-asset running totals) so the
first tick of a new month reports that month's fresh total as its own delta,
not a large negative blip from comparing against last month's final total.
One matching scope limit: a restatement to a PRIOR month's data, arriving
after this sensor has already rolled over, falls outside this tick's
month-to-date query entirely and won't be caught -- a narrower version of
the same tradeoff, scoped to Fivetran's own calendar-month billing period
rather than an arbitrary window.

An AssetObservation is emitted per asset ONLY when the delta is non-zero --
a daily-synced usage table polled hourly should produce zero events on most
ticks, not a flood of no-op observations. The delta is attached as a plain
numeric metadata field (`fivetran/mar`, MetadataValue.int) under a fixed key
specifically so it can be registered as a Dagster+ Insights custom metric.
"""
import json
from datetime import date
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
    """Deterministic synthetic rows -- the CUMULATIVE month-to-date total per
    table, as a real query would return. Two grow across ticks, one never
    changes, so a demo run visibly exercises both the "emit a delta" and
    "skip, unchanged" paths."""
    return [
        {"schema_name": "postgres_orders", "table_name": "orders", "mar_month_to_date": 1000 + tick * 250, "measured_through": date.today().isoformat()},
        {"schema_name": "postgres_orders", "table_name": "customers", "mar_month_to_date": 4200, "measured_through": date.today().isoformat()},
        {"schema_name": "salesforce", "table_name": "opportunity", "mar_month_to_date": 300 + tick * 15, "measured_through": date.today().isoformat()},
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
            current_month = date.today().strftime("%Y-%m")
            cursor_state = json.loads(context.cursor) if context.cursor else {}
            tick = cursor_state.get("tick", 0)
            # A new calendar month means every asset's running total resets to
            # zero (it's a fresh billing period) -- forget last month's
            # baselines so this tick's delta is that fresh total, not a large
            # negative blip from diffing against last month's final number.
            if cursor_state.get("month") != current_month:
                last_mar: Dict[str, float] = {}
            else:
                last_mar = cursor_state.get("last_mar", {})

            if _self.demo_mode:
                rows = _demo_rows(tick)
            else:
                client = getattr(context.resources, resource_key, None)
                if client is None:
                    return dg.SensorResult(skip_reason=f"resource '{resource_key}' not found on context")
                # Full month-to-date, not a rolling window -- see the module
                # docstring for why: this is what makes a late restatement to
                # any day this month get picked up automatically, with no
                # window for it to fall outside of.
                month_start = date.today().replace(day=1).isoformat()
                where = [f"measured_date >= '{month_start}'"]
                if _self.free_type_filter:
                    where.append(f"free_type = '{_self.free_type_filter}'")
                sql = (
                    f"SELECT schema_name, table_name, SUM(incremental_rows) AS mar_month_to_date, "
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
                mar = row.get("mar_month_to_date")
                if schema_name is None or table_name is None or mar is None:
                    continue
                asset_key = dg.AssetKey([*prefix, str(schema_name), str(table_name)])
                asset_key_str = asset_key.to_user_string()
                current_total = float(mar)
                delta = current_total - new_last_mar.get(asset_key_str, 0.0)

                # Only emit a non-zero delta -- a daily-synced usage table
                # polled hourly should produce zero events on most ticks, not
                # a no-op flood. Insights sums whatever's reported under a
                # metric key with no other aggregation choice (matching every
                # built-in metric, all additive), so this reports the CHANGE,
                # not the cumulative level -- summing deltas telescopes back
                # to the real total for any period; summing repeated levels
                # would not.
                if delta == 0:
                    continue
                new_last_mar[asset_key_str] = current_total

                _event_cls = dg.AssetMaterialization if _self.emit_materialization else dg.AssetObservation
                events.append(_event_cls(
                    asset_key=asset_key,
                    metadata={
                        # The ONLY numeric field -- register exactly this key as a
                        # Dagster+ Insights custom metric. It's a delta, meant to be
                        # summed; `mar_month_to_date` below is a cumulative level
                        # (text on purpose, so it can't be mistakenly registered as
                        # a second, double-counting metric).
                        "fivetran/mar": dg.MetadataValue.int(int(delta)),
                        "fivetran/mar_month_to_date": dg.MetadataValue.text(str(int(current_total))),
                        "fivetran/measured_through": dg.MetadataValue.text(str(row.get("measured_through", ""))),
                        "fivetran/schema_name": dg.MetadataValue.text(str(schema_name)),
                        "fivetran/table_name": dg.MetadataValue.text(str(table_name)),
                    },
                ))

            new_cursor = json.dumps({"tick": tick + 1, "month": current_month, "last_mar": new_last_mar})
            if not events:
                return dg.SensorResult(skip_reason="no changed MAR values since last check", cursor=new_cursor)
            return dg.SensorResult(asset_events=events, cursor=new_cursor)

        return dg.Definitions(sensors=[_fivetran_usage_obs])
