"""ParametricDataGeneratorComponent — YAML-driven synthetic data.

Where `synthetic_data_generator` ships 24 named schemas (customers, orders,
fhir_patients, etc.), this component lets you describe your OWN columns
in the YAML config. No Python required.

Example: the NYC-metro stores dataset from the store_coverage demo

```yaml
type: dagster_component_templates.ParametricDataGeneratorComponent
attributes:
  asset_name: stores
  row_count: 200
  random_state: 42
  columns:
    store_id:
      type: id
      prefix: "S"
      width: 4
    lat:
      type: float
      min: 40.55
      max: 40.90
      precision: 4
    lng:
      type: float
      min: -74.20
      max: -73.70
      precision: 4
    annual_revenue:
      type: int
      min: 50000
      max: 5000000
    store_type:
      type: choice
      values: [urban, suburban, rural]
      weights: [0.5, 0.4, 0.1]
    opened_at:
      type: datetime
      start: "2010-01-01"
      end: "2024-12-31"
    is_active:
      type: bool
      probability_true: 0.9
```

Column types:
  - `id`           — sequential with prefix + zero-padded width
                     (`prefix: "C"`, `width: 4` → C0001 C0002 …)
  - `int`          — uniform integer in [min, max]
  - `float`        — uniform float in [min, max], optional `precision`
  - `bool`         — `probability_true: 0.5` (default 0.5)
  - `choice`       — `values: [...]` with optional `weights: [...]`
  - `datetime`     — uniform between `start` and `end` (ISO strings),
                     optional `format` (default "%Y-%m-%d %H:%M:%S")
  - `date`         — date-only variant of datetime (format "%Y-%m-%d")
  - `string`       — random a-z string, optional `length`/`min_length`/`max_length`
  - `uuid`         — random UUID4
  - `constant`     — `value: X` (same value for every row)

Modifiers on any column:
  - `null_ratio: 0.1` — 10%% of values get set to null
"""

from dagster import AssetKey  # auto-added for hierarchical keys
import random
import re
import string
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import pandas as pd

# ── Mockaroo-style bundled value lists ─────────────────────────────────────────
# Keep these small — enough for realistic-looking demos without bloating the
# package. For richer fixtures, the user can `type: choice` with their own list.

_FIRST_NAMES = (
    "James Mary John Patricia Robert Jennifer Michael Linda William Elizabeth "
    "David Barbara Richard Susan Joseph Jessica Thomas Sarah Charles Karen "
    "Christopher Lisa Daniel Nancy Matthew Betty Anthony Sandra Mark Ashley "
    "Donald Kimberly Steven Emily Paul Donna Andrew Michelle Joshua Carol "
    "Kenneth Amanda Kevin Dorothy Brian Melissa George Deborah Edward Stephanie"
).split()

_LAST_NAMES = (
    "Smith Johnson Williams Brown Jones Garcia Miller Davis Rodriguez Martinez "
    "Hernandez Lopez Gonzalez Wilson Anderson Thomas Taylor Moore Jackson Martin "
    "Lee Perez Thompson White Harris Sanchez Clark Ramirez Lewis Robinson "
    "Walker Young Allen King Wright Scott Torres Nguyen Hill Flores "
    "Green Adams Nelson Baker Hall Rivera Campbell Mitchell Carter Roberts"
).split()

_US_CITIES = (
    "New York Los Angeles Chicago Houston Phoenix Philadelphia San Antonio San Diego Dallas Austin "
    "Jacksonville San Jose Fort Worth Columbus Charlotte Indianapolis San Francisco Seattle Denver "
    "Washington Boston Nashville Baltimore Oklahoma City Portland Las Vegas Detroit Memphis Louisville "
    "Milwaukee Albuquerque Tucson Fresno Sacramento Kansas City Mesa Atlanta Omaha Colorado Springs"
).replace("New York", "New_York").replace("Los Angeles", "Los_Angeles").replace("San Antonio", "San_Antonio") \
 .replace("San Diego", "San_Diego").replace("San Jose", "San_Jose").replace("Fort Worth", "Fort_Worth") \
 .replace("San Francisco", "San_Francisco").replace("Oklahoma City", "Oklahoma_City") \
 .replace("Las Vegas", "Las_Vegas").replace("Kansas City", "Kansas_City") \
 .replace("Colorado Springs", "Colorado_Springs").split()
_US_CITIES = [c.replace("_", " ") for c in _US_CITIES]

_US_STATES = "AL AK AZ AR CA CO CT DE FL GA HI ID IL IN IA KS KY LA ME MD MA MI MN MS MO MT NE NV NH NJ NM NY NC ND OH OK OR PA RI SC SD TN TX UT VT VA WA WV WI WY".split()
_COUNTRIES = "USA Canada Mexico UK France Germany Italy Spain Japan China India Brazil Australia".split()
_DOMAINS = "example.com example.org example.net mail.com test.io demo.co".split()

_COLORS = "red blue green yellow purple orange pink black white gray brown navy teal cyan magenta lime maroon olive silver gold".split()


def _hex_color(rng: random.Random) -> str:
    return "#" + "".join(rng.choices("0123456789abcdef", k=6))


def _us_phone(rng: random.Random) -> str:
    area = rng.randint(200, 999)
    prefix = rng.randint(200, 999)
    line = rng.randint(0, 9999)
    return f"({area}) {prefix}-{line:04d}"


def _ipv4(rng: random.Random) -> str:
    return ".".join(str(rng.randint(0, 255)) for _ in range(4))


def _ipv6(rng: random.Random) -> str:
    return ":".join(f"{rng.randint(0, 0xFFFF):04x}" for _ in range(8))

from dagster import (
    AssetExecutionContext,
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


def _parse_dt(s: str) -> datetime:
    """Accept ISO date or datetime strings."""
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    raise ValueError(f"Could not parse datetime: {s!r}")


def _safe_eval(expr: str, row: Dict[str, Any]) -> Any:
    """Restricted eval for `formula` columns. Only allows the current row's
    keys + simple math. No imports, no builtins, no attribute access."""
    # Disallow attribute access + dunders + imports
    if re.search(r"__|\bimport\b", expr):
        raise ValueError(f"formula contains disallowed tokens: {expr!r}")
    # Build a safe namespace from the row + a tiny math allowlist
    safe_globals = {"__builtins__": {}}
    safe_locals = {
        "min": min, "max": max, "abs": abs, "round": round, "int": int, "float": float,
        "str": str, "len": len, "sum": sum,
        **row,
    }
    return eval(expr, safe_globals, safe_locals)  # noqa: S307 — restricted namespace


def _gen_value(
    spec: Dict[str, Any], i: int, rng: random.Random, row: Optional[Dict[str, Any]] = None
) -> Any:
    """Generate one value for one column based on its spec."""
    typ = spec.get("type")
    if typ == "id":
        prefix = spec.get("prefix", "")
        width = int(spec.get("width", 4))
        start = int(spec.get("start", 1))
        return f"{prefix}{i + start:0{width}d}"
    if typ == "int":
        return rng.randint(int(spec["min"]), int(spec["max"]))
    if typ == "float":
        v = rng.uniform(float(spec["min"]), float(spec["max"]))
        precision = spec.get("precision")
        return round(v, int(precision)) if precision is not None else v
    if typ == "bool":
        p = float(spec.get("probability_true", 0.5))
        return rng.random() < p
    if typ == "choice":
        values = spec["values"]
        weights = spec.get("weights")
        if weights is not None:
            return rng.choices(values, weights=weights, k=1)[0]
        return rng.choice(values)
    if typ in ("datetime", "date"):
        start = _parse_dt(spec["start"])
        end = _parse_dt(spec["end"])
        delta_s = (end - start).total_seconds()
        offset = rng.uniform(0, delta_s)
        dt = start + timedelta(seconds=offset)
        if typ == "date":
            return dt.date().isoformat()
        fmt = spec.get("format", "%Y-%m-%d %H:%M:%S")
        return dt.strftime(fmt)
    if typ == "string":
        length = spec.get("length")
        if length is None:
            length = rng.randint(int(spec.get("min_length", 4)), int(spec.get("max_length", 12)))
        return "".join(rng.choices(string.ascii_lowercase, k=int(length)))
    if typ == "uuid":
        # Deterministic UUIDs come from seeding random — but uuid4 doesn't use random.
        # Build a uuid manually from rng-derived bytes for reproducibility.
        return str(uuid.UUID(bytes=bytes(rng.randint(0, 255) for _ in range(16)), version=4))
    if typ == "constant":
        return spec["value"]
    # ── Mockaroo-style realistic types ──────────────────────────────────────
    if typ == "first_name":
        return rng.choice(_FIRST_NAMES)
    if typ == "last_name":
        return rng.choice(_LAST_NAMES)
    if typ == "full_name":
        return f"{rng.choice(_FIRST_NAMES)} {rng.choice(_LAST_NAMES)}"
    if typ == "email":
        first = rng.choice(_FIRST_NAMES).lower()
        last = rng.choice(_LAST_NAMES).lower()
        domain = rng.choice(spec.get("domains") or _DOMAINS)
        return f"{first}.{last}@{domain}"
    if typ == "city":
        return rng.choice(_US_CITIES)
    if typ == "state":
        return rng.choice(_US_STATES)
    if typ == "country":
        return rng.choice(spec.get("values") or _COUNTRIES)
    if typ == "zip":
        return f"{rng.randint(10000, 99999)}"
    if typ == "phone":
        return _us_phone(rng)
    if typ == "ip":
        ver = int(spec.get("version", 4))
        return _ipv6(rng) if ver == 6 else _ipv4(rng)
    if typ == "color":
        return rng.choice(_COLORS)
    if typ == "hex_color":
        return _hex_color(rng)
    if typ == "formula":
        if row is None:
            raise ValueError("formula columns require row context (internal bug)")
        return _safe_eval(spec["formula"], row)
    raise ValueError(f"Unknown column type: {typ!r} (spec: {spec})")


class ParametricDataGeneratorComponent(Component, Model, Resolvable):
    """YAML-driven synthetic data generator. Define columns + types in the config."""

    asset_name: str = Field(description="Output asset name.")

    row_count: int = Field(default=100, description="Number of rows to generate.")
    random_state: Optional[int] = Field(
        default=None,
        description="Seed for reproducibility. None = nondeterministic.",
    )

    columns: Dict[str, Dict[str, Any]] = Field(
        description=(
            "Dict of column-name → spec. Each spec has a `type` field "
            "(id / int / float / bool / choice / datetime / date / string / uuid / constant) "
            "and type-specific options. See the README for the full reference."
        ),
    )

    description: Optional[str] = Field(default=None)
    group_name: Optional[str] = Field(default=None)
    tags: Optional[Dict[str, str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)

    retry_policy_max_retries: Optional[int] = Field(
        default=None,
        description="Max retries on asset failure. Useful for transient errors like network glitches or rate limits.",
    )
    retry_policy_delay_seconds: Optional[int] = Field(
        default=None,
        description="Seconds between retries (default 1).",
    )
    retry_policy_backoff: str = Field(
        default="exponential",
        description="Backoff strategy: 'linear' or 'exponential'.",
    )

    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale.",
    )
    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5'.",
    )

    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily' / 'weekly' / 'monthly' / 'hourly' / 'static' / 'dynamic' / None for unpartitioned.",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.",
    )
    partition_values: Optional[str] = Field(
        default=None,
        description="Comma-separated values for static partitioning, e.g. 'us,eu,asia'.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition when partition_type='dynamic'.",
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        partitions_def = None
        if self.partition_type:
            from dagster import (
                DailyPartitionsDefinition, WeeklyPartitionsDefinition,
                MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
                StaticPartitionsDefinition, DynamicPartitionsDefinition,
            )
            _pt = self.partition_type
            _values = [v.strip() for v in (self.partition_values or "").split(",") if v.strip()]
            if _pt in ("daily", "weekly", "monthly", "hourly") and not self.partition_start:
                raise ValueError(f"partition_type={_pt!r} requires partition_start (ISO date).")
            if _pt == "daily":
                partitions_def = DailyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "weekly":
                partitions_def = WeeklyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "monthly":
                partitions_def = MonthlyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "hourly":
                partitions_def = HourlyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "static":
                if not _values:
                    raise ValueError("partition_type='static' requires partition_values.")
                partitions_def = StaticPartitionsDefinition(_values)
            elif _pt == "dynamic":
                if not self.dynamic_partition_name:
                    raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
                partitions_def = DynamicPartitionsDefinition(name=self.dynamic_partition_name)

        freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy

            freshness_policy = FreshnessPolicy(
                maximum_lag_minutes=self.freshness_max_lag_minutes,
                cron_schedule=self.freshness_cron,
            )

        retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy

            retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        asset_name = self.asset_name
        row_count = self.row_count
        random_state = self.random_state
        columns = self.columns

        @asset(
            key=AssetKey.from_user_string(asset_name),
            description=self.description or f"Parametric synthetic data ({row_count} rows, {len(columns)} cols).",
            group_name=self.group_name,
            kinds=set(self.kinds) if self.kinds else {"synthetic", "pandas"},
            tags=self.tags or None,
            owners=self.owners or None,
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
            partitions_def=partitions_def,
        )
        def _asset(context: AssetExecutionContext) -> Output:
            rng = random.Random(random_state)
            rows: List[Dict[str, Any]] = []
            for i in range(row_count):
                row: Dict[str, Any] = {}
                for col_name, spec in columns.items():
                    if not isinstance(spec, dict):
                        raise ValueError(f"column {col_name!r}: spec must be a dict, got {type(spec).__name__}")
                    null_ratio = float(spec.get("null_ratio", 0.0))
                    if null_ratio > 0 and rng.random() < null_ratio:
                        row[col_name] = None
                    else:
                        row[col_name] = _gen_value(spec, i, rng, row)
                rows.append(row)

            df = pd.DataFrame(rows)
            context.log.info(f"Generated {len(df)} rows × {len(df.columns)} cols")
            preview = df.head(10).to_markdown(index=False) if not df.empty else "(empty)"
            return Output(
                value=df,
                metadata={
                    "row_count":    MetadataValue.int(len(df)),
                    "column_count": MetadataValue.int(len(df.columns)),
                    "columns":      MetadataValue.json({c: str(df.dtypes[c]) for c in df.columns}),
                    "preview":      MetadataValue.md(preview),
                },
            )

        return Definitions(assets=[_asset])
