# `ProfileAssetComponent` + `@profile` decorator

Auto-emit a **data profile** on every materialization. Global counts + per-column dtype / null rate / distinct count / (numeric) min/max/mean/std / (categorical) top-value ratio. Extend with `custom_probes` for anything else.

The profile lands in Dagster's event log as an `AssetObservation` with typed metadata. Over time, the event log IS the profile history — drift detection becomes a `context.instance.get_event_records` query, no external metrics store required.

## Two shapes

| Shape | Use when |
|---|---|
| **`ProfileAssetComponent`** (YAML) | New profiled asset |
| **`@profile` decorator** (Python) | Wrap an existing `@dg.asset` |

## What gets profiled

### Global
- `row_count` (int)
- `column_count` (int)
- `profiled_columns` (int — may be < column_count if `top_n_columns` set)

### Per column
- `dtype` (str)
- `null_count` (int), `null_ratio` (float)
- `distinct_count` (int)
- **Numeric only**: `min`, `max`, `mean`, `std`
- **Numeric only, opt-in**: `histogram` (bin_edges + counts) when `histogram_bins` is set
- **Numeric only, opt-in**: `quantiles` (`{"p25": ..., "p50": ..., "p75": ..., "p95": ..., "p99": ...}`) when `quantiles` is non-empty
- **Categorical only** (< `categorical_max_distinct` distinct): `top_value_ratio`

### Top-level (opt-in)
- `correlation_matrix`: Pearson correlation across numeric columns when `correlation_matrix: true`. Emitted as `{col_a: {col_b: 0.87, ...}, ...}`. Pairs where either column is all-null are skipped. Off by default — expensive on wide tables.

### User extensions

`custom_probes`: `[{name, python: 'mod:fn'}]`. Each probe function receives the DataFrame and returns a dict (mixed into the observation metadata):

```python
# my_project/probes.py
def avg_order_value(df):
    return {"avg_order_value": float(df["amount"].mean())}

def count_outliers_iqr(df):
    q1, q3 = df["amount"].quantile([0.25, 0.75])
    iqr = q3 - q1
    outliers = df[(df["amount"] < q1 - 1.5*iqr) | (df["amount"] > q3 + 1.5*iqr)]
    return {"n_outliers": int(len(outliers))}
```

## Why this belongs in Dagster

Every profile stat is emitted as `AssetObservation` with typed `MetadataValue`. Because it's in the event log:
- **Drift detection is free** — query prior observations, compute deltas.
- **Alerting is free** — sensor watches for `profile_row_count` dropping > 20% vs. prior.
- **Dashboards are free** — Dagster UI's observation panel shows the profile every time.
- **Agent lookups are free** — planner can ask "what's this asset's typical row count?" without an external metrics store.

Doing this outside Dagster requires an external time-series store + poller + retention policy. Doing it inside → 20 lines of decorator config.

## Full YAML example

```yaml
type: dagster_community_components.ProfileAssetComponent
attributes:
  asset_name: orders_profiled

  compute:
    kind: python
    python: "my_project.orders:build_daily"

  categorical_max_distinct: 100

  custom_probes:
    - name: avg_order_value
      python: "my_project.probes:avg_order_value"
    - name: outlier_count
      python: "my_project.probes:count_outliers_iqr"

  group_name: sales
  kinds: [python, profile, observability, sales]
```

## `@profile` decorator

```python
import dagster as dg
from dagster_community_components import profile

@dg.asset
@profile(
    categorical_max_distinct=100,
    custom_probes=[
        {"name": "avg_order_value", "python": "my_project.probes:avg_order_value"},
    ],
)
def orders(context):
    return build_orders()
```

## Composes with

- **`@data_contract`** — profile stats can feed contract SLA checks (drift on null_ratio = quality regression).
- **`@lifecycle`** — profile the STAGING data before publish.
- **`@cached`** — profile only fires on cache miss (compute actually ran).

## Drift detection recipe

```python
def drift_sensor(context):
    """Emit a run request when row_count drops > 20% vs the prior materialization."""
    from dagster import EventRecordsFilter, DagsterEventType
    records = context.instance.get_event_records(
        event_records_filter=EventRecordsFilter(
            event_type=DagsterEventType.ASSET_OBSERVATION,
            asset_key=AssetKey("orders_profiled"),
        ),
        limit=2, ascending=False,
    )
    if len(records) < 2:
        return
    cur = records[0].asset_observation.tags.get("profile_row_count")
    prev = records[1].asset_observation.tags.get("profile_row_count")
    if cur and prev and int(cur) < int(prev) * 0.8:
        # ALERT
        ...
```

## Expanded numeric profile (histograms, quantiles, correlation)

Three opt-in extensions to the numeric profile:

- `histogram_bins: int` — per-numeric-column histogram with this many bins. Emitted as `histogram: {bin_edges: [...], counts: [...]}` inside each numeric column profile. Default `None` → skipped.
- `quantiles: List[float]` — quantile fractions per numeric column. Emitted as `quantiles: {"p25": ..., "p50": ..., ...}`. Default `[0.25, 0.5, 0.75, 0.95, 0.99]`. Empty list disables.
- `correlation_matrix: bool` — Pearson correlation across numeric columns, emitted as a nested dict on the top-level profile. Default `False`. Skips pairs where either column is all-null. Off by default — expensive on wide tables.

### YAML

```yaml
type: dagster_community_components.ProfileAssetComponent
attributes:
  asset_name: orders_profiled
  compute:
    kind: python
    python: "my_project.orders:build_daily"

  histogram_bins: 10
  quantiles: [0.25, 0.5, 0.75, 0.95, 0.99]
  correlation_matrix: true
```

### `@profile`

```python
@dg.asset
@profile(
    histogram_bins=10,
    quantiles=[0.25, 0.5, 0.75, 0.95, 0.99],
    correlation_matrix=True,
)
def orders(context):
    return build_orders()
```

## CLI demos using this template

| Demo | Setup script | What it shows |
|---|---|---|
| [`profile_asset.md`](https://github.com/eric-thomas-dagster/dagster-community-components-cli/blob/main/examples/profile_asset.md) | [`setup_profile_asset_demo.sh`](https://github.com/eric-thomas-dagster/dagster-community-components-cli/blob/main/examples/setup_profile_asset_demo.sh) | 100% offline. Demonstrates BOTH shapes side by side: (1) `@profile` Python decorator over a `@dg.asset` returning a 500-row DataFrame, (2) `ProfileAssetComponent { wraps: SyntheticDataGeneratorComponent }` — zero Python, pure YAML composability. The outer profile extracts the DataFrame the inner returns and emits per-column `null_ratio` / `distinct_count` / `min` / `max` / `mean` / `std` as one `AssetObservation`. Every materialization is a JSON snapshot in the event log — drift detection = query the log, diff adjacent snapshots. |

```bash
curl -fsSL https://raw.githubusercontent.com/eric-thomas-dagster/dagster-community-components-cli/main/examples/setup_profile_asset_demo.sh | bash
```

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## What's not in v1 (roadmap)

- **PSI / KS drift scores** — compute drift score vs. baseline observation.
- **Freshness of profile** — emit a warning if the profile is stale (asset hasn't materialized recently).

## Fields

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | — | Dagster asset name. Required when NOT using `wraps:` (inherited from inner in wraps mode). |
| `group_name` | `str` | — | — |
| `description` | `str` | — | — |
| `owners` | `List[str]` | — | — |
| `tags` | `Dict[str, str]` | — | — |
| `kinds` | `List[str]` | — | Asset kinds. Default: ['python', 'profile', 'observability']. |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `upstream_asset_key` | `str` | — | — |
| `compute` | `Dict[str, Any]` | — | `{kind: python, python: 'mod:fn'}`. Returns pandas DataFrame. Mutually exclusive with `wraps`. |
| `wraps` | `Dict[str, Any]` | — | Wrap another DCC component's assets with auto-profiling instead of defining new compute. Shape: `{type: 'dagster_community_components.<Component>', attributes: {...}}`. Inner asset must return pandas.DataFrame. Mutually… _(full docs in schema.json + component README)_ |
| `categorical_max_distinct` | `int` | `50` | Columns with <= this many distinct values get `top_value_ratio` computed. |
| `top_n_columns` | `int` | — | Profile only first N columns (for very wide DataFrames). Omit to profile all. |
| `custom_probes` | `List[Dict[str, Any]]` | — | Extensions: [{name, python: 'mod:fn'}]. fn(df) returns dict. |
| `histogram_bins` | `int` | — | If set, emit per-numeric-column histogram with this many bins. |
| `quantiles` | `List[float]` | `lambda: [0.25, 0.5, 0.75, 0.95, 0.99]()` | Quantile fractions to compute per numeric column. Empty list disables. |
| `correlation_matrix` | `bool` | `false` | If True, compute Pearson correlation between numeric columns and emit as metadata. Expensive on wide tables — off by default. |
| `histogram_render` | `str` | `"ascii"` | How histograms render in the Metadata panel. 'ascii' (default, zero deps) = Unicode-bar table + sparkline; 'png' (requires matplotlib) = embedded PNG data-URI; 'both' = sparkline preview + PNG. Falls back to 'ascii' if m… _(full docs in schema.json + component README)_ |

[//]: # (FIELDS:END)
