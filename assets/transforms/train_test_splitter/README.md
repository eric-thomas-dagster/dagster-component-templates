# Train/Test Splitter Component

Split a DataFrame into **train**, **test**, and (optionally) **validation** Dagster assets. Three strategies cover the common ML/statistics workflows: random sampling, chronological cutoff, and stable hash-based bucketing.

Unlike most transforms, this component emits **two or three downstream assets** from a single component definition:
- `<asset_name>_train`
- `<asset_name>_test`
- `<asset_name>_val` (only when `val_size > 0`)

## Strategies

| Strategy | How It Works | Use When |
|---|---|---|
| `random` | Deterministic random split (`seed` controls it). Optionally stratified by class label or grouped by an ID column | Default ML workflow; classification tasks where class balance matters |
| `time` | Sort by `time_column`; oldest rows → train, newest → test | Time-series modeling; prevents temporal leakage where future leaks into past |
| `hash` | Stable hash of `group_column` (or row index) → bucket | Reproducibility across schema/seed changes; multi-table consistent splits (use the same `group_column` everywhere) |

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Output asset prefix; final names are `<asset_name>_train` / `_test` / `_val` |
| `upstream_asset_key` | `str` | required | Upstream DataFrame asset |
| `strategy` | `str` | `"random"` | One of `random`, `time`, `hash` |
| `test_size` | `float` | `0.2` | Fraction of rows in the test split |
| `val_size` | `float` | `0.0` | Fraction of rows in validation. `0.0` skips emitting a validation asset |
| `seed` | `int` | `42` | Deterministic seed for `random` and `hash` strategies |
| `stratify_column` | `Optional[str]` | `null` | Class label to stratify on (random strategy only) |
| `group_column` | `Optional[str]` | `null` | Group key — all rows sharing a value land in the same split (prevents leakage for related rows like sessions, users) |
| `time_column` | `Optional[str]` | `null` | Required for `strategy: time`; column to sort by |

## Stratify vs Group

These two options solve different problems and are mutually exclusive:

- **`stratify_column`** preserves the **distribution of class labels** across splits — important for imbalanced classification (e.g. fraud detection where 1% of rows are positive).
- **`group_column`** ensures rows that **belong together** stay together (e.g. all sessions from the same user, all rows from the same medical patient). This prevents the model from cheating by seeing related rows in both train and test.

If you need both (rare), pre-aggregate to one row per group, stratify, then re-join.

## Example YAML

```yaml
type: dagster_component_templates.TrainTestSplitterComponent
attributes:
  asset_name: customer_features
  upstream_asset_key: scaled_customer_features
  strategy: random
  test_size: 0.2
  val_size: 0.1
  seed: 42
  stratify_column: churned
  group_name: ml_datasets
```

This produces three assets: `customer_features_train`, `customer_features_test`, `customer_features_val`.

### Time-based example

```yaml
attributes:
  asset_name: order_features
  upstream_asset_key: cleaned_orders
  strategy: time
  time_column: order_date
  test_size: 0.2
```

### Hash-based, group-aware example

```yaml
attributes:
  asset_name: session_features
  upstream_asset_key: enriched_sessions
  strategy: hash
  group_column: user_id
  test_size: 0.2
  seed: 42
```

The same `group_column` + `seed` produces the same split every run, even if rows are added or reordered.

## Output Metadata

Each emitted asset records:
- `split_role` — `train`, `test`, or `val`
- `split_strategy` — the configuration used
- `split_fraction` — actual achieved fraction (may differ slightly from configured `test_size`/`val_size` due to rounding or stratification balancing)
- `rows_in_upstream` — total upstream rows for cross-checking
- Standard `dagster/row_count`, `dagster/column_schema`, `dagster/column_lineage`

## Caveats

- Each output asset re-runs the split internally on the upstream DataFrame. With deterministic strategies (`time`, `hash`, fixed `seed` for `random`), this is consistent. If you replace `seed` between materializations, train/test/val will diverge.
- For very large DataFrames where re-splitting is expensive, consider materializing the upstream into a partitioned format and using `strategy: hash` so each split asset only filters in-place.
