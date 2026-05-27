# Synthetic Data Generator

Generate realistic synthetic/mock data for demos, testing, and prototyping without needing external data sources.

## Purpose

This component creates realistic fake data based on pre-defined schemas. Perfect for:
- **Demonstrations**: Show off your pipeline without setting up real data sources
- **Testing**: Test data transformations and quality checks with known data
- **Prototyping**: Build pipelines before connecting to production data
- **Training**: Create safe, realistic datasets for learning and experimentation

## Features

- **24 Pre-defined Schemas** across business + industry-data + media domains:
  - **Business / e-commerce**: Customers, Orders, Products, Transactions, Events, Sensors, Users, Subscriptions, Sparse Sensors, Customer Churn Metrics, Stripe Charges, Stripe Subscriptions, A/B Experiment, Support Tickets, Product Reviews, Employees
  - **Healthcare / clinical**: FHIR Patients (R4/R5), HL7 v2 Messages
  - **Fintech / trading**: ISO 20022 Payments (pacs.008/002), FIX 4.4 Messages, X12 EDI (270/271/835/837/850)
  - **Insurance**: ACORD XML (PolicyAddRq / ChangeRq / ClaimsNotificationRq / QuoteInqRq)
  - **Media**: Audio Samples (sine-tone WAVs), Image Prompts
- **Configurable Size**: Generate 1 to 100,000 rows
- **Reproducible**: Optional random seed for consistent data generation
- **No External Dependencies**: Pure Python, no external APIs or services needed
- **DataFrame Output**: Returns pandas DataFrame ready for downstream processing

## Available Schemas

### Customers
Realistic customer profiles with:
- Customer ID, name, email, phone
- Address (city, state)
- Signup date
- Lifetime value
- Active status

### Orders
E-commerce orders, shaped to match the canonical Snowflake `RAW.ORDERS`
table (so `dataframe_to_snowflake` / `write_pandas` can `if_exists: append`
without schema mismatches):

| Column | Type | Detail |
|---|---|---|
| `order_id` | `str` | `'ORD'` + 10-digit zero-padded sequence |
| `customer_id` | `str` | `'CUST'` + 6-digit zero-padded (random 1..1000) |
| `order_date` | `datetime` | Python `datetime` object (not a string) — maps to Snowflake `TIMESTAMP_NTZ` via `write_pandas`. With `partition_target_date` set, falls within that day; otherwise sampled uniformly over the last 30 days |
| `category` | `str` | lowercase: `electronics`, `clothing`, `books`, `home`, `sports`, `toys`, `beauty`, `food` |
| `num_items` | `int` | 1..10 |
| `subtotal` | `float` | uniform 10.0..1000.0 |
| `shipping` | `float` | uniform 0.0..30.0 |
| `tax` | `float` | `round(subtotal * 0.08, 2)` |
| `total` | `float` | `round(subtotal + shipping + tax, 2)` |
| `status` | `str` | `pending`, `paid`, `shipped`, `delivered`, `cancelled` |
| `region` | `str` | `NA`, `EU`, `APAC`, `LATAM` |

Matching Snowflake DDL:

```sql
CREATE TABLE RAW.ORDERS (
    ORDER_ID     VARCHAR,        -- 'ORD' + 10-digit
    CUSTOMER_ID  VARCHAR,        -- 'CUST' + 6-digit
    ORDER_DATE   TIMESTAMP_NTZ,
    CATEGORY     VARCHAR,
    NUM_ITEMS    NUMBER,
    SUBTOTAL     NUMBER(18,2),
    SHIPPING     NUMBER(10,2),
    TAX          NUMBER(10,2),
    TOTAL        NUMBER(18,2),
    STATUS       VARCHAR,
    REGION       VARCHAR
)
```

### Products
Product catalog with:
- Product ID, name, category
- Pricing (price, cost, margin)
- Stock quantity
- Ratings and reviews
- Availability status

### Transactions
Financial transaction records with:
- Transaction ID, account ID, timestamp
- Transaction type (deposit, withdrawal, transfer, payment, refund)
- Amount and merchant
- Category and status

### Events
Web/app event logs with:
- Event ID, user ID, session ID, timestamp
- Event type (page_view, click, form_submit, download, purchase, signup, login, logout)
- Page path
- Duration
- Device and browser info

### Sensors
IoT sensor readings with:
- Sensor ID, timestamp
- Sensor type (temperature, humidity, pressure, motion, light, sound)
- Location
- Value with appropriate units
- Status (normal, warning)

### Users
User account data with:
- User ID, username, email
- Role and subscription plan
- Account creation and last login dates
- Login count
- Storage usage
- Verification and active status

### Subscriptions
SaaS subscription rows shaped for **survival analysis** (Kaplan-Meier, Cox):
- subscription_id, plan_tier, days_active, cancelled (event indicator), signup_date
- Tier-dependent churn rates: free churns fast, enterprise barely churns
- Customize via `schema_options.tiers`:
  ```yaml
  schema_options:
    tiers:
      - {name: free, weight: 0.55, daily_churn_rate: 0.020, max_days: 180}
      - {name: pro, weight: 0.35, daily_churn_rate: 0.005, max_days: 365}
      - {name: enterprise, weight: 0.10, daily_churn_rate: 0.001, max_days: 730}
  ```

### Sparse Sensors
IoT sensor readings with **random gaps** for gap-fill and resample demos:
- reading_ts, sensor_id, temperature_c
- Multiple sensors over a configurable timespan with diurnal cycle + noise
- Configurable dropout rate to simulate flaky devices
- Customize via `schema_options`:
  ```yaml
  schema_options:
    sensor_count: 3
    duration_hours: 336      # 14 days * 24
    dropout_rate: 0.25       # ~25% of rows dropped
    base_temp: 22.0
    noise_amplitude: 2.0
    start_date: "2026-04-01"
  ```

### Customer Churn Metrics
Per-customer aggregated state for churn modeling / scoring:
- customer_id, last_activity, total_orders, total_revenue, lifetime_days
- Activity-recency distribution biased toward recent with a long tail
- Customize via `schema_options`:
  ```yaml
  schema_options:
    reference_date: "2026-05-01"   # last_activity computed relative to this
    activity_mix:
      - [0, 30, 5]                 # (low, high, weight)
      - [31, 90, 3]
      - [91, 365, 2]
  ```

### Stripe Charges
Stripe-shaped charge events for revenue / attribution demos. Matches the column shape of Stripe's `charges` API:
- id, _resource_type, customer_id, amount (cents), created (epoch), status
- `schema_options`: `plans` (list of dollar amounts), `lookback_days`

### Stripe Subscriptions
Stripe-shaped subscription rows for SaaS metrics / MRR demos:
- id, _resource_type, customer_id, status (active/trialing/canceled), created, canceled_at, current_period_end, plan_amount (cents), plan_interval, plan_nickname
- Customize via `schema_options`:
  ```yaml
  schema_options:
    plans: [[10, "starter"], [29, "basic"], [49, "pro"], [99, "business"], [199, "enterprise"]]
    plan_weights: [3, 4, 3, 2, 1]
    status_mix: {active: 0.65, trialing: 0.10, canceled: 0.25}
    lookback_days: 540
  ```

### A/B Experiment
Per-user exposure rows for stat-test demos (z-test, t-test, chi-squared):
- experiment_id, user_id, variant (`control`/`treatment`), converted (0/1), exposed_at
- Configurable lift so the treatment shows the level of effect you want
- Customize via `schema_options`:
  ```yaml
  schema_options:
    experiment_id: "exp_001"
    control_conversion: 0.10        # 10% baseline
    lift: 0.3                        # treatment is 30% better (relative)
    treatment_share: 0.5
    lookback_days: 14
  ```
  Set `lift: 0.0` to simulate a null result.

### Support Tickets
Multilingual customer-support tickets — useful for NLP/codec/email demos:
- ticket_id, customer_id, channel, priority, created_at
- `ticket_text` includes embedded German (Müller), Spanish (é/í), and other non-ASCII for codec_convert / language_detector / pii_detector demos
- `expected_language` tag (en/de/fr/es/ja)

### Product Reviews
Customer review text with sentiment labels — for sentiment_analyzer / text_classifier:
- review_id, product_id, customer_id, rating (1-5)
- `review_text` with vocabulary aligned to rating (positive/negative/neutral)
- `expected_sentiment` ground-truth tag

### Employees
HRIS-style employee records — for hris_normalizer demos:
- employee_number, work_email, first_name, last_name
- department, location, employment_status, employment_type, hire_dt
- Vendor-style messy values (e.g. `Full-Time` / `FT` / `FULL_TIME`) to exercise `value_maps`

### Audio Samples
Sine-tone WAVs written to disk — for audio_transform / speech_to_text demos:
- `output_dir` written with files; returns DataFrame of (path, sample_rate, channels, duration_seconds)
- Configurable sample rate / channels / duration / frequencies

### Image Prompts
Text prompts for image-gen demos (nano_banana / gemini_image_generation):
- prompt_id, prompt_text, style, aspect_ratio
- Mix of nouns, adjectives, scene types

### FHIR Patients (R4/R5)
Full FHIR resource mix — for fhir_resource_normalizer demos:
- One DataFrame row per resource, `resource` column is the parsed JSON dict
- Each "patient" cycle emits: Patient + 2 Observations + Practitioner + Organization + Coverage + Claim
- Covers 10 resource types end-to-end (Patient, Observation, Encounter, Condition, MedicationRequest, Claim, Coverage, Practitioner, Organization, Bundle)

### HL7 v2 Messages
Pipe-delimited HL7 v2.x messages — for hl7_v2_parser demos:
- Rotates ADT^A01 (admit), ORU^R01 (lab result), ORM^O01 (order)
- Each message includes the full segment chain (MSH + EVN + PID + PV1 + DG1 + AL1 / ORC + OBR + OBX)
- Standard pipe / SOH encoding; auto-decodes RFC-2047

### ISO 20022 Payments
ISO 20022 pacs.008 (credit transfer) + pacs.002 (status report) XML — for iso20022_payment_parser:
- One row per message; alternates between the two transaction shapes
- Realistic IBANs, BICs, currencies (USD/EUR/GBP)
- Status codes follow standard (ACCP / ACSC / RJCT / PDNG)

### X12 EDI Messages
ASC X12 EDI envelopes — for x12_edi_parser demos:
- Rotates 5 transaction sets: 270 (eligibility inquiry), 271 (response), 835 (remittance), 837 (claim), 850 (purchase order)
- Each wrapped in full ISA/GS/ST envelope
- Configurable delimiter (`~` segment / `*` element default)

### FIX Messages (4.4)
Trading-protocol messages — for fix_message_parser demos:
- Mix of NewOrderSingle (D) + ExecutionReport (8)
- Proper FIX 4.4 wire format with `8=FIX.4.4`, `9=<body_length>`, `10=<checksum>`
- Pipe-rendered by default for legible pandas output; canonical SOH (`\x01`) also supported

### ACORD Messages (Insurance)
ACORD insurance XML envelopes — for acord_xml_parser demos:
- Rotates 4 envelopes: InsurancePolicyAddRq, InsurancePolicyChangeRq, ClaimsNotificationRq, InsurancePolicyQuoteInqRq
- Realistic insureds (P&C carrier + Life), policies, claims, quotes
- Full ACORD namespace + SignonRq boilerplate

## Configuration

### Required Fields

- **asset_name** (string): Name of the asset to create
- **schema_type** (enum): Type of data to generate
  - Options:
    - Business: `customers`, `orders`, `products`, `transactions`, `events`, `sensors`, `users`, `subscriptions`, `sparse_sensors`, `customer_churn_metrics`, `stripe_charges`, `stripe_subscriptions`, `ab_experiment`, `support_tickets`, `product_reviews`, `employees`
    - Healthcare: `fhir_patients`, `hl7_messages`
    - Fintech: `iso20022_payments`, `fix_messages`, `x12_messages`
    - Insurance: `acord_messages`
    - Media: `audio_samples`, `image_prompts`
- **row_count** (integer): Number of rows to generate (1-100,000)

### Optional Fields

- **random_state** (integer, optional): Random seed for reproducible data. If not specified, data will be different each time.
- **schema_options** (object, optional): Per-schema knobs. Only used by `subscriptions` (tiers list) and `sparse_sensors` (sensor_count, duration_hours, dropout_rate, base_temp, noise_amplitude, start_date).
- **description** (string): Asset description
- **group_name** (string): Asset group for organization

<!-- FIELDS:START - auto-generated by tools/regen_readme_fields.py -->

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `asset_name` | `str` | Name of the asset |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `description` | `str` | — | Description of the asset |
| `group_name` | `str` | — | Asset group name for organization |
| `owners` | `List[str]` | — | Asset owners — list of team names or email addresses, e.g. ['team:analytics', 'user@company.com'] |
| `asset_tags` | `Dict[str, str]` | — | Additional key-value tags to apply to the asset, e.g. {'domain': 'finance', 'tier': 'gold'} |
| `kinds` | `List[str]` | — | Asset kinds for the Dagster catalog, e.g. ['snowflake', 'python']. Auto-inferred from component name if not set. |
| `column_lineage` | `Dict[str, List[str]]` | — | Column-level lineage mapping: output column name → list of upstream column names it was derived from, e.g. {'revenue': ['price', 'quantity']} |
| `deps` | `list[str]` | — | Upstream asset keys this asset depends on (e.g. ['raw_orders', 'schema/asset']) |

### Freshness

| Field | Type | Default | Description |
|---|---|---|---|
| `freshness_max_lag_minutes` | `int` | — | Maximum acceptable lag in minutes before the asset is considered stale. Defines a FreshnessPolicy. |
| `freshness_cron` | `str` | — | Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5' (weekdays at 9am). |

### Partitions

| Field | Type | Default | Description |
|---|---|---|---|
| `partition_type` | `str` | — | Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', or None for unpartitioned |
| `partition_start` | `str` | — | Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types. |
| `partition_date_column` | `str` | — | Column used to filter upstream DataFrame to the current date partition key. |
| `partition_dimensions` | `List[Dict[str, Any]]` | — | Multi-axis partition spec: list of {name, type, start, values, dynamic_partition_name} dicts. Overrides flat fields when set. |
| `partition_values` | `str` | — | Comma-separated values for static or multi partitioning, e.g. 'customer_a,customer_b,customer_c'. |
| `partition_static_dim` | `str` | — | Dimension name for the static axis in multi-partitioning, e.g. 'customer' or 'region'. |
| `partition_static_column` | `str` | — | Column used to filter upstream DataFrame to the current static partition dimension (e.g. 'customer_id'). |

### Retry policy

| Field | Type | Default | Description |
|---|---|---|---|
| `retry_policy_max_retries` | `int` | — | Max retries on asset failure. Defines a RetryPolicy. Useful for transient network failures, rate limits, etc. |
| `retry_policy_delay_seconds` | `int` | — | Seconds between retries (default 1). |
| `retry_policy_backoff` | `str` | `"exponential"` | Backoff strategy: 'linear' or 'exponential'. |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `schema_type` | `Literal['customers', 'orders', 'products', 'transactions', 'ab_experiment', 'events', 'sensors', 'users', 'subscriptions', 'sparse_sensors', 'customer_churn_metrics', 'stripe_charges', 'stripe_subscriptions', 'support_tickets', 'product_reviews', 'audio_samples', 'image_prompts', 'employees', 'fhir_patients', 'hl7_messages', 'iso20022_payments', 'x12_messages', 'fix_messages', 'acord_messages', 'moderation_content']` | `"customers"` | Type of data schema to generate |
| `row_count` | `int` | `100` | Number of rows to generate |
| `random_state` | `int` | — | Random seed for reproducible data generation (leave empty for random) |
| `schema_options` | `Dict[str, Any]` | — | Per-schema knobs. Keys recognized by specific schemas: subscriptions: tiers: list of {name, weight, daily_churn_rate, max_days} defaults: free (55%, 2%/day, 180d), pro (35%, 0.5%/day, 365d), enterprise (10%, 0.1%/day, 730d) sparse_sensors: sensor_count: int (default 3) duration_hours: int (default 336 = 14 days) dropout_rate: float in [0,1] (default 0.25) base_temp: float (default 22.0) noise_amplitude: float (default 2.0) start_date: 'YYYY-MM-DD' (default '2026-04-01') |
| `dynamic_partition_name` | `str` | — | Name for DynamicPartitionsDefinition (when partition_type='dynamic'), e.g. 'tenants'. |
| `include_preview_metadata` | `bool` | `false` | Include a preview of the output data in metadata (first 5 rows as markdown table). Used by builder UIs to render asset shape without warehouse access. |
| `preview_rows` | `int` | `25` | Rows to include in the preview metadata when `include_preview_metadata` is True. For long DataFrames (>10x preview_rows), a random sample is used so the preview reflects the data distribution; otherwise head() is used. |

<!-- FIELDS:END -->

## Example Usage

### Basic Customer Data
```yaml
type: synthetic_data_generator.SyntheticDataGeneratorComponent

attributes:
  asset_name: demo_customers
  schema_type: customers
  row_count: 1000
```

### Reproducible Orders Data
```yaml
type: synthetic_data_generator.SyntheticDataGeneratorComponent

attributes:
  asset_name: test_orders
  schema_type: orders
  row_count: 500
  random_state: 42
  description: "Test orders dataset for pipeline validation"
  group_name: test_data
```

### IoT Sensor Readings
```yaml
type: synthetic_data_generator.SyntheticDataGeneratorComponent

attributes:
  asset_name: sensor_readings
  schema_type: sensors
  row_count: 10000
  description: "Simulated IoT sensor data over 24 hours"
```

## Output

Returns a pandas DataFrame with columns appropriate for the selected schema type. The DataFrame can be consumed by:
- **DataFrame Transformer**: Apply transformations
- **DuckDB Writer**: Persist to local database
- **File Writer**: Save to CSV/Parquet
- **Data Quality Checks**: Validate with assertion checks

## Demo Pipeline Example

Build a complete demo pipeline:

```
Synthetic Data Generator (customers)
  → DuckDB Writer (raw_customers table)
    → DuckDB Query Reader (SELECT * FROM raw_customers WHERE is_active = true)
      → DataFrame Transformer (add customer_tier column)
        → Enhanced Data Quality Checks
```

## Tips

1. **Start Small**: Use 100-1000 rows during development, scale up for demos
2. **Use Seeds**: Set `random_state` for reproducible demos and tests
3. **Mix Schemas**: Generate multiple schemas to create related datasets
4. **Chain Components**: Combine with transformers, writers, and checks for complete workflows

## Technical Notes

- All dates are generated relative to the current time
- Data distributions are designed to be realistic (e.g., 75% active customers)
- No external APIs or internet connection required
- Lightweight - only depends on pandas

## Asset Dependencies & Lineage

This component supports a `deps` field for declaring upstream Dagster asset dependencies:

```yaml
attributes:
  # ... other fields ...
  deps:
    - raw_orders              # simple asset key
    - raw/schema/orders       # asset key with path prefix
```

`deps` draws lineage edges in the Dagster asset graph without loading data at runtime. Use it to express that this asset depends on upstream tables or assets produced by other components.

Dependencies can also be wired externally via `map_resolved_asset_specs()` in `definitions.py` — the same approach used by [Dagster Designer](https://github.com/eric-thomas-dagster/dagster_designer).
