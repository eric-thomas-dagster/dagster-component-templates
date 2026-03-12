# Dagster Component Templates

A community library of 190+ reusable [Dagster component](https://docs.dagster.io/guides/components) templates covering data ingestion, AI/LLM enrichment, observability, sensors, and enterprise tool integrations — all configurable via YAML with no Python required.

## What are Dagster Components?

Components are reusable, YAML-configurable building blocks that package common data engineering patterns. Drop one into your project's `defs/` folder and configure it in YAML — no boilerplate Python needed.

```yaml
# defs/components/sqs_ingest.yaml
type: dagster_component_templates.SQSToDatabaseAssetComponent
attributes:
  asset_name: raw_events
  queue_url_env_var: SQS_QUEUE_URL
  database_url_env_var: DATABASE_URL
  table_name: raw_events
  partition_type: daily
```

## Component Library

### Assets (115)

**Ingestion — cloud storage**
`s3_to_database_asset` · `gcs_to_database_asset` · `adls_to_database_asset`

**Ingestion — messaging & streaming**
`kafka_to_database_asset` · `sqs_to_database_asset` · `kinesis_to_database_asset` · `eventhubs_to_database_asset` · `servicebus_to_database_asset` · `rabbitmq_to_database_asset` · `pubsub_to_database_asset` · `redis_streams_to_database_asset` · `nats_to_database_asset` · `pulsar_to_database_asset` · `mqtt_to_database_asset`

**Ingestion — files & databases**
`sftp_to_database_asset` · `sql_to_database_asset` · `csv_file_ingestion` · `rest_api_fetcher`

**AI / LLM enrichment**
`litellm_inference_asset` · `ollama_inference_asset` · `langchain_chain_asset` · `llm_prompt_executor` · `llm_chain_executor` · `document_summarizer` · `entity_extractor` · `embeddings_generator` · `moderation_scorer` · `anthropic_llm` · `conversation_memory`

**dbt**
`dbt_docs_enriched_project` — extends `DbtProjectComponent` with exposures, metrics, semantic models, contracts, source freshness, and clickable dbt docs links on every asset

**Enterprise tool runs**
`coalesce_run_asset` · `abinitio_run_asset` · `matillion_run_asset` · `rivery_run_asset` · `precisely_run_asset`

**Analytics & ML**
`anomaly_detection` · `customer_segmentation` · `ltv_prediction` · `lead_scoring` · `propensity_scoring` · `customer_health_score` · `cohort_analysis` · `funnel_analysis` · `ab_test_analysis` · `campaign_performance` · `multi_touch_attribution` · `product_recommendations` · and more

---

### Sensors (28)

**Cloud storage**
`s3_monitor` · `gcs_monitor` · `adls_monitor`

**Messaging & streaming**
`kafka_monitor` · `sqs_monitor` · `kinesis_monitor` · `eventhubs_monitor` · `servicebus_monitor` · `rabbitmq_monitor` · `pubsub_monitor` · `redis_streams_monitor` · `nats_monitor` · `pulsar_monitor` · `mqtt_monitor`

**Files**
`sftp_monitor` · `sql_monitor`

**Enterprise tools**
`coalesce_job_sensor` · `abinitio_job_sensor` · `matillion_job_sensor` · `rivery_job_sensor` · `precisely_job_sensor`

**Notifications**
`slack_notification` · `pagerduty_alert` · `teams_notification` · `opsgenie_alert`

---

### Asset Checks (7)

`dq_check` · `great_expectations_check` · `soda_check` · `monte_carlo_check` · `sifflet_check` · `acceldata_check` · `freshness_check`

---

### Observations (20)

`clickhouse_table_observation_sensor` · `snowflake_table_observation` · `bigquery_table_observation` · `postgres_table_observation` · `redshift_table_observation` and more

---

### External Assets (21)

`external_clickhouse_table` · `external_snowflake_table` · `external_bigquery_table` · `external_postgres_table` · `external_s3_object` · `external_kafka_topic` and more

---

### Integrations (18)

`aws_glue` · `aws_dms` · `aws_kinesis` · `aws_redshift` · `aws_sagemaker` · `azure_data_factory` · `azure_stream_analytics` · `azure_synapse` · `databricks_asset_bundle` · `databricks_workspace` · `google_bigquery` · `google_cloud_functions` · `google_cloud_run_jobs` · `google_dataflow` · `google_datastream` · `google_pubsub` · `google_vertex_ai` · `snowflake_workspace`

---

## Component Structure

Every component follows the same layout:

```
component_name/
├── component.py       # Dagster component class (Component + Model + Resolvable)
├── example.yaml       # Working YAML configuration example
├── README.md          # Documentation and field reference
├── requirements.txt   # pip dependencies
└── schema.json        # Component registry metadata
```

## Quick Start

### 1. Install dagster-components

```bash
pip install dagster
```

### 2. Copy a component into your project

```bash
cp -r sensors/s3_monitor/ my_project/lib/
```

### 3. Configure in YAML

```yaml
# defs/components/my_s3_sensor.yaml
type: dagster_component_templates.S3MonitorSensor
attributes:
  sensor_name: raw_data_monitor
  bucket_env_var: DATA_BUCKET
  prefix: incoming/
  target_asset: raw_data_ingest
```

### 4. Load in definitions.py

```python
import dagster as dg

defs = dg.load_from_defs_folder(project_root=Path(__file__).parent)
```

## Asset Dependencies and Lineage

Every asset component supports a `deps` field for declaring upstream dependencies:

```yaml
type: dagster_component_templates.LiteLLMInferenceAssetComponent
attributes:
  asset_name: enriched_tickets
  upstream_asset_key: raw_tickets   # loads data + draws lineage edge
  deps:                              # additional lineage-only edges
    - support_schema/tickets_raw
  model: claude-3-5-sonnet-20241022
  ...
```

Dependencies can also be wired externally via `map_resolved_asset_specs()` in `definitions.py` — the same approach used by [Dagster Designer](https://github.com/eric-thomas-dagster/dagster_designer).

## Partitioning

All ingestion assets support time-based partitioning out of the box:

```yaml
attributes:
  partition_type: daily          # none | daily | weekly | monthly
  partition_start_date: "2024-01-01"
  table_name: events_{partition_key}   # {partition_key} is substituted at runtime
```

## Sensor → Asset Pairing

Most sensors are designed to trigger a companion ingestion asset. The sensor detects new data and fires a `RunRequest` with source info in `run_config`; the asset reads and ingests it.

| Sensor | Companion Asset |
|--------|----------------|
| `s3_monitor` | `s3_to_database_asset` |
| `kafka_monitor` | `kafka_to_database_asset` |
| `sqs_monitor` | `sqs_to_database_asset` |
| `eventhubs_monitor` | `eventhubs_to_database_asset` |
| `sftp_monitor` | `sftp_to_database_asset` |
| … | … (all 15 pairs complete) |

## dbt Docs Enrichment

The `dbt_docs_enriched_project` component is a drop-in replacement for `DbtProjectComponent` that adds rich metadata from the dbt manifest to every asset in the Dagster UI:

```yaml
type: dagster_component_templates.DbtDocsEnrichedProjectComponent
attributes:
  project: "{{ project_root }}/dbt_project"
  dbt_docs_url: "https://dbt-docs.internal.mycompany.com"
  include_exposures: true
  include_metrics: true
  include_semantic_models: true
```

Each dbt model asset then shows: a clickable link to the dbt docs page, downstream BI exposures, metrics, semantic models, contract status, and source freshness SLAs — all visible in the Dagster Asset Catalog.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for how to add new components.

## License

MIT License
