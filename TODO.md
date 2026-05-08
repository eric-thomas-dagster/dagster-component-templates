# TODO

Open work tracked across the registry. Closed items get deleted, not crossed
out — git log is the history.

## Examples to write

Per the project pattern (`dagster-community-components-cli/examples/setup_*.sh`
+ `examples/<demo>.md`), every component should have at least one walkthrough
that materializes it end-to-end. The following changed components currently
have no example:

- **`moderation_scorer`** — needs a walkthrough that scores a batch of
  user-generated text and demonstrates the threshold/flag output. Likely
  pairs naturally with `synthetic_data_generator` (support_tickets schema)
  or a slim "user comments" synthetic source.
- **`text_moderator`** — same shape as `moderation_scorer`; could share an
  example (`setup_content_moderation_demo.sh`?) that runs both side-by-side
  and contrasts their output.
- **`ollama_inference_asset`** — needs an example that doesn't require
  OpenAI credentials. Should document the local Ollama install step
  (e.g. `brew install ollama && ollama pull llama3.2:3b`) and run a
  small inference batch. Validation level `live` requires Ollama running
  on the target machine.

## iceberg_io_manager broken import

`setup_local_io_demo.sh` fails on project load because
`iceberg_io_manager/component.py` imports
`from dagster_iceberg.io_manager.pyarrow import IcebergPyarrowIOManager`,
but that module path doesn't exist in the installed `dagster-iceberg`
version. Either pin a compatible version or update the import path.

## Canonicalize text_column → input_column across AI components

Four AI components (`keyword_extractor`, `language_detector`,
`pii_detector`, `pii_redactor`) use `text_column`; the rest of the AI
family (embeddings_generator, entity_extractor, document_summarizer,
sentiment_analyzer, etc.) uses the canonical `input_column`. Sweep
these four to `input_column` for consistency across the YAML surface
users actually write.

## OpenLineage op-job export

The current `lineage_to_openlineage` (and the broader `lineage_to_*`
family) expose lineage as **assets** that materialize a graph snapshot.
That works for a Dagster-native view, but doesn't fit teams that
already have an OpenLineage collector and want events emitted on the
**job-run** boundary (each op start / complete → an OpenLineage event).

What to build: an op-shaped job component that, when added to a code
location, registers run-event hooks (or a sensor) that emit OpenLineage
events to a configured endpoint (Marquez, Datakin, etc.) for each op
execution. Independent of the asset-lineage sinks; not a replacement,
a complement.

## Partition shape rework

Tracked separately as Phase 1 of the partition-handling improvements.
See conversation context, not yet broken into sub-tasks here.

## Demo failures discovered during validation (analytics)

`setup_analytics_demo.sh` now scaffolds + validates clean (was broken on
all 8 multi-role components — fixed). At runtime, several still fail on
unrelated bugs:

- **`subscription_metrics_output`** — `KeyError: 'status'`. The component
  expects a `status` column (presumably `active` / `cancelled`) but the
  synthetic ecommerce_dataset doesn't have one. Either give synthetic
  data the right columns, or make the component tolerate missing
  optional columns.
- **`pip_output`** — `ValueError: One of geojson_path or geojson_url
  must be provided`. Demo's defs.yaml is missing a required field.
- **`gradient_boosting_model_output`, `nn_output`** — sklearn errors
  about classifier/regressor mismatch on synthetic data ("Unknown label
  type: continuous"). Data shape vs component config.
- **`stepwise_output`** — `n_features_to_select must be < n_features`.
  Synthetic data has too few features for the demo's config.
- **`model_compare`** — fails because upstream `gradient_boosting_*` and
  `nn_*` failed.

## Demo failures discovered during validation (transforms)

`setup_transformations_demo.sh` materializes 34/37 components clean. Three
fail with bugs unrelated to convention drift:

- **`orders_sql` (sql_transform)** — demo's defs.yaml references env var
  `DUCKDB_PATH_VAR`, but `setup_transformations_demo.sh` exports
  `SQL_DB_URL`. Either fix the setup script to also export the var the
  demo expects, or change the demo to point at `SQL_DB_URL`.
- **`orders_formula` (multi_field_formula)** — demo passes
  `expression: 'col * 1.1'` with `columns: [unit_price]`, expecting the
  component to substitute `col` with each column name in turn. Component
  doesn't support that placeholder — `NameError: name 'col' is not defined`.
  Either add `col` substitution to the component or rewrite the demo to
  use the column name literally.
- **`orders_row_formula` (multi_row_formula)** — fails with
  `KeyError: 'column'`. Component appears to require a `column` key in
  each operation, but the demo uses `output` + `expression`. Reconcile.
