# topic_modeling

Discover latent topics in a text column using Latent Dirichlet Allocation (LDA) via scikit-learn. Can assign a dominant topic ID per row, add per-topic probability scores for all topics, or return a topic-word summary table. Top words per topic are always logged to Dagster asset metadata.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Output Dagster asset name |
| `upstream_asset_key` | `str` | required | Upstream asset key providing a DataFrame |
| `text_column` | `str` | required | Column name containing the text to model |
| `n_topics` | `int` | `10` | Number of topics to discover |
| `n_top_words` | `int` | `10` | Words per topic to include in metadata |
| `max_features` | `int` | `1000` | Vocabulary size for CountVectorizer |
| `max_iter` | `int` | `10` | Maximum LDA iterations |
| `random_state` | `int` | `42` | Random seed for reproducibility |
| `output_mode` | `str` | `"dominant_topic"` | `"dominant_topic"`, `"all_topics"`, or `"topic_table"` |
| `language` | `str` | `"english"` | Stop word language for CountVectorizer |
| `group_name` | `Optional[str]` | `null` | Dagster asset group name |

## Output Modes

- **`dominant_topic`**: Adds a `dominant_topic` integer column to the original DataFrame.
- **`all_topics`**: Adds a `topic_N_score` column per topic to the original DataFrame.
- **`topic_table`**: Returns a new DataFrame with `topic` and `top_words` columns — original rows are not included.

## Example YAML

```yaml
component_type: topic_modeling
description: Discover 8 latent topics in support tickets.

asset_name: support_ticket_topics
upstream_asset_key: support_tickets_clean_text
text_column: cleaned_text
n_topics: 8
n_top_words: 15
output_mode: dominant_topic
group_name: nlp_pipeline
```

## Metadata Logged

- `n_topics` — Number of topics modeled
- `vocabulary_size` — Vocabulary size after CountVectorizer
- `documents` — Number of documents processed
- `topics` — JSON mapping of topic ID to top words

## IO Manager Note

This component uses Dagster's IO manager to pass DataFrames between assets. No IO manager configuration is required for local development — Dagster's default FilesystemIOManager handles serialization automatically.

## Requirements

- `dagster`
- `pandas`
- `scikit-learn`
- `nltk`
