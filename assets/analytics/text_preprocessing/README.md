# text_preprocessing

Clean and normalize text data for NLP tasks using NLTK. Supports lowercasing, punctuation and number removal, stopword filtering, Porter stemming, WordNet lemmatization, and minimum token length filtering. Can output cleaned text as a joined string or as a list of tokens.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Output Dagster asset name |
| `upstream_asset_key` | `str` | required | Upstream asset key providing a DataFrame |
| `text_column` | `str` | required | Column name containing raw text |
| `output_column` | `Optional[str]` | `null` | Output column name (defaults to overwriting `text_column`) |
| `lowercase` | `bool` | `true` | Convert text to lowercase |
| `remove_punctuation` | `bool` | `true` | Remove punctuation characters |
| `remove_numbers` | `bool` | `false` | Remove numeric characters |
| `remove_stopwords` | `bool` | `true` | Remove NLTK stopwords |
| `language` | `str` | `"english"` | Stopword language |
| `stem` | `bool` | `false` | Apply Porter stemming |
| `lemmatize` | `bool` | `false` | Apply WordNet lemmatization |
| `min_token_length` | `int` | `2` | Minimum token length to keep |
| `output_tokens` | `bool` | `false` | Output as token list instead of joined string |
| `group_name` | `Optional[str]` | `null` | Dagster asset group name |

## Example YAML

```yaml
component_type: text_preprocessing
description: Clean support ticket text for NLP modeling.

asset_name: support_tickets_clean_text
upstream_asset_key: support_tickets_raw
text_column: ticket_body
output_column: cleaned_text
lowercase: true
remove_punctuation: true
remove_stopwords: true
lemmatize: true
min_token_length: 3
group_name: nlp_pipeline
```

## Metadata Logged

- `rows_processed` — Number of non-null rows processed
- `avg_tokens_per_row` — Average token count after cleaning
- `output_column` — Name of the output column
- `stopwords_removed`, `stemmed`, `lemmatized` — Processing flags applied

## IO Manager Note

This component uses Dagster's IO manager to pass DataFrames between assets. No IO manager configuration is required for local development — Dagster's default FilesystemIOManager handles serialization automatically.

## Requirements

- `dagster`
- `pandas`
- `nltk`
