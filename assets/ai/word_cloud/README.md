# Word Cloud Component

Compute word frequency tables, top-N word lists, or TF-IDF weighted word rankings from a text column. Returns a DataFrame of `word` and `frequency` (and optionally `tfidf_score`).

## Overview

The Word Cloud Component aggregates word counts across all rows in a text column, removing stop words and applying filters. The output DataFrame is ready for word cloud visualization libraries or further analysis.

## Features

- **Three Output Modes**: `frequency_table`, `top_n`, `tfidf`
- **Stop Word Removal**: NLTK-based with custom additions
- **Configurable Filters**: Minimum frequency and word length
- **TF-IDF Scoring**: Weight words by corpus importance
- **Multi-language**: Any NLTK-supported stop word language

## Use Cases

1. **Word Cloud Visualization**: Feed to wordcloud or similar libraries
2. **Keyword Extraction**: Identify dominant topics
3. **Content Comparison**: Compare vocabularies across datasets
4. **SEO Analysis**: Find most frequent terms

## Prerequisites

- `pandas>=1.5.0`, `nltk>=3.7`
- For `tfidf` mode: `scikit-learn>=1.0.0`

## Configuration

### Frequency Table (default)

```yaml
type: dagster_component_templates.WordCloudComponent
attributes:
  asset_name: word_frequencies
  upstream_asset_key: raw_text
  text_column: content
  output_mode: frequency_table
  min_frequency: 2
  min_word_length: 3
```

### Top N Words

```yaml
type: dagster_component_templates.WordCloudComponent
attributes:
  asset_name: top_100_words
  upstream_asset_key: raw_text
  text_column: content
  output_mode: top_n
  top_n: 100
```

### TF-IDF Weighted

```yaml
type: dagster_component_templates.WordCloudComponent
attributes:
  asset_name: tfidf_words
  upstream_asset_key: raw_text
  text_column: content
  output_mode: tfidf
```

## Configuration Options

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `asset_name` | string | required | Output asset name |
| `upstream_asset_key` | string | required | Upstream DataFrame asset key |
| `text_column` | string | required | Column with text |
| `output_mode` | string | `frequency_table` | Output mode |
| `top_n` | int | `100` | Top words for top_n mode |
| `min_frequency` | int | `1` | Minimum word count |
| `stop_words` | list | `None` | Additional stop words |
| `language` | string | `english` | NLTK stop word language |
| `min_word_length` | int | `2` | Minimum word length |
| `group_name` | string | `None` | Asset group |

## Output Schema

| Column | Description |
|--------|-------------|
| `word` | Lowercase word |
| `frequency` | Occurrence count |
| `tfidf_score` | TF-IDF score (tfidf mode only) |

## Troubleshooting

- **NLTK download fails**: Set `NLTK_DATA` env var or use `stop_words` manually
- **ImportError (sklearn)**: Install with `pip install scikit-learn`
- **Empty output**: Lower `min_frequency` or `min_word_length`
