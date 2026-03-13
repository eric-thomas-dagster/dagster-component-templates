# TextSimilarityComponent

Compute semantic or lexical similarity between two text columns, or between each row and a fixed query string. Returns a float score between 0 and 1.

## Fields

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `asset_name` | string | yes | — | Output Dagster asset name |
| `upstream_asset_key` | string | yes | — | Upstream asset key providing a DataFrame |
| `group_name` | string | no | null | Dagster asset group name |
| `text_column_a` | string | yes | — | First text column |
| `text_column_b` | string | no | null | Second text column (or use `query`) |
| `query` | string | no | null | Fixed query to compare all rows against |
| `output_column` | string | no | `"similarity_score"` | Output column name |
| `method` | enum | no | `"cosine_tfidf"` | `cosine_tfidf`, `jaccard`, `sequence_matcher`, `semantic` |
| `model_name` | string | no | `"all-MiniLM-L6-v2"` | Sentence-transformers model (semantic only) |

## Methods

- `cosine_tfidf`: TF-IDF vectorization + cosine similarity (scikit-learn)
- `jaccard`: Word-level Jaccard index
- `sequence_matcher`: Python difflib SequenceMatcher ratio
- `semantic`: Sentence embeddings + cosine similarity (sentence-transformers)

## Example

```yaml
component_type: dagster_component_templates.TextSimilarityComponent
asset_name: text_similarity_scores
upstream_asset_key: document_pairs
text_column_a: question
text_column_b: answer
method: cosine_tfidf
```
