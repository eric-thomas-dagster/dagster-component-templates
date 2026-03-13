# TopicModelerComponent

Assign topic labels to rows using LDA (Latent Dirichlet Allocation) or NMF (Non-negative Matrix Factorization) topic modeling. Each row gets a topic ID and a human-readable label of top keywords.

## Fields

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `asset_name` | string | yes | — | Output Dagster asset name |
| `upstream_asset_key` | string | yes | — | Upstream asset key providing a DataFrame |
| `group_name` | string | no | null | Dagster asset group name |
| `text_column` | string | yes | — | Column containing text |
| `output_column` | string | no | `"topic_id"` | Column to write topic ID |
| `topic_label_column` | string | no | `"topic_keywords"` | Column to write top-keyword label |
| `n_topics` | integer | no | `10` | Number of topics to discover |
| `method` | enum | no | `"lda"` | `lda` or `nmf` |
| `max_features` | integer | no | `1000` | Vocabulary size |
| `top_words_per_topic` | integer | no | `5` | Keywords in topic label |

## Example

```yaml
component_type: dagster_component_templates.TopicModelerComponent
asset_name: topic_modeled_documents
upstream_asset_key: document_corpus
text_column: body
n_topics: 10
method: lda
```
