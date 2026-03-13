# KeywordExtractorComponent

Extract keywords and key phrases from a text column. Supports TF-IDF (corpus-level), YAKE (unsupervised statistical), and RAKE (rapid automatic keyword extraction).

## Fields

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `asset_name` | string | yes | — | Output Dagster asset name |
| `upstream_asset_key` | string | yes | — | Upstream asset key providing a DataFrame |
| `group_name` | string | no | null | Dagster asset group name |
| `text_column` | string | yes | — | Column containing text |
| `output_column` | string | no | `"keywords"` | Column to write keyword list |
| `method` | enum | no | `"tfidf"` | `tfidf`, `yake`, or `rake` |
| `top_n` | integer | no | `10` | Number of keywords per row |
| `ngram_range` | array [int, int] | no | `[1, 2]` | Min/max n-gram size |
| `language` | string | no | `"en"` | Text language |

## Example

```yaml
component_type: dagster_component_templates.KeywordExtractorComponent
asset_name: extracted_keywords
upstream_asset_key: article_text
text_column: body
method: tfidf
top_n: 10
ngram_range: [1, 2]
```
