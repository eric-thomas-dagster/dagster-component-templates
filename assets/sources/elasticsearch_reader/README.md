# Elasticsearch Reader Component

Search and query an Elasticsearch index, returning matching documents as a Dagster asset DataFrame.

## Overview

The `ElasticsearchReaderComponent` connects to an Elasticsearch cluster and executes search queries, returning results as a pandas DataFrame. Supports both simple text search and full Elasticsearch query DSL.

## Use Cases

- **Full-text search**: Index and search document content
- **Log analytics**: Query application or system logs
- **E-commerce search**: Search product catalogs
- **Data exploration**: Ad-hoc queries against indexed data

## Configuration

### Simple text search

```yaml
type: dagster_component_templates.ElasticsearchReaderComponent
attributes:
  asset_name: product_search
  index_name: products
  search_text: "wireless headphones"
  search_fields:
    - title
    - description
  n_results: 50
```

### Query DSL

```yaml
type: dagster_component_templates.ElasticsearchReaderComponent
attributes:
  asset_name: recent_errors
  index_name: application_logs
  query:
    query:
      bool:
        must:
          - match:
              level: ERROR
        filter:
          - range:
              timestamp:
                gte: "now-24h"
  n_results: 1000
```

### Authenticated cluster

```yaml
type: dagster_component_templates.ElasticsearchReaderComponent
attributes:
  asset_name: secured_search
  index_name: documents
  host_env_var: ELASTICSEARCH_URL
  api_key_env_var: ELASTICSEARCH_API_KEY
  search_text: "quarterly report"
```

## Output Schema

| Column | Description |
|--------|-------------|
| `_id` | Elasticsearch document ID |
| `_score` | Relevance score |
| `[source fields]` | Document source fields |

## Requirements

- `ELASTICSEARCH_URL` environment variable (e.g., `http://localhost:9200`)

## Dependencies

- `pandas>=1.5.0`
- `elasticsearch>=8.0.0`
