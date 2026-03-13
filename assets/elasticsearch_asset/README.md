# ElasticsearchAssetComponent

Index data into [Elasticsearch](https://www.elastic.co/elasticsearch) or query an ES index and write results to a relational database table — all as observable Dagster assets.

## Modes

### `index` (default)

Reads rows from a relational database source table (via SQLAlchemy) and bulk-indexes them as documents into an Elasticsearch index using `elasticsearch.helpers.bulk()`.

```yaml
type: dagster_component_templates.ElasticsearchAssetComponent
attributes:
  asset_name: product_search_index
  elasticsearch_url_env_var: ELASTICSEARCH_URL
  index_name: products
  mode: index
  source_database_url_env_var: DATABASE_URL
  source_table_name: products
  id_field: product_id
  chunk_size: 500
  group_name: search
  deps:
    - raw/products
```

Materialization metadata: `index`, `num_docs`, `source_table`, `chunk_size`, `id_field`.

### `query`

Runs an Elasticsearch DSL query against an index and writes the matching document `_source` fields to a destination database table.

```yaml
type: dagster_component_templates.ElasticsearchAssetComponent
attributes:
  asset_name: electronics_search_results
  elasticsearch_url_env_var: ELASTICSEARCH_URL
  index_name: products
  mode: query
  query: '{"query": {"term": {"category": "electronics"}}}'
  database_url_env_var: DATABASE_URL
  table_name: electronics_hits
  if_exists: replace
  group_name: search
```

Materialization metadata: `index`, `num_docs`, `destination_table`, `if_exists`.

## Prerequisites

```bash
pip install "elasticsearch>=8.0" pandas sqlalchemy
```

For Elastic Cloud, set `api_key_env_var` in addition to `elasticsearch_url_env_var`.

## Fields Reference

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Dagster asset key |
| `elasticsearch_url_env_var` | `str` | required | Env var with ES URL |
| `api_key_env_var` | `str` | `None` | Env var with Elastic Cloud API key |
| `index_name` | `str` | required | Elasticsearch index name |
| `mode` | `str` | `"index"` | `"index"` or `"query"` |
| `source_database_url_env_var` | `str` | `None` | Env var with source DB URL (index mode) |
| `source_table_name` | `str` | `None` | Source table name (index mode) |
| `id_field` | `str` | `None` | Column to use as document `_id` (index mode) |
| `chunk_size` | `int` | `500` | Documents per bulk request (index mode) |
| `query` | `str` | `"{}"` | JSON ES DSL query body (query mode) |
| `database_url_env_var` | `str` | `None` | Env var with destination DB URL (query mode) |
| `table_name` | `str` | `None` | Destination table name (query mode) |
| `if_exists` | `str` | `"replace"` | Table conflict behaviour: `replace`, `append`, `fail` |
| `group_name` | `str` | `"elasticsearch"` | Dagster asset group |
| `upstream_asset_key` | `str` | `None` | Upstream asset key for lineage |
| `deps` | `list[str]` | `None` | Additional upstream asset keys |

## Asset Lineage

```yaml
deps:
  - raw/products          # wait for upstream ETL to complete
upstream_asset_key: raw/products   # equivalent single-key shorthand
```

Both `deps` and `upstream_asset_key` are merged into the asset's dependency list.
