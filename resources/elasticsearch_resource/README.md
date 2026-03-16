# Elasticsearch Resource

Register an Elasticsearch resource for full-text search and analytics

A Dagster resource component that provides an Elasticsearch `ConfigurableResource` backed by the official `elasticsearch` Python client.

## Installation

```
pip install "elasticsearch>=8.0.0"
```

## Configuration

```yaml
type: elasticsearch_resource.component.ElasticsearchResourceComponent
attributes:
  resource_key: elasticsearch_resource
  hosts: "https://my-cluster.es.io:9243"
  api_key_env_var: ELASTICSEARCH_API_KEY
  verify_certs: true
```

## Auth

Provide either `api_key_env_var` (preferred for Elastic Cloud) or `username` + `password_env_var` for basic auth. Set the referenced environment variables to their respective secret values before running.
