# CouchDB Resource

Register a CouchDB resource for document-oriented database access via HTTP

A Dagster resource component that provides a CouchDB `ConfigurableResource` backed by the `couchdb` Python library.

## Installation

```
pip install couchdb
```

## Configuration

```yaml
type: couchdb_resource.component.CouchDBResourceComponent
attributes:
  resource_key: couchdb_resource
  url: "http://localhost:5984"
  username: admin
  password_env_var: COUCHDB_PASSWORD
```

## Auth

Set `password_env_var` to the name of an environment variable containing the CouchDB password. If `username` is omitted, the server is accessed without credentials.
