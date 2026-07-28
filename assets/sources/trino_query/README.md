# Trino Query

Run a Trino SQL query and materialize the result as a pandas DataFrame asset. Trino is a distributed SQL query engine — use this component to federate reads across Postgres, MySQL, Iceberg, Hive, Delta, S3, etc. from one asset.

Auth via basic auth (`user` + `password_env_var`) or unauthenticated (no `password_env_var`).

See `schema.json` for full attribute reference.
