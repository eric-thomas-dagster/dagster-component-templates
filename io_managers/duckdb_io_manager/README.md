# DuckDB IO Manager

Register a `DuckDBPandasIOManager` so assets are automatically stored in and loaded from a local DuckDB file — wraps the official `dagster-duckdb-pandas` integration.

## Installation

```
pip install dagster-duckdb-pandas
```

## Configuration

```yaml
type: dagster_component_templates.DuckDBIOManagerComponent
attributes:
  resource_key: io_manager
  database: data/warehouse.duckdb
  schema_name: main
```

For MotherDuck (serverless DuckDB cloud), use the `motherduck_io_manager` component instead.

## Quack client mode (concurrent writers)

A path-based DuckDB file doesn't support multiple writer processes safely. The [Quack remote protocol](https://duckdb.org/2026/05/12/quack-remote-protocol) (DuckDB v1.5.2+, May 2026) solves this with an HTTP-based client/server architecture — one DuckDB instance runs `quack_serve(...)`, everyone else connects as a client. Set `quack_remote:` on this IO manager to route all asset reads/writes through that server:

```yaml
type: dagster_component_templates.DuckDBIOManagerComponent
attributes:
  resource_key: io_manager
  database: ":memory:"                    # local proxy; actual data lives on the remote
  schema_name: warehouse
  quack_remote: "quack:duckdb.internal"   # or "quack:host:port"
  quack_token: "{{ env('QUACK_TOKEN') }}"
  quack_attach_as: remote                  # default
```

When `quack_remote:` is set, each connection the IO manager opens:

1. `INSTALL quack FROM core_nightly; LOAD quack;`
2. `CREATE OR REPLACE SECRET quack_default (TYPE quack, TOKEN ?);`
3. `ATTACH 'quack:...' AS remote;`
4. `CREATE SCHEMA IF NOT EXISTS remote.<schema_name>;`

Asset tables land at `remote.<schema_name>.<asset_name>` on the server. Reads pull the same path.

**Caveats:**
- Currently handles **pandas DataFrames only** in Quack mode. Polars / PySpark equivalents would require similar additions to `duckdb_polars_io_manager` / `duckdb_pyspark_io_manager` — pandas first.
- Quack is new — the extension lives in `core_nightly`. Switch `quack_extension_repo: core` once it stabilizes.
- The Quack server is a single point of failure for writes. Set it up separately (sidecar container, dedicated host running `CALL quack_serve('quack:0.0.0.0', token=...);`). Do not expose the bare Quack port to the public internet — no SSL by default; put it behind a reverse proxy.
