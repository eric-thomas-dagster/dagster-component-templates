# DuckDB Resource

Register a DuckDB resource for fast in-process analytical queries

A Dagster resource component that provides a DuckDB `ConfigurableResource` backed by `duckdb`.

## Installation

```
pip install duckdb
```

## Configuration

```yaml
type: duckdb_resource.component.DuckDBResourceComponent
attributes:
  resource_key: duckdb_resource
  database: ":memory:"
```

## Auth

DuckDB has no external credentials. Set `database` to a file path for persistent storage, or leave it as `":memory:"` for an ephemeral in-process database. The resource exposes `get_connection()` which returns a `duckdb.DuckDBPyConnection`.

## Quack client mode (concurrent writers)

In-process DuckDB doesn't support multiple writer processes safely. The [Quack remote protocol](https://duckdb.org/2026/05/12/quack-remote-protocol) (DuckDB v1.5.2+, May 2026) adds an HTTP-based client/server protocol that solves this. One DuckDB instance runs `quack_serve(...)`; everyone else connects as a client.

Set `quack_remote:` on this resource to enable client mode:

```yaml
type: duckdb_resource.component.DuckDBResourceComponent
attributes:
  resource_key: duckdb_resource
  database: ":memory:"                    # local proxy; actual data lives on the remote
  quack_remote: "quack:localhost"          # or "quack:host:port"; default port 9494
  quack_token: "{{ env('QUACK_TOKEN') }}"  # matches the server's quack_serve(token=...)
  quack_attach_as: remote                  # default
  quack_use_remote_by_default: true        # USE remote; — so unqualified table refs go to the remote
```

On each `get_connection()`, the resource:

1. `INSTALL quack FROM core_nightly; LOAD quack;`
2. `CREATE OR REPLACE SECRET quack_default (TYPE quack, TOKEN ?)` — bound to your token
3. `ATTACH 'quack:remote' AS remote;`
4. Optionally `USE remote;` if `quack_use_remote_by_default` is set

Reference remote tables as `remote.<table>` in your SQL, or set `quack_use_remote_by_default: true` to make unqualified references resolve to the remote.

**Caveats:**
- Quack is new — currently lives in the `core_nightly` extension repo. Switch `quack_extension_repo: core` once the extension stabilizes.
- The server is a single point of failure for writes.
- Adds a network hop per query. Same-host (`quack:localhost`) is essentially free; cross-AZ adds latency.
- Set up the server separately — typically a sidecar / dedicated host running `CALL quack_serve('quack:0.0.0.0', token=...);`. Do NOT expose the bare Quack port to the public internet (no SSL by default); put it behind nginx/Cloudflare/your load balancer.
