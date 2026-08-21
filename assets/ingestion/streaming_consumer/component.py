"""StreamingConsumerComponent — always-running queue → sink asset.

An asset whose compute is a long-running consumer loop, not a sensor
firing per-batch. Reach for this when your source is a queue (Kafka,
RabbitMQ, SQS, Redis Streams, NATS) and you want a single continuous
consumer rather than a stream of many short runs.

Shape:

  queue → (poll batch) → polars transform → sink → repeat until
  max_seconds → emit final AssetMaterialization → exit cleanly

Pair with `StreamingRunHealthSensorComponent` for auto-restart: the
sensor polls run status every N seconds, and when no run for the job
is currently RUNNING / STARTING / QUEUED, it fires a new RunRequest.
Together they give an "always-on" asset that self-heals on crash /
Serverless timeout / code-location restart.

Serverless notes:
- Set `max_seconds` LESS than your Dagster+ Serverless per-run timeout.
- Use non-isolated runs + in_process_executor for lowest overhead.
- The health sensor + a small `max_seconds` (e.g. 3600) gives 24/7 uptime
  without hitting Serverless run-duration caps.

Supported queues (v1): kafka.
Coming: rabbitmq, sqs, redis_stream, nats.
"""
import time
import json as _json
from typing import Any, Dict, List, Optional

import dagster as dg
from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetMaterialization,
    Component,
    ComponentLoadContext,
    Definitions,
    MetadataValue,
    Model,
    Resolvable,
    asset,
)
from pydantic import Field


# ── Queue dispatchers ─────────────────────────────────────────────────


class _KafkaConsumer:
    """Thin wrapper over confluent-kafka's Consumer.

    Config keys (under `queue:`):
      kind: kafka
      topic: <topic name>
      bootstrap_servers OR bootstrap_servers_env_var
      group_id
      auto_offset_reset: earliest | latest   (default: latest)
      security_protocol / sasl_* : forwarded verbatim
      extra_config: {any librdkafka config}
    """

    def __init__(self, cfg: Dict[str, Any], log):
        try:
            from confluent_kafka import Consumer
        except ImportError:
            raise ImportError("streaming_consumer with kind=kafka needs: pip install confluent-kafka")
        import os as _os

        bootstrap = cfg.get("bootstrap_servers")
        if not bootstrap and cfg.get("bootstrap_servers_env_var"):
            bootstrap = _os.environ.get(cfg["bootstrap_servers_env_var"])
        if not bootstrap:
            raise ValueError("kafka queue requires `bootstrap_servers` or `bootstrap_servers_env_var`.")
        group_id = cfg.get("group_id")
        if not group_id:
            raise ValueError("kafka queue requires `group_id`.")

        conf: Dict[str, Any] = {
            "bootstrap.servers": bootstrap,
            "group.id": group_id,
            "auto.offset.reset": cfg.get("auto_offset_reset", "latest"),
            "enable.auto.commit": False,
        }
        for k in ("security_protocol", "sasl_mechanism", "sasl_username", "sasl_password",
                  "ssl_ca_location", "ssl_certificate_location", "ssl_key_location"):
            if cfg.get(k):
                conf[k.replace("_", ".")] = cfg[k]
        # Env-backed passwords / tokens.
        for k in ("sasl_password_env_var", "sasl_username_env_var"):
            if cfg.get(k):
                v = _os.environ.get(cfg[k])
                if v is None:
                    raise ValueError(f"kafka {k}={cfg[k]!r} but env var unset")
                conf[k.replace("_env_var", "").replace("_", ".")] = v
        # extra_config keys ending in `_env_var` are unwrapped from the
        # environment before dispatch — same convention as the top-level
        # `bootstrap_servers_env_var` / `sasl_password_env_var` fields.
        # Keeps secrets out of committed YAML.
        for k, v in (cfg.get("extra_config") or {}).items():
            if k.endswith("_env_var"):
                actual_key = k[: -len("_env_var")]
                env_name = str(v)
                val = _os.environ.get(env_name)
                if val is None:
                    raise ValueError(
                        f"streaming_consumer extra_config {k}={env_name!r} "
                        f"but env var unset"
                    )
                conf[actual_key] = val
            else:
                conf[k] = v

        self._consumer = Consumer(conf)
        self._topic = cfg["topic"]
        self._consumer.subscribe([self._topic])
        self._log = log
        self._value_deserializer = (cfg.get("value_deserializer") or "json").lower()

    def poll_batch(self, batch_size: int, timeout_ms: int) -> List[Dict[str, Any]]:
        """Pull up to `batch_size` messages within `timeout_ms` total wait."""
        rows: List[Dict[str, Any]] = []
        deadline_s = time.time() + (timeout_ms / 1000.0)
        while len(rows) < batch_size and time.time() < deadline_s:
            remaining = max(0.01, deadline_s - time.time())
            msg = self._consumer.poll(timeout=remaining)
            if msg is None:
                continue
            if msg.error():
                self._log.warning(f"kafka poll error: {msg.error()}")
                continue
            rows.append(self._parse(msg))
        return rows

    def _parse(self, msg) -> Dict[str, Any]:
        raw = msg.value()
        if raw is None:
            payload: Any = None
        elif self._value_deserializer == "json":
            try:
                payload = _json.loads(raw.decode("utf-8"))
            except Exception:  # noqa: BLE001
                payload = {"_raw": raw.decode("utf-8", errors="replace")}
        elif self._value_deserializer == "utf-8":
            payload = raw.decode("utf-8", errors="replace")
        else:  # 'bytes'
            payload = raw
        row = {"_topic": msg.topic(), "_partition": msg.partition(),
               "_offset": msg.offset(), "_timestamp_ms": msg.timestamp()[1] if msg.timestamp() else None}
        if isinstance(payload, dict):
            row.update(payload)
        else:
            row["value"] = payload
        return row

    def commit(self):
        self._consumer.commit(asynchronous=False)

    def close(self):
        try:
            self._consumer.close()
        except Exception:  # noqa: BLE001
            pass


_QUEUE_DISPATCHERS = {"kafka": _KafkaConsumer}


# ── Transform: reuse polars ops via a light applier ───────────────────
#
# The transform block accepts the same op vocabulary as
# `polars_pipeline.operations` (filter / with_columns / select / drop /
# rename / sort / drop_nulls / fill_null / cast / limit / head / unique).
# For consistency, keep the shape identical: `{op: <name>, ...}` dicts.


def _apply_ops(pl_module, lf, ops: List[Dict[str, Any]]):
    """Minimal polars ops applier — subset of polars_pipeline._apply_op
    kept self-contained per the components-are-self-contained convention.
    Ops: filter, with_columns, select, drop, rename, cast, drop_nulls,
    fill_null, sort, limit, head, unique."""
    for op in ops or []:
        kind = op["op"].lower()
        if kind == "filter":
            lf = lf.filter(pl_module.sql_expr(op["predicate"]))
        elif kind == "with_columns":
            exprs = op.get("expressions") or {}
            lf = lf.with_columns([pl_module.sql_expr(e).alias(name) for name, e in exprs.items()])
        elif kind == "select":
            lf = lf.select(op["columns"])
        elif kind == "drop":
            lf = lf.drop(op["columns"])
        elif kind == "rename":
            lf = lf.rename(op["mapping"])
        elif kind == "cast":
            mapping = op["mapping"]
            lf = lf.with_columns([pl_module.col(c).cast(getattr(pl_module, dt)) for c, dt in mapping.items()])
        elif kind == "drop_nulls":
            subset = op.get("columns")
            lf = lf.drop_nulls(subset=subset) if subset else lf.drop_nulls()
        elif kind == "fill_null":
            lf = lf.fill_null(op["value"])
        elif kind == "sort":
            cols = op.get("columns") or op.get("by")
            lf = lf.sort(cols, descending=op.get("descending", False))
        elif kind in ("limit", "head"):
            lf = lf.limit(int(op["n"]))
        elif kind == "unique":
            lf = lf.unique(subset=op.get("columns"))
        else:
            raise ValueError(f"streaming_consumer: unsupported transform op {kind!r}")
    return lf


# ── Sink: write to any Dagster resource (or DuckDB directly) ──────────


def _write_sink(df, sink_cfg: Dict[str, Any], context) -> int:
    """Write a batch to the configured sink. Returns rows actually inserted.

    Sink kinds:
      - `table`: SQLAlchemy-compat via a Dagster resource.
      - `duckdb`: direct `INSERT INTO` via a Dagster resource that has
        `.get_connection()` returning a DuckDB connection.

    Idempotency: when `dedup_on: [col1, col2, ...]` is set, rows whose key
    tuple already exists in the target table are silently skipped
    (NOT-EXISTS anti-join). Handy for Kafka replays after crash — since
    the consumer already prepends `_topic`, `_partition`, `_offset` to
    every row, `dedup_on: [_topic, _partition, _offset]` gives global
    uniqueness at zero user cost.
    """
    kind = (sink_cfg.get("kind") or "table").lower()
    dedup_on: List[str] = list(sink_cfg.get("dedup_on") or [])

    # Backward compat: `kind: duckdb` is now handled as an auto-detected
    # fast path under `kind: table`. Old YAML with `kind: duckdb` still
    # routes correctly since we probe the resource for `.register()`.
    if kind == "duckdb":
        kind = "table"

    if kind == "table":
        resource_key = sink_cfg.get("resource_key")
        if not resource_key:
            raise ValueError("sink kind=table requires resource_key")
        table = sink_cfg["table"]
        schema = sink_cfg.get("schema")
        if_exists = sink_cfg.get("if_exists", "append")
        resource = getattr(context.resources, resource_key)

        # Auto-detect the fast path: if the resource yields something with a
        # `.register()` method (DuckDB-style — register polars/pandas as a
        # view, then INSERT ... SELECT * FROM registered_view), use it — the
        # register-and-INSERT path is 10-100x faster than SQLAlchemy to_sql on
        # large batches. Otherwise fall back to SQLAlchemy to_sql, which
        # works with every SQLAlchemy resource (postgres / snowflake /
        # bigquery / mysql / mssql / oracle / db2 / …). Users write
        # `kind: table` either way — no vendor-specific YAML flags.
        from contextlib import nullcontext

        def _acquire():
            if hasattr(resource, "get_connection"):
                gc = resource.get_connection()
                if hasattr(gc, "__enter__"):
                    return gc
                return nullcontext(gc)
            if hasattr(resource, "get_engine"):
                eng = resource.get_engine()
                if hasattr(eng, "__enter__"):
                    return eng
                return nullcontext(eng)
            raise ValueError(f"resource {resource_key!r} needs .get_connection() or .get_engine()")

        with _acquire() as conn:
            # Fast path: connection has DuckDB's `.register()` method (register
            # polars/pandas as a view + INSERT ... SELECT * FROM). Zero-copy
            # for polars; much faster than to_sql. Auto-detected — user only
            # ever writes `kind: table`.
            if hasattr(conn, "register") and hasattr(conn, "execute") and hasattr(conn, "unregister"):
                conn.register("_batch", df)
                try:
                    if if_exists == "replace":
                        conn.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM _batch")
                        return df.height
                    conn.execute(f"CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM _batch WHERE 1=0")
                    if dedup_on:
                        match = " AND ".join(f"t.{c} = b.{c}" for c in dedup_on)
                        conn.execute("BEGIN TRANSACTION")
                        try:
                            conn.execute(
                                f"INSERT INTO {table} "
                                f"SELECT b.* FROM _batch b "
                                f"WHERE NOT EXISTS (SELECT 1 FROM {table} t WHERE {match})"
                            )
                            conn.execute("COMMIT")
                        except Exception:
                            conn.execute("ROLLBACK")
                            raise
                        return df.height  # optimistic; dedup rate visible via heartbeat delta
                    conn.execute("BEGIN TRANSACTION")
                    try:
                        conn.execute(f"INSERT INTO {table} SELECT * FROM _batch")
                        conn.execute("COMMIT")
                    except Exception:
                        conn.execute("ROLLBACK")
                        raise
                    return df.height
                finally:
                    try:
                        conn.unregister("_batch")
                    except Exception:  # noqa: BLE001
                        pass

            # Fallback: SQLAlchemy path (postgres / snowflake / bigquery / mysql
            # / mssql / oracle / db2 / duckdb-via-SQLAlchemy / ...). Slower but
            # portable. `conn` here is a SQLAlchemy Connection or Engine.
            if dedup_on and if_exists == "append":
                pdf = df.to_pandas()
                qual = f"{schema}.{table}" if schema else table
                select_cols = ", ".join(dedup_on)
                try:
                    import pandas as _pd
                    existing = _pd.read_sql(f"SELECT DISTINCT {select_cols} FROM {qual}", conn)
                except Exception:  # noqa: BLE001
                    existing = None
                if existing is not None and not existing.empty:
                    pdf = pdf.merge(existing, on=dedup_on, how="left", indicator=True)
                    pdf = pdf[pdf["_merge"] == "left_only"].drop(columns=["_merge"])
                if len(pdf) == 0:
                    return 0
                pdf.to_sql(table, conn, schema=schema, if_exists=if_exists, index=False)
                return len(pdf)
            df.to_pandas().to_sql(table, conn, schema=schema, if_exists=if_exists, index=False)
            return df.height
    raise ValueError(f"streaming_consumer sink kind={kind!r} not supported. Use 'table'.")


# ── The component ─────────────────────────────────────────────────────


class StreamingConsumerComponent(Component, Model, Resolvable):
    """Always-running queue → transform → sink asset.

    Emits one AssetMaterialization per batch (heartbeat) with batch_size,
    total_messages, elapsed_seconds, and last_offset in metadata. Exits
    cleanly at `max_seconds` OR when `max_messages` (if set) is reached —
    pair with a health sensor for auto-restart.

    Example:
        ```yaml
        type: dagster_community_components.StreamingConsumerComponent
        attributes:
          asset_name: order_events
          queue:
            kind: kafka
            topic: orders
            bootstrap_servers_env_var: KAFKA_BROKERS
            group_id: order_ingest_v1
            auto_offset_reset: latest
            value_deserializer: json
          batch_size: 500
          batch_timeout_ms: 1000
          max_seconds: 3600        # 1h bounded run; sensor restarts after
          transform:
            - {op: filter, predicate: "amount > 0"}
            - {op: with_columns, expressions: {ingest_ts: "NOW()"}}
          sink:
            kind: duckdb
            resource_key: duckdb
            table: order_events
            if_exists: append
        ```
    """

    asset_name: str = Field(description="Output asset name (also used by the health sensor).")
    queue: Dict[str, Any] = Field(
        description=(
            "Queue config. `{kind: kafka, topic, bootstrap_servers OR "
            "bootstrap_servers_env_var, group_id, auto_offset_reset?, "
            "value_deserializer?: json|utf-8|bytes, extra_config?, sasl_* / ssl_*?}`. "
            "v1 supports `kind: kafka`. Coming: rabbitmq, sqs, redis_stream, nats."
        ),
    )
    batch_size: int = Field(
        default=100, ge=1,
        description="Max messages per batch. Also caps AssetMaterialization heartbeat frequency.",
    )
    batch_timeout_ms: int = Field(
        default=500, ge=10,
        description="Max wait (ms) per batch before flushing even if under batch_size.",
    )
    max_seconds: Optional[int] = Field(
        default=3600,
        description=(
            "Bounded run duration in seconds. Set to `null` for a truly "
            "unbounded loop (runs until Dagster/Serverless kills the process). "
            "Bounded is safer: graceful shutdown (final Kafka commit + summary "
            "metadata + SUCCESS status), then the health sensor launches the "
            "next run. Unbounded skips the summary but eliminates gaps between "
            "runs. Either way, per-batch AssetMaterialization events fire "
            "continuously so the catalog UI shows a live heartbeat."
        ),
    )
    max_messages: Optional[int] = Field(
        default=None,
        description="Optional cap on total messages consumed before exit.",
    )
    transform: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description=(
            "Ordered polars ops applied to each batch. Same shape as "
            "polars_pipeline.operations — filter / with_columns / select / "
            "drop / rename / cast / drop_nulls / fill_null / sort / limit / "
            "unique. Omit for no-op passthrough."
        ),
    )
    sink: Dict[str, Any] = Field(
        description=(
            "Where to write each batch. `{kind: table | duckdb, resource_key, "
            "table, schema?, if_exists?: append|replace}`. `kind: duckdb` uses "
            "the DuckDB connection directly (register-and-INSERT) — 10-100x "
            "faster than the SQLAlchemy `table` path on large batches."
        ),
    )

    group_name: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    asset_tags: Optional[Dict[str, str]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)

    @classmethod
    def get_description(cls) -> str:
        return "Always-running queue consumer — polls a queue, transforms with polars, writes to a sink."

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        # Copy attrs into closure so the compute fn doesn't hold `self`.
        asset_name = self.asset_name
        queue_cfg = dict(self.queue)
        batch_size = self.batch_size
        batch_timeout_ms = self.batch_timeout_ms
        max_seconds = self.max_seconds
        max_messages = self.max_messages
        transform_ops = list(self.transform or [])
        sink_cfg = dict(self.sink)

        kind = queue_cfg.get("kind")
        if kind not in _QUEUE_DISPATCHERS:
            raise ValueError(
                f"streaming_consumer: queue.kind={kind!r} not supported. "
                f"v1 supports: {sorted(_QUEUE_DISPATCHERS)}"
            )
        if not sink_cfg.get("resource_key"):
            raise ValueError("streaming_consumer: sink.resource_key is required")

        _kinds = list(self.kinds or []) or ["streaming", "polars"]
        all_tags = dict(self.asset_tags or {})
        for k in _kinds:
            all_tags[f"dagster/kind/{k}"] = ""

        required_resource_keys = {sink_cfg["resource_key"]}

        @asset(
            key=AssetKey.from_user_string(asset_name),
            description=self.description or self.get_description(),
            owners=self.owners or [],
            tags=all_tags,
            group_name=self.group_name,
            kinds=set(_kinds),
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
            required_resource_keys=required_resource_keys,
        )
        def _streaming_asset(context: AssetExecutionContext) -> Dict[str, Any]:
            try:
                import polars as pl
            except ImportError:
                raise ImportError("streaming_consumer requires polars: pip install polars")

            consumer_cls = _QUEUE_DISPATCHERS[kind]
            consumer = consumer_cls(queue_cfg, context.log)

            context.log.info(
                f"streaming_consumer[{kind}] topic={queue_cfg.get('topic')!r} "
                f"batch_size={batch_size} max_seconds={max_seconds}"
            )
            start = time.time()
            # None → run forever (unbounded); Dagster / Serverless timeout will
            # eventually kill the process, and the health sensor restarts.
            deadline: Optional[float] = (start + max_seconds) if max_seconds is not None else None
            total_messages = 0
            total_batches = 0
            last_offset: Optional[int] = None
            # Track the max offset seen per (topic, partition) — this IS the
            # checkpoint marker. Kafka's broker-level commit stores the same
            # thing (`group_id` resumes automatically), but surfacing it in
            # asset metadata means a human can inspect "where did we get to"
            # per partition from the Dagster catalog.
            last_offset_by_partition: Dict[str, int] = {}
            stop_reason = "max_seconds" if deadline is not None else "external_signal"

            try:
                while deadline is None or time.time() < deadline:
                    if max_messages is not None and total_messages >= max_messages:
                        stop_reason = "max_messages"
                        break

                    batch_rows = consumer.poll_batch(batch_size, batch_timeout_ms)
                    if not batch_rows:
                        continue

                    df = pl.DataFrame(batch_rows).lazy()
                    df = _apply_ops(pl, df, transform_ops)
                    materialized = df.collect()
                    rows_written = 0
                    if materialized.height > 0:
                        rows_written = _write_sink(materialized, sink_cfg, context)

                    total_batches += 1
                    total_messages += len(batch_rows)
                    if "_offset" in materialized.columns:
                        try:
                            last_offset = int(materialized["_offset"].max())
                        except Exception:  # noqa: BLE001
                            last_offset = None
                        # Update the per-partition high-water mark.
                        if "_partition" in materialized.columns:
                            try:
                                for pkey, poff in materialized.group_by("_partition").agg(
                                    pl.col("_offset").max().alias("_max_off")
                                ).iter_rows():
                                    key = str(pkey)
                                    if poff is not None and (key not in last_offset_by_partition
                                                             or int(poff) > last_offset_by_partition[key]):
                                        last_offset_by_partition[key] = int(poff)
                            except Exception:  # noqa: BLE001
                                pass
                    # Commit AFTER the sink write — at-least-once semantics.
                    # Combined with sink.dedup_on, that gives effective exactly-once.
                    consumer.commit()

                    # Per-batch materialization heartbeat — visible in the catalog.
                    context.log_event(AssetMaterialization(
                        asset_key=asset_name,
                        description=f"batch {total_batches} — {len(batch_rows)} msgs ({rows_written} written)",
                        metadata={
                            "batch_size": MetadataValue.int(len(batch_rows)),
                            "rows_written": MetadataValue.int(rows_written),
                            "batch_index": MetadataValue.int(total_batches),
                            "total_messages": MetadataValue.int(total_messages),
                            "elapsed_seconds": MetadataValue.float(round(time.time() - start, 2)),
                            "last_offset": MetadataValue.int(last_offset) if last_offset is not None else MetadataValue.text("n/a"),
                            "checkpoint": MetadataValue.json(dict(last_offset_by_partition)),
                        },
                    ))
            finally:
                consumer.close()

            elapsed = time.time() - start
            context.log.info(
                f"streaming_consumer exiting: stop_reason={stop_reason} "
                f"batches={total_batches} msgs={total_messages} elapsed={elapsed:.1f}s"
            )
            # The final @asset return — one MaterializeResult; the batch
            # heartbeats above show up as separate materialization events.
            context.add_output_metadata({
                "stop_reason": MetadataValue.text(stop_reason),
                "total_messages": MetadataValue.int(total_messages),
                "total_batches": MetadataValue.int(total_batches),
                "elapsed_seconds": MetadataValue.float(round(elapsed, 2)),
                "msgs_per_second": MetadataValue.float(round(total_messages / max(elapsed, 1e-6), 2)),
                "queue_kind": MetadataValue.text(str(kind)),
                "queue_topic": MetadataValue.text(str(queue_cfg.get("topic") or "")),
                "final_checkpoint": MetadataValue.json(dict(last_offset_by_partition)),
                "sink_dedup_on": MetadataValue.json(list(sink_cfg.get("dedup_on") or [])),
            })
            return {
                "stop_reason": stop_reason,
                "total_messages": total_messages,
                "total_batches": total_batches,
                "elapsed_seconds": elapsed,
                "checkpoint": dict(last_offset_by_partition),
            }

        return Definitions(assets=[_streaming_asset])
