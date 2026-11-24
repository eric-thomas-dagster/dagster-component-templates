"""Webhook Inbox Sensor Component.

Polls the local JSONL "inbox" file that `resources/webhook_inbox_receiver` appends to and
triggers jobs when new webhook callbacks arrive. This is the second half of a two-piece
pattern -- see `README.md` in this directory for why inbound webhooks need two components,
not one: a Dagster sensor is a poll loop evaluated by the Dagster daemon on an interval; it
cannot itself bind a port and receive a pushed HTTP request. `webhook_inbox_receiver` is the
always-on process that does that; this sensor is the ordinary polling consumer of what it
writes, exactly like `sqs_monitor` polls SQS or `s3_monitor` polls S3 -- just pointed at a
local file instead of an AWS service.

Cursor = a line count into the inbox file. Appends to that file are single `f.write(...)`
calls under a lock (see webhook_inbox_receiver), so a line is either fully written or not
present at all in a reader's view -- EXCEPT for the file's trailing partial line if a read
happens to land mid-write. This sensor defends against that: it only advances its cursor
past lines that both end in a newline AND parse as valid JSON; a half-written or corrupt
last line is left for the next tick rather than crashing the sensor or losing the event.
"""
import hashlib
import json
import os
from typing import Optional

from dagster import (
    AddDynamicPartitionsRequest,
    Component,
    ComponentLoadContext,
    Definitions,
    RunRequest,
    SensorEvaluationContext,
    SensorResult,
    sensor,
    Resolvable,
    Model,
)
from dagster._core.definitions.sensor_definition import DefaultSensorStatus
from pydantic import Field


def _dotted_lookup(obj, dotted_path: str):
    """Walks a dotted path (e.g. 'data.job.id') into a parsed JSON value. Returns None on any
    missing key / non-dict intermediate -- callers fall back to a content hash in that case."""
    current = obj
    for part in dotted_path.split("."):
        if not isinstance(current, dict) or part not in current:
            return None
        current = current[part]
    return current


def _event_id_for(body, event_id_field: str) -> str:
    """The event_id_field is a best-effort convenience, not a guaranteed dedupe key -- webhook
    payloads have no universal id convention the way S3 keys/etags do. Falls back to a stable
    hash of the whole payload (sorted keys) when the configured field is absent, documented
    here rather than silently treated as equivalent to a real vendor-assigned id."""
    value = _dotted_lookup(body, event_id_field) if isinstance(body, dict) else None
    if value is not None:
        return str(value)
    try:
        canonical = json.dumps(body, sort_keys=True, default=str)
    except TypeError:
        canonical = str(body)
    return "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]


def read_new_inbox_lines(inbox_path: str, start_line: int, max_events: Optional[int] = None):
    """Reads lines [start_line:] from inbox_path. Returns (records, next_cursor_line,
    skipped_malformed_count).

    A line is only counted toward next_cursor_line (i.e. will never be re-read) when it ends
    with '\\n' AND parses as JSON. A line ending in '\\n' that fails to parse is logged/skipped
    but STILL advances the cursor (a permanently corrupt line must not wedge the sensor
    forever). A line NOT ending in '\\n' is a write still in progress -- the read stops there
    entirely and that line is retried next tick.
    """
    if not os.path.isfile(inbox_path):
        return [], start_line, 0

    with open(inbox_path, "r", encoding="utf-8") as f:
        lines = f.readlines()  # each element keeps its trailing '\n', except possibly the last

    records = []
    skipped = 0
    cursor = start_line
    for i in range(start_line, len(lines)):
        line = lines[i]
        if not line.endswith("\n"):
            break  # partial write in progress -- stop here, retry this line next tick
        stripped = line.strip()
        cursor = i + 1
        if not stripped:
            continue
        try:
            records.append(json.loads(stripped))
        except ValueError:
            skipped += 1
            continue
        if max_events is not None and len(records) >= max_events:
            break

    return records, cursor, skipped


class WebhookInboxSensorComponent(Component, Model, Resolvable):
    """Component for polling a local webhook inbox JSONL file for new callbacks.

    Pairs with `resources/webhook_inbox_receiver`, which writes one JSONL line per accepted
    POST to that same `inbox_path`. See this directory's README.md for the full two-piece
    pattern and a worked end-to-end example.

    Example:
        ```yaml
        type: dagster_community_components.WebhookInboxSensorComponent
        attributes:
          sensor_name: ocr_callback_sensor
          inbox_path: /var/lib/webhooks/inbox.jsonl
          job_name: process_ocr_callback_job
          minimum_interval_seconds: 15
        ```
    """

    sensor_name: str = Field(description="Unique name for this sensor")

    inbox_path: str = Field(
        description="Path to the JSONL inbox file written by webhook_inbox_receiver. Must be "
                    "the same path/filesystem the receiver's --inbox-path points at."
    )

    job_name: str = Field(description="Name of the job to trigger when a webhook callback arrives")

    op_name: Optional[str] = Field(
        default=None,
        description="Op name the webhook payload is delivered to via run_config: "
                    "`{'ops': {op_name: {'config': {...}}}}`. Defaults to 'config'.",
    )

    minimum_interval_seconds: int = Field(
        default=30,
        description="Minimum time (in seconds) between sensor evaluations"
    )

    max_events_per_tick: int = Field(
        default=100,
        description="Maximum number of new inbox lines to turn into RunRequests in a single "
                    "evaluation. Caps how much work one tick does after a large backlog; the "
                    "remainder is picked up on the next tick."
    )

    partition_mode: str = Field(
        default="run_config",
        description=(
            "How a new webhook event is surfaced to downstream assets:\n"
            "  'run_config' (default) -- yields RunRequest with the event (received_at, "
            "remote_addr, path, body) in run_config.\n"
            "  'dynamic_partition' -- registers event_id_field's value (or a content hash "
            "fallback -- see event_id_field) as a dynamic partition via "
            "AddDynamicPartitionsRequest, then yields RunRequest(partition_key=event_id). "
            "Requires dynamic_partitions_name."
        ),
    )

    dynamic_partitions_name: Optional[str] = Field(
        default=None,
        description="Required when partition_mode is 'dynamic_partition'. Must match the "
                    "downstream asset's dynamic partition definition name.",
    )

    event_id_field: str = Field(
        default="id",
        description="Dotted-path key (e.g. 'job.id') looked up in each webhook payload to use "
                    "as a dedupe/partition key. Webhooks have no universal id convention, so "
                    "when this field is absent from a given payload, a stable hash of the full "
                    "payload is used instead -- documented behavior, not a guaranteed-unique id."
    )

    default_status: str = Field(
        default="running",
        description="Default status of the sensor (running or stopped)"
    )

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='Webhook Inbox Sensor', editable=True)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        sensor_name = self.sensor_name
        inbox_path = self.inbox_path
        job_name = self.job_name
        op_name = self.op_name or "config"
        minimum_interval_seconds = self.minimum_interval_seconds
        max_events_per_tick = self.max_events_per_tick
        partition_mode = (self.partition_mode or "run_config").lower()
        dynamic_partitions_name = self.dynamic_partitions_name
        event_id_field = self.event_id_field or "id"
        default_status_str = self.default_status

        if partition_mode not in ("run_config", "dynamic_partition"):
            raise ValueError(f"partition_mode must be 'run_config' | 'dynamic_partition', got {partition_mode!r}")
        if partition_mode == "dynamic_partition" and not dynamic_partitions_name:
            raise ValueError("partition_mode='dynamic_partition' requires dynamic_partitions_name.")

        default_status = (
            DefaultSensorStatus.RUNNING
            if default_status_str == "running"
            else DefaultSensorStatus.STOPPED
        )

        @sensor(
            name=sensor_name,
            minimum_interval_seconds=minimum_interval_seconds,
            default_status=default_status,
            job_name=job_name,
        )
        def webhook_inbox_sensor(context: SensorEvaluationContext):
            """Sensor that polls a local webhook inbox JSONL file for new callbacks."""
            try:
                start_line = int(context.cursor) if context.cursor else 0
            except (TypeError, ValueError):
                context.log.warning(f"Unparseable cursor {context.cursor!r} -- restarting from line 0")
                start_line = 0

            records, next_cursor, skipped = read_new_inbox_lines(inbox_path, start_line, max_events_per_tick)

            if skipped:
                context.log.warning(f"Skipped {skipped} malformed inbox line(s) in {inbox_path}")

            if not records:
                context.update_cursor(str(next_cursor))
                return SensorResult(skip_reason=f"No new webhook events in {inbox_path}")

            run_requests = []
            new_partition_keys = []
            for record in records:
                body = record.get("body") if isinstance(record, dict) else None
                event_id = _event_id_for(body, event_id_field)

                if partition_mode == "dynamic_partition":
                    new_partition_keys.append(event_id)
                    run_requests.append(RunRequest(run_key=event_id, partition_key=event_id))
                else:
                    run_requests.append(
                        RunRequest(
                            run_key=event_id,
                            run_config={
                                "ops": {
                                    op_name: {
                                        "config": {
                                            "event_id": event_id,
                                            "received_at": record.get("received_at", "") if isinstance(record, dict) else "",
                                            "remote_addr": record.get("remote_addr", "") if isinstance(record, dict) else "",
                                            "path": record.get("path", "") if isinstance(record, dict) else "",
                                            "body": body,
                                        }
                                    }
                                }
                            },
                        )
                    )

            dynamic_partitions_requests = (
                [AddDynamicPartitionsRequest(partitions_def_name=dynamic_partitions_name, partition_keys=new_partition_keys)]
                if new_partition_keys and dynamic_partitions_name else []
            )

            context.log.info(f"Found {len(run_requests)} new webhook event(s) in {inbox_path} (partition_mode={partition_mode})")
            context.update_cursor(str(next_cursor))
            return SensorResult(run_requests=run_requests, dynamic_partitions_requests=dynamic_partitions_requests)

        return Definitions(sensors=[webhook_inbox_sensor])
