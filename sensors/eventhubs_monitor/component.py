"""Azure Event Hubs Monitor Sensor Component.

Monitors an Azure Event Hub for new events and triggers jobs when events are
detected. Passes event information via run_config to downstream assets.
Per-partition sequence numbers are tracked in the sensor cursor.
"""

import json
from typing import Optional

from dagster import (
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


class EventHubsMonitorSensorComponent(Component, Model, Resolvable):
    """Component for monitoring an Azure Event Hub for new events.

    This sensor reads events from all partitions of an Event Hub and triggers
    jobs when events are detected. Per-partition sequence numbers are stored in
    the sensor cursor to avoid reprocessing after restarts.

    Authentication uses DefaultAzureCredential by default. Alternatively, provide
    a connection string via an environment variable.

    Example:
        ```yaml
        type: dagster_component_templates.EventHubsMonitorSensorComponent
        attributes:
          sensor_name: eventhubs_sensor
          fully_qualified_namespace: mynamespace.servicebus.windows.net
          eventhub_name: my-event-hub
          job_name: process_events_job
          minimum_interval_seconds: 30
        ```
    """

    sensor_name: str = Field(description="Unique name for this sensor")

    eventhub_name: str = Field(description="Name of the Event Hub to monitor")

    job_name: str = Field(description="Name of the job to trigger when events are detected")

    fully_qualified_namespace: Optional[str] = Field(
        default=None,
        description="Event Hubs namespace hostname (e.g., 'mynamespace.servicebus.windows.net'). "
                    "Required when not using a connection string."
    )

    connection_string_env_var: Optional[str] = Field(
        default=None,
        description="Name of the environment variable containing the Event Hubs connection string. "
                    "If not provided, DefaultAzureCredential is used with fully_qualified_namespace."
    )

    consumer_group: str = Field(
        default="$Default",
        description="Event Hubs consumer group name"
    )

    max_events_per_partition: int = Field(
        default=100,
        description="Maximum number of events to read per partition per evaluation"
    )

    minimum_interval_seconds: int = Field(
        default=30,
        description="Minimum time (in seconds) between sensor evaluations"
    )

    resource_key: Optional[str] = Field(
        default=None,
        description="Optional Dagster resource key providing a pre-configured client. "
                    "When set, context.resources.<resource_key> is used instead of creating "
                    "a connection from the other fields. See README for the expected interface."
    )

    default_status: str = Field(
        default="running",
        description="Default status of the sensor (running or stopped)"
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        sensor_name = self.sensor_name
        eventhub_name = self.eventhub_name
        job_name = self.job_name
        fully_qualified_namespace = self.fully_qualified_namespace
        connection_string_env_var = self.connection_string_env_var
        consumer_group = self.consumer_group
        max_events_per_partition = self.max_events_per_partition
        minimum_interval_seconds = self.minimum_interval_seconds
        default_status_str = self.default_status
        resource_key = self.resource_key

        default_status = (
            DefaultSensorStatus.RUNNING
            if default_status_str == "running"
            else DefaultSensorStatus.STOPPED
        )

        required_resource_keys = {resource_key} if resource_key else set()

        @sensor(
            name=sensor_name,
            minimum_interval_seconds=minimum_interval_seconds,
            default_status=default_status,
            job_name=job_name,
            required_resource_keys=required_resource_keys,
        )
        def eventhubs_sensor(context: SensorEvaluationContext):
            """Sensor that monitors an Azure Event Hub for new events."""
            import os

            try:
                from azure.eventhub import EventHubConsumerClient
            except ImportError:
                return SensorResult(
                    skip_reason="azure-eventhub is not installed. "
                                "Run: pip install azure-eventhub azure-identity"
                )

            # cursor: {partition_id: sequence_number}
            cursor_data = {}
            if context.cursor:
                try:
                    cursor_data = json.loads(context.cursor)
                except json.JSONDecodeError:
                    context.log.warning("Could not parse cursor, starting from beginning")

            # Build client
            try:
                if connection_string_env_var:
                    conn_str = os.environ.get(connection_string_env_var)
                    if not conn_str:
                        return SensorResult(
                            skip_reason=f"Environment variable '{connection_string_env_var}' is not set"
                        )
                    client = EventHubConsumerClient.from_connection_string(
                        conn_str=conn_str,
                        consumer_group=consumer_group,
                        eventhub_name=eventhub_name,
                    )
                else:
                    if not fully_qualified_namespace:
                        return SensorResult(
                            skip_reason="Either connection_string_env_var or fully_qualified_namespace must be provided"
                        )
                    from azure.identity import DefaultAzureCredential
                    client = EventHubConsumerClient(
                        fully_qualified_namespace=fully_qualified_namespace,
                        eventhub_name=eventhub_name,
                        consumer_group=consumer_group,
                        credential=DefaultAzureCredential(),
                    )
            except Exception as e:
                context.log.error(f"Failed to create Event Hubs client: {e}")
                return SensorResult(skip_reason=f"Failed to create Event Hubs client: {e}")

            run_requests = []
            new_cursor_data = dict(cursor_data)

            try:
                partition_ids = client.get_partition_ids()

                for partition_id in partition_ids:
                    starting_position = cursor_data.get(partition_id, "@earliest")

                    events_collected = []

                    def on_event(partition_context, event):
                        events_collected.append((partition_context, event))
                        if len(events_collected) >= max_events_per_partition:
                            raise StopIteration

                    try:
                        client.receive(
                            on_event=on_event,
                            partition_id=partition_id,
                            starting_position=starting_position,
                            max_wait_time=5,
                        )
                    except StopIteration:
                        pass
                    except Exception as e:
                        context.log.error(f"Error reading partition {partition_id}: {e}")
                        continue

                    for partition_ctx, event in events_collected:
                        body = event.body_as_str(encoding="utf-8") if event.body else ""
                        sequence_number = event.sequence_number
                        offset = event.offset
                        enqueued_time = event.enqueued_time

                        run_requests.append(
                            RunRequest(
                                run_key=f"{eventhub_name}-{partition_id}-{sequence_number}",
                                run_config={
                                    "ops": {
                                        "config": {
                                            "eventhub_name": eventhub_name,
                                            "partition_id": partition_id,
                                            "sequence_number": sequence_number,
                                            "offset": str(offset),
                                            "enqueued_time": enqueued_time.isoformat() if enqueued_time else "",
                                            "body": body,
                                            "consumer_group": consumer_group,
                                        }
                                    }
                                },
                            )
                        )

                        new_cursor_data[partition_id] = sequence_number

            except Exception as e:
                context.log.error(f"Error reading from Event Hub '{eventhub_name}': {e}")
                return SensorResult(skip_reason=f"Error reading from Event Hubs: {e}")
            finally:
                client.close()

            if run_requests:
                context.log.info(f"Found {len(run_requests)} new event(s) from Event Hub '{eventhub_name}'")
                return SensorResult(
                    run_requests=run_requests,
                    cursor=json.dumps(new_cursor_data),
                )

            return SensorResult(
                skip_reason=f"No new events in Event Hub '{eventhub_name}'",
                cursor=json.dumps(new_cursor_data),
            )

        return Definitions(sensors=[eventhubs_sensor])
