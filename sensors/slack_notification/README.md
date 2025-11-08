# Slack Notification Sensor

Monitor Slack channels for new messages and trigger Dagster jobs when messages match specified criteria.

## Features

- Monitor any Slack channel your bot has access to
- Filter by keyword to only trigger on relevant messages
- Filter by user to only trigger on messages from specific users
- Cursor-based tracking prevents reprocessing messages
- Passes message content and metadata to your job

## Configuration

### Required Parameters

- **sensor_name** (string) - Unique identifier for this sensor
- **slack_token** (string) - Slack Bot User OAuth token (requires `channels:history` and `channels:read` scopes)
- **channel_id** (string) - Slack channel ID to monitor (e.g., `C1234567890`)
- **job_name** (string) - Name of the Dagster job to trigger

### Optional Parameters

- **keyword_filter** (string) - Only trigger on messages containing this keyword (default: none, triggers on all messages)
- **user_filter** (string) - Only trigger on messages from this Slack user ID (default: none, triggers on messages from any user)
- **minimum_interval_seconds** (number) - How often to check for new messages (default: 60 seconds)
- **default_status** (string) - Initial sensor status: "RUNNING" or "STOPPED" (default: "RUNNING")

## Usage Example

```yaml
type: dagster_component_templates.SlackNotificationSensorComponent
attributes:
  sensor_name: alert_monitor
  slack_token: ${SLACK_TOKEN}
  channel_id: C1234567890
  job_name: process_alert_job
  keyword_filter: "alert"
  minimum_interval_seconds: 60
```

## Setup Instructions

### 1. Create a Slack App

1. Go to https://api.slack.com/apps
2. Click "Create New App" → "From scratch"
3. Name your app and select your workspace

### 2. Add Bot Token Scopes

Go to "OAuth & Permissions" and add these Bot Token Scopes:
- `channels:history` - View messages in public channels
- `channels:read` - View basic channel information

### 3. Install App to Workspace

1. Click "Install to Workspace"
2. Copy the "Bot User OAuth Token" (starts with `xoxb-`)
3. Set as environment variable: `SLACK_TOKEN=xoxb-...`

### 4. Add Bot to Channel

In Slack, type `/invite @YourBotName` in the channel you want to monitor.

### 5. Get Channel ID

1. Right-click the channel name → "View channel details"
2. Scroll down to find the Channel ID
3. Use this ID in your configuration

## Environment Variables

This component uses the following environment variables:

- `SLACK_TOKEN` - Slack Bot User OAuth token (xoxb-...)

## Requirements

### Python Packages

- slack-sdk >= 3.19.0

### Slack Permissions

- Bot Token Scopes: `channels:history`, `channels:read`
- Bot must be invited to the channel

## Job Configuration

The sensor passes the following data to your job via `run_config`:

```python
{
  "ops": {
    "config": {
      "message_text": "The message content",
      "user_id": "U1234567890",
      "channel_id": "C1234567890",
      "timestamp": "1234567890.123456",
      "thread_ts": "1234567890.123456"  # If message is in a thread
    }
  }
}
```

### Example Job

```python
from dagster import op, job

@op
def process_slack_message(context):
    message_text = context.op_config["message_text"]
    user_id = context.op_config["user_id"]

    context.log.info(f"Processing message from {user_id}: {message_text}")

    # Your logic here
    # - Send notifications
    # - Trigger workflows
    # - Store in database
    # etc.

@job
def process_alert_job():
    process_slack_message()
```

## How It Works

1. **Polling**: Sensor checks the configured Slack channel at the specified interval
2. **Filtering**: Messages are filtered by keyword and/or user if specified
3. **Cursor Tracking**: The sensor tracks the timestamp of the last processed message
4. **Job Triggering**: For each matching message, a job run is triggered with message metadata
5. **State Management**: Cursor is updated to prevent reprocessing messages

## Limitations

- Only monitors public channels (not private channels or DMs)
- Maximum 100 messages fetched per check
- Bot messages are automatically filtered out
- Requires bot to be invited to the channel

## Troubleshooting

### Issue: "channel_not_found" error

**Solution:**
1. Verify the channel ID is correct
2. Ensure the bot is invited to the channel (use `/invite @BotName`)
3. Check that the channel is not private (or add `groups:history` scope for private channels)

### Issue: "not_authed" or "invalid_auth" error

**Solution:**
1. Verify `SLACK_TOKEN` environment variable is set correctly
2. Ensure the token starts with `xoxb-`
3. Check that the app is installed to the workspace

### Issue: No messages detected

**Solution:**
1. Ensure there are new messages in the channel after the sensor starts
2. Check keyword_filter and user_filter settings aren't too restrictive
3. Verify minimum_interval_seconds allows enough time between checks

### Issue: Sensor triggers multiple times for same message

**Solution:**
- Check that cursor is being properly persisted
- Ensure sensor isn't being reset or redeployed frequently

## Advanced Usage

### Multiple Channel Monitoring

To monitor multiple channels, create separate component configurations:

```yaml
# alerts.yaml
type: dagster_component_templates.SlackNotificationSensorComponent
attributes:
  sensor_name: alerts_sensor
  channel_id: C111111
  # ...

# incidents.yaml
type: dagster_component_templates.SlackNotificationSensorComponent
attributes:
  sensor_name: incidents_sensor
  channel_id: C222222
  # ...
```

### Filtering by Regular Expressions

While the component supports simple keyword matching, you can extend the logic to support regex patterns by modifying `component.py`.

## Contributing

Found a bug or have a feature request?

- Open an issue: https://github.com/eric-thomas-dagster/dagster-component-templates/issues
- Submit a PR: https://github.com/eric-thomas-dagster/dagster-component-templates/pulls

## License

MIT License
