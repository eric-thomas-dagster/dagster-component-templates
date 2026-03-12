# Twilio Notification Sensor

Send SMS or WhatsApp messages to one or more recipients when Dagster assets fail or succeed.  Follows the same notification-sensor pattern as the `slack_notification` and `pagerduty_alert` components.

The sensor calls the **Twilio Messaging REST API directly** via `requests` — the official `twilio` Python SDK is not required, keeping the dependency footprint minimal.

> **Note on `dagster-twilio`:** The official `dagster-twilio` package provides a `TwilioResource` that wraps the Twilio SDK, but it does not include a sensor.  This component fills that gap: it monitors asset events and dispatches messages without requiring any additional Dagster integration package.

## Features

- SMS and WhatsApp delivery via Twilio
- Configurable per-event-type: alert on failures, successes, or both
- Optional asset allowlist — watch every asset or only a named subset
- Customisable message template with `{asset_key}`, `{event_type}`, `{timestamp}`, `{run_id}` placeholders
- Cursor-based event tracking prevents duplicate notifications across evaluations
- Credentials resolved from environment variables at runtime — never stored in YAML

## SMS vs WhatsApp

Set `channel` to control the messaging channel:

| Value | Behaviour |
|---|---|
| `sms` (default) | Standard SMS — `from_number` must be a Twilio SMS-capable number |
| `whatsapp` | WhatsApp message — the sensor automatically prefixes `from_number` and each `to_number` with `whatsapp:` |

For WhatsApp you need a Twilio number enrolled in the WhatsApp Business API (or a Twilio Sandbox number for testing).  See the [Twilio WhatsApp docs](https://www.twilio.com/docs/whatsapp) for setup steps.

## Message Templates

The `message_template` field is a Python format string.  Available placeholders:

| Placeholder | Value |
|---|---|
| `{asset_key}` | The asset key string (e.g., `"namespace/my_asset"`) |
| `{event_type}` | Human-readable event label, e.g., `"FAILED"` or `"materialized successfully"` |
| `{timestamp}` | UTC timestamp of the event, e.g., `"2024-03-01 14:30:00 UTC"` |
| `{run_id}` | Dagster run ID (UUID) |

Example template:

```yaml
message_template: "Alert: {asset_key} {event_type} at {timestamp} — run {run_id}"
```

If your template contains an unknown placeholder, the sensor falls back to a default message and logs a warning.

## Monitoring Specific vs All Assets

Leave `monitored_assets` unset (or `null`) to receive alerts for every asset in your deployment:

```yaml
# Watch everything
monitored_assets: ~
```

Provide a list of asset key strings to watch only those assets:

```yaml
monitored_assets:
  - revenue_forecast
  - warehouse/fact_orders
  - ml/feature_store/user_embeddings
```

Asset key strings use `/` as the namespace separator, matching the display in the Dagster UI.

## Configuration

### Required Parameters

- **sensor_name** — Unique sensor identifier
- **account_sid_env_var** — Environment variable name for the Twilio Account SID
- **auth_token_env_var** — Environment variable name for the Twilio Auth Token
- **from_number** — Twilio sender number in E.164 format (e.g., `"+15551234567"`)
- **to_numbers** — List of recipient numbers in E.164 format

### Optional Parameters

- **channel** (default: `"sms"`) — `"sms"` or `"whatsapp"`
- **message_template** (default: `"Dagster alert: {asset_key} {event_type} at {timestamp}"`)
- **monitored_assets** (default: all assets)
- **on_failure** (default: `true`) — notify on asset failure
- **on_success** (default: `false`) — notify on asset success
- **minimum_interval_seconds** (default: `30`)

## Usage Examples

### Basic failure alerts

```yaml
type: dagster_component_templates.TwilioNotificationSensor
attributes:
  sensor_name: pipeline_sms_alerts
  account_sid_env_var: TWILIO_ACCOUNT_SID
  auth_token_env_var: TWILIO_AUTH_TOKEN
  from_number: "+15551234567"
  to_numbers:
    - "+15559876543"
  on_failure: true
  on_success: false
  message_template: "Dagster alert: {asset_key} {event_type} at {timestamp} (run: {run_id})"
```

### WhatsApp alerts on specific assets

```yaml
type: dagster_component_templates.TwilioNotificationSensor
attributes:
  sensor_name: critical_asset_whatsapp_alerts
  account_sid_env_var: TWILIO_ACCOUNT_SID
  auth_token_env_var: TWILIO_AUTH_TOKEN
  from_number: "+14155238886"
  to_numbers:
    - "+15559876543"
    - "+15550001111"
  channel: whatsapp
  message_template: "CRITICAL: {asset_key} {event_type} at {timestamp}"
  monitored_assets:
    - revenue_forecast
    - fact_orders
  on_failure: true
  on_success: true
  minimum_interval_seconds: 60
```

## Twilio Account Setup

1. Sign up at [twilio.com](https://www.twilio.com).
2. From the Twilio Console dashboard, copy your **Account SID** and **Auth Token**.
3. Provision a phone number (SMS) or enrol a number for WhatsApp.
4. Set environment variables:

```bash
export TWILIO_ACCOUNT_SID=ACxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
export TWILIO_AUTH_TOKEN=your_auth_token_here
```

5. For WhatsApp in development, use the [Twilio Sandbox](https://www.twilio.com/docs/whatsapp/sandbox) — no business approval required.

## Environment Variables

| Variable | Description |
|---|---|
| `TWILIO_ACCOUNT_SID` | Your Twilio Account SID (`ACxxx...`) |
| `TWILIO_AUTH_TOKEN` | Your Twilio Auth Token |

Variable names are configurable via `account_sid_env_var` and `auth_token_env_var`.

## Requirements

- `requests>=2.28.0`
- A Twilio account with a provisioned phone number

## How It Works

1. On each evaluation the sensor reads the Dagster instance event log for records newer than the stored cursor (a `storage_id` integer).
2. Events are filtered by type (`ASSET_MATERIALIZATION` for successes, `STEP_FAILURE` for failures) and optionally by asset key.
3. For each matching event a message is formatted from the template and sent to every `to_numbers` recipient via `POST https://api.twilio.com/2010-04-01/Accounts/{sid}/Messages.json`.
4. The cursor is advanced to the latest `storage_id` seen, preventing re-notification on the next evaluation.

## Troubleshooting

**"Environment variable 'TWILIO_ACCOUNT_SID' is not set"**
Set the variable in your Dagster deployment environment (daemon, webserver, and user code server all need it).

**Messages not delivered**
- Verify `from_number` is a valid Twilio number with the correct capability (SMS or WhatsApp).
- Confirm `to_numbers` are in E.164 format (`+` followed by country code and number).
- Check the Twilio Console Message Logs for delivery errors.

**WhatsApp messages not delivered**
- Ensure both the sender and recipient have joined the Twilio Sandbox (sandbox only) or that your number is production-approved.
- The `whatsapp:` prefix is added automatically — do not include it in your config.

**Duplicate notifications**
Cursor persistence requires a durable Dagster instance storage backend. In-memory SQLite (used in `dagster dev` by default) does not persist across restarts.  Use PostgreSQL or another persistent backend in production.
