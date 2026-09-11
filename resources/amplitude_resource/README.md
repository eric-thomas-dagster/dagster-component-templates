# Amplitude Resource

Registers an `AmplitudeResource` under a resource key. Holds Amplitude
auth (API key in JSON body per HTTP V2 spec) and exposes typed batch
operations (`track_events_bulk`) for downstream Amplitude sinks.

Pairs with **`DataframeToAmplitudeComponent`**.

## Configuration

| Field | Required | Default | What |
|---|---|---|---|
| `resource_key` | | `amplitude` | Dagster resource key |
| `api_key_env_var` | | `AMPLITUDE_API_KEY` | Env var holding the Amplitude project API key |
| `base_url` | | `https://api2.amplitude.com` | Amplitude API base URL. Use `https://api.eu.amplitude.com` for EU projects |
| `request_timeout_seconds` | | `30` | Per-request timeout |

## Example

```yaml
type: dagster_community_components.AmplitudeResourceComponent
attributes:
  resource_key: amplitude
  api_key_env_var: AMPLITUDE_API_KEY
```
