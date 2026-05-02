# Http Poll Sensor

Generic HTTP polling sensor. Hits a URL on each evaluation, computes a hash of a **targeted slice** of the response, and triggers a `RunRequest` when the hash differs from the cursor.

## Why a targeted hash?

Hashing the whole response body is fragile in practice — most pages and APIs change small things on every request that aren't real content changes:

- "Last updated 5 seconds ago" widgets
- Ad slot rotations
- Nonces / CSRF tokens / session IDs
- Trace IDs reflected in HTML

A naive whole-body hash treats those as content changes and fires the sensor on every tick. The targeting strategies below let you hash only the parts you actually care about.

## Targeting strategies

Pick one (or combine `strip_patterns` with anything):

| Field | Use when | Example |
|---|---|---|
| `json_path: "data.score"` | Response is JSON; hash a single value | `"current.temperature_2m"` |
| `regex_extract: "..."` | Pull match groups; hash their concatenation | `"score: ([\d-]+)"` |
| `css_selector: "..."` | Response is HTML; hash text of selected elements (needs `beautifulsoup4`) | `"div.scores"` |
| `strip_patterns: [...]` | Remove known noise before hashing the rest | `["Last updated:[^<]*", "<!--.*?-->"]` |
| (none) | Hash the full body — only safe for stable APIs | n/a |

## Fields

| Field | Type | Default | Required | Notes |
|---|---|---|---|---|
| `sensor_name` | `str` | — | ✓ | Unique sensor name |
| `asset_keys` | `List[str]` | — | ✓ | Asset keys to materialize when the response changes |
| `url` | `str` | — | ✓ | URL to poll |
| `method` | `str` | `GET` | | HTTP method |
| `headers` | `Dict[str, str]` | `None` | | Optional headers |
| `timeout_seconds` | `int` | `30` | | HTTP timeout |
| `expected_status` | `int` | `200` | | Non-match → skip |
| `json_path` | `str` | `None` | | Dotted JSON path |
| `regex_extract` | `str` | `None` | | Regex; hashes concat of matches |
| `css_selector` | `str` | `None` | | CSS selector (needs bs4) |
| `strip_patterns` | `List[str]` | `None` | | Regexes to remove before hashing |
| `minimum_interval_seconds` | `int` | `60` | | Seconds between evaluations |
| `default_status` | `str` | `STOPPED` | | `RUNNING` or `STOPPED` |

## Examples

### NBA scoreboard (no auth)

```yaml
type: dagster_component_templates.HttpPollSensorComponent
attributes:
  sensor_name: nba_scoreboard_sensor
  asset_keys: [nba_scoreboard_summary]
  url: "https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json"
  json_path: "scoreboard.gameDate"
  minimum_interval_seconds: 600
```

### Weather forecast change (no auth)

```yaml
attributes:
  sensor_name: nyc_temp_change
  asset_keys: [weather_alert]
  url: "https://api.open-meteo.com/v1/forecast?latitude=40.71&longitude=-74.01&current=temperature_2m"
  json_path: "current.temperature_2m"
  minimum_interval_seconds: 600
```

### HTML page with noise

```yaml
attributes:
  sensor_name: status_page_sensor
  asset_keys: [downstream_check]
  url: "https://status.example.com/"
  css_selector: "div.incident-list"
  strip_patterns:
    - "Last updated:[^<]*"
    - "data-trace-id=\"[^\"]+\""
  minimum_interval_seconds: 60
```

### Combining strategies

```yaml
# JSON response that's mostly stable but has a server-side `last_polled_at`
# timestamp at the top level. Strip it, then hash the rest.
attributes:
  sensor_name: regulatory_filings
  asset_keys: [filings_summary]
  url: "https://api.example.com/filings"
  strip_patterns:
    - '"last_polled_at":\s*"[^"]+",?'
```

## Tags emitted on the RunRequest

The sensor tags each RunRequest with `hash_strategy` (which targeting strategy fired it) and `digest_prefix` (first 8 chars of the hash) so you can answer "why did this sensor fire?" from the run log.

## Requirements

```
requests
beautifulsoup4   # only needed if you use css_selector
```
