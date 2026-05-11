# HL7 v2 Parser

Parse pipe-delimited HL7 v2 messages into a flat DataFrame. Emits one row per segment-of-interest with the MSH (message header) inherited as context columns on every row.

```yaml
type: dagster_component_templates.Hl7V2ParserComponent
attributes:
  asset_name: hl7_segments
  upstream_asset_key: hl7_messages
  message_column: message
  keep_segments: [MSH, PID, OBX]
```

## Why this exists

HL7 v2 is the dominant messaging format inside hospitals — ADT (admit/discharge/transfer), LAB results, RIS, etc. It's pipe-delimited text, not JSON, so it doesn't fit the typical ingestion components. This component does the parse-and-flatten step that every HL7 ingest pipeline needs.

## Supported segments (with full extractors)

| Segment | What it carries |
|---|---|
| `MSH` | message header — sending app, datetime, message type, control id, version |
| `PID` | patient demographics — id, name, DOB, sex, address |
| `OBX` | observation result — code, value, units, ref range, abnormal flags |

Other segments (`ORC`, `OBR`, `PV1`, `EVN`, `DG1`, `AL1`) are listed in the enum but currently skipped. Fork the component or wrap [`hl7apy`](https://pypi.org/project/hl7apy/) for full coverage.

## Output shape

One row per **kept segment** per **input message**. Every row carries the parent MSH's `msg_control_id`, `message_type`, `sending_app`, `version_id` so you can group by message without re-joining.

Example: for the typical ADT^A01 with MSH + PID + 1 PV1, with `keep_segments: [MSH, PID]`, you get 2 rows. For a LAB ORU^R01 with MSH + PID + OBR + 3 OBX, with `keep_segments: [MSH, PID, OBX]`, you get 5 rows (1 MSH + 1 PID + 3 OBX).

## Carry-over columns

Any non-message column on the upstream row (e.g. `ingested_at`, `source_system`, `batch_id`) is carried into every output row.

## Standard delimiters

HL7 declares its own delimiters in the MSH header:
- `MSH-1` is the field separator (almost always `|`)
- `MSH-2` is the encoding chars: component=`^`, repetition=`~`, escape=`\`, subcomponent=`&`

The parser auto-detects both. The vast majority of real HL7 traffic uses standard delimiters.

## Validation

Messages that don't start with `MSH` are recorded as parse errors (one row with `_error` set). The `errors` metadata field counts them.

## Sister components

- `fhir_resource_normalizer` — modern healthcare standard (JSON, R4/R5)
- `synthetic_data_generator` `hl7_messages` schema — common upstream for demos
- `dataframe_to_bigquery` — warehouse downstream
