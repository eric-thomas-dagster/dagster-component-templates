# FIX Message Parser

Parse FIX (Financial Information eXchange) trading messages — the dominant electronic-trading protocol — into a flat DataFrame. Resolves common tag IDs to friendly columns; preserves the full tag map in `tags_raw`.

```yaml
type: dagster_component_templates.FixMessageParserComponent
attributes:
  asset_name: fix_messages_parsed
  upstream_asset_key: fix_messages
  message_column: message
  msg_type_filter: [D, 8]   # orders + executions only
```

## Auto-detected delimiter

FIX canonical delimiter is SOH (`\x01`). Log files often render it as `|`. Both are auto-detected — no configuration needed.

## Message types resolved

| MsgType | Name |
|---|---|
| `D` | NewOrderSingle |
| `F` | OrderCancelRequest |
| `G` | OrderCancelReplaceRequest |
| **`8`** | **ExecutionReport** (fills, partial fills, rejects — the heavy-volume one) |
| `9` | OrderCancelReject |
| `3` | Reject (session-level) |
| `0` | Heartbeat |
| `1` | TestRequest |
| `2` | ResendRequest |
| `A` | Logon |
| `5` | Logout |
| `W` | MarketDataSnapshotFullRefresh |

Others come through with `msg_type` populated but `msg_type_name = None`.

## Columns produced

| Column | FIX tag | Notes |
|---|---|---|
| `begin_string` | 8 | e.g. `FIX.4.4`, `FIXT.1.1` |
| `msg_type` / `msg_type_name` | 35 | Raw + resolved |
| `sender` / `target` | 49 / 56 | SenderCompID, TargetCompID |
| `cl_ord_id` / `order_id` / `exec_id` | 11 / 37 / 17 | Identifiers |
| `symbol` | 55 | Instrument |
| `side_code` / `side` | 54 | `1`→`buy`, `2`→`sell`, etc. |
| `ord_type_code` / `ord_type` | 40 | `1`→`market`, `2`→`limit`, etc. |
| `time_in_force_code` / `time_in_force` | 59 | `0`→`day`, `3`→`ioc`, `4`→`fok`, etc. |
| `ord_status_code` / `ord_status` | 39 | New / partially_filled / filled / canceled / rejected / etc. |
| `exec_type_code` / `exec_type` | 150 | Specific event type within ExecutionReport |
| `order_qty`, `price`, `last_px`, `last_qty`, `cum_qty`, `leaves_qty`, `avg_px` | 38, 44, 31, 32, 14, 151, 6 | Numeric (float-cast) |
| `sending_time`, `transact_time` | 52, 60 | UTC timestamps |
| `tags_raw` | — | Full dict of every tag in the message |

## `msg_type_filter`

For high-volume trading feeds, filter at parse time:
```yaml
msg_type_filter: [D, 8]    # orders + executions only — skip heartbeats, market data
```

## Carry-over columns

Any non-message column on the upstream row (e.g. `ingested_at`, `session_id`, `exchange`) is carried into every output row.

## Sister components

- [`iso20022_payment_parser`](https://github.com/eric-thomas-dagster/dagster-component-templates/tree/main/assets/transforms/iso20022_payment_parser) — payments standard (different domain in fintech)
- [`x12_edi_parser`](https://github.com/eric-thomas-dagster/dagster-component-templates/tree/main/assets/transforms/x12_edi_parser) — US-domestic B2B EDI
- [`synthetic_data_generator`](https://github.com/eric-thomas-dagster/dagster-component-templates/tree/main/assets/ai/synthetic_data_generator) `fix_messages` schema — common upstream for demos
